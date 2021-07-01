# -*- coding: utf-8 -*-
# =============================================================================
#     FileName: args_demo.py
#         Desc: 演示解析命令参数和读取文件
#       Author: GGA
#        Email:
#     HomePage:
#      Version: 1.0.1
#   LastChange: 2020-12-20
#      History:
# =============================================================================
import os
import sys
import traceback
import json
import argparse
from common_utils.mysql_server import MySQLServer

from common_utils.logger_helper import LogHelper
from common_utils.redis_parser import MyRedisParser
from aliyun_utils.ali_helper import AliHelper
from aliyun_utils.ali_config import AliConfig
from aliyun_redis.ali_redis_helper import AliRedisHelper

logger = LogHelper.get_logger()


class BaseConfig(object):
    RDB_DOWNLOAD_METHOD = ""
    TARGET_MYSQL_SERVER = None
    MYSQL_BATCH_INSERT_ROWS = 1000
    MYSQL_BATCH_SLEEP_SECONDS = 0.1
    CHECK_REDIS_SERVERS = []
    SKIP_REDIS_SERVERS = []
    MIN_REDIS_KEY_LENGTH = 0


class RedisServer(object):
    def __init__(self, instance_id, instance_name, region_id, instance_host, instance_port, instance_class):
        self.instance_id = instance_id
        self.instance_name = instance_name
        self.region_id = region_id
        self.instance_host = instance_host
        self.instance_port = instance_port
        self.instance_class = instance_class


def get_parser():
    parser = argparse.ArgumentParser(
        description='Parse MySQL binlog to SQL you want',
        add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    parser.add_argument(
        '--help',
        dest='help',
        action='store_true',
        help='help information',
        default=False
    )
    connect_setting.add_argument(
        '--config_file',
        dest='config_file',
        type=str,
        default=None,
        help='the file path which contain host info'
    )
    return parser


def parse_args(command_args):
    need_print_help = False if command_args else True
    parser = get_parser()
    args = parser.parse_args(command_args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if args.config_file is None:
        logger.warning("parameter config_file must input")
        parser.print_help()
    return args


def load_configs(config_file):
    logger.info("配置文件为：{}".format(config_file))
    with open(config_file, 'r', encoding="utf8") as file_handler:
        dict_configs = json.load(file_handler)
    AliConfig.access_key_id = dict_configs["common_configs"]["access_key_id"]
    AliConfig.access_key_secret = dict_configs["common_configs"]["access_key_secret"]
    AliConfig.region_ids = dict_configs["common_configs"]["region_ids"]
    BaseConfig.RDB_DOWNLOAD_METHOD = dict_configs["common_configs"].get("rdb_download_method", "INTRANET")
    BaseConfig.MYSQL_BATCH_INSERT_ROWS = dict_configs["common_configs"].get("mysql_batch_insert_rows", 100)
    BaseConfig.MYSQL_BATCH_SLEEP_SECONDS = dict_configs["common_configs"].get("mysql_batch_sleep_seconds", 0.01)
    BaseConfig.MIN_REDIS_KEY_LENGTH = dict_configs["common_configs"].get("min_redis_key_length", 0)
    mysql_configs = dict_configs["mysql_configs"]
    BaseConfig.TARGET_MYSQL_SERVER = MySQLServer(
        mysql_host=mysql_configs.get("mysql_host"),
        mysql_port=mysql_configs.get("mysql_port", 3306),
        mysql_user=mysql_configs.get("mysql_user"),
        mysql_password=mysql_configs.get("mysql_password"),
        mysql_charset=mysql_configs.get("mysql_charset", "utf8mb4"),
        database_name=mysql_configs.get("database_name"),
        connect_timeout=mysql_configs.get("connect_timeout", 60),
    )
    redis_server_tags = dict_configs["check_server_tags"]
    skip_redis_ids = []
    check_redis_ids = []
    redis_list = AliRedisHelper.get_instance_list()
    for redis_server in dict_configs["skip_servers"]:
        skip_redis_ids.append(redis_server["instance_id"])
    for redis_server in dict_configs["check_servers"]:
        check_redis_ids.append(redis_server["instance_id"])
    for redis_item in redis_list:
        is_check_server = False
        for redis_server_tag in redis_server_tags:
            if redis_item.redis_tags.find(redis_server_tag) >= 0:
                logger.info("实例{instance_name}({instance_id})满足标签{redis_server_tag}".format(
                    instance_name=redis_item.instance_name,
                    instance_id=redis_item.instance_id,
                    redis_server_tag=redis_server_tag
                ))
                is_check_server = True
            else:
                logger.info("实例{instance_name}({instance_id})满足不标签{redis_server_tag}".format(
                    instance_name=redis_item.instance_name,
                    instance_id=redis_item.instance_id,
                    redis_server_tag=redis_server_tag
                ))
        if redis_item.instance_id in check_redis_ids:
            is_check_server = True
        if redis_item.instance_id in skip_redis_ids:
            is_check_server = False
        if is_check_server:
            logger.info("实例{instance_name}({instance_id})被选中".format(
                instance_name=redis_item.instance_name,
                instance_id=redis_item.instance_id
            ))
            BaseConfig.CHECK_REDIS_SERVERS.append(
                RedisServer(
                    region_id=redis_item.region_id,
                    instance_id=redis_item.instance_id,
                    instance_name=redis_item.instance_name,
                    instance_host=redis_item.instance_host,
                    instance_port=redis_item.instance_port,
                    instance_class=redis_item.instance_class_desc
                )
            )
        else:
            logger.info("实例{instance_name}({instance_id})被忽略".format(
                instance_name=redis_item.instance_name,
                instance_id=redis_item.instance_id
            ))
    logger.info("需要被检查实例有：")
    for redis_item in BaseConfig.CHECK_REDIS_SERVERS:
        logger.info("实例{instance_name}({instance_id})--{instance_host}:{instance_port}".format(
            instance_name=redis_item.instance_name,
            instance_id=redis_item.instance_id,
            instance_host=redis_item.instance_host,
            instance_port=redis_item.instance_port
        ))


def load_redis_rdb():
    for redis_server in BaseConfig.CHECK_REDIS_SERVERS:
        redis_backups = AliRedisHelper.get_redis_last_backups(
            region_id=redis_server.region_id,
            instance_id=redis_server.instance_id,
            last_backup_days=5
        )
        logger.info("对实例{}({})找到{}个备份文件".format(
            redis_server.instance_name,
            redis_server.instance_id,
            len(redis_backups)
        ))
        for redis_backup in redis_backups:
            if BaseConfig.RDB_DOWNLOAD_METHOD == "INTRANET":
                download_url = redis_backup["BackupIntranetDownloadURL"]
            else:
                download_url = redis_backup["BackupDownloadURL"]
            rdb_folder = os.path.join(os.path.dirname(__file__), "rdb_files")
            if not os.path.exists(rdb_folder):
                os.mkdir(rdb_folder)
            download_path = os.path.join(rdb_folder, redis_backup["NodeInstanceId"] + ".rbd")
            redis_backup["download_path"] = download_path
            AliHelper.download_rdb_file(download_url=download_url, download_path=download_path)
        for redis_backup in redis_backups:
            backup_time = AliHelper.convert_from_ali_time(redis_backup["BackupStartTime"])
            if BaseConfig.MIN_REDIS_KEY_LENGTH == 0:
                mysql_table_name = str(redis_server.instance_name).replace("-", "_").strip().lower() \
                                   + "_full_" + backup_time.strftime("%Y%m%d")
            else:
                mysql_table_name = str(redis_server.instance_name).replace("-", "_").strip().lower() \
                                   + "_" + backup_time.strftime("%Y%m%d")
            rdb_file_path = redis_backup["download_path"]
            logger.info("准备解析备份文件{}".format(rdb_file_path))
            MyRedisParser.parse_rdb_to_mysql(
                rdb_file_path=rdb_file_path,
                mysql_server=BaseConfig.TARGET_MYSQL_SERVER,
                mysql_table_name=mysql_table_name,
                min_key_length=BaseConfig.MIN_REDIS_KEY_LENGTH,
                mysql_batch_insert_rows=BaseConfig.MYSQL_BATCH_INSERT_ROWS,
                mysql_batch_sleep_seconds=BaseConfig.MYSQL_BATCH_SLEEP_SECONDS
            )


def create_redis_info_table():
    sql_script = """
CREATE TABLE IF NOT EXISTS `redis_instances` (
  `instance_id` VARCHAR (24) NOT NULL,
  `instance_name` VARCHAR (128) NOT NULL DEFAULT '',
  `region_id` VARCHAR (32) NOT NULL DEFAULT '',
  `instance_host` VARCHAR (128) NOT NULL DEFAULT '',
  `instance_port` INT (11) NOT NULL DEFAULT '5719',
  `instance_class` VARCHAR (128) NOT NULL DEFAULT '',
  `rdb_table_prefix` VARCHAR (128) NOT NULL DEFAULT '',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`instance_id`)
) ENGINE = INNODB DEFAULT CHARSET = utf8mb4;
"""
    target_server = BaseConfig.TARGET_MYSQL_SERVER
    target_server.mysql_exec(
        sql_script=sql_script
    )


def update_redis_info():
    sql_script = """
REPLACE INTO `redis_instances`
(
  `instance_id`,
  `instance_name`,
  `region_id`,
  `instance_host`,
  `instance_port`,
  `instance_class`,
  `rdb_table_prefix`
)
VALUES(%s,%s,%s,%s,%s,%s,%s);
"""
    # target_server = MySQLServer()
    target_server = BaseConfig.TARGET_MYSQL_SERVER
    for redis_item in BaseConfig.CHECK_REDIS_SERVERS:
        # redis_item = RedisServer()
        target_server.mysql_exec(
            sql_script=sql_script,
            sql_paras=[
                redis_item.instance_id, redis_item.instance_name,
                redis_item.region_id, redis_item.instance_host,
                redis_item.instance_port, redis_item.instance_class,
                str(redis_item.instance_name).replace("-", "_").strip().lower()
            ]
        )


def main(command_args):
    try:
        LogHelper.init_logger(log_file_prefix="ali_redis_loader")
        args = parse_args(command_args)
        load_configs(config_file=args.config_file)
        create_redis_info_table()
        update_redis_info()
        load_redis_rdb()
    except Exception as ex:
        logger.warning(str(ex))
        logger.warning(traceback.format_exc())


if __name__ == '__main__':
    main(sys.argv[1:])
