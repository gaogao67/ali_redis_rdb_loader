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
import datetime
import logging
import argparse
import logging.config
from utils.mysql_server import MySQLServer

from utils.log_helper import LogHelper
from utils.ali_helper import AliHelper
from utils.redis_parser import MyRedisParser

logger = LogHelper.get_logger()


class BaseConfig(object):
    ACCESS_KEY_ID = ""
    ACCESS_KEY_SECRET = ""
    RDB_DOWNLOAD_METHOD = ""
    RDB_BACKUP_DAYS = 5
    TARGET_MYSQL_SERVER = None
    MYSQL_BATCH_INSERT_ROWS = 100
    MYSQL_BATCH_SLEEP_SECONDS = 0.1
    CHECK_REDIS_SERVERS = []
    MIN_REDIS_KEY_LENGTH = 0


class RedisServer(object):
    def __init__(self, instance_id, instance_name, region_id):
        self.instance_id = instance_id
        self.instance_name = instance_name
        self.region_id = region_id


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
    with open(config_file, 'r') as file_handler:
        dict_configs = json.load(file_handler)

    BaseConfig.ACCESS_KEY_ID = dict_configs["common_configs"]["access_key_id"]
    BaseConfig.ACCESS_KEY_SECRET = dict_configs["common_configs"]["access_key_secret"]
    BaseConfig.RDB_DOWNLOAD_METHOD = dict_configs["common_configs"].get("rdb_download_method", "INTRANET")
    BaseConfig.MYSQL_BATCH_INSERT_ROWS = dict_configs["common_configs"].get("mysql_batch_insert_rows", 100)
    BaseConfig.MYSQL_BATCH_SLEEP_SECONDS = dict_configs["common_configs"].get("mysql_batch_sleep_seconds", 0.01)
    BaseConfig.MIN_REDIS_KEY_LENGTH = dict_configs["common_configs"].get("min_redis_key_length", 0)
    BaseConfig.RDB_BACKUP_DAYS = dict_configs["common_configs"].get("rdb_backup_days", 5)
    mysql_configs = dict_configs["mysql_configs"]
    BaseConfig.TARGET_MYSQL_SERVER = MySQLServer(
        mysql_host=mysql_configs.get("host_name"),
        mysql_port=mysql_configs.get("host_port", 3306),
        mysql_user=mysql_configs.get("user_name"),
        mysql_password=mysql_configs.get("user_password"),
        mysql_charset=mysql_configs.get("mysql_charset", "utf8mb4"),
        database_name=mysql_configs.get("database_name"),
        connect_timeout=mysql_configs.get("connect_timeout", 60),
    )
    for redis_server in dict_configs["redis_servers"]:
        BaseConfig.CHECK_REDIS_SERVERS.append(
            RedisServer(
                region_id=redis_server["region_id"],
                instance_id=redis_server["instance_id"],
                instance_name=redis_server["instance_name"]
            )
        )


def main(command_args):
    try:
        LogHelper.init_logger(log_file_prefix="ali_redis_loader")
        rdb_folder = os.path.join(os.path.dirname(__file__), "rdb_files")
        if not os.path.exists(rdb_folder):
            os.mkdir(rdb_folder)
        args = parse_args(command_args)
        load_configs(config_file=args.config_file)
        for redis_server in BaseConfig.CHECK_REDIS_SERVERS:
            # redis_server = RedisServer()
            redis_backups = AliHelper.get_redis_last_backups(
                access_key_id=BaseConfig.ACCESS_KEY_ID,
                access_key_secret=BaseConfig.ACCESS_KEY_SECRET,
                region_id=redis_server.region_id,
                instance_id=redis_server.instance_id,
                last_backup_days=BaseConfig.RDB_BACKUP_DAYS
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
                download_path = os.path.join(rdb_folder, redis_backup["NodeInstanceId"] + ".rbd")
                redis_backup["download_path"] = download_path
                AliHelper.download_rdb_file(download_url=download_url, download_path=download_path)
            for redis_backup in redis_backups:
                backup_time = AliHelper.convert_from_ali_time(redis_backup["BackupStartTime"])
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
    except Exception as ex:
        logger.warning(str(ex))
        logger.warning(traceback.format_exc())


if __name__ == '__main__':
    main(sys.argv[1:])
