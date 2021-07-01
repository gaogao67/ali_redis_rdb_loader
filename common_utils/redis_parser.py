# -*- coding: utf-8 -*-
# =============================================================================
#     FileName:
#         Desc:
#       Author:
#        Email:
#     HomePage:
#      Version:
#   LastChange:
#      History:
# =============================================================================
import time
import traceback
from common_utils.logger_helper import LogHelper
from common_utils.mysql_server import MySQLServer
from rdbtools import RdbParser, MemoryCallback

logger = LogHelper.get_logger()


class MySQLCallback(object):
    def __init__(self, mysql_server,
                 mysql_table_name,
                 mysql_batch_insert_rows,
                 mysql_batch_sleep_seconds,
                 min_key_length):
        self.mysql_server = mysql_server
        self.min_key_length = min_key_length
        self.mysql_table_name = mysql_table_name
        self.mysql_batch_insert_rows = mysql_batch_insert_rows
        self.mysql_batch_sleep_seconds = mysql_batch_sleep_seconds
        self.cache_keys = []

    def load_data(self):
        if len(self.cache_keys) == 0:
            return
        logger.info("载入{}个key到表{}".format(len(self.cache_keys), self.mysql_table_name))
        sql_header = """ INSERT INTO `{table_name}` (
  `key_db`,
  `key_type`,
  `key_name`,
  `key_size_bytes`,
  `key_encoding`,
  `key_element_count`,
  `len_largest_element`,
  `expire_time`
)""".format(table_name=self.mysql_table_name)
        cur_batch_rows = 0
        sql_paras = []
        sql_script = ""
        for record in self.cache_keys:
            if cur_batch_rows == 0:
                sql_script = sql_header + """VALUES({})""".format("%s,%s,%s,%s,%s,%s,%s,%s")
                cur_batch_rows += 1
            else:
                sql_script += ",({})".format("%s,%s,%s,%s,%s,%s,%s,%s")
            sql_paras.extend([
                record.database, record.type, record.key,
                record.bytes, record.encoding, record.size,
                record.len_largest_element,
                record.expiry.isoformat() if record.expiry else ''
            ])
            cur_batch_rows += 1
        try:
            self.mysql_server.mysql_exec(sql_script=sql_script, sql_paras=sql_paras)
        except Exception as ex:
            logger.info(sql_script)
            logger.info(sql_paras)
            logger.warning(str(ex))
            logger.warning(traceback.format_exc())
        self.cache_keys = []

    def next_record(self, record):
        if record.key is None:
            return
        if record.bytes >= self.min_key_length:
            self.cache_keys.append(record)
        if len(self.cache_keys) == self.mysql_batch_insert_rows:
            self.load_data()
            time.sleep(self.mysql_batch_sleep_seconds)

    def end_rdb(self):
        self.load_data()
        logger.info("载入完成...")

    def init_env(self):
        logger.info("初始化数据表")
        create_script = """
CREATE TABLE IF NOT EXISTS `{table_name}`
(   
    `key_id` BIGINT AUTO_INCREMENT PRIMARY KEY ,
    `key_db` INT,
    `key_type` VARCHAR(128),
    `key_name` VARCHAR(128),
    `key_size_bytes` INT,
    `key_encoding` VARCHAR(128),
    `key_element_count` INT,
    `len_largest_element` VARCHAR(128),
    `expire_time` VARCHAR(200)
);        
""".format(table_name=self.mysql_table_name)
        self.mysql_server.mysql_exec(sql_script=create_script)


class MyRedisParser(object):
    @classmethod
    def parse_rdb_to_mysql(cls, rdb_file_path, mysql_server,
                           mysql_table_name, min_key_length,
                           mysql_batch_insert_rows=100,
                           mysql_batch_sleep_seconds=0.01):
        redis_arch = 64
        string_escape = "raw"
        stream_handler = MySQLCallback(
            mysql_server=mysql_server,
            mysql_table_name=mysql_table_name,
            mysql_batch_insert_rows=mysql_batch_insert_rows,
            mysql_batch_sleep_seconds=mysql_batch_sleep_seconds,
            min_key_length=min_key_length
        )
        stream_handler.init_env()
        parser = RdbParser(
            MemoryCallback(
                stream_handler,
                redis_arch,
                string_escape=string_escape
            )
        )
        parser.parse(rdb_file_path)

