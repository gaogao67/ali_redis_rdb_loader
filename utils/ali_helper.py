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
import os
import time
import datetime
import json
import requests
from aliyunsdkcore.client import AcsClient
from aliyunsdkr_kvstore.request.v20150101.DescribeBackupsRequest import DescribeBackupsRequest
from .log_helper import LogHelper

logger = LogHelper.get_logger()


class AliHelper(object):
    @classmethod
    def convert_from_ali_time(cls, str_time):
        if 'Z' in str_time:
            str_time = str_time.split('Z')[0]
        if 'T' in str_time:
            Ymd = str_time.split('T')[0]
            HMS = str_time.split('T')[1].split('Z')[0]
            str_time = '%s %s' % (Ymd, HMS)
            utc_time = datetime.datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S")
            dt_time = utc_time + datetime.timedelta(hours=8)
        else:
            dt_time = datetime.datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S")
        return dt_time

    @classmethod
    def convert_to_ali_time(cls, dt_time):
        utc_time = dt_time - datetime.timedelta(hours=8)
        return utc_time.strftime('%Y-%m-%dT%H:%MZ')

    @classmethod
    def convert_to_ali_time2(cls, dt_time):
        utc_time = dt_time - datetime.timedelta(hours=8)
        return utc_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    @classmethod
    def convert_ali_time_to_string_time(cls, str_time):
        return AliHelper.convert_from_ali_time(str_time).strftime("%Y-%m-%d %H:%M:%S")

    @classmethod
    def get_redis_backups(cls, access_key_id, access_key_secret, region_id, instance_id, last_backup_days):
        client = AcsClient(access_key_id, access_key_secret, region_id)
        request = DescribeBackupsRequest()
        request.set_accept_format('json')
        start_time = datetime.datetime.now() - datetime.timedelta(days=last_backup_days)
        end_time = datetime.datetime.now()
        request.set_StartTime(cls.convert_to_ali_time(start_time))
        request.set_EndTime(cls.convert_to_ali_time(end_time))
        request.set_InstanceId(instance_id)
        response = client.do_action_with_exception(request)
        return json.loads(response)["Backups"]["Backup"]

    @classmethod
    def get_redis_last_backups(cls, access_key_id, access_key_secret, region_id, instance_id, last_backup_days):
        last_backups = []
        redis_backups = cls.get_redis_backups(
            access_key_id, access_key_secret, region_id, instance_id, last_backup_days
        )
        success_backups = list(filter(lambda item: item["BackupStatus"] == "Success", redis_backups))
        node_ids = set(map(lambda item: item["NodeInstanceId"], success_backups))
        for node_id in node_ids:
            last_backup = max(
                list(filter(lambda item: item["NodeInstanceId"] == node_id, success_backups)),
                key=lambda item: cls.convert_from_ali_time(item["BackupStartTime"])
            )
            last_backups.append(last_backup)
        return last_backups

    @classmethod
    def download_rdb_file(cls, download_url, download_path):
        logger.info("使用{}地址下载文件至{}".format(download_url, download_path))
        if os.path.exists(download_path):
            os.remove(download_path)
        startTime = time.time()
        with requests.get(download_url, stream=True) as r:
            contentLength = int(r.headers['content-length'])
            download_info = 'content-length: %dB/ %.2fKB/ %.2fMB'
            download_info = download_info % (contentLength, contentLength / 1024, contentLength / 1024 / 1024)
            logger.info(download_info)
            downSize = 0
            with open(download_path, 'wb') as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
                    downSize += len(chunk)
                    download_info = '%d KB/s - %.2f MB， 共 %.2f MB'
                    download_info = download_info % (
                        downSize / 1024 / (time.time() - startTime), downSize / 1024 / 1024,
                        contentLength / 1024 / 1024)
                    logger.info(download_info)
                    if downSize >= contentLength:
                        break
            timeCost = time.time() - startTime
            download_info = '共耗时: %.2f s, 平均速度: %.2f KB/s'
            download_info = download_info % (timeCost, downSize / 1024 / timeCost)
            logger.info(download_info)
