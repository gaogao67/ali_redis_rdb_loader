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
import requests
from aliyunsdkcore.client import AcsClient
from aliyun_utils.ali_config import AliConfig
from common_utils.logger_helper import LogHelper

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
    def get_acs_client(cls, region_id=None):
        if region_id is None:
            region_id = AliConfig.default_region_id
        return AcsClient(AliConfig.access_key_id, AliConfig.access_key_secret, region_id)

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

