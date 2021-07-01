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
import datetime
import json

from aliyunsdkr_kvstore.request.v20150101.DescribeInstancesRequest import DescribeInstancesRequest
from aliyunsdkr_kvstore.request.v20150101.DescribeLogicInstanceTopologyRequest import \
    DescribeLogicInstanceTopologyRequest
from aliyunsdkr_kvstore.request.v20150101.DescribeBackupsRequest import DescribeBackupsRequest

from aliyun_utils.ali_helper import AliHelper
from aliyun_utils.ali_config import AliConfig
from aliyun_redis.ali_redis import AliRedis
from aliyun_redis.ali_redis import AliRedisNode
from common_utils.logger_helper import LogHelper

logger = LogHelper.get_logger()


class AliRedisHelper(object):
    @classmethod
    def get_instance_list_by_region(cls, region_id):
        instance_list = []
        instance_count = cls.get_instance_count_by_region(region_id)
        if instance_count == 0:
            return []
        page_size = 100
        max_page_index = int(instance_count / page_size) + 1
        for page_index in range(1, max_page_index + 1):
            client = AliHelper.get_acs_client(region_id)
            request = DescribeInstancesRequest()
            request.set_accept_format('json')
            request.set_PageNumber(page_index)
            request.set_PageSize(page_size)
            response = client.do_action_with_exception(request)
            json_data = json.loads(str(response, encoding='utf-8'))
            tmp_instance_list = json_data["Instances"]["KVStoreInstance"]
            instance_list.extend(tmp_instance_list)
        return instance_list

    @classmethod
    def get_instance_count_by_region(cls, region_id):
        client = AliHelper.get_acs_client(region_id)
        request = DescribeInstancesRequest()
        request.set_PageNumber(1)
        request.set_PageSize(1)
        request.set_accept_format('json')
        response = client.do_action_with_exception(request)
        json_data = json.loads(str(response, encoding='utf-8'))
        if "TotalCount" in json_data.keys():
            return int(json_data["TotalCount"])
        else:
            logger.warning("获取Redis实例数量失败")
            return 0

    @classmethod
    def get_instance_list(cls):
        instance_list = []
        for region_id in AliConfig.region_ids:
            tmp_instance_list = cls.get_instance_list_by_region(region_id)
            for tmp_instance_json in tmp_instance_list:
                redis_instance = AliRedis(tmp_instance_json)
                instance_list.append(redis_instance)
        return instance_list

    @classmethod
    def get_instance_nodes(cls, region_id, instance_id):
        client = AliHelper.get_acs_client(region_id)
        request = DescribeLogicInstanceTopologyRequest()
        request.set_accept_format('json')
        request.set_InstanceId(instance_id)
        response = client.do_action_with_exception(request)
        json_data = json.loads(str(response, encoding='utf-8'))
        nodes = list()
        if "RedisShardList" in json_data.keys():
            for node_json in json_data["RedisShardList"]["NodeInfo"]:
                nodes.append(AliRedisNode(redis_node_json=node_json))
        return nodes

    @classmethod
    def get_redis_backups(cls, region_id, instance_id, last_backup_days):
        client = AliHelper.get_acs_client(region_id=region_id)
        request = DescribeBackupsRequest()
        request.set_accept_format('json')
        start_time = datetime.datetime.now() - datetime.timedelta(days=last_backup_days)
        end_time = datetime.datetime.now()
        request.set_StartTime(AliHelper.convert_to_ali_time(start_time))
        request.set_EndTime(AliHelper.convert_to_ali_time(end_time))
        request.set_InstanceId(instance_id)
        response = client.do_action_with_exception(request)
        return json.loads(response)["Backups"]["Backup"]

    @classmethod
    def get_redis_last_backups(cls, region_id, instance_id, last_backup_days):
        last_backups = []
        redis_backups = cls.get_redis_backups(
            region_id=region_id,
            instance_id=instance_id,
            last_backup_days=last_backup_days
        )
        success_backups = list(filter(lambda item: item["BackupStatus"] == "Success", redis_backups))
        node_ids = set(map(lambda item: item["NodeInstanceId"], success_backups))
        for node_id in node_ids:
            last_backup = max(
                list(filter(lambda item: item["NodeInstanceId"] == node_id, success_backups)),
                key=lambda item: AliHelper.convert_from_ali_time(item["BackupStartTime"])
            )
            last_backups.append(last_backup)
        return last_backups
