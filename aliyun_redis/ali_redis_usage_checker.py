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

from aliyun_redis.ali_redis import AliRedis
from aliyun_redis.ali_redis import AliRedisNode
from aliyun_utils.ali_helper import AliHelper
from aliyun_redis.ali_redis_monitor import AliRedisMonitor
from aliyun_redis.ali_redis_helper import AliRedisHelper
from common_utils.logger_helper import LogHelper

logger = LogHelper.get_logger()


class AliRedisUsageChecker(object):
    @staticmethod
    def check_redis_node_usage(redis_item: AliRedis, node_id, start_time, stop_time):
        check_messages = list()
        json_data = AliRedisMonitor.get_monitor_values(
            region_id=redis_item.region_id,
            instance_id=redis_item.instance_id,
            start_time=start_time,
            stop_time=stop_time,
            monitor_keys="CpuUsage,memoryUsage,intranetInRatio,intranetOutRatio,qpsUsage,connectionUsage",
            node_id=node_id
        )
        max_item_time, max_item_value = AliRedisMonitor.get_max_value(json_data, "CpuUsage")
        if node_id is not None:
            redis_name = redis_item.instance_name + '--' + node_id
        else:
            redis_name = redis_item.instance_name
        if max_item_value is None:
            check_messages.append("未获取到实例{}的CPU使用率".format(redis_name))
        elif max_item_value > 20:
            check_messages.append("实例{}的CPU使用率在{}时达到最大值{}".format(redis_name, max_item_time, max_item_value))
        max_item_time, max_item_value = AliRedisMonitor.get_max_value(json_data, "memoryUsage")
        if max_item_value is None:
            check_messages.append("未获取到实例{}的内存使用率".format(redis_name))
        elif max_item_value > 50:
            check_messages.append("实例{}的内存使用率在{}时达到最大值{}".format(redis_name, max_item_time, max_item_value))
        max_item_time, max_item_value = AliRedisMonitor.get_max_value(json_data, "IntranetOutRatio")
        if max_item_value is None:
            check_messages.append("未获取到实例{}的流出使用率".format(redis_name))
        elif max_item_value > 30:
            check_messages.append("实例{}的流出使用率在{}时达到最大值{}".format(redis_name, max_item_time, max_item_value))
        max_item_time, max_item_value = AliRedisMonitor.get_max_value(json_data, "IntranetInRatio")
        if max_item_value is None:
            check_messages.append("未获取到实例{}的流入使用率".format(redis_name))
        elif max_item_value > 30:
            check_messages.append("实例{}的流入使用率在{}时达到最大值{}".format(redis_name, max_item_time, max_item_value))
        max_item_time, max_item_value = AliRedisMonitor.get_max_value(json_data, "TrafficControlReadTime")
        min_item_time, min_item_value = AliRedisMonitor.get_min_value(json_data, "TrafficControlReadTime")
        if max_item_value is not None:
            if min_item_value is None:
                min_item_value = 0
            diff_value = max_item_value - min_item_value
            if diff_value > 0:
                check_messages.append("实例{}在此段时间内触发流量带宽限制{}ms".format(
                    redis_name,
                    diff_value
                ))
        return check_messages

    @staticmethod
    def check_redis_usage(redis_item: AliRedis, start_time, stop_time):
        logger.info("开始检查实例{}:{}".format(redis_item.instance_name, redis_item.redis_host))
        check_messages = AliRedisUsageChecker.check_redis_node_usage(
            redis_item=redis_item,
            node_id=None,
            start_time=start_time,
            stop_time=stop_time
        )
        if redis_item.arch_type == "SplitRW" or redis_item.arch_type == "cluster":
            redis_nodes = AliRedisHelper.get_instance_nodes(
                region_id=redis_item.region_id,
                instance_id=redis_item.instance_id
            )
            for check_node in redis_nodes:
                check_messages = check_messages + AliRedisUsageChecker.check_redis_node_usage(
                    redis_item=redis_item,
                    node_id=check_node.node_id,
                    start_time=start_time,
                    stop_time=stop_time
                )
        return check_messages
