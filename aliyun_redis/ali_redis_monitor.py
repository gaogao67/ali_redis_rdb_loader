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
from aliyunsdkr_kvstore.request.v20150101.DescribeHistoryMonitorValuesRequest import DescribeHistoryMonitorValuesRequest

from aliyun_utils.ali_helper import AliHelper


class AliRedisMonitor(object):
    @classmethod
    def get_monitor_values(cls, region_id, instance_id, start_time, stop_time, monitor_keys, node_id=None):
        client = AliHelper.get_acs_client(region_id=region_id)
        request = DescribeHistoryMonitorValuesRequest()
        request.set_accept_format('json')
        ali_start_time = AliHelper.convert_to_ali_time2(start_time)
        ali_stop_time = AliHelper.convert_to_ali_time2(stop_time)
        request.set_StartTime(ali_start_time)
        request.set_EndTime(ali_stop_time)
        request.set_IntervalForHistory("01m")
        request.set_InstanceId(instance_id)
        request.set_MonitorKeys(monitor_keys)
        if node_id is not None:
            request.set_NodeId(node_id)
        response = client.do_action_with_exception(request)
        json_data = json.loads(str(response, encoding='utf-8'))
        return json_data

    @classmethod
    def get_max_value(cls, monitor_json, monitor_key):
        max_item_value = None
        max_item_time = None
        for tmp_key, tmp_values in json.loads(monitor_json["MonitorHistory"]).items():
            for monitor_key2 in tmp_values.keys():
                if str(monitor_key2).lower() != str(monitor_key).lower():
                    continue
                tmp_value = float(tmp_values[monitor_key2])
                if max_item_value is None or tmp_value > max_item_value:
                    max_item_value = tmp_value
                    max_item_time = tmp_key
        if max_item_time is not None:
            max_item_time = AliHelper.convert_ali_time_to_string_time(max_item_time)
        return max_item_time, max_item_value

    @classmethod
    def get_min_value(cls, monitor_json, monitor_key):
        min_item_value = None
        min_item_time = None
        for tmp_key, tmp_values in json.loads(monitor_json["MonitorHistory"]).items():
            if monitor_key in tmp_values.keys():
                tmp_value = float(tmp_values[monitor_key])
                if min_item_value is None or tmp_value < min_item_value:
                    min_item_value = tmp_value
                    min_item_time = tmp_key
        return min_item_time, min_item_value
