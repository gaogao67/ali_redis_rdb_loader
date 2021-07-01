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
from aliyun_utils.ali_helper import AliHelper


class AliRedis(object):
    def __init__(self, redis_instance_json):
        self.instance_id = redis_instance_json["InstanceId"]
        self.region_id = redis_instance_json["RegionId"]
        self.max_connection = redis_instance_json["Connections"]
        self.create_time = AliHelper.convert_from_ali_time(redis_instance_json["CreateTime"])
        self.instance_port = redis_instance_json["Port"]
        self.instance_host = redis_instance_json["ConnectionDomain"]
        self.memory_mb = redis_instance_json["Capacity"]
        self.max_qps = redis_instance_json["QPS"]
        self.band_width_mbs = redis_instance_json["Bandwidth"]
        self.instance_name = redis_instance_json["InstanceName"]
        self.instance_status = redis_instance_json["InstanceStatus"]
        self.instance_status_desc = self.get_status_desc()
        self.arch_type = redis_instance_json["ArchitectureType"]
        self.arch_type_desc = self.get_arch_type_desc()
        self.charge_type = redis_instance_json["ChargeType"]
        self.charge_type_desc = self.get_charge_type_desc()
        self.instance_class = redis_instance_json["InstanceClass"]
        self.redis_version = redis_instance_json["EngineVersion"]
        self.package_type = redis_instance_json["PackageType"]
        self.package_type_desc = self.get_package_type_desc()
        self.redis_tags = self.get_instance_tag(tags=dict(redis_instance_json["Tags"]))
        self.read_node_count = self.get_read_node_count()
        self.split_node_count = self.get_split_node_count()
        self.instance_class_desc = self.get_instance_class_desc()
        if self.charge_type == "PostPaid":
            self.stop_time = datetime.datetime(year=9999, month=12, day=31)
        elif "EndTime" in redis_instance_json.keys():
            self.stop_time = AliHelper.convert_from_ali_time(redis_instance_json["EndTime"])
        else:
            self.stop_time = None
        self.is_auto_renewal = False
        self.temporary_band_width_mbs = None
        self.temporary_expire_time = None

    def get_status_desc(self):
        if self.instance_status == "Normal":
            return "正常运行"
        elif self.instance_status == "Creating":
            return "创建中"
        elif self.instance_status in (
                "Migrating", "MinorVersionUpgrading", "NetworkModifying", "Migrating", "SSLModifying"):
            return "升级中"
        else:
            return "未知状态"

    def get_instance_tag(self, tags: dict):
        redis_tags = ""
        if "Tag" in tags.keys():
            for tag_item in tags["Tag"]:
                redis_tags += tag_item["Key"] + tag_item["Value"] + ","
        if redis_tags.endswith(","):
            redis_tags = redis_tags[0:-1]
        return redis_tags.replace("业务线", "")

    def get_arch_type_desc(self):
        if self.arch_type == "cluster":
            return "集群版"
        elif self.arch_type == "rwsplit":
            return "读写分离版"
        elif self.arch_type == "standard":
            return "标准版"
        else:
            return "未知类型"

    def get_charge_type_desc(self):
        if self.charge_type == "PrePaid":
            return "包年包月"
        elif self.charge_type == "PostPaid":
            return "按量付费"
        else:
            return "未知类型"

    def get_package_type_desc(self):
        if self.package_type == "standard":
            return "社区版"
        elif self.package_type == "customized":
            return "企业版"
        else:
            return "未知类型"

    def get_read_node_count(self):
        class_items = self.instance_class.split(".")
        for class_item in class_items:
            if class_item.find("rodb") > 0:
                return class_item.replace("rodb", "")
        return 0

    def get_split_node_count(self):
        class_items = self.instance_class.split(".")
        for class_item in class_items:
            find_db = class_item.find("db") > 0
            not_find_rodb = class_item.find("rodb") < 0
            if find_db and not_find_rodb:
                return int(class_item.replace("db", ""))
        return 0

    def get_instance_class_desc(self):
        """
        从instance_class描述找出Redis实例规格
        :param class_name:
        :param memory_size_mb:
        :return:
        """
        class_desc = "{}G".format(self.memory_mb / 1024)
        instance_class = str(self.instance_class)
        if instance_class.find("splitrw") > 0 and instance_class.find("sharding") > 0:
            class_desc += "读写分离集群版({}分片{}只读)".format(self.split_node_count, self.read_node_count)
        elif instance_class.find("splitrw") > 0:
            class_desc += "读写分离版(1主{}只读)".format(self.read_node_count)
        elif instance_class.find("sharding") > 0:
            class_desc += "集群版({}分片)".format(self.split_node_count)
        else:
            class_desc += "标准版"
        return class_desc


class AliRedisNode(object):
    def __init__(self, redis_node_json):
        self.memory_size_mb = redis_node_json["Capacity"]
        self.band_width_mbs = redis_node_json["Bandwidth"]
        self.node_id = redis_node_json["NodeId"]
        self.node_type = redis_node_json["NodeType"]
        self.max_connection = redis_node_json["Connection"]
