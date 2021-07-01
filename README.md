## 功能描述
将阿里云上特定Redis实例最近一次的RDB备份复制到本地，解析RDS文件并将KEY信息录入到指定的MySQL中方便查询分析

## 使用步骤
- 按照配置说明进行配置
- 基于配置文件运行程序
```shell
python redis_rds_loader.py --config_file="./configs/demo.json"
```

## 实现逻辑
```JS
按照配置文件中的Redis实例列表去拉取最新的备份文件至本地,再通过RDB TOOL将RDB文件解析并导入到指定的MySQL中。
表名规则：str("Redis实例名").replace("-", "_").strip().lower()+"_"+"Redis备份时间".strftime("%Y%m%d")
```


## 配置文件
```json
{
  "common_configs": {
    "_access_key_id": "阿里云access key",
    "access_key_id": "##access_key_id",
    "_access_key_secret": "阿里云access key secret",
    "access_key_secret": "##access_key_secret",
    "_region_ids": "阿里云可用区标识",
    "region_ids": [
      "cn-hangzhou",
      "cn-beijing"
    ],
    "_rdb_backup_days": "阿里云保存RDB文件的时间，用于查找最近N天内的最新备份",
    "rdb_backup_days": 5,
    "_rdb_download_method": "设置网络下载模式,推荐使用INTRANET内网模式",
    "rdb_download_method": "INTRANET",
    "_mysql_batch_insert_rows": "每次向mysql中插入的记录数",
    "mysql_batch_insert_rows": 100,
    "_mysql_batch_sleep_seconds": "每次插入后休眠时间,以秒为单位",
    "mysql_batch_sleep_seconds": 0,
    "_min_redis_key_length": "最小redis键大小,小于该值的键不会录入到MySQL,设置为0时会录入所有redis键",
    "min_redis_key_length": 1000
  },
  "mysql_configs": {
    "_mysql_name": "MySQL实例名称",
    "mysql_name": "mysql01",
    "_mysql_host": "MySQL实例地址",
    "mysql_host": "xxxxx.mysql.polardb.rds.aliyuncs.com",
    "_mysql_port": "MySQL实例端口",
    "mysql_port": 3306,
    "_mysql_user": "MySQL用户账号,需要DDL权限和DML权限",
    "mysql_user": "mysql_admin",
    "_mysql_password": "MySQL账号密码",
    "mysql_password": "mysql_password",
    "_database_name": "数据库名称",
    "database_name": "redis_db",
    "_mysql_charset": "数据库字符集,推荐使用utf8mb4",
    "mysql_charset": "utf8mb4",
    "_connect_timeout": "连接超时时间,默认60秒",
    "connect_timeout": 60
  },
  "_check_server_tags": "需要检测的实例标签",
  "check_server_tags": [
    "测试标签"
  ],
  "_check_servers": "需要检查的实例列表",
  "check_servers": [
    {
      "_instance_id": "阿里云实例ID",
      "instance_id": "r-xxxxxxxxxxxx",
      "_instance_name": "阿里云实例名称,仅用于标识实例",
      "instance_name": "测试实例"
    }
  ],
  "_skip_servers": "不需要检查的实例列表",
  "skip_servers": [
    {
      "_instance_id": "阿里云实例ID",
      "instance_id": "r-xxxxxxxxxxxx",
      "_instance_name": "阿里云实例名称,仅用于标识实例",
      "instance_name": "测试实例"
    }
  ]
}
```