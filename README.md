
## 使用帮助
```JS
python3 rdb_loader.py --config_file="./configs/demo.json" 
```

## 实现逻辑
```JS
按照配置文件中的Redis实例列表去拉取最新的备份文件至本地,再通过RDB TOOL将RDB文件解析并导入到指定的MySQL中。
表名规则：str("Redis实例名").replace("-", "_").strip().lower()+"_"+"Redis备份时间".strftime("%Y%m%d")
```

## 配置文件说明
```JS
{
  "common_configs": {
    "_access_key_id": "阿里云KEY",
    "access_key_id": "xxxxx",
    "_access_key_secret": "阿里云密钥",
    "access_key_secret": "xxxxxx",
    "_rdb_backup_days": "查找最近N天的备份记录",
    "rdb_backup_days": 7,
    "_rdb_download_method": "默认值INTRANET,可选值：INTERNET、INTRANET,INTRANET表示内网下载，INTERNET表示公网下载",
    "rdb_download_method": "INTRANET",
    "_mysql_batch_insert_rows": "每次向数据库批量插入的记录数,默认值100,建议将该值控制在1000以内",
    "mysql_batch_insert_rows": 100,
    "_mysql_batch_sleep_seconds": "每次向数据库批量插入后的休眠时间,默认0.01,设置为0则表示无休眠时间",
    "mysql_batch_sleep_seconds": 0.01,
    "_min_redis_key_length": "Redis Key的最小占用内存数，单位为byte,小于指定值则不会导入到数据库中,设置为0则表示导入所有记录",
    "min_redis_key_length": 0
  },
  "mysql_configs": {
    "_host_name": "数据库实例地址",
    "host_name": "xxx.xxx.xxx.xxx",
    "_host_port": "数据库端口",
    "host_port": 3306,
    "_user_name": "数据库账号",
    "user_name": "xxxxxxx",
    "_user_password": "数据库密码",
    "user_password": "xxxxxx",
    "_database_name": "数据库库名",
    "database_name": "redis_his",
    "_mysql_charset": "数据库字符集",
    "mysql_charset": "utf8mb4",
    "_connect_timeout": "数据均连接超时时间",
    "connect_timeout": 60
  },
  "_redis_servers": "要导入的Redis实例列表",
  "redis_servers": [
    {
      "_instance_name": "实例名称,必填,将作为表名的前缀,和备份开始时间一起组成表名",
      "instance_name": "test-redis",
      "_instance_id": "阿里云实例编号,必填",
      "instance_id": "r-xxxxx",
      "_region_id": "实例坐在的可用区,必填",
      "region_id": "cn-beijing"
    }
  ]
}
```
