SparkBucketJoin

----

# 项目说明
Bucket Join是Hadoop/Spark中非常重要的优化Join性能的方法。为了提升特定场景下Spark Join的性能，本项目使用人工读取Bucket信息并手工Join的方式，提升了Join性能

# 参数说明
usage: SparkBucketJoin  
 -a,--core-site <core-site.xml>         Hadoop configuration file. eg.
                                        core-site.xml  
 -b,--base-table <database.table>       Base table name, including
                                        database name  
 -d,--delta-table <database.table>      Incremental table name, including
                                        database name  
 -h,--help                              Help  
 -k,--join-key <join key>               Join key  
 -l,--local-mode                        Local mode  
 -n,--spark-app-name <spark app name>   Spark Application Name  
 -o,--output-table <database.table>     Output table name, including
                                        database name  
 -p,--properties <key=value>            Hive properties  
 -s,--hive-site <hive-site.xml>         Hive configuration file. eg.
                                        hive-site.xmls

# 使用场景
## 名词解释
 - 事实表（base table），也即存量表（全量表），保存历史的全量信息
 - 增量表（delta table），最新一批次的更新数据，可能需要对部分数据作更新（如订单状态有变化，会新插入一条最新状态的数据，而非直接修改已有数据，该条新数据即存于增量表中），也有可能有新的数据需要插入（如增加新订单）
 - 输出表（output table），保存base table与delta table的合并结果，并作为下一次合并时的base table  
 
## 适用条件
 - 必须都使用了bucket，且bucket key必须一致
 - 同时两张表的bucket数必须一致
 - 必须根据bucket key排序，也即ditributed by (key1, key2) sort by (key1, key2)  
  
## 典型业务场景
 - 在数据仓库中，需要处理缓慢变化维（Slowly Changing Dimension）问题，而部分SCD策略要求更新已有数据，但常见大数据系统不支持直接更新数据（一般是Append Only），因此可能需要将已有数据与增量数据作合并，实现Upsert。本项目即可适用该业务场景
 - 典型的数据仓库中，事实表（fact table）一般为Append Only的，但有些业务场景下也可能需要做少量的更新，实现Upsert，本项目可适用于该场景