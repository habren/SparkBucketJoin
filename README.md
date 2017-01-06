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
事实表（base table）和增量表（delta data）必须都使用了bucket，且bucket key必须一致，同时必须根据bucket key排序，也即ditributed by (key1, key2) sort by (key1, key2)。同时两张表的bucket数必须一致。