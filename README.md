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

```
-b juguo.test_base
-d juguo.test_delta
-o juguo.test_upsert
-k id
-l
-n SparkBucketJoinUpsert
-p hive.metastore.uris=thrift://hadoop1-6421.slc01.dev.ebayc3.com:9083
-e fs.defaultFS=hdfs://hadoop1-6421.slc01.dev.ebayc3.com:8020
```

# 使用场景
## 名词解释
 - 事实表（base table），也即存量表（全量表），保存历史的全量信息
 - 增量表（delta table），最新一批次的更新数据，可能需要对部分数据作更新（如订单状态有变化，会新插入一条最新状态的数据，而非直接修改已有数据，该条新数据即存于增量表中），也有可能有新的数据需要插入（如增加新订单）
 - 输出表（output table），保存base table与delta table的合并结果，并作为下一次合并时的base table  
 
## 适用条件
 - 必须都使用了bucket，且bucket key必须一致
 - 同时两张表的bucket数必须一致
 - 必须根据bucket key排序，也即ditributed by (key1, key2) sort by (key1, key2) 
 - 不支持TIMESTAMP / int96
  
## 典型业务场景
 - 在数据仓库中，需要处理缓慢变化维（Slowly Changing Dimension）问题，而部分SCD策略要求更新已有数据，但常见大数据系统不支持直接更新数据（一般是Append Only），因此可能需要将已有数据与增量数据作合并，实现Upsert。本项目即可适用该业务场景
 - 典型的数据仓库中，事实表（fact table）一般为Append Only的，但有些业务场景下也可能需要做少量的更新，实现Upsert，本项目可适用于该场景
 
 
# 测试用例

```sql
CREATE TABLE juguo.test_base
(
id INT,
f2 TINYINT,
f3 SMALLINT,
f4 BIGINT,
f5 FLOAT,
f6 DOUBLE,
f8 DECIMAL,
f1 DECIMAL(9, 7),
f9 TIMESTAMP,
f10 DATE,
f12 STRING,
f13 VARCHAR(10),
f14 CHAR(10),
f16 BOOLEAN,
f17 BINARY
)
CLUSTERED BY(id) INTO 3 BUCKETS
STORED AS PARQUET;

insert into test_base values
(1, 1, 1, 1, 1,1,1, 1, '2017-01-23 00:11:22', '2017-01-23', '1', '1', '1', true, '1');

insert into test_base (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f13, f14, f16, f17) values
(2, 2, 2, 2, 2, 2, 2, 2, '2017-01-23 00:11:22', '2017-01-23', '2', '2', '2', true, '2');

insert into test_base (id, f3, f4, f5, f6, f8, f1, f9, f10, f12, f13, f14, f16, f17) values
(3, 3, 3, 3, 3, 3, 3, '2017-01-23 00:11:22', '2017-01-23', '3', '3', '3', true, '3');
insert into test_base (id, f2, f3, f4, f6, f8, f1, f9, f10, f12, f13, f14, f16, f17) values
(4, 4, 4, 4, 4, 4, 4, '2017-01-23 00:11:22', '2017-01-23', '4', '4', '4', true, '4');
insert into test_base (id, f2, f3, f4, f5, f6, f1, f9, f10, f12, f13, f14, f16, f17) values
(5, 5, 5, 5, 5, 5, 5, '2017-01-23 00:11:22', '2017-01-23', '5', '5', '5', true, '5');
insert into test_base (id, f2, f3, f4, f5, f6, f8, f1, f10, f12, f13, f14, f16, f17) values
(6, 6, 6, 6, 6, 6, 6, 6, '2017-01-23', '6', '6', '6', true, '6');
insert into test_base (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f13, f14, f16, f17) values
(7, 7, 7, 7, 7, 7, 7, 7, '2017-01-23 00:11:22', '2017-01-23', '7', '7', true, '7');
insert into test_base (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f14, f16, f17) values
(8, 8, 8, 8, 8, 8, 8, 8, '2017-01-23 00:11:22', '2017-01-23', '8', '8', true, '8');
insert into test_base (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f13, f16, f17) values
(9, 9, 9, 9, 9, 9, 9, 9, '2017-01-23 00:11:22', '2017-01-23', '9', '9', true, '9');
insert into test_base (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f13, f14, f17) values
(10, 10, 10, 10, 10, 10, 10, 10, '2017-01-23 00:11:22', '2017-01-23', '10', '10', '10', '10');
insert into test_base (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f13, f14, f16) values
(11, 11, 11, 11, 11, 11, 11, 11, '2017-01-23 00:11:22', '2017-01-23', '11', '11', '11', true);




CREATE TABLE juguo.test_delta
(
id INT,
f2 TINYINT,
f3 SMALLINT,
f4 BIGINT,
f5 FLOAT,
f6 DOUBLE,
f8 DECIMAL,
f1 DECIMAL(9, 7),
f9 TIMESTAMP,
f10 DATE,
f12 STRING,
f13 VARCHAR(10),
f14 CHAR(10),
f16 BOOLEAN,
f17 BINARY
)
CLUSTERED BY(id) INTO 3 BUCKETS
STORED AS PARQUET;

insert into test_delta (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f13, f14, f16, f17) values
(2, 2, 2, 2, 2, 2, 2, 2, '2017-01-23 00:11:22', '2017-01-23', '2', '2', '2', true, '2');

insert into test_delta (id, f2, f3, f4, f5, f6, f8, f1, f9, f12, f13, f14, f16, f17) values
(6, 16, 16, 16, 16, 16, 16, 6, '2017-11-23 11:22:33', '16', '16', '16', true, '16');
insert into test_delta (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f14, f16, f17) values
(7, 17, 17, 17, 17, 17, 17, 17, '2017-11-23 00:11:22', '2117-01-23', '17', '17', false, '17');
insert into test_delta (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f13, f16, f17) values
(8, 18, 18, 18, 18, 18, 18, 18, '2017-11-23 00:11:22', '2017-11-23', '18', '18', false, '18');
insert into test_delta (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f13, f14, f17) values
(9, 19, 19, 19, 19, 19, 19, 19, '2017-11-23 00:11:22', '2017-11-23', '19', '19', 'false', '19');
insert into test_delta (id, f2, f3, f4, f5, f6, f8, f1, f9, f10, f12, f13, f14, f16) values
(10, 20, 20, 20, 20, 20, 20, 20, '2017-11-23 00:11:22', '2017-11-23', '20', '20', '20', false);
insert into test_delta (id, f2, f3, f5, f6, f8, f1, f9, f10, f12, f13, f14, f16, f17) values
(11, 21, 21, 21, 21, 21, 21, '2017-11-23 00:11:22', '2017-11-23', '21', '21', '21', true, '21');
insert into test_delta (id, f2, f3, f4, f5, f6, f8, f1, f10, f12, f13, f14, f16, f17) values
(12, 22, 22, 22, 22, 22, 22, 22, '2017-01-23', '22', '22', '22', true, '22');
insert into test_delta (id, f2, f3, f4, f5, f6, f8, f1, f9, f12, f13, f14, f16, f17) values
(13, 23, 23, 23, 23, 23, 23, 23, '2017-01-23 00:11:22', '23', '23', '23', true, '23');

CREATE TABLE juguo.test_upsert
(
id INT,
f2 TINYINT,
f3 SMALLINT,
f4 BIGINT,
f5 FLOAT,
f6 DOUBLE,
f8 DECIMAL,
f1 DECIMAL(9, 7),
f9 TIMESTAMP,
f10 DATE,
f12 STRING,
f13 VARCHAR(10),
f14 CHAR(10),
f16 BOOLEAN,
f17 BINARY
)
CLUSTERED BY(id) INTO 3 BUCKETS
STORED AS PARQUET;
```