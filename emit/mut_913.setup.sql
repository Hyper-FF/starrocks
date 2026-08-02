create table t(k int);
alter table t distributed by random buckets 10;
create table t(k int) distributed by hash(k) buckets 10;
insert into t values(1),(2),(3);
alter table t distributed by hash(k);
CREATE TABLE `duplicate_table_with_null_partition` (
    `k1` date,
    `k2` datetime,
    `k3` char(20),
    `k4` varchar(20),
    `k5` boolean,
    `k6` tinyint,
    `k7` smallint,
    `k8` int,
    `k9` bigint,
    `k10` largeint,
    `k11` float,
    `k12` double,
    `k13` decimal(27,9)
)
DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
COMMENT "OLAP"
PARTITION BY RANGE(`k1`)
(
    PARTITION `p202006` VALUES LESS THAN ("2020-07-01"),
    PARTITION `p202007` VALUES LESS THAN ("2020-08-01"),
    PARTITION `p202008` VALUES LESS THAN ("2020-09-01")
)
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3
PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "v2"
);
alter table duplicate_table_with_null_partition PARTITIONS(p202006,p202007,p202008) DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 4;
create table t(k int) distributed by hash(k) buckets 10;
insert into t values(1),(2),(3);
alter table t distributed by hash(k) buckets 4;
alter table t distributed by random;
create table t(k int, k1 date) PARTITION BY RANGE(`k1`)
(
    PARTITION `p202006` VALUES LESS THAN ("2020-07-01"),
    PARTITION `p202007` VALUES LESS THAN ("2020-08-01"),
    PARTITION `p202008` VALUES LESS THAN ("2020-09-01")
) distributed by hash(k) buckets 10;
insert into t values(1, '2020-06-01'),(2, '2020-07-01'),(3, '2020-08-01');
alter table t distributed by hash(k) buckets 4;
alter table t distributed by hash(k);
alter table t distributed by hash(k1) buckets 4;
alter table t distributed by random;
create table t(k int, k1 date) PARTITION BY RANGE(`k1`)
(
    PARTITION `p202006` VALUES LESS THAN ("2020-07-01"),
    PARTITION `p202007` VALUES LESS THAN ("2020-08-01"),
    PARTITION `p202008` VALUES LESS THAN ("2020-09-01")
) distributed by hash(k) buckets 10;
insert into t values(1, '2020-06-01'),(2, '2020-07-01'),(3, '2020-08-01');
alter table t partitions(p202006,p202008) distributed by hash(k) buckets 4;
alter table t partitions(p202006,p202008) distributed by hash(k1) buckets 4;
alter table t partitions(p202006,p202008) distributed by random;
CREATE TABLE demo2_alter_0 (    
    `user_name` VARCHAR(32) DEFAULT '',
    `city_code` VARCHAR(100),
    `from`  VARCHAR(32) DEFAULT '',
    `pv` BIGINT DEFAULT '0'
)
PRIMARY KEY(`user_name`)
DISTRIBUTED BY HASH(`user_name`) BUCKETS 5
PROPERTIES('replication_num'='1');
ALTER TABLE demo2_alter_0 DISTRIBUTED BY HASH(`user_name`) BUCKETS 10;
create table t(k int, k1 date) PARTITION BY RANGE(`k1`)
(
    PARTITION `p202006` VALUES LESS THAN ("2020-07-01"),
    PARTITION `p202007` VALUES LESS THAN ("2020-08-01"),
    PARTITION `p202008` VALUES LESS THAN ("2020-09-01")
) distributed by hash(k) buckets 10;
insert into t values(1, '2020-06-01'),(1, '2020-07-01'),(1, '2020-08-01');
alter table t distributed by hash(k);
INSERT INTO t VALUES(2, '2020-06-01'),(2, '2020-07-01'),(2, '2020-08-01');
INSERT INTO t VALUES(3, '2020-06-01'),(3, '2020-07-01'),(3, '2020-08-01');
INSERT INTO t VALUES(4, '2020-06-01'),(4, '2020-07-01'),(4, '2020-08-01');
INSERT INTO t VALUES(5, '2020-06-01'),(5, '2020-07-01'),(5, '2020-08-01');
INSERT INTO t VALUES(6, '2020-06-01'),(6, '2020-07-01'),(6, '2020-08-01');
INSERT INTO t VALUES(7, '2020-06-01'),(7, '2020-07-01'),(7, '2020-08-01');
INSERT INTO t VALUES(8, '2020-06-01'),(8, '2020-07-01'),(8, '2020-08-01');
INSERT INTO t VALUES(9, '2020-06-01'),(9, '2020-07-01'),(9, '2020-08-01');
INSERT INTO t VALUES(10, '2020-06-01'),(10, '2020-07-01'),(10, '2020-08-01');
INSERT INTO t VALUES(11, '2020-06-01'),(11, '2020-07-01'),(11, '2020-08-01');
INSERT INTO t VALUES(12, '2020-06-01'),(12, '2020-07-01'),(12, '2020-08-01');
INSERT INTO t VALUES(13, '2020-06-01'),(13, '2020-07-01'),(13, '2020-08-01');
INSERT INTO t VALUES(14, '2020-06-01'),(14, '2020-07-01'),(14, '2020-08-01');
INSERT INTO t VALUES(15, '2020-06-01'),(15, '2020-07-01'),(15, '2020-08-01');
INSERT INTO t VALUES(16, '2020-06-01'),(16, '2020-07-01'),(16, '2020-08-01');
INSERT INTO t VALUES(17, '2020-06-01'),(17, '2020-07-01'),(17, '2020-08-01');
INSERT INTO t VALUES(18, '2020-06-01'),(18, '2020-07-01'),(18, '2020-08-01');
INSERT INTO t VALUES(19, '2020-06-01'),(19, '2020-07-01'),(19, '2020-08-01');
INSERT INTO t VALUES(20, '2020-06-01'),(20, '2020-07-01'),(20, '2020-08-01');
INSERT INTO t VALUES(21, '2020-06-01'),(21, '2020-07-01'),(21, '2020-08-01');
create table tpk(k int) primary key(k) distributed by hash(k) buckets 10;
insert into tpk values(1);
alter table tpk distributed by hash(k);
insert into tpk values(2);
insert into tpk values(3);
insert into tpk values(4);
insert into tpk values(5);
insert into tpk values(6);
insert into tpk values(7);
insert into tpk values(8);
insert into tpk values(9);
insert into tpk values(10);
insert into tpk values(11);
insert into tpk values(12);
insert into tpk values(13);
insert into tpk values(14);
insert into tpk values(15);
insert into tpk values(16);
insert into tpk values(17);
insert into tpk values(18);
insert into tpk values(19);
insert into tpk values(20);
create table t(k int, k1 date) PARTITION BY RANGE(`k1`)
(
    PARTITION `p202006` VALUES LESS THAN ("2020-07-01"),
    PARTITION `p202007` VALUES LESS THAN ("2020-08-01"),
    PARTITION `p202008` VALUES LESS THAN ("2020-09-01")
) distributed by hash(k) buckets 10;
insert into t values(1, '2020-06-01'),(2, '2020-07-01'),(3, '2020-08-01');
alter table t distributed by hash(k);
create table `t#t`(k int) distributed by hash(k) buckets 10;
alter table `t#t` distributed by hash(k) buckets 20;
alter table `t#t` distributed by hash(k) buckets 30;
create table t(k int, k1 date) PARTITION BY date_trunc('day', k1)
distributed by hash(k) buckets 10;
insert into t values(1, '2020-06-01'),(2, '2020-07-01'),(3, '2020-08-01');
alter table t distributed by hash(k);
insert into t values(4, '2020-06-01'),(5, '2020-07-01'),(6, '2020-08-01');
insert into t values(4, '2020-06-01'),(5, '2020-07-01'),(6, '2020-08-01');
insert into t values(4, '2020-06-02'),(5, '2020-07-01'),(6, '2020-08-02');
insert into t values(4, '2020-06-01'),(5, '2020-07-02'),(6, '2020-08-02');
insert into t values(4, '2020-06-03'),(5, '2020-07-01'),(6, '2020-08-03');
insert into t values(4, '2020-06-01'),(5, '2020-07-03'),(6, '2020-08-03');
insert into t values(4, '2020-06-01'),(5, '2020-07-01'),(6, '2020-08-04');
insert into t values(4, '2020-06-04'),(5, '2020-07-04'),(6, '2020-08-04');
insert into t values(4, '2020-06-05'),(5, '2020-07-01'),(6, '2020-08-05');
insert into t values(4, '2020-06-01'),(5, '2020-07-05'),(6, '2020-08-05');
insert into t values(4, '2020-06-06'),(5, '2020-07-06'),(6, '2020-08-06');
insert into t values(4, '2020-06-01'),(5, '2020-07-01'),(6, '2020-08-06');
insert into t values(4, '2020-06-07'),(5, '2020-07-01'),(6, '2020-08-07');
insert into t values(4, '2020-06-07'),(5, '2020-07-07'),(6, '2020-08-07');
insert into t values(4, '2020-06-03'),(5, '2020-07-01'),(6, '2020-08-04');
insert into t values(4, '2020-06-03'),(5, '2020-07-01'),(6, '2020-08-04');
insert into t values(4, '2020-06-04'),(5, '2020-07-01'),(6, '2020-08-06');
insert into t values(4, '2020-06-04'),(5, '2020-07-01'),(6, '2020-08-06');
insert into t values(4, '2020-06-10'),(5, '2020-07-10'),(6, '2020-08-10');
insert into t values(4, '2020-06-10'),(5, '2020-07-10'),(6, '2020-08-10');
create table t(k int) distributed by hash(k) buckets 10;
alter table t distributed by hash(k);
create table t(k int, k1 date) PARTITION BY RANGE(`k1`)
(
    PARTITION `p202006` VALUES LESS THAN ("2020-07-01"),
    PARTITION `p202007` VALUES LESS THAN ("2020-08-01"),
    PARTITION `p202008` VALUES LESS THAN ("2020-09-01")
) distributed by hash(k) buckets 10;
alter table t distributed by hash(k);
CREATE TABLE `duplicate_table_with_null_partition` (
    `k1` date,
    `k2` datetime,
    `k3` char(20),
    `k4` varchar(20),
    `k5` boolean,
    `k6` tinyint,
    `k7` smallint,
    `k8` int,
    `k9` bigint,
    `k10` largeint,
    `k11` float,
    `k12` double,
    `k13` decimal(27,9)
)
DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
PARTITION BY RANGE(`k1`)
(
    PARTITION `p202006` VALUES LESS THAN ("2020-07-01"),
    PARTITION `p202007` VALUES LESS THAN ("2020-08-01"),
    PARTITION `p202008` VALUES LESS THAN ("2020-09-01")
)
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3;
alter table duplicate_table_with_null_partition PARTITIONS(p202006,p202006,p202007) DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 4;
create table t(k int, k1 date) PARTITION BY RANGE(`k1`)
(
    PARTITION `p202006` VALUES LESS THAN ("2020-07-01"),
    PARTITION `p202007` VALUES LESS THAN ("2020-08-01"),
    PARTITION `p202008` VALUES LESS THAN ("2020-09-01")
) distributed by hash(k) buckets 10;
alter table t add temporary partition tp202008 VALUES LESS THAN ("2020-09-01");
alter table t add temporary partition tp202009 VALUES LESS THAN ("2020-10-01");
alter table t drop all temporary partitions;