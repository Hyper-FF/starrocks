CREATE TABLE t1 (
    k1 INT,
    k2 VARCHAR(20))
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) PROPERTIES('replication_num'='1');
insert into t1 values (1,"1");
CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(20))
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) PROPERTIES('replication_num'='1');
insert into t2 select sum(k1),k2 from t1 group by k2;
create table t0 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(1,  4096));
insert into t0 select * from t0;
insert into t0 select * from t0;
create table t3 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t3 SELECT generate_series, 40960 - generate_series FROM TABLE(generate_series(1,  40960));
insert into t3 values (null,null);
insert into t3 select * from t3;
insert into t3 select * from t3;
CREATE TABLE `t4` (
  `c0` int(11) NOT NULL COMMENT "",
  `c1` int(11) NOT NULL
) ENGINE=OLAP 
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
PARTITION BY RANGE(`c0`)
(
PARTITION p1 VALUES [("-2147483648"), ("0")),
PARTITION p2 VALUES [("0"), ("1024")),
PARTITION p3 VALUES [("1024"), ("2048")),
PARTITION p4 VALUES [("2048"), ("4096")),
PARTITION p5 VALUES [("4096"), ("8192")),
PARTITION p6 VALUES [("8192"), ("65536")),
PARTITION p7 VALUES [("65536"), ("2100000000")))
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "spill_agg_colocate_1",
"compression" = "LZ4"
);
insert into t2 select * from t1;
create table t5 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 16 PROPERTIES('replication_num' = '1');
insert into t5 SELECT generate_series, 650000 - generate_series FROM TABLE(generate_series(1,  650000));