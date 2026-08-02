create table t0 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t0 SELECT null, null FROM TABLE(generate_series(1,  65536));
insert into t0 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  40960));
create table t1 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  10));
CREATE TABLE `t2` (
  `c0` int(11) NOT NULL COMMENT "",
  `c1` int(11) NOT NULL
) ENGINE=OLAP 
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
PARTITION BY RANGE(`c1`)
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
"replication_num" = "1");
insert into t2 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  63336));
create table t3 (
    c0 INT,
    c1 BIGINT
) UNIQUE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t3 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  7));
insert into t3 SELECT generate_series, generate_series + 1 FROM TABLE(generate_series(1,  7));
insert into t3 SELECT generate_series, generate_series + 3 FROM TABLE(generate_series(1,  81920));
insert into t3 SELECT generate_series, generate_series + 2 FROM TABLE(generate_series(1,  7));
create table t4 (
    c0 INT,
    c1 BIGINT REPLACE
) UNIQUE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t4 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  7));
insert into t4 SELECT generate_series, generate_series + 1 FROM TABLE(generate_series(1,  7));
insert into t4 SELECT generate_series, generate_series + 3 FROM TABLE(generate_series(1,  81920));
insert into t4 SELECT generate_series, generate_series + 2 FROM TABLE(generate_series(1,  7));
create table t5 (
    c0 INT,
    c1 BIGINT
) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t5 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  7));
insert into t5 SELECT generate_series, generate_series + 1 FROM TABLE(generate_series(1,  7));
insert into t5 SELECT generate_series, generate_series + 3 FROM TABLE(generate_series(1,  81920));
insert into t5 SELECT generate_series, generate_series + 2 FROM TABLE(generate_series(1,  7));
insert into tlow SELECT generate_series, if (generate_series<10, null, generate_series%10), generate_series, generate_series FROM TABLE(generate_series(1,  40960));