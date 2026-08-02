CREATE TABLE `t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 2
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  4096));
insert into t0 select * from t0;
CREATE TABLE `t1` (
  `c0` string DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 96
PROPERTIES (
"replication_num" = "1"
);
insert into t1 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  4096));
insert into t1 select * from t1;
CREATE TABLE `t2` (
  `c0` string DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 96
PROPERTIES (
"replication_num" = "1"
);
insert into t2 select * from t1 where crc32(c0) % 96 = 95;