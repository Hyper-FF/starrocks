CREATE TABLE `t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  409600));