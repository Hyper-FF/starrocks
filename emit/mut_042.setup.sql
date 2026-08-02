CREATE TABLE `t1` (
  `c0` bigint NOT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL,
  `c3` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);
insert into t1 SELECT generate_series, generate_series, generate_series, null FROM TABLE(generate_series(1,  40960));