CREATE TABLE `t_hash_test` (
  `c0` int DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` string DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);
insert into t_hash_test SELECT generate_series, generate_series, generate_series FROM TABLE(generate_series(1, 1000));
DROP TABLE t_hash_test;