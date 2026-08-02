CREATE TABLE `test_window_with_null_child` (
  `c0` string NOT NULL,
  `c1` bigint NOT NULL,
  `c2` bigint NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 16
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `tsmall` (
  `c0` string NOT NULL,
  `c1` bigint NOT NULL,
  `c2` bigint NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 16
PROPERTIES (
"replication_num" = "1"
);
insert into test_window_with_null_child SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  40960));
insert into tsmall SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  1));