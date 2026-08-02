CREATE TABLE `t0` (
  `v1` bigint NULL COMMENT "",
  `v2` bigint NULL COMMENT "",
  `v3` bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`, `v2`, v3)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false"
);
INSERT INTO t0 VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9), (NULL, NULL, NULL);