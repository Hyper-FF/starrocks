CREATE TABLE `t0` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT "",
  `v4` varchar NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`, `v2`, `v3`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into t0 values(1, 2, 3, 'a'), (1, 3, 4, 'b'), (2, 3, 4, 'a'), (null, 1, null, 'c'), (4, null, 1 , null),
(5, 1 , 3, 'c'), (2, 2, null, 'a'), (4, null, 4, 'c'), (null, null, 2, null);