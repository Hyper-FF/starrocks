CREATE TABLE `t0` (
  `v1` bigint NULL COMMENT "",
  `v2` bigint NULL COMMENT "",
  `v3` bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`, `v2`, v3)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t1` (
  `v4` bigint NULL COMMENT "",
  `v5` bigint NULL COMMENT "",
  `v6` bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`v4`, `v5`, v6)
DISTRIBUTED BY HASH(`v4`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t2` (
  `v7` bigint NULL COMMENT "",
  `v8` bigint NULL COMMENT "",
  `v9` bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`v7`, `v8`, v9)
DISTRIBUTED BY HASH(`v7`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
insert into t0 values (null, 1, 1), (2, null, null), (3, 3, null), (4, 4, 4), (5, 5, 5), (6, 6, 6), (null, null, null), (7, 7, 7), (8, 8, 8);
insert into t1 values (null, 1, 1), (2, null, null), (3, 3, null), (4, 4, 4), (5, 5, 5), (6, 6, 6), (null, null, null), (7, 7, 7), (8, 8, 8);
insert into t2 values (null, 1, 1), (2, null, null), (3, 3, null), (4, 4, 4), (5, 5, 5), (6, 6, 6), (null, null, null), (7, 7, 7), (8, 8, 8);