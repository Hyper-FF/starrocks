CREATE TABLE `t1_nulls` (
  `v1` int NULL,
  `v2` bigint NULL,
  `v3` largeint NULL,
  `v4` string NULL,
  `v5` json NULL,
  `v6` map<int, string> NULL,
  `v7` array<int> NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
insert into t1_nulls SELECT generate_series, generate_series, generate_series, generate_series, generate_series, map(generate_series, generate_series), [generate_series] FROM TABLE(generate_series(1,  40960));
CREATE TABLE `t1_not_nulls` (
  `v1` int,
  `v2` bigint,
  `v3` largeint,
  `v4` string,
  `v5` json,
  `v6` map<int, string>,
  `v7` array<int>
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
insert into t1_not_nulls SELECT generate_series, generate_series, generate_series, generate_series, generate_series, map(generate_series, generate_series), [generate_series] FROM TABLE(generate_series(1,  40960));