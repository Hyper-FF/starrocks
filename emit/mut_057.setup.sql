CREATE TABLE t1 (
    c1 int,
    c2 double
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 select generate_series, generate_series from table(generate_series(1, 50000, 3));
CREATE TABLE `test_sorted_streaming_agg_percentile`
(
    `id_int` int(11) NOT NULL COMMENT "",
    `value` double NOT NULL COMMENT ""
)
ENGINE=OLAP 
DUPLICATE KEY(`id_int`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id_int`)
BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into test_sorted_streaming_agg_percentile values(2,1),(2,6),(4,3),(4,8);
CREATE TABLE `t0_convert_to_serialize_format` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`v1`, `v2`, `v3`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3 
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
CREATE TABLE `t1_convert_to_serialize_format` (
  `v4` bigint(20) NULL COMMENT "",
  `v5` bigint(20) NULL COMMENT "",
  `v6` bigint(20) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`v4`, `v5`, `v6`)
DISTRIBUTED BY HASH(`v4`) BUCKETS 3 
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into t0_convert_to_serialize_format values(1,2,3),(4,5,6),(7,8,9);
insert into t1_convert_to_serialize_format values(1,2,3),(4,5,6),(7,8,9);