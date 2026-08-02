CREATE TABLE `tarray` (
  `id` int(4) NULL COMMENT "",
  `val` array<int> NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 2
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "false",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into tarray SELECT generate_series, [generate_series % 4, generate_series%3] FROM TABLE(generate_series(1,  4096));