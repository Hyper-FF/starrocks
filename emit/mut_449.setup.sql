CREATE TABLE `string300` (
  `v1` varchar(20) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
insert into string300 SELECT generate_series FROM TABLE(generate_series(1,  300));