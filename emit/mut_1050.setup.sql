CREATE TABLE `t1` (
  `c_1_0` decimal(14, 11) NULL COMMENT "",
  `c_1_1` decimal(34, 32) NULL COMMENT "",
  `c_1_13` boolean NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c_1_0`)
DISTRIBUTED BY HASH(`c_1_1`)
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into t1
values (1, 1, 1), (2, 2, 0), (3,3,null);