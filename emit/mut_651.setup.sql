CREATE TABLE `lineitem` (
  `l_orderkey` int(11) NOT NULL COMMENT "",
  `l_partkey` int(11) NOT NULL COMMENT "",
  `l_suppkey` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into lineitem values (1,1,1),(1,2,1),(1,3,2),(11,1,11),(11,2,1),(2,3,2),(2,3,null);