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
CREATE TABLE `lineitem_nullable` (
  `l_orderkey` int(11) COMMENT "",
  `l_partkey` int(11)  COMMENT "",
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
insert into lineitem_nullable values (1,1,1),(1,2,1),(1,3,2),(11,1,11),(11,2,1),(2,3,2),(2,3,null),(null,null,null);
CREATE TABLE `aware3` (
  `k1` int(11) COMMENT "",
  `k2` int(11)  COMMENT "",
  `k3` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into aware3 SELECT generate_series, if(generate_series<4095, 1, null), if(generate_series<4095, generate_series, null) FROM TABLE(generate_series(1,  4096));
CREATE TABLE `build1` (
  `k1` int(11) COMMENT "",
  `k2` int(11)  COMMENT "",
  `k3` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into build1 values (1,1,1),(1,1,1),(1,1,1),(1,1,1),(1,1,1),(1,1,1),(1,1,1),(2,2,2),(3,3,3),(4,4,4);
CREATE TABLE `build2` (
  `k1` int(11) COMMENT "",
  `k2` int(11)  COMMENT "",
  `k3` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into build2 values (1,1,1),(NULL,NULL,NULL);