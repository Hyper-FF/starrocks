CREATE TABLE `t0` (
  `v1` bigint(20) COMMENT "",
  `v2` bigint(20) COMMENT "",
  `v3` bigint(20) COMMENT "",
  `v4` varchar COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`, `v2`, `v3`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
CREATE TABLE `tt0` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) not NULL COMMENT "",
  `c2` varchar(200) not NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 4
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into tt0 values (1,"test","test",1);