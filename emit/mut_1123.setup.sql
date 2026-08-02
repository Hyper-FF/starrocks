CREATE TABLE `t0` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT "",
  `c4` int(11) NULL COMMENT "",
  `c5` int(11) NULL COMMENT "",
  `c6` int(11) NULL COMMENT "",
  `c7` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 48
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series, generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));