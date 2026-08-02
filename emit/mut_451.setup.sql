CREATE TABLE `t1` (
  `v1` varchar(20) NOT NULL COMMENT "",
  `v2` varchar(20) NOT NULL COMMENT ""
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
CREATE TABLE `t4` (
  `v1` varchar(20) COMMENT ""
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
insert into t1 SELECT generate_series%100, generate_series%255 FROM TABLE(generate_series(1, 65535));
insert into t2 SELECT generate_series%100, generate_series%255 FROM TABLE(generate_series(1, 65535));
insert into t3 SELECT generate_series%100 + 1000, generate_series%255 + 1000 FROM TABLE(generate_series(1, 65535));
insert into t4 values ('a');
insert into t4 values (NULL);