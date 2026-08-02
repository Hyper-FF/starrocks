CREATE TABLE `expr_join_ta` (
  `key1` int(11) NULL COMMENT "",
  `key2` varchar(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`key1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`key1`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
CREATE TABLE `expr_join_tb` (
  `key1` int(11) NULL COMMENT "",
  `key2` varchar(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`key1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`key1`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
insert into expr_join_ta SELECT generate_series, cast(generate_series as varchar) FROM TABLE(generate_series(1, 40960));
insert into expr_join_tb SELECT generate_series, cast(generate_series as varchar) FROM TABLE(generate_series(1, 40960));
insert into expr_join_ta values (40961, null), (40962, null);
insert into expr_join_tb values (40961, null);