CREATE TABLE `tx3` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 2
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
insert into tx3 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  1400));