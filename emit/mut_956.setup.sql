CREATE TABLE `small_table1` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`, `c2`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `small_table2` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`, `c2`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `small_table3` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));
insert into t0 values (null,null,null,null);
insert into small_table1 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100));
insert into small_table2 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  2));
insert into small_table3 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  10));
insert into TMP select * from small_table2 s1 union all select * from small_table2 s2 union all select * from small_table2 s3 union all select * from small_table2 s4 union all select * from small_table2 s4 union all select * from small_table2 s5;