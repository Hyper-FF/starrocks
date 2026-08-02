CREATE TABLE `t0` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
CREATE TABLE `t1` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));
insert into t1 select * from t0;
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(40960, 40960 + 20000));
insert into t1 values(null,null,null,null);
CREATE TABLE `nullaware_anti_join_test` (
  `k1` int(11) COMMENT "",
  `k2` int(11)  COMMENT "",
  `k3` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into nullaware_anti_join_test values (1,1,1),(1,2,1),(1,3,2),(11,1,11),(11,2,1),(2,3,2),(2,3,null),(null,null,null);
CREATE TABLE `t2` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
CREATE TABLE `t3` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
insert into t2 values(null,null,null,null);
insert into t3 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  409600));