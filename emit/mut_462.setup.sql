CREATE TABLE `t0` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 48
PROPERTIES (
"replication_num" = "1");
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));
insert into t0 values (null,null,null,null);
insert into blackhole() select * from t0 limit 10;
insert into blackhole() select * from t0 order by 1 limit 10;
insert into blackhole() select * from t0 limit 10;
insert into blackhole() select * from t0 order by 1 limit 10;