CREATE TABLE `small_table` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`, `c2`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));
insert into t0 values (null,null,null,null);
insert into t1 SELECT * FROM t0;
insert into small_table SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100));
insert into blackhole() select BE_ID from information_schema.be_bvars l join t0 r on l.BE_ID = r.c0;