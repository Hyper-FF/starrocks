CREATE TABLE `t0` (
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT ""
) ENGINE=OLAP
PROPERTIES("replication_num"="1");
insert into t0 select i/19 as v1, i/11 as v2, i/5 as v3 from table(generate_series(1,100)) t(i);