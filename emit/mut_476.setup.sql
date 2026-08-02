drop table if exists t0;
drop table if exists t1;
CREATE TABLE `t0` (
  `c0` int(11) NULL COMMENT "",
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c0`, `c1`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 10
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
CREATE TABLE `t1` (
  `c0` int(11) NULL COMMENT "",
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c0`, `c1`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 10
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into t0 select i%100 as c0, i%100 as c1, i%100 as c2 from table(generate_series(1,100000))t(i);
insert into t1 select i%100 as c0, i%100 as c1, i%100 as c2 from table(generate_series(1,1000))t(i);
drop table if exists t0;
drop table if exists t1;
CREATE TABLE `t0` (
  `c0` int(11) NULL COMMENT "",
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c0`, `c1`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 13
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
CREATE TABLE `t1` (
  `c0` int(11) NULL COMMENT "",
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c0`, `c1`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 13
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into t0 select i%100 as c0, i%100 as c1, i%100 as c2 from table(generate_series(1,100000))t(i);
insert into t1 select i%100 as c0, i%100 as c1, i%100 as c2 from table(generate_series(1,1000))t(i);
drop table if exists t0;
drop table if exists t1;
CREATE TABLE `t0` (
  `c0` int(11) NULL COMMENT "",
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c0`, `c1`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 19
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
CREATE TABLE `t1` (
  `c0` int(11) NULL COMMENT "",
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c0`, `c1`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 19
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into t0 select i%100 as c0, i%100 as c1, i%100 as c2 from table(generate_series(1,100000))t(i);
insert into t1 select i%100 as c0, i%100 as c1, i%100 as c2 from table(generate_series(1,1000))t(i);