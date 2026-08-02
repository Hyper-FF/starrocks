CREATE TABLE `t1` (
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t2` (
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t3` (
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t4` (
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t1 select generate_series, generate_series from table(generate_series(1, 5));
insert into t2 select generate_series, generate_series from table(generate_series(1, 2));
insert into t3 select generate_series, generate_series from table(generate_series(1, 9));
insert into t4 select generate_series, generate_series from table(generate_series(2, 8));
insert into t1 select generate_series, generate_series from table(generate_series(1, 4098));
insert into t2 select generate_series, generate_series from table(generate_series(2, 8));
insert into t1 select generate_series, generate_series from table(generate_series(2, 8));
insert into t2 select generate_series, generate_series from table(generate_series(1, 4098));
insert into t1 select generate_series, generate_series from table(generate_series(1, 10000));
insert into t2 select generate_series, generate_series from table(generate_series(1, 10000));
insert into t1 select generate_series, generate_series from table(generate_series(1, 9998));
insert into t2 select generate_series, generate_series from table(generate_series(1, 10000));
insert into t1 select generate_series, generate_series from table(generate_series(1, 10000));
insert into t2 select generate_series, generate_series from table(generate_series(1, 9998));
insert into t1 select generate_series, generate_series from table(generate_series(1, 77));
insert into t2 select generate_series, generate_series from table(generate_series(1, 133));
insert into t1 select generate_series, generate_series from table(generate_series(1, 2000000));
insert into t2 select generate_series, generate_series from table(generate_series(1, 1));
create table t5 (x int null, v double null)
duplicate key(x) distributed by hash(x) buckets 4 properties("replication_num"="1");
insert into t5 select g%100000, cast(g%500 as double)
from table(generate_series(1,20000)) t(g);