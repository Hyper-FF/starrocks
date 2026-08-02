CREATE TABLE `t1` (
  c1 string,
  c2 string,
  c3 string
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 48
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE `t2` (
  c1 string,
  c2 string,
  c3 string
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 48
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE `t3` (
  c1 string,
  c2 string,
  c3 string
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 48
PROPERTIES (
  "replication_num" = "1"
);
insert into t3 select 'c1-1', 'unknown', 'c3';
insert into t2 select 'c1-1', 'c2-1', 'c3';
insert into t1 select 'c1-1', 'c2-1', 'c3';