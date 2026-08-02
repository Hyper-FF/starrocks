CREATE TABLE `t1` (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap REPLACE_IF_NOT_NULL NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 values (1, bitmap_from_string("1,2,3")), (2, bitmap_from_string("4,5,6"));
insert into t1 values (1, null);
insert into t1 values (1, bitmap_from_string("7,8,9"));