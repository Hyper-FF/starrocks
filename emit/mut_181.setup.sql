CREATE TABLE `t1` (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 select 1, bitmap_empty();
insert into t1 select 1, to_bitmap(1);
insert into t1 select 1, bitmap_agg(generate_series) from table(generate_series(1, 10));
insert into t1 select 1, bitmap_agg(generate_series) from table(generate_series(1, 40));
insert into t1 select 1, bitmap_agg(generate_series) from table(generate_series(0, 4093));
insert into t1 select 2, bitmap_agg(generate_series) from table(generate_series(4094, 8000));