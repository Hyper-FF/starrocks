CREATE TABLE `t1` (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
CREATE TABLE `t2` (`c1` int, `c2` string);
insert into t1 values (1, bitmap_empty());
insert into t1 values (1, to_bitmap(1));
insert into t1 values (1, to_bitmap(17179869184));
insert into t1 select 1, bitmap_agg(generate_series) from table(generate_series(1, 5));
insert into t1 select 1, bitmap_agg(generate_series) from table(generate_series(1, 40));
insert into t1 select 1, bitmap_agg(generate_series) from table(generate_series(1, 20));
insert into t1 select 1, bitmap_agg(generate_series) from table(generate_series(17179869184, 17179869284));
insert into t1 select 1, bitmap_agg(generate_series) from table(generate_series(1, 80));
insert into t1 select 2, bitmap_agg(generate_series) from table(generate_series(1, 200));
insert into t1 select 2, bitmap_agg(generate_series) from table(generate_series(900, 910));
insert into t1 select 1, bitmap_agg(generate_series) from table(generate_series(1, 80));
insert into t2 select c1, bitmap_to_binary(c2) from t1;