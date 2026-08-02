drop table if exists test_bitmap_table1;
CREATE TABLE test_bitmap_table1(
    k1 INT,
    v1 BITMAP BITMAP_UNION
) AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3;
insert into test_bitmap_table1 select 0, NULL;
insert into test_bitmap_table1 select 1, to_bitmap('1');
insert into test_bitmap_table1 select 2, to_bitmap(cast(x as string)) FROM TABLE(generate_series(1, 10, 1)) t(x);
insert into test_bitmap_table1 select 3, to_bitmap(cast(x as string)) FROM TABLE(generate_series(1, 100, 1)) t(x);
CREATE TABLE `t1` (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
CREATE TABLE `t2` (
  `c1` int(11) NULL COMMENT "",
  `c2` int(11) NULL COMMENT "",
  `c3` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`, `c2`)
DISTRIBUTED BY HASH(`c1`, `c2`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
CREATE TABLE `t3` (
  `c1` int(11) NOT NULL COMMENT "",
  `c2` bitmap BITMAP_UNION NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
CREATE TABLE `t4` (
  `c1` int(11) NOT NULL COMMENT "",
  `c2` int(11) NOT NULL COMMENT "",
  `c3` bitmap BITMAP_UNION NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`, `c2`)
DISTRIBUTED BY HASH(`c1`, `c2`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 select 1, null;
insert into t1 select 2, bitmap_empty();
insert into t1 select 3, to_bitmap(1);
insert into t1 select 4, bitmap_agg(generate_series) from table(generate_series(1, 10));
insert into t1 select 5, bitmap_agg(generate_series) from table(generate_series(1, 60));
insert into t1 select 5, bitmap_agg(generate_series) from table(generate_series(8589934592, 8589934610));
insert into t2 select 1, 0, bitmap_empty();
insert into t2 select 2, 1, bitmap_empty();
insert into t2 select 3, 1, to_bitmap(1);
insert into t2 select 4, 2, to_bitmap(1);
insert into t2 select 5, 3, bitmap_agg(generate_series) from table(generate_series(1, 10));
insert into t2 select 6, 10, bitmap_agg(generate_series) from table(generate_series(1, 60));
insert into t2 select 6, 10, bitmap_agg(generate_series) from table(generate_series(8589934592, 8589934610));
insert into t2 select 7, null, bitmap_agg(generate_series) from table(generate_series(8589934592, 8589934610));
insert into t2 select 8, 10, null;
insert into t3 select 1, bitmap_empty();
insert into t3 select 2, to_bitmap(1);
insert into t3 select 3, bitmap_agg(generate_series) from table(generate_series(1, 10));
insert into t3 select 4, bitmap_agg(generate_series) from table(generate_series(1, 60));
insert into t3 select 4, bitmap_agg(generate_series) from table(generate_series(8589934592, 8589934610));
insert into t4 select 1, 0, bitmap_empty();
insert into t4 select 2, 1, bitmap_empty();
insert into t4 select 3, 1, to_bitmap(1);
insert into t4 select 4, 2, to_bitmap(1);
insert into t4 select 5, 3, bitmap_agg(generate_series) from table(generate_series(1, 10));
insert into t4 select 6, 10, bitmap_agg(generate_series) from table(generate_series(1, 60));
insert into t4 select 6, 10, bitmap_agg(generate_series) from table(generate_series(8589934592, 8589934610));