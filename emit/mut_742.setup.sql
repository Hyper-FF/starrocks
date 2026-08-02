CREATE TABLE `mock_tbl` (
 k1 date,
  k2 int,
  v1 int
) ENGINE=OLAP
PARTITION BY RANGE(`k1`)
(
   PARTITION p0 values [('2021-07-23'),('2021-07-26')),
   PARTITION p1 values [('2021-07-26'),('2021-07-29')),
   PARTITION p2 values [('2021-07-29'),('2021-08-02')),
   PARTITION p3 values [('2021-08-02'),('2021-08-04'))
)
DISTRIBUTED BY HASH(`k2`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
insert into mock_tbl values('2021-07-23',2,10), ('2021-07-27',2,10), ('2021-07-29',2,10), ('2021-08-02',2,10);
create materialized view test_mv_with_many_to_many 
partition by date_trunc('month',k1) 
distributed by hash(k2) buckets 3 
refresh deferred manual
properties('replication_num' = '1', 'partition_refresh_number'='1')
as select k1, k2, v1 from mock_tbl;
insert into mock_tbl values ('2021-07-29',3,10), ('2021-08-02',3,10);
drop table mock_tbl;
CREATE TABLE `mock_tbl` (
 k1 date,
  k2 int,
  v1 int
) ENGINE=OLAP
PARTITION BY RANGE(`k1`)
(
   PARTITION p0 values [('2021-07-01'),('2021-08-01')),
   PARTITION p1 values [('2021-08-01'),('2021-09-01')),
   PARTITION p2 values [('2021-09-01'),('2021-10-01'))
)
DISTRIBUTED BY HASH(`k2`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
insert into mock_tbl values('2021-07-01',2,10), ('2021-08-01',2,10), ('2021-08-02',2,10), ('2021-09-03',2,10);
create materialized view test_mv_with_one_to_many 
partition by date_trunc('day',k1) 
distributed by hash(k2) buckets 3 
refresh deferred manual
properties('replication_num' = '1', 'partition_refresh_number'='1')
as select k1, k2, v1 from mock_tbl;
insert into mock_tbl values ('2021-08-02',3,10), ('2021-09-03',3,10);
drop table mock_tbl;