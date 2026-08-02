drop table if exists unpartitioned_table;
create table unpartitioned_table (k1 int) properties("replication_num"="1");
insert into unpartitioned_table select generate_series from table(generate_series(1, 1000));
insert into unpartitioned_table select generate_series from table(generate_series(1, 2000));
insert into unpartitioned_table select generate_series from table(generate_series(1, 3000));
insert overwrite test_overwrite_statistics_behavior.unpartitioned_table select generate_series from table(generate_series(1, 5000));
insert overwrite test_overwrite_statistics_behavior.unpartitioned_table select generate_series from table(generate_series(1, 7000));
insert into unpartitioned_table select generate_series from table(generate_series(1, 3000));
insert overwrite test_overwrite_statistics_behavior.unpartitioned_table select generate_series from table(generate_series(1, 5000));
drop table unpartitioned_table;
create table unpartitioned_table (k1 int) properties("replication_num"="1");
insert into unpartitioned_table select generate_series from table(generate_series(1, 1000));
insert into unpartitioned_table select generate_series from table(generate_series(1, 4000));
insert overwrite unpartitioned_table select generate_series from table(generate_series(1, 3000));
insert overwrite unpartitioned_table select generate_series from table(generate_series(1, 3000));
drop table unpartitioned_table;
create table unpartitioned_table (k1 int) properties("replication_num"="1");
insert into unpartitioned_table select generate_series from table(generate_series(1, 250000));
insert into unpartitioned_table select generate_series from table(generate_series(250000, 500000));
insert overwrite unpartitioned_table select generate_series from table(generate_series(500000, 1000000));
insert overwrite unpartitioned_table select generate_series from table(generate_series(60000, 70000));
drop table if exists range_partitioned_table;
create table range_partitioned_table (
    k1 int,
    k2 int,
    k3 varchar(20)
)
partition by range(k1) (
    partition p1 values [("0"), ("1000")),
    partition p2 values [("1000"), ("2000")),
    partition p3 values [("2000"), ("3000")),
    partition p4 values [("3000"), ("4000"))
)
properties("replication_num"="1");
insert into range_partitioned_table select generate_series, generate_series, "data1" from table(generate_series(1, 100));
insert into range_partitioned_table select generate_series, generate_series, "data2" from table(generate_series(500, 2500));
insert overwrite range_partitioned_table partition(p2)
select generate_series, generate_series, "overwrite" from table(generate_series(1000, 1499));
insert overwrite range_partitioned_table
select generate_series, generate_series, "full_overwrite" from table(generate_series(0, 3999));
insert into range_partitioned_table select generate_series, generate_series, "p4_data" from table(generate_series(3000, 3299));
insert overwrite range_partitioned_table partition(p1) select generate_series, generate_series, "p1_new" from table(generate_series(0, 499));
insert into range_partitioned_table select generate_series, generate_series, "no_stat" from table(generate_series(500, 799));
drop table if exists list_partitioned_table;
create table list_partitioned_table (
    region int,
    k1 int,
    k2 varchar(20)
)
partition by list(region) (
    partition p1 values in ("1"),
    partition p2 values in ("2"),
    partition p3 values in ("3"),
    partition p4 values in ("4")
)
properties("replication_num"="1");
insert into list_partitioned_table select 1, generate_series, "data1" from table(generate_series(1, 100));
insert into list_partitioned_table select 1, generate_series, "data2" from table(generate_series(101, 200));
insert into list_partitioned_table select 2, generate_series, "data3" from table(generate_series(1, 500));
insert into list_partitioned_table select 3, generate_series, "data4" from table(generate_series(1, 300));
insert overwrite list_partitioned_table partition(p2) select 2, generate_series, "overwrite" from table(generate_series(1, 600));
insert overwrite list_partitioned_table
select case when generate_series <= 1000 then 1
            when generate_series <= 2000 then 2
            when generate_series <= 3000 then 3
            else 4 end as region,
       generate_series, "full_overwrite"
from table(generate_series(1, 4000));
insert into list_partitioned_table select 4, generate_series, "p4_data" from table(generate_series(1, 300));
insert overwrite list_partitioned_table partition(p1) select 1, generate_series, "p1_new" from table(generate_series(1, 500));
insert into list_partitioned_table select 1, generate_series, "no_stat" from table(generate_series(1, 300));
drop table if exists expr_range_partitioned_table;
create table expr_range_partitioned_table (
    dt datetime,
    k1 int,
    k2 varchar(20)
)
partition by date_trunc('day', dt)
properties("replication_num"="1");
insert into expr_range_partitioned_table values 
    ('2024-01-01 10:00:00', 1, 'data1'),
    ('2024-01-01 11:00:00', 2, 'data2'),
    ('2024-01-01 12:00:00', 3, 'data3');
insert into expr_range_partitioned_table values 
    ('2024-01-02 10:00:00', 1, 'data5'),
    ('2024-01-02 11:00:00', 2, 'data6'),
    ('2024-01-03 10:00:00', 1, 'data7');
insert overwrite expr_range_partitioned_table values ('2024-01-01 08:00:00', 100, 'overwrite');
insert into expr_range_partitioned_table
select date_add('2024-01-04 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'day4'
from table(generate_series(0, 11));
insert into expr_range_partitioned_table
select date_add('2024-01-05 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'day5'
from table(generate_series(0, 5));
insert overwrite expr_range_partitioned_table
select date_add('2024-01-01 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'new_day'
from table(generate_series(0, 9));
insert overwrite expr_range_partitioned_table partition(p20240101)
select '2024-01-01 09:00:00',generate_series, 'new_day' from table(generate_series(0, 4));
insert into expr_range_partitioned_table select '2024-01-06 10:00:00', generate_series, 'no_stat' from table(generate_series(1, 15));
drop table if exists expr_range_v2_partitioned_table;
create table expr_range_v2_partitioned_table (
    dt_str varchar(20),
    k1 int,
    k2 varchar(20)
)
partition by range(str2date(dt_str, '%Y-%m-%d')) (
    partition p1 values [('2024-01-01'), ('2024-01-02')),
    partition p2 values [('2024-01-02'), ('2024-01-03')),
    partition p3 values [('2024-01-03'), ('2024-01-04'))
)
properties("replication_num"="1");
insert into expr_range_v2_partitioned_table values 
    ('2024-01-01', 1, 'data1'),
    ('2024-01-01', 2, 'data2'),
    ('2024-01-01', 3, 'data3');
insert into expr_range_v2_partitioned_table values 
    ('2024-01-02', 1, 'data5'),
    ('2024-01-02', 2, 'data6'),
    ('2024-01-03', 1, 'data7');
insert overwrite expr_range_v2_partitioned_table partition(p1) values ('2024-01-01', 100, 'overwrite');
insert overwrite expr_range_v2_partitioned_table partition(p1) select '2024-01-01', generate_series, 'p1_new' from table(generate_series(1, 10));
alter table expr_range_v2_partitioned_table add partition p4 values [('2024-01-04'), ('2024-01-05'));
insert into expr_range_v2_partitioned_table values 
    ('2024-01-04', 1, 'no_stat'),
    ('2024-01-04', 2, 'no_stat'),
    ('2024-01-04', 3, 'no_stat');
drop table if exists test_overwrite_statistics_behavior.t3_predicate;
create table t3_predicate(k1 int, k2 int, k3 int) properties("replication_num"="1");
insert into t3_predicate values(1,2,3), (4,5,6);