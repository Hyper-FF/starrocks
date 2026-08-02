create table unpartitioned_table (k1 int) properties("replication_num"="1");
insert into unpartitioned_table select generate_series from table(generate_series(1, 1000));
drop table unpartitioned_table;
create table unpartitioned_table (k1 int) properties("replication_num"="1");
insert into unpartitioned_table select generate_series from table(generate_series(1, 1000));
drop table unpartitioned_table;
create table unpartitioned_table (k1 int) properties("replication_num"="1");
insert into unpartitioned_table select generate_series from table(generate_series(1, 250000));
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
insert into range_partitioned_table select generate_series, generate_series, "no_stat" from table(generate_series(500, 799));
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
insert into list_partitioned_table select 3, generate_series, "data4" from table(generate_series(1, 300));
insert into list_partitioned_table select 1, generate_series, "no_stat" from table(generate_series(1, 300));
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
insert overwrite expr_range_partitioned_table values ('2024-01-01 08:00:00', 100, 'overwrite');
insert into expr_range_partitioned_table
select date_add('2024-01-05 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'day5'
from table(generate_series(0, 5));
insert overwrite expr_range_partitioned_table
select date_add('2024-01-01 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'new_day'
from table(generate_series(0, 9));
insert into expr_range_partitioned_table select '2024-01-06 10:00:00', generate_series, 'no_stat' from table(generate_series(1, 15));
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
alter table expr_range_v2_partitioned_table add partition p4 values [('2024-01-04'), ('2024-01-05'));
insert into expr_range_v2_partitioned_table values 
    ('2024-01-04', 1, 'no_stat'),
    ('2024-01-04', 2, 'no_stat'),
    ('2024-01-04', 3, 'no_stat');
create table t3_predicate(k1 int, k2 int, k3 int) properties("replication_num"="1");
insert into t3_predicate values(1,2,3), (4,5,6);