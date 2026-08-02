CREATE TABLE partition_str2date (
        create_time varchar(100),
        sku_id varchar(100),
        total_amount decimal,
        id int
)
PARTITION BY RANGE(str2date(create_time, '%Y-%m-%d %H:%i:%s'))(
START ("2021-01-01") END ("2021-01-10") EVERY (INTERVAL 1 DAY)
);
insert into partition_str2date values('2021-01-04 23:59:59', '1', 1, 1);
drop table partition_str2date;
CREATE TABLE partition_str2date (
        create_time varchar(100),
        sku_id varchar(100),
        total_amount decimal,
        id int
)
PARTITION BY RANGE(str2date(create_time, '%Y-%m-%d'))(
START ("2021-01-01") END ("2021-01-10") EVERY (INTERVAL 1 DAY)
);
insert into partition_str2date values('2021-01-04', '1', 1, 1);
drop table partition_str2date;
CREATE TABLE partition_str2date (
        create_time varchar(100),
        sku_id varchar(100),
        total_amount decimal,
        id int
)
PARTITION BY RANGE(str2date(create_time, '%Y%m%d%H'))(
START ("2021-01-01") END ("2021-01-10") EVERY (INTERVAL 1 DAY)
);
insert into partition_str2date values('2021010423', '1', 1, 1);
drop table partition_str2date;
CREATE TABLE partition_str2date (
        create_time varchar(100),
        sku_id varchar(100),
        total_amount decimal,
        id int
)
PARTITION BY RANGE(str2date(create_time, '%Y-%m-%d'))(
START ("2021-01-01") END ("2021-01-10") EVERY (INTERVAL 1 DAY)
)
PROPERTIES (
"replication_num" = "1"
);
alter table partition_str2date add partition p20231226 VALUES [("2023-12-26"), ("2023-12-27"));
CREATE TABLE partition_str2date (
        create_time varchar(100),
        sku_id varchar(100),
        total_amount decimal,
        id int
)
PARTITION BY RANGE(str2date(create_time, '%Y-%m-%d'))(
START ("2021-01-01") END ("2021-01-10") EVERY (INTERVAL 1 DAY)
);
insert into partition_str2date values('2021-01-04','1',1.1,1);
insert into partition_str2date values('2021-01-05','1',1.1,1);
insert into partition_str2date values('2021-01-06','1',1.1,1);
create table t1 (dt varchar(20), id int) partition by range(str2date(dt, "%Y-%m-%d")) (start ("2021-01-01") end ("2021-01-10") every (interval 1 day));
create table t2 (dt varchar(20), id int) partition by range(str2date(dt, "%Y-%m")) (start ("2021-01-01") end ("2021-01-10") every (interval 1 day));
create table t3 (dt varchar(20), id int) partition by str2date(dt, "%Y-%m-%d");