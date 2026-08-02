create temporary table `t0` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create temporary table if not exists `t0` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create temporary table `t0` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
insert into `t0` values (1,1),(2,2),(3,3);
drop temporary table `t0`;
insert into `t0` values (1,1),(2,2),(3,3);
insert into `t1` values (1,1),(2,2);
insert into `t0` values (1,1),(2,2),(3,3);
create temporary table `t1` as select * from `t0`;
create temporary table `t1` as select * from `t0`;
create temporary table if not exists `t1` as select * from `t0`;
create temporary table `t2` as select  * from `t0`;
insert into `t1` values (4,4),(5,5);
create temporary table `t0` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
insert into `t0` values (1,1),(2,2),(3,3);
create table `t` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
insert into `t` values (1,1),(2,2),(3,3);
create temporary table `t` (
    `c1` int,
    `c2` int,
    `c3` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
insert into `t` values (1,1,1),(2,2,2);
drop temporary table `t`;
create temporary table `t` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
alter table `t` add column `c3` int default null;
create temporary table `t` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create temporary table `t` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
drop temporary table `a`.`b`.`c`;
drop temporary table `b`.`c`;
drop temporary table `c`;
create temporary table `t` (
    `c1` int,
    `c2` int,
    `c3` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
insert into `t` values (1,1,1),(2,2,2);
create temporary table `t` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create view `v1` as select * from `t`;
create materialized view `m1` refresh immediate manual as select * from `t`;
create table `t1` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create view `v1` as select * from `t1`;
create temporary table `t` (
    `c1` int,
    `c2` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create temporary table `t` (
    `c1` int,
    `c2` int,
    `c3` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
drop temporary table `t`;
create temporary table `t` (
    `c1` int,
    `c2` int,
    `c3` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create table `t` (
    `c1` int,
    `c2` int,
    `c3` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create temporary table `tbl1` (
    `c1` int,
    `c2` int,
    `c3` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create temporary table `tbl3` as select * from `t`;
create temporary table `tbl1` (
    `c1` int,
    `c2` int,
    `c3` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
create temporary table `tbl3` as select * from `t`;
create table `t` (
    `c1` int,
    `c2` int,
    `c3` int
) engine=OLAP primary key(`c1`) distributed by hash(`c1`) buckets 3 properties("replication_num" = "1");
drop table `t1`;
drop temporary table `t1`;