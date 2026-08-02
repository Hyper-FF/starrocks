CREATE TABLE `tab1` (
  `c0` int(11) NULL COMMENT "",
  `c1` STRUCT<v1 INT, v2 Struct<v3 int, v4 int>>
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into tab1 values (1, row(1, row(1,1))), (2, row(2, row(2,2)));
alter table tab1 modify column c1 add field v1.v5 int;
alter table tab1 modify column c1 add field v2 int;
alter table tab1 modify column c1 add field v2.v3 int;
alter table tab1 modify column c1 add field val1 int;
insert into tab1 values (3, row(3, row(3,3), 3));
CREATE MATERIALIZED VIEW mv1
            distributed by hash(c0) as
            SELECT * from tab1;
alter table tab1 modify column c1 drop field v2.v5;
alter table tab1 modify column c1 drop field v1;
alter table tab1 modify column c1 add field v1 int AFTER v2;
insert into tab1 values (4, row(row(4,4), 4, 4));
drop table tab1;
CREATE TABLE `tab1` (
  `c0` int(11) NULL COMMENT "",
  `c1` Array<Struct<v1 int, v2 int>>
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into tab1 values (1, [row(1,1), row(1,2)]), (2, [row(2,1), row(2,2)]);
alter table tab1 modify column c1 drop field [*];
alter table tab1 modify column c1 drop field [*].v3;
alter table tab1 modify column c1 add field val1 int;
alter table tab1 modify column c1 add field [*].val1 int;
insert into tab1 values (3, [row(3,1,1), row(3,2,1)]);
CREATE MATERIALIZED VIEW mv1
            distributed by hash(c0) as
            SELECT * from tab1;
alter table tab1 modify column c1 drop field [*].v1;
insert into tab1 values (4, [row(4,4), row(4,5)]);
alter table tab1 modify column c1 add field [*].v1 int;
insert into tab1 values (5, [row(5,5,5), row(5,6,6)]);
drop table tab1;
CREATE TABLE `tab1` (
  `c0` int(11) NULL COMMENT "",
  `c1` Array<Struct<v1 int, v2 int>>
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "false",
"replicated_storage" = "true",
"replication_num" = "1"
);
alter table tab1 modify column c1 add field [*].val1 int;
alter table tab1 modify column c1 drop field [*].v1;
drop table tab1;
CREATE TABLE `tab1` (
  `c0` int(11) NULL COMMENT "",
  `c1` Struct<v1 int>
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
alter table tab1 modify column c1 drop field v1;
drop table tab1;
CREATE TABLE `t` (
  `c1` int(11) NULL COMMENT "",
  `c2` struct<v2_1 int(11)> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into t values(1, row(1)),(2,row(2));
alter table t modify column c2 add field v2_2 string;
insert into t values(3, row(3, 'Beijing')),(4,row(4,'Shanghai'));
alter table t modify column c2 drop field v2_2;
alter table t modify column c2 add field v2_2 date;
drop table t;