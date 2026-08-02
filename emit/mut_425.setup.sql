create table t1 (c1 int, c2 varchar(100), c3 double) PROPERTIES (
    "replication_num" = "1"
);
insert into t1(c1, c2, c3) values (0, "hello",  5.5), (1, "world", 6.01), (2, "star", 5.0), (3, "rocks", 1.1111);
create view vvv as select cast(1 as double) as c1;
create view vv as select cast(1 as float) as c1;
create view v as select cast(1 as boolean) as c1;
create view dv as select cast('2022-02-02' as date) c1;
create view dvv as select cast('2022-02-02' as datetime) c1;
create table t2 (c1 int, c2 string) DUPLICATE KEY(c1) DISTRIBUTED BY HASH(c1) BUCKETS 16 PROPERTIES (
    "replication_num" = "1"
);
insert into t2 values (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"), (7, "g"), (8, "h"), (9, "i"),
                      (10, "j"), (11, "k"), (12, "l"), (13, "m"), (14, "n"), (15, "o"), (16, "p");