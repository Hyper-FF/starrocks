create table t1 (c1 int, c2 varchar(64), c3 date) properties("replication_num" = "1");
insert into t1 values (1, 'aaa', '2024-01-01'), (2, 'bbb', '2024-06-15'), (3, 'ccc', '2024-12-31');