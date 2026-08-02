create table t1(c1 int, c2 bigint, c3 string, c4 string) 
properties('replication_num'='1');
insert into t1 values (1, 1, 's1', 's1');
insert into t1 values (2, 2, 's2', 's2');
insert into t1 values (3, 3, 's3', 's3');
create table t2(c5 int, c6 bigint, c7 string, c8 string) 
properties('replication_num'='1');
insert into t2 select * from t1;