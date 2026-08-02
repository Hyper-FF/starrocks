create table t1 (id int, v varchar(64)) properties("replication_num" = "1");
insert into t1 values (1, 'abc'), (2, 'l'), (3, ''), (4, NULL), (5, 'StarRocks'), (6, 'hello');