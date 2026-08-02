create table t1 (c1 int, c2 string) DUPLICATE KEY(c1) DISTRIBUTED BY HASH(c1) PROPERTIES('replication_num' = '1');
insert into t1 values (1, 'a'), (2, 'b'), (3, 'c');
create table t2 (c1 int, c2 int, c3 string) DUPLICATE KEY(c1) DISTRIBUTED BY HASH(c1) PROPERTIES('replication_num' = '1');
insert into t2 values (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z');