create table t (k int NOT NULL, v varchar(32) NOT NULL) duplicate key(k) distributed by hash(k) buckets 1 properties("replication_num"="1");
insert into t values (1, '100'), (2, 'abc'), (3, '99999999999999');
alter table t modify column v int NOT NULL;