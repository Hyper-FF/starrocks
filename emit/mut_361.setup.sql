create table t1 (k1 int, k2 int, v1 int) duplicate key(k1)
  distributed by hash(k1) buckets 1 properties("replication_num" = "1");
insert into t1 values (1, 10, 100), (2, 20, 200), (3, 30, 300);
alter table t1 add column k0 int key default "0" first;
insert into t1 values (-1, 4, 40, 400);