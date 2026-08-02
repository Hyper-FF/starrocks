create table t (
    c0 int
) duplicate key (c0)
distributed by hash (c0) buckets 1
properties (
    "replication_num" = "1"
);
insert into t values (1);
alter table t add column c1 int key NOT NULL default "2";
insert into t values (2);