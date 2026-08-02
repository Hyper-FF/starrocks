create table t0 (
    c0 int,
    c1 int not null
) duplicate key(c0) distributed by hash(c0) buckets 3 properties("replication_num" = "1");
create table t1 (
    c0 int,
    c1 int not null
) duplicate key(c0) distributed by hash(c0) buckets 3 properties("replication_num" = "1");
insert into t0 values (1,1),(2,2),(3,3);
insert into t1 values (1,1),(2,2),(NULL,4);