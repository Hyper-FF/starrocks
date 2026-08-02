create table t1 (
    k1 int,
    k2 int,
    k3 string
)
duplicate key(k1)
distributed by hash(k1) buckets 1
properties("replication_num" = "1");
insert into t1 
select s1, s1 % 1000, repeat('a', 5) FROM TABLE(generate_series(1, 3)) s(s1);