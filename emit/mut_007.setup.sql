create table t1 (
    k1 int,
    k2 int,
    c1 string
)
duplicate key(k1, k2)
distributed by hash(k1) buckets 3
properties("replication_num" = "1");
insert into t1
select generate_series, generate_series + 10000, concat('a', generate_series) from TABLE(generate_series(0, 10000 - 1));
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;