create table t1 (
    k1 int
)
duplicate key(k1)
distributed by hash(k1) buckets 32
properties("replication_num" = "1");
insert into t1 select generate_series FROM TABLE(generate_series(1, 65535));
insert into t1 select k1 + 65535 from t1;
insert into t1 select k1 + 65535*2 from t1;
insert into t1 select k1 + 65535*3 from t1;