create table t0 (
    c0 string,
    c1 string,
    c2 int,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series%100, generate_series%100, generate_series%100, generate_series%100 FROM TABLE(generate_series(1, 65535));
insert into t0 values (null, null, null, null);