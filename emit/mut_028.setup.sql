create table t0 (
    c0 STRING,
    c1 STRING,
    c2 STRING
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  4096));