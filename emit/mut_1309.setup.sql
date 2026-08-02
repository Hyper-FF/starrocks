create table t0 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(1, 4096, 3));
insert into t0 SELECT generate_series, generate_series FROM TABLE(generate_series(1, 4096, 2));
insert into t0 SELECT generate_series, null FROM TABLE(generate_series(1, 4096, 2));