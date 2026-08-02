create table t0 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 4 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(1,  6553500));
insert into t1 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(4096,  8192));
insert into empty_t values (-1,-1);