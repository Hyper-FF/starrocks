create table t0 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t0 SELECT null, null FROM TABLE(generate_series(1,  65536));
insert into t0 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  257* 4096));