create table t0 (
    c0 STRING,
    c1 STRING NOT NULL,
    c2 int,
    c3 int NOT NULL
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  30000));