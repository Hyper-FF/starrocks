create table t0 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  409600));
insert into t0 select * from t0;
insert into t0 select * from t0;
insert into t0 values (null,null);