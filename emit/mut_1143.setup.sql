create table t0 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(1,  4096));
insert into t0 select * from t0;
insert into t1 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(4096,  8192));
insert into t2 SELECT generate_series, 40960 - generate_series FROM TABLE(generate_series(1,  409600));
create table tstring (
    c0 STRING,
    c1 STRING
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into tstring SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(1,  4096));
insert into tstring select * from tstring;