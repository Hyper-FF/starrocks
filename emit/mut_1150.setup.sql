CREATE TABLE t_hang (
    k1 INT,
    v1 INT
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES('replication_num' = '1');
insert into t_hang select generate_series, generate_series from TABLE(generate_series(1, 1000000));
drop table t_hang;