create table t_sort (
    c0 INT,
    c1 INT,
    c2 INT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 4 PROPERTIES('replication_num' = '1');
insert into t_sort
SELECT generate_series, generate_series % 100000, generate_series % 5000
FROM TABLE(generate_series(1, 500000));