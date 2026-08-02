CREATE TABLE t1 (
    k1 INT,
    k2 VARCHAR(100))
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) PROPERTIES('replication_num'='1');
insert into t1 SELECT generate_series, 100000 - generate_series FROM TABLE(generate_series(1, 300000));