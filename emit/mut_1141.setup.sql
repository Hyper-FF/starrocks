CREATE TABLE t1 (
    k1 INT,
    k2 VARCHAR(20))
DUPLICATE KEY(k1)
PROPERTIES('replication_num'='1');
insert into t1 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  40960));
create table t2 (
    c0 INT,
    c1 BIGINT NOT NULL
) DUPLICATE KEY(c0) DISTRIBUTED BY RANDOM BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t2 SELECT generate_series, 650000 - generate_series FROM TABLE(generate_series(1,  650000));
insert into blackhole() select c0, sum(c1) from t2 group by c0;