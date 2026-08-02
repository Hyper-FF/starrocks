CREATE TABLE t0 (
  k1 bigint null
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 48
PROPERTIES (
    "replication_num" = "1"
);
insert into t0 select generate_series from TABLE(generate_series(0, 10000 - 1));
insert into t0 select k1 + 20000 from t0;
insert into t0 select k1 + 40000 from t0;
insert into t0 select k1 + 80000 from t0;