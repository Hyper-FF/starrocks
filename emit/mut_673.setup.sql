CREATE TABLE t1 (
  k1 bigint NULL,
  c1 bigint
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 6
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 select generate_series, generate_series from TABLE(generate_series(0, 10000 - 1));