CREATE TABLE t1 (
  k1 bigint NULL,
  c1 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE t2 (
  k1 bigint NULL,
  c1 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 select generate_series, generate_series from TABLE(generate_series(0, 100 - 1));
insert into t2 select * from t1;