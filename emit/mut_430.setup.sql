CREATE TABLE t_hll (k INT, b VARBINARY)
  DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
  PROPERTIES("replication_num" = "1");
INSERT INTO t_hll SELECT generate_series, unhex("0201000000FFFF01") FROM TABLE(generate_series(1, 20000));