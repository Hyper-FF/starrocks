CREATE TABLE t_fp (k INT, v INT)
  DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
  PROPERTIES("replication_num" = "1");
INSERT INTO t_fp SELECT generate_series, generate_series FROM TABLE(generate_series(1, 5000));