CREATE TABLE t_avgd (
  k INT,
  x INT
) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1");
INSERT INTO t_avgd VALUES (1, 10), (1, 10), (1, 40);
CREATE MATERIALIZED VIEW mv_avgd
REFRESH MANUAL
AS SELECT k, sum(x) AS s, count(x) AS c FROM t_avgd GROUP BY k;