CREATE TABLE wkt (
  dt DATE,
  v INT
) DUPLICATE KEY(dt)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO wkt VALUES ('2024-12-30', 1), ('2024-12-31', 1), ('2025-01-01', 1);
CREATE MATERIALIZED VIEW mv_week
REFRESH MANUAL
AS SELECT date_trunc('week', dt) AS wk, sum(v) AS s FROM wkt GROUP BY date_trunc('week', dt);