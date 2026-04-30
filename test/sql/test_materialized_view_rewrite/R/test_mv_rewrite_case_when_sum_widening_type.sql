-- name: test_mv_rewrite_case_when_sum_widening_type
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE TABLE t1 (
    k1 DATE NULL,
    k2 INT  NULL,
    k3 SMALLINT NULL
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
CREATE MATERIALIZED VIEW test_mv1 AS
  SELECT k1, k2, sum(k3) AS sum1 FROM t1 GROUP BY k1, k2;
-- result:
-- !result
function: wait_materialized_view_finish()
-- result:
None
-- !result
INSERT INTO t1 VALUES
    ('2024-01-01', 0, 30000),
    ('2024-01-01', 0, 30000),
    ('2024-01-01', 0, 30000),
    ('2024-01-01', 0, 10000),
    ('2024-01-01', 1, 5);
-- result:
-- !result
SELECT k1,
       sum(k3) AS sum1,
       sum(case when k2=0 then k3 else 0 end) AS sum2
FROM t1 GROUP BY k1;
-- result:
2024-01-01	100005	100000
-- !result
SELECT k1,
       sum(if(k2=0, k3, 0)) AS sum_if
FROM t1 GROUP BY k1;
-- result:
2024-01-01	100000
-- !result
DROP DATABASE db_${uuid0};
-- result:
-- !result
