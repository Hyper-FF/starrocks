-- name: test_mv_rewrite_case_when_sum_widening_type
-- Regression test for sync-MV rewrite of `sum(case when ... then col else 0 end)`
-- when the MV stores `sum(col)` and the aggregator widens the column's type.
-- Before the fix the rewritten plan kept the inner CASE WHEN / IF declared
-- as the original (narrower) column type while the THEN branch was already
-- the widened MV column, so any group whose sum exceeded the original type's
-- range would be silently truncated at the outer SUM.
create database db_${uuid0};
use db_${uuid0};

CREATE TABLE t1 (
    k1 DATE NULL,
    k2 INT  NULL,
    k3 SMALLINT NULL
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES("replication_num" = "1");

CREATE MATERIALIZED VIEW test_mv1 AS
  SELECT k1, k2, sum(k3) AS sum1 FROM t1 GROUP BY k1, k2;
function: wait_materialized_view_finish()

-- Per-(k1, k2) sum(k3) deliberately overflows SMALLINT (max 32767):
--   for ('2024-01-01', k2=0) the sum is 30000+30000+30000+10000 = 100000.
INSERT INTO t1 VALUES
    ('2024-01-01', 0, 30000),
    ('2024-01-01', 0, 30000),
    ('2024-01-01', 0, 30000),
    ('2024-01-01', 0, 10000),
    ('2024-01-01', 1, 5);

-- The query rewrites `case when k2=0 then k3 else 0` against test_mv1's
-- mv_sum_k3 column.  After the fix, the rewritten IF/CASE WHEN result type
-- is BIGINT (matching mv_sum_k3) so the outer sum is not truncated.
SELECT k1,
       sum(k3) AS sum1,
       sum(case when k2=0 then k3 else 0 end) AS sum2
FROM t1 GROUP BY k1;

-- Same shape, expressed with IF instead of CASE WHEN.
SELECT k1,
       sum(if(k2=0, k3, 0)) AS sum_if
FROM t1 GROUP BY k1;

DROP DATABASE db_${uuid0};
