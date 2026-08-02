drop table if exists t1;
CREATE TABLE t1 (
    k1 int,
    k2 int
) DUPLICATE KEY(k1)
properties (
    "replication_num" = "1"
);
INSERT INTO t1 VALUES (1,1),(1,2),(null,null);
CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL
properties (
    "replication_num" = "1",
    "mv_rewrite_staleness_second" = "10"
)
 AS SELECT k1,sum(k2) FROM t1 group by k1;
INSERT INTO t1 VALUES (2,2);