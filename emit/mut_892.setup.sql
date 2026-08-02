drop table if exists t1;
CREATE TABLE t1 (
  dt datetime,
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `c0`, `c1`)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 48
PROPERTIES (
  "replication_num" = "1"
);
insert into t1 SELECT '2026-01-01', generate_series % 10, generate_series, generate_series, generate_series FROM TABLE(generate_series(1, 100));
CREATE MATERIALIZED VIEW test_mv1 
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY hash(c0) 
PROPERTIES (
  "replication_num" = "1",
  "query_rewrite_consistency" = "force_mv"
)
AS 
SELECT dt, c0, count(c1) as cnt_c1 from t1 group by dt, c0;
INSERT INTO t1 VALUES ('2026-01-01', 0, "c1_0", "c2_0", 0);
ALTER TABLE t1 ADD COLUMN c4 int;
INSERT INTO t1 VALUES ('2026-01-01', 0, "c1_0", "c2_0", 0, 0);
INSERT INTO t1 VALUES ('2026-01-01', 0, "c1_0", "c2_0", 0, 0);
INSERT INTO t1 VALUES ('2026-01-01', 0, "c1_0", "c2_0", 0, 0);
INSERT INTO t1 VALUES ('2026-01-01', 0, "c1_0", "c2_0", 0, 0);
INSERT INTO t1 VALUES ('2026-01-01', 0, "c1_0", "c2_0", 0, 0);
INSERT INTO t1 VALUES ('2026-01-01', 0, "c1_0", "c2_0", 0, 0);
DROP TABLE t1;
CREATE TABLE t2 (
  dt datetime,
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `c0`, `c1`)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(`c0`, `c1`) 
PROPERTIES (
"replication_num" = "1"
);
insert into t2 SELECT '2026-01-01', generate_series % 5, generate_series, generate_series, generate_series FROM TABLE(generate_series(1, 100));
CREATE MATERIALIZED VIEW test_mv2 
PARTITION BY date_trunc('day', dt)  
DISTRIBUTED BY HASH(`c0`, `c1`) 
AS 
SELECT dt, c0, c1 from t2;
ALTER TABLE t2 ADD COLUMN c4 int;
INSERT INTO t2 VALUES ('2026-01-01', 0, "c1_0", "c2_0", 0, 0);
DROP TABLE t2;