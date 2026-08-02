CREATE TABLE t1 (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 48
PROPERTIES (
"replication_num" = "1"
);
insert into t1 SELECT generate_series % 100, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  4096));
CREATE MATERIALIZED VIEW test_mv1 
DISTRIBUTED BY hash(c0, c1) 
AS 
SELECT * from t1;
CREATE INDEX idx1 ON test_mv1(c0) USING BITMAP COMMENT 'bitmap index on c0';
CREATE INDEX idx2 ON test_mv1(c1) USING BITMAP COMMENT 'bitmap index on c1';
insert into t1 SELECT generate_series % 100, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  4096));
DROP INDEX idx2 ON test_mv1;
insert into t1 SELECT generate_series % 100, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  4096));