create table test_tbl_with_params (
 k1 INT,
  k2 string,
  v1 INT,
  v2 INT
) ENGINE=OLAP
PARTITION BY RANGE(`k1`)
(
  PARTITION `p1` VALUES LESS THAN ('3'),
  PARTITION `p2` VALUES LESS THAN ('6'),
  PARTITION `p3` VALUES LESS THAN ('9')
)
DISTRIBUTED BY HASH(`k1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
insert into test_tbl_with_params values (1,'a',1,1), (4,'aa',1,1), (8,'aa',1,1);
CREATE MATERIALIZED VIEW test_tbl_with_params_mv3
PARTITION BY k1
REFRESH DEFERRED MANUAL 
properties (
    "partition_refresh_number" = "1"
)
AS SELECT * from test_tbl_with_params;