create table t0 (
    c0 string
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t0 SELECT concat("s_", generate_series) FROM TABLE(generate_series(0,  80000));
insert into t0 SELECT * FROM t0;
insert into t0 SELECT * FROM t0;
insert into t0 SELECT * FROM t0;
insert into t0 SELECT * FROM t0;
insert into t0 SELECT * FROM t0;
insert into t1 select * from t0 order by c0;
CREATE TABLE `test003` (
  `data_date` date NOT NULL COMMENT "",
  `user_id` varchar(128) NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`data_date`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`data_date`)
PROPERTIES (
"replication_num" = "1"
);
insert into test003 values("2025-08-12", NULL);