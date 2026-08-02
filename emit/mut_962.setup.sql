CREATE TABLE `t0` (
  `c0` int DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` string DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  4));
insert into t0 SELECT generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  409600));
CREATE TABLE `t1` (
  `c0` int DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` string DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);
insert into t1 SELECT generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  409600));
insert into blackhole() select distinct c1,c2,c0 from t1;
insert into blackhole() select c2,sum(c0),c1 from t1 group by c1,c2;
insert into blackhole() select distinct c1,c2,c0 from t1;
insert into blackhole() select sum(c0),c1,c2 from t1 group by c1, c2;
insert into blackhole() select * from t1;
insert into blackhole() select distinct c1,c2 from t1;