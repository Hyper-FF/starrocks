CREATE TABLE `lineitem` (
  `l_orderkey` int(11) NOT NULL COMMENT "",
  `l_partkey` int(11) NOT NULL COMMENT "",
  `l_suppkey` int(11),
  `l_shipdate` date
) ENGINE=OLAP
DUPLICATE KEY(`l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
);
insert into lineitem values (1,1,1,'2000-01-01'),(1,2,1,'2000-01-01'),(1,3,2,'2000-01-02'),(11,1,11,'2000-01-01'),(11,2,1,'2000-01-02'),(2,3,2,'2000-01-03'),(2,3,null,null);