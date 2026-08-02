create table olap_table (
    k1 int,
    k2 int,
    v1 string
)
duplicate key(k1)
distributed by hash(k1) buckets 3
properties("replication_num" = "1");
insert into olap_table values
(1, 10, 'a'),
(2, 20, 'b'),
(3, 30, 'c'),
(4, 40, 'd'),
(5, 50, 'e');
CREATE TABLE `t0` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));