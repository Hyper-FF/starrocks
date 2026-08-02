CREATE TABLE `ptest` (
  `k1` int(11) NOT NULL COMMENT "",
  `d2` date NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `v3` int(11) NULL COMMENT "",
  `s1` varchar(255) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k1`, `d2`)
COMMENT "OLAP"
PARTITION BY RANGE(`d2`)
(PARTITION p202001 VALUES [('1000-01-01'), ('2020-01-01')),
PARTITION p202004 VALUES [('2020-01-01'), ('2020-04-01')),
PARTITION p202007 VALUES [('2020-04-01'), ('2020-07-01')),
PARTITION p202008 VALUES [('2020-07-01'), ('2020-07-29')),
PARTITION p202012 VALUES [('2020-08-01'), (MAXVALUE)))
DISTRIBUTED BY HASH(`k1`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `ptest_expr` (
  `k1` int(11) NOT NULL COMMENT "",
  `d2` date NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `v3` int(11) NULL COMMENT "",
  `s1` varchar(255) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k1`, `d2`)
COMMENT "OLAP"
PARTITION BY date_trunc('month', d2)
DISTRIBUTED BY HASH(`k1`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);
insert into ptest (k1, d2) values (1, '1000-01-01'), (2, '1000-01-02'), (3, '1100-05-06'), (4, '1500-06-06'), (5, '1999-07-08'),
(6, '2020-01-01'), (7, '2020-01-04'), (8, '2020-02-06'), (9, '2020-03-21'), (10, '2020-03-31'), (11, '2020-04-01'),
(12, '2020-04-26'), (13, '2020-06-30'), (14, '2020-07-01'), (14, '2050-01-05');
insert into ptest_expr (k1, d2) values (1, '1000-01-01'), (2, '1000-01-02'), (3, '1100-05-06'), (4, '1500-06-06'), (5, '1999-07-08'),
(6, '2020-01-01'), (7, '2020-01-04'), (8, '2020-02-06'), (9, '2020-03-21'), (10, '2020-03-31'), (11, '2020-04-01'),
(12, '2020-04-26'), (13, '2020-06-30'), (14, '2020-07-01'), (14, '2050-01-05'), (15, null), (16, null);