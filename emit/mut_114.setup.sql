CREATE TABLE `t0` (
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT "",
  `v4` varchar(50) NULL COMMENT "",
  `v5` decimal(10,2) NULL COMMENT "",
  `v6` float NULL COMMENT ""
) ENGINE=OLAP
PROPERTIES("replication_num"="1");
insert into t0 select i/19 as v1, i/11 as v2, i/5 as v3, concat('item_', cast(i as varchar)) as v4, i/3.14 as v5, i/2.71 as v6 from table(generate_series(1,100)) t(i);
CREATE TABLE `t1` (
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT "",
  `v4` varchar(50) NULL COMMENT "",
  `v5` decimal(10,2) NULL COMMENT "",
  `v6` float NULL COMMENT ""
) ENGINE=OLAP
PROPERTIES("replication_num"="1");
insert into t1 values
  (1, 1, 100, 'a', 1.1, 1.1),
  (1, 2, NULL, 'b', 2.2, NULL),
  (1, NULL, 300, NULL, 3.3, 3.3),
  (NULL, 4, 400, 'd', NULL, 4.4),
  (2, 1, 500, 'e', 5.5, 5.5),
  (2, NULL, NULL, NULL, NULL, NULL),
  (2, 3, 700, 'g', 7.7, 7.7),
  (NULL, NULL, NULL, NULL, NULL, NULL),
  (3, 1, 900, 'i', 9.9, 9.9),
  (3, 2, 1000, 'j', 10.1, 10.1);