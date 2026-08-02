CREATE TABLE `tab1` (
  `k0` int(11) NOT NULL COMMENT "",
  `k1` int(11) NULL COMMENT "",
  `k2` int(11) NULL COMMENT "",
  `k3` int(11) NULL COMMENT "",
  `k4` int(11) NULL COMMENT "",
  `k5` int(11) NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k0`)
DISTRIBUTED BY HASH(`k0`) BUCKETS 1
ORDER BY(`k3`, `k2`)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO `tab1` (`k0`, `k1`, `k2`, `k3`, `k4`, `k5`, `v1`, `v2`) VALUES (1, 1, 1, 1, 1, 1, 1, 1), (2, 2, 2, 2, 2, 2, 2, 2), (3, 3, 3, 3, 3, 3, 3, 3), (4, 4, 4, 4, 4, 4, 4, 4);