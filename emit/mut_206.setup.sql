CREATE TABLE `t1` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 5
PROPERTIES (
"colocate_with" = "group1",
"replication_num" = "1",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
CREATE TABLE `t2` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 5
PROPERTIES (
"colocate_with" = "group1",
"replication_num" = "1",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
CREATE TABLE `t3` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 5
PROPERTIES (
"colocate_with" = "group1",
"replication_num" = "1",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
CREATE TABLE `t4` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 5
PROPERTIES (
"colocate_with" = "group1",
"replication_num" = "1",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t1 (c0, c1, c2, c3) VALUES
  (1, 'a', 'Value1', 10),
  (2, 'b', 'Value2', 20),
  (null, 'c', 'Value3', 30),
  (4, 'd', 'Value4', 40),
  (5, null, 'Value5', 50),
  (5, 'f', 'Value6', 60),
  (8, 'h', 'Value7', 70),
  (8, 'h', 'Value8', 80),
  (null, null, 'Value9', 90),
  (10, 'j', 'Value10', 100),
  (null, 'k', 'Value11', 110),
  (12, 'l', 'Value12', 120),
  (12, 'l', 'Value13', 130),
  (14, 'l', 'Value14', 140),
  (15, 'o', 'Value15', 150);
INSERT INTO t2 (c0, c1, c2, c3) VALUES
  (null, null, 'Value1', 10),
  (2, 'b', 'Value2', 20),
  (null, 'c', 'Value3', 30),
  (4, 'd', 'Value4', 40),
  (5, null, 'Value5', 50),
  (5, 'f', 'Value6', 60),
  (5, 'h', 'Value7', 70),
  (8, 'h', 'Value8', 80),
  (8, null, 'Value9', 90),
  (10, 'j', 'Value10', 100),
  (null, 'k', 'Value11', 110),
  (12, 'l', 'Value12', 120),
  (12, 'l', 'Value13', 130),
  (14, 'm', 'Value14', 140),
  (null, 'o', 'Value15', 150);
INSERT INTO t3 (c0, c1, c2, c3) VALUES
  (1, 'a', 'Value1', 10),
  (2, 'b', 'Value2', 20),
  (3, 'c', 'Value3', 30),
  (3, 'd', 'Value4', 40),
  (5, null, 'Value5', 50),
  (null, 'f', 'Value6', 60),
  (5, 'h', 'Value7', 70),
  (8, 'h', 'Value8', 80),
  (8, null, 'Value9', 90),
  (10, 'j', 'Value10', 100),
  (null, 'k', 'Value11', 110),
  (12, 'l', 'Value12', 120),
  (12, 'l', 'Value13', 130),
  (14, 'm', 'Value14', 140),
  (null, 'o', 'Value15', 150);
INSERT INTO t4 (c0, c1, c2, c3) VALUES
  (1, null, 'Value1', 10),
  (2, 'b', 'Value2', 20),
  (null, 'c', 'Value3', 30),
  (4, 'd', 'Value4', 40),
  (5, null, 'Value5', 50),
  (5, 'f', 'Value6', 60),
  (5, 'h', 'Value7', 70),
  (8, 'h', 'Value8', 80),
  (8, null, 'Value9', 90),
  (10, 'j', 'Value10', 100),
  (null, 'k', 'Value11', 110),
  (12, 'l', 'Value12', 120),
  (12, null, 'Value13', 130),
  (14, 'm', 'Value14', 140),
  (null, 'o', 'Value15', 150);