CREATE TABLE `TABLE_EVENT` (
  `CG_group6` int(11) NOT NULL COMMENT "",
  `ID` bigint(20) NOT NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`CG_group6`, `ID`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`CG_group6`) BUCKETS 20
PROPERTIES (
  "replication_num" = "1"
);
INSERT INTO TABLE_EVENT SELECT generate_series, generate_series FROM TABLE(generate_series(1, 200000));
CREATE TABLE `TABLE_OBJECT` (
  `CG_group3` int(11) NOT NULL COMMENT "",
  `ID` bigint(20) NOT NULL COMMENT "",
  `ARRAY_LARGEINT` array<largeint> NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`CG_group3`, `ID`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`CG_group3`) BUCKETS 20
PROPERTIES (
  "replication_num" = "1"
);
INSERT INTO TABLE_OBJECT SELECT generate_series, generate_series, [1, 2, 3, 4, 5] FROM TABLE(generate_series(1, 1000000));