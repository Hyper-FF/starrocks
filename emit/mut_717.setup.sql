CREATE TABLE `s2` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `a1` array<varchar(65533)> NULL COMMENT "",
  `a2` array<varchar(65533)> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"light_schema_change" = "true",
"compression" = "LZ4"
);
insert into s2 values
(1, 1, null, []);
CREATE TABLE `s2` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `a1` array<varchar(65533)> NULL COMMENT "",
  `a2` array<varchar(65533)> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"light_schema_change" = "true",
"compression" = "LZ4"
);
insert into s2 values
(1, 1, null, []),
(2, 1, null, []),
(3, 1, null, null),
(4, 1, [], null),
(5, 1, [], null),
(6, 1, null, null),
(7, 1, ["e", "f"], ["c", "d"]),
(8, 1, null, null),
(9, 1, ["g", "h"], null),
(10, 1, null, ["a", "b"]);