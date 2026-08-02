CREATE TABLE `t` (
  `id` int(11) NOT NULL COMMENT "",
  `grp` int(11) NOT NULL COMMENT "",
  `str1` varchar(65533) NOT NULL COMMENT "",
  `str2` varchar(65533) NULL COMMENT "",
  `val` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t2` (
  `id` int(11) NOT NULL COMMENT "",
  `grp` int(11) NOT NULL COMMENT "",
  `str1` varchar(65533) NOT NULL COMMENT "",
  `str2` varchar(65533) NULL COMMENT "",
  `val` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t VALUES
    (1,  1, 'apple',  'X', 10),
    (2,  1, 'banana', 'Y', 20),
    (3,  1, 'apple',  'Z', 15),
    (4,  1, 'cherry', 'X', 25),
    (5,  2, 'banana', 'Y', 30),
    (6,  2, 'cherry', 'Z', 35),
    (7,  2, 'apple',  'X', 40),
    (8,  2, 'date',   'Y', 45),
    (9,  3, 'cherry', 'Z', 50),
    (10, 3, 'cherry', 'X', 55),
    (11, 3, 'date',   NULL, 60),
    (12, 3, 'banana', 'Y', 65);
INSERT INTO t2 
SELECT i, i % 5, CAST(i % 5 AS STRING), CAST(i AS STRING), i * 10
FROM TABLE (generate_series(1, 300)) AS g(i);
drop table t;
drop table t2;