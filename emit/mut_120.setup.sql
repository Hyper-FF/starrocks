CREATE TABLE `array_map_test` (
  `id` tinyint(4) NOT NULL COMMENT "",
  `arr_str` array<string> NULL COMMENT "",
  `arr_largeint` array<largeint> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY RANDOM
PROPERTIES (
"replication_num" = "1"
);
insert into array_map_test values (1, array_repeat("abcdefghasdasdasirnqwrq", 20000), array_repeat(100, 20000));
CREATE TABLE `t` (
  `k` bigint NOT NULL COMMENT "",
  `arr_0` array<bigint> NOT NULL COMMENT "",
  `arr_1` array<bigint> NULL COMMENT "",
  `arr_2` array<bigint> NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t values (1, [1,2], [1,2],[2,3]), (2, [1,2], null, [2,3]), (3, [1,2],[1,2],null),(4, [1,2],[null,null],[2,3]), (5, [1], [1,2], [3]);
CREATE TABLE `array_map_x` (
  `id` tinyint(4) NOT NULL COMMENT "",
  `arr_str` array<varchar(65533)> NULL COMMENT "",
  `arr_largeint` array<largeint(40)> NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`id`)
DISTRIBUTED BY RANDOM
PROPERTIES (
"replication_num" = "1"
);
insert into array_map_x values (1, array_repeat("abcdefghasdasdasirnqwrq", 2), array_repeat(100, 2));