CREATE TABLE `sc2` (
  `v1` bigint(20) NULL COMMENT "",
  `s2` string NULL,
  `array1` ARRAY<INT> NULL,
  `array2` ARRAY<MAP<INT, INT>> NULL,
  `array3` ARRAY<STRUCT<a INT, b INT>> NULL,
  `map1` MAP<INT, INT> NULL,
  `map2` MAP<INT, ARRAY<INT>> NULL,
  `map3` MAP<INT, STRUCT<c INT, b INT>> NULL,
  `st1` STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into sc2 values (0, "abc", [1,2,3], [map{2:20, 1:10, 3:30}], [row(1, 2)], map{2:20, 1:10, 4:40}, map{2:[2,3,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)));
insert into sc2 values (1, "abc", [2,1,3], [map{2:20, 1:10, 3:30}], [row(1, 3)], map{2:20, 1:10, 4:40}, map{2:[2,3,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)));
insert into sc2 values (2, "abc", [1,3,2], [map{2:20, 1:10, 3:30}], [row(1, 2)], map{2:20, 1:10, 3:30}, map{2:[2,3,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)));
insert into sc2 values (3, "abc", [1,2,3], [map{2:20, 1:10}],       [row(1, 2)], map{2:20, 1:10, 3:30}, map{2:[3,2,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)));
insert into sc2 values (4, "abc", [1,2,3], [map{2:20, 1:10}],       [row(1, 3)], map{2:20, 1:10, 3:30}, map{2:[3,2,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)));
insert into sc2 values (5, "abd", [1,2,3], [map{1:10, 2:20}],       [row(1, 2)], map{1:10, 2:20, 3:30}, map{1:[1,2,3], 2:[2,3,4]}, map{1:row(1,3), 2:row(2,3)}, row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)));
insert into sc2 values (6, "abd", [1,3,2], [map{1:10, 2:20}],       [row(1, 2)], map{1:10, 2:20, 4:40}, map{1:[2,1,3], 2:[2,3,4]}, map{1:row(2,3), 2:row(2,3)}, row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)));
insert into sc2 values (7, "abd", [2,1,3], [map{1:10, 3:30, 2:20}], [row(2, 1)], map{1:10, 2:20, 4:40}, map{1:[2,1,3], 2:[2,4,3]}, map{1:row(2,3), 2:row(2,4)}, row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)));
insert into sc2 values (8, "abd", [2,1,3], [map{1:10, 3:30, 2:20}], [row(2, 1)], map{2:20, 1:10, 3:30}, map{1:[1,2,3], 2:[2,4,3]}, map{1:row(2,3), 2:row(2,4)}, row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)));
insert into sc2 values (9, "abd", [1,2,3], [map{1:10, 3:30, 2:20}], [row(1, 2)], map{1:10, 2:20, 3:30}, map{1:[1,2,3], 2:[2,4,3]}, map{1:row(1,3), 2:row(2,4)}, row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)));
insert into sc2 values (0, "abc", [1,2,null], [map{2:20, 1:10, null:30}], [row(1, 2)], map{2:20, 1:10, 4:40}, map{2:[2,null,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)));
insert into sc2 values (1, "abc", [2,1,null], [map{2:20, 1:10, null:30}], [row(1, null)], map{2:20, 1:10, 4:40}, map{2:[2,null,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)));
insert into sc2 values (2, "abc", [1,null,2], [map{2:20, 1:10, null:30}], [row(1, 2)], map{2:20, 1:10, null:30}, map{2:[2,null,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)));
insert into sc2 values (3, "abc", [1,2,null], [map{2:20, 1:10}],       [row(1, 2)], map{2:20, 1:10, null:30}, map{2:[null,2,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)));
insert into sc2 values (4, "abc", [1,2,null], [map{2:20, 1:10}],       [row(1, null)], map{2:20, 1:10, null:30}, map{2:[null,2,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)));
insert into sc2 values (5, "abd", [1,2,null], [map{1:10, 2:20}],       [row(1, 2)], map{1:10, 2:20, null:30}, map{1:[1,2,null], 2:[2,null,4]}, map{1:row(1,null), 2:row(2,null)}, row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)));
insert into sc2 values (6, "abd", [1,null,2], [map{1:10, 2:20}],       [row(1, 2)], map{1:10, 2:20, 4:40}, map{1:[2,1,null], 2:[2,null,4]}, map{1:row(2,null), 2:row(2,null)}, row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)));
insert into sc2 values (7, "abd", [2,1,null], [map{1:10, null:30, 2:20}], [row(2, 1)], map{1:10, 2:20, 4:40}, map{1:[2,1,null], 2:[2,4,null]}, map{1:row(2,null), 2:row(2,4)}, row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)));
insert into sc2 values (8, "abd", [2,1,null], [map{1:10, null:30, 2:20}], [row(2, 1)], map{2:20, 1:10, null:30}, map{1:[1,2,null], 2:[2,4,null]}, map{1:row(2,null), 2:row(2,4)}, row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)));
insert into sc2 values (9, "abd", [1,2,null], [map{1:10, null:30, 2:20}], [row(1, 2)], map{1:10, 2:20, null:30}, map{1:[1,2,null], 2:[2,4,null]}, map{1:row(1,null), 2:row(2,4)}, row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)));