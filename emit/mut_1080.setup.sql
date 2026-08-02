CREATE TABLE `js1` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `j1` json NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 10 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"fast_schema_evolution" = "true",
"compression" = "LZ4"
);
insert into js1 values
(1,1, parse_json('[{"s1": 4}, {"s2": 5}]')),
(2,2, parse_json('"a"')),
(3,3, parse_json('1')),
(4,4, parse_json('2020-12-12')),
(5,5, parse_json('1.000000')),
(6,6, parse_json('')),
(6,7, parse_json(null)),
(6,8, parse_json(TRUE)),
(7,9, parse_json('{"k1": null, "k2": 2}')),
(8,8, json_object('1')),
(9,9, json_object('"a"')),
(10,10, json_object('')),
(11,11, json_object()),
(12,12, json_object(null));