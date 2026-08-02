CREATE TABLE `js1` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `j1` json NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"fast_schema_evolution" = "true",
"compression" = "LZ4"
);
insert into js1 values 
(1,  11,  parse_json('{"a": true,   "b": 1, "c": "qwe",    "d": 1210.010,  "e":  [1,2,3,4]}')),
(2,  22,  parse_json('{"a": false,  "b": 2, "c": "asdf",   "d": 2220.020,  "e":  {"e1": 1, "e2": 3}}')),
(3,  33,  parse_json('{"a": true,   "b": 3, "c": "qcve",   "d": 3230.030,  "e":  "asdf"}')),
(4,  44,  parse_json('{"a": true,   "b": 4, "c": "234.234",    "d": 4240.040,  "e":  123123}')),
(5,  55,  parse_json('{"a": true,   "b": 5, "c": "1233",    "d": 5250.050,  "e":  "zxcvzvxc"}')),
(6,  66,  parse_json('{"a": true,   "b": 6, "c": "0",   "d": 6260.060,  "e":  null}')),
(7,  77,  parse_json('{"a": false,  "b": 7, "c": "vTp49OF2Ezc0RWJVN",  "d": 7270.070,  "e":  {"e1": 4, "e2": 5}}')),
(8,  88,  parse_json('{"a": true,   "b": 8, "c": "qw1e",   "d": 8280.080,  "e":  "0.000"}')),
(9,  99,  parse_json('{"a": false,  "b": 0, "c": "q123",   "d": 0.000,  "e":  true}')),
(10, 101, parse_json('{"a": false,  "b": -1, "c": "true",   "d": -1.123,  "e":  "false"}')),
(11, 102, parse_json('{"a": false,  "b": 9, "c": "false",  "d": 9290.090,  "e":  [1,2,3,4]}'));
CREATE TABLE `js2` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `j1` json NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"fast_schema_evolution" = "true",
"compression" = "LZ4"
);
insert into js2 values 
(1,  11,  parse_json('{"a": true,   "b": 1.123, "c": "qwe",    "d": "asdfsfd",  "e":  [1,2,3,4]}')),
(2,  22,  parse_json('{"a": false,  "b": 2,     "c": "asdf",   "d": 2220.020,  "e":  {"e1": 1, "e2": 3}}')),
(3,  33,  parse_json('{"a": 3,      "b": 3,     "c": "qcve",   "d": 3230.030,  "e":  "asdf"}')),
(4,  44,  parse_json('{"a": 5,      "b": 4,     "c": "qre",    "d": 4240.040,  "e":  123123}')),
(5,  55,  parse_json('{"a": true,   "b": 5,     "c": "eeeasdfasdfsafsfasdfasdfasdfasfd",    "d": 5250.050,  "e":  "zxcvzvxc"}')),
(6,  66,  parse_json('{"a": 6,      "b": 6.465, "c": "aqwe",   "d": 6260.060,  "e":  null}')),
(7,  77,  parse_json('{"a": false,  "b": 7,     "c": "qwxve",  "d": 7270.070,  "e":  {"e1": 4, "e2": 5}}')),
(8,  88,  parse_json('{"a": true,   "b": 8,     "c": "qw1e",   "d": 8280.080,  "e":  [1,2,3,4]}')),
(9,  99,  parse_json('{"a": false,  "b": 9,     "c": [1,23,456],   "d": 9290.090,  "e":  [1,2,3,4]}')),
(10, 101, parse_json('{"a": false,  "b": 9, "c": "true",   "d": 9290.090,  "e":  [1,2,3,4]}')),
(11, 102, parse_json('{"a": false,  "b": 9, "c": "false",  "d": 9290.090,  "e":  [1,2,3,4]}'));
CREATE TABLE `js3` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `j1` json NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"fast_schema_evolution" = "true",
"compression" = "LZ4"
);
insert into js3 values 
(1, 21, parse_json('{"a": null, "b": null,  "c": null, "e": null}')),
(2, 22, parse_json('{"a": 123,  "b": "avx", "c": true, "e": null}')),
(3, 23, parse_json('{"a": 234,  "b": "sse", "c": false, "e": null}'));