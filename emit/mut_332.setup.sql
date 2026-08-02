CREATE TABLE `t` (
  `v1` bigint NOT NULL COMMENT "",
  `v2` bigint NULL COMMENT "",
  `v3` bigint NULL COMMENT "",
  `v4` array<string> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
DISTRIBUTED BY RANDOM
PROPERTIES (
"replication_num" = "1"
);
insert into t select generate_series, generate_series, generate_series, array_repeat(cast(generate_series as string), 5) from table(generate_series(1, 100));
CREATE TABLE `t0` (
  `k` bigint(20) NOT NULL COMMENT "",
  `v1` array<bigint(20)> NULL COMMENT "",
  `v2` array<bigint(20)> NULL COMMENT "",
  `v3` array<bigint(20)> NULL COMMENT "",
  `v4` struct<a int(11), b struct<a int(11)>> NULL COMMENT "",
  `v5` struct<a int(11), b struct<a array<bigint(20)>>> NULL COMMENT "",
  `v6` map<int(11),int(11)> NULL COMMENT "",
  `v7` map<int(11),int(11)> NULL COMMENT "",
  `v8` json NULL COMMENT "",
  `v9` json NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
insert into t0 values 
(1,[1],[1],[1],row(1,row(1)),row(1,row([1])),map{1:1},map{1:1,2:2},parse_json('{"a":{"b":1}}'),parse_json('{"a":1,"b":[1]}')),
(2,[2],[2],[2],row(2,row(2)),row(2,row([2])),map{1:1},map{1:2,2:2},parse_json('{"a":{"b":2}}'),parse_json('{"a":1,"b":[1]}')),
(3,[3],[3],[3],row(3,row(3)),row(3,row([3])),map{1:1},map{1:3,2:2},parse_json('{"a":{"b":3}}'),parse_json('{"a":1,"b":[1]}')),
(4,[4],[4],[4],row(4,row(4)),row(4,row([4])),map{1:1},map{1:4,2:2},parse_json('{"a":{"b":4}}'),parse_json('{"a":1,"b":[1]}')),
(5,[5],[5],[5],row(5,row(5)),row(5,row([5])),map{1:1},map{1:5,2:2},parse_json('{"a":{"b":5}}'),parse_json('{"a":1,"b":[1]}'));