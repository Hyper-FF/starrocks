CREATE TABLE `test_json_config` (
  `id` bigint(20) NOT NULL COMMENT "",
  `my_json_col` json NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"flat_json.enable" = "true",
"flat_json.null.factor" = "0.1",
"flat_json.sparsity.factor" = "0.8",
"flat_json.column.max" = "50"
);
INSERT INTO test_json_config VALUES
(1, parse_json('{"key1": "value1", "key2": 100, "key3": true}')),
(2, parse_json('{"key1": "value2", "key2": 200, "key4": "extra"}')),
(3, parse_json('{"key1": "value3", "key2": 300}'));
ALTER TABLE test_json_config SET ("flat_json.null.factor" = "0.2");
INSERT INTO test_json_config VALUES
(4, parse_json('{"key1": "value4", "key2": 400, "key5": "new"}')),
(5, parse_json('{"key1": "value5", "key2": 500}'));
ALTER TABLE test_json_config SET ("flat_json.sparsity.factor" = "0.9");
INSERT INTO test_json_config VALUES
(6, parse_json('{"key1": "value6", "key2": 600, "key6": "sparsity"}')),
(7, parse_json('{"key1": "value7", "key2": 700}'));
ALTER TABLE test_json_config SET ("flat_json.column.max" = "100");
INSERT INTO test_json_config VALUES
(8, parse_json('{"key1": "value8", "key2": 800, "key3": false, "key7": "max"}')),
(9, parse_json('{"key1": "value9", "key2": 900}'));
ALTER TABLE test_json_config SET ("flat_json.enable" = "false");
INSERT INTO test_json_config VALUES
(10, parse_json('{"key1": "value10", "key2": 1000, "key8": "disabled"}')),
(11, parse_json('{"key1": "value11", "key2": 1100}'));
ALTER TABLE test_json_config SET ("flat_json.enable" = "true");
INSERT INTO test_json_config VALUES
(12, parse_json('{"key1": "value12", "key2": 1200, "key9": "reenabled"}')),
(13, parse_json('{"key1": "value13", "key2": 1300}'));
ALTER TABLE test_json_config SET ("flat_json.enable" = "false");
INSERT INTO test_json_config VALUES
(14, parse_json('{"key1": "value14", "key2": 1400, "key10": "disabled2"}')),
(15, parse_json('{"key1": "value15", "key2": 1500}'));
ALTER TABLE test_json_config SET ("flat_json.enable" = "true");
INSERT INTO test_json_config VALUES
(16, parse_json('{"key1": "value16", "key2": 1600, "key11": "final"}')),
(17, parse_json('{"key1": "value17", "key2": 1700}'));
DROP TABLE test_json_config;