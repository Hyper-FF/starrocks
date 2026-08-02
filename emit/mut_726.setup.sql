CREATE TABLE `json_test_table` (
  `id` int(11) NOT NULL COMMENT "",
  `json_data` json ,
  `json_array` json ,
  `json_nested` json,
  `sort_key` varbinary(1024) AS (
    encode_sort_key(
      get_json_int(json_data, '$.age'),
      get_json_string(json_data, '$.name'),
      get_json_string(json_data, '$.city'),
      get_json_string(json_array, '$[0]'),
      get_json_double(json_nested, '$.user.profile.score')
    )
  ) COMMENT "Auto-generated sort key from extracted JSON fields"
) ENGINE=OLAP 
DISTRIBUTED BY HASH(sort_key) BUCKETS 2
ORDER BY (sort_key)
PROPERTIES ( "replication_num" = "1");
INSERT INTO json_test_table (id, json_data, json_array, json_nested) VALUES
(1, parse_json('{"name": "Alice", "age": 25, "city": "New York"}'), 
     parse_json('["apple", "banana", "cherry"]'),
     parse_json('{"user": {"id": 101, "profile": {"verified": true, "score": 95.5}}}')),
(2, parse_json('{"name": "Bob", "age": 30, "city": "Los Angeles"}'), 
     parse_json('["orange", "grape"]'),
     parse_json('{"user": {"id": 102, "profile": {"verified": false, "score": 87.2}}}')),
(3, parse_json('{"name": "Charlie", "age": 28, "city": "Chicago"}'), 
     parse_json('["mango", "pineapple", "kiwi", "strawberry"]'),
     parse_json('{"user": {"id": 103, "profile": {"verified": true, "score": 92.8}}}')),
(4, parse_json('{"name": "Diana", "age": 22, "city": "Miami"}'), 
     parse_json('["pear"]'),
     parse_json('{"user": {"id": 104, "profile": {"verified": true, "score": 89.1}}}')),
(5, parse_json('{"name": "Eve", "age": 35, "city": "Seattle"}'), 
     parse_json('["blueberry", "raspberry", "blackberry"]'),
     parse_json('{"user": {"id": 105, "profile": {"verified": false, "score": 78.9}}}'));
INSERT INTO json_test_table (id, json_data, json_array, json_nested) VALUES
(6, NULL, parse_json('["test"]'), parse_json('{"test": null}')),
(7, parse_json('{"age": 40}'), parse_json('[]'), parse_json('{"user": {"id": 106}}'));