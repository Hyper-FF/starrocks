CREATE TABLE test_json_scalar (
    id INT,
    json_data JSON,
    description VARCHAR(100)
)
ENGINE=OLAP 
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO test_json_scalar VALUES
(1, parse_json('123'), 'number'),
(2, parse_json('"hello"'), 'string'),
(3, parse_json('true'), 'boolean'),
(4, json_query(parse_json('{"a": null}'), '$.a'), 'json null'),
(5, parse_json('{}'), 'empty object'),
(6, parse_json('{"key": "value"}'), 'object'),
(7, parse_json('[]'), 'empty array'),
(8, parse_json('[1, 2, 3]'), 'array'),
(9, NULL, 'sql null'),
(10, parse_json('3.14159'), 'float');