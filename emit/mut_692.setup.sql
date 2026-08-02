CREATE TABLE `js7` (
  `v1` int(11) NOT NULL COMMENT "",
  `js` json NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`v1`)
COMMENT "olap"
DISTRIBUTED BY HASH(`v1`) BUCKETS 1 
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into js7 values
(1, parse_json('{\"job\": \"Designer, television/film set\", \"company1\": \"Rodriguez-Meadows1\", \"ssn\": \"192-89-6614\", \"mail\": \"zbarnes@gmail.com\", \"birthdate\": \"1914-03-20\"}')),
(2, parse_json('{\"job\": \"Designer, television/film set\", \"company2\": \"Rodriguez-Meadows1\", \"ssn\": \"192-89-6614\", \"mail\": \"zbarnes@gmail.com\", \"birthdate\": \"1914-03-20\"}')),
(3, parse_json('{\"job\": \"Designer, television/film set\", \"company3\": \"Rodriguez-Meadows1\", \"ssn\": \"192-89-6614\", \"mail\": \"zbarnes@gmail.com\", \"birthdate\": \"1914-03-20\"}')),
(4, parse_json('{\"job\": \"Designer, television/film set\", \"company4\": \"Rodriguez-Meadows1\", \"ssn\": \"192-89-6614\", \"mail\": \"zbarnes@gmail.com\", \"birthdate\": \"1914-03-20\"}')),
(5, parse_json('{\"job\": \"Designer, television/film set\", \"company5\": \"Rodriguez-Meadows1\", \"ssn\": \"192-89-6614\", \"mail\": \"zbarnes@gmail.com\", \"birthdate\": \"1914-03-20\"}')),
(6, parse_json('{\"job\": \"Designer, television/film set\", \"company6\": \"Rodriguez-Meadows1\", \"ssn\": \"192-89-6614\", \"mail\": \"zbarnes@gmail.com\", \"birthdate\": \"1914-03-20\"}'));
insert into js7 select * from js7;
insert into js7 select * from js7;
ALTER TABLE js7 COMPACT;
insert into js7 values
(11, parse_json('1985-07-10 01:35:29')),
(21, parse_json('{}'));
ALTER TABLE js7 COMPACT;