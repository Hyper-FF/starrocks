CREATE TABLE `test_basic` (
  `id_int` int(11) NOT NULL COMMENT "",
  `id_tinyint` tinyint(4) NOT NULL COMMENT "",
  `id_smallint` smallint(6) NOT NULL COMMENT "",
  `id_bigint` bigint(20) NOT NULL COMMENT "",
  `id_largeint` largeint(40) NOT NULL COMMENT "",
  `id_float` float NOT NULL COMMENT "",
  `id_double` double NOT NULL COMMENT "",
  `id_char` char(10) NOT NULL COMMENT "",
  `id_string` varchar(100) NOT NULL COMMENT "",
  `id_date` date NOT NULL COMMENT "",
  `id_datetime` datetime NOT NULL COMMENT "",
  `id_decimal` decimal128(27, 9) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`id_int`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id_int`) BUCKETS 2
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t_array` (
    `c_1_0` int NULL COMMENT "",
    `c_1_1` array <int> NULL COMMENT ""
) ENGINE = OLAP 
DUPLICATE KEY(`c_1_0`) COMMENT "OLAP" 
DISTRIBUTED BY HASH(`c_1_0`) BUCKETS 1 PROPERTIES (
    "replication_num" = "1"
);
insert into t_array values (null, null), (1, [1]), (1, [1]), (1, [1,2,3]), (1, null), (null, [1]);
insert into t_array values (null, null), (1, [1]), (1, [2]), (2, [1,2,3]), (1, null), (null, [1]);
insert into t_array values (null, null), (1, [1, null]), (1, [2, null]), (1, [1, null]);
CREATE TABLE `t_struct` (
    `c_1_0` int NULL COMMENT "",
    `c_1_1` struct<name varchar(20), age int> NULL COMMENT ""
) ENGINE = OLAP 
DUPLICATE KEY(`c_1_0`) COMMENT "OLAP" 
DISTRIBUTED BY HASH(`c_1_0`) BUCKETS 1 PROPERTIES (
    "replication_num" = "1"
);
insert into t_struct values (null, null), (1, row('Alice', 25)), (1, row('Alice', 25)), (1, row('Bob', 30)), (1, null), (null, row('Alice', 25));
insert into t_struct values (null, null), (1, row('Alice', 25)), (1, row('Alice', 30)), (2, row('Bob', 30)), (1, null), (null, row('Charlie', 35));
insert into t_struct values (null, null), (1, row('Alice', null)), (1, row('Bob', null)), (1, row('Alice', null));