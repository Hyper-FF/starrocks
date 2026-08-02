CREATE TABLE `t_without_null` (
  `c_id` INT(11) NOT NULL,
  `c_int` INT(11) NOT NULL,
  `c_tinyint` TINYINT NOT NULL,
  `c_smallint` SMALLINT NOT NULL,
  `c_bigint` BIGINT NOT NULL,
  `c_largeint` LARGEINT NOT NULL,
  `c_float` FLOAT NOT NULL,
  `c_double` DOUBLE NOT NULL,
  `c_char` CHAR(10) NOT NULL,
  `c_varchar` VARCHAR(100) NOT NULL,
  `c_date` DATE NOT NULL,
  `c_datetime` DATETIME NOT NULL,
  `c_decimal` DECIMAL64(9,3) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_id`)
DISTRIBUTED BY HASH(`c_id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
INSERT INTO `t_without_null` (
  `c_id`, `c_tinyint`, `c_smallint`, `c_int`, `c_bigint`, `c_largeint`, `c_float`, `c_double`, `c_char`, `c_varchar`, `c_date`, `c_datetime`, `c_decimal`) 
VALUES 
  (1, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (2, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (3, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (4, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (5, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (11, 2, 22, 222, 2222, 22222, 2.2, 22.22, 'char2', 'varchar2', '2021-01-02', '2021-01-02 00:00:00', 222.22),
  (12, 2, 22, 222, 2222, 22222, 2.2, 22.22, 'char2', 'varchar2', '2021-01-02', '2021-01-02 00:00:00', 222.22),
  (13, 2, 22, 222, 2222, 22222, 2.2, 22.22, 'char2', 'varchar2', '2021-01-02', '2021-01-02 00:00:00', 222.22),
  (21, 3, 33, 333, 3333, 33333, 3.3, 33.33, 'char3', 'varchar3', '2021-01-03', '2021-01-03 00:00:00', 333.33),
  (22, 3, 33, 333, 3333, 33333, 3.3, 33.33, 'char3', 'varchar3', '2021-01-03', '2021-01-03 00:00:00', 333.33),
  (31, 4, 44, 444, 4444, 44444, 4.4, 44.44, 'char4', 'varchar4', '2021-01-04', '2021-01-04 00:00:00', 444.44),
  (91, 10, 100, 1000, 10000, 100000, 1.01, 100.01, 'char10', 'varchar10', '2021-01-10', '2021-01-10 00:00:00', 1000.01);
CREATE TABLE `t_with_null` (
  `c_id` INT(11) NOT NULL,
  `c_int` INT(11) NULL,
  `c_tinyint` TINYINT NULL,
  `c_smallint` SMALLINT NULL,
  `c_bigint` BIGINT NULL,
  `c_largeint` LARGEINT NULL,
  `c_float` FLOAT NULL,
  `c_double` DOUBLE NULL,
  `c_char` CHAR(10) NULL,
  `c_varchar` VARCHAR(100) NULL,
  `c_date` DATE NULL,
  `c_datetime` DATETIME NULL,
  `c_decimal` DECIMAL64(9,3) NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_id`)
DISTRIBUTED BY HASH(`c_id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
INSERT INTO `t_with_null` (
  `c_id`, `c_tinyint`, `c_smallint`, `c_int`, `c_bigint`, `c_largeint`, `c_float`, `c_double`, `c_char`, `c_varchar`, `c_date`, `c_datetime`, `c_decimal`)
VALUES
  (1, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (2, NULL, 22, 222, 2222, 22222, 2.2, NULL, 'char2', 'varchar2', '2021-01-02', NULL, 222.22),
  (3, 3, NULL, NULL, 3333, 33333, NULL, 33.33, NULL, 'varchar3', NULL, '2021-01-03 00:00:00', NULL),
  (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (5, 0, 5, 5, 5, 5, 0.5, 5.5, 'char0', 'varchar0', '2020-12-31', '2020-12-31 23:59:59', 5.55);