CREATE TABLE IF NOT EXISTS `next_day_test` (
  `d1` DATE,
  `d2` DATETIME
)
DISTRIBUTED BY HASH(`d1`)
PROPERTIES(
  "replication_num" = "1"
);
INSERT INTO `next_day_test`(d1, d2)
VALUES ('2023-04-01', '2023-04-01 20:13:14'),
       ('2023-04-02', '2023-04-02 20:13:14'),
       ('2023-04-03', '2023-04-03 20:13:14'),
       ('2023-04-04', '2023-04-04 20:13:14'),
       ('2023-04-05', '2023-04-05 20:13:14'),
       ('2023-04-06', '2023-04-06 20:13:14'),
       ('2023-04-07', '2023-04-07 20:13:14'),
       ('2023-02-27', '2023-02-27 20:13:14'),
       ('2023-02-28', '2023-02-28 20:13:14'),
       ('2024-02-27', '2024-02-27 20:13:14'),
       ('2024-02-28', '2024-02-28 20:13:14'),
       ('2024-02-29', '2024-02-29 20:13:14');
CREATE TABLE IF NOT EXISTS `dow_test` (
  `d3` DATE,
  `dow_2` CHAR(20),
  `dow_3` VARCHAR(30),
  `dow_full` STRING
)
DISTRIBUTED BY HASH(`d3`)
PROPERTIES(
  "replication_num" = "1"
);
INSERT INTO `dow_test`(d3, dow_2, dow_3, dow_full)
VALUES ('2023-04-01', 'Mo', 'Mon', 'Monday'),
       ('2023-04-02', 'Tu', 'Tue', 'Tuesday'),
       ('2023-04-03', 'We', 'Wed', 'Wednesday'),
       ('2023-04-04', 'Th', 'Thu', 'Thursday'),
       ('2023-04-05', 'Fr', 'Fri', 'Friday'),
       ('2023-04-06', 'Sa', 'Sat', 'Saturday'),
       ('2023-04-07', 'Su', 'Sun', 'Sunday');
create table t (
`d` date not null,
`w` string default null
) engine=olap duplicate key(`d`);
insert into t values ('9999-12-31', 'Sun');
CREATE TABLE IF NOT EXISTS `previous_day_test` (
  `d1` DATE,
  `d2` DATETIME
)
DISTRIBUTED BY HASH(`d1`)
PROPERTIES(
  "replication_num" = "1"
);
INSERT INTO `previous_day_test`(d1, d2)
VALUES ('2023-04-09', '2023-04-09 23:08:11'),
       ('2023-04-10', '2023-04-10 23:08:11'),
       ('2023-04-11', '2023-04-11 23:08:11'),
       ('2023-04-12', '2023-04-12 23:08:11'),
       ('2023-04-13', '2023-04-13 23:08:11'),
       ('2023-04-14', '2023-04-14 23:08:11'),
       ('2023-04-15', '2023-04-15 23:08:11'),
       ('2023-02-28', '2023-02-28 23:08:11'),
       ('2023-03-01', '2023-03-01 23:08:11'),
       ('2024-02-29', '2024-02-29 23:08:11'),
       ('2024-03-01', '2024-03-01 23:08:11'),
       ('2024-03-02', '2024-03-02 23:08:11');
CREATE TABLE IF NOT EXISTS `previous_dow` (
  `d3` DATE,
  `dow_2` CHAR(20),
  `dow_3` VARCHAR(30),
  `dow_full` STRING
)
DISTRIBUTED BY HASH(`d3`)
PROPERTIES(
  "replication_num" = "1"
);
INSERT INTO `previous_dow`(d3, dow_2, dow_3, dow_full)
VALUES ('2023-04-09', 'Mo', 'Mon', 'Monday'),
       ('2023-04-10', 'Tu', 'Tue', 'Tuesday'),
       ('2023-04-11', 'We', 'Wed', 'Wednesday'),
       ('2023-04-12', 'Th', 'Thu', 'Thursday'),
       ('2023-04-13', 'Fr', 'Fri', 'Friday'),
       ('2023-04-14', 'Sa', 'Sat', 'Saturday'),
       ('2023-04-15', 'Su', 'Sun', 'Sunday');
create table t (
`d` date not null,
`w` string default null
) engine=olap duplicate key(`d`);
insert into t values ('0000-01-01', 'Sun');
CREATE TABLE IF NOT EXISTS `makedate_test` (
  `id` int,
  `col_year` int,
  `col_day` int
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES(
  "replication_num" = "1"
);
INSERT INTO `makedate_test`(id, col_year, col_day)
VALUES (1, NUll, NUll),
       (2, 1, NUll),
       (3, NULL, 1),
       (4, 0, 1),
       (5, 2023, 0),
       (6, 2023, 32),
       (7, 2023, 365),
       (8, 2023, 366),
       (9, 9999, 1),
       (10, 9999, 365),
       (11, 9999, 366),
       (12, 10000, 1),
       (13, 1, -1);
CREATE TABLE IF NOT EXISTS `last_day_table` (
  `d1` DATE,
  `d2` DATETIME
)
DISTRIBUTED BY HASH(`d1`)
PROPERTIES(
  "replication_num" = "1"
);
INSERT INTO `last_day_table`(d1, d2)
VALUES ('2020-02-12', '2020-02-12 08:08:14'),
       ('2021-03-28', '2021-03-28 08:08:14'),
       ('2022-04-28', '2022-04-28 08:08:14'),
       ('2023-05-29', '2023-05-29 08:08:14');
CREATE TABLE IF NOT EXISTS `last_day_with_optional_table` (
  `d1` DATE,
  `d2` DATETIME,
  `optional` CHAR(8)
)
DISTRIBUTED BY HASH(`d1`)
PROPERTIES(
  "replication_num" = "1"
);
INSERT INTO `last_day_with_optional_table`(d1, d2, optional)
VALUES ('2020-02-12', '2020-02-12 08:08:14', 'month'),
       ('2021-03-28', '2021-03-28 08:08:14', 'quarter'),
       ('2022-04-28', '2022-04-28 08:08:14', 'year'),
       ('2023-05-29', '2023-05-29 08:08:14', 'year');
CREATE TABLE IF NOT EXISTS `to_iso8601_test`(
  `d1` DATE,
  `d2` DATETIME
)
DISTRIBUTED BY HASH(`d1`)
PROPERTIES(
  "replication_num" = "1"
);
INSERT INTO `to_iso8601_test`(d1, d2)
VALUES ('2020-01-01','2020-01-01 00:00:00'),
  ('2020-01-02', '2020-01-01 00:00:00.1'),
  ('2020-01-03', '2020-01-01 00:00:00.12'),
  ('2020-01-04', '2020-01-01 00:00:00.123'),
  ('2020-01-05', '2020-01-01 00:00:00.1234');