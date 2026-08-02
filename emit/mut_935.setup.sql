CREATE TABLE `date_trunc_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('month', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO date_trunc_test VALUES 
(1, '2020-01-15 10:30:00', 'jan'),
(2, '2020-01-25 14:20:00', 'jan'),
(3, '2020-02-10 09:15:00', 'feb'),
(4, '2020-02-28 16:45:00', 'feb'),
(5, '2020-03-05 11:00:00', 'mar'),
(6, '2020-03-20 13:30:00', 'mar'),
(7, '2020-04-12 08:20:00', 'apr'),
(8, '2020-04-28 17:10:00', 'apr'),
(9, '2020-06-15 12:40:00', 'jun'),
(10, '2020-07-08 15:25:00', 'jul');
INSERT INTO date_trunc_day_test VALUES 
(1, '2020-01-15 10:30:00', 'day1'),
(2, '2020-01-15 14:20:00', 'day1'),
(3, '2020-01-16 09:15:00', 'day2'),
(4, '2020-01-17 16:45:00', 'day3');
CREATE TABLE `date_trunc_year_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('year', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO date_trunc_year_test VALUES 
(1, '2020-03-15 10:30:00', 'year2020'),
(2, '2020-08-25 14:20:00', 'year2020'),
(3, '2021-02-10 09:15:00', 'year2021'),
(4, '2021-11-28 16:45:00', 'year2021');
CREATE TABLE `date_trunc_quarter_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('quarter', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO date_trunc_quarter_test VALUES 
(1, '2020-01-15 10:30:00', 'q1'),
(2, '2020-02-25 14:20:00', 'q1'),
(3, '2020-04-10 09:15:00', 'q2'),
(4, '2020-05-28 16:45:00', 'q2'),
(5, '2020-07-15 11:00:00', 'q3'),
(6, '2020-08-20 13:30:00', 'q3'),
(7, '2020-10-12 08:20:00', 'q4'),
(8, '2020-11-28 17:10:00', 'q4');
INSERT INTO date_trunc_minute_test VALUES 
(1, '2020-01-15 10:30:15', 'min30'),
(2, '2020-01-15 10:30:45', 'min30'),
(3, '2020-01-15 10:31:20', 'min31'),
(4, '2020-01-15 10:32:50', 'min32');
CREATE TABLE `date_trunc_second_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('second', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO date_trunc_second_test VALUES 
(1, '2020-01-15 10:30:15', 'sec15'),
(2, '2020-01-15 10:30:16', 'sec16'),
(3, '2020-01-15 10:30:17', 'sec17');
INSERT INTO date_trunc_hour_test VALUES 
(1, '2020-01-15 10:15:30', 'hour10'),
(2, '2020-01-15 10:45:20', 'hour10'),
(3, '2020-01-15 11:20:15', 'hour11'),
(4, '2020-01-15 11:50:45', 'hour11');