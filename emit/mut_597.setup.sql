CREATE TABLE `test_column_default` (
  `date` date NOT NULL DEFAULT "1970-01-01" COMMENT "",
  `device_id` varchar(150) NOT NULL DEFAULT "" COMMENT "",
  `server_ip` varchar(150) NOT NULL DEFAULT "" COMMENT "",
  `id` int(11) NOT NULL COMMENT "",
  `device_type` tinyint(4) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`date`, `device_id`)
DISTRIBUTED BY RANDOM
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `test_column_datetime_precision` (
  `id` int NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `d` date NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
"replication_num" = "1"
);
drop table test_column_datetime_precision;