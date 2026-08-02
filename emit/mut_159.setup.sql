CREATE TABLE position_v6 (
  `group_id` varchar(100) NOT NULL,
  `position_id` bigint(20) NOT NULL,
  `location_country` varchar(100) NULL,
  `job_function` varchar(100) NULL,
  `business_unit` varchar(100) NULL,
  `position_type` varchar(50) NULL
) ENGINE=OLAP
PRIMARY KEY(`group_id`, `position_id`)
PARTITION BY (`group_id`)
DISTRIBUTED BY HASH(`position_id`) BUCKETS 2
ORDER BY(`location_country`, `job_function`, `business_unit`)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO position_v6 (group_id, position_id, position_type, location_country, job_function, business_unit) VALUES
  ('eightfolddemo-hiring-test.com', 1001, 'role', 'US', 'eng', 'hr'),
  ('eightfolddemo-hiringtest.com', 1002, 'role', 'US', 'eng', 'hr'),
  ('eightfolddemo-hiringtest.com', 1003, 'role', 'US', 'eng', 'hr'),
  ('eightfolddemo-hiring-test.com', 1004, 'role', 'US', 'eng', 'hr');
CREATE TABLE collision_tbl (
  `key_col` varchar(100) NOT NULL,
  `val` int NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`key_col`)
PARTITION BY (`key_col`)
DISTRIBUTED BY HASH(`key_col`) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO collision_tbl VALUES
  ('ab', 1),
  ('a-b', 2),
  ('a:b', 3),
  ('a b', 4);
INSERT INTO collision_tbl VALUES ('a-b', 5);