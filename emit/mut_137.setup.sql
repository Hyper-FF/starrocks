CREATE TABLE events (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `event_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");
CREATE TABLE snapshots (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
CREATE TABLE events_with_nulls (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NULL,
  `event_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");
CREATE TABLE snapshots_with_nulls (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
INSERT INTO events VALUES
(1, 101, '2024-01-01 10:00:00', 'EVENT_A'),
(2, 101, '2024-01-01 15:30:00', 'EVENT_B'),
(3, 102, '2024-01-01 11:00:00', 'EVENT_C'),
(4, 102, '2024-01-01 16:00:00', 'EVENT_D'),
(5, 103, '2024-01-01 12:00:00', 'EVENT_E');
INSERT INTO snapshots VALUES
(101, '2024-01-01 08:00:00', 'SNAPSHOT_1'),
(101, '2024-01-01 14:00:00', 'SNAPSHOT_2'),
(102, '2024-01-01 09:00:00', 'SNAPSHOT_3'),
(102, '2024-01-01 13:00:00', 'SNAPSHOT_4'),
(103, '2024-01-01 11:30:00', 'SNAPSHOT_5');
INSERT INTO snapshots VALUES (101, '2024-01-01 10:00:00', 'SNAPSHOT_1_10AM');
INSERT INTO events_with_nulls VALUES
(1, 201, '2024-01-01 10:00:00', 'EVENT_A'),
(2, 201, NULL, 'EVENT_B'),
(3, 202, '2024-01-01 11:00:00', 'EVENT_C'),
(4, 202, '2024-01-01 16:00:00', 'EVENT_D'),
(5, 203, NULL, 'EVENT_E');
INSERT INTO snapshots_with_nulls VALUES
(201, '2024-01-01 08:00:00', 'SNAPSHOT_1'),
(201, NULL, 'SNAPSHOT_2'),
(202, '2024-01-01 09:00:00', 'SNAPSHOT_3'),
(202, '2024-01-01 13:00:00', 'SNAPSHOT_4'),
(203, '2024-01-01 11:30:00', 'SNAPSHOT_5');
CREATE TABLE empty_snapshots (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
CREATE TABLE snapshots_large_gap (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
INSERT INTO events_large_gap VALUES
(1, 301, '2024-01-01 10:00:00', 'EVENT_A'),
(2, 301, '2024-01-01 15:30:00', 'EVENT_B'),
(3, 302, '2024-01-01 11:00:00', 'EVENT_C');
INSERT INTO snapshots_large_gap VALUES
(301, '2023-12-01 08:00:00', 'SNAPSHOT_1'),
(301, '2023-12-15 14:00:00', 'SNAPSHOT_2'),
(302, '2023-11-01 09:00:00', 'SNAPSHOT_3');
CREATE TABLE snapshots_dups (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO events_dups VALUES (1,401,'2024-01-01 10:00:00');
INSERT INTO snapshots_dups VALUES
(401,'2024-01-01 10:00:00','SNAP_D1'),
(401,'2024-01-01 10:00:00','SNAP_D2');
CREATE TABLE snapshots_ops (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO events_ops VALUES (1,501,'2024-01-01 12:00:00');
INSERT INTO snapshots_ops VALUES (501,'2024-01-01 12:00:00');
CREATE TABLE snapshots_ooo (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO events_ooo VALUES (1,551,'2024-01-01 10:30:00');
INSERT INTO snapshots_ooo VALUES (551,'2024-01-01 10:00:00','S10'),(551,'2024-01-01 09:00:00','S09');
CREATE TABLE snapshots_bounds (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO snapshots_bounds VALUES (601,'2024-01-01 09:00:00','SB09'),(601,'2024-01-01 18:00:00','SB18');
INSERT INTO events_bounds VALUES (1,601,'2024-01-01 07:00:00'),(2,601,'2024-01-01 20:00:00');
CREATE TABLE snapshots_equi_null (
  `user_id` int(11) NULL,
  `snapshot_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO events_equi_null VALUES (1,NULL,'2024-01-01 12:00:00');
INSERT INTO snapshots_equi_null VALUES (NULL,'2024-01-01 11:00:00');