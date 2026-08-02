CREATE TABLE IF NOT EXISTS `t1` (
    `v1` int(11) NULL,
    `v2` int(11) NULL,
    `v3` int(11) NOT NULL,
    `v4` int(11) NULL
)
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO `t1` values (1, 1, 1, NULL), (1, 1, 2, NULL), (1, NULL, 3, NULL), (1, NULL, 4, NULL),
(1, 2, 5, NULL), (1, 2, 6, NULL), (1, NULL, 7, NULL), (1, NULL, 8, NULL), (2, 3, 9, NULL),
(2, 3, 10, NULL), (2, NULL, 11, NULL), (2, NULL, 12, NULL), (2, 4, 13, NULL), (2, 4, 14, NULL),
(2, NULL, 15, NULL), (2, NULL, 16, NULL), (NULL, 3, 17, NULL), (NULL, 3, 18, NULL),
(NULL, NULL, 19, NULL), (NULL, NULL, 20, NULL), (NULL, 4, 21, NULL), (NULL, 4, 22, NULL),
(NULL, NULL, 23, NULL), (NULL, NULL, 24, NULL);
CREATE TABLE IF NOT EXISTS `t2` (
    `v1` int(11) NULL,
    `v2` STRING NULL
)
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t2 VALUES
(1, '1'),
(2, NULL),
(3, '2'),
(4, '2'),
(5, '2'),
(6, '3'),
(7, '3'),
(8, '200'),
(9, '40'),
(10, '50'),
(11, '60'),
(12, '10'),
(13, '20');