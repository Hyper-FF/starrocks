CREATE TABLE `test_range_offset_window` (
    `id` int NOT NULL,
    `k` int NULL,
    `dec_k` decimal(10, 2) NULL,
    `d` date NULL,
    `ts` datetime NULL,
    `v` int NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO `test_range_offset_window` VALUES
    (1, NULL, NULL, NULL, NULL, 10),
    (2, NULL, NULL, NULL, NULL, 20),
    (3, 1, 1.00, '2024-01-01', '2024-01-01 00:00:00', 1),
    (4, 2, 2.25, '2024-01-02', '2024-01-15 00:00:00', 2),
    (5, 2, 2.25, '2024-01-02', '2024-02-01 00:00:00', 3),
    (6, 3, 3.50, '2024-01-04', '2024-02-15 00:00:00', 4),
    (7, 5, 5.75, '2024-02-01', '2024-03-01 00:00:00', 5);