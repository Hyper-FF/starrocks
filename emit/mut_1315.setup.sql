DROP TABLE IF EXISTS window_merge_sort_t1;
CREATE TABLE window_merge_sort_t1 (
    p INT NULL,
    s INT NULL,
    x INT NULL
) ENGINE=OLAP
DUPLICATE KEY(p, s, x)
DISTRIBUTED BY HASH(p) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO window_merge_sort_t1 VALUES
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3),
    (NULL, NULL, 4),
    (NULL, NULL, 5),
    (NULL, NULL, 6),
    (NULL, NULL, 7),
    (NULL, NULL, 8),
    (NULL, NULL, 9),
    (NULL, NULL, 10),
    (NULL, NULL, 11),
    (NULL, NULL, 12),
    (NULL, NULL, 13);
DROP TABLE IF EXISTS window_merge_sort_t1;