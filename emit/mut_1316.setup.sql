CREATE TABLE t1 (
    c1 INT NOT NULL,
    c2 INT NOT NULL,
    c3 INT NOT NULL
)
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE t2 (
    c4 INT NOT NULL,
    c5 INT NOT NULL,
    c6 INT NOT NULL
)
DUPLICATE KEY(c4)
DISTRIBUTED BY HASH(c4) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1 (c1, c2, c3) VALUES
(1, 1, 1),
(2, 1, 11),
(3, 1, 111),
(4, 2, 2),
(5, 2, 22),
(6, 2, 222);
INSERT INTO t2 (c4, c5, c6) VALUES
(1, 1, 1),
(2, 1, 11),
(3, 1, 111),
(4, 2, 2),
(5, 2, 22),
(6, 2, 222);
DROP TABLE IF EXISTS t_rank_bitmap_crash;
CREATE TABLE t_rank_bitmap_crash (
    k INT,
    ts INT,
    uid BIGINT NULL
)
DUPLICATE KEY(k, ts)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO t_rank_bitmap_crash VALUES
(1, 100, 1), (1, 90, 2), (1, 80, NULL), (1, 70, 3),
(2, 100, 4), (2, 90, NULL), (2, 80, 5);