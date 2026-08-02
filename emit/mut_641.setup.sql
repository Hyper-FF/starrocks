CREATE TABLE t1 (
    k1 INT,
    k2 INT,
    v1 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");
CREATE TABLE t2 (
    k1 INT,
    k2 INT,
    v2 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");
CREATE TABLE t3 (
    k1 INT,
    k2 INT,
    v3 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");
CREATE TABLE t4 (
    k1 INT,
    k2 INT,
    v4 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");
INSERT INTO t1 VALUES
    (1, 10, 'a1'),
    (2, 20, 'a2'),
    (3, NULL, 'a3'),
    (NULL, 40, 'a4'),
    (NULL, NULL, 'a5');
INSERT INTO t2 VALUES
    (1, 10, 'b1'),
    (4, 40, 'b2'),
    (5, NULL, 'b3'),
    (NULL, 60, 'b4'),
    (NULL, NULL, 'b5');
INSERT INTO t3 VALUES
    (1, 10, 'c1'),
    (2, 20, 'c2'),
    (6, 60, 'c3'),
    (NULL, 70, 'c4'),
    (NULL, NULL, 'c5');
INSERT INTO t4 VALUES
    (1, 10, 'd1'),
    (7, 70, 'd2'),
    (8, NULL, 'd3'),
    (NULL, 80, 'd4'),
    (NULL, NULL, 'd5');