CREATE TABLE t1 (
    idx BIGINT, 
    k BIGINT NULL, 
    val1 BIGINT NULL,
    val2 BIGINT NULL
) PRIMARY KEY(idx) 
DISTRIBUTED BY HASH (idx) BUCKETS 32
PROPERTIES("replication_num" = "1");
CREATE TABLE t1_nonnull (
    idx BIGINT, 
    k BIGINT NOT NULL, 
    val1 BIGINT NOT NULL,
    val2 BIGINT NOT NULL
) PRIMARY KEY(idx) 
DISTRIBUTED BY HASH (idx) BUCKETS 32
PROPERTIES("replication_num" = "1");
INSERT INTO t1 (val2, val1, k, idx) values
    (10, 1,1,2),
    (20, 2,2,4),
    (30, 3,3,6),
    (NULL, 4,4,8),
    (50, NULL,5,10),
    (NULL, NULL,6,12),
    (70, NULL,7,14),
    (80, 8,8,16),
    (90, 9,9,18),
    (100, 10,10,20);
INSERT INTO t1_nonnull (val2, val1, k, idx) values
    (10, 1,1,2),
    (20, 2,2,4),
    (30, 3,3,6),
    (40, 4,4,8),
    (50, 5,5,10),
    (60, 6,6,12),
    (70, 7,7,14),
    (80, 8,8,16),
    (90, 9,9,18),
    (100, 10,10,20);