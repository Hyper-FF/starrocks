CREATE TABLE t (
    region VARCHAR(8),
    nullable_score INT
) DUPLICATE KEY(region) DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t VALUES
    ('CN', 5),
    ('US', NULL),
    ('CN', NULL),
    ('US', 8),
    ('CN', 12);