CREATE TABLE t_trim_src(id INT, c VARCHAR(65533), c2 VARCHAR(65533))
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
INSERT INTO t_trim_src VALUES
    (1, 'xxxbarxxx', 'xyz'),
    (2, 'xyzfooxyz', 'abc'),
    (3, NULL,        'def');