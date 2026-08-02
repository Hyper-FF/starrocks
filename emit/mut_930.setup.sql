DROP TABLE IF EXISTS tab1;
CREATE table IF NOT EXISTS tab1 (
          k1 INTEGER,
          k2 INTEGER,
          k3 INTEGER,
          v1 INTEGER,
          v2 INTEGER,
          v3 INTEGER,
          v4 INTEGER
    )
    ENGINE=OLAP
    PRIMARY KEY(`k1`, `k2`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );
INSERT INTO `tab1` (`k1`, `k2`, `k3`, `v1`, `v2`, `v3`, `v4`) VALUES (1, 1, 1, 1, 1, 1, 1), (2, 2, 2, 2, 2, 2, 2), (3, 3, 3, 3, 3, 3, 3), (4, 4, 4, 4, 4, 4, 4), (5, 5, 5, 5, 5, 5, 5), (6, 6, 6, 6, 6, 6, 6), (7, 7, 7, 7, 7, 7, 7), (8, 8, 8, 8, 8, 8, 8), (9, 9, 9, 9, 9, 9, 9), (10, 10, 10, 10, 10, 10, 10);
CREATE table IF NOT EXISTS tab2 (
          k1 INTEGER,
          k2 INTEGER,
          k3 INTEGER,
          v1 INTEGER,
          v2 INTEGER,
          v3 INTEGER,
          v4 INTEGER
    )
    ENGINE=OLAP
    PRIMARY KEY(`k1`, `k2`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    order by(`k2`, `k1`)
    PROPERTIES (
        "replication_num" = "1"
    );
INSERT INTO `tab2` (`k1`, `k2`, `k3`, `v1`, `v2`, `v3`, `v4`) VALUES (1, 10, 1, 1, 1, 1, 1), (2, 9, 2, 2, 2, 2, 2), (3, 8, 3, 3, 3, 3, 3), (4, 7, 4, 4, 4, 4, 4), (5, 6, 5, 5, 5, 5, 5), (6, 5, 6, 6, 6, 6, 6), (7, 4, 7, 7, 7, 7, 7), (8, 3, 8, 8, 8, 8, 8), (9, 2, 9, 9, 9, 9, 9), (10, 1, 10, 10, 10, 10, 10);