DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;
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
    ORDER BY(`k3`, `k2`)
    PROPERTIES (
        "replication_num" = "1"
    );
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
    ORDER BY(`k1`, `k2`)
    PROPERTIES (
        "replication_num" = "1"
    );