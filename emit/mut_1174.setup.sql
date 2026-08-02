CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100),
    age INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");