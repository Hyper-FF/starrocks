CREATE TABLE t1 (
    k1 int,
    k2 int,
    v1 int,
    v2 string
) PRIMARY KEY(k1, k2)
DISTRIBUTED BY HASH(k1) BUCKETS 3
PROPERTIES ("replication_num" = "1");
insert into t1 values (1, 1, 10, 'aa'), (2, 2, 20, 'bb'), (3, 3, 30, 'cc');
ALTER TABLE t1 ADD INDEX idx_v1(v1) USING BITMAP;