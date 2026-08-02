CREATE TABLE t (
  id INT,
  s VARCHAR(65533)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t VALUES (1, unhex('F0')), (2, char(228)), (3, unhex('E4B8')), (4, 'ok');