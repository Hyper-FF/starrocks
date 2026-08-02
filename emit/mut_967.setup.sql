CREATE TABLE t1(id int, os string, version string) PRIMARY KEY (id) PROPERTIES ("replication_num" = "1");
INSERT INTO t1(id, os, version) VALUES (1, '1', '1');
INSERT INTO t1 (id, version) VALUES (1, '2');
DROP TABLE t1;