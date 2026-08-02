DROP TABLE IF EXISTS t_struct_gs;
CREATE TABLE t_struct_gs (c0 INT, c2 STRUCT<a INT, b INT>)
DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num'='1');
INSERT INTO t_struct_gs VALUES (1, row(1,10)), (1, row(2,20)), (2, row(3,30));
DROP TABLE t_struct_gs;