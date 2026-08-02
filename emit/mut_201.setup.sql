CREATE TABLE `t_gen` (
    k1 int,
    v0 int,
    v1 int
)
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO t_gen VALUES (1, 1, 1);
ALTER TABLE t_gen DROP COLUMN v0;
ALTER TABLE t_gen ADD COLUMN v0 int default '10';
ALTER TABLE t_gen ADD COLUMN v2 bigint AS v0+v1;
INSERT INTO t_gen (k1, v1) VALUES (2, 2);
INSERT INTO t_gen (k1, v0, v1) VALUES (3, 20, 3);