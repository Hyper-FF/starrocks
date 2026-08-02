CREATE TABLE `test_delete_dup_rename` (
    `name1` varchar(255)
) ENGINE=OLAP
DUPLICATE KEY(`name1`)
DISTRIBUTED BY HASH(`name1`) BUCKETS 2
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO test_delete_dup_rename VALUES ("mon"), ("tue");
ALTER TABLE test_delete_dup_rename RENAME COLUMN name1 TO name2;
INSERT INTO test_delete_dup_rename VALUES ("wed"), ("thu");