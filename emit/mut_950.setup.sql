CREATE TABLE `t1_alter_with_data` (
    `dt` date NOT NULL COMMENT "",
    `id` int(11) NULL COMMENT "",
    `value` bigint NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
    PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01"))
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1_alter_with_data VALUES('2025-01-15', 1, 100);
INSERT INTO t1_alter_with_data VALUES('2025-02-15', 2, 200);
INSERT INTO t2_multi_alter VALUES('2025-01-15', 1);
ALTER TABLE t2_multi_alter ADD COLUMN col1 INT NULL;
INSERT INTO t3_alter_then_insert VALUES('2025-01-15', 1);
INSERT INTO t3_alter_then_insert VALUES('2025-02-15', 3, 200);
INSERT INTO t4_query_after_alter VALUES
('2025-01-15', 1, 100),
('2025-02-15', 2, 200),
('2025-03-15', 3, 300);
INSERT INTO t5_list_alter VALUES(1, 100), (3, 200);