CREATE TABLE `t1_partial_empty` (
    `dt` date NOT NULL COMMENT "",
    `id` int(11) NULL COMMENT "",
    `value` bigint NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
    PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01")),
    PARTITION p202503 VALUES [("2025-03-01"), ("2025-04-01"))
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1_partial_empty VALUES('2025-02-15', 1, 100);
INSERT INTO t2_schema_change_minmax VALUES('2025-01-15', 1);
INSERT INTO t2_schema_change_minmax VALUES('2025-02-15', 2);
INSERT INTO t2_schema_change_minmax VALUES('2025-02-20', 4, 200);
INSERT INTO t3_list_minmax VALUES(1, 100, 'a'), (5, 200, 'b'), (9, 300, 'c');
INSERT INTO t4_prune_with_minmax VALUES('2025-01-15', 1);
INSERT INTO t4_prune_with_minmax VALUES('2025-02-15', 2);
INSERT INTO t4_prune_with_minmax VALUES('2025-03-15', 3);
INSERT INTO t4_prune_with_minmax VALUES('2025-04-15', 4);
INSERT INTO t5_filter_minmax VALUES('2025-01-15', 1, 100);
INSERT INTO t5_filter_minmax VALUES('2025-02-15', 2, 200);