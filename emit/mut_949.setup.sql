CREATE TABLE `t1_range_basic` (
    `dt` date NULL COMMENT "",
    `id` int(11) NULL COMMENT "",
    `name` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `id`, `name`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p20250428 VALUES [("2025-04-28"), ("2025-04-29")),
    PARTITION p20250429 VALUES [("2025-04-29"), ("2025-04-30"))
)
DISTRIBUTED BY HASH(`id`, `name`)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1_range_basic VALUES('2025-04-29', 1, 'bar');
INSERT INTO t2_list_rollup VALUES(1, 100, 'a'), (3, 200, 'b'), (5, 300, 'c');
INSERT INTO t3_range_empty VALUES('2025-02-15', 1, 100), ('2025-02-20', 2, 200);
INSERT INTO t4_prune_after_alter VALUES
('2025-01-15', 1, 'jan'),
('2025-02-15', 2, 'feb'),
('2025-03-15', 3, 'mar'),
('2025-04-15', 4, 'apr');
INSERT INTO t5_multi_alter VALUES('2025-01-15', 1), ('2025-02-15', 2);
ALTER TABLE t5_multi_alter ADD COLUMN col1 INT NULL;
INSERT INTO t6_explain_check VALUES('2025-02-15', 1);