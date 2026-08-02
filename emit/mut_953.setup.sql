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
ALTER TABLE t1_alter_with_data ADD COLUMN new_col INT NULL DEFAULT "0";
CREATE TABLE `t2_multi_alter` (
    `dt` date NOT NULL COMMENT "",
    `id` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p1 VALUES [("2025-01-01"), ("2025-02-01"))
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t2_multi_alter VALUES('2025-01-15', 1);
ALTER TABLE t2_multi_alter ADD COLUMN col1 INT NULL;
ALTER TABLE t2_multi_alter ADD COLUMN col2 STRING NULL;
ALTER TABLE t2_multi_alter ADD COLUMN col3 DOUBLE NULL;
CREATE TABLE `t3_alter_then_insert` (
    `dt` date NOT NULL COMMENT "",
    `id` int(11) NULL COMMENT ""
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
INSERT INTO t3_alter_then_insert VALUES('2025-01-15', 1);
ALTER TABLE t3_alter_then_insert ADD COLUMN new_col INT NULL;
INSERT INTO t3_alter_then_insert VALUES('2025-01-20', 2, 100);
INSERT INTO t3_alter_then_insert VALUES('2025-02-15', 3, 200);
CREATE TABLE `t4_query_after_alter` (
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
INSERT INTO t4_query_after_alter VALUES
('2025-01-15', 1, 100),
('2025-02-15', 2, 200),
('2025-03-15', 3, 300);
ALTER TABLE t4_query_after_alter ADD COLUMN col1 STRING NULL;
CREATE TABLE `t5_list_alter` (
    `c1` int NOT NULL,
    `c2` int
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
PARTITION BY LIST(`c1`)
(
    PARTITION p1 VALUES IN ('1', '2'),
    PARTITION p2 VALUES IN ('3', '4')
)
DISTRIBUTED BY HASH(`c1`)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t5_list_alter VALUES(1, 100), (3, 200);
ALTER TABLE t5_list_alter ADD COLUMN col1 STRING NULL;