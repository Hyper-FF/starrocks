CREATE TABLE `pk_parallel_2` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` VARCHAR(100),
    `v2` BIGINT
)
PRIMARY KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "2"
);
INSERT INTO pk_parallel_2 VALUES (1, 1, 'a', 100), (2, 2, 'b', 200);
INSERT INTO pk_parallel_2 VALUES (3, 3, 'c', 300), (4, 4, 'd', 400);
INSERT INTO pk_parallel_2 VALUES (5, 5, 'e', 500), (6, 6, 'f', 600);
INSERT INTO pk_parallel_2 VALUES (7, 7, 'g', 700), (8, 8, 'h', 800);
INSERT INTO pk_parallel_2 VALUES (9, 9, 'i', 900), (10, 10, 'j', 1000);
CREATE TABLE `pk_parallel_5` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` VARCHAR(100),
    `v2` BIGINT
)
PRIMARY KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "5"
);
INSERT INTO pk_parallel_5 VALUES (1, 1, 'a', 100), (2, 2, 'b', 200);
INSERT INTO pk_parallel_5 VALUES (3, 3, 'c', 300), (4, 4, 'd', 400);
INSERT INTO pk_parallel_5 VALUES (5, 5, 'e', 500), (6, 6, 'f', 600);
INSERT INTO pk_parallel_5 VALUES (7, 7, 'g', 700), (8, 8, 'h', 800);
INSERT INTO pk_parallel_5 VALUES (9, 9, 'i', 900), (10, 10, 'j', 1000);
INSERT INTO pk_parallel_5 VALUES (11, 11, 'k', 1100), (12, 12, 'l', 1200);
INSERT INTO pk_parallel_5 VALUES (13, 13, 'm', 1300), (14, 14, 'n', 1400);
INSERT INTO pk_parallel_5 VALUES (15, 15, 'o', 1500), (16, 16, 'p', 1600);
CREATE TABLE `dup_parallel` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` VARCHAR(100),
    `v2` BIGINT
)
DUPLICATE KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "3"
);
INSERT INTO dup_parallel VALUES (1, 1, 'a', 100), (2, 2, 'b', 200);
INSERT INTO dup_parallel VALUES (3, 3, 'c', 300), (4, 4, 'd', 400);
INSERT INTO dup_parallel VALUES (5, 5, 'e', 500), (6, 6, 'f', 600);
INSERT INTO dup_parallel VALUES (7, 7, 'g', 700), (8, 8, 'h', 800);
INSERT INTO dup_parallel VALUES (9, 9, 'i', 900), (10, 10, 'j', 1000);
CREATE TABLE `agg_parallel` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` BIGINT SUM,
    `v2` BIGINT MAX
)
AGGREGATE KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "3"
);
INSERT INTO agg_parallel VALUES (1, 1, 100, 100), (2, 2, 200, 200);
INSERT INTO agg_parallel VALUES (3, 3, 300, 300), (4, 4, 400, 400);
INSERT INTO agg_parallel VALUES (1, 1, 50, 150), (2, 2, 100, 250);
INSERT INTO agg_parallel VALUES (5, 5, 500, 500), (6, 6, 600, 600);
INSERT INTO agg_parallel VALUES (3, 3, 200, 400), (4, 4, 300, 500);
CREATE TABLE `uniq_parallel` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` VARCHAR(100),
    `v2` BIGINT
)
UNIQUE KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "4"
);
INSERT INTO uniq_parallel VALUES (1, 1, 'a', 100), (2, 2, 'b', 200);
INSERT INTO uniq_parallel VALUES (3, 3, 'c', 300), (4, 4, 'd', 400);
INSERT INTO uniq_parallel VALUES (1, 1, 'a_updated', 150), (2, 2, 'b_updated', 250);
INSERT INTO uniq_parallel VALUES (5, 5, 'e', 500), (6, 6, 'f', 600);
INSERT INTO uniq_parallel VALUES (3, 3, 'c_updated', 350), (7, 7, 'g', 700);
CREATE TABLE `pk_no_parallel` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` VARCHAR(100),
    `v2` BIGINT
)
PRIMARY KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "0"
);
INSERT INTO pk_no_parallel VALUES (1, 1, 'a', 100), (2, 2, 'b', 200);
INSERT INTO pk_no_parallel VALUES (3, 3, 'c', 300), (4, 4, 'd', 400);
INSERT INTO pk_no_parallel VALUES (5, 5, 'e', 500), (6, 6, 'f', 600);
CREATE TABLE `pk_alter_parallel` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` VARCHAR(100),
    `v2` BIGINT
)
PRIMARY KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "2"
);
ALTER TABLE pk_alter_parallel SET ("lake_compaction_max_parallel" = "5");
ALTER TABLE pk_alter_parallel SET ("lake_compaction_max_parallel" = "0");
ALTER TABLE pk_alter_parallel SET ("lake_compaction_max_parallel" = "3");
CREATE TABLE `pk_with_updates` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` VARCHAR(100),
    `v2` BIGINT
)
PRIMARY KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "3"
);
INSERT INTO pk_with_updates VALUES (1, 1, 'a', 100), (2, 2, 'b', 200), (3, 3, 'c', 300);
INSERT INTO pk_with_updates VALUES (4, 4, 'd', 400), (5, 5, 'e', 500), (6, 6, 'f', 600);
INSERT INTO pk_with_updates VALUES (7, 7, 'g', 700), (8, 8, 'h', 800);
CREATE TABLE `pk_large_data` (
    `k1` BIGINT NOT NULL,
    `v1` VARCHAR(1000),
    `v2` BIGINT
)
PRIMARY KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "4"
);
INSERT INTO pk_large_data SELECT generate_series, REPEAT('x', 100), generate_series * 10 FROM TABLE(generate_series(1, 1000));
INSERT INTO pk_large_data SELECT generate_series, REPEAT('y', 100), generate_series * 10 FROM TABLE(generate_series(1001, 2000));
INSERT INTO pk_large_data SELECT generate_series, REPEAT('z', 100), generate_series * 10 FROM TABLE(generate_series(2001, 3000));
INSERT INTO pk_large_data SELECT generate_series, REPEAT('w', 100), generate_series * 10 FROM TABLE(generate_series(3001, 4000));
INSERT INTO pk_large_data SELECT generate_series, REPEAT('v', 100), generate_series * 10 FROM TABLE(generate_series(4001, 5000));
CREATE TABLE `pk_multi_buckets` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` VARCHAR(100),
    `v2` BIGINT
)
PRIMARY KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 3
PROPERTIES (
    "lake_compaction_max_parallel" = "2"
);
INSERT INTO pk_multi_buckets VALUES (1, 1, 'a', 100), (2, 2, 'b', 200), (3, 3, 'c', 300);
INSERT INTO pk_multi_buckets VALUES (4, 4, 'd', 400), (5, 5, 'e', 500), (6, 6, 'f', 600);
INSERT INTO pk_multi_buckets VALUES (7, 7, 'g', 700), (8, 8, 'h', 800), (9, 9, 'i', 900);
INSERT INTO pk_multi_buckets VALUES (10, 10, 'j', 1000), (11, 11, 'k', 1100), (12, 12, 'l', 1200);
INSERT INTO pk_multi_buckets VALUES (13, 13, 'm', 1300), (14, 14, 'n', 1400), (15, 15, 'o', 1500);