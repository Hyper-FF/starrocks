CREATE TABLE `t_empty` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` BIGINT NOT NULL COMMENT "",
  `id3` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
DROP TABLE t_empty;
CREATE TABLE `t_basic_operation` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` BIGINT NOT NULL COMMENT "",
  `id3` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_basic_operation VALUES (1, 2, 3);
DROP TABLE test_dictionary_basic_operation;
CREATE TABLE `t_type_combination` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` TINYINT NOT NULL COMMENT "",
  `id3` INT NOT NULL COMMENT "",
  `id4` BOOLEAN NOT NULL COMMENT "",
  `id5` LARGEINT NOT NULL COMMENT "",
  `id6` VARCHAR(2000) NOT NULL COMMENT "",
  `id7` DATE NOT NULL COMMENT "",
  `id8` DATETIME NOT NULL COMMENT "",
  `id9` SMALLINT NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_type_combination VALUES (1, 2, 3, "true", 5, "a", "2020-01-01", "2020-01-01 00:00:00", 6);
DROP TABLE t_type_combination;
CREATE TABLE `t_dictionary_definition` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` STRING NOT NULL COMMENT "",
  `id3` DATE  NULL COMMENT "",
  `id4` DATETIME NOT NULL COMMENT "",
  `id5` STRING  NULL COMMENT "",
  `id6` DATE NULL COMMENT "",
  `id7` DATETIME NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictionary_definition VALUES (1, "a", "2020-01-01", "2020-01-01 00:00:00", "a", "2020-01-01", "2020-01-01 00:00:00");
INSERT INTO t_dictionary_definition VALUES (1, "a", NULL, "2020-01-01 00:00:00", NULL, NULL, "2020-01-01 00:00:00");
INSERT INTO t_dictionary_definition VALUES (1, "a", "2020-01-01", "2020-01-01 00:00:00", "a", "2020-01-01", "2020-01-01 00:00:00");
DROP TABLE t_dictionary_definition;
CREATE TABLE `t_dictionary_error` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` BIGINT NOT NULL COMMENT "",
  `id3` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictionary_error VALUES (1, 2, 3);
DROP TABLE t_dictionary_error;
CREATE TABLE `t_dictionary_source_1` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` BIGINT NOT NULL COMMENT "",
  `id3` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
CREATE TABLE `t_dictionary_source_2` (
  `id4` BIGINT NOT NULL COMMENT "",
  `id5` BIGINT NOT NULL COMMENT "",
  `id6` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id4`)
DISTRIBUTED BY HASH(`id4`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictionary_source_1 VALUES (1, 2, 3);
INSERT INTO t_dictionary_source_2 VALUES (4, 5, 6);
DROP VIEW IF EXISTS test_dictionary_source_view;
CREATE VIEW test_dictionary_source_view AS
SELECT t_dictionary_source_1.id1, t_dictionary_source_1.id2, t_dictionary_source_1.id3,
t_dictionary_source_2.id4, t_dictionary_source_2.id5, t_dictionary_source_2.id6 FROM t_dictionary_source_1, t_dictionary_source_2;
DROP VIEW test_dictionary_source_view;
CREATE MATERIALIZED VIEW test_dictionary_source_mv
DISTRIBUTED BY HASH(`id1`)
REFRESH ASYNC
AS SELECT
    t_dictionary_source_1.id1, t_dictionary_source_1.id2, t_dictionary_source_1.id3,
    t_dictionary_source_2.id4, t_dictionary_source_2.id5, t_dictionary_source_2.id6
FROM t_dictionary_source_1, t_dictionary_source_2;
DROP TABLE t_dictionary_source_1;
DROP TABLE t_dictionary_source_2;
CREATE TABLE `t_dictionary_insert_1` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
CREATE TABLE `t_dictionary_insert_2` (
  `id3` BIGINT NOT NULL COMMENT "",
  `id4` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`id3`)
DISTRIBUTED BY HASH(`id3`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictionary_insert_1 VALUES (1, 2);
INSERT INTO t_dictionary_insert_2 VALUES (1, dictionary_get("test_dictionary_insert", 1)[1]);
DROP TABLE t_dictionary_insert_1;
DROP TABLE t_dictionary_insert_2;
CREATE TABLE `t_dictionary_generated_column_create_table_1` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
CREATE TABLE `t_dictionary_generated_column_create_table_2` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` BIGINT NOT NULL COMMENT "",
  `id3` BIGINT AS dictionary_get("test_dictionary_generated_column_create_table", id1)[1]
) ENGINE=OLAP
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
DROP TABLE t_dictionary_generated_column_create_table_1;
DROP TABLE t_dictionary_generated_column_create_table_2;
CREATE TABLE `t_dictionary_common_expression` (
  `id1` BIGINT NOT NULL COMMENT "",
  `id2` BIGINT NOT NULL COMMENT "",
  `id3` BIGINT NOT NULL COMMENT "",
  `id4` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictionary_common_expression VALUES (1,2,3,4);
DROP TABLE t_dictionary_common_expression;
CREATE TABLE `t_dictionary_show_create_table_gen_col_1` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictionary_show_create_table_gen_col_1 VALUES (1,1);
CREATE TABLE `t_dictionary_show_create_table_gen_col_2` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` BIGINT AS dictionary_get("test_dictionary_show_create_table_gen_col", k)[1] COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictionary_show_create_table_gen_col_2 VALUES (1);
DROP TABLE t_dictionary_show_create_table_gen_col_1;
DROP TABLE t_dictionary_show_create_table_gen_col_2;
CREATE TABLE `t_dictionary_null_if_not_exist` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictionary_null_if_not_exist VALUES (1,1),(3,3),(5,5);
INSERT INTO t_dictionary_null_if_not_exist VALUES (2,2),(4,4);
CREATE TABLE `t_dictionary_null_if_not_exist_gen_column` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` BIGINT AS dictionary_get("test_dictionary_null_if_not_exist", k)[1]
) ENGINE=OLAP
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictionary_null_if_not_exist_gen_column VALUES (1),(2),(3),(4),(5);
INSERT into t_dictionary_null_if_not_exist select generate_series, generate_series from Table(generate_series(1, 1000));
INSERT into t_dictionary_null_if_not_exist select generate_series, generate_series from Table(generate_series(1001, 2000));
DROP TABLE t_dictionary_null_if_not_exist;
CREATE TABLE `t_dictionary_null_if_not_exist` (
  `k` BIGINT NOT NULL COMMENT "",
  `v1` BIGINT NOT NULL COMMENT "",
  `v2` BIGINT NOT NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT into t_dictionary_null_if_not_exist select generate_series, generate_series, generate_series from Table(generate_series(1, 1000));
INSERT into t_dictionary_null_if_not_exist select generate_series, generate_series, generate_series from Table(generate_series(1001, 2000));
DROP TABLE t_dictionary_null_if_not_exist;