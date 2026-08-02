CREATE TABLE `t_test_basic_create_index_pk` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `id2` bigint(20) NOT NULL COMMENT "",
  `id3` bigint(20) NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE INDEX index_1 ON t_test_basic_create_index_pk (id2) USING BITMAP;
CREATE INDEX index_2 ON t_test_basic_create_index_pk (id3) USING BITMAP;
CREATE TABLE `t_test_basic_create_index_dup` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `id2` bigint(20) NOT NULL COMMENT "",
  `id3` bigint(20) NOT NULL COMMENT "",
  `id4` string NOT NULL COMMENT "",
  `id5` string NOT NULL COMMENT "",
  `id6` bigint(20) NOT NULL COMMENT "",
  `id7` string NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE INDEX index_1 ON t_test_basic_create_index_dup (id2) USING BITMAP;
CREATE INDEX index_2 ON t_test_basic_create_index_dup (id3) USING BITMAP;
CREATE INDEX index_3 ON t_test_basic_create_index_dup (id4) USING GIN;
CREATE INDEX index_4 ON t_test_basic_create_index_dup (id5) USING GIN;
CREATE INDEX index_5 ON t_test_basic_create_index_dup (id6) USING BITMAP;
CREATE INDEX index_6 ON t_test_basic_create_index_dup (id7) USING GIN;
CREATE TABLE `t_test_basic_create_index_replicated` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `id2` bigint(20) NOT NULL COMMENT "",
  `id3` bigint(20) NOT NULL COMMENT "",
  `id4` string NOT NULL COMMENT "",
  `id5` string NOT NULL COMMENT "",
  `id6` bigint(20) NOT NULL COMMENT "",
  `id7` string NOT NULL COMMENT "",
  INDEX `gin_id4` (`id4`) USING GIN ("parser" = "none") COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
DROP TABLE t_test_basic_create_index_pk;
DROP TABLE t_test_basic_create_index_dup;
CREATE TABLE `t_test_gin_index_query` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `query_none_analyzer` varchar(255) NOT NULL COMMENT "",
  `query_english` varchar(255) NOT NULL COMMENT "",
  `query_chinese` varchar(255) NOT NULL COMMENT "",
   INDEX gin_none (`query_none_analyzer`) USING GIN ("parser" = "none") COMMENT 'whole line index',
   INDEX gin_english (`query_english`) USING GIN ("parser" = "english") COMMENT 'english index',
   INDEX gin_chinese (`query_chinese`) USING GIN ("parser" = "chinese") COMMENT 'chinese index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
insert into t_test_gin_index_query values
(1, 'starrocks', 'hello starrocks', '极速分析'),
(2, 'starrocks', 'hello world', '你好世界'),
(3, 'lakehouse', 'hello lakehouse', '湖仓一体'),
(4, 'materialized view', 'materialized view', '物化视图'),
(5, '中文测试', 'chinese test', '中文测试');
drop table t_test_gin_index_query;
CREATE TABLE `t_gin_index_single_predicate_none` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_none (`text_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_index_single_predicate_none VALUES
(1, "ABC"),
(2, "abc"),
(3, "ABD"),
(4, "This is Gin Index"),
(5, NULL);
DROP TABLE t_gin_index_single_predicate_none;
CREATE TABLE `t_gin_index_single_predicate_english` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_english (`text_column`) USING GIN ("parser" = "english") COMMENT 'english index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_index_single_predicate_english VALUES
(1, "ABC"),
(2, "abc"),
(3, "ABD"),
(4, "This is Gin Index"),
(5, NULL);
DROP TABLE t_gin_index_single_predicate_english;
CREATE TABLE `t_gin_index_multiple_predicate_none` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_none (`text_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_index_multiple_predicate_none VALUES
(1, "ABC"),
(2, "abc"),
(3, "ABD"),
(4, "This is Gin Index"),
(5, NULL);
DROP TABLE t_gin_index_multiple_predicate_none;
CREATE TABLE `t_gin_index_multiple_predicate_english` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_english (`text_column`) USING GIN ("parser" = "english") COMMENT 'english index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_index_multiple_predicate_english VALUES
(1, "ABC"),
(2, "abc"),
(3, "ABD"),
(4, "This is Gin Index"),
(5, NULL);
DROP TABLE t_gin_index_multiple_predicate_english;
CREATE TABLE `t_gin_index_compaction_none_base` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_none (`text_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_index_compaction_none_base VALUES (1, "abc");
INSERT INTO t_gin_index_compaction_none_base VALUES (2, "ABC");
INSERT INTO t_gin_index_compaction_none_base VALUES (3, "bcd");
INSERT INTO t_gin_index_compaction_none_base VALUES (4, "BCD");
ALTER TABLE t_gin_index_compaction_none_base BASE COMPACT;
CREATE TABLE `t_gin_index_compaction_none_cumu` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_none (`text_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_index_compaction_none_cumu VALUES (1, "abc");
INSERT INTO t_gin_index_compaction_none_cumu VALUES (2, "ABC");
INSERT INTO t_gin_index_compaction_none_cumu VALUES (3, "bcd");
INSERT INTO t_gin_index_compaction_none_cumu VALUES (4, "BCD");
ALTER TABLE t_gin_index_compaction_none_cumu CUMULATIVE COMPACT;
DROP TABLE t_gin_index_compaction_none_base;
DROP TABLE t_gin_index_compaction_none_cumu;
CREATE TABLE `t_gin_index_compaction_english_base` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_english (`text_column`) USING GIN ("parser" = "english") COMMENT 'english index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_index_compaction_english_base VALUES (1, "This is Gin Index");
INSERT INTO t_gin_index_compaction_english_base VALUES (2, "This is Not Gin Index");
INSERT INTO t_gin_index_compaction_english_base VALUES (3, "Gin Index");
ALTER TABLE t_gin_index_compaction_english_base BASE COMPACT;
CREATE TABLE `t_gin_index_compaction_english_cumu` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_english (`text_column`) USING GIN ("parser" = "english") COMMENT 'english index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_index_compaction_english_cumu VALUES (1, "This is Gin Index");
INSERT INTO t_gin_index_compaction_english_cumu VALUES (2, "This is Not Gin Index");
INSERT INTO t_gin_index_compaction_english_cumu VALUES (3, "Gin Index");
ALTER TABLE t_gin_index_compaction_english_cumu CUMULATIVE COMPACT;
DROP TABLE t_gin_index_compaction_english_base;
DROP TABLE t_gin_index_compaction_english_cumu;
CREATE TABLE `t_gin_index_type_1` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `test_column` varchar(255) NULL COMMENT "",
  INDEX gin_none (`test_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t_gin_index_type_2` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `test_column` String NULL COMMENT "",
  INDEX gin_none (`test_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t_gin_index_type_3` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `test_column` CHAR NULL COMMENT "",
  INDEX gin_none (`test_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t_gin_index_type_4` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `test_column` BIGINT NULL COMMENT "",
  INDEX gin_none (`test_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t_gin_index_type_5` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `test_column` DOUBLE NULL COMMENT "",
  INDEX gin_none (`test_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t_gin_index_type_6` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `test_column` DATETIME NULL COMMENT "",
  INDEX gin_none (`test_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t_gin_index_type_7` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `test_column` DATE NULL COMMENT "",
  INDEX gin_none (`test_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t_clone_for_gin` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column_1` varchar(255) NULL COMMENT "",
  `text_column_2` varchar(255) NULL COMMENT "",
  `text_column_3` varchar(255) NULL COMMENT "",
  `text_column_4` varchar(255) NULL COMMENT "",
  INDEX gin_none_1 (`text_column_1`) USING GIN ("parser" = "none") COMMENT 'whole line index',
  INDEX gin_none_2 (`text_column_2`) USING BITMAP,
  INDEX gin_none_3 (`text_column_3`) USING GIN ("parser" = "none") COMMENT 'whole line index',
  INDEX gin_none_4 (`text_column_4`) USING BITMAP
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "2",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_clone_for_gin VALUES (1, "abc","abc","abc","abc"),(2, "ABC","ABC","ABC","ABC");
CREATE TABLE `t_complex_predicate_for_gin_none` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_none (`text_column`) USING GIN ("parser" = "none") COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t_complex_predicate_for_gin_english` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_english (`text_column`) USING GIN ("parser" = "english") COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_complex_predicate_for_gin_none VALUES (1, "abc cbd");
INSERT INTO t_complex_predicate_for_gin_none VALUES (2, "cbd edf");
INSERT INTO t_complex_predicate_for_gin_english VALUES (1, "abc cbd");
INSERT INTO t_complex_predicate_for_gin_english VALUES (2, "cbd edf");
DROP TABLE t_complex_predicate_for_gin_none;
DROP TABLE t_complex_predicate_for_gin_english;
CREATE TABLE `t_delete_and_column_prune` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_none (`text_column`) USING GIN ("parser" = "english") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_delete_and_column_prune VALUES (1, "b"),(2, "b"),(3, "b");
DROP TABLE t_delete_and_column_prune;
CREATE TABLE `t_upper_case_column_name` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `TeXt` varchar(255) NULL COMMENT "",
  INDEX gin_none (`TeXt`) USING GIN ("parser" = "english") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_delete_and_column_prune VALUES (1, "b"),(2, "b"),(3, "b");
DROP TABLE t_upper_case_column_name;
CREATE TABLE `t_alter_replicated_storage` (
  `id` bigint(20) NOT NULL COMMENT "",
  `text` varchar(255) NULL COMMENT "",
  INDEX gin_none (`text`) USING GIN ("parser" = "english")
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
ALTER TABLE t_alter_replicated_storage SET ("replicated_storage" = "true");
DROP TABLE t_alter_replicated_storage;
CREATE TABLE `t_disable_global_dict_rewrite` (
  `id` bigint(20) NOT NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT "",
  `v2` varchar(255) NULL COMMENT "",
  INDEX gin_none (`v1`) USING GIN ("parser" = "english")
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_disable_global_dict_rewrite VALUES (1, "abc", "bcd"), (2, "cbd", "dbs");
DROP TABLE t_disable_global_dict_rewrite;
CREATE TABLE `t_create_mv_with_match` (
  `id` bigint(20) NOT NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT "",
  `v2` varchar(255) NULL COMMENT "",
  INDEX gin_none (`v1`) USING GIN ("parser" = "english")
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_create_mv_with_match VALUES (1, "abc", "bcd");
DROP TABLE t_create_mv_with_match;
CREATE TABLE `t_alter_gin_col_into_other_type` (
  `id` bigint(20) NOT NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT "",
  INDEX gin_none (`v1`) USING GIN ("parser" = "english")
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
ALTER TABLE t_alter_gin_col_into_other_type MODIFY COLUMN v1 BIGINT;
INSERT INTO t_alter_gin_col_into_other_type VALUES (1, "abc");
ALTER TABLE t_alter_gin_col_into_other_type MODIFY COLUMN v1 VARCHAR(2000);
DROP TABLE t_alter_gin_col_into_other_type;
CREATE TABLE `t_gin_var` (
  `id` bigint(20) NOT NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT "",
  INDEX gin_none (`v1`) USING GIN ("parser" = "standard")
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_var VALUES (1, "abc bcd");
DROP TABLE t_gin_var;
CREATE TABLE `t_gin_var` (
  `id` bigint(20) NOT NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT "",
  INDEX gin_none (`v1`) USING GIN ("parser" = "standard")
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
CREATE TABLE `t_gin_var` (
  `id` bigint(20) NOT NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
ALTER TABLE t_gin_var add index idx (v1) USING GIN('parser' = 'standard');
DROP TABLE t_gin_var;
CREATE TABLE `t_gin_var` (
  `id` bigint(20) NOT NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT "",
  INDEX gin_none (`v1`) USING GIN ("parser" = "standard")
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
DROP TABLE t_gin_var;
CREATE TABLE `t_gin_match_empty` (
  `k` BIGINT NOT NULL COMMENT "",
  `v1` string COMMENT "",
  `v2` string COMMENT "",
  `v3` string COMMENT "",
   INDEX idx1 (v1) USING GIN ('parser' = 'english'),
   INDEX idx2 (v2) USING GIN ('parser' = 'chinese'),
   INDEX idx3 (v3) USING GIN ('parser' = 'standard')
) ENGINE=OLAP
DUPLICATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "false"
);
insert into t_gin_match_empty values (1, "中文50中文", "中文50中文", "中文50中文");
DROP TABLE t_gin_match_empty;
CREATE TABLE `t_gin_view` (
  `id` bigint(20) NOT NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT "",
  INDEX gin_none (`v1`) USING GIN ("parser" = "english")
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_gin_view VALUES (1, "abd bcd");
CREATE VIEW test_view1 (column1, column2) AS SELECT * FROM t_gin_view;
DROP TABLE t_gin_view;
CREATE TABLE duplicate_table_demo_datatype_not_replicated_all_varchar ( AAA DATETIME not NULL COMMENT "", BBB VARCHAR(200) not NULL COMMENT "", CCC VARCHAR(200) not NULL COMMENT "", DDD VARCHAR(20000) COMMENT "", EEE LARGEINT  NULL COMMENT "", FFF DECIMAL(20,10) NULL COMMENT "", GGG VARCHAR(200)  NULL COMMENT "", HHH FLOAT  NULL COMMENT "", III BOOLEAN  NULL COMMENT "", KKK CHAR(20)   NULL COMMENT "", LLL STRING   NULL COMMENT "", MMM VARCHAR(20)   NULL COMMENT "", NNN BINARY  NULL COMMENT "", OOO TINYINT NULL COMMENT "", PPP DATETIME NULL COMMENT "", QQQ ARRAY<INT> NULL COMMENT "", RRR JSON NULL COMMENT "", SSS MAP<INT,INT> NULL COMMENT "", TTT STRUCT<a INT, b INT> NULL COMMENT "", INDEX init_bitmap_index (KKK) USING BITMAP ) duplicate KEY(AAA, BBB, CCC) PARTITION BY RANGE (`AAA`) ( START ("1970-01-01") END ("2030-01-01") EVERY (INTERVAL 30 YEAR) ) DISTRIBUTED BY HASH(`AAA`, `BBB`) BUCKETS 3 ORDER BY(`AAA`,`BBB`,`CCC`,`DDD`) PROPERTIES ( "replicated_storage"="false", "replication_num" = "1", "storage_format" = "v2", "enable_persistent_index" = "true", "bloom_filter_columns" = "MMM", "unique_constraints" = "GGG" );
create view test_view (AAA, DDD) as select AAA, max(DDD) from duplicate_table_demo_datatype_not_replicated_all_varchar group by AAA;
CREATE INDEX idx ON duplicate_table_demo_datatype_not_replicated_all_varchar(DDD) USING GIN('parser' = 'english');
insert into duplicate_table_demo_datatype_not_replicated_all_varchar values ('1974-08-20 23:13:25', 'xIjfSXnegdnZiZGQMaxo', 'syHwIOMctmDLDGCibEun', 'hIbilUEGdLbCnaZASCVL', 6299, 25361.52081, 'QuTsacRyxiIkBjEmjhNu', -11.4812925061712, True, 'QcLRdQJMhtPXojJUjkUd', 'yUeFlbzomaPDwKeaHylx', 'WqQyGEjEYpvLzfBXYUCB', '', 8, '2015-11-03 16:31:47', [2621, 5950, 13171], '{"job": "Administrator, Civil Service", "company": "Morris-Anderson", "ssn": "823-67-5554", "residence": "59688 Hanna Shoal Apt. 586\nWest Waynefort, CO 69652", "current_location": ["-64.3777465", "21.079566"], "blood_group": "O-", "website": ["http://young.biz/", "https://cobb-bell.com/", "http://www.roberts-garrison.com/", "http://jones.com/"], "username": "howardarcher", "name": "John Mccullough", "sex": "M", "address": "1361 Susan Mountain\nJasonbury, MI 85084", "mail": "lovejennifer@gmail.com", "birthdate": "1928-06-25"}', null, null);
DROP VIEW test_view;
DROP TABLE duplicate_table_demo_datatype_not_replicated_all_varchar;
CREATE TABLE `t_vertical_compaction` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  `col1` varchar(255) DEFAULT "ABC" COMMENT "",
  `col2` varchar(255) DEFAULT "ABC" COMMENT "",
  `col3` varchar(255) DEFAULT "ABC" COMMENT "",
  `col4` varchar(255) DEFAULT "ABC" COMMENT "",
  `col5` varchar(255) DEFAULT "ABC" COMMENT "",
  `col6` varchar(255) DEFAULT "ABC" COMMENT "",
  `col7` varchar(255) DEFAULT "ABC" COMMENT "",
  `col8` varchar(255) DEFAULT "ABC" COMMENT "",
  `col9` varchar(255) DEFAULT "ABC" COMMENT "",
  `col10` varchar(255) DEFAULT "ABC" COMMENT "",
  INDEX gin_none (`text_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_vertical_compaction (id1, text_column) VALUES (1, "abc");
INSERT INTO t_vertical_compaction (id1, text_column) VALUES (2, "ABC");
INSERT INTO t_vertical_compaction (id1, text_column) VALUES (3, "bcd");
INSERT INTO t_vertical_compaction (id1, text_column) VALUES (4, "BCD");
ALTER TABLE t_vertical_compaction BASE COMPACT;
DROP TABLE t_vertical_compaction;
CREATE TABLE `t_gin_like_underscore` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `text_column` varchar(255) NULL COMMENT "",
  INDEX gin_none (`text_column`) USING GIN ("parser" = "none") COMMENT 'whole line index'
) ENGINE=OLAP
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1
PROPERTIES ("replication_num" = "1", "enable_persistent_index" = "true",
            "replicated_storage" = "false", "compression" = "LZ4");
INSERT INTO t_gin_like_underscore VALUES
(1, "abc"),
(2, "aXc"),
(3, "ac"),
(4, "abbc"),
(5, "a_c"),
(6, "中a文"),
(7, NULL);
DROP TABLE t_gin_like_underscore;