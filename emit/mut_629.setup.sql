CREATE TABLE `t_tokenized_table` (
  `id` bigint(20) NOT NULL COMMENT "",
  `english_text` varchar(255) NULL COMMENT "",
  `standard_text` varchar(255) NULL COMMENT "",
  `chinese_text` varchar(255) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "false",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO t_tokenized_table VALUES
(1, 'hello', 'Привет', '你好'),
(2, 'hello world', 'Привет, мир', '你好世界'),
(3, 'Shanghai tap water comes from the sea', 'Водопроводная вода в Шанхае поступает из моря', '上海自来水来自海上'),
(4, "", "", "");
INSERT INTO t_tokenized_table(id) VALUES
(5);
DROP TABLE t_tokenized_table;