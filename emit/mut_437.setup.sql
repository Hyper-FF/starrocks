CREATE TABLE `ts` (
  `str` varchar(65533) NULL COMMENT "",
  `regex` varchar(65533) NULL COMMENT "",
  `replaced` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");
insert into ts values ('abcd', '.*', 'xx'), ('abcd', 'a.*', 'xx'), ('abcd', '.*abc.*', 'xx'), ('abcd', '.*cd', 'xx'), ('abcd', 'bc', 'xx'), ('', '', 'xx'), (NULL, '', 'xx'), ('abc中文def', '[\\p{Han}]+', 'xx');
insert into ts values ('a b c', " ", "-"), ('           XXXX', '       ', '');
insert into ts values ('xxxx', "x", "-"), ('xxxx', "xx", "-"), ('xxxx', "xxx", "-"), ('xxxx', "xxxx", "-");
insert into ts values ('xxxx', "not", "xxxxxxxx"), ('xxaxx', 'xx', 'aaa'), ('xaxaxax', 'xax', '-');
CREATE TABLE `tsr` (
  `str` varchar(65533) NULL COMMENT "",
  `regex` varchar(65533) NULL COMMENT "",
  `pos` int NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");
insert into tsr values ("AbCdExCeF", "([[:lower:]]+)C([[:lower:]]+)", 3), ("AbCdExCeF", "([[:lower:]]+)C([[:lower:]]+)", 0);
CREATE TABLE `tsp_general` (
  `str` varchar(65533) NULL COMMENT "",
  `pattern` varchar(65533) NULL COMMENT "",
  `start_pos` int NULL COMMENT "",
  `occurrence` int NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");
insert into tsp_general values 
('abcdef123ghi', '[0-9]+', 1, 1),
('abc def ghi', '\\s+', 1, 1),
('test@email.com', '@[a-zA-Z]+', 1, 1),
('hello world hello universe', 'hello', 1, 2),
('complex(pattern)test', '\\([^)]*\\)', 1, 1),
('abc123def456ghi', '[0-9]{2,}', 1, 2),
('', '.', 1, 1),
(NULL, '[0-9]', 1, 1),
('test string', NULL, 1, 1),
(NULL, NULL, 1, 1);
CREATE TABLE `tsc` (
  `str` varchar(65533) NULL COMMENT "",
  `regex` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");
insert into tsc values ('abc123def456', '[0-9]'), ('test.com test.net test.org', '\\.'), ('a b  c   d', '\\s+'), ('ababababab', 'ab'), ('', '.'), (NULL, '.');
CREATE TABLE `tsc_invalid` (
  `str` varchar(65533) NULL COMMENT "",
  `regex` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");
insert into tsc_invalid values 
('abc123def456', '[0-9'),
('test string', '(unclosed'),
('repetition test', '?invalid'),
('valid test', 'valid');
CREATE TABLE `test_regexp_replace_multirow` (
  `column_value` varchar(100) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`column_value`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`column_value`) BUCKETS 1 PROPERTIES ("replication_num" = "1");
insert into test_regexp_replace_multirow values 
('activity_60_package_2'),
('activity_50_package_7'),
('activity_70_package_1'),
('nomatch_value'),
('test_package_suffix');
DROP TABLE test_regexp_replace_multirow;