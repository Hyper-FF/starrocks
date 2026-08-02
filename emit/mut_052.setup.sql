CREATE TABLE `test_pc` (
  `date` date NULL COMMENT "",
  `datetime` datetime NULL COMMENT "",
  `db` double NULL COMMENT "",
  `id` int(11) NULL COMMENT "",
  `name` varchar(255) NULL COMMENT "",
  `subject` varchar(255) NULL COMMENT "",
  `score` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`date`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into test_pc values ("2018-01-01","2018-01-01 00:00:01",11.1,1,"Tom","English",90);
insert into test_pc values ("2019-01-01","2019-01-01 00:00:01",11.2,1,"Tom","English",91);
insert into test_pc values ("2020-01-01","2020-01-01 00:00:01",11.3,1,"Tom","English",92);
insert into test_pc values ("2021-01-01","2021-01-01 00:00:01",11.4,1,"Tom","English",93);
insert into test_pc values ("2022-01-01","2022-01-01 00:00:01",11.5,1,"Tom","English",94);
insert into test_pc values (NULL,NULL,NULL,NULL,"Tom","English",NULL);