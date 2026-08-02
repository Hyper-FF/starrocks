CREATE TABLE `allstring` (
  `v1` varchar(20) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
insert into allstring select * from (select 'C4' union select 'A10' union select 1 )tb;
insert into allstring select * from (select 'C8' union select 'A10' union select 1 ) tb;