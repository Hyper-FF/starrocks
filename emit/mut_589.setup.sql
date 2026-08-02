CREATE TABLE ngram_index(
    timestamp DATETIME NOT NULL,
    username STRING,
    price INT NULL,
    INDEX idx_name1(username) USING NGRAMBF ("gram_num" = "4", "bloom_filter_fpp" = "0.05")
)PROPERTIES ("replication_num" = "1");
insert into ngram_index values ('2023-01-01',"chinese",3),('2023-01-02',"chineaaaaaaaaaaaab",4),('2023-01-03',"我爱吃烤全羊yangyangchin",4),('2023-01-04',"tonightisgreadnight",4);
drop index idx_name1 on ngram_index;
ALTER TABLE ngram_index ADD INDEX idx_name1(username) USING NGRAMBF ('gram_num' = "4", "bloom_filter_fpp" = "0.01");
CREATE TABLE ngram_index_default_1(
    timestamp DATETIME NOT NULL,
    username STRING,
    price INT NULL,
    INDEX idx_name1(username) USING NGRAMBF ("gram_num" = "4")
)PROPERTIES ("replication_num" = "1");
CREATE TABLE ngram_index_default_2(
    timestamp DATETIME NOT NULL,
    username STRING,
    price INT NULL,
    INDEX idx_name1(username) USING NGRAMBF ("bloom_filter_fpp" = "0.05")
)PROPERTIES ("replication_num" = "1");
insert into ngram_index_default_2 values ('2023-01-01',"chinese",3),('2023-01-02',"chineaaaaaaaaaaaab",4),('2023-01-03',"我爱吃烤全羊yangyangchin",4),('2023-01-04',"tonightisgreadnight",4);
CREATE TABLE ngram_index_default_3(
    timestamp DATETIME NOT NULL,
    username STRING,
    price INT NULL,
    INDEX idx_name1(username) USING NGRAMBF
)PROPERTIES ("replication_num" = "1");
CREATE TABLE ngram_index_like(
    timestamp DATETIME NOT NULL,
    username STRING,
    price INT NULL,
    INDEX idx_name1(username) USING NGRAMBF ("gram_num" = "4", "bloom_filter_fpp" = "0.05")
)PROPERTIES ("replication_num" = "1");
insert into ngram_index_like values ('2023-01-01',"hina",3);
insert into ngram_index_like values ('2023-01-01',"chinese",3);
CREATE TABLE ngram_index_case_in_sensitive(
    timestamp DATETIME NOT NULL,
    Username STRING,
    price INT NULL
)PROPERTIES ("replication_num" = "1");
insert into ngram_index_case_in_sensitive values ('2023-01-01',"aAbac",3);
insert into ngram_index_case_in_sensitive values ('2023-01-01',"AaBAa",3);
ALTER TABLE ngram_index_case_in_sensitive ADD INDEX idx_name1(Username) USING NGRAMBF ('gram_num' = "4", "bloom_filter_fpp" = "0.01");
drop index idx_name1 on ngram_index_case_in_sensitive;
ALTER TABLE ngram_index_case_in_sensitive ADD INDEX idx_name1(Username) USING NGRAMBF ('gram_num' = "4", "bloom_filter_fpp" = "0.05", "case_sensitive" = "false");
CREATE TABLE ngram_index_char(
    timestamp DATETIME NOT NULL,
    username char(20) NOT NULL,
    price INT NULL,
    INDEX idx_name1(username) USING NGRAMBF ("gram_num" = "4", "bloom_filter_fpp" = "0.05")
)PROPERTIES ("replication_num" = "1");
insert into ngram_index_char values ('2023-01-01',"chinese",3),('2023-01-02',"chineaaa",4),('2023-01-03',"我爱chin",4),('2023-01-04',"toniggrht",4);
CREATE TABLE `common_duplicate` (
  `c0` int(11) NOT NULL COMMENT "",
  `c1` int(11) NOT NULL COMMENT "",
  `c2` varchar(500)  NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`, `c2`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"bloom_filter_columns" = "c1,c2",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into common_duplicate values (1,1,"abc"),(2,2,"abc"),(3,3,"abc"),(4,4,"abc"),(5,5,"abc"),(6,6,NULL);