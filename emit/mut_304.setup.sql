CREATE TABLE `t_dict_mapping_streamload` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `id2` bigint(20) NOT NULL AUTO_INCREMENT COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dict_mapping_streamload VALUES (1, DEFAULT),(2, DEFAULT),(3, DEFAULT);
CREATE TABLE `test_table` (
  `id1` bigint(20) NOT NULL COMMENT "",
  `id2` bigint(20) NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id1`)
DISTRIBUTED BY HASH(`id1`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
DROP TABLE t_dict_mapping_streamload;
create table dict(col_1 int, col_2 string, col_3 bigint not null auto_increment, col_4 int)
primary key(col_1, col_2)
distributed by hash(col_1, col_2)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO dict VALUES (1, "abc", default, 0);
create table t(col_1 int, col_2 string)
primary key(col_1)
distributed by hash(col_1)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO t VALUES (1, "abc");
create table dict(col_1 int, col_2 string, col_3 bigint not null auto_increment, col_4 int)
primary key(col_1, col_2)
distributed by hash(col_1, col_2)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO dict VALUES (1, "abc", default, 0);
create table t(col_1 int, col_2 string)
primary key(col_1)
distributed by hash(col_1)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO t VALUES (1, NULL);
create table dict(col_1 int, col_2 string, col_3 bigint not null auto_increment, col_4 int)
                primary key(col_1)
                distributed by hash(col_1)
PROPERTIES (
"replication_num" = "1"
);
insert into dict values(1, 'hello world 1', default, 1 * 10);
insert into dict values(2, 'hello world 2', default, 2 * 10);
insert into dict values(3, 'hello world 3', default, 3 * 10);
insert into dict values(4, 'hello world 4', default, 4 * 10);
insert into dict values(5, 'hello world 5', default, 5 * 10);
insert into dict values(6, 'hello world 6', default, 6 * 10);
insert into dict values(7, 'hello world 7', default, 7 * 10);
insert into dict values(8, 'hello world 8', default, 8 * 10);
insert into dict values(9, 'hello world 9', default, 9 * 10);
insert into dict values(10, 'hello world 10', default, 10 * 10);
create table fact_tbl_2(col_i int, col_2 varchar(20), col_mapvalue bigint)
PROPERTIES (
"replication_num" = "1"
);
insert into fact_tbl_2
                values
                (1, 'Beijing', DICT_mapping("dict", 1)),
                (2, 'Shenzhen', DICT_MAPping("dict", 2, "col_3")),
                (3, 'Shanghai', Dict_Mapping("dict", 3, "col_4"));
create table dict(col_1 int, col_2 string, col_3 bigint not null auto_increment, col_4 int)
primary key(col_1, col_2)
distributed by hash(col_1, col_2)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO dict VALUES (1, "abc", default, 0);
create table t_dictmapping_add_generated_column_with_dict_mapping(col_1 int, col_2 string)
primary key(col_1)
distributed by hash(col_1)
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO t_dictmapping_add_generated_column_with_dict_mapping VALUES (1, "abc");
ALTER TABLE t_dictmapping_add_generated_column_with_dict_mapping ADD COLUMN newcol BIGINT AS
dict_mapping("dict", col_1, col_2);
DROP TABLE t_dictmapping_add_generated_column_with_dict_mapping;
create table dict(col_1 int, col_2 string, col_3 bigint not null auto_increment, col_4 int)
primary key(col_1)
distributed by hash(col_1)
PROPERTIES (
"replication_num" = "1"
);
create table fact_tbl_1(col_1 int, col_2 varchar(20), col_mapvalue bigint as Dict_Mapping("dict", COL_1, false))
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t_dictmapping_null_if_not_found` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` BIGINT AUTO_INCREMENT
) ENGINE=OLAP 
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into t_dictmapping_null_if_not_found values (1,default);
drop table t_dictmapping_null_if_not_found;
CREATE TABLE `t_dictmapping_multiple_open_crash` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` BIGINT AUTO_INCREMENT
) ENGINE=OLAP 
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t_dictmapping_multiple_open_crash VALUES (1, DEFAULT);
DROP TABLE t_dictmapping_multiple_open_crash;