CREATE TABLE `ss` (
  `id` int(11) NULL COMMENT "",
  `name` varchar(255) NULL COMMENT "",
  `subject` varchar(255) NULL COMMENT "",
  `score` int(11) NULL COMMENT "",
   arr array<int>,
   mmap map<int,varchar(20)>
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into ss values (1,"Tom","English",90, [1,2],map{2:'name'});
insert into ss values (1,"Tom","Math",80, [1,2], map{2:'name'});
insert into ss values (2,"Tom","English",NULL,[], map{});
insert into ss values (2,"Tom",NULL,NULL,null,null);
insert into ss values (3,"May",NULL,NULL,[2],map{3:'3',4:'4'});
insert into ss values (3,"Ti","English",98, [null],map{null:null});
insert into ss values (4,NULL,NULL,NULL,null,null);
insert into ss values (NULL,NULL,NULL,NULL,null,null);
insert into ss values (NULL,"Ti","物理Phy",99,[3,4],map{9:''});
insert into ss values (11,"张三此地无银三百两","英文English",98,[0,2], map{});
insert into ss values (11,"张三掩耳盗铃","Math数学欧拉方程",78,[],map{7:'y'});
insert into ss values (12,"李四大闹天空","英语外语美誉",NULL,[89], map{6:'6'});
insert into ss values (2,"王武程咬金","语文北京上海",22,[23],map{8:''});
insert into ss values (3,"欧阳诸葛方程","数学大不列颠",NULL,[],null);
drop table if exists test_array_agg;
create table test_array_agg (
    id INT,
    col_boolean BOOLEAN,
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_int INT,
    col_bigint BIGINT,
    col_largeint LARGEINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_varchar VARCHAR(100),
    col_char CHAR(10),
    col_datetime DATETIME,
    col_date DATE,
    col_array ARRAY<INT>,
    col_map MAP<STRING, INT>,
    col_struct STRUCT<f1 INT, f2 STRING>
) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
insert into test_array_agg values
(1, true, 10, 100, 1000, 10000, 100000, 1.1, 2.2, 'hello', 'char_test', '2024-01-01 12:00:00', '2024-01-01', [1,2,3], map{"key1": 1, "key2": 2}, row(1, "test1")),
(2, false, 20, 200, 2000, 20000, 200000, 3.3, 4.4, 'world', 'char_test2', '2024-02-02 13:00:00', '2024-02-02', [4,5,6], map{"key3": 3, "key4": 4}, row(2, "test2")),
(3, null, 30, 300, 3000, 30000, 300000, 5.5, 6.6, null, null, null, null, null, null, null),
(4, true, 10, 100, 1000, 10000, 100000, 1.1, 2.2, 'hello', 'char_test', '2024-01-01 12:00:00', '2024-01-01', [1,2,3], map{"key1": 1, "key2": 2}, row(1, "test1")),
(5, false, 20, 200, 2000, 20000, 200000, 3.3, 4.4, 'world', 'char_test2', '2024-02-02 13:00:00', '2024-02-02', [4,5,6], map{"key3": 3, "key4": 4}, row(2, "test2")),
(1, true, 10, 100, 1000, 10000, 100000, 1.1, 2.2, 'hello', 'char_test', '2024-01-01 12:00:00', '2024-01-01', [1,2,3], map{"key1": 1, "key2": 2}, row(1, "test1")),
(2, false, 20, 200, 2000, 20000, 200000, 3.3, 4.4, 'world', 'char_test2', '2024-02-02 13:00:00', '2024-02-02', [4,5,6], map{"key3": 3, "key4": 4}, row(2, "test2"));