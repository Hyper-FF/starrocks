create table t0(c0 varchar(16), c1 INT(16))
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
insert into t0 values ('test', 8), ('test', 2), ('test', 0);
create table t1(c0 varchar(20), c1 varchar(20))
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
insert into t1 values ('hello world', 'abc##567###234');
CREATE TABLE IF NOT EXISTS `test_url_extract_host` (
  `id` varchar(10),
  `url` varchar(100)
)
PROPERTIES(
  "replication_num" = "1"
);
insert into test_url_extract_host(id,url)
values ('1', 'https://starrocks.com/doc?k1=10&k2=3&k1=100'),
       ('2', 'https://starrocks.快速.com/doc?k1=10&k2=3&k1=100');
create table t2(c0 varchar(20), c1 varchar(20))
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
insert into t2 values ('hello world', 'com.mysql.com');
create table crc01(c0 varchar(20), c1 varchar(20), c2 varchar(20))
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
insert into crc01 values ('hello world', 'com.mysql.com', "镜舟科技");
CREATE TABLE `string_table` (
  `rowkey` varchar(300) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`rowkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowkey`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1",
    "storage_volume" = "builtin_storage_volume",
    "enable_persistent_index" = "true",
    "compression" = "LZ4"
);
insert into string_table values
("000073a7-274f-46bf-bfaf-678868cc26cd"),
("e6249ba1-5b54-46bf-bfaf-89d69094b757"),
("93da4b36-5401-46bf-bfa7-2bde65779623"),
("2548c7aa-d94f-46bf-b0a4-d769f248cbb2"),
("1bd32347-274f-4a30-93f3-9087594de9cd");
CREATE TABLE __row_util_1 (
  k1 bigint null
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 48
PROPERTIES (
    "replication_num" = "1"
);
insert into __row_util_1 select generate_series from TABLE(generate_series(0, 5000));
CREATE TABLE left_table (
    id int,
    nation string
)
ENGINE=olap
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) buckets 32
PROPERTIES (
    "replication_num" = "1" 
);
insert into left_table 
select
    cast(rand() * 100000000 as int),
    CASE 
        WHEN k1 % 5 = 0 THEN 'china'
        WHEN k1 % 5 = 1 THEN 'usa'
        WHEN k1 % 5 = 2 THEN 'russian'
        WHEN k1 % 5 = 3 THEN 'canada'
        ELSE 'japan'
    END
from __row_util_1;
CREATE TABLE utf8_names (
  id INT,
  name_cyrillic VARCHAR(100),
  name_latin VARCHAR(100)
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES ('replication_num'='1');
INSERT INTO utf8_names VALUES
(1, 'Расулов Сунатулло Ашуралиевич', 'Rasulov Sunatullo Ashuralevich'),
(2, 'Бабоев Мухаммаджон Алиджонович', 'Baboev Mukhammadjon Alidjonovich'),
(3, 'Акрамов Хусейн Махмадалиевич', 'Akramov Khusein Makhmadalievich'),
(4, 'Назаров Назарали Захирджонович', 'Nazarov Nazarali Zakhirdjonovich');
create table t (
  id int,
  name string
) duplicate key(id)
distributed by random buckets 3
properties("replication_num" = "1");
insert into t values(1, 'теКст'), (2, 'ТЕкСТ'), (3, 'теКст hello'), (4, 'ТЕкСТ world'),
(5, 'München'), (6, 'München Tum'), (7, 'hello WOrld'), (8, ''),
(9, 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'), (10, 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ'),
(11, 'abcdefghijklmnopqrstuvwxyzäöüß'), (12, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜẞ'),
(13, 'abcçdefgğhıijklmnoöprsştuüvyz'), (14, 'ABCÇDEFGĞHIİJKLMNOÖPRSŞTUÜVYZ'),
(15, 'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ'), (16, 'ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ');
create table t_strpos(c0 varchar(20), c1 varchar(20), c2 int)
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
insert into t_strpos values ('hello world', 'world', 1), ('hello world', 'world', -1), ('abcabc', 'abc', 2);
create table t_format_bytes(bytes bigint)
        DUPLICATE KEY(bytes)
        DISTRIBUTED BY HASH(bytes)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
insert into t_format_bytes values (0), (123), (1024), (1048576), (1073741824), (-1), (null);
create table t_raise_error(id int, msg varchar(100))
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
insert into t_raise_error values (1, null), (2, null);