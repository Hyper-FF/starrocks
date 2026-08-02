drop table if exists t_initcap_test;
create table t_initcap_test (
    id int,
    str varchar(100)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
insert into t_initcap_test values
(1, 'hello world'),
(2, 'HELLO WORLD'),
(3, '1st place'),
(4, NULL),
(5, ''),
(6, '   spaces   '),
(7, 'complex-string_here.123');