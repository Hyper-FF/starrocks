create table t_base64_bitmap_test (k int);
insert into t_base64_bitmap_test values (1), (2), (3);
drop table t_base64_bitmap_test;
create table t_base64_bitmap_pred (k int) properties('replication_num'='1');
insert into t_base64_bitmap_pred values (90), (95), (100), (200), (50);
drop table t_base64_bitmap_pred;
create table t_base64_bitmap_nonconst (k int, b varchar(200));
insert into t_base64_bitmap_nonconst values (1, 'CgsAAABaAAAAAAAAAFsAAAAAAAAAXAAAAAAAAABdAAAAAAAAAF4AAAAAAAAAXwAAAAAAAABgAAAAAAAAAGEAAAAAAAAAYgAAAAAAAABjAAAAAAAAAGQAAAAAAAA');
drop table t_base64_bitmap_nonconst;