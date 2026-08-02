create table t_pj (id int, s varchar(100)) properties('replication_num' = '1');
insert into t_pj values (1, '{"a": 1}'), (2, '{'), (3, '[1,2,3]'), (4, '{"a":}');
create table t_dst (id int, j json) properties('replication_num' = '1');
insert into t_dst select id, parse_json(s) from t_pj;
drop table if exists t_dst;
drop table if exists t_pj;