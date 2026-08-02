CREATE TABLE t1 (
    k1 INT,
    k2 VARCHAR(100))
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) PROPERTIES('replication_num'='1');
CREATE TABLE tempty (
    k1 INT,
    k2 VARCHAR(100))
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) PROPERTIES('replication_num'='1');
insert into t1 SELECT generate_series, 100000 - generate_series FROM TABLE(generate_series(1, 300000));
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 1;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 100;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 1000;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 10000;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 100000;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 10000000;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 1;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 100;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 1000;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 10000;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 100000;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 10000000;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 1;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 100;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 1000;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 10000;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 100000;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 10000000;
drop table tmp;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 1;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 100;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 1000;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 10000;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 100000;
insert into blackhole() select k1,k2 from t1 order by k1,k2 limit 10000000;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 1;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 100;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 1000;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 10000;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 100000;
insert into blackhole() select k1,k2 from tempty order by k1,k2 limit 10000000;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 1;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 100;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 1000;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 10000;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 100000;
drop table tmp;
create table tmp as select k1,k2 from t1 order by k1,k2 limit 10000000;
drop table tmp;