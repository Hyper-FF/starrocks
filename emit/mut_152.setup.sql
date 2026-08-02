create table t(k int);
alter table t set('bucket_size'='1024');
create table t0(k int) properties('bucket_size'='0');
create table t(k int) properties('bucket_size'='-1');
create table t(k int) properties('bucket_size'='1024');
alter table t set('bucket_size'='0');
alter table t set('bucket_size'='-1');
alter table t set('bucket_size'='2048');
create table t(k int);
insert into t values(1);
insert into t values(1);
alter table t set('bucket_size'='1');
create table t(k date, v int) PARTITION BY RANGE(`k`)
(PARTITION p20210101 VALUES [("2021-01-01"), ("2021-01-02")),
PARTITION p20210102 VALUES [("2021-01-02"), ("2021-01-03")),
PARTITION p20210103 VALUES [("2021-01-03"), ("2021-01-04")),
PARTITION p20210104 VALUES [("2021-01-04"), ("2021-01-05")),
PARTITION p20210105 VALUES [("2021-01-05"), ("2021-01-06")))
PROPERTIES (
"replication_num" = "1",
"bucket_size" = "1"
);
insert into t values('2021-01-01', 1);
insert into t values('2021-01-03', 1);
insert into t values('2021-01-05', 1);
insert into t values('2021-01-01', 1);
create table t(k date not null, v int) PARTITION BY LIST(`k`)
(PARTITION p20210101 VALUES IN ("2021-01-01"),
PARTITION p20210102 VALUES IN ("2021-01-02"),
PARTITION p20210103 VALUES IN ("2021-01-03"),
PARTITION p20210104 VALUES IN ("2021-01-04"),
PARTITION p20210105 VALUES IN ("2021-01-05"))
PROPERTIES (
"replication_num" = "1",
"bucket_size" = "1"
);
insert into t values('2021-01-01', 1);
insert into t values('2021-01-03', 1);
insert into t values('2021-01-05', 1);
insert into t values('2021-01-01', 1);
create table t(k date, v int) PARTITION BY DATE_TRUNC('DAY', `k`)
PROPERTIES (
"replication_num" = "1",
"bucket_size" = "1"
);
insert into t values('2021-01-01', 1);
insert into t values('2021-01-03', 1);
insert into t values('2021-01-05', 1);
insert into t values('2021-01-01', 1);
create table t(k date, v int) DUPLICATE KEY(k) PARTITION BY DATE_TRUNC('DAY', `k`)
PROPERTIES (
"replication_num" = "1",
"bucket_size" = "1"
);
insert into t values('2021-01-01', 1);
insert into t values('2021-01-03', 1);
insert into t values('2021-01-05', 1);
insert into t values('2021-01-01', 1);
insert into t values('2021-01-01', 2);
insert into t values('2021-01-03', 2);
insert into t values('2021-01-05', 2);
insert into t values('2021-01-01', 2);
insert into t values('2021-01-01', 3);
insert into t values('2021-01-03', 3);
insert into t values('2021-01-05', 3);
insert into t values('2021-01-01', 3);
alter table t add column c bigint;
alter table t order by (k,c,v);
create table t(k date, v int, v1 int) DUPLICATE KEY(k) PARTITION BY DATE_TRUNC('DAY', `k`)
PROPERTIES (
"replication_num" = "1",
"bucket_size" = "1"
);
insert into t values('2021-01-01', 1, 1);
insert into t values('2021-01-03', 1, 1);
insert into t values('2021-01-05', 1, 1);
insert into t values('2021-01-01', 1, 1);
insert into t values('2021-01-01', 2, 2);
insert into t values('2021-01-03', 2, 2);
insert into t values('2021-01-05', 2, 2);
insert into t values('2021-01-01', 2, 2);
insert into t values('2021-01-01', 3, 3);
insert into t values('2021-01-03', 3, 3);
insert into t values('2021-01-05', 3, 3);
insert into t values('2021-01-01', 3, 3);
create table t(k int, v int)
PROPERTIES (
"replication_num" = "1",
"bucket_size" = "1"
);
insert into t values(1,1);
insert into t values(1,2);
insert into t values(1,3);
insert into t values(1,4);
insert into t values(1,5);
create table t(k date, v int) PARTITION BY DATE_TRUNC('DAY', `k`)
PROPERTIES (
"replication_num" = "1",
"bucket_size" = "1",
"mutable_bucket_num" = "2"
);
insert into t values('2021-01-01', 1);
alter table t set('mutable_bucket_num'='3');
insert into t values('2021-01-01', 1);
alter table t set('mutable_bucket_num'='-1');
alter table t set('mutable_bucket_num'='a');
create table t(k int)properties('bucket_size'='1');
insert into t values(1);
insert into t values(1);
insert into t values(1);
create table t(k int)properties('bucket_size'='1');
insert into t select generate_series from TABLE(generate_series(1, 10000000));