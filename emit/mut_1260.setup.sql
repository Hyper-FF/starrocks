insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));
create table dup_tbl(
col_int int,
col_integer integer,
col_tinyint tinyint,
col_float float,
col_double double,
col_decimal decimal,
col_date date,
col_timestamp datetime,
col_varchar varchar(100),
col_char char(100),
col_boolean boolean)
DUPLICATE KEY(`col_int`)
DISTRIBUTED BY HASH(`col_int`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into dup_tbl select 1,0,1,1.001,10.1,1110.1,'2001-01-01','2021-10-30 12:10:23','hello world','hi hello',true;
insert into dup_tbl select 1,1,1,1.001,10.1,1110.1,'2001-01-02','2021-10-30 12:10:23','hello world','hi hello',true;