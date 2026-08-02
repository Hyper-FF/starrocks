CREATE TABLE t1 (
    c_key      INT NOT NULL,
    c_tinyint  TINYINT,
    c_smallint SMALLINT,
    c_int      INT,
    c_bigint   BIGINT,
    c_largeint LARGEINT,
    c_float    FLOAT,
    c_double   DOUBLE,
    c_decimal  DECIMAL(26,2),
    c_date     DATE,
    c_datetime DATETIME,
    c_string   STRING
)
DUPLICATE KEY(c_key)
DISTRIBUTED BY HASH(c_key) BUCKETS 1
PROPERTIES (
    "replication_num"="1"
);
CREATE TABLE t2 (
    c_key      INT NOT NULL,
    c_tinyint  TINYINT,
    c_smallint SMALLINT,
    c_int      INT,
    c_bigint   BIGINT,
    c_largeint LARGEINT,
    c_float    FLOAT,
    c_double   DOUBLE,
    c_decimal  DECIMAL(26,2),
    c_date     DATE,
    c_datetime DATETIME,
    c_string   STRING
)
DUPLICATE KEY(c_key)
DISTRIBUTED BY HASH(c_key) BUCKETS 1
PROPERTIES (
    "replication_num"="1"
);
insert into t1(c_key, c_tinyint) values (1, 1), (2, 1), (3, 1), (4, 2), (5, 2), (6, 3);
insert into t2(c_key, c_tinyint) values (1, 1), (2, 2), (3, 3);
insert into t1(c_key, c_smallint) values (1, 1), (2, 1), (3, 1), (4, 1111), (5, 1111), (6, 3);
insert into t2(c_key, c_smallint) values (1, 1), (2, 1111), (3, 3);
insert into t1(c_key, c_int) values (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);
insert into t2(c_key, c_int) values (1, 1), (2, 1234567), (3, 3);
insert into t1(c_key, c_bigint) values (1, 1), (2, 1), (3, 1), (4, 12345678912), (5, 12345678912), (6, 3);
insert into t2(c_key, c_bigint) values (1, 1), (2, 12345678912), (3, 3);
insert into t1(c_key, c_largeint) values (1, 1), (2, 1), (3, 1), (4, 18446744073709551620), (5, 18446744073709551620), (6, 3);
insert into t2(c_key, c_largeint) values (1, 1), (2, 18446744073709551620), (3, 3);
insert into t1(c_key, c_float) values (1, 1.1), (2, 1.1), (3, 1.1), (4, 2.1), (5, 2.1), (6, 3);
insert into t2(c_key, c_float) values (1, 1.1), (2, 2.1), (3, 3);
insert into t1(c_key, c_double) values (1, 1.1), (2, 1.1), (3, 1.1), (4, 2.1), (5, 2.1), (6, 3);
insert into t2(c_key, c_double) values (1, 1.1), (2, 2.1), (3, 3);
insert into t1(c_key, c_decimal) values (1, 1.1), (2, 1.1), (3, 1.1), (4, 2.1), (5, 2.1), (6, 3);
insert into t2(c_key, c_decimal) values (1, 1.1), (2, 2.1), (3, 3);
insert into t1(c_key, c_date) values
    (1, "2024-01-01"), (2, "2024-01-01"), (3, "2024-01-01"),
    (4, "2024-01-02"), (5, "2024-01-02"), (6, "2024-01-03");
insert into t2(c_key, c_date) values (1, "2024-01-01"), (2, "2024-01-02"), (3, "2024-01-03");
insert into t1(c_key, c_datetime) values
    (1, "2024-01-01 00:00:00"),
    (2, "2024-01-01 00:00:00"),
    (3, "2024-01-01 00:00:00"),
    (4, "2024-01-02 00:00:00"),
    (5, "2024-01-02 00:00:00"),
    (6, "2024-01-03 00:00:00");
insert into t2(c_key, c_datetime) values
    (1, "2024-01-01 00:00:00"),
    (2, "2024-01-02 00:00:00"),
    (3, "2024-01-03 00:00:00");
insert into t1(c_key, c_string) values ("1", "1"), ("2", "1"), ("3", "1"), ("4", "1234567"), ("5", "1234567"), ("6", "3");
insert into t2(c_key, c_string) values ("1", "1"), ("2", "1234567"), ("3", "3");
insert into t1(c_key, c_int) values (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);
insert into t2(c_key, c_bigint) values (1, 1), (2, 1234567), (3, 3);
insert into t1(c_key, c_bigint) values (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);
insert into t2(c_key, c_int) values (1, 1), (2, 1234567), (3, 3);
insert into t1(c_key, c_bigint) values (1, null), (2, 1), (3, 2), (4, -1);
insert into t2(c_key, c_int) values (1, 1), (2, 2), (3, 3);
insert into t1(c_key, c_string) values (1, "1"), (2, "1"), (3, "1"), (4, "1234567"), (5, "1234567"), (6, "3");
insert into t2(c_key, c_int) values (1, 1), (2, 1234567), (3, 3);
insert into t1(c_key, c_bigint) values (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);
insert into t2(c_key, c_int) values (1, 1), (2, 1234567), (3, 3);
insert into t1(c_key, c_bigint) values (7,4);
insert into t2(c_key, c_int) values (4,9);
CREATE TABLE skew_t1 (
    c_key      INT NOT NULL,
    c_tinyint  TINYINT,
    c_smallint SMALLINT,
    c_int      INT,
    c_bigint   BIGINT,
    c_largeint LARGEINT,
    c_float    FLOAT,
    c_double   DOUBLE,
    c_decimal  DECIMAL(26,2),
    c_date     DATE,
    c_datetime DATETIME,
    c_string   STRING
)
DUPLICATE KEY(c_key)
DISTRIBUTED BY HASH(c_key) BUCKETS 1
PROPERTIES (
    "replication_num"="1"
);
INSERT INTO skew_t1(c_key, c_tinyint, c_smallint, c_int, c_bigint, c_largeint, c_float, c_double, c_decimal, c_date, c_datetime, c_string)
SELECT t.c_key, t.c_key % 100 as c_tinyint, t.c_key % 1000 as c_smallint, t.c_key % 10000 as c_int, t.c_key % 100000 as c_bigint, t.c_key % 1000000 as c_largeint, t.c_key % 10000000 as c_float, t.c_key % 100000000 as c_double, t.c_key % 1000000000 as c_decimal, t.c_key % 10000000000 as c_date, t.c_key % 100000000000 as c_datetime, t.c_key % 1000000000000 as c_string
FROM TABLE(generate_series(1, 4095*2)) t(c_key);
INSERT INTO t2(c_key, c_tinyint, c_smallint, c_int, c_bigint, c_largeint, c_float, c_double, c_decimal, c_date, c_datetime, c_string)
SELECT t.c_key, t.c_key % 100 as c_tinyint, t.c_key % 1000 as c_smallint, t.c_key % 10000 as c_int, t.c_key % 100000 as c_bigint, t.c_key % 1000000 as c_largeint, t.c_key % 10000000 as c_float, t.c_key % 100000000 as c_double, t.c_key % 1000000000 as c_decimal, t.c_key % 10000000000 as c_date, t.c_key % 100000000000 as c_datetime, t.c_key % 1000000000000 as c_string
FROM TABLE(generate_series(1, 4095*2)) t(c_key);
INSERT INTO skew_t1(c_key, c_tinyint, c_smallint, c_int, c_bigint, c_largeint, c_float, c_double, c_decimal, c_date, c_datetime, c_string)
SELECT t.c_key % 1, t.c_key % 1 as c_tinyint, t.c_key % 1 as c_smallint, t.c_key % 1 as c_int, t.c_key % 1 as c_bigint, t.c_key % 1 as c_largeint, t.c_key % 1 as c_float, t.c_key % 1 as c_double, t.c_key % 1 as c_decimal, t.c_key % 1 as c_date, t.c_key % 1 as c_datetime, t.c_key % 1 as c_string
FROM TABLE(generate_series(1, 4095*2)) t(c_key);
drop table if exists t_mcv_l;
drop table if exists t_mcv_r;
create table t_mcv_l (
    k bigint null,
    v int null
)
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
create table t_mcv_r (
    k bigint null,
    v int null
)
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
insert into t_mcv_l select 1, 1 from table(generate_series(1, 5000));
insert into t_mcv_l select 2, 1 from table(generate_series(1, 2500));
insert into t_mcv_l select generate_series + 2, 1 from table(generate_series(1, 2500));
insert into t_mcv_r select (generate_series % 2502) + 1, 1 from table(generate_series(1, 20000));
drop table if exists t_mcv_l2;
drop table if exists t_mcv_r2;
create table t_mcv_l2 (
    k bigint null,
    v int null
)
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
create table t_mcv_r2 (
    k bigint null,
    v int null
)
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
insert into t_mcv_l2 select generate_series, 1 from table(generate_series(1, 10000));
insert into t_mcv_r2 select 1, 1 from table(generate_series(1, 8000));
insert into t_mcv_r2 select 2, 1 from table(generate_series(1, 4000));
insert into t_mcv_r2 select generate_series + 2, 1 from table(generate_series(1, 8000));
drop table t_mcv_l2;
drop table t_mcv_r2;
drop table if exists t_null_l;
drop table if exists t_null_r;
create table t_null_l (
    k bigint null,
    v int null
)
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
create table t_null_r (
    k bigint null,
    v int null
)
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
insert into t_null_l select null, 1 from table(generate_series(1, 7000));
insert into t_null_l select 1, 1 from table(generate_series(1, 2000));
insert into t_null_l select 2, 1 from table(generate_series(1, 1000));
insert into t_null_r select (generate_series % 2000) + 1, 1 from table(generate_series(1, 10000));
drop table t_null_l;
drop table t_null_r;
drop table t_mcv_l;
drop table t_mcv_r;
drop table skew_t1;
drop table t1;
drop table t2;