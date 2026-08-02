create table all_t0 (
    c1 tinyint,
    c2 smallint,
    c3 int,
    c4 bigint,
    c5 largeint,
    c6 date,
    c7 datetime,
    c8 string,
    c9 string,
    c10 char(100),
    c11 float,
    c12 double,
    c13 tinyint NOT NULL,
    c14 smallint NOT NULL,
    c15 int NOT NULL,
    c16 bigint NOT NULL,
    c17 largeint NOT NULL,
    c18 date NOT NULL,
    c19 datetime NOT NULL,
    c20 string NOT NULL,
    c21 string NOT NULL,
    c22 char(100) NOT NULL,
    c23 float NOT NULL,
    c24 double NOT NULL
) DUPLICATE KEY(c1) DISTRIBUTED BY RANDOM PROPERTIES('replication_num' = '1');
insert into all_t0 SELECT x%200, x%200, x%200, x%200, x%200, x, x, x%200, x, x, x, x, x % 8, x % 8, x % 16, x %200, x%200, '2020-02-02', '2020-02-02', x%200, x, x, x, x FROM TABLE(generate_series(1,  30000)) as g(x);
insert into all_t0 values (null, null, null, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, '2000-01-28', '2000-01-28', 'literal', 'literal', 'literal', -1, -1);
insert into all_t0 values (-1, -2, -3, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, '2000-01-28', '2000-01-28', 'literal', 'literal', 'literal', -1, -1);
create table all_decimal (
    c1 decimal(4,2),
    c2 decimal(10,2),
    c3 decimal(27,9),
    c4 decimal(38,5)
) DUPLICATE KEY(c1) DISTRIBUTED BY RANDOM PROPERTIES('replication_num' = '1');
insert into all_decimal SELECT x%100, x%200, x%200, x%200 FROM TABLE(generate_series(1,  30000)) as g(x);
create table all_numbers_t0 (
    c1 tinyint,
    c2 smallint,
    c3 int,
    c4 bigint,
    c5 largeint,
    c13 tinyint NOT NULL,
    c14 smallint NOT NULL,
    c15 int NOT NULL,
    c16 bigint NOT NULL,
    c17 largeint NOT NULL
) DUPLICATE KEY(c1) DISTRIBUTED BY RANDOM PROPERTIES('replication_num' = '1');
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (-128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728);
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (null, null, null, null, null, 0, 0, 0, 0, 0);
insert into all_numbers_t0 SELECT x%128, x%200, x%200, x%200, x%200, x%128, x%200, x%200, x%200, x%200 FROM TABLE(generate_series(1,  30000)) as g(x);
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727);
CREATE TABLE agged_table (
    k1 int,
    k2 int sum 
) 
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1)
properties (
    "replication_num" = "1"
);
insert into agged_table values(1,1);
insert into agged_table values(1,2);
insert into agged_table values(1,3);
insert into agged_table values(1,4);
CREATE TABLE trand (
    k1 int,
    k2 int
) DUPLICATE KEY(k1)
properties (
    "replication_num" = "1"
);
insert into trand values(1,1);
insert into trand values(2,2);
create table all_t1 (
    c1 tinyint,
    c2 tinyint,
    c3 tinyint,
    c4 tinyint,
    c5 smallint,
    c6 smallint,
    c7 smallint,
    c8 smallint,
    c9 int,
    c10 int,
    c11 int,
    c12 int,
    c13 bigint,
    c14 bigint,
    c15 bigint,
    c16 bigint,
    c17 largeint,
    c18 largeint,
    c19 largeint,
    c20 largeint,
    c21 date,
    c22 date,
    c23 date,
    c24 date
) DUPLICATE KEY(c1) DISTRIBUTED BY RANDOM PROPERTIES('replication_num' = '1');
insert into all_t1 SELECT x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x FROM TABLE(generate_series(1,  300000)) as g(x);