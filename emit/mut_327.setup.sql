CREATE TABLE pksk_tbl (
    c1 int,
    c2 date,
    c3 varchar(10),
    c4 bigint,
    c5 varchar(3),
    c6 datetime,
    c7 string,
    c8 decimal(10,5),
    c9 boolean,
    c10 largeint,
    c11 date,
    c12 float,
    c13 double)
PRIMARY KEY(c1,c2)
DISTRIBUTED BY HASH(c1) BUCKETS 3
ORDER BY(c1,c6,c11,c2);