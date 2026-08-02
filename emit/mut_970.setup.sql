CREATE TABLE IF NOT EXISTS prepare_stmt (
    k1 INT,
    k2 TINYINT Default '20',
    k3 BIGINT,
    k4 SMALLINT  Default '4',
    k5 varchar(10) Default 'k5',
    k6 BOOLEAN,
    k7 decimal(10, 2),
    k8 float,
    k9 double,
    v1 date not null,
    v2 date,
    v3 datetime not null,
    v4 datetime,
    v5 array<int>,
    v6 array<date>,
    v7 array<array<datetime>>,
    v8 STRUCT<a INT, b INT>,
    v9 MAP<INT,DATETIME>,
    v10 json)
    PRIMARY KEY (k1, k2, k3, k4, k5)
    DISTRIBUTED BY HASH(k1, k2, k3, k4, k5) BUCKETS 8 PROPERTIES("replication_num" = "1");
insert into prepare_stmt values (1, 2, 3, 4, '5', true, 7.2, 8.1, 9.222, '2010-01-01', null, '2010-01-01 01:02:03',
null, [1, 2, 3, null, 4], [date '2021-01-05', null], [[null, datetime '2021-01-01 01:02:03'], [null, datetime '2021-01-01 01:02:03']],
row(1, null), map{1:'2021-01-01',3:NULL}, json_object('a', 4, 'b', false));