DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;
CREATE table tab1 (
      k1 INTEGER,
      k2 VARCHAR(50),
      v1 INTEGER,
      v2 INTEGER,
      v3 INTEGER,
      v4 varchar(50),
      v5 varchar(50)
)
ENGINE=OLAP
PRIMARY KEY(`k1`,`k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
insert into tab1 values (100, "k2_100", 100, 100, 100, "v4_100", "v5_100");
insert into tab1 values (200, "k2_200", 200, 200, 200, "v4_200", "v5_200");
insert into tab1 values (300, "k3_300", 300, 300, 300, "v4_300", "v5_300");
CREATE MATERIALIZED VIEW `mv1`
REFRESH ASYNC
AS
select k1, k2, sum(v1), sum(v2), sum(v3) from tab1 group by k1, k2;