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
alter table tab1 add column (v6 INTEGER DEFAULT "100");