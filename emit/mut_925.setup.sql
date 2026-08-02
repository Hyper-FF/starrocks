CREATE table tab1 (
      k1 INTEGER,
      k2 VARCHAR(50),
      v1 INTEGER,
      v2 INTEGER,
      v3 INTEGER,
      v4 varchar(50),
      v5 varchar(50),
      v6 INTEGER,
      v7 INTEGER,
      v8 INTEGER,
      v9 varchar(50),
      v10 varchar(50),
      v11 INTEGER,
      v12 INTEGER,
      v13 INTEGER,
      v14 varchar(50),
      v15 varchar(50)
)
ENGINE=OLAP
PRIMARY KEY(`k1`,`k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
insert into tab1 values (100, "k2_100", 100, 100, 100, "v4_100", "v5_100", 100, 100, 100, "v9_100", "v10_100", 100, 100, 100, "v14_100", "v15_100");
insert into tab1 values (200, "k2_200", 200, 200, 200, "v4_200", "v5_200", 200, 200, 200, "v9_200", "v10_200", 200, 200, 200, "v14_200", "v15_200");
insert into tab1 values (300, "k2_300", 300, 300, 300, "v4_300", "v5_300", 300, 300, 300, "v9_200", "v10_200", 200, 200, 200, "v14_200", "v15_200");
insert into tab1 values (400, "k2_400", 300, 300, 300, "v4_300", "v5_300", 300, 300, 300, "v9_200", "v10_200", 200, 200, 200, "v14_200", "v15_200");
insert into tab1 values (500, "k2_500", 300, 300, 300, "v4_300", "v5_300", 300, 300, 300, "v9_200", "v10_200", 200, 200, 200, "v14_200", "v15_200");
insert into tab1 values (600, "k2_600", 300, 300, 300, "v4_300", "v5_300", 300, 300, 300, "v9_200", "v10_200", 200, 200, 200, "v14_200", "v15_200");