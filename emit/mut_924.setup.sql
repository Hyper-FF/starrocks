CREATE table tab1 (
      k1 INTEGER,
      k2 VARCHAR(50),
      v1 INTEGER,
      v2 INTEGER,
      v3 INTEGER
)
ENGINE=OLAP
PRIMARY KEY(`k1`,`k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
insert into tab1 values (100, "k2_100", 100, 100, 100);
insert into tab1 values (200, "k2_200", 200, 200, 200);
insert into tab1 values (300, "k3_300", 300, 300, 300);
alter table tab1 drop column v2;