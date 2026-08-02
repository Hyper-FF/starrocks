CREATE table tab1 (
      k1 INTEGER,
      v1 CHAR(20),
      v2 VARCHAR(20),
      v3 CHAR(20)
)
ENGINE=OLAP
PRIMARY KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
insert into tab1 values (1, 'aaa', 'aaa', 'aaa'), 
(2, 'aaa', 'aaa', 'aaa'), 
(3, 'aaa', 'aaa', 'aaa'), 
(4, 'aaa', 'aaa', 'aaa');