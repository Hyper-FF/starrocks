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
insert into tab1 properties("merge_condition" = "v1") values (300, "k3_300", 200, 400, 400, "v4_400", "v5_400");
insert into tab1 properties("merge_condition" = "v1") values (300, "k3_300", 400, 400, 400, "v4_400", "v5_400");
CREATE table expr_partition_tbl (
      org_id INT,
      id INT,
      v1 INT,
      modified_at INT
)
ENGINE=OLAP
PRIMARY KEY(`org_id`,`id`)
PARTITION BY (`org_id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);