CREATE TABLE `t1`
(
    `id` int NOT NULL,
    `name` varchar(65533),
    `score` int NOT NULL
)
ENGINE=OLAP
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
 "replication_num" = "1"
);
alter table t1 set('enable_load_profile'='true');
CREATE TABLE `t2`
(
    `id` int(11) NOT NULL,
    `name` varchar(65533) NOT NULL,
    `score` int(11) NOT NULL
)
ENGINE=OLAP
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
 "replication_num" = "1"
);