CREATE TABLE `t0`
(
    `id` int(11) NOT NULL COMMENT "user id",
    `name` varchar(65533) NULL COMMENT "user name",
    `score` int(11) NOT NULL COMMENT "user score"
)
ENGINE=OLAP
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
 "replication_num" = "1"
);