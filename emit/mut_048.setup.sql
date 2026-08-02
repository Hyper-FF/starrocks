CREATE TABLE IF NOT EXISTS `lineorder` (
    `lo_orderkey` int(11) NOT NULL COMMENT "",
    `lo_shipmode` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE `ss` (
  `id` int(11) NULL COMMENT "",
  `name` varchar(255) NULL COMMENT "",
  `subject` varchar(255) NULL COMMENT "",
  `score` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into ss values (1,"Tom","English",90);
insert into ss values (1,"Tom","Math",80);
insert into ss values (2,"Tom","English",NULL);
insert into ss values (2,"Tom",NULL,NULL);
insert into ss values (3,"May",NULL,NULL);
insert into ss values (3,"Ti","English",98);
insert into ss values (4,NULL,NULL,NULL);
insert into ss values (NULL,NULL,NULL,NULL);
insert into ss values (NULL,"Ti","物理Phy",99);
insert into ss values (11,"张三此地无银三百两","英文English",98);
insert into ss values (11,"张三掩耳盗铃","Math数学欧拉方程",78);
insert into ss values (12,"李四大闹天空","英语外语美誉",NULL);
insert into ss values (2,"王武程咬金","语文北京上海",22);
insert into ss values (3,"欧阳诸葛方程","数学大不列颠",NULL);
CREATE TABLE t1 (
    id        tinyint(4)      NULL,
    value   varchar(65533)  NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(id)
PROPERTIES (
 "replication_num" = "1"
);
INSERT INTO t1 VALUES
(1,'fruit'),
(1,'fruit'),
(1,'fruit'),
(2,'fruit'),
(2,'fruit'),
(2,'fruit');