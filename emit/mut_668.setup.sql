CREATE TABLE `nullable_t1` ( `t1_c1` int, `t1_c2` int, `t1_c3` int, `t1_c4` varchar(10))
DUPLICATE KEY(`t1_c1`) COMMENT "OLAP" DISTRIBUTED BY HASH(`t1_c1`) PROPERTIES ( "replication_num" = "1");
CREATE TABLE `t2` (`t2_c1` int NOT NULL default "0", `t2_c2` int NOT NULL default "0", `t2_c3` int NOT NULL default "0", `t2_c4` varchar(10) NOT NULL default "")
DUPLICATE KEY(`t2_c1`) COMMENT "OLAP" DISTRIBUTED BY HASH(`t2_c1`) PROPERTIES ( "replication_num" = "1");
insert into nullable_t1 (t1_c1, t1_c2, t1_c3, t1_c4) values
(1, 11, 111, '1111'), (2, 22, 222, '2222'), (3, null, 333, '3333'), (4, null, null, '4444'), (null, 55, 555, null), (6, 66, null, '6666'), (null, null, null, null);
insert into t2 (t2_c1, t2_c2, t2_c3, t2_c4) values
(1, 11, 111, '1111'), (2, 22, 222, '2222'), (3, 33, 333, '3333'), (4, 44, 444, '4444'), (5, 55, 555, '5555'), (6, 66, 666, '6666'), (7, 77, 777, '7777');