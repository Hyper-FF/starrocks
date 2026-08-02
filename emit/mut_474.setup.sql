drop table if exists tbl_with_null1;
CREATE TABLE `tbl_with_null1` ( 
    `k1`  date, 
    `k2`  datetime, 
    `k3`  varchar(20), 
    `k4`  varchar(20), 
    `k5`  boolean, 
    `k6`  tinyint, 
    `k7`  smallint, 
    `k8`  int, 
    `K9`  bigint, 
    `K10` largeint, 
    `K11` float, 
    `K12` double, 
    `K13` decimal(27,9) 
) DUPLICATE KEY(`k1`) 
DISTRIBUTED BY HASH(`k1`) BUCKETS 3 
PROPERTIES ( "replication_num" = "1");
INSERT INTO tbl_with_null1 VALUES
 ('2020-10-22','2020-10-23 12:12:12','k1','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-23','2020-10-23 12:12:12','k2','k4',0,0,2,3,4,5,1.1,1.12,2.889)
,('2020-10-24','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-25','2020-10-23 12:12:12','k4','k4',0,1,2,3,4,NULL,NULL,NULL,2.889)
,('2020-10-26',NULL, NULL, NULL,NULL,NULL,NULL,NULl,NULL,NULL,NULL,NULL,NULL);
CREATE TABLE `tbl_with_null2` ( 
    `k1`  date, 
    `k2`  datetime, 
    `k3`  varchar(20), 
    `k4`  varchar(20), 
    `k5`  boolean, 
    `k6`  tinyint, 
    `k7`  smallint, 
    `k8`  int, 
    `K9`  bigint, 
    `K10` largeint, 
    `K11` float, 
    `K12` double, 
    `K13` decimal(27,9) 
) DUPLICATE KEY(`k1`) 
DISTRIBUTED BY HASH(`k1`) BUCKETS 9
PROPERTIES ( "replication_num" = "1");
INSERT INTO tbl_with_null2 VALUES
 ('2020-10-22','2020-10-23 12:12:12','k1','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-23','2020-10-23 12:12:12','k2','k4',0,0,2,3,4,5,1.1,1.12,2.889)
,('2020-10-24','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-25','2020-10-23 12:12:12','k4','k4',0,1,2,3,4,NULL,NULL,NULL,2.889)
,('2020-10-26',NULL, NULL, NULL,NULL,NULL,NULL,NULl,NULL,NULL,NULL,NULL,NULL);
drop table if exists tbl_with_null1;
drop table if exists tbl_with_null2;