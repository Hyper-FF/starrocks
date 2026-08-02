drop table if exists primary_key_with_null;
CREATE TABLE `primary_key_with_null` ( 
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
) PRIMARY KEY(`k1`, `k2`, `k3`) 
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3 
PROPERTIES ( "replication_num" = "1");
INSERT INTO primary_key_with_null VALUES
 ('2020-10-22','2020-10-23 12:12:12','k1','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-23','2020-10-23 12:12:12','k2','k4',0,0,2,3,4,5,1.1,1.12,2.889)
,('2020-10-24','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-25','2020-10-23 12:12:12','k4','k4',0,1,2,3,4,NULL,NULL,NULL,2.889);
drop table if exists primary_key_with_null;
CREATE TABLE `pk_tbl1` (
 `k1` bigint(20) NOT NULL AUTO_INCREMENT,
 `k2` datetime NULL,
 `k3` bigint(20) NULL,
 `k4` bigint(20) NULL,
 `k5` int(11) NULL
) ENGINE=OLAP
PRIMARY KEY(`k1`)
DISTRIBUTED BY HASH(`k1`);
insert into pk_tbl1(k1, k2, k3, k4, k5) values(DEFAULT, '2024-01-01', 1, 2, 3), (DEFAULT, '2024-01-01', 1, 2, 3);
drop table if exists pk_tbl1;
create table pk_tbl2 (k1 int, k2 varchar(1)) primary key (k1) distributed by hash(k1);
insert into pk_tbl2 values (1, "a"), (2, "bb");
insert into pk_tbl2 values (1, "a"), (2, "bb");
drop table if exists pk_tbl2;