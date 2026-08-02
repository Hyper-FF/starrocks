create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('replication_num' = '1');
insert into user_tags values('2023-04-13', 1, 'a', 1), ('2023-04-13', 1, 'b', 2), ('2023-04-13', 1, 'c', 3), ('2023-04-13', 1, 'd', 4), ('2023-04-13', 1, 'e', 5), ('2023-04-13', 2, 'e', 5), ('2023-04-13', 3, 'e', 6);
insert into user_tags values('2023-04-13', 1, 'a', 1), ('2023-04-13', 1, 'b', 2), ('2023-04-13', 1, 'c', 3), ('2023-04-13', 1, 'd', 4), ('2023-04-13', 1, 'e', 5), ('2023-04-13', 2, 'e', 5), ('2023-04-13', 3, 'e', 6);
drop table if exists tbl1;
CREATE TABLE `tbl1` (
  `k1` tinyint(4) NULL DEFAULT "0",
  `k2` varchar(64) NULL DEFAULT "",
  `k3` bigint NULL DEFAULT "0",
  `k4` varchar(64) NULL DEFAULT ""
) ENGINE=OLAP 
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1;
insert into tbl1 values (1, 'a', 1, 'aa'), (2, 'b', 1, NULL), (3, NULL, NULL, NULL);
insert into tbl1 values (1, 'a', 1, 'aa'), (2, 'b', 1, NULL), (3, NULL, NULL, NULL);
CREATE TABLE UPPER_TBL1 
(
    K1 date,
    K2 int,
    V1 int sum
)
PARTITION BY RANGE(K1)
(
    PARTITION p1 values [('2020-01-01'),('2020-02-01')),
    PARTITION p2 values [('2020-02-01'),('2020-03-01'))
)
DISTRIBUTED BY HASH(K2) BUCKETS 3
PROPERTIES('replication_num' = '1');
insert into UPPER_TBL1 values ('2020-01-01', 1, 1), ('2020-01-01', 1, 1), ('2020-01-01', 1, 2),  ('2020-01-01', 2, 1);
insert into UPPER_TBL1 values ('2020-01-01', 1, 1), ('2020-01-01', 1, 1), ('2020-01-01', 1, 2),  ('2020-01-01', 2, 1);
create table sync_mv_base_table_with_delete (k1 bigint, k2 bigint, k3 bigint) duplicate key(k1) distributed by hash(k1) buckets 1 properties ("replication_num" = "1");
insert into sync_mv_base_table_with_delete values (1, 1, 1), (2, 2, 2);