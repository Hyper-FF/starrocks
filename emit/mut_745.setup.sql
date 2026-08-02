create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('replication_num' = '1');
insert into user_tags values('2023-04-13', 1, 'a', 1);
insert into user_tags values('2023-04-13', 1, 'b', 2);
insert into user_tags values('2023-04-13', 1, 'c', 3);
insert into user_tags values('2023-04-13', 1, 'd', 4);
insert into user_tags values('2023-04-13', 1, 'e', 5);
insert into user_tags values('2023-04-13', 2, 'e', 5);
insert into user_tags values('2023-04-13', 3, 'e', 6);
create materialized view test_mv1 
distributed by hash(user_id) 
as select user_id, bitmap_union(to_bitmap(tag_id)), bitmap_union(to_bitmap(user_name)) from user_tags group by user_id;
create materialized view test_mv1 
distributed by hash(user_id) 
as select user_id, time, hll_union(hll_hash(tag_id)) as agg1, hll_union(hll_hash(user_name)) as agg2  from user_tags group by user_id, time;