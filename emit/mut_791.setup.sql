create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('replication_num' = '1');
create materialized view user_tags_mv1 
distributed by hash(user_id) 
REFRESH DEFERRED MANUAL
as select user_id, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id;