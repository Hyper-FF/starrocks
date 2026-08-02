create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) 
partition by date_trunc('day', time)  
distributed by hash(time) buckets 3 
properties('replication_num' = '1');
insert into user_tags values('2023-04-13', 1, 'a', 1);
insert into user_tags values('2023-04-13', 1, 'b', 2);
insert into user_tags values('2023-04-14', 2, 'e', 5);
insert into user_tags values('2023-04-14', 3, 'e', 6);
create materialized view user_tags_mv1  distributed by hash(user_id) 
partition by date_trunc('day', time)
properties(
    'partition_refresh_number' = '-1',
    'partition_refresh_strategy' = 'force'
)
refresh deferred manual
as select user_id, time, count(tag_id) from user_tags group by user_id, time;