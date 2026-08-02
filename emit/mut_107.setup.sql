CREATE TABLE test_update_stats (
    k2 int,
    k3 varchar(20)
) 
PRIMARY KEY (k2)
DISTRIBUTED BY HASH(k2) BUCKETS 1
PROPERTIES ( "replication_num" = "1");
CREATE VIEW statistic_verify AS
select column_name, partition_name, row_count, max, min 
from _statistics_.column_statistics 
where table_name = "test_update_trigger_statistics.test_update_stats";
CREATE VIEW analyze_status_verify AS
select `Table`, Columns, Type, 
    IF(cast(Id as bigint) > @last_analyze_id, 'new analyze', 'no analyze') as is_new 
from information_schema.analyze_status
where 
    `Database` = 'test_update_trigger_statistics' 
    and `Table` = "test_update_stats" 
order by cast(Id as bigint) desc limit 1;
CREATE VIEW last_analyze_id_view AS
select ifnull(max(cast(Id as bigint)), 0) as last_id
from information_schema.analyze_status 
where `Database` = 'test_update_trigger_statistics' 
  and `Table` = "test_update_stats";
insert into test_update_stats select generate_series, 'data' from table(generate_series(1, 1000000));
drop table test_update_stats;
drop view  statistic_verify;
drop view  analyze_status_verify;
drop view  last_analyze_id_view;