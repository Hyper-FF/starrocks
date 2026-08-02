create table ss (event_day date, pv bigint) duplicate key(event_day)
distributed by hash(event_day) buckets 2 properties("replication_num" = "1");
insert into ss values ('2020-01-14', 1), ('2020-01-15', 2);
create materialized view mv_manual distributed by hash(event_day)
refresh deferred manual
as select event_day, sum(pv) as sum_pv from ss group by event_day;
create materialized view mv_onchange distributed by hash(event_day)
refresh deferred async
as select event_day, sum(pv) as sum_pv from ss group by event_day;
create materialized view mv_sched distributed by hash(event_day)
refresh deferred async start('2024-01-01 00:00:00') every(interval 1 hour)
as select event_day, sum(pv) as sum_pv from ss group by event_day;
create table pt (dt date, v int) duplicate key(dt)
partition by range(dt) (partition p1 values [('2024-01-01'),('2024-01-02')), partition p2 values [('2024-01-02'),('2024-01-03')))
distributed by hash(dt) buckets 2 properties('replication_num'='1');
insert into pt values ('2024-01-01',1),('2024-01-02',2);
create materialized view mv_part partition by dt distributed by hash(dt)
refresh deferred manual
as select dt, sum(v) sv from pt group by dt;