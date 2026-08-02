CREATE TABLE `t0` (
	`id` int(11) NULL COMMENT "",
	`data` int(11) NULL COMMENT "",
	`date` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1");
CREATE TABLE `t1` (
	`id` int(11) NULL COMMENT "",
	`data` int(11) NULL COMMENT "",
	`date` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1");
insert into t0 select generate_series, generate_series, generate_series from table(generate_series(1, 200));
insert into t1 select generate_series, generate_series, generate_series from table(generate_series(1, 200));
insert into blackhole() select * from t0 limit 10;
insert into blackhole() select * from t0 where id < 10 limit 10;
insert into blackhole()
with cte as (select id as kid,* from t0 limit 10)
select * from cte l join cte r on l.kid=r.kid limit 10;
insert into blackhole()
with cte as (select id as kid,* from t0 limit 10)
select * from cte where data < 10 limit 5;
insert into blackhole()
with cte as (select id as kid,* from t0 limit 10)
select * from cte where data < 10 order by 1 limit 5;
insert into blackhole()
with cte as (select id as kid,* from t0 limit 10)
select * from cte l join cte r on l.kid = r.kid order by l.kid limit 10;
insert into blackhole()
with cte as (select id as kid,* from t0)
(select * from cte order by id ) union all (select * from cte limit 10) order by kid limit 10;
insert into blackhole()
with cte as (select id as kid,* from t0)
(select * from cte order by id limit 10) union all (select * from cte limit 10);
insert into blackhole() select *, row_number() over (partition by id) from t0 limit 10;
insert into blackhole () select count(*) from t0 group by id limit 10;
insert into blackhole() with cte as (select id,date from t0 limit 10)
select distinct id,date from cte order by id, date limit 10;
insert into blackhole() select * from t0 order by id limit 1025;