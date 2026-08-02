CREATE TABLE `t0` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 48
PROPERTIES (
"replication_num" = "1");
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));
insert into t0 values (null,null,null,null);
CREATE TABLE `tsemi` (
  `c0` int(11) NULL COMMENT "",
  `c1` array<int> NULL COMMENT "",
  `c2` map<int,int> NULL COMMENT "",
  `c3` struct<k int, v int> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 48
PROPERTIES (
"replication_num" = "1");
insert into tsemi SELECT generate_series, [generate_series, generate_series], map(generate_series, generate_series), named_struct('k', generate_series, 'v', generate_series) FROM TABLE(generate_series(1,  40960));
insert into blackhole() select * from t0 limit 10;
insert into blackhole() select * from t0 where c0 < 10 limit 10;
insert into blackhole() select *,upper(c1),upper(c2) from t0 where c0 < 10 limit 10;
INSERT into blackhole() select *,upper(c1),upper(c2) from t0 where upper(c0) < 10 limit 10;
INSERT into blackhole() select c0 + c1,* from t0 where c0 < 10 limit 100,10;
create table tjs1 (
  c0 int,
  c1 json
) engine=olap
duplicate key(c0)
distributed by hash(c0) buckets 1
properties ("replication_num" = "1");
insert into tjs1 values (1, '{"a": 1}');
insert into tjs1 values (2, '{"a": 2}');
insert into tjs1 values (3, '{"a": 3}');
insert into blackhole() select c0,get_json_string(c1, "$.a") from tjs1 limit 1;
CREATE TABLE `tc1` (
  `c0` int(11) NULL COMMENT "",
  `c1` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "cg1");
CREATE TABLE `tc2` (
  `c0` int(11) NULL COMMENT "",
  `c1` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "cg1");
insert into tc1 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  20));
insert into tc2 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  20));
CREATE TABLE `tpart` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
PARTITION BY RANGE(`c0`) (
  PARTITION p1 VALUES LESS THAN ("10"),
  PARTITION p2 VALUES LESS THAN ("20"),
  PARTITION p3 VALUES LESS THAN ("2100000000")
)
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1");
insert into tpart select generate_series, cast(generate_series as varchar) from table(generate_series(1, 30));
insert into blackhole() select t0.*,tc1.* from t0 join tc1 on t0.c0 = tc1.c0 where t0.c0 < 20 limit 1000;
insert into blackhole() select t0.*,tc1.* from t0 left join tc1 on t0.c0 = tc1.c0 where t0.c0 < 20 limit 1000;
insert into blackhole() select t0.*,tc1.* from t0 right join tc1 on t0.c0 = tc1.c0 where t0.c0 < 20 limit 1000;
insert into blackhole() select t0.* from t0 left semi join tc1 on t0.c0 = tc1.c0 where t0.c0 < 20 limit 1000;
insert into blackhole() select t0.* from t0 where t0.c0 in (select c0 from tc1 where tc1.c0 < 20) limit 1000;
insert into blackhole() select t0.* from t0 where t0.c0 not in (select c0 from tc1 where tc1.c0 < 20) limit 1000;
insert into blackhole() with cte as (select * from t0 where c0 < 20) select * from cte where c0 < 10 union all select * from cte order by 1,2,3,4 limit 1000;
insert into blackhole() select * from t0 where c0 < (select max(c0) from t0) limit 1000;
insert into blackhole() select * from t0 where c0 < 10 union all select * from t0 where c0 >= 10 limit 1000;
insert into blackhole() select c0, count(*) from t0 group by c0 order by c0 limit 1000;
insert into blackhole() select c0, row_number() over (order by c0) from t0 where c0 < 20 limit 1000;
insert into blackhole() select * from (select c0, row_number() over (order by c0) rn from t0 where c0 < 20) v where rn <= 1 order by c0 limit 1000;
insert into blackhole() select * from tpart where c0 < 20 order by c0 limit 1000;