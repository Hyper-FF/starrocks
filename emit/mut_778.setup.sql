CREATE TABLE ss( event_day DATE, pv BIGINT) DUPLICATE KEY(event_day) DISTRIBUTED BY HASH(event_day) BUCKETS 8 PROPERTIES("replication_num" = "1");
CREATE TABLE jj( event_day DATE, pv BIGINT) DUPLICATE KEY(event_day) DISTRIBUTED BY HASH(event_day) BUCKETS 8 PROPERTIES("replication_num" = "1");
insert into ss values('2020-01-14', 2);
insert into ss values('2020-01-14', 3);
insert into ss values('2020-01-15', 2);
insert into jj values('2020-01-14', 2);
insert into jj values('2020-01-14', 3);
insert into jj values('2020-01-15', 2);
CREATE MATERIALIZED VIEW mv1 DISTRIBUTED BY hash(event_day) AS SELECT event_day, sum(pv) as sum_pv FROM ss GROUP BY event_day;
CREATE MATERIALIZED VIEW mv2 DISTRIBUTED BY hash(event_day) AS SELECT event_day, count(pv) as count_pv FROM ss GROUP BY event_day;
INSERT INTO ss values('2020-01-15', 2);
ALTER TABLE ss SWAP WITH mv1;
CREATE MATERIALIZED VIEW mv_on_mv_1 REFRESH ASYNC 
AS SELECT sum(sum_pv) as sum_sum_pv FROM mv1;
CREATE MATERIALIZED VIEW mv_on_mv_2 REFRESH ASYNC 
AS SELECT sum_sum_pv + 1 FROM mv_on_mv_1;
CREATE MATERIALIZED VIEW mv_on_table_1 REFRESH ASYNC 
AS SELECT ss.event_day, sum(ss.pv) as ss_sum_pv, sum(jj.pv) as jj_sum_pv
    FROM ss JOIN jj on (ss.event_day = jj.event_day) 
    GROUP BY ss.event_day;
ALTER TABLE ss SWAP WITH jj;