CREATE TABLE ss( event_day DATE, pv BIGINT) DUPLICATE KEY(event_day) DISTRIBUTED BY HASH(event_day) BUCKETS 8 PROPERTIES("replication_num" = "1");
insert into ss values('2020-01-14', 1), ('2020-01-14', 3), ('2020-01-15', 2);
CREATE MATERIALIZED VIEW mv1 DISTRIBUTED BY hash(event_day) 
REFRESH DEFERRED MANUAL
AS SELECT event_day, sum(pv) as sum_pv FROM ss GROUP BY event_day;