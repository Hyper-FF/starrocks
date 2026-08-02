CREATE TABLE t_p_error (
  calendar_day varchar(65533) NOT NULL COMMENT "",
  calendar_year varchar(65533) NOT NULL COMMENT "",
  calendar_id varchar(65533) NOT NULL COMMENT ""
) ENGINE=OLAP 
PARTITION BY (calendar_year)
DISTRIBUTED BY HASH(calendar_day) BUCKETS 2 
PROPERTIES ( "replication_num" = "1");
insert into t_p_error values ('2022-01-01','2011','104979534377373696');
insert into t_p_error 
select calendar_day, cast(calendar_year as int) + 1, calendar_id from t_p_error;
insert into t_p_error values 
    ('2022-01-01','2011','104979534377373696'),
    ('2022-01-01','2012','104979534377373696');
ALTER TABLE t_p_error ADD PARTITION IF NOT EXISTS p2024 VALUES IN ('2024');
insert into t_p_error values ('2022-01-01','2024','abc');
ALTER TABLE t_p_error ADD PARTITION IF NOT EXISTS p2024 VALUES IN (('2030'));
insert into t_p_error values ('2022-01-01','2030','abc');
ALTER TABLE t_p_error ADD TEMPORARY PARTITION IF NOT EXISTS p2024_tmp VALUES IN ('2024');
insert into t_p_error TEMPORARY PARTITION(p2024_tmp) values ('2022-01-01','2024','xyz');
ALTER TABLE t_p_error REPLACE PARTITION (p2024) WITH TEMPORARY PARTITION (p2024_tmp);
insert into t_p_error select calendar_day, cast(calendar_year as int) + 1, calendar_id from t_p_error;