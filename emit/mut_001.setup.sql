CREATE TABLE `source` (
  `k1` int(11) NULL COMMENT "",
  `event_day` datetime NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k1`, `event_day`)
PARTITION BY date_trunc('day', event_day)
PROPERTIES (
"replication_num" = "1"
);
insert into source select generate_series, '2020-01-01' from table(generate_series(1, 250000));
insert into source select generate_series, '2021-01-01' from table(generate_series(1, 250000));
insert into source select generate_series, '2022-01-01' from table(generate_series(1, 250000));
insert into source select generate_series, '2023-01-01' from table(generate_series(1, 250000));
insert into source select generate_series, '2024-01-01' from table(generate_series(1, 150000));
CREATE TABLE `target_table` (
  `k1` int(11) NULL COMMENT "",
  `event_day` datetime NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k1`, `event_day`)
PARTITION BY date_trunc('day', event_day)
PROPERTIES (
"replication_num" = "1"
);
insert into target_table select * from source where event_day = '2020-01-01';
insert into target_table select * from source where event_day >= '2022-01-01';
insert into target_table select * from source;
insert into target_table select * from target_table;
insert into target_table select * from source where event_day = '2020-01-01' limit 100000;
insert into target_table select k1, '2026-01-01' from source where event_day = '2021-01-01';
insert into target_table select * from source;
insert into target_table select * from target_table;
drop table target_table;