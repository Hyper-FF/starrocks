CREATE TABLE test_first_load (
    event_day datetime,
    k1 int
) PARTITION BY date_trunc('day', event_day)
PROPERTIES (
"replication_num" = "1"
);
insert into test_first_load select '2020-01-01', generate_series from table(generate_series(1,3000000));