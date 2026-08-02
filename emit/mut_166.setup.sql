CREATE TABLE t1 (
    dt date,
    province string,
    num int
)
DUPLICATE KEY(dt, province)
PARTITION BY date_trunc('day', dt)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1(dt, province, num)
SELECT minutes_add(hours_add(date_add('2025-01-01', x), x%24), x%60), concat('x-', x%3), x
FROM TABLE(generate_series(0, 500)) as t(x);
INSERT INTO t1(dt, province, num) SELECT NULL, NULL, NULL;