CREATE TABLE t1 (
    k1 int,
    k2 date,
    k3 string
)
DUPLICATE KEY(k1)
PARTITION BY date_trunc("day", k2);
INSERT INTO t1 VALUES (1,'2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2,'2020-07-02','SH');
CREATE MATERIALIZED VIEW mv1
partition by (date_trunc("day", k2))
REFRESH MANUAL
AS select sum(k1), k2, k3 from t1 group by k2, k3;
alter table t1 partition by date_trunc("month", k2);
INSERT INTO t1 VALUES (1,'2020-06-02','BJ'),(2,'2020-07-02','SH');