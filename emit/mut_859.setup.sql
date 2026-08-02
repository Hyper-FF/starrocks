CREATE TABLE `t1` (
  `id` int(11) NOT NULL,
  `dt` date NOT NULL
) ENGINE=OLAP 
PRIMARY KEY(`id`, `dt`)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
      "replication_num" = "1"
);
INSERT INTO t1 VALUES
 (1,"2020-07-02"),(2,"2020-07-05"),(3,"2020-07-08"),(4,"2020-07-11"),
 (1,"2020-07-16"),(2,"2020-07-19"),(3,"2020-07-22"),(4,"2020-07-25"),
 (2,"2020-06-15"),(3,"2020-06-18"),(4,"2020-06-21"),(5,"2020-06-24"),
 (2,"2020-07-02"),(3,"2020-07-05"),(4,"2020-07-08"),(5,"2020-07-11"),
 (2,"2020-07-16"),(3,"2020-07-19"),(4,"2020-07-22"),(5,"2020-07-25");
CREATE MATERIALIZED VIEW mv1 PARTITION BY date_trunc("month", dt1) 
REFRESH DEFERRED MANUAL 
PROPERTIES (
      "replication_num" = "1",
      "session.insert_max_filter_ratio" = "1",
      "session.query_debug_options" = "{'mvRefreshTraceMode':'LOGS', 'mvRefreshTraceModule':'OPTIMIZER'}"
)
AS SELECT time_slice(dt, interval 5 day) as dt1, sum(id) FROM t1 GROUP BY dt1;
CREATE MATERIALIZED VIEW mv2 PARTITION BY date_trunc("month", dt1) 
REFRESH DEFERRED MANUAL 
PROPERTIES (
      "replication_num" = "1",
      "session.insert_max_filter_ratio" = "1",
      "session.query_debug_options" = "{'mvRefreshTraceMode':'LOGS', 'mvRefreshTraceModule':'OPTIMIZER'}"
)
AS SELECT dt as dt1, sum(id) FROM t1 GROUP BY dt1;
INSERT INTO t1 VALUES (1,"2020-07-02"),(2,"2020-07-05"),(3,"2020-07-08"),(4,"2020-07-11");