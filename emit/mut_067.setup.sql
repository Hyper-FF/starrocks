CREATE TABLE src (k INT, s VARCHAR(100))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");
INSERT INTO src
SELECT g1.generate_series AS k,
       concat('str_', cast((g2.generate_series % 40) AS string)) AS s
FROM TABLE(generate_series(1, 10000)) g1, TABLE(generate_series(1, 40)) g2;
CREATE TABLE st (k INT, v array_agg_distinct(varchar(100)))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");
INSERT INTO st SELECT k, array_agg_distinct_combine(s) FROM src GROUP BY k;
INSERT INTO blackhole() SELECT array_agg_distinct_state_merge(v) FROM st;