CREATE TABLE t_udaf_cache (c0 int, c1 bigint, c2 bigint) distributed by hash(c0) buckets 4 properties("replication_num"="1");
INSERT INTO t_udaf_cache SELECT generate_series % 100, generate_series, cast(rand()*100 as int)  FROM table(generate_series(1, 10000));
DROP TABLE t_udaf_cache;