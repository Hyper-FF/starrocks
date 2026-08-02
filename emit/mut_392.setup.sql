create table t_sink (a BIGINT, b VARCHAR(64))
distributed by hash(a) buckets 1 properties("replication_num" = "1");