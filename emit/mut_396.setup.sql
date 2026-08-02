create table t_sink_big (x BIGINT)
distributed by hash(x) buckets 1 properties("replication_num" = "1");