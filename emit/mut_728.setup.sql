CREATE TABLE test_map
    (c1 int,
    c2 map<varchar(8), int>)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");
insert into test_map SELECT generate_series, map(generate_series, generate_series) FROM TABLE(generate_series(1,  4096));