CREATE TABLE map_array_tbl
    (c1 int,
    c2 map<varchar(8), int>,
    c3 array<int>)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");
insert into map_array_tbl values
(1, map{"key1":1}, [1]),
(2, map{"key1":5, "key2":6}, [1, 2]),
(3, null, null);