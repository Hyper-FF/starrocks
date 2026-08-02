CREATE TABLE map_top_n
    (c1 int,
    c2 map<varchar(8), int>)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");
insert into map_top_n values
(1, map{"key1":1}),
(2, map{"key1":5, "key2":6}),
(3, map{"key1":2, "key2":3, "key4":4}),
(4, map{"key1":12, "key2":13, "key3":14, "key4":15}),
(5, map{"key1":7, "key2":8, "key3":9, "key4":10, "key5":11});
CREATE TABLE test_map(
    col_int INT,
    col_map MAP<VARCHAR(50),INT>
  )
DUPLICATE KEY(col_int)
PROPERTIES ("replication_num" = "1");
INSERT INTO test_map VALUES
(1,map{"a":1,"b":2}),
(2,map{"c":3}),
(3,map{"d":4,"e":5});