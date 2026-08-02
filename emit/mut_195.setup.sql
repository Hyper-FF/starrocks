CREATE TABLE t (
    c1 int,
    c2 json
) PROPERTIES ("replication_num" = "1");
insert into t values
(1, '[1,2,3]'),
(2, '"abc"'),
(3, 'null'),
(4, 'true'),
(5, '1'),
(6, '{"1":1, "2":true, "3":null, "4":[5,6,7], "5":{"k51":"v51","k52":"v52"}}');