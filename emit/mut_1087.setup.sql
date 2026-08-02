CREATE TABLE struct_upper_case
    (c1 int,
    c2 struct<c2_sub1 int, C2_SUB2 int>)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");
insert into struct_upper_case values (1, named_struct('c2_sub1', 1, 'c2_sub2', 1)), (2, named_struct('C2_SUB1', 2, 'C2_SUB2', 2));