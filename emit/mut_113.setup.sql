CREATE TABLE `tbl` (k1 string,k2 string,k3 int) DUPLICATE KEY(`k1`) DISTRIBUTED BY HASH(`k1`) BUCKETS 3 PROPERTIES ("replication_num" = "1");
insert into tbl values
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1);