CREATE TABLE T (
    id INT,
    value VARCHAR(10),
    arr ARRAY<VARCHAR(10)>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO T VALUES (1, 'a', ['aa', 'aaa']), (2, 'b', ['bb', 'bbb']), (3, 'c', ['cc', 'ccc']);