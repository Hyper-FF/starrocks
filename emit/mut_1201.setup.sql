CREATE TABLE ta (id INT, u VARCHAR(100)) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1");
CREATE TABLE tb (id INT, u VARCHAR(100)) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1");
INSERT INTO ta VALUES (1, 'http://x/p?k1=ab%'), (2, 'ABABABAB');
INSERT INTO tb VALUES (1, 'http://x/p?k1=ab%'), (2, 'ZZZZZZZZ');
INSERT INTO ta VALUES (3, 'http://x/p?k1=cd%4');
INSERT INTO ta VALUES (4, 'http://x/p?k1=z%41');