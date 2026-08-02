CREATE TABLE test_varbinary_create (
    id INT NOT NULL,
    name VARCHAR(50),
    binary_col VARBINARY DEFAULT "",
    binary_no_default VARBINARY
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_varbinary_create (id, name) VALUES (1, 'user1');
INSERT INTO test_varbinary_create (id, name, binary_no_default) VALUES (2, 'user2', 'custom');
INSERT INTO test_varbinary_create VALUES (3, 'user3', 'explicit', 'data');
INSERT INTO test_varbinary_create VALUES (4, 'user4', '', NULL);
INSERT INTO test_varbinary_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
ALTER TABLE test_varbinary_alter ADD COLUMN binary_col VARBINARY DEFAULT "";
INSERT INTO test_varbinary_traditional VALUES (1, 'eve'), (2, 'frank');
ALTER TABLE test_varbinary_traditional ADD COLUMN binary_col VARBINARY DEFAULT "";
INSERT INTO test_varbinary_primary (order_id, customer) VALUES (1, 'customer1');
INSERT INTO test_varbinary_primary VALUES (2, 'customer2', 'sign123');
INSERT INTO test_varbinary_primary VALUES (3, 'customer3', '');
INSERT INTO test_varbinary_edge (id) VALUES (1);
INSERT INTO test_varbinary_edge VALUES (2, NULL);
INSERT INTO test_varbinary_edge VALUES (3, '');
INSERT INTO test_varbinary_sized (id) VALUES (1);
INSERT INTO test_varbinary_sized VALUES (2, 'short', 'this_is_longer');
CREATE TABLE without_default (
    id INT,
    binary_col VARBINARY
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO with_default (id) VALUES (1);
INSERT INTO without_default (id) VALUES (1);