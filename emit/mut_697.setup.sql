CREATE TABLE test_col_value (
    col_1 INT,
    col_2 INT,
    col_3 INT NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);
INSERT INTO test_col_value (col_1, col_2, col_3) VALUES (1, 1, 11), (2, 2, 22), (3, 3, 33), (4, NULL, 44), (5, 5, 55), (6, 6, 66);
CREATE TABLE test_col_value2 (
    col_1 INT,
    col_2 INT,
    col_3 VARCHAR(255) NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);
INSERT INTO test_col_value2 (col_1, col_2, col_3) VALUES (1, 1, '11'), (2, 2, '22'), (3, 3, '33'), (4, NULL, '44'), (5, 5, '55'), (6, 6, '66'), (7, 7, '7djaiojdoa'), (8, NULL, '8djaiojdoa'), (9, 9, '0djagdfoi');
CREATE TABLE test_array_value (
    col_1 INT,
    arr1 ARRAY<INT>,
    arr2 ARRAY<INT> NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);
INSERT INTO test_array_value (col_1, arr1, arr2) VALUES
    (1, [1, 11], [101, 111]),
    (2, [2, 22], [102, 112]),
    (3, [3, 33], [103, 113]),
    (4, NULL,    [104, 114]),
    (5, [5, 55], [105, 115]),
    (6, [6, 66], [106, 116]);
CREATE TABLE test_varchar_value (
    col_1 INT,
    v1 VARCHAR(255),
    v2 VARCHAR(255) NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);
INSERT INTO test_varchar_value (col_1, v1, v2) VALUES
    (1, '1',  '11'),
    (2, '2',  '22'),
    (3, '3',  '33'),
    (4, NULL, '44'),
    (5, '5',  '55'),
    (6, '6',  '66');
CREATE TABLE test_array_varchar_value (
    col_1 INT,
    arr1 ARRAY<VARCHAR(10)>,
    arr2 ARRAY<VARCHAR(10)> NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);
INSERT INTO test_array_varchar_value (col_1, arr1, arr2) VALUES
    (1, ['1','11'], ['101','111']),
    (2, ['2','22'], ['102','112']),
    (3, ['3','33'], ['103','113']),
    (4, NULL,       ['104','114']),
    (5, ['5','55'], ['105','115']),
    (6, ['6','66'], ['106','116']);