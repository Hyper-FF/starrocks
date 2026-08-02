CREATE TABLE test_array_top_n (
    id INT,
    array_int ARRAY<INT>,
    array_bigint ARRAY<BIGINT>,
    array_float ARRAY<FLOAT>,
    array_double ARRAY<DOUBLE>,
    array_decimalv2 ARRAY<DECIMALV2(10, 2)>,
    array_boolean ARRAY<BOOLEAN>,
    array_date ARRAY<DATE>,
    array_datetime ARRAY<DATETIME>,
    array_varchar ARRAY<VARCHAR(100)>
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO test_array_top_n VALUES
(1, [100, 1, NULL, 5, 100, 1], [100, 1, NULL, 5, 100, 1], [100.0, 1.0, NULL, 5.0, 100.0, 1.0],
 [100.0, 1.0, NULL, 5.0, 100.0, 1.0], [100.0, 1.0, NULL, 5.0, 100.0, 1.0], [true, false, NULL, true, true, false],
 ['2023-12-31', '2023-01-01', NULL, '2023-06-15', '2023-12-31', '2023-01-01'],
 ['2023-12-31 23:59:59', '2023-01-01 00:00:01', NULL, '2023-06-15 12:00:00', '2023-12-31 23:59:59', '2023-01-01 00:00:01'],
 ['zzz', 'a', NULL, 'g', 'zzz', 'a']),
(2, [], [], [], [], [], [], [], [], []),
(3, [100], [100], [100.0], [100.0], [100.0], [true], ['2023-12-31'], ['2023-12-31 23:59:59'], ['zzz']),
(4, [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL],
 [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL]),
(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(6, [5, 5, 5, 3, 3], [5, 5, 5, 3, 3], [5.0, 5.0, 5.0, 3.0, 3.0], [5.0, 5.0, 5.0, 3.0, 3.0],
 [5.0, 5.0, 5.0, 3.0, 3.0], [true, true, true, false, false],
 ['2023-05-05', '2023-05-05', '2023-05-05', '2023-03-03', '2023-03-03'],
 ['2023-05-05 12:00:00', '2023-05-05 12:00:00', '2023-05-05 12:00:00', '2023-03-03 10:00:00', '2023-03-03 10:00:00'],
 ['eee', 'eee', 'eee', 'ccc', 'ccc']);
DROP TABLE test_array_top_n;