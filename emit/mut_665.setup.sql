CREATE TABLE t_int (
    id int NOT NULL,
    tinyint_col tinyint,
    smallint_col smallint,
    int_col int,
    bigint_col bigint,
    value varchar(50)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE t_string (
    id int NOT NULL,
    varchar_col varchar(50),
    char_col char(10),
    text_col text,
    category varchar(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE t_mixed (
    id int NOT NULL,
    int_val int,
    str_val varchar(50),
    float_val float,
    double_val double,
    decimal_val decimal(10,2),
    date_val date,
    datetime_val datetime,
    bool_val boolean
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE t_large (
    id bigint NOT NULL,
    category_id int,
    status varchar(20),
    score double,
    created_date date
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE t_dimension (
    dim_id int NOT NULL,
    dim_name varchar(50),
    dim_type varchar(20)
) ENGINE=OLAP
DUPLICATE KEY(dim_id)
DISTRIBUTED BY HASH(dim_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t_int VALUES
(1, 10, 100, 1000, 10000, 'value1'),
(2, 20, 200, 2000, 20000, 'value2'),
(3, 30, 300, 3000, 30000, 'value3'),
(4, 40, 400, 4000, 40000, 'value4'),
(5, 50, 500, 5000, 50000, 'value5'),
(6, 60, 600, 6000, 60000, 'value6'),
(7, 70, 700, 7000, 70000, 'value7'),
(8, 80, 800, 8000, 80000, 'value8'),
(9, 90, 900, 9000, 90000, 'value9'),
(10, 100, 1000, 10000, 100000, 'value10'),
(11, -10, -100, -1000, -10000, 'negative1'),
(12, -20, -200, -2000, -20000, 'negative2'),
(13, 0, 0, 0, 0, 'zero'),
(14, null, null, null, null, 'null_values'),
(15, 15, 150, 1500, 15000, 'extra1');
INSERT INTO t_string VALUES
(1, 'apple', 'fruit', 'This is apple text', 'food'),
(2, 'banana', 'fruit', 'This is banana text', 'food'),
(3, 'carrot', 'vegetable', 'This is carrot text', 'food'),
(4, 'dog', 'animal', 'This is dog text', 'pet'),
(5, 'elephant', 'animal', 'This is elephant text', 'wild'),
(6, 'fish', 'animal', 'This is fish text', 'aquatic'),
(7, 'grape', 'fruit', 'This is grape text', 'food'),
(8, 'house', 'building', 'This is house text', 'shelter'),
(9, 'ice', 'water', 'This is ice text', 'cold'),
(10, 'jungle', 'nature', 'This is jungle text', 'wild'),
(11, 'a''b', 'special', 'Text with quote', 'test'),
(12, 'c"d', 'special', 'Text with double quote', 'test'),
(13, 'e\\f', 'special', 'Text with backslash', 'test'),
(14, '', 'empty', 'Empty string test', 'test'),
(15, null, null, null, null);
INSERT INTO t_mixed VALUES
(1, 100, 'str100', 1.1, 1.11, 100.50, '2024-01-01', '2024-01-01 10:00:00', true),
(2, 200, 'str200', 2.2, 2.22, 200.75, '2024-01-02', '2024-01-02 11:00:00', false),
(3, 300, 'str300', 3.3, 3.33, 300.25, '2024-01-03', '2024-01-03 12:00:00', true),
(4, 400, 'str400', 4.4, 4.44, 400.00, '2024-01-04', '2024-01-04 13:00:00', false),
(5, 500, 'str500', 5.5, 5.55, 500.99, '2024-01-05', '2024-01-05 14:00:00', true),
(6, 600, 'str600', 6.6, 6.66, 600.10, '2024-01-06', '2024-01-06 15:00:00', false),
(7, 700, 'str700', 7.7, 7.77, 700.20, '2024-01-07', '2024-01-07 16:00:00', true),
(8, 800, 'str800', 8.8, 8.88, 800.30, '2024-01-08', '2024-01-08 17:00:00', false),
(9, 900, 'str900', 9.9, 9.99, 900.40, '2024-01-09', '2024-01-09 18:00:00', true),
(10, 1000, 'str1000', 10.0, 10.10, 1000.50, '2024-01-10', '2024-01-10 19:00:00', false);
INSERT INTO t_large VALUES
(1, 1, 'active', 85.5, '2024-01-01'),
(2, 1, 'inactive', 72.3, '2024-01-02'),
(3, 2, 'active', 91.2, '2024-01-03'),
(4, 2, 'pending', 68.7, '2024-01-04'),
(5, 3, 'active', 94.1, '2024-01-05'),
(6, 3, 'inactive', 55.9, '2024-01-06'),
(7, 4, 'active', 88.8, '2024-01-07'),
(8, 4, 'pending', 77.4, '2024-01-08'),
(9, 5, 'active', 92.6, '2024-01-09'),
(10, 5, 'inactive', 63.2, '2024-01-10'),
(11, 1, 'active', 89.3, '2024-01-11'),
(12, 2, 'active', 95.7, '2024-01-12'),
(13, 3, 'pending', 71.8, '2024-01-13'),
(14, 4, 'active', 84.4, '2024-01-14'),
(15, 5, 'inactive', 59.6, '2024-01-15'),
(16, 1, 'pending', 78.9, '2024-01-16'),
(17, 2, 'active', 93.2, '2024-01-17'),
(18, 3, 'active', 87.1, '2024-01-18'),
(19, 4, 'inactive', 66.5, '2024-01-19'),
(20, 5, 'active', 90.8, '2024-01-20');
INSERT INTO t_dimension VALUES
(1, 'Category A', 'primary'),
(2, 'Category B', 'secondary'),
(3, 'Category C', 'primary'),
(4, 'Category D', 'tertiary'),
(5, 'Category E', 'secondary'),
(6, 'Category F', 'primary'),
(7, 'Category G', 'tertiary'),
(8, 'Category H', 'secondary'),
(9, 'Category I', 'primary'),
(10, 'Category J', 'tertiary');