CREATE TABLE left_table (
    name VARCHAR(20),
    id1 INT,
    age INT,
    city VARCHAR(20),
    id2 INT,
    salary DECIMAL(10,2),
    status VARCHAR(10)
) DUPLICATE KEY(name) DISTRIBUTED BY HASH(name) BUCKETS 1 PROPERTIES ("replication_num" = "1");
CREATE TABLE right_table (
    dept VARCHAR(20),
    id1 INT,
    bonus DECIMAL(8,2),
    id2 INT
) DUPLICATE KEY(dept) DISTRIBUTED BY HASH(dept) BUCKETS 1 PROPERTIES ("replication_num" = "1");
INSERT INTO left_table VALUES
    ('Alice', 1, 25, 'New York', 100, 5000.00, 'Active'),
    ('Bob', 2, 30, 'Boston', 200, 6000.00, 'Active'),
    ('Charlie', 3, 35, 'Chicago', 300, 7000.00, 'Inactive'),
    ('David', 4, 28, 'Denver', NULL, 5500.00, 'Active'),
    ('Eve', 5, 32, 'Seattle', 500, 6500.00, 'Active'),
    ('Frank', 6, 40, NULL, 600, 8000.00, 'Inactive');
INSERT INTO right_table VALUES 
    ('Engineering', 1, 1000.00, 100),
    ('Marketing', 2, 800.00, 200),
    ('Sales', 7, 1200.00, 700),
    ('HR', 8, 900.00, NULL),
    ('Finance', 9, NULL, 900);