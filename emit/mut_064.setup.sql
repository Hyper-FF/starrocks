CREATE TABLE sales (
    id INT,
    product VARCHAR(50),
    amount DECIMAL(10, 2),
    quantity INT
) properties ("replication_num"="1");
INSERT INTO sales (id, product, amount, quantity) VALUES
(1, 'A', 100.00, 10),
(2, 'B', 150.00, 20),
(3, 'A', 200.00, 15),
(4, 'B', 250.00, 25),
(5, 'C', 300.00, 30),
(6, 'Laptop', 500.00, 40);
CREATE TABLE products (
    product_id INT,
    product VARCHAR(50),
    category VARCHAR(50)
) properties ("replication_num"="1");
INSERT INTO products (product_id, product, category) VALUES
(1, 'Laptop', 'Electronics'),
(2, 'Smartphone', 'Electronics'),
(3, 'Desk', 'Furniture'),
(4, 'Chair', 'Furniture'),
(5, 'Headphones', 'Electronics');
drop table products;
CREATE TABLE sales (
    id INT,
    product VARCHAR(50),
    category VARCHAR(50),
    amount DECIMAL(10,2),
    quantity INT,
    sale_date DATE,
    region VARCHAR(20),
    gender CHAR(1)
) properties("replication_num" = "1");
CREATE TABLE products (
    product VARCHAR(50),
    category VARCHAR(50),
    brand VARCHAR(30)
) properties("replication_num" = "1");
CREATE TABLE customers (
    id INT,
    name VARCHAR(50),
    age INT,
    vip_level VARCHAR(10)
) PROPERTIES ("replication_num" = "1");
CREATE TABLE regions (
    region VARCHAR(20),
    country VARCHAR(30),
    timezone VARCHAR(20)
) PROPERTIES ("replication_num" = "1");
INSERT INTO sales VALUES
(11, 'Laptop', 'Electronics', 1599.99, 3, '2024-02-01', 'North', 'M'),
(12, 'Dress', 'Clothing', 159.99, 8, '2024-02-02', 'South', 'F'),
(13, 'Phone Case', 'Electronics', 19.99, 50, '2024-02-03', 'East', 'M'),
(14, 'Sneakers', 'Clothing', 89.99, 22, '2024-02-04', 'West', 'F'),
(15, 'Tablet', 'Electronics', 399.99, 0, '2024-02-05', 'North', 'M'),
(16, 'Scarf', 'Clothing', 39.99, 35, '2024-02-06', 'South', 'F'),
(17, 'Monitor', 'Electronics', 299.99, 7, '2024-02-07', 'East', 'M'),
(18, NULL, 'Clothing', 99.99, 11, '2024-02-08', 'West', 'F'),
(19, 'Keyboard', 'Electronics', 79.99, NULL, '2024-02-09', 'North', 'M'),
(20, 'Hat', NULL, 29.99, 40, '2024-02-10', 'South', 'F');
INSERT INTO sales VALUES
(1, 'iPhone', 'Electronics', 999.99, 10, '2024-01-15', 'North', 'M'),
(2, 'MacBook', 'Electronics', 1299.99, 5, '2024-01-16', 'South', 'F'),
(3, 'Shirt', 'Clothing', 29.99, 25, '2024-01-17', 'North', 'M'),
(4, 'Jeans', 'Clothing', 79.99, 15, '2024-01-18', 'East', 'F'),
(5, 'iPad', 'Electronics', 599.99, 8, '2024-01-19', 'West', 'M'),
(6, 'Shoes', 'Clothing', 129.99, 12, '2024-01-20', 'North', 'F'),
(7, 'Watch', 'Electronics', 299.99, 20, '2024-01-21', 'South', 'M'),
(8, 'Jacket', 'Clothing', 199.99, 6, '2024-01-22', 'East', 'F'),
(9, 'Headphones', 'Electronics', 199.99, 30, '2024-01-23', 'West', 'M'),
(10, 'Bag', 'Clothing', 89.99, 18, '2024-01-24', 'North', 'F');
INSERT INTO products VALUES
('iPhone', 'Electronics', 'Apple'),
('MacBook', 'Electronics', 'Apple'),
('iPad', 'Electronics', 'Apple'),
('Watch', 'Electronics', 'Generic'),
('Headphones', 'Electronics', 'Sony');
INSERT INTO customers VALUES
(1, 'Alice', 25, 'Gold'),
(2, 'Bob', 30, 'Silver'),
(3, 'Charlie', 35, 'Bronze'),
(4, 'Diana', 28, 'Gold'),
(5, 'Eve', 32, 'Silver');
INSERT INTO regions VALUES
('North', 'USA', 'EST'),
('South', 'USA', 'CST'),
('East', 'USA', 'EST'),
('West', 'USA', 'PST');