drop table if exists t1_agg;
CREATE TABLE t1_agg (
    dt DATE,
    c0 INT,
    c1 VARCHAR(20) REPLACE,
    c2 VARCHAR(200) REPLACE,
    c3 INT SUM,
    c4 INT SUM,
    row_cnt INT SUM
) ENGINE=OLAP
AGGREGATE KEY(dt, c0)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(c0) BUCKETS 48
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1_agg
SELECT '2026-01-01', generate_series % 10, 'val_' || CAST(generate_series AS VARCHAR),
       'desc_' || CAST(generate_series AS VARCHAR), generate_series, generate_series * 2, 1
FROM TABLE(generate_series(1, 100));
CREATE MATERIALIZED VIEW test_mv_agg1
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY hash(c0)
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT dt, c0, SUM(c3) as sum_c3, SUM(row_cnt) as cnt_rows
FROM t1_agg
GROUP BY dt, c0;
INSERT INTO t1_agg VALUES ('2026-01-01', 0, 'val_0', 'desc_0', 10, 20, 1);
ALTER TABLE t1_agg ADD COLUMN c5 VARCHAR(100) REPLACE;
ALTER TABLE t1_agg ADD COLUMN c6 INT SUM;
INSERT INTO t1_agg VALUES ('2026-01-01', 0, 'val_1', 'desc_1', 15, 30, 1, 'desc_2', 25);
INSERT INTO t1_agg VALUES ('2026-01-01', 0, 'val_2', 'desc_2', 20, 40, 1, 'desc_3', 35);
INSERT INTO t1_agg VALUES ('2026-01-01', 0, 'val_4', 'desc_4', 30, 60, 1, 'desc_5', 55);
INSERT INTO t1_agg VALUES ('2026-01-01', 0, 'val_5', 'desc_5', 35, 70, 1, 'desc_6', 65);
INSERT INTO t1_agg VALUES ('2026-01-01', 0, 'val_6', 'desc_6', 40, 80, 1, 'desc_7', 75);
DROP TABLE t1_agg;
drop table if exists t2_agg;
CREATE TABLE t2_agg (
    dt DATE,
    c0 INT,
    c1 VARCHAR(20) REPLACE,
    c2 INT SUM,
    c3 BIGINT SUM,
    c4 VARCHAR(100) REPLACE,
    c5 DOUBLE SUM
) ENGINE=OLAP
AGGREGATE KEY(dt, c0)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(c0)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t2_agg
SELECT '2026-01-01', generate_series % 5, 'val_' || CAST(generate_series AS VARCHAR),
       generate_series, generate_series * 100, 'replace_' || CAST(generate_series AS VARCHAR), generate_series * 0.5
FROM TABLE(generate_series(1, 50));
CREATE MATERIALIZED VIEW test_mv_agg2
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(c0)
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT dt, c0, SUM(c2) as sum_c2, MAX(c3) as max_c3, MIN(c3) as min_c3, AVG(c5) as avg_c5
FROM t2_agg
GROUP BY dt, c0;
ALTER TABLE t2_agg ADD COLUMN c6 INT MAX;
INSERT INTO t2_agg VALUES ('2026-01-01', 0, 'new_val', 100, 1000, 'replace_new', 10.5, 50);
DROP TABLE t2_agg;
drop table if exists t3_agg;
CREATE TABLE t3_agg (
    dt DATE,
    c0 INT,
    c1 VARCHAR(20) REPLACE,
    c2 INT SUM,
    c3 VARCHAR(200) REPLACE
) ENGINE=OLAP
AGGREGATE KEY(dt, c0)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(c0)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t3_agg
SELECT '2026-01-01', generate_series % 5, 'val_' || CAST(generate_series AS VARCHAR),
       generate_series, 'desc_' || CAST(generate_series AS VARCHAR)
FROM TABLE(generate_series(1, 50));
CREATE MATERIALIZED VIEW test_mv_agg3
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(c0)
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT dt, c0, SUM(c2) as total_c2
FROM t3_agg
GROUP BY dt, c0;
ALTER TABLE t3_agg ADD COLUMN c4 INT SUM;
INSERT INTO t3_agg VALUES ('2026-01-01', 0, 'new_val', 200, 'new_desc', 50);
DROP TABLE t3_agg;
drop table if exists t4_agg;
CREATE TABLE t4_agg (
    dt DATE,
    category_id INT,
    product_id INT REPLACE,
    sales_amount DECIMAL(20, 2) SUM,
    quantity INT SUM,
    discount_amount DECIMAL(20, 2) SUM,
    product_name VARCHAR(100) REPLACE
) ENGINE=OLAP
AGGREGATE KEY(dt, category_id)
PARTITION BY date_trunc('month', dt)
DISTRIBUTED BY HASH(category_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t4_agg
SELECT '2026-01-01', 1, 100, 1500.50, 10, 100.25, 'Product A'
UNION ALL
SELECT '2026-01-01', 1, 101, 2500.75, 15, 200.50, 'Product A'
UNION ALL
SELECT '2026-01-01', 2, 200, 3000.00, 20, 300.00, 'Product B'
UNION ALL
SELECT '2026-01-02', 1, 100, 1800.25, 12, 150.00, 'Product A';
CREATE MATERIALIZED VIEW test_mv_agg4
PARTITION BY date_trunc('month', dt)
DISTRIBUTED BY HASH(category_id)
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT dt, category_id,
       SUM(sales_amount) as total_sales,
       SUM(quantity) as total_quantity,
       SUM(discount_amount) as total_discount
FROM t4_agg
GROUP BY dt, category_id;
ALTER TABLE t4_agg ADD COLUMN profit DECIMAL(20, 2) SUM;
INSERT INTO t4_agg VALUES ('2026-01-03', 3, 300, 5000.00, 30, 500.00, 'Product C', 1.00);
ALTER TABLE t4_agg ADD COLUMN region VARCHAR(50) REPLACE;
INSERT INTO t4_agg VALUES ('2026-01-03', 3, 300, 0, 0, 0, 'Product C', 0, 'north');
DROP TABLE t4_agg;