create table test_overwrite_stats_table (k1 int) properties("replication_num"="1");
insert into test_overwrite_stats_table select 123;
INSERT INTO sales_data VALUES
(1, '2024-01-15'),
(2, '2024-01-20'),
(3, '2024-02-10'),
(4, '2024-02-15'),
(5, '2024-03-05'),
(6, '2024-03-12'),
(7, '2024-04-08'),
(8, '2024-04-18');
create table test_overwrite_statistics.test_overwrite_with_full (k1 int) properties("replication_num"="1");
insert overwrite test_overwrite_statistics.test_overwrite_with_full select generate_series from table(generate_series(1, 5000));
create table test_overwrite_statistics.test_overwrite_with_sample (k1 int) properties("replication_num"="1");
insert overwrite test_overwrite_statistics.test_overwrite_with_sample select generate_series from table(generate_series(1, 300000));
CREATE TABLE sales_data (
    id BIGINT,
    sale_date DATE
)
DUPLICATE KEY(id)
PARTITION BY RANGE(sale_date) (
    PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
    PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),
    PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),
    PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01'))
)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
alter table test_overwrite_statistics.sales_data set("enable_statistic_collect_on_first_load"="false");
INSERT INTO sales_data VALUES
(1, '2024-01-15'),
(2, '2024-01-20'),
(3, '2024-02-10'),
(4, '2024-02-15'),
(5, '2024-03-05'),
(6, '2024-03-12'),
(7, '2024-04-08'),
(8, '2024-04-18');
INSERT OVERWRITE test_overwrite_statistics.sales_data partition("p202401") VALUES (102, '2024-01-10');
drop table test_overwrite_statistics.test_overwrite_stats_table;
create table test_overwrite_stats_table (k1 int) properties("replication_num"="1");
insert into test_overwrite_stats_table select generate_series from table(generate_series(1, 1000));
insert overwrite test_overwrite_stats_table select generate_series from table(generate_series(10000, 20000));
drop table if exists expr_range_partitioned_table;
create table expr_range_partitioned_table (
    dt datetime,
    k1 int,
    k2 varchar(20)
)
partition by date_trunc('day', dt)
properties("replication_num"="1");
insert into expr_range_partitioned_table select '2024-01-01 08:00:00', generate_series, "data1" from table(generate_series(1, 1000));
insert overwrite expr_range_partitioned_table select '2024-01-01 08:00:00', generate_series, "data1" from table(generate_series(1, 2000));