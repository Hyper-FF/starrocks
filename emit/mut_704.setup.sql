CREATE TABLE partitions_multi_column_1 (
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 int
)
PARTITION BY (c1, c2) properties("replication_num" = "1");
INSERT INTO partitions_multi_column_1 VALUES
    (1,1,1),
    (1,2,4),
    (1,2,4),
    (1,2,4),
    (2,3,2),
    (2,4,5),
    (3,5,3),
    (3,6,6);
INSERT INTO partitions_multi_column_1 
SELECT 4, 7, generate_series FROM TABLE(generate_series(1, 1000));
CREATE TABLE partitions_multi_column_2 (
    c1 int,
    c2 int,
    c3 int,
    p1 int
)
PARTITION BY (p1) properties("replication_num" = "1");
insert into partitions_multi_column_2 select generate_series % 10, generate_series % 10, generate_series % 10, 1 from table(generate_series(1, 1000000));
insert into partitions_multi_column_2 select generate_series % 10, generate_series % 10, generate_series % 10, 2 from table(generate_series(1, 1000000));
insert into partitions_multi_column_2 select generate_series % 10, generate_series % 10, generate_series % 10, 3 from table(generate_series(1, 1000000));
insert into partitions_multi_column_2 select generate_series % 10, generate_series % 10, generate_series % 10, 4 from table(generate_series(1, 1000000));
insert into partitions_multi_column_2 select generate_series % 10, generate_series % 10, generate_series % 10, 5 from table(generate_series(1, 1000000));
CREATE TABLE partitions_multi_column_3 (
    c1 int NOT NULL,
    p1 int
)
PARTITION BY (p1) properties("replication_num"="1");
insert into partitions_multi_column_3 select 1, 1 from table(generate_series(1, 90));
insert into partitions_multi_column_3 select 2, 1 from table(generate_series(1, 10));
insert into partitions_multi_column_3 select 3, 2 from table(generate_series(1, 100));
insert into partitions_multi_column_3 select 3, 3 from table(generate_series(1, 100));
insert into partitions_multi_column_3 select 3, 4 from table(generate_series(1, 100));
insert into partitions_multi_column_3 select 3, 5 from table(generate_series(1, 100));