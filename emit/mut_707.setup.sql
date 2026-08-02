CREATE TABLE partitions_multi_column_1 (
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 int
)
PARTITION BY (c1, c2) properties("replication_num"="1");
INSERT INTO partitions_multi_column_1 SELECT 0, 0, generate_series FROM TABLE(generate_series(1, 1000));
INSERT INTO partitions_multi_column_1 SELECT 0, 1, generate_series FROM TABLE(generate_series(1, 100));
INSERT INTO partitions_multi_column_1 SELECT 0, 2, generate_series FROM TABLE(generate_series(1, 10));
INSERT INTO partitions_multi_column_1 SELECT 0, 3, generate_series FROM TABLE(generate_series(1, 1));
INSERT INTO partitions_multi_column_1 SELECT 1, 0, generate_series FROM TABLE(generate_series(1, 100));
INSERT INTO partitions_multi_column_1 SELECT 2, 0, generate_series FROM TABLE(generate_series(1, 100));
INSERT INTO partitions_multi_column_1 SELECT 3, 0, generate_series FROM TABLE(generate_series(1, 100));
INSERT INTO partitions_multi_column_1 SELECT 4, 0, generate_series FROM TABLE(generate_series(1, 100));
CREATE TABLE partitions_multi_column_2 (
    c1 int,
    p1 int
)
PARTITION BY (p1) properties("replication_num"="1");
insert into partitions_multi_column_2 select 1,1;
insert into partitions_multi_column_2 select 2,1;
insert into partitions_multi_column_2 select 1,2;
insert into partitions_multi_column_2 select null,2;
insert into partitions_multi_column_2 select null, generate_series from table(generate_series(3,10));