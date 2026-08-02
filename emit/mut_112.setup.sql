CREATE TABLE t1 (
    k1 bigint,
    c1 array < varchar(65536) > 
) ENGINE = OLAP 
DUPLICATE KEY(k1) PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE t2 (
    k1 bigint,
    c1 bigint
) ENGINE = OLAP 
DUPLICATE KEY(k1) PROPERTIES (
    "replication_num" = "1"
);
insert into t1
values
    (1, ["1","2"]        ), 
    (2, ["0","2","1"]    ), 
    (3, ["0","2","1"]    ), 
    (4, ["1","2"]        ), 
    (5, ["0","2","1"]    ), 
    (6, ["0","2","1","1"]), 
    (7, ["0","2","1"]    ), 
    (8, ["1","2"]        ), 
    (9, ["L","2","1"]    ), 
    (10, ["1","2"]       );
insert into t2
values
    (1, 1),
    (2, 1),
    (3, 3),
    (4, 5);
INSERT INTO t1 (k1, c1)
VALUES 
(1, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
)),
(2, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
)),
(3, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
)),
(4, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
)),
(5, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
));
CREATE TABLE table1 (
    id INT,
    arr_largeint ARRAY<INT> NOT NULL
)PROPERTIES ("replication_num" = "1");
INSERT INTO table1 (id, arr_largeint) VALUES
(1, [1, 2]),
(2, [3, 4, 5]),
(3, [6]);
CREATE TABLE table2 (
    id INT,
    arr_str ARRAY<INT> NOT NULL
) PROPERTIES ("replication_num" = "1");
INSERT INTO table2 (id, arr_str) VALUES
(1, [1, 2, 3]),
(2, [4, 5]),
(3, [6, 7, 8, 9]);