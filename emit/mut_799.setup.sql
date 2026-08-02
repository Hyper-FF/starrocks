CREATE TABLE t1 (
    k1 string NOT NULL,
    k2 string,
    k3 DECIMAL(34,0),
    k4 DATE NOT NULL,
    v1 BIGINT sum DEFAULT "0"
)
AGGREGATE KEY(k1,  k2, k3,  k4)
DISTRIBUTED BY HASH(k4);
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);
ALTER TABLE t1 COMPACT;
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);
ALTER TABLE t1 COMPACT;