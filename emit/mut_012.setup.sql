CREATE TABLE t3 (
    c_2_0 LARGEINT NOT NULL,
    c_2_1 LARGEINT NOT NULL,
    c_2_2 LARGEINT NOT NULL,
    c_2_3 LARGEINT NOT NULL,
    c_2_4 LARGEINT NOT NULL,
    c_2_5 LARGEINT NOT NULL,
    c_2_6 LARGEINT NOT NULL,
    c_2_7 LARGEINT NOT NULL,
    c_2_8 LARGEINT NOT NULL,
    c_2_9 LARGEINT NOT NULL,
    c_2_10 LARGEINT NOT NULL,
    c_2_11 LARGEINT NOT NULL,
    c_2_12 LARGEINT NOT NULL,
    c_2_13 LARGEINT NOT NULL,
    c_2_14 LARGEINT NOT NULL,
    c_2_15 LARGEINT NOT NULL
) DUPLICATE KEY (c_2_0) DISTRIBUTED BY HASH (c_2_0) properties("replication_num" = "1");
insert into t3 values (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
insert into t3 values (128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128);
insert into t3 values (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
CREATE TABLE t4 (
    c_2_0 LARGEINT NOT NULL
) DUPLICATE KEY (c_2_0) 
DISTRIBUTED BY HASH (c_2_0) 
properties("replication_num" = "1");
insert into t4 values (1024), (-2139922094);