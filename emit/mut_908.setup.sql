DROP TABLE if exists t0;
CREATE TABLE if not exists t0
    (
        c0 VARCHAR(32) NOT NULL,
        c1 VARCHAR(32) NOT NULL,
        c2 VARCHAR(32) NOT NULL,
        c3 VARCHAR(32) NOT NULL,
        c4 VARCHAR(32) NOT NULL,
        v0 INT NOT NULL,
        v1 INT NOT NULL,
        v2 INT NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`c0`, `c1`, `c2`
    )
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`c0`, `c1`, `c2`
    ) BUCKETS 8
    PROPERTIES(
        "replication_num" = "1",
        "in_memory" = "false",
        "storage_format" = "default"
    );
INSERT INTO t0
    (c0, c1, c2, c3, c4, v0, v1, v2
    )
    VALUES
        ('C0_4', 'C1_5', 'C2_2', 'C3_4', 'C4_9', '-1', '4167', '-3559'
            ),
        ('C0_4', 'C1_7', 'C2_3', 'C3_7', 'C4_3', '31', '95750482', '449147'
            ),
        ('C0_8', 'C1_4', 'C2_1', 'C3_3', 'C4_1', '-285372464', '162472548', '-30'
            ),
        ('C0_6', 'C1_7', 'C2_9', 'C3_5', 'C4_2', '196903', '1506722', '-106'
            ),
        ('C0_6', 'C1_4', 'C2_9', 'C3_3', 'C4_0', '32', '21', '-191811167'
            ),
        ('C0_3', 'C1_4', 'C2_4', 'C3_1', 'C4_6', '1821830096', '5009313', '-8'
            ),
        ('C0_9', 'C1_9', 'C2_8', 'C3_1', 'C4_3', '-235', '-22064666', '-49233'
            ),
        ('C0_4', 'C1_2', 'C2_3', 'C3_4', 'C4_3', '-91843372', '5251096', '55519503'
            );