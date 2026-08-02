DROP TABLE if exists t0;
CREATE TABLE if not exists t0
    (
        c0 INT NOT NULL,
        c1 VARCHAR(32) NOT NULL,
        c2 VARCHAR(32) NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`c0`
    )
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`c0`, `c1`
    ) BUCKETS 64
    PROPERTIES(
        "replication_num" = "1",
        "in_memory" = "false",
        "storage_format" = "default"
    );
DROP TABLE if exists t1;
CREATE TABLE if not exists t1
    (
        c0 INT NOT NULL,
        c1 VARCHAR(32) NOT NULL,
        c2 VARCHAR(32) NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`c0`
    )
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`c0`, `c1`
    ) BUCKETS 64
    PROPERTIES(
        "replication_num" = "1",
        "in_memory" = "false",
        "storage_format" = "default"
    );
DROP TABLE if exists t2;
CREATE TABLE if not exists t2
    (
        c0 INT NOT NULL,
        c1 VARCHAR(32) NOT NULL,
        c2 VARCHAR(32) NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`c0`
    )
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`c0`, `c1`
    ) BUCKETS 64
    PROPERTIES(
        "replication_num" = "1",
        "in_memory" = "false",
        "storage_format" = "default"
    );
INSERT INTO t0
    (c0, c1, c2
    )
    VALUES
        ('4', 'DvvRNNLAAKj5mc3e', 'oHHGEp'
            ),
        ('8', 'YMV5W6K3Jcv5kp', 'dm9h5J8cHtX6GfDZ8J7odAIJxyZdJSa'
            ),
        ('4', 'nqrRk', 'D1ephlGonrHHWnY4ThjhO11'
            ),
        ('6', 'A9fWZWnk1WWTJ37', 'V'
            ),
        ('9', 'A6f2iV', 'kd2S'
            ),
        ('2', 'KxNCswN6q1xZgBmvGxjr24Y', '6SqBgNekQtxOWiZXBrZuaAI1r'
            ),
        ('2', 'Sl4ZtHpUMR6JKt0uYkcHvjsNs', 'TmysYNLEyxNGnkgk4NlbSAkS'
            ),
        ('8', '985m0SmtQLKH4zZHS', '9BRWRd8pAbBjtqFYfdaeu'
            ),
        ('7', 'TRXAIbYTmrTPV1F0', 'KJHp'
            ),
        ('3', 'GLRmc1tmqiHpyi4dMaAb0F', 'Q3OkhuxxGMDwTfo273'
            );
INSERT INTO t1
    (c0, c1, c2
    )
    VALUES
        ('7', '5WUQFPf', '9GszO3v'
            ),
        ('4', 'uGywLy7', 'XdPwCy8Kb9wgLzco'
            ),
        ('6', '', 'QiWAR1yoW2TsD4hrC16saOw'
            ),
        ('8', 'a1HBEfnDhbed4gLtlq', '7QgGrBQDn14bTfdOLSKF'
            ),
        ('4', 'r2Igrl0jHPT', 'BPhnKYMqDSTQtXKUEKpjyKodTeLFI9'
            ),
        ('4', 'SAztGc55aDcK9jeAnq57eMsXEfYdHA', 'NEfQ'
            ),
        ('11', 'mtXUECE2TG23OLToBhvxpPaO', 'zxX'
            ),
        ('6', '', 'tji7m72J'
            ),
        ('6', '00RSjBQ71AuYlAZg', 'yGGjrU3Y'
            ),
        ('9', 'w', 'f9VzBNpbKLt5'
            );
INSERT INTO t2
    (c0, c1, c2
    )
    VALUES
        ('10', 'hHG9GCIJaE0R1LGw1YOON14EP', 'TSIquQ1H9SuiyrkWXHsPAb'
            ),
        ('12', 'GZjvI0nqA29SNPeJu', 'Jv8hlqasc4X'
            ),
        ('13', 'yZRjWXPvJ5K7', 'Yy8BcdS'
            ),
        ('13', 'Eq3zv98', 'enUAyzJ'
            ),
        ('11', 'KfXyxjNoR1UMJLQVov9gqvqW', 'D8tVXXUt'
            ),
        ('13', '0EMj6DoEnTBfu', '46aDAUNOiCYdlhFt7N1AUtkQduVmr0'
            ),
        ('7', 'sMPdn6duIpSPEakZvgWTHaqGmVCx', 'EXN0OJXKgpJ5GXS5M527hGCutewN2Y'
            ),
        ('8', 'GwtKaeSezvRskPeKQA', '2Q3oSkQI4FjMyNKgBA3qZZwPfAL'
            ),
        ('6', 'RlUBfggH9iqhgxsUcU4e', 'x8xTgfsJX5ymxNUr'
            ),
        ('13', 'RrpepqVmxIYhRv', 'JkLAXbF3n0qFrhKJBFiektS3K1vajU'
            );