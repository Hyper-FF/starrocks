CREATE TABLE t_dict_in (
    id INT NOT NULL,
    val VARCHAR(50) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t_dict_in VALUES
    (1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta'), (5, 'epsilon'),
    (6, 'zeta'), (7, 'eta'), (8, 'theta'), (9, 'iota'), (10, 'kappa'),
    (11, 'lambda'), (12, 'mu'), (13, 'nu'), (14, 'xi'), (15, 'omicron'),
    (16, 'pi'), (17, 'rho'), (18, 'sigma'), (19, 'tau'), (20, 'upsilon'),
    (21, 'alpha'), (22, 'beta'), (23, 'gamma'), (24, 'delta'), (25, 'epsilon'),
    (26, 'zeta'), (27, 'eta'), (28, 'theta'), (29, 'iota'), (30, 'kappa'),
    (31, 'lambda'), (32, 'mu'), (33, 'nu'), (34, 'xi'), (35, 'omicron'),
    (36, 'pi'), (37, 'rho'), (38, 'sigma'), (39, 'tau'), (40, 'upsilon'),
    (41, 'alpha'), (42, 'beta'), (43, 'gamma'), (44, 'delta'), (45, 'epsilon'),
    (46, 'zeta'), (47, 'eta'), (48, 'theta'), (49, 'iota'), (50, 'kappa');
CREATE TABLE t_dict_large (
    id INT NOT NULL,
    val VARCHAR(50) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t_dict_large
SELECT
    generate_series,
    CONCAT('val_', CAST(generate_series % 128 AS VARCHAR))
FROM TABLE(generate_series(1, 3000));
CREATE TABLE t_dict_eq (
    id INT NOT NULL,
    val VARCHAR(50) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t_dict_eq VALUES
    (1, 'foo'), (2, 'bar'), (3, 'baz'), (4, 'foo'), (5, 'qux'),
    (6, 'bar'), (7, 'foo'), (8, 'quux'), (9, 'baz'), (10, 'foo');