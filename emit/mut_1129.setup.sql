CREATE TABLE IF NOT EXISTS invalid_plan (
    `time` DATETIME NOT NULL,
    `country` STRING NOT NULL,
    `spend` DOUBLE SUM DEFAULT "0",
    `revenue` DOUBLE SUM DEFAULT "0"
)
ENGINE=OLAP
AGGREGATE KEY(`time`, `country`)
PARTITION BY date_trunc('day', `time`)
DISTRIBUTED BY HASH(`country`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1" 
);
INSERT INTO invalid_plan VALUES ('2020-01-01', 'US', 100, 1000);