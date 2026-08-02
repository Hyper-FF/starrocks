CREATE TABLE lineitem (l_shipdate    DATE,
                       l_orderkey    BIGINT,
                       l_linenumber  INT,
                       l_partkey     INT,
                       l_suppkey     INT,
                       l_quantity    DECIMAL(15, 2),
                       l_extendedprice  DECIMAL(15, 2),
                       l_discount    DECIMAL(15, 2),
                       l_tax         DECIMAL(15, 2),
                       l_returnflag  VARCHAR(1),
                       l_linestatus  VARCHAR(1),
                       l_commitdate  DATE,
                       l_receiptdate DATE,
                       l_shipinstruct VARCHAR(25),
                       l_shipmode     VARCHAR(10),
                       l_comment      VARCHAR(44)
) ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`, `l_linenumber`)
COMMENT "OLAP"
PARTITION BY RANGE(`l_shipdate`)
(
   START ("1992-01-01") END ("1999-01-01") EVERY (INTERVAL 1 year)
)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO lineitem (l_shipdate, l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES
('1995-03-10', 100001, 1, 5501, 101, 12.00, 1560.00, 0.05, 0.08, 'N', 'O', '1995-03-10', '1995-03-20', 'DELIVER IN PERSON', 'TRUCK', 'First item of order'),
('1995-03-10', 100001, 2, 7203, 203, 8.50, 892.50, 0.00, 0.08, 'N', 'O', '1995-03-10', '1995-03-18', 'TAKE BACK RETURN', 'AIR', 'Second item, fragile'),
('1995-03-22', 100002, 1, 3345, 156, 24.00, 3120.00, 0.10, 0.07, 'R', 'F', '1996-08-18', '1996-08-25', 'COLLECT COD', 'SHIP', 'Bulk order item'),
('1995-02-05', 100003, 1, 8890, 89, 3.00, 450.00, 0.02, 0.09, 'A', 'O', '1997-12-01', '1997-12-10', 'NONE', 'MAIL', 'Small quantity item'),
('1995-05-30', 100004, 1, 4567, 305, 150.00, 22500.00, 0.15, 0.06, 'N', 'O', '1998-05-25', '1998-06-05', 'DELIVER IN PERSON', 'RAIL', 'Large bulk shipment');
INSERT INTO lineitem (l_shipdate, l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES
('1996-01-15', 100011, 1, 5501, 101, 12.00, 1560.00, 0.05, 0.08, 'N', 'O', '1995-03-10', '1995-03-20', 'DELIVER IN PERSON', 'TRUCK', 'First item of order'),
('1996-02-15', 100011, 2, 7203, 203, 8.50, 892.50, 0.00, 0.08, 'N', 'O', '1995-03-10', '1995-03-18', 'TAKE BACK RETURN', 'AIR', 'Second item, fragile'),
('1996-03-22', 100012, 1, 3345, 156, 24.00, 3120.00, 0.10, 0.07, 'R', 'F', '1996-08-18', '1996-08-25', 'COLLECT COD', 'SHIP', 'Bulk order item'),
('1996-12-05', 100013, 1, 8890, 89, 3.00, 450.00, 0.02, 0.09, 'A', 'O', '1997-12-01', '1997-12-10', 'NONE', 'MAIL', 'Small quantity item'),
('1996-04-30', 100014, 1, 4567, 305, 150.00, 22500.00, 0.15, 0.06, 'N', 'O', '1998-05-25', '1998-06-05', 'DELIVER IN PERSON', 'RAIL', 'Large bulk shipment');
INSERT INTO lineitem (l_shipdate, l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES
('1997-01-15', 100101, 1, 5501, 101, 12.00, 1560.00, 0.05, 0.08, 'N', 'O', '1995-03-10', '1995-03-20', 'DELIVER IN PERSON', 'TRUCK', 'First item of order'),
('1997-02-15', 100101, 2, 7203, 203, 8.50, 892.50, 0.00, 0.08, 'N', 'O', '1995-03-10', '1995-03-18', 'TAKE BACK RETURN', 'AIR', 'Second item, fragile'),
('1997-03-22', 100102, 1, 3345, 156, 24.00, 3120.00, 0.10, 0.07, 'R', 'F', '1996-08-18', '1996-08-25', 'COLLECT COD', 'SHIP', 'Bulk order item'),
('1997-11-05', 100103, 1, 8890, 89, 3.00, 450.00, 0.02, 0.09, 'A', 'O', '1997-12-01', '1997-12-10', 'NONE', 'MAIL', 'Small quantity item'),
('1997-04-30', 100104, 1, 4567, 305, 150.00, 22500.00, 0.15, 0.06, 'N', 'O', '1998-05-25', '1998-06-05', 'DELIVER IN PERSON', 'RAIL', 'Large bulk shipment');
INSERT INTO lineitem (l_shipdate, l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES
('1998-01-15', 101001, 1, 5501, 101, 12.00, 1560.00, 0.05, 0.08, 'N', 'O', '1995-03-10', '1995-03-20', 'DELIVER IN PERSON', 'TRUCK', 'First item of order'),
('1998-02-15', 101001, 2, 7203, 203, 8.50, 892.50, 0.00, 0.08, 'N', 'O', '1995-03-10', '1995-03-18', 'TAKE BACK RETURN', 'AIR', 'Second item, fragile'),
('1998-03-22', 101002, 1, 3345, 156, 24.00, 3120.00, 0.10, 0.07, 'R', 'F', '1996-08-18', '1996-08-25', 'COLLECT COD', 'SHIP', 'Bulk order item'),
('1998-11-05', 101003, 1, 8890, 89, 3.00, 450.00, 0.02, 0.09, 'A', 'O', '1997-12-01', '1997-12-10', 'NONE', 'MAIL', 'Small quantity item'),
('1998-04-30', 101004, 1, 4567, 305, 150.00, 22500.00, 0.15, 0.06, 'N', 'O', '1998-05-25', '1998-06-05', 'DELIVER IN PERSON', 'RAIL', 'Large bulk shipment');
INSERT INTO lineitem (l_shipdate, l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES    
('1995-01-16', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
('1995-01-16', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
drop table if exists lineitem;
CREATE TABLE `t2` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 48
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));
insert into t0 values (null,null,null,null);
insert into t1 SELECT * FROM t0;
insert into t2 SELECT * FROM t0;
drop table if exists t0;
drop table if exists t1;