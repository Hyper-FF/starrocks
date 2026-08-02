CREATE TABLE t1 ( 
k1  date, 
c0 boolean,
c1 tinyint(4),
c2 smallint(6),
c3 int(11),
c4 bigint(20),
c5 largeint(40),
c6 double,
c7 float,
c8 decimal(10, 2),
c9 char(100),
c10 date,
c11 datetime,
c12 array<boolean>,
c13 array<tinyint(4)>,
c14 array<smallint(6)>,
c15 array<int(11)>,
c16 array<bigint(20)>,
c17 array<largeint(40)>,
c18 array<double>,
c19 array<float>,
c20 array<DECIMAL64(10,2)>,
c21 array<char(100)>,
c22 array<date>,
c23 array<datetime>,
c24 varchar(100),
c25 json,
c26 varbinary,
c27 map<varchar(1048576),varchar(1048576)>,
c28 struct<col1 array<varchar(1048576)>>,
c29 array<varchar(100)>) DUPLICATE KEY(k1) 
DISTRIBUTED BY HASH(k1) 
PROPERTIES (  "replication_num" = "1");
INSERT INTO t1 VALUES 
(
  '2024-01-01',                    
  TRUE,                            
  127,                             
  32000,                           
  2147483647,                      
  9223372036854775807,             
  170141183460469231731687303715884105727,  
  12345.6789,                      
  12345.67,                        
  123.45678901,                    
  'ExampleCharData',               
  '2024-01-02',                    
  '2024-01-01 12:34:56',           
  [TRUE, FALSE, TRUE],        
  [1, -1, 127],               
  [100, -100, 32767],         
  [100000, -100000, 2147483647], 
  [10000000000, -10000000000, 9223372036854775807], 
  [12345678901234567890, -12345678901234567890, 170141183460469231731687303715884105727], 
  [1.23, 4.56, 7.89],         
  [1.23, 4.56, 7.89],         
  [1.23456789, 2.34567890, 3.45678901], 
  ['abc', 'def', 'ghi'],      
  ['2024-01-01', '2024-01-02'], 
  ['2024-01-01 12:34:56', '2024-01-02 12:34:56'], 
  'ExampleVarcharData',            
  '{"key": "value"}',              
  x'4D7956617262696E61727944617461', 
  MAP{'key1':'value1', 'key2':'value2'}, 
  row('val1'),    
  ['str1', 'str2', 'str3']    
);
CREATE TABLE t0 (
    k1 INT,
    v1 INT,
    v2 INT)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1)
PROPERTIES(
    "replication_num" = "1"
);
insert into t0 SELECT generate_series % 50, if(generate_series % 2, generate_series, NULL), if(generate_series % 2, 0,1)  FROM TABLE(generate_series(1,  40960));
CREATE TABLE `t2` (
  `k1` varchar(255) NOT NULL COMMENT "",
  `k2` varchar(255) NOT NULL COMMENT "",
  `k3` int NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`k1`)
PROPERTIES ( "replication_num" = "1");
insert into t2 values(1,2,3);