CREATE TABLE IF NOT EXISTS `customer` (
  `c_custkey` int(11) NULL,
  `c_id` int(11) NULL,
  `c_name` varchar(26) NOT NULL ,
  `c_address` varchar(41) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 2
PROPERTIES (
"replication_num" = "1"
);
insert into customer values (1, 1, "name_1", "address_1"), (2, 2, "name_2", "address_2"), (3, 3, "name_3", "address_3"), (null, 4, "name_null", "address_null");
CREATE TABLE IF NOT EXISTS `lineorder` (
  `lo_orderkey` int(11) NULL,
  `lo_linenumber` int(11) NOT NULL COMMENT "",
  `lo_custkey` int(11) NULL COMMENT "",
  `lo_quantity` int(11) NOT NULL,
  `lo_revenue` int(11) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 2
PROPERTIES (
"replication_num" = "1"
);
insert into lineorder values(10001, 1, 1, 10, 1000), (10002, 1, 2, 20, 2000), (10003, 1, 3, 30, 3000), (10004, 1, null, 40, 4000);
create materialized view mv_left_outer
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey, c_name
from lineorder left outer join customer
on lo_custkey = c_custkey;
create materialized view mv_left_outer_2
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, c_custkey, c_name
from lineorder left outer join customer
on lo_custkey = c_custkey;
insert into lineorder values(10001, 2, 1, 10, 1400), (10002, 2, 2, 20, 2500), (10003, 1, 3, 30, 3600);
create materialized view mv_left_outer_agg
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, c_name, sum(lo_revenue) as total_revenue
from lineorder left outer join customer
on lo_custkey = c_custkey
group by lo_orderkey, c_name;
create materialized view mv_right_outer
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_custkey, c_custkey, c_name
from lineorder right outer join customer
on lo_custkey = c_custkey;
create materialized view mv_right_outer_2
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_custkey, c_custkey, c_name, c_id
from lineorder right outer join customer
on lo_custkey = c_custkey and lo_linenumber = c_id;
create materialized view mv_right_outer_agg
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_custkey, sum(lo_revenue) as total_revenue
from lineorder right outer join customer
on lo_custkey = c_custkey
group by lo_orderkey, lo_custkey;
alter table customer set ("unique_constraints"="c_custkey");
create materialized view mv_inner_join
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_custkey, c_custkey, c_name
from lineorder inner join customer
on lo_custkey = c_custkey;
CREATE TABLE IF NOT EXISTS `customer_primary` (
  `c_custkey` int(11) NOT NULL,
  `c_id` int(11) NULL,
  `c_name` varchar(26) NOT NULL ,
  `c_address` varchar(41) NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 2
PROPERTIES (
"replication_num" = "1"
);
insert into customer_primary values (1, 1, "name_1", "address_1"), (2, 2, "name_2", "address_2"), (3, 3, "name_3", "address_3");
create materialized view mv_inner_join_2
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_custkey, c_custkey, c_name
from lineorder inner join customer_primary
on lo_custkey = c_custkey;
CREATE TABLE IF NOT EXISTS `customer_unique` (
  `c_custkey` int(11) NOT NULL,
  `c_id` int(11) NULL,
  `c_name` varchar(26) NOT NULL ,
  `c_address` varchar(41) NOT NULL
) ENGINE=OLAP
UNIQUE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 2
PROPERTIES (
"replication_num" = "1"
);
insert into customer_unique values (1, 1, "name_1", "address_1"), (2, 2, "name_2", "address_2"), (3, 3, "name_3", "address_3");
create materialized view mv_inner_join_3
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_custkey, c_custkey, c_name
from lineorder inner join customer_unique
on lo_custkey = c_custkey;
create materialized view mv_inner_join_4
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_custkey, c_custkey, c_name
from customer inner join lineorder
on lo_custkey = c_custkey;
create materialized view mv_inner_join_5
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_custkey, c_custkey, c_name
from customer_primary inner join lineorder
on lo_custkey = c_custkey;
create materialized view mv_inner_join_6
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_custkey, c_custkey, c_name
from customer_unique inner join lineorder
on lo_custkey = c_custkey;
create materialized view mv_full_outer_join
distributed by hash(`lo_orderkey`) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_custkey, c_custkey, c_name
from lineorder full outer join customer
on lo_custkey = c_custkey;
CREATE TABLE t1 (
                k1 int,
                k2 int not null
            )
            DUPLICATE KEY(k1);
CREATE TABLE t2 (
                a int,
                b int not null
            )
            DUPLICATE KEY(a);
INSERT INTO t1 VALUES (1,1),(3,2),(null,1);
INSERT INTO t2 VALUES (1,1),(2,2),(null,1);
CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL AS select * from t1 full outer join t2 on k1=a;
CREATE TABLE t1 (
                                    k1 int,
                                    k2 int
                                )
                                DUPLICATE KEY(k1);
CREATE TABLE t2 (
                  a int,
                  b int
              )
              DUPLICATE KEY(a);
INSERT INTO t1 VALUES (1,1),(3,2),(1,null),(null,null);
INSERT INTO t2 VALUES (1,1),(2,2),(null,1),(null,null);
CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL AS select * from t1 right outer join t2 on k1=a;