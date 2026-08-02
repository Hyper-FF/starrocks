CREATE TABLE t_recharge_detail (
    id bigint  ,
    user_id  bigint  ,
    recharge_money decimal(32,2) ,
    province varchar(20) not null,
    dt varchar(20) not null
) ENGINE=OLAP
DUPLICATE KEY(id)
PARTITION BY (dt,province)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);
insert into t_recharge_detail values(1,1,1,'hangzhou', '2022-04-01');
alter table t_recharge_detail drop partition p20220401_hangzhou force;
insert into t_recharge_detail values(1,1,1,'hangzhou', '2022-04-01'),(1,1,1,'beijing', '2022-03-01');
CREATE TABLE t1 (
    id bigint  not null,
    user_id  bigint  not null,
    recharge_money decimal(32,2) not null,
    province varchar(20) not null,
    dt varchar(20) not null
) ENGINE=OLAP
DUPLICATE KEY(id)
PARTITION BY (id)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);
insert into t1 values(1,1,1,'hangzhou', '2022-04-01');
CREATE TABLE t2 (
    dt datetime  not null,
    user_id  bigint  not null,
    recharge_money decimal(32,2) not null,
    province varchar(20) not null,
    id varchar(20) not null
) ENGINE=OLAP
DUPLICATE KEY(dt)
PARTITION BY (dt)
DISTRIBUTED BY HASH(`dt`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);
insert into t2 values('2022-04-01',1,1,'hangzhou', 1);
CREATE TABLE t3 (
    dt date  not null,
    user_id  bigint  not null,
    recharge_money decimal(32,2) not null,
    province varchar(20) not null,
    id varchar(20) not null
) ENGINE=OLAP
DUPLICATE KEY(dt)
PARTITION BY (dt)
DISTRIBUTED BY HASH(`dt`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);
insert into t3 values('2022-04-01',1,1,'hangzhou', 1);
CREATE TABLE list_auto_single (
    id bigint  ,
    user_id  bigint  ,
    recharge_money decimal(32,2) ,
    province varchar(20) not null,
    dt varchar(20) not null
) ENGINE=OLAP
DUPLICATE KEY(id)
PARTITION BY (dt)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);
ALTER TABLE list_auto_single ADD PARTITION psingle VALUES IN ("2023-04-01");
ALTER TABLE list_auto_single ADD PARTITION pmul VALUES IN ("2022-04-01", "2022-04-02");
CREATE TABLE list_normal_single (
    id bigint  ,
    user_id  bigint  ,
    recharge_money decimal(32,2) ,
    province varchar(20) not null,
    dt varchar(20) not null
) ENGINE=OLAP
DUPLICATE KEY(id)
PARTITION BY LIST (dt)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);
ALTER TABLE list_normal_single ADD PARTITION psingle VALUES IN ("2023-04-01");
ALTER TABLE list_normal_single ADD PARTITION pmul VALUES IN ("2022-04-01", "2022-04-02");
create table t(k string, v int)partition by (k) distributed by hash(v) buckets 1000;
insert into t values('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1),('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 2);
insert into t1 select * from t;