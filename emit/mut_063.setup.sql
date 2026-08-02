CREATE TABLE t1 (
    c1 int,
    c2 map<int, int>,
    c3 map<varchar(10), bigint>,
    c4 map<int, double>
)
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO t1 values
    (1, map{1:10, 2:20, 3:30}, map{"a":100, "b":200}, map{1:1.5, 2:2.5}),
    (2, map{1:5, 2:15, 4:40}, map{"a":50, "c":150}, map{1:0.5, 3:3.5}),
    (3, map{2:25, 3:35, 5:50}, map{"b":250, "c":350}, map{2:1.0, 3:2.0});
INSERT INTO t1 values (4, null, null, null);
INSERT INTO t1 values (5, map{}, map{}, map{});
CREATE TABLE t2 (
    id int,
    m_tinyint map<int, tinyint>,
    m_smallint map<int, smallint>,
    m_int map<int, int>,
    m_bigint map<int, bigint>,
    m_largeint map<int, largeint>,
    m_float map<int, float>,
    m_double map<int, double>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO t2 values
    (1, map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000}, 
        map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5}),
    (2, map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
        map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5});
CREATE TABLE t3 (
    id int,
    m_key_tinyint map<tinyint, int>,
    m_key_smallint map<smallint, int>,
    m_key_int map<int, int>,
    m_key_bigint map<bigint, int>,
    m_key_largeint map<largeint, int>,
    m_key_varchar map<varchar(20), int>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO t3 values
    (1, map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, 
        map{1:10000, 2:20000}, map{1:100000, 2:200000}, map{"a":10, "b":20}),
    (2, map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, 
        map{1:5000, 3:30000}, map{1:50000, 3:300000}, map{"a":5, "c":30});
CREATE TABLE t5 (
    id int,
    m_nullable map<int, int>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO t5 values
    (1, map{1:10, 2:20, NULL:100}),
    (2, map{1:5, NULL:50, 3:30});
INSERT INTO t5 values
    (3, map{1:15, 2:NULL, 4:40});
INSERT INTO t5 values
    (4, map{NULL:25, 5:NULL, 6:60});
CREATE TABLE t8 (
    category varchar(20),
    metrics map<varchar(20), int>
)
DUPLICATE KEY(category)
DISTRIBUTED BY HASH(category) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO t8 values
    ('A', map{"clicks":100, "views":200}),
    ('A', map{"clicks":50, "impressions":150}),
    ('B', map{"views":300, "impressions":250}),
    ('B', map{"clicks":75, "views":100});
CREATE TABLE t10 (
    id int,
    data map<int, bigint>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO t10 select 
    generate_series as id,
    map{1:generate_series, 2:generate_series*2, 3:generate_series*3} as data
from table(generate_series(1, 100));
CREATE TABLE t11_type_matrix (
    id int,
    m_ti_ti map<tinyint, tinyint>,
    m_ti_si map<tinyint, smallint>,
    m_ti_i map<tinyint, int>,
    m_ti_bi map<tinyint, bigint>,
    m_ti_li map<tinyint, largeint>,
    m_ti_f map<tinyint, float>,
    m_ti_d map<tinyint, double>,
    m_si_ti map<smallint, tinyint>,
    m_si_si map<smallint, smallint>,
    m_si_i map<smallint, int>,
    m_si_bi map<smallint, bigint>,
    m_si_li map<smallint, largeint>,
    m_si_f map<smallint, float>,
    m_si_d map<smallint, double>,
    m_i_ti map<int, tinyint>,
    m_i_si map<int, smallint>,
    m_i_i map<int, int>,
    m_i_bi map<int, bigint>,
    m_i_li map<int, largeint>,
    m_i_f map<int, float>,
    m_i_d map<int, double>,
    m_bi_ti map<bigint, tinyint>,
    m_bi_si map<bigint, smallint>,
    m_bi_i map<bigint, int>,
    m_bi_bi map<bigint, bigint>,
    m_bi_li map<bigint, largeint>,
    m_bi_f map<bigint, float>,
    m_bi_d map<bigint, double>,
    m_li_ti map<largeint, tinyint>,
    m_li_si map<largeint, smallint>,
    m_li_i map<largeint, int>,
    m_li_bi map<largeint, bigint>,
    m_li_li map<largeint, largeint>,
    m_li_f map<largeint, float>,
    m_li_d map<largeint, double>,
    m_f_ti map<float, tinyint>,
    m_f_si map<float, smallint>,
    m_f_i map<float, int>,
    m_f_bi map<float, bigint>,
    m_f_li map<float, largeint>,
    m_f_f map<float, float>,
    m_f_d map<float, double>,
    m_d_ti map<double, tinyint>,
    m_d_si map<double, smallint>,
    m_d_i map<double, int>,
    m_d_bi map<double, bigint>,
    m_d_li map<double, largeint>,
    m_d_f map<double, float>,
    m_d_d map<double, double>,
    m_date_i map<date, int>,
    m_dt_i map<datetime, int>,
    m_str_i map<varchar(10), int>,
    m_i_date map<int, date>,
    m_i_dt map<int, datetime>,
    m_i_str map<int, varchar(10)>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO t11_type_matrix VALUES (
    1,
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    map{1.0:10, 2.0:20}, map{1.0:100, 2.0:200}, map{1.0:1000, 2.0:2000}, map{1.0:10000, 2.0:20000},
    map{1.0:100000, 2.0:200000}, map{1.0:1.5, 2.0:2.5}, map{1.0:10.5, 2.0:20.5},
    map{1.0:10, 2.0:20}, map{1.0:100, 2.0:200}, map{1.0:1000, 2.0:2000}, map{1.0:10000, 2.0:20000},
    map{1.0:100000, 2.0:200000}, map{1.0:1.5, 2.0:2.5}, map{1.0:10.5, 2.0:20.5},
    map{'2024-01-01':100, '2024-01-02':200}, map{'2024-01-01 10:00:00':100, '2024-01-01 11:00:00':200},
    map{"a":100, "b":200},
    map{1:'2024-01-01', 2:'2024-01-02'}, map{1:'2024-01-01 10:00:00', 2:'2024-01-01 11:00:00'},
    map{1:"a", 2:"b"}
);
INSERT INTO t11_type_matrix VALUES (
    2,
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    map{1.0:5, 3.0:30}, map{1.0:50, 3.0:300}, map{1.0:500, 3.0:3000}, map{1.0:5000, 3.0:30000},
    map{1.0:50000, 3.0:300000}, map{1.0:0.5, 3.0:3.5}, map{1.0:5.5, 3.0:30.5},
    map{1.0:5, 3.0:30}, map{1.0:50, 3.0:300}, map{1.0:500, 3.0:3000}, map{1.0:5000, 3.0:30000},
    map{1.0:50000, 3.0:300000}, map{1.0:0.5, 3.0:3.5}, map{1.0:5.5, 3.0:30.5},
    map{'2024-01-01':50, '2024-01-03':300}, map{'2024-01-01 10:00:00':50, '2024-01-01 12:00:00':300},
    map{"a":50, "c":300},
    map{1:'2024-01-03', 3:'2024-01-04'}, map{1:'2024-01-01 12:00:00', 3:'2024-01-01 13:00:00'},
    map{1:"c", 3:"d"}
);