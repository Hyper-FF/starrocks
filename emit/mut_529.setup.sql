create external table ice_tbl_0(
    c_tinyint tinyint,
    c_smallint smallint,
    c_int int,
    c_bigint bigint,
    c_bool boolean,
    c_float float,
    c_double double,
    c_decimal decimal(38, 18),
    c_datetime datetime, 
    c_string string,
    c_date date,
    c_time time
);
INSERT INTO ice_tbl_0 (
    c_tinyint,
    c_smallint,
    c_int,
    c_bigint,
    c_bool,
    c_float,
    c_double,
    c_decimal,
    c_datetime,
    c_string,
    c_date,
    c_time
) VALUES
(1, 10, 100, 1000, true, 1.1, 2.2, 10.000000000000000001, '2025-06-29 08:00:00', 'alpha', '2025-06-29', '08:00:00'),
(2, 20, 200, 2000, false, 2.2, 3.3, 20.000000000000000002, '2025-06-29 08:10:00', 'beta', '2025-06-30', '08:10:00'),
(3, 30, 300, 3000, true, 3.3, 4.4, 30.000000000000000003, '2025-06-29 08:20:00', 'gamma', '2025-07-01', '08:20:00'),
(4, 40, 400, 4000, false, 4.4, 5.5, 40.000000000000000004, '2025-06-29 08:30:00', 'delta', '2025-07-02', '08:30:00'),
(5, 50, 500, 5000, true, 5.5, 6.6, 50.000000000000000005, '2025-06-29 08:40:00', 'epsilon', '2025-07-03', '08:40:00'),
(6, 60, 600, 6000, false, 6.6, 7.7, 60.000000000000000006, '2025-06-29 08:50:00', 'zeta', '2025-07-04', '08:50:00'),
(7, 70, 700, 7000, true, 7.7, 8.8, 70.000000000000000007, '2025-06-29 09:00:00', 'eta', '2025-07-05', '09:00:00'),
(8, 80, 800, 8000, false, 8.8, 9.9, 80.000000000000000008, '2025-06-29 09:10:00', 'theta', '2025-07-06', '09:10:00'),
(9, 90, 900, 9000, true, 9.9, 10.1, 90.000000000000000009, '2025-06-29 09:20:00', 'iota', '2025-07-07', '09:20:00'),
(10, 100, 1000, 10000, false, 10.1, 11.2, 100.000000000000000010, '2025-06-29 09:30:00', 'kappa', '2025-07-08', '09:30:00');
INSERT INTO ice_tbl_0 (
    c_tinyint,
    c_smallint,
    c_int,
    c_bigint,
    c_bool,
    c_float,
    c_double,
    c_decimal,
    c_datetime,
    c_string,
    c_date,
    c_time
) VALUES
(null, 10, 100, null, true, 1.1, 2.2, 10.000000000000000001, '2025-06-29 08:00:00', 'alpha', '2025-06-29', '08:00:00');
INSERT overwrite ice_tbl_0 (
    c_tinyint,
    c_smallint,
    c_int,
    c_bigint,
    c_bool,
    c_float,
    c_double,
    c_decimal,
    c_datetime,
    c_string,
    c_date,
    c_time
) VALUES
(null, 10, 100, null, true, 1.1, 2.2, 10.000000000000000001, '2025-06-29 08:00:00', 'alpha', '2025-06-29', '08:00:00');
drop table ice_tbl_0 force;
create external table ice_tbl_dt(
    c_int int,
    c_datetime datetime
) partition by (hour(c_datetime));
INSERT INTO ice_tbl_dt (c_int, c_datetime) VALUES
(1, '2025-06-29 08:00:00'),
(2, '2025-06-29 09:10:00'),
(3, '2025-06-29 10:20:00');
drop table ice_tbl_dt force;
create external table ice_tbl_1(
    c_int int,
    c_bigint bigint    
);
INSERT INTO ice_tbl_1 (
    c_int,
    c_bigint    
) VALUES
(null, null);
drop table ice_tbl_1 force;
create external table ice_tbl_2(
    c_int int,
    dt datetime
) partition by (dt);
INSERT INTO ice_tbl_2 (
       c_int, dt
) VALUES
(10, '2021-01-01 13:00:00'),
(20, '2022-01-01 14:00:00');
drop table ice_tbl_2 force;
create external table ice_tbl_3(
    id int,
    name string
) partition by (bucket(id, 4));
INSERT INTO ice_tbl_3 (id, name) VALUES
(1, 'alice'),
(2, 'bob'),
(3, 'carol'),
(4, 'dave'),
(5, 'eve');
drop table ice_tbl_3 force;