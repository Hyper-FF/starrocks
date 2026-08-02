create table dt_test(
    c1 datetime,
    c2 varchar(100),
    c3 varchar(100)
) duplicate key(c1)
PROPERTIES("replication_num" = "1");
insert into dt_test values ('2020-01-01 00:00:00', 'Asia/Shanghai', 'America/Los_Angeles'),
('2020-01-01 00:00:00.01', 'Asia/Shanghai', 'America/Los_Angeles'),
('2020-01-01 00:00:00.01', '+08:00', 'America/Los_Angeles'),
('2020-01-01 00:00:00.01', '+08:00', '+09:00'),
('2020-01-01 00:00:00.01', 'Asia/Shanghai', 'America/Lima');