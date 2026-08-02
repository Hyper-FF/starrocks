create table src (
    id int,
    x varchar(20)
) properties ("replication_num" = "1");
create table dst (
    id int,
    xi int
) properties ("replication_num" = "1");
create table dim (
    k int,
    y varchar(20)
) properties ("replication_num" = "1");
insert into src values
    (1, '1'),
    (2, '2'),
    (3, '-1'),
    (4, 'bad'),
    (5, '10');
insert into dim values
    (1, '1'),
    (2, '10'),
    (3, '100');
insert into dst select id, x::int from src where x::int is not null;
create table ctas_dst properties ("replication_num" = "1") as
    select id, x::int as xi from src where x::int > 0;