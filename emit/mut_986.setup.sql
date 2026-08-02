create table t0 (
c0 string
) properties("replication_num"="1");
insert into t0 with cte as (
select i from table(generate_series(1,10000)) t(i)
)
select concat(i, ".", "foo", case i%3 when 0 then ".bar" when 1 then ".buzz" else "" end, ".xxxx") c0 
from cte;
create table result (
fp bigint 
) properties("replication_num"="1");
insert into result 
with cte as (
select distinct regexp_replace(c0, "^\\d+(\\.foo\.b\\w+).*$", "\\1") as c0 from t0 
)
select sum(murmur_hash3_32(cte.c0)) fp from cte;
insert into result 
with cte as (
select distinct regexp_replace(c0, "^\\d+(\\.foo\.b\\w+).*$", "\\1") as c0 from t0 
)
select sum(murmur_hash3_32(cte.c0)) fp from cte;
insert into result 
with cte as (
select distinct regexp_replace(c0, "\\d+(\\.foo\.b\\w+).*", "\\1") as c0 from t0 
)
select sum(murmur_hash3_32(cte.c0)) fp from cte;
insert into result 
with cte as (
select distinct regexp_replace(c0, "\\d+(\\.foo\.b\\w+).*", "\\1") as c0 from t0 
)
select sum(murmur_hash3_32(cte.c0)) fp from cte;
insert into result 
with cte as (
select distinct regexp_replace(c0, ".*foo.bar*", "deadbeef") as c0 from t0 
)
select sum(murmur_hash3_32(cte.c0)) fp from cte;
insert into result 
with cte as (
select distinct regexp_replace(c0, ".*foo.bar*", "deadbeef") as c0 from t0 
)
select sum(murmur_hash3_32(cte.c0)) fp from cte;
CREATE TABLE IF NOT EXISTS tbl_transaction_001 (
    id              BIGINT          COMMENT 'Primary Key',
    field_date_1    DATE            COMMENT 'Date Field 1',
    field_int_1     INT             COMMENT 'Integer Field 1',
    field_varchar_1 VARCHAR(100)    COMMENT 'Varchar Field 1',
    field_varchar_2 VARCHAR(50)     COMMENT 'Varchar Field 2 (Category)',
    field_varchar_3 VARCHAR(100)    COMMENT 'Varchar Field 3 (Foreign Key)',
    field_text_1    VARCHAR(2000)   COMMENT 'Text Field 1 (JSON Data)'
)
COMMENT 'Test Table: Generic Transaction Table'
PROPERTIES('replication_num'='1');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (1, DATE '2025-12-24', 1, 'DATA_001', 'VALUE_ALPHA', 'FK_001',
     '{
        "key_json_1": "RESULT_A_001",
        "key_json_2": "[\"pattern_match_1\"]",
        "key_json_3": "NULL"
      }');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (2, DATE '2025-12-24', 1, 'DATA_002', 'VALUE_GAMMA', 'FK_002',
     '{
        "key_json_1": "NULL",
        "key_json_2": "[\"pattern_match_2\"]",
        "key_json_3": "RESULT_B_001"
      }');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (3, DATE '2025-12-25', 1, 'DATA_003', 'VALUE_BETA', 'FK_003',
     '{
        "key_json_1": "RESULT_A_002",
        "key_json_2": "[\"pattern_match_1\"]",
        "key_json_3": "NULL"
      }');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (4, DATE '2025-12-25', 1, 'DATA_004', 'VALUE_BETA', 'FK_004',
     '{
        "key_json_1": "NULL",
        "key_json_2": "[\"pattern_match_2\"]",
        "key_json_3": "RESULT_B_002"
      }');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (5, DATE '2025-12-24', 1, 'DATA_005', 'VALUE_DELTA', '',
     '{
        "key_json_1": "NULL",
        "key_json_2": "[\"other_pattern\"]",
        "key_json_3": "NULL"
      }');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (6, DATE '2025-12-24', 1, 'DATA_006', 'VALUE_ALPHA', '',
     '{
        "key_json_1": "RESULT_A_003",
        "key_json_2": "[\"pattern_match_1\"]",
        "key_json_3": "NULL"
      }');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (7, DATE '2025-12-24', 1, 'DATA_007', 'VALUE_ALPHA', '0',
     '{
        "key_json_1": "RESULT_A_004",
        "key_json_2": "[\"pattern_match_1\"]",
        "key_json_3": "NULL"
      }');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (8, DATE '2025-12-24', 2, 'DATA_008', 'VALUE_ALPHA', 'FK_008',
     '{
        "key_json_1": "RESULT_A_005",
        "key_json_2": "[\"pattern_match_1\"]",
        "key_json_3": "NULL"
      }');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (9, DATE '2025-12-20', 1, 'DATA_009', 'VALUE_ALPHA', 'FK_009',
     '{
        "key_json_1": "RESULT_A_006",
        "key_json_2": "[\"pattern_match_1\"]",
        "key_json_3": "NULL"
      }');
INSERT INTO tbl_transaction_001
    (id, field_date_1, field_int_1, field_varchar_1, field_varchar_2, field_varchar_3, field_text_1)
VALUES
    (10, DATE '2025-12-25', 1, 'DATA_010', 'VALUE_BETA', 'FK_010',
     '{
        "key_json_1": "RESULT_A_007",
        "key_json_2": "[\"pattern_match_1\",\"pattern_match_2\"]",
        "key_json_3": "RESULT_B_003"
      }');