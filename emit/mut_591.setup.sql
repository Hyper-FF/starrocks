CREATE TABLE t_ngram (
    haystack VARCHAR(100) NOT NULL,
    needle   VARCHAR(100) NOT NULL
) PROPERTIES ("replication_num" = "1");
insert into t_ngram values
    ('abcde',   'abcde'),
    ('abcde',   'xyzwv'),
    ('abcdfg',  'abcde'),
    ('chinese', 'chinese');
CREATE TABLE t_ngram_null (
    haystack VARCHAR(100),
    needle   VARCHAR(100)
) PROPERTIES ("replication_num" = "1");
insert into t_ngram_null values
    ('abcde', 'abcde'),
    ('abcde', null),
    ('xyzwv', 'abcde');
CREATE TABLE t_ngram_ci (
    haystack VARCHAR(100) NOT NULL,
    needle   VARCHAR(100) NOT NULL
) PROPERTIES ("replication_num" = "1");
insert into t_ngram_ci values
    ('Hello',   'hello'),
    ('CHINESE', 'chinese'),
    ('abcde',   'ABCDE');
CREATE TABLE t_ngram_idx (
    haystack VARCHAR(100) NOT NULL,
    needle   VARCHAR(100) NOT NULL,
    INDEX idx_haystack(haystack) USING NGRAMBF ("gram_num" = "4", "bloom_filter_fpp" = "0.05")
) PROPERTIES ("replication_num" = "1");
insert into t_ngram_idx values
    ('chinese', 'chinese'),
    ('chineaaaa', 'chinese'),
    ('tonightisgreadnight', 'chinese');