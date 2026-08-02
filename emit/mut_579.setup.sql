CREATE TABLE test_shredding (
    data VARIANT,
    id BIGINT,
    age INT,
    city VARCHAR
) properties(
  'format-version' = '3',
  'iceberg.enableVariantShredding' = 'true'
);
CREATE TABLE test_noshredding (
    data VARIANT,
    id BIGINT,
    age INT,
    city VARCHAR
) properties(
  'format-version' = '3'
);
drop table test_shredding force;
drop table test_noshredding force;