CREATE TABLE test_shredding_variant (
    data VARIANT,
    id BIGINT,
    age INT,
    city VARCHAR
) properties(
  'format-version' = '3',
  'iceberg.enableVariantShredding' = 'true'
);
CREATE TABLE test_noshredding_variant (
    data VARIANT,
    id BIGINT,
    age INT,
    city VARCHAR
) properties(
  'format-version' = '3'
);
drop table test_shredding_variant force;
drop table test_noshredding_variant force;