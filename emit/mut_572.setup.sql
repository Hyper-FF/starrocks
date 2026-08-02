CREATE TABLE variant_string_unicode_test (
    test_id INT,
    test_label STRING,
    expected_value STRING,
    variant_col VARIANT
)
properties(
  'format-version' = '3'
);
CREATE TABLE variant_string_size_test (
    test_id INT,
    test_label STRING,
    size_bytes INT,
    variant_col VARIANT
)
properties(
  'format-version' = '3'
);
drop table variant_string_unicode_test force;
drop table variant_string_size_test force;