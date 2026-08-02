CREATE TABLE variant_test_cases (
    case_id INT,
    case_label STRING,
    int_val INT,
    string_val STRING,
    float_val DOUBLE,
    decimal_val DECIMAL(18, 4),
    struct_val STRUCT<id INT, name STRING, counters MAP<STRING, INT>, status STRING>,
    json_val STRING,
    map_val MAP<STRING, STRING>,
    array_val ARRAY<DOUBLE>,
    variant_from_int VARIANT,
    variant_from_string VARIANT,
    variant_from_float VARIANT,
    variant_from_decimal VARIANT,
    variant_from_struct VARIANT,
    variant_from_json VARIANT,
    variant_from_map VARIANT,
    variant_from_array VARIANT,
    variant_mixed VARIANT,
    deep_struct_val STRUCT<
        profile STRUCT<
            handle STRING,
            contact STRUCT<
                emails ARRAY<STRING>,
                phones ARRAY<STRUCT<channel STRING, number STRING>>
            >,
            preferences MAP<STRING, STRUCT<enabled BOOLEAN, path STRING>>
        >,
        history ARRAY<STRUCT<ts STRING, status STRING, meta MAP<STRING, STRING>>>
    >,
    deep_map_val MAP<
        STRING,
        STRUCT<
            limits MAP<STRING, INT>,
            routes ARRAY<STRUCT<pattern STRING, methods ARRAY<STRING>>>,
            metadata STRUCT<label STRING, path STRING>
        >
    >,
    deep_array_val ARRAY<
        STRUCT<
            slot INT,
            nodes ARRAY<
                STRUCT<
                    id STRING,
                    status STRING,
                    metrics MAP<STRING, DOUBLE>,
                    transitions ARRAY<STRUCT<state STRING, at STRING>>
                >
            >
        >
    >,
    deep_json_val STRING,
    variant_deep_struct VARIANT,
    variant_deep_map VARIANT,
    variant_deep_array VARIANT,
    variant_deep_json VARIANT
) 
properties(
  'format-version' = '3'
);
drop table variant_test_cases force;