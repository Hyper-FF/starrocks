CREATE TABLE variant_json_roundtrip (
    id INT,
    json_str STRING,
    variant_from_json VARIANT
)
properties(
  'format-version' = '3'
);
insert into variant_json_roundtrip
select
    1 as id,
    '{"id":1,"name":"alpha","scores":[1.5,2.5,3.5],"flags":{"active":true,"skip":null},"paths":{"first":"$.scores[0]"},"keys":{"key.with.dot":{"leaf":7},"key-with-dash":false}}' as json_str,
    cast(parse_json('{"id":1,"name":"alpha","scores":[1.5,2.5,3.5],"flags":{"active":true,"skip":null},"paths":{"first":"$.scores[0]"},"keys":{"key.with.dot":{"leaf":7},"key-with-dash":false}}') as variant) as variant_from_json
union all
select
    2 as id,
    '{"id":-7,"name":"beta","scores":[-0.5,null,0.5],"flags":{"active":false,"skip":null},"paths":{"first":"$.scores[2]"},"keys":{"key.with.dot":{"leaf":-3},"key-with-dash":true}}' as json_str,
    cast(parse_json('{"id":-7,"name":"beta","scores":[-0.5,null,0.5],"flags":{"active":false,"skip":null},"paths":{"first":"$.scores[2]"},"keys":{"key.with.dot":{"leaf":-3},"key-with-dash":true}}') as variant) as variant_from_json
union all
select
    3 as id,
    '{"id":2048,"name":"","scores":[0.0,1.0,2.0],"flags":{"active":true,"skip":false},"paths":{"first":"$.scores[1]"},"keys":{"key.with.dot":{"leaf":123},"key-with-dash":false},"deep":{"matrix":[[{"val":11},{"val":22}],[{"val":33},{"val":44}]],"layers":[{"name":"edge","segments":[{"path":"/edge/**","status":"ok"}]},{"name":"core","segments":[{"path":"/core/**","status":"warn","steps":[{"id":"prep","ok":true},{"id":"dispatch","ok":false}]}]}]}}' as json_str,
    cast(parse_json('{"id":2048,"name":"","scores":[0.0,1.0,2.0],"flags":{"active":true,"skip":false},"paths":{"first":"$.scores[1]"},"keys":{"key.with.dot":{"leaf":123},"key-with-dash":false},"deep":{"matrix":[[{"val":11},{"val":22}],[{"val":33},{"val":44}]],"layers":[{"name":"edge","segments":[{"path":"/edge/**","status":"ok"}]},{"name":"core","segments":[{"path":"/core/**","status":"warn","steps":[{"id":"prep","ok":true},{"id":"dispatch","ok":false}]}]}]}}') as variant) as variant_from_json;
CREATE TABLE variant_json_write (
    id INT,
    json_str STRING,
    variant_from_json VARIANT
)
properties(
  'format-version' = '3'
);
insert into variant_json_write
select
    id,
    json_str,
    variant_from_json
from variant_json_roundtrip;
drop table variant_json_roundtrip force;
drop table variant_json_write force;