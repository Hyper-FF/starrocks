create table t(id int, j json) duplicate key(id)
  distributed by hash(id) buckets 1
  properties("replication_num"="1","flat_json.enable"="true");
insert into t select generate_series,
  parse_json(concat('{"o":{"inner":', generate_series, '},"leaf":', generate_series, '}'))
  from table(generate_series(1,300));