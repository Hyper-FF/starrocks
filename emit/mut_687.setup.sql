CREATE TABLE activity (
  id bigint,
  tenant_id bigint,
  fields json
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES("replication_num" = "1");
INSERT INTO activity VALUES
 (1, 1, parse_json('{"Campaign":"CapA","campaign":"lowa","title":"t1"}')),
 (2, 1, parse_json('{"Campaign":"CapB","campaign":"lowb","title":"t2"}')),
 (3, 1, parse_json('{"Campaign":"CapC","title":"t3"}')),
 (4, 1, parse_json('{"campaign":"lowd"}')),
 (5, 1, parse_json('{}'));
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;
INSERT INTO activity SELECT id, tenant_id, fields FROM activity;