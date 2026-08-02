DROP TABLE if exists t0;
CREATE TABLE if not exists t0
(
id INT NOT NULL,
url VARCHAR(255) NOT NULL,
param_key VARCHAR(255) NOT NULL,
expect VARCHAR(255) NOT NULL,
only_null BOOLEAN NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES(
"replication_num" = "1",
"storage_format" = "default"
);
INSERT INTO t0
  (id, url, param_key, expect, only_null)
VALUES
  ('0', 'https://starrocks.com/doc?k1=10', 'k1', '10', 'false'),
  ('1', 'https://starrocks.com/doc?kk12=10', 'k1', '', 'true'),
  ('2', 'https://starrocks.com/doc?kk12=10', 'kk1', '', 'true'),
  ('3', 'https://starrocks.com/doc?kk12=10', 'k12', '', 'true'),
  ('4', 'https://starrocks.com/doc?k0=100&k1=10', 'k1', '10', 'false'),
  ('5', 'https://starrocks.com/doc?k0=abc&kk12=10', 'k1', '', 'true'),
  ('6', 'https://starrocks.com/doc?k0=100&kk12=10', 'kk1', '', 'true'),
  ('7', 'https://starrocks.com/doc?k0=1000kk12=10', 'k12', '', 'true'),
  ('8', 'https://starrocks.com/doc?k0=abc&k1=10#section1', 'k1', '10', 'false'),
  ('9', 'https://starrocks.com/doc?k0=100&k1=10=20', 'k1', '10=20', 'false'),
  ('10', 'https://starrocks.com/doc?k0=1000&k1', 'k1', '', 'false'),
  ('11', 'https://starrocks.com/doc?k0=1000&k1&k1=2', 'k1', '', 'false'),
  ('12', 'https://starrocks.com/doc?k0=1000&k1&=2', 'k1', '', 'false'),
  ('13', 'https://starrocks.com/doc?k0=1000&k1=100&k1=200', 'k1', '100', 'false'),
  ('14', 'https://starrocks.com/doc?k0=1000&k1:200=100&k1=200', 'k1:200', '100', 'false'),
  ('15', 'https://starrocks.com/doc?k0=1000&k1: 200=100&k1=200', 'k1: 200', '', 'true'),
  ('16', '', 'k1', '', 'true'),
  ('17', 'https://starrocks.com/doc?k0=10&k1=%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D%20%22%25%2D%2E%3C%3E%5C%5E%5F%60%7B%7C%7D%7E&k2', 'k1', '!#$&\'()*+,/:;=?@[] "%-.<>\\^_`{|}~', 'false');
create table t_url_extract_parameter(c0 varchar(200), c1 varchar(200))
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
insert into t_url_extract_parameter values ('http://host:80/123?k=1&v=1', 'k'), ('/123?k=1&v=1', 'v'), ('', 'k'), ('','');