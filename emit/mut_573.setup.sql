CREATE TABLE variant_analytics_users (
    user_id INT,
    variant_profile VARIANT
)
properties(
  'format-version' = '3'
);
CREATE TABLE variant_analytics_orders (
    order_id INT,
    variant_order VARIANT
)
properties(
  'format-version' = '3'
);
CREATE TABLE variant_analytics_logs (
    log_id INT,
    variant_log VARIANT
)
properties(
  'format-version' = '3'
);
CREATE TABLE variant_analytics_events (
    event_id INT,
    variant_event VARIANT
)
properties(
  'format-version' = '3'
);
drop table variant_analytics_users force;
drop table variant_analytics_orders force;
drop table variant_analytics_logs force;
drop table variant_analytics_events force;