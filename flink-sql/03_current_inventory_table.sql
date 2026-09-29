-- CUT 2: current inventory per SKU, as a primary-keyed (upsert) table/topic.
--
-- PRIMARY KEY + changelog.mode=upsert makes Confluent Cloud back this table with a
-- compacted Kafka topic, keyed by `sku` -- exactly the shape the Real-Time Context
-- Engine / Lightning Query API (CUT 3) expects for upsert-mode tables.
CREATE TABLE current_inventory (
  sku      STRING,
  quantity BIGINT,
  PRIMARY KEY (sku) NOT ENFORCED
) WITH (
  'changelog.mode' = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'value.format' = 'json-registry'
);
