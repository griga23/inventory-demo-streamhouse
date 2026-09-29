-- CUT 1 (stock side): the real, schema-registered Kafka topic that warehouse
-- restocks land in. Terraform submits this as its own confluent_flink_statement
-- (see terraform/flink.tf) before the Faker source and the INSERT that feeds it.
CREATE TABLE item_added_to_stock (
  sku          STRING,
  quantity     INT,
  warehouse_id STRING
) WITH (
  'value.format' = 'json-registry'
);
