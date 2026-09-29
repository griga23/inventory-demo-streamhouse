-- CUT 2: fold both event streams into current_inventory.
--
-- `item_purchased` is provisioned by Terraform directly (confluent_kafka_topic +
-- confluent_schema, see terraform/topics.tf), not by a Flink CREATE TABLE statement:
-- Confluent Cloud Flink auto-discovers any schema-registered topic as a table, so as
-- long as Terraform creates that topic+schema before this statement runs (see the
-- depends_on in terraform/flink.tf), Flink resolves it here without a DDL statement
-- of its own.
-- No CAST needed: confirmed empirically against Confluent Cloud's Flink SQL engine
-- (via a live "Column types of query result and sink ... do not match" error while
-- testing an INT sink) that SUM() over an INT column widens to BIGINT here, matching
-- current_inventory.quantity's BIGINT type exactly with no cast required. (An earlier
-- version of this comment claimed the opposite, based on generic Apache Flink docs
-- rather than Confluent Cloud's actual behavior - trust the engine's error message
-- over community docs if the two ever disagree again.)
INSERT INTO current_inventory
SELECT sku, SUM(delta) AS quantity
FROM (
  SELECT sku, quantity AS delta FROM item_added_to_stock
  UNION ALL
  SELECT sku, -quantity AS delta FROM item_purchased
)
GROUP BY sku;
