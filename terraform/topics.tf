# CUT 1 (purchase side): item_purchased is Terraform-owned (topic + schema), not
# created by a Flink CREATE TABLE statement. The storefront app only ever produces
# to it (storefront-app/kafka_producer.py) - it looks up this schema's id, it doesn't
# register it. Confluent Cloud Flink auto-discovers schema-registered topics as
# tables, so flink-sql/05_current_inventory_insert.sql can reference `item_purchased`
# directly once this resource exists (see the depends_on in terraform/flink.tf).

resource "confluent_kafka_topic" "item_purchased" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.this.id
  }
  topic_name    = var.topic_item_purchased
  rest_endpoint = data.confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.storefront_kafka.id
    secret = confluent_api_key.storefront_kafka.secret
  }

  # RBAC role bindings on Confluent Cloud take time to propagate after creation, so
  # this must wait for the role binding to actually finish, not just be created in
  # Terraform's dependency graph order.
  depends_on = [confluent_role_binding.storefront_cluster_admin]
}

resource "confluent_schema" "item_purchased_value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.this.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.this.rest_endpoint
  subject_name  = "${var.topic_item_purchased}-value"
  format        = "JSON"
  schema        = file("${path.module}/schemas/item_purchased.json")
  credentials {
    key    = confluent_api_key.storefront_schema_registry.id
    secret = confluent_api_key.storefront_schema_registry.secret
  }

  depends_on = [
    confluent_kafka_topic.item_purchased,
    confluent_role_binding.storefront_schema_registry_owner,
  ]
}
