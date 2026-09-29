# CUT 3: enable the Real-Time Context Engine (Lightning Table) on current_inventory.
# Read access for the storefront app is granted separately in service_accounts.tf
# (DeveloperRead on this topic + on the Schema Registry cluster).

resource "confluent_rtce_topic" "current_inventory" {
  cloud       = var.cloud_provider
  region      = var.cloud_region
  topic_name  = var.topic_current_inventory
  description = "Current inventory per SKU (Job 2 shoe store demo)"

  environment {
    id = var.environment_id
  }
  kafka_cluster {
    id = data.confluent_kafka_cluster.this.id
  }

  depends_on = [
    confluent_flink_statement.current_inventory_insert,
    confluent_flink_statement.current_inventory_isolation_level,
  ]
}
