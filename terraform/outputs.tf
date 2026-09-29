# `terraform apply` writes all of these straight into config/local.env (see
# local_env below) so there's normally nothing to copy by hand. They're also
# exposed as outputs for scripting/debugging (`terraform output -raw kafka_api_key`).

output "storefront_principal_id" {
  value = confluent_service_account.storefront.id
}

output "kafka_api_key" {
  value = confluent_api_key.storefront_kafka.id
}

output "kafka_api_secret" {
  value     = confluent_api_key.storefront_kafka.secret
  sensitive = true
}

output "schema_registry_api_key" {
  value = confluent_api_key.storefront_schema_registry.id
}

output "schema_registry_api_secret" {
  value     = confluent_api_key.storefront_schema_registry.secret
  sensitive = true
}

output "lightning_api_key" {
  value = confluent_api_key.storefront_lightning.id
}

output "lightning_api_secret" {
  value     = confluent_api_key.storefront_lightning.secret
  sensitive = true
}

resource "local_sensitive_file" "local_env" {
  filename        = "${path.module}/../config/local.env"
  file_permission = "0600"

  content = templatefile("${path.module}/templates/local.env.tpl", {
    environment_id             = var.environment_id
    kafka_cluster_id           = var.kafka_cluster_id
    schema_registry_cluster_id = data.confluent_schema_registry_cluster.this.id
    cloud_provider             = var.cloud_provider
    cloud_region               = var.cloud_region

    # bootstrap_endpoint comes back as "SASL_SSL://host:port" on this cluster; strip
    # the scheme since kafka-python's bootstrap_servers wants plain "host:port".
    bootstrap_servers = replace(data.confluent_kafka_cluster.this.bootstrap_endpoint, "SASL_SSL://", "")

    kafka_api_key    = confluent_api_key.storefront_kafka.id
    kafka_api_secret = confluent_api_key.storefront_kafka.secret

    schema_registry_url        = data.confluent_schema_registry_cluster.this.rest_endpoint
    schema_registry_api_key    = confluent_api_key.storefront_schema_registry.id
    schema_registry_api_secret = confluent_api_key.storefront_schema_registry.secret

    topic_item_added_to_stock = var.topic_item_added_to_stock
    topic_item_purchased      = confluent_kafka_topic.item_purchased.topic_name
    topic_current_inventory   = var.topic_current_inventory

    storefront_principal_id = confluent_service_account.storefront.id

    lightning_api_key    = confluent_api_key.storefront_lightning.id
    lightning_api_secret = confluent_api_key.storefront_lightning.secret
  })
}
