# Existing infrastructure this demo runs inside of. Nothing in this file is created
# or destroyed by Terraform - only referenced.

data "confluent_organization" "this" {}

data "confluent_environment" "this" {
  id = var.environment_id
}

data "confluent_kafka_cluster" "this" {
  id = var.kafka_cluster_id
  environment {
    id = var.environment_id
  }
}

# Essentials Stream Governance packages have exactly one Schema Registry cluster per
# environment, so it resolves from the environment alone.
data "confluent_schema_registry_cluster" "this" {
  environment {
    id = var.environment_id
  }
}

data "confluent_flink_compute_pool" "this" {
  id = var.flink_compute_pool_id
  environment {
    id = var.environment_id
  }
}

# Gives the Flink SQL statements' REST endpoint (compute pools don't expose it directly).
data "confluent_flink_region" "this" {
  cloud  = var.cloud_provider
  region = var.cloud_region
}

# Only looked up when lightning_api_key_owner_email is set - see its description in
# variables.tf and the comment on confluent_api_key.storefront_lightning.
data "confluent_user" "lightning_key_owner" {
  count = var.lightning_api_key_owner_email != "" ? 1 : 0
  email = var.lightning_api_key_owner_email
}
