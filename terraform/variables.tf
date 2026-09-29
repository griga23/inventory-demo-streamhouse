variable "environment_id" {
  description = "Existing Confluent Cloud environment ID (env-xxxxxx) this demo runs in"
  type        = string
}

variable "kafka_cluster_id" {
  description = "Existing Kafka cluster ID (lkc-xxxxxx) this demo runs in"
  type        = string
}

variable "flink_compute_pool_id" {
  description = "Existing Flink compute pool ID (lfcp-xxxxxx) that runs the demo's Flink statements"
  type        = string
}

variable "cloud_provider" {
  description = "Cloud provider hosting the cluster/compute pool (AWS, AZURE, or GCP) - Real-Time Context Engine currently requires AWS"
  type        = string
  default     = "AWS"
}

variable "cloud_region" {
  description = "Cloud region hosting the cluster/compute pool, e.g. us-west-2"
  type        = string
}

variable "topic_item_added_to_stock" {
  description = "Topic name for synthetic warehouse restock events (created by Flink SQL)"
  type        = string
  default     = "item_added_to_stock"
}

variable "topic_item_purchased" {
  description = "Topic name for real checkout purchase events (created by Terraform)"
  type        = string
  default     = "item_purchased"
}

variable "topic_current_inventory" {
  description = "Topic name for the materialized current-inventory-per-SKU upsert table (created by Flink SQL)"
  type        = string
  default     = "current_inventory"
}

variable "lightning_api_key_owner_email" {
  description = <<-EOT
    Optional: email of an existing Confluent Cloud user to own the Lightning Query
    API's Global API key, instead of the shoe-store-storefront service account.
    Some orgs (seen on a shared multi-tenant sandbox org) reject Global API key
    creation for service accounts with a plain 403, while allowing it for users -
    set this to work around that. When set, that user is also granted the same
    CloudClusterAdmin/ResourceOwner read access as the service account, since
    Lightning queries run as whichever principal owns the key.
  EOT
  type        = string
  default     = ""
}
