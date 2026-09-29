# --- Storefront app: CUT 1 (publishes item_purchased) and CUT 4 (reads current_inventory) ---

resource "confluent_service_account" "storefront" {
  display_name = "shoe-store-storefront"
  description  = "Publishes item_purchased events and queries current_inventory via the Lightning Query API"
}

resource "confluent_api_key" "storefront_kafka" {
  display_name = "shoe-store-storefront-kafka-key"
  description  = "Kafka API key for the storefront app"
  owner {
    id          = confluent_service_account.storefront.id
    api_version = confluent_service_account.storefront.api_version
    kind        = confluent_service_account.storefront.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.this.id
    api_version = data.confluent_kafka_cluster.this.api_version
    kind        = data.confluent_kafka_cluster.this.kind
    environment {
      id = var.environment_id
    }
  }
}

resource "confluent_api_key" "storefront_schema_registry" {
  display_name = "shoe-store-storefront-sr-key"
  description  = "Schema Registry API key for the storefront app"
  owner {
    id          = confluent_service_account.storefront.id
    api_version = confluent_service_account.storefront.api_version
    kind        = confluent_service_account.storefront.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.this.id
    api_version = data.confluent_schema_registry_cluster.this.api_version
    kind        = data.confluent_schema_registry_cluster.this.kind
    environment {
      id = var.environment_id
    }
  }
}

# Owner is normally the storefront service account, but some orgs (seen on a shared
# multi-tenant sandbox org) reject Global API key creation for service accounts with
# a plain 403 Forbidden while allowing it for users - set
# lightning_api_key_owner_email to work around that on such an org.
locals {
  lightning_key_owner = length(data.confluent_user.lightning_key_owner) > 0 ? {
    id          = data.confluent_user.lightning_key_owner[0].id
    api_version = data.confluent_user.lightning_key_owner[0].api_version
    kind        = data.confluent_user.lightning_key_owner[0].kind
    } : {
    id          = confluent_service_account.storefront.id
    api_version = confluent_service_account.storefront.api_version
    kind        = confluent_service_account.storefront.kind
  }
}

# A Global API key ("confluent api-key create --resource global"), NOT a Cloud API
# key (that's the no-managed_resource form, and organization-scoped Cloud keys are
# rejected by the Lightning Query API with "not a key that authenticates a whole
# organization" - confirmed against the live endpoint). managed_resource's
# id/api_version/kind here are fixed sentinel values for this key type, not
# references to another resource.
resource "confluent_api_key" "storefront_lightning" {
  display_name = "shoe-store-storefront-lightning-key"
  description  = "Global API key for the Lightning Query API"
  owner {
    id          = local.lightning_key_owner.id
    api_version = local.lightning_key_owner.api_version
    kind        = local.lightning_key_owner.kind
  }
  managed_resource {
    id          = "global"
    api_version = "global/v1"
    kind        = "Global"
  }
}

# Lightning queries run as whichever principal owns the key above, so when that's a
# user (not the storefront service account), the user needs the same read access.
resource "confluent_role_binding" "lightning_key_owner_cluster_admin" {
  count       = length(data.confluent_user.lightning_key_owner) > 0 ? 1 : 0
  principal   = "User:${data.confluent_user.lightning_key_owner[0].id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = data.confluent_kafka_cluster.this.rbac_crn
}

resource "confluent_role_binding" "lightning_key_owner_schema_registry_owner" {
  count       = length(data.confluent_user.lightning_key_owner) > 0 ? 1 : 0
  principal   = "User:${data.confluent_user.lightning_key_owner[0].id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.this.resource_name}/subject=*"
}

# CUT 1 (write item_purchased) + CUT 3/4 (read current_inventory via the Lightning
# Query API). This cluster is on the Basic tier, which rejects per-resource role
# bindings ("Basic Clusters can not use resource roles" - confirmed against the live
# cluster) - only cluster-wide roles work, so a single CloudClusterAdmin binding
# stands in for what would otherwise be separate DeveloperWrite/DeveloperRead
# bindings scoped to individual topics. Narrow this to per-topic ACLs
# (confluent_kafka_acl) if you move this demo to a Standard+ cluster.
resource "confluent_role_binding" "storefront_cluster_admin" {
  principal   = "User:${confluent_service_account.storefront.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = data.confluent_kafka_cluster.this.rbac_crn
}

# ResourceOwner (not just DeveloperRead) because Terraform itself must be able to
# register item_purchased's schema (confluent_schema in topics.tf), on top of the
# app's runtime need to read it back (kafka_producer.py) and RTCE's own prerequisite
# of read access to Schema Registry.
resource "confluent_role_binding" "storefront_schema_registry_owner" {
  principal   = "User:${confluent_service_account.storefront.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.this.resource_name}/subject=*"
}

# --- Flink statement runner: CUT 1 (stock side) and CUT 2 ---

resource "confluent_service_account" "flink_runner" {
  display_name = "shoe-store-flink-runner"
  description  = "Runs the Flink SQL statements for the Job 2 shoe store demo"
}

resource "confluent_api_key" "flink_runner" {
  display_name = "shoe-store-flink-runner-key"
  description  = "Flink API key for the Flink SQL statements"
  owner {
    id          = confluent_service_account.flink_runner.id
    api_version = confluent_service_account.flink_runner.api_version
    kind        = confluent_service_account.flink_runner.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.this.id
    api_version = data.confluent_flink_region.this.api_version
    kind        = data.confluent_flink_region.this.kind
    environment {
      id = var.environment_id
    }
  }
}

# Lets this principal submit/manage Flink statements in the environment.
resource "confluent_role_binding" "flink_runner_flink_developer" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = data.confluent_environment.this.resource_name
}

# Statements run AS this principal against Kafka, so it also needs data-plane rights:
# it creates item_added_to_stock and current_inventory (DDL) and reads/writes across
# the cluster. CloudClusterAdmin is a deliberate demo-scale simplification rather than
# per-topic ACLs/role bindings - narrow this for anything beyond a sandbox demo.
resource "confluent_role_binding" "flink_runner_cluster_admin" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = data.confluent_kafka_cluster.this.rbac_crn
}

# 'value.format' = 'json-registry' in the Flink DDL (flink-sql/01 and 04) registers a
# schema on CREATE TABLE, so this principal needs Schema Registry write access too.
resource "confluent_role_binding" "flink_runner_schema_registry_owner" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.this.resource_name}/subject=*"
}
