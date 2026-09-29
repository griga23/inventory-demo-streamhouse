# CUT 1 (stock side) and CUT 2: the demo's Flink SQL, applied in dependency order.
# Each file under ../flink-sql holds exactly one statement, since Terraform submits
# each confluent_flink_statement independently.

locals {
  flink_statement_defaults = {
    organization = { id = data.confluent_organization.this.id }
    environment  = { id = var.environment_id }
    compute_pool = { id = var.flink_compute_pool_id }
    principal    = { id = confluent_service_account.flink_runner.id }
  }
  flink_properties = {
    "sql.current-catalog"  = data.confluent_environment.this.display_name
    "sql.current-database" = data.confluent_kafka_cluster.this.display_name
  }
}

resource "confluent_flink_statement" "item_added_to_stock_table" {
  statement  = file("${path.module}/../flink-sql/01_item_added_to_stock_table.sql")
  properties = local.flink_properties

  organization {
    id = local.flink_statement_defaults.organization.id
  }
  environment {
    id = local.flink_statement_defaults.environment.id
  }
  compute_pool {
    id = local.flink_statement_defaults.compute_pool.id
  }
  principal {
    id = local.flink_statement_defaults.principal.id
  }
  rest_endpoint = data.confluent_flink_region.this.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }
}

resource "confluent_flink_statement" "item_added_to_stock_seed" {
  statement  = file("${path.module}/../flink-sql/02_item_added_to_stock_seed.sql")
  properties = local.flink_properties

  organization {
    id = local.flink_statement_defaults.organization.id
  }
  environment {
    id = local.flink_statement_defaults.environment.id
  }
  compute_pool {
    id = local.flink_statement_defaults.compute_pool.id
  }
  principal {
    id = local.flink_statement_defaults.principal.id
  }
  rest_endpoint = data.confluent_flink_region.this.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [confluent_flink_statement.item_added_to_stock_table]
}

resource "confluent_flink_statement" "current_inventory_table" {
  statement  = file("${path.module}/../flink-sql/03_current_inventory_table.sql")
  properties = local.flink_properties

  organization {
    id = local.flink_statement_defaults.organization.id
  }
  environment {
    id = local.flink_statement_defaults.environment.id
  }
  compute_pool {
    id = local.flink_statement_defaults.compute_pool.id
  }
  principal {
    id = local.flink_statement_defaults.principal.id
  }
  rest_endpoint = data.confluent_flink_region.this.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }
}

resource "confluent_flink_statement" "current_inventory_isolation_level" {
  statement  = file("${path.module}/../flink-sql/05_current_inventory_isolation_level.sql")
  properties = local.flink_properties

  organization {
    id = local.flink_statement_defaults.organization.id
  }
  environment {
    id = local.flink_statement_defaults.environment.id
  }
  compute_pool {
    id = local.flink_statement_defaults.compute_pool.id
  }
  principal {
    id = local.flink_statement_defaults.principal.id
  }
  rest_endpoint = data.confluent_flink_region.this.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [confluent_flink_statement.current_inventory_table]
}

resource "confluent_flink_statement" "current_inventory_insert" {
  statement  = file("${path.module}/../flink-sql/04_current_inventory_insert.sql")
  properties = local.flink_properties

  organization {
    id = local.flink_statement_defaults.organization.id
  }
  environment {
    id = local.flink_statement_defaults.environment.id
  }
  compute_pool {
    id = local.flink_statement_defaults.compute_pool.id
  }
  principal {
    id = local.flink_statement_defaults.principal.id
  }
  rest_endpoint = data.confluent_flink_region.this.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  # Needs item_added_to_stock seeded (so the source table has rows) and
  # item_purchased already registered as a table (Terraform-owned, see topics.tf).
  depends_on = [
    confluent_flink_statement.current_inventory_table,
    confluent_flink_statement.item_added_to_stock_seed,
    confluent_schema.item_purchased_value,
  ]
}
