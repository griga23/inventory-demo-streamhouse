# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Confluent Cloud sales-engineering demo for the CUJ "Job 2: One consistent state for
all" (see README.md for the full narrative and Confluence link): stock and purchase
events flow through Kafka, Flink materializes current inventory per SKU into an
upsert (compacted) topic, and a small storefront app reads that state in milliseconds
via the Lightning Query API (Confluent Cloud's Real-Time Context Engine, Early
Access).

There is no local/mocked mode — everything runs against a real Confluent Cloud
environment. All Confluent Cloud resources (topics, schemas, service accounts, API
keys, role bindings, the Flink SQL statements, and the Lightning Table) are managed by
Terraform in `terraform/` — there is no manual console/CLI provisioning step anywhere
in this repo *except* the destructive parts of a full reset (see "Resetting the
demo's data" below), which intentionally fall outside Terraform. Terraform
*references* an existing environment/cluster/Flink compute pool (via
`environment_id`/`kafka_cluster_id`/`flink_compute_pool_id` in
`terraform/terraform.tfvars`); it does not create them.

An MCP server named `confluent-rtce` is registered for this project in
`~/.claude.json` (HTTP transport, Global API key baked into the header) — it exposes
`listTopics`/`getMetadata`/`queryData` against the same RTCE-backed tables the
storefront app queries. Useful for debugging `current_inventory` directly from a
Claude Code session without going through the app or hand-rolling HTTP calls.

## Architecture

The pipeline has four stages mapped to Critical User Tasks (CUTs) from the CUJ.
Schema/topic ownership is deliberately split between Flink SQL and Terraform, and
`terraform/flink.tf`'s `depends_on` chains encode the resulting build order:

1. **CUT 1, stock side — Flink-owned.** `flink-sql/01_item_added_to_stock_table.sql`
   creates the real, schema-registered `item_added_to_stock` topic.
   `02_item_added_to_stock_seed.sql` is a one-time bounded `INSERT ... VALUES`
   statement that seeds a fixed starting quantity (20) per SKU — it runs to
   completion and leaves no standing job. No hand-written producer for this side.
   **Once applied, never edit this file's SQL text** (not even a comment): Terraform
   tracks the resource by its `statement` string, so any change forces a resubmit,
   and because Kafka is append-only that means re-adding +20 to every SKU already
   listed, on top of whatever real purchases have happened. Add a new SKU/size via a
   brand-new file + a brand-new `confluent_flink_statement` resource instead (see git
   history for the pattern — this file itself is already a consolidation of three
   such additive files from earlier iterations of the catalog).
2. **CUT 1, purchase side — Terraform + app-owned.** `terraform/topics.tf` creates the
   `item_purchased` topic and registers its JSON schema
   (`terraform/schemas/item_purchased.json`) directly — not via a Flink CREATE TABLE.
   `storefront-app/kafka_producer.py` publishes to it when a customer clicks Buy, using
   `schema_registry.py` to look up the existing schema id (it never registers or
   creates anything). Confluent Cloud Flink auto-discovers any schema-registered topic
   as a table, so statement 4 below can reference `item_purchased` once Terraform has
   created it (see the `depends_on` in `terraform/flink.tf`).
3. **CUT 2.** `flink-sql/03_current_inventory_table.sql` /
   `04_current_inventory_insert.sql` sum both event streams
   (`item_added_to_stock` positive, `item_purchased` negative) grouped by `sku` into
   `current_inventory`, a `PRIMARY KEY` + `changelog.mode=upsert` +
   `kafka.cleanup-policy=compact` table — the compacted/keyed shape RTCE's upsert mode
   requires. `quantity` is `BIGINT` with **no cast** on the `SUM()` — confirmed
   empirically (a live Flink type-checker error, not just docs) that `SUM()` over an
   `INT` column widens to `BIGINT` in Confluent Cloud's Flink SQL, so this already
   matches with no cast; making the column `INT` instead would require a *narrowing*
   cast, not eliminate one. See the comment in `04_current_inventory_insert.sql`.
   `flink-sql/05_current_inventory_isolation_level.sql` additionally `ALTER TABLE`s
   `kafka.consumer.isolation-level` to `read-uncommitted` on this table - confirmed
   applied (via `SHOW CREATE TABLE`) but confirmed via direct timing tests to have
   **no effect on RTCE's read latency** (RTCE only reads Kafka-committed records, and
   commits happen at Flink checkpoint boundaries - a producer/job-side concern the
   consumer-side isolation level can't touch). It's harmless to leave applied. Root
   cause of RTCE's ~17s-ish read latency is Flink's checkpoint interval, which is
   **not configurable at all** on Confluent Cloud's managed Flink (confirmed via a
   live "Unsupported configuration options" error attempting
   `execution.checkpointing.interval` in a statement's `properties` - that option only
   exists for self-managed Confluent Platform Flink, a different product). Don't
   re-attempt tuning this from Terraform/Flink SQL; see
   [[reference-confluent-terraform-gotchas]] memory for the full investigation. Also
   learned the hard way: `confluent_flink_statement.properties` cannot be changed via
   `terraform apply` on an existing statement under any combination with `stopped`
   (only `stopped`/`principal`/`compute_pool`/`credentials` are live-updatable) -
   changing `properties` requires destroying and recreating the statement.
4. **CUT 3.** `terraform/rtce.tf`'s `confluent_rtce_topic` resource turns on the
   Lightning Table on `current_inventory`, gated behind the Flink insert statement via
   `depends_on`. `terraform/service_accounts.tf` grants the `shoe-store-storefront`
   service account `DeveloperRead` on that topic and on the Schema Registry cluster
   (RTCE's stated prerequisites) plus `DeveloperWrite` on `item_purchased`, and creates
   its three API keys (Kafka, Schema Registry, and a Global API key — specifically
   `managed_resource { id = "global", api_version = "global/v1", kind = "Global" }`,
   *not* the no-`managed_resource` Cloud API key shape, which the Lightning Query API
   rejects with a 401).
5. **CUT 4.** `storefront-app/lightning_client.py` reads current stock via the
   Lightning Query API: `POST https://sql.<region>.<cloud>.confluent.cloud/query/v1alpha1`,
   HTTP Basic auth with the Global API key, JSON body of
   `{catalog_name, database_name, query, options, client_info}`. This is an Early
   Access API; the response shape is confirmed (not a guess) against a live org with
   it enabled: `{api_version, kind, result: {result_format, schema: {columns: [...]},
   data: [[...], ...]}}` - rows/columns are nested under `result`, not top-level as
   the staging docs implied. If a different org's response shape differs, only
   `_extract_quantity` should need changing, not the endpoint/auth/request
   construction. If this 403s with `api_not_enabled`, that's a separate org-level EA
   flag from RTCE itself, not yet enabled everywhere - RTCE can be fully working (and
   queryable via the `confluent-rtce` MCP server) while this direct REST path is
   still gated on a given org.

There's also a separate `flink_runner` service account (in `service_accounts.tf`)
that owns the Flink statements from stages 1 and 3 — it's granted `FlinkDeveloper` on
the environment plus `CloudClusterAdmin` on the Kafka cluster (a deliberate demo-scale
simplification over per-topic ACLs/RBAC, called out in a comment there). This
particular Kafka cluster is on the **Basic** tier, which rejects per-resource RBAC
role bindings outright (`403 Basic Clusters can not use resource roles`) — only
cluster-wide roles work, which is *why* it's `CloudClusterAdmin` rather than
narrower per-topic grants; narrow this if the demo ever moves to a Standard+ cluster.

`terraform/outputs.tf` renders `config/local.env` directly via a `local_sensitive_file`
resource + `terraform/templates/local.env.tpl` — running `terraform apply` is the only
setup step; there's nothing to copy from `terraform output` by hand.
`storefront-app/config.py` is the single place that file is read from (via
`python-dotenv`); every other Python module imports from `config.py` rather than
reading env vars directly.

`storefront-app/app.py` (Flask) ties the app together: `GET /` renders the product
grid, `GET /api/inventory/<sku>` calls `lightning_client.py`,
`POST /api/buy/<sku>` calls `kafka_producer.py`. `static/app.js` polls inventory every
2s normally and switches to fast polling (300ms x10) right after a Buy click to
visibly show the Flink materialization lag. `storefront-app/Dockerfile` runs the same
`app` object under gunicorn (not Flask's dev server) for hosted deployment - see
README.md's "Sharing the storefront" section; secrets are injected via the host
platform's env vars/secret store at runtime (`config.py` already reads from
`os.environ` first), never baked into the image.

`storefront-app/products.py` holds the demo's 6-SKU catalog (3 shoes × 2 sizes each)
— it must stay in sync with the SKUs seeded in `flink-sql/02_item_added_to_stock_seed.sql`,
since Flink doesn't read the Python catalog. `products.families()` groups the flat
SKU list by (brand, model, color) into one storefront card per shoe, each with a
size-selector toggle (`.size-btn` in `templates/index.html` / `static/app.js`) that
swaps the card's active SKU; a variant with only one size (none currently in the
catalog, but the code path still exists) would render its size as plain text instead.
Product images are hand-authored inline-shape SVGs in `static/images/` (one shared
file per shoe/color, reused across its sizes) — deliberately not real product photos,
to avoid trademark/hotlinking concerns and keep the demo dependency-free. Getting the
sneaker silhouette to actually look like a shoe took rendering iterations (headless
Chrome screenshot + visual check) rather than guessing bezier coordinates blind — the
first couple of attempts looked like blobs.

## Resetting the demo's data

`terraform/RESET.md` is the authoritative runbook — read it before doing this rather
than improvising, since getting the order wrong either fails outright or silently
double-seeds every SKU. Short version: destroy the relevant `confluent_flink_statement`
/ `confluent_rtce_topic` resources via a narrowly-`-target`ed `terraform destroy`,
delete the underlying Kafka topics via the `confluent` CLI, **hard-delete their
Schema Registry subjects too** (a step that's easy to miss — neither the CLI delete
nor Flink's `DROP TABLE` actually removes the schema, only soft-deletes it, which
causes real failures on recreation), then `terraform apply` to rebuild. That file also
documents a real RTCE backend bug (`DP_INVALID_TABLE`, persists indefinitely) hit
during development after repeated reset cycles — if you hit the same thing, it's a
platform issue to escalate to Confluent support, not something to keep debugging
locally.

The full-reset `terraform destroy -target=...` spanning multiple resource *types*
(Kafka topic + schema + RTCE + several Flink statements at once) gets blocked by
Claude Code's auto-mode classifier as a "Cloud Storage Mass Delete" — that step needs
to be run by the user directly, not from inside a session. Smaller, single-purpose
destroys (one or two closely-related resources) and direct `confluent`/Schema
Registry CLI or REST calls have gone through fine.

## Multiple Confluent Cloud orgs / environments

This demo has been deployed to more than one Confluent Cloud org (different orgs
have different EA feature availability - e.g. only some have the Lightning Query
REST API enabled, not just RTCE). Each org's deployment is isolated in its own
Terraform workspace (`terraform workspace list` / `new` / `select`), with its own
`<name>.tfvars` and `.env.local.<name>` files (both gitignored, alongside the
default `terraform.tfvars`/`.env.local`) - `terraform apply` always writes to the
single `config/local.env`, so whichever workspace you last applied is what the
storefront app currently points at. As of 2026-09-28 the active one is the
`se-s-emea` workspace/org (Lightning Query REST API confirmed working there).

Not every org grants the same permissions for the same Terraform config. On
`se-s-emea` specifically, Global API key creation for a *service account* returns a
plain `403 Forbidden`, but works for a *user* - `variables.tf`'s
`lightning_api_key_owner_email` exists to work around exactly this (see its
description and the conditional resources in `service_accounts.tf`/`data.tf`). Global
API keys are also capped per-user (2 on this org) - `confluent api-key list
--resource global` / `confluent api-key delete` if you hit `402 Payment Required`.

`storefront-app/venv/` bakes in an absolute path at creation time - moving or
renaming the project directory breaks it silently (`ModuleNotFoundError` for
already-installed packages). Recreate it (`rm -rf venv && python3 -m venv venv &&
source venv/bin/activate && pip install -r requirements.txt`) after any such move.

## Commands

```bash
# Provision everything (CUT 1-3) and generate config/local.env
cd terraform
terraform workspace select se-s-emea   # or: terraform workspace new <name> for a new org
source .env.local.se-s-emea            # or .env.local - see "Multiple Confluent Cloud orgs" above
terraform init
terraform validate   # can be run offline, no credentials needed
terraform plan -var-file=se-s-emea.tfvars
terraform apply -var-file=se-s-emea.tfvars

# Run the storefront app (CUT 4)
cd ../storefront-app
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python app.py         # http://localhost:5050 (PORT env var overrides; 5000 clashes with macOS AirPlay)

# Deploy the storefront app so someone else can hit it (see README.md's
# "Sharing the storefront" section) - live at https://shoe-store-demo.fly.dev
flyctl deploy
grep -v '^#' ../config/local.env | grep -v '^$' | flyctl secrets import   # after config/local.env changes

# Tear down (leaves Flink-created Kafka topics behind - see README.md and RESET.md)
cd ../terraform
terraform destroy -var-file=se-s-emea.tfvars
```

There is no test suite or linter configured in this repo — it's a demo, not a
library. After editing Terraform, `terraform fmt -recursive && terraform validate` in
`terraform/` catches syntax/schema errors without needing real credentials (validate
still needs `terraform init` to have downloaded the provider once); always follow up
with `terraform plan` and check the diff matches what you expect before applying,
especially near anything already-applied and seed-like (see the warning in stage 1
above). After editing a Python module, `python3 -m py_compile <file>.py` is a
reasonable sanity check. Flink SQL statements can only be fully validated by actually
applying them via Terraform against a live Confluent Cloud environment.
