# Shoe Store Demo — Job 2: One consistent state for all

Demo for the Confluent Cloud CUJ: *"Keep one consistent current state that every
system can read in real time."* Stock and purchase events flow through Kafka, Flink
materializes current inventory per SKU into an upsert (compacted) topic, and a small
storefront app reads that state in milliseconds via the Lightning Query API
(Real-Time Context Engine).

Reference: [Job 2: One consistent state for all](https://confluentinc.atlassian.net/wiki/spaces/FLINK/pages/6171328535/Job+2+One+consistent+state+for+all)

## Demo narrative

1. A customer selects **Nike Air Max, black, size 42**.
2. The storefront queries the Lightning Table for current stock.
3. Customer clicks **Buy** → Kafka receives a sale event.
4. Flink updates the materialized `current_inventory` state.
5. The storefront reads the new availability.
6. When quantity reaches zero, the product card shows **Out of stock**.

## Architecture

All Confluent Cloud infrastructure, demo data generation, and Flink SQL are managed
by Terraform (`terraform/`) — there are no manual console/CLI provisioning steps.
The only thing that isn't Terraform is the storefront app itself and the real-time
"click Buy" event it produces.

```
                     ┌─────────────────────────┐
 fixed seed     ───► │ item_added_to_stock     │─┐
 (Flink, once)       └─────────────────────────┘ │
                                                  ├──► Flink SQL ──► current_inventory  ──► Real-Time Context
 storefront app ───► ┌─────────────────────────┐ │    (upsert per SKU)   (compacted)        Engine / Lightning
 (Buy click)         │ item_purchased          │─┘                                          Query API
                     └─────────────────────────┘                                                  │
                                                                                                    ▼
                                                                                          storefront app (reads)
```

| CUT | What | Where |
| --- | --- | --- |
| 1 | Publish `item_added_to_stock` (a fixed starting quantity, seeded once) and `item_purchased` (real, from Buy clicks) events, each with a registered schema | `flink-sql/01-02_item_added_to_stock_*.sql` (Flink-owned), `terraform/topics.tf` + `storefront-app/kafka_producer.py` (Terraform/app-owned) |
| 2 | Compute current inventory per SKU in Flink, publish to an upsert topic | `flink-sql/03_current_inventory_table.sql`, `flink-sql/04_current_inventory_insert.sql` |
| 3 | Enable a Lightning Table (Real-Time Context Engine) on `current_inventory` and grant read access | `terraform/rtce.tf`, `terraform/service_accounts.tf` |
| 4 | Query current stock in milliseconds from the app | `storefront-app/lightning_client.py` |

Stock starts at a fixed, known quantity per SKU (seeded once via a bounded Flink
`INSERT ... VALUES`, not continuously generated), while purchases are real events
triggered by clicking Buy in the UI — so the demo stays fully interactive and the
"hits zero → Out of stock" moment is fully in your control.

Schema ownership is also split, and Terraform enforces it: `item_added_to_stock` and
`current_inventory` are created by Flink SQL (`CREATE TABLE ... WITH (...)`, applied
via `confluent_flink_statement` resources), while `item_purchased` — the one real,
app-produced stream — is created directly by Terraform
(`confluent_kafka_topic` + `confluent_schema`, `terraform/topics.tf`). The storefront
app only ever looks up that schema's id; it never registers or creates anything.

## Prerequisites

- A Confluent Cloud environment with an existing Kafka cluster, Schema Registry, and
  Flink compute pool (Terraform references these — it does not create them)
- The Real-Time Context Engine / Lightning Query API Early Access feature enabled for
  your organization
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- A Cloud API key/secret for your Confluent Cloud account, exported as
  `CONFLUENT_CLOUD_API_KEY` / `CONFLUENT_CLOUD_API_SECRET`
- Python 3.10+

## Setup

### 1. Provision everything with Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in your environment/cluster/compute pool IDs
export CONFLUENT_CLOUD_API_KEY=...
export CONFLUENT_CLOUD_API_SECRET=...
terraform init
terraform apply
```

This creates, in order:

- **CUT 1/2 (Flink SQL):** `item_added_to_stock` (+ a one-time seed of 20 units per
  SKU) and `current_inventory` (+ its standing insert), by submitting
  `flink-sql/*.sql` as `confluent_flink_statement` resources.
- **CUT 1 (item_purchased):** the topic and its JSON schema, owned directly by
  Terraform.
- **CUT 3:** the Lightning Table on `current_inventory` (`confluent_rtce_topic`), plus
  a `shoe-store-storefront` service account with `DeveloperRead` on that topic and on
  the Schema Registry cluster, and `DeveloperWrite` on `item_purchased`.
- Kafka, Schema Registry, and a Global (Lightning Query API) API key for that service
  account.

It finishes by writing `config/local.env` for you (see `terraform/outputs.tf` /
`terraform/templates/local.env.tpl`) — there's nothing to copy by hand.

### 2. Run the storefront app

```bash
cd storefront-app
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open http://localhost:5050 (set `PORT` to override - 5000 clashes with macOS AirPlay
Receiver, which is why the app doesn't default to it). Click **Buy** on a product a few times and watch the
quantity drop in near real time; once it hits zero the card flips to **Out of
stock**.

## Notes on the Lightning Query API

`storefront-app/lightning_client.py` is written against the documented Early Access
contract (POST `/query/v1alpha1`, HTTP Basic auth with a Global API key, JSON body of
`catalog_name`/`database_name`/`query`). Response parsing (`_extract_quantity`)
matches a real response confirmed against a live org with the API enabled -
`{api_version, kind, result: {schema: {columns: [...]}, data: [[...], ...]}}`, nested
under `result` rather than top-level as the staging docs implied. If a different org's
response shape differs, that's the only place you should need to adjust. If you get
`403 api_not_enabled`, that's a separate org-level EA flag from RTCE itself.

## Sharing the storefront with someone else

The app is a normal containerized Flask app (`storefront-app/Dockerfile`, served by
gunicorn) — deploy it anywhere that runs a Docker image, pointed at the same
Confluent Cloud credentials as your local `config/local.env`. Deployed once to
[Fly.io](https://fly.io) as `shoe-store-demo` (https://shoe-store-demo.fly.dev):

```bash
cd storefront-app
flyctl deploy                                              # redeploy after a code change
grep -v '^#' ../config/local.env | grep -v '^$' | flyctl secrets import  # after config/local.env changes (e.g. a reset)
```

`fly.toml` is checked in (no secrets in it); the actual credentials live only in Fly's
secret store, imported from `config/local.env` — re-run the `secrets import` command
above any time that file regenerates (e.g. after `terraform apply`) so the deployed
app doesn't drift onto stale API keys. The machine auto-stops when idle
(`auto_stop_machines = 'stop'`, `min_machines_running = 0` in `fly.toml`) and restarts
on the next request, so there's no cost while nobody's using it.

## Tearing down

```bash
cd terraform
terraform destroy
```

Note: `confluent_flink_statement` destroy stops the statement but does not run
reverse DDL, so the Kafka topics Flink created (`item_added_to_stock`,
`current_inventory`) persist in Confluent Cloud even after `terraform destroy` — drop
them by hand if you want a fully clean slate.
