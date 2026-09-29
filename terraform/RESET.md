# Full reset

Wipes all accumulated stock/purchase history and rebuilds `item_added_to_stock`,
`current_inventory`, and `item_purchased` from scratch with a clean starting state
(20 units of every SKU in `flink-sql/02_item_added_to_stock_seed.sql`, zero purchase
history). Needed whenever the demo's numbers have drifted too far from a clean
baseline to present, or after changing the product catalog (a new SKU added via an
additive seed file still starts clean; a *removed* SKU's old data doesn't disappear
on its own - only a full reset clears it).

**This is destructive and irreversible** - it deletes real Kafka topics/schemas and
everything in them. Order matters: doing this out of order either fails outright or
silently double-seeds every SKU (Kafka is append-only, so a statement whose SQL text
changes gets resubmitted in full rather than "updated").

Run all of this from `terraform/`, with credentials loaded (`source .env.local` or
equivalent).

## 1. Stop everything that touches the topics being reset

```bash
terraform destroy -auto-approve \
  -target=confluent_flink_statement.current_inventory_insert \
  -target=confluent_flink_statement.current_inventory_table \
  -target=confluent_flink_statement.item_added_to_stock_table \
  -target=confluent_rtce_topic.current_inventory \
  -target=confluent_schema.item_purchased_value \
  -target=confluent_kafka_topic.item_purchased
```

- `current_inventory_insert` must stop before its source topics disappear.
- `item_added_to_stock_table` / `current_inventory_table` must be removed from
  Terraform *state* (not just have their topic deleted) - otherwise `terraform
  apply` sees no config diff for their `CREATE TABLE` statement and won't resubmit
  it, so the topic never gets recreated.
- Any bounded/completed seed statements (e.g. `item_added_to_stock_seed`) don't need
  to be targeted here - the next plain `apply` handles them (either because their
  SQL text changed, or because they were removed from config).

If `terraform destroy` is blocked by Claude Code's auto-mode classifier ("Cloud
Storage Mass Delete"), run this step yourself - it isn't something that can be routed
around from inside a session.

## 2. Drop the two Flink-owned topics AND their schemas

`confluent_flink_statement` destroy does **not** run reverse DDL (per the provider's
own docs), so step 1 alone leaves the topics in place. Drop them explicitly:

```bash
confluent kafka topic delete item_added_to_stock --cluster <kafka_cluster_id> --environment <environment_id> --force
confluent kafka topic delete current_inventory --cluster <kafka_cluster_id> --environment <environment_id> --force
```

**Critical, easy to miss:** neither this CLI delete nor Flink SQL's `DROP TABLE`
actually removes the Schema Registry subject behind the topic - both leave it in a
soft-deleted state (still occupying the subject name, just with all versions
inaccessible via a normal GET). If you skip this, the next `CREATE TABLE` fails with
`Cannot create table because the Schema Registry subject '<topic>-value' doesn't
match the existing one` (if its schema changed) or can leave the *new* schema
registered as version N+1 alongside the old, still-referenceable version - which was
the root cause of a real `current_inventory` RTCE outage (`DP_INVALID_TABLE`) hit
while developing this demo. Hard-delete both subjects for each topic you dropped:

```bash
python3 -c "
import requests
url = 'https://psrc-....confluent.cloud'          # SCHEMA_REGISTRY_URL from config/local.env
auth = ('<SCHEMA_REGISTRY_API_KEY>', '<SCHEMA_REGISTRY_API_SECRET>')
for subj in ['item_added_to_stock-value', 'item_added_to_stock-key',
             'current_inventory-value', 'current_inventory-key']:
    requests.delete(f'{url}/subjects/{subj}', auth=auth)                    # soft delete
    requests.delete(f'{url}/subjects/{subj}?permanent=true', auth=auth)     # hard delete
"
```

(`item_purchased`'s topic+schema were already deleted in step 1, via
`confluent_kafka_topic`/`confluent_schema` directly - no separate drop needed for it.)

## 3. Rebuild

```bash
terraform apply
```

Recreates, in order: `item_added_to_stock` (+ the consolidated seed, 20 units per
SKU), `item_purchased` (+ schema, empty), `current_inventory` (+ materialization),
and re-enables RTCE. Also regenerates `config/local.env`.

## 4. Verify

Query `current_inventory` (via the Lightning Query API once enabled, or the RTCE MCP
endpoint in the meantime) and confirm every current catalog SKU shows exactly 20, and
no stale/removed SKUs appear. A brief `DP_TABLE_NOT_AVAILABLE` (retryable) right after
`apply` is normal sync lag - give it 30-60s.

## Known issue: RTCE can get stuck after repeated resets

After enough drop/recreate cycles on the same topic name (even doing steps 1-3
correctly, schemas included), the RTCE query layer has been observed to get stuck
returning `DP_INVALID_TABLE` ("Internal server error", non-retryable) or oscillating
between that and `DP_TABLE_NOT_AVAILABLE`, indefinitely - while `listTopics`/
`getMetadata` report the table as healthy and the underlying Kafka data (verified
directly with a `kafka-python` consumer) is completely correct. Disabling and
re-enabling RTCE on just that topic
(`terraform destroy -target=confluent_rtce_topic.current_inventory` then
`terraform apply`) did **not** fix it in the one case this happened. This looks like a
genuine Confluent Cloud RTCE (Early Access) backend bug, not something fixable from
this repo - if you hit it, that's the point to actually take the error's own advice
and contact Confluent support, with the environment/cluster/topic ids and a
timestamp.
