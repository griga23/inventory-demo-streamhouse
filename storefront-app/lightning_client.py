"""CUT 4: reads current stock per SKU via the Confluent Cloud Lightning Query API
(Real-Time Context Engine), a millisecond point-lookup over the current_inventory
upsert table that Flink maintains (CUT 2 / CUT 3).

Endpoint contract (Early Access; confirmed live against an org with the REST API
enabled, 2026-09-28 - request shape matches the staging docs, response shape did not
and is corrected below):
  POST https://sql.<region>.<cloud>.confluent.cloud/query/v1alpha1
  Auth: HTTP Basic with a Global API key (LIGHTNING_API_KEY / LIGHTNING_API_SECRET)
  Body: {catalog_name, database_name, query, options, client_info}
  Response: {api_version, kind, result: {result_format, schema: {columns: [...]},
  data: [[...], ...]}} - JSON format returns rows as plain arrays of string-encoded
  values, nested under `result`, not at the top level as the staging docs implied.
"""
import requests

import config

QUERY_ENDPOINT = f"https://sql.{config.CLOUD_REGION}.{config.CLOUD_PROVIDER}.confluent.cloud/query/v1alpha1"


class InventoryQueryError(RuntimeError):
    pass


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def current_quantity(sku: str) -> int | None:
    """Returns the current quantity for `sku`, or None if it has no rows yet."""
    query = f"SELECT quantity FROM {config.TOPIC_CURRENT_INVENTORY} WHERE sku = '{_escape_sql_literal(sku)}'"

    response = requests.post(
        QUERY_ENDPOINT,
        auth=(config.LIGHTNING_API_KEY, config.LIGHTNING_API_SECRET),
        json={
            "catalog_name": config.ENV_ID,
            "database_name": config.KAFKA_CLUSTER_ID,
            "query": query,
            "options": {"max_result_rows": 1, "result_format": "JSON"},
            "client_info": {},
        },
        timeout=5,
    )
    if response.status_code != 200:
        raise InventoryQueryError(f"Lightning query failed with HTTP {response.status_code}: {response.text}")

    return _extract_quantity(response.json())


def _extract_quantity(body: dict) -> int | None:
    result = body.get("result") or {}
    rows = result.get("data") or []
    if not rows:
        return None

    quantity_index = 0
    columns = (result.get("schema") or {}).get("columns") or []
    for i, column in enumerate(columns):
        name = column if isinstance(column, str) else column.get("name", "")
        if name.lower() == "quantity":
            quantity_index = i
            break

    first_row = rows[0]
    if quantity_index >= len(first_row):
        return None
    return int(first_row[quantity_index])
