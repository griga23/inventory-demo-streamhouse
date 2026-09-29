"""Minimal Schema Registry REST client: just enough to look up the schema id for a
subject, so kafka_producer.py can write Confluent wire-format bytes without depending
on a native client library (confluent-kafka needs librdkafka installed).

The item_purchased schema itself is owned by Terraform (see
terraform/topics.tf + terraform/schemas/item_purchased.json) - this app only reads
the id back, it never registers or changes the schema.
"""
import requests

import config


def get_latest_schema_id(subject: str) -> int:
    """Returns the schema id of the latest version registered for `subject`."""
    response = requests.get(
        f"{config.SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest",
        auth=(config.SCHEMA_REGISTRY_API_KEY, config.SCHEMA_REGISTRY_API_SECRET),
        timeout=10,
    )
    response.raise_for_status()
    return response.json()["id"]
