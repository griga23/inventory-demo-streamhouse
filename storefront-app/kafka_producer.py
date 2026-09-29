"""CUT 1 (purchase side) / interactive checkout: publishes `item purchased` events to
Kafka when a customer clicks Buy on the storefront.

The item_purchased topic and its JSON schema are provisioned by Terraform (see
terraform/topics.tf) - this module only looks up the existing schema id and produces
against it, it never creates the topic or registers a schema itself.

Uses the pure-Python `kafka-python-ng` client plus a hand-rolled Confluent wire-format
encoder (magic byte + 4-byte schema id + JSON payload), so there's no native
dependency (librdkafka) to install to run this demo.
"""
import json
import struct
import uuid

from kafka import KafkaProducer

import config
import schema_registry

MAGIC_BYTE = b"\x00"

_producer = None
_schema_id = None


def _client():
    global _producer, _schema_id
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=config.BOOTSTRAP_SERVERS,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=config.KAFKA_API_KEY,
            sasl_plain_password=config.KAFKA_API_SECRET,
            key_serializer=lambda k: k.encode("utf-8"),
        )
        subject = f"{config.TOPIC_ITEM_PURCHASED}-value"
        _schema_id = schema_registry.get_latest_schema_id(subject)
    return _producer, _schema_id


def _encode(value: dict, schema_id: int) -> bytes:
    return MAGIC_BYTE + struct.pack(">I", schema_id) + json.dumps(value).encode("utf-8")


def publish_purchase(sku: str, quantity: int, order_id: str | None = None) -> None:
    """CUT 1: publish one `item purchased` event and block until it's acked."""
    producer, schema_id = _client()
    value = {"sku": sku, "quantity": quantity, "order_id": order_id or str(uuid.uuid4())}
    future = producer.send(config.TOPIC_ITEM_PURCHASED, key=sku, value=_encode(value, schema_id))
    future.get(timeout=10)
