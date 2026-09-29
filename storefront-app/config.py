"""Loads Confluent Cloud connection details from config/local.env (untracked)."""
import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / "config" / "local.env"

load_dotenv(dotenv_path=ENV_FILE)


def require(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(
            f"Missing required config value '{key}'. Set it in {ENV_FILE} "
            f"(copy config/local.env.example if you haven't yet) or as an environment variable."
        )
    return value


def get(key: str, default: str = "") -> str:
    return os.environ.get(key) or default


ENV_ID = require("ENV_ID")
KAFKA_CLUSTER_ID = require("KAFKA_CLUSTER_ID")
CLOUD_PROVIDER = require("CLOUD_PROVIDER")
CLOUD_REGION = require("CLOUD_REGION")

BOOTSTRAP_SERVERS = require("BOOTSTRAP_SERVERS")
KAFKA_API_KEY = require("KAFKA_API_KEY")
KAFKA_API_SECRET = require("KAFKA_API_SECRET")

SCHEMA_REGISTRY_URL = require("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = require("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = require("SCHEMA_REGISTRY_API_SECRET")

TOPIC_ITEM_PURCHASED = get("TOPIC_ITEM_PURCHASED", "item_purchased")
TOPIC_CURRENT_INVENTORY = get("TOPIC_CURRENT_INVENTORY", "current_inventory")

LIGHTNING_API_KEY = require("LIGHTNING_API_KEY")
LIGHTNING_API_SECRET = require("LIGHTNING_API_SECRET")
