terraform {
  required_version = ">= 1.5.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

# Reads CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET from the environment.
# These are the org-level (Cloud) API key/secret for the account running `confluent
# login` / used elsewhere in this demo - not the resource-scoped keys this module
# creates below.
provider "confluent" {}
