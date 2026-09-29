"""CUT 4: the transactional storefront. Renders the product page and exposes two
JSON endpoints: one that reads current stock via the Lightning Query API, and one
that publishes an `item purchased` event when a customer clicks Buy."""
import os

from flask import Flask, jsonify, render_template

import config
import kafka_producer
import lightning_client
import products

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html", families=products.families())


@app.route("/api/inventory/<sku>")
def get_inventory(sku):
    if sku not in products.BY_SKU:
        return jsonify(error="unknown sku"), 404
    try:
        quantity = lightning_client.current_quantity(sku)
    except lightning_client.InventoryQueryError as e:
        return jsonify(error=str(e)), 502
    return jsonify(sku=sku, quantity=quantity, in_stock=bool(quantity and quantity > 0))


@app.route("/api/buy/<sku>", methods=["POST"])
def buy(sku):
    if sku not in products.BY_SKU:
        return jsonify(error="unknown sku"), 404
    kafka_producer.publish_purchase(sku, quantity=1)
    return jsonify(ok=True)


if __name__ == "__main__":
    # Default changed from Flask's usual 5000: that port is claimed by macOS's
    # AirPlay Receiver on most Macs, so the dev server would silently fail to bind.
    port = int(os.environ.get("PORT", 5050))
    print(f"Confluent Cloud env={config.ENV_ID} cluster={config.KAFKA_CLUSTER_ID}")
    app.run(host="0.0.0.0", port=port, debug=True)
