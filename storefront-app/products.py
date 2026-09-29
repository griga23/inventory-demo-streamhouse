"""Demo product catalog shown on the storefront product page. Must match the SKUs
seeded across flink-sql/02*_item_added_to_stock_seed*.sql (each new size/variant was
added as its own additive seed file - see those files for why)."""

CATALOG = [
    {"sku": "NIKE-AIRMAX-BLK-42", "brand": "Nike", "model": "Air Max", "color": "Black", "size": "42",
     "image": "nike-airmax-blk.svg"},
    {"sku": "NIKE-AIRMAX-BLK-43", "brand": "Nike", "model": "Air Max", "color": "Black", "size": "43",
     "image": "nike-airmax-blk.svg"},
    {"sku": "HOKA-CLIFTON-WHT-42", "brand": "Hoka", "model": "Clifton", "color": "White", "size": "42",
     "image": "hoka-clifton-wht.svg"},
    {"sku": "HOKA-CLIFTON-WHT-43", "brand": "Hoka", "model": "Clifton", "color": "White", "size": "43",
     "image": "hoka-clifton-wht.svg"},
    {"sku": "ADIDAS-ULTRABOOST-BLK-41", "brand": "Adidas", "model": "Ultraboost", "color": "Black", "size": "41",
     "image": "adidas-ultraboost-blk.svg"},
    {"sku": "ADIDAS-ULTRABOOST-BLK-42", "brand": "Adidas", "model": "Ultraboost", "color": "Black", "size": "42",
     "image": "adidas-ultraboost-blk.svg"},
]

BY_SKU = {p["sku"]: p for p in CATALOG}


def display_name(product: dict) -> str:
    return f"{product['brand']} {product['model']}, {product['color']}, size {product['size']}"


def families() -> list[dict]:
    """Groups CATALOG by (brand, model, color) into one storefront card per variant,
    each listing its available sizes -> SKU. A variant with multiple sizes renders as
    a single card with a size-selector toggle instead of one card per size; a
    single-size variant renders with just its size shown as plain text."""
    grouped: dict[tuple[str, str, str], dict] = {}
    order: list[tuple[str, str, str]] = []
    for p in CATALOG:
        key = (p["brand"], p["model"], p["color"])
        if key not in grouped:
            grouped[key] = {
                "brand": p["brand"],
                "model": p["model"],
                "color": p["color"],
                "image": p["image"],
                "sizes": [],
            }
            order.append(key)
        grouped[key]["sizes"].append({"sku": p["sku"], "size": p["size"]})
    return [grouped[key] for key in order]
