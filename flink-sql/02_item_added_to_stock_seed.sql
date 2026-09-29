-- CUT 1 (stock side): a fixed starting quantity per SKU, seeded once. This is a
-- bounded statement (a literal VALUES list, not a streaming source) - it runs to
-- completion and there's no standing job left behind.
--
-- This is the consolidated, canonical catalog (replacing an earlier version plus two
-- follow-on additive seed files created when Hoka Clifton White replaced Nike Air Max
-- White, and when second sizes were added for Hoka and Adidas). It's only safe to
-- consolidate like this immediately after a full topic reset - see
-- terraform/RESET.md. Once this has been applied against a live topic, do NOT edit
-- it again without a full reset: Terraform tracks this resource by its statement
-- text, so any edit forces a resubmit, and because Kafka is append-only that means
-- re-adding +20 to every SKU below on top of whatever real purchases have already
-- happened - add a new file + new confluent_flink_statement resource instead (see
-- git history around 02b/02c for the pattern).
INSERT INTO item_added_to_stock (sku, quantity, warehouse_id) VALUES
  ('NIKE-AIRMAX-BLK-42', 20, 'WH-1'),
  ('NIKE-AIRMAX-BLK-43', 20, 'WH-1'),
  ('HOKA-CLIFTON-WHT-42', 20, 'WH-1'),
  ('HOKA-CLIFTON-WHT-43', 20, 'WH-1'),
  ('ADIDAS-ULTRABOOST-BLK-41', 20, 'WH-1'),
  ('ADIDAS-ULTRABOOST-BLK-42', 20, 'WH-1');
