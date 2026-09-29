-- CUT 3/4: reduce read latency. Flink's sink writes to current_inventory
-- transactionally (checkpoint-committed), and the default consumer isolation level
-- (read-committed) makes readers - including RTCE's own internal consumer - wait
-- until the next checkpoint commits before seeing new rows. read-uncommitted drops
-- that wait, trading strict transactional isolation (a reader could in principle see
-- a value from a checkpoint that later aborts) for materially lower observed latency,
-- which is the right tradeoff for this demo's "watch it update live" narrative.
ALTER TABLE current_inventory SET ('kafka.consumer.isolation-level' = 'read-uncommitted');
