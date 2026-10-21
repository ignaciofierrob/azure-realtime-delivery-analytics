# Event Hub Topic Design

This document specifies the Azure Event Hubs topic design for the
real-time food delivery analytics pipeline. It covers the rationale for two
topics, partition strategy, message keying, sizing, throughput, and consumer
group strategy.

---

## 1. Topic inventory

| Topic (Event Hub) | Purpose | Producer | Schema |
|---|---|---|---|
| `group_10_orders`   | Order lifecycle events (every state transition of every order)        | `generator.py stream --feed orders`   | `OrderEvent` (Avro)   |
| `group_10_couriers` | Courier status pings (location + state every 30 s per courier)        | `generator.py stream --feed couriers` | `CourierEvent` (Avro) |

**Namespace:** `iesstsabbadbaa-grp-06-10` (provisioned by the course; names are fixed by the instructor)

### Why two topics, not one

I split the marketplace's two distinct event streams into separate topics
rather than multiplexing them into a single one. Reasons:

- **Different schemas and lifecycle semantics.** Orders are state-machine
  transitions; courier pings are periodic samples. Mixing them would force a
  union schema that is harder to evolve.
- **Different volume and partitioning requirements.** Couriers produce ~25 k
  events per 4-hour simulation (one ping per courier every 30 s); orders
  produce ~1.2 k. Co-locating them would force one topic's partitioning
  strategy on the other.
- **Independent consumer scaling.** Analytics jobs can consume each topic at
  its own pace and scale partitions independently.
- **Cleaner stream-stream joins downstream.** Spark joins two named streams
  more naturally than a single mixed stream that has to be filter-split first.

A third topic (e.g. payments or reviews) was considered but deemed
non-essential for this version. It would be added in a future iteration as
either a third Event Hub or, more likely, a slowly-changing reference table
joined into the streams.

---

## 2. Partition strategy

| Setting       | `group_10_orders` | `group_10_couriers` |
|---|---|---|
| Partition count | 4 | 4 |
| Cleanup policy  | Delete | Delete |
| Retention       | 24 hours | 24 hours |

### Why 4 partitions per topic

- **Parallelism for Spark consumers.** With 4 partitions, Spark Structured
  Streaming can run up to 4 reader tasks per topic concurrently, matching the
  default `spark.sql.shuffle.partitions = 4` in the notebooks.
- **Fits within shared-namespace TU budget.** The namespace is shared between
  several course projects, so I deliberately did not over-provision partitions. 4 keeps
  the throughput envelope well below the namespace TU ceiling.
- **Roughly aligned with the data's natural grain.** There are 6 city zones; 4
  partitions hash zones into a balanced spread without one-zone-per-partition
  micro-management.

### Why 24-hour retention

- Event Hubs is not used as long-term storage. The bronze Parquet sink in
  Blob Storage handles persistence; Event Hubs is purely the in-flight
  message bus.
- 24 hours is enough buffer to recover from a Spark job restart or failure
  without re-publishing data.
- The shared namespace policy is to keep retention low to limit storage
  cost.

---

## 3. Message keying

All messages use **`zone_id` as the partition key**. The producer
(`generator.py`) sets the Kafka message key to the zone string before
publishing:

```python
producer.produce(
    topic=eventhub,
    value=avro_payload,
    key=event["zone_id"].encode("utf-8"),
    ...
)
```

### Why `zone_id`

Zone-level analytics are the single most common access pattern in this
project (orders per zone, supply/demand per zone, anomaly detection per
zone). Using `zone_id` as the partition key guarantees that all events for
the same zone land on the same partition, which means:

- **Per-zone state in Spark stays on a single executor.** No shuffle is
  required for zone-level windowed aggregates.
- **Ordering preserved within a zone.** Order lifecycle events for a single
  order (which always belong to a single zone) arrive in roughly the order
  they were produced.

### Tradeoff

One alternative I considered for the courier topic was keying on
`courier_id` to better isolate per-courier session-window state. I chose
`zone_id` for both topics for symmetry — it keeps cross-feed joins clean
since both topics have zone-aligned partitions. Courier session windows
still work correctly under `zone_id` keying because Spark's session-window
operator does its own per-courier grouping; partitioning only affects
parallelism and shuffle cost, not correctness.

---

## 4. Message size and throughput

### Message size

Avro-encoded payloads (no compression — Event Hubs Basic tier does not
accept client-side compression):

| Topic | Approx. payload size | Rationale |
|---|---|---|
| `group_10_orders`   | ~220 bytes | 16 fields, mostly short strings + a few floats |
| `group_10_couriers` | ~180 bytes | 15 fields, similar shape |

Both well below the 1 MB Event Hubs message limit.

### Expected throughput

At default generator config (4 simulated hours per producer run):

| Topic | Total events | Peak rate | Avg rate |
|---|---|---|---|
| `group_10_orders`   | ~1,200 events     | ~10 events/sec at lunch/dinner peaks | ~0.08 events/sec |
| `group_10_couriers` | ~25,000 events    | ~2 events/sec (50 couriers × 1 ping per 30 s) | ~1.7 events/sec |

In bytes:

| Topic | Total bytes | Peak | Avg |
|---|---|---|---|
| `group_10_orders`   | ~260 KB     | ~2.2 KB/sec | ~18 B/sec |
| `group_10_couriers` | ~4.5 MB     | ~360 B/sec | ~300 B/sec |

Both topics together fit comfortably inside a fraction of a single TU's
1 MB/s ingress allowance — the TU budget is not a binding constraint at
this scale.

The producer is configured to replay events at a 60× speedup against the
simulated clock, so a 4-hour simulation completes in ~4 real minutes,
producing the volumes above over that interval.

---

## 5. Consumer group strategy

Azure Event Hubs requires consumers to belong to a consumer group. Spark
Structured Streaming's Kafka source generates consumer group IDs
automatically, prefixed by the configured `groupIdPrefix`. I use distinct
prefixes per consumer to keep their offset state isolated.

| Consumer notebook | `groupIdPrefix`              | Reads from |
|---|---|---|
| `consumer_orders.ipynb`   | `FoodDelivery_Orders_Analytics`   | `group_10_orders` |
| `consumer_couriers.ipynb` | `FoodDelivery_Couriers_Analytics` | `group_10_couriers` |
| `consumer_blob.ipynb`     | `FoodDelivery_BlobSink_Orders` and `FoodDelivery_BlobSink_Couriers` | both |

### Why distinct prefixes per consumer

- **Independent offset progress.** The bronze sink can fall behind
  analytics queries (or vice versa) without one starving the other of
  events.
- **Reproducible re-reads during development.** Adding a timestamp suffix
  to the prefix (e.g. `FoodDelivery_Couriers_v3_<unix_ts>`) gives any new query
  a fresh group, so it reads from `earliest` regardless of what other
  queries have already consumed. This was essential during development to
  rerun individual analytics queries without having to restart producers.
- **No conflicts at the Azure Portal level.** Spark-managed groups are
  internal to Spark and don't appear as named consumer groups in the
  portal — but the offset commits still flow back to Event Hubs, so
  isolation matters.

### Starting offset policy

All consumers use `startingOffsets: earliest` in development so that
analytics and the blob sink can be restarted at any time and replay all
events still within the 24-hour retention window. For a production
deployment I would switch to `latest` once the system has been backfilled,
to avoid re-processing on every restart.

---

## 6. Operational notes

- **Producer compression:** Snappy compression is **not** supported on the
  Event Hubs Basic tier — initial tests returned
  `UNSUPPORTED_FOR_MESSAGE_FORMAT`. The producer runs uncompressed.
- **Partition key correctness:** Confirmed by inspecting the partition
  count on the namespace's incoming-messages chart — no partition
  hot-spotting observed despite uneven zone weights, because the four
  partitions absorb the six zones with acceptable balance.
- **Schema registry:** Not used. Schemas are embedded inline as JSON
  strings in each consumer notebook and as Python dicts in `generator.py`.
  In a production system I would publish the Avro schemas to Azure Schema
  Registry and have producer/consumer fetch them at runtime; for the
  course project the inline copy was sufficient.
