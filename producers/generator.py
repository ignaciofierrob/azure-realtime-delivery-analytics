"""
Real-Time Food Delivery Event Generator
========================================
Generates two streaming data feeds:
  1. Order Lifecycle Events  — tracks every order state transition
  2. Courier Status Events   — tracks courier location & availability

Edge cases injected (all configurable):
  - Out-of-order / late events
  - Duplicate events
  - Missing steps (e.g. delivered without picked_up)
  - Impossible durations (anomaly detection targets)
  - Courier goes offline mid-delivery

Two modes
---------
  batch   — Write JSON + AVRO sample files to disk (sample data)
  stream  — Publish continuously to Azure Event Hubs (live pipeline)

Usage
-----
# Batch mode (default — regenerates sample_data/)
python generator.py batch

# Stream orders feed to Event Hubs
python generator.py stream \\
    --namespace iesstsabbadbaa-grp-06-10 \\
    --eventhub  group_10_orders \\
    --conn-str  "Endpoint=sb://...;SharedAccessKeyName=Producer;SharedAccessKey=..." \\
    --feed      orders \\
    --speedup   60

# Stream couriers feed (run in parallel with the above)
python generator.py stream \\
    --namespace iesstsabbadbaa-grp-06-10 \\
    --eventhub  group_10_couriers \\
    --conn-str  "Endpoint=sb://...;SharedAccessKeyName=Producer;SharedAccessKey=..." \\
    --feed      couriers \\
    --speedup   60
"""

import argparse
import copy
import io
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

# ─── Optional deps ────────────────────────────────────────────────────────────
try:
    import fastavro
    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False
    print("[WARN] fastavro not installed — AVRO output will be skipped.")
    print("       Install with: pip install fastavro")

# confluent-kafka is imported lazily inside stream mode only

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

CONFIG = {
    # Entity counts
    "num_restaurants": 30,
    "num_couriers": 50,
    "num_zones": 6,
    "num_customers": 200,

    # How many events to generate
    "num_orders": 200,
    "courier_ping_interval_sec": 30,
    "simulation_duration_hours": 4,

    # Edge case probabilities (0.0 – 1.0)
    "prob_late_event": 0.08,
    "prob_duplicate_event": 0.04,
    "prob_missing_step": 0.05,
    "prob_impossible_duration": 0.03,
    "prob_courier_offline_mid": 0.04,

    # Demand shaping
    "lunch_peak_hour": 12,
    "dinner_peak_hour": 19,
    "peak_multiplier": 3.0,
    "weekend_multiplier": 1.5,
    "weekend_late_night_boost": 2.0,
    "promo_probability": 0.20,

    # Cancellation & refund
    "cancel_probability": 0.10,
    "refund_probability": 0.08,

    # Zone-level demand skew (relative weights)
    "zone_demand_weights": {
        "zone_north":   0.12,
        "zone_south":   0.14,
        "zone_centre":  0.28,
        "zone_east":    0.22,
        "zone_west":    0.14,
        "zone_suburbs": 0.10,
    },

    # Output paths (batch mode)
    "output_dir": "sample_data",
}

# ══════════════════════════════════════════════════════════════════════════════
# AVRO SCHEMAS
# ══════════════════════════════════════════════════════════════════════════════

ORDER_AVRO_SCHEMA = {
    "type": "record",
    "name": "OrderEvent",
    "namespace": "com.fooddelivery.events.v1",
    "doc": "Represents a single state transition in the order lifecycle.",
    "fields": [
        {"name": "event_id",       "type": "string",  "doc": "Unique event UUID"},
        {"name": "order_id",       "type": "string",  "doc": "Stable order UUID"},
        {"name": "customer_id",    "type": "string"},
        {"name": "restaurant_id",  "type": "string"},
        {"name": "courier_id",     "type": ["null", "string"], "default": None,
         "doc": "Null until a courier is assigned"},
        {"name": "zone_id",        "type": "string"},
        {
            "name": "event_type",
            "type": {
                "type": "enum",
                "name": "OrderEventType",
                "symbols": [
                    "ORDER_PLACED", "ORDER_ACCEPTED", "PREPARING",
                    "READY_FOR_PICKUP", "PICKED_UP", "DELIVERED",
                    "CANCELLED", "REFUND_ISSUED"
                ]
            }
        },
        {"name": "event_time",         "type": "string",  "doc": "ISO-8601 UTC timestamp"},
        {"name": "ingestion_time",     "type": "string",  "doc": "When the platform received the event"},
        {"name": "order_value",        "type": "float",   "doc": "Total order value in EUR"},
        {"name": "promo_code",         "type": ["null", "string"], "default": None},
        {"name": "promo_discount_pct", "type": ["null", "float"],  "default": None},
        {"name": "estimated_prep_min", "type": "int",     "doc": "Restaurant-estimated prep time (minutes)"},
        {"name": "is_late_event",      "type": "boolean", "default": False,
         "doc": "True if this event was intentionally delayed (edge-case injection)"},
        {"name": "is_duplicate",       "type": "boolean", "default": False},
        {"name": "schema_version",     "type": "string",  "default": "1.0"},
    ]
}

COURIER_AVRO_SCHEMA = {
    "type": "record",
    "name": "CourierEvent",
    "namespace": "com.fooddelivery.events.v1",
    "doc": "Periodic courier location and status ping.",
    "fields": [
        {"name": "event_id",    "type": "string"},
        {"name": "courier_id",  "type": "string"},
        {"name": "order_id",    "type": ["null", "string"], "default": None,
         "doc": "Non-null when courier is assigned to an order"},
        {"name": "zone_id",     "type": "string"},
        {
            "name": "status",
            "type": {
                "type": "enum",
                "name": "CourierStatus",
                "symbols": [
                    "AVAILABLE", "ASSIGNED", "EN_ROUTE_PICKUP",
                    "EN_ROUTE_DELIVERY", "OFFLINE"
                ]
            }
        },
        {"name": "event_time",    "type": "string"},
        {"name": "ingestion_time","type": "string",  "default": ""},
        {"name": "latitude",      "type": "float"},
        {"name": "longitude",     "type": "float"},
        {"name": "speed_kmh",     "type": "float",  "doc": "Reported GPS speed"},
        {"name": "battery_pct",   "type": "int",    "doc": "Device battery 0–100"},
        {"name": "is_late_event", "type": "boolean","default": False},
        {"name": "is_duplicate",  "type": "boolean","default": False},
        {"name": "schema_version","type": "string", "default": "1.0"},
    ]
}

# ══════════════════════════════════════════════════════════════════════════════
# REFERENCE DATA BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

ZONE_BOUNDS = {
    "zone_north":      [41.42, 41.46, 2.14, 2.20],
    "zone_south":      [41.35, 41.39, 2.14, 2.20],
    "zone_centre":     [41.38, 41.42, 2.16, 2.19],
    "zone_east":       [41.38, 41.42, 2.20, 2.25],
    "zone_west":       [41.38, 41.42, 2.10, 2.15],
    "zone_suburbs":    [41.30, 41.36, 2.08, 2.14],
}
ZONES = list(ZONE_BOUNDS.keys())


def weighted_zone() -> str:
    zones   = list(CONFIG["zone_demand_weights"].keys())
    weights = list(CONFIG["zone_demand_weights"].values())
    return random.choices(zones, weights=weights, k=1)[0]


def build_restaurants(n: int) -> list[dict]:
    cuisines = ["Italian", "Japanese", "Mexican", "Indian", "American",
                "Thai", "Chinese", "Mediterranean", "Vegan", "Burgers"]
    restaurants = []
    for i in range(n):
        zone   = weighted_zone()
        bounds = ZONE_BOUNDS[zone]
        restaurants.append({
            "restaurant_id": f"rest_{i:04d}",
            "name": f"Restaurant {i}",
            "zone_id": zone,
            "cuisine": random.choice(cuisines),
            "avg_prep_min": random.randint(8, 35),
            "lat": random.uniform(bounds[0], bounds[1]),
            "lon": random.uniform(bounds[2], bounds[3]),
        })
    return restaurants


def build_couriers(n: int) -> list[dict]:
    couriers = []
    for i in range(n):
        zone = weighted_zone()
        bounds = ZONE_BOUNDS[zone]
        couriers.append({
            "courier_id": f"courier_{i:04d}",
            "home_zone": zone,
            "lat": random.uniform(bounds[0], bounds[1]),
            "lon": random.uniform(bounds[2], bounds[3]),
            "battery": random.randint(40, 100),
        })
    return couriers


def build_customers(n: int) -> list[str]:
    return [f"cust_{i:05d}" for i in range(n)]

# ══════════════════════════════════════════════════════════════════════════════
# TIME HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def demand_weight(t: datetime) -> float:
    hour        = t.hour
    weekday     = t.weekday()
    is_weekend  = weekday >= 5
    is_fri_sat  = weekday in (4, 5)

    lunch_start  = CONFIG["lunch_peak_hour"]
    dinner_start = CONFIG["dinner_peak_hour"]

    if lunch_start <= hour < lunch_start + 2:
        hour_w = CONFIG["peak_multiplier"]
    elif dinner_start <= hour < dinner_start + 2:
        hour_w = CONFIG["peak_multiplier"] * 1.2
    elif 8 <= hour < 11 or 14 <= hour < 18:
        hour_w = 1.4
    elif 0 <= hour < 6:
        hour_w = 0.2
    else:
        hour_w = 1.0

    if is_weekend:
        hour_w *= CONFIG["weekend_multiplier"]
    if is_fri_sat and (hour >= 22 or hour < 2):
        hour_w *= CONFIG["weekend_late_night_boost"]

    return hour_w


def weighted_random_time(sim_start: datetime, sim_end: datetime) -> datetime:
    max_weight = CONFIG["peak_multiplier"] * 1.2 * CONFIG["weekend_multiplier"] * CONFIG["weekend_late_night_boost"]
    for _ in range(100):
        t = sim_start + timedelta(
            seconds=random.uniform(0, (sim_end - sim_start).total_seconds())
        )
        w = demand_weight(t)
        if random.random() < w / max_weight:
            return t
    return sim_start + timedelta(
        seconds=random.uniform(0, (sim_end - sim_start).total_seconds())
    )


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def now_iso() -> str:
    return iso(datetime.now(timezone.utc))

# ══════════════════════════════════════════════════════════════════════════════
# EDGE CASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def maybe_late(event: dict, field: str = "event_time") -> dict:
    if random.random() < CONFIG["prob_late_event"]:
        original = datetime.fromisoformat(event[field].replace("Z", "+00:00"))
        # Cap late-event offset to 30 minutes — long enough to exercise watermarks,
        # short enough that records aren't discarded as pathological.
        delay = timedelta(minutes=random.randint(5, 30))
        event[field] = iso(original - delay)
        event["ingestion_time"] = now_iso()
        event["is_late_event"] = True
    return event


def maybe_duplicate(events: list[dict], event: dict) -> list[dict]:
    if random.random() < CONFIG["prob_duplicate_event"]:
        dup = copy.deepcopy(event)
        dup["event_id"] = str(uuid.uuid4())
        dup["is_duplicate"] = True
        events.append(dup)
    return events


def courier_rand_position(courier: dict) -> tuple[float, float]:
    bounds = ZONE_BOUNDS[courier["home_zone"]]
    return (
        round(random.uniform(bounds[0], bounds[1]), 6),
        round(random.uniform(bounds[2], bounds[3]), 6),
    )

# ══════════════════════════════════════════════════════════════════════════════
# FEED 1: ORDER LIFECYCLE EVENTS
# ══════════════════════════════════════════════════════════════════════════════

PROMO_CODES = ["SAVE10", "WELCOME15", "FLASH20", "LOYAL5", "SUMMER25"]

def generate_order_events(
    restaurants: list[dict],
    couriers: list[dict],
    customers: list[str],
    sim_start: datetime,
    sim_end: datetime,
) -> list[dict]:

    events = []
    n_orders = CONFIG["num_orders"]

    for _ in range(n_orders):
        order_id   = str(uuid.uuid4())
        restaurant = random.choice(restaurants)
        courier    = random.choice(couriers)
        customer   = random.choice(customers)
        zone_id    = restaurant["zone_id"]

        placed_at  = weighted_random_time(sim_start, sim_end)
        prep_min   = restaurant["avg_prep_min"] + random.randint(-3, 8)
        order_val  = round(random.uniform(8.0, 65.0), 2)

        promo_code     = None
        promo_discount = None
        if random.random() < CONFIG["promo_probability"]:
            promo_code     = random.choice(PROMO_CODES)
            promo_discount = round(random.uniform(5.0, 25.0), 1)

        def make_event(etype: str, t: datetime, courier_id=None) -> dict:
            return {
                "event_id":         str(uuid.uuid4()),
                "order_id":         order_id,
                "customer_id":      customer,
                "restaurant_id":    restaurant["restaurant_id"],
                "courier_id":       courier_id,
                "zone_id":          zone_id,
                "event_type":       etype,
                "event_time":       iso(t),
                "ingestion_time":   iso(t + timedelta(seconds=random.uniform(0.1, 2.0))),
                "order_value":      order_val,
                "promo_code":       promo_code,
                "promo_discount_pct": promo_discount,
                "estimated_prep_min": prep_min,
                "is_late_event":    False,
                "is_duplicate":     False,
                "schema_version":   "1.0",
            }

        t_placed   = placed_at
        t_accepted = t_placed + timedelta(seconds=random.randint(10, 90))
        t_prep     = t_accepted + timedelta(seconds=random.randint(5, 20))
        t_ready    = t_prep + timedelta(minutes=prep_min)

        if random.random() < CONFIG["prob_impossible_duration"]:
            if random.random() < 0.5:
                t_ready = t_prep + timedelta(seconds=random.randint(10, 55))
            else:
                t_ready = t_prep + timedelta(hours=random.randint(3, 6))

        if random.random() < CONFIG["cancel_probability"]:
            order_events = [
                make_event("ORDER_PLACED",   t_placed),
                make_event("ORDER_ACCEPTED", t_accepted),
                make_event("CANCELLED",      t_accepted + timedelta(minutes=random.randint(1, 10))),
            ]
            for e in order_events:
                events.append(maybe_late(e))
                events = maybe_duplicate(events, e)
            continue

        t_pickup    = t_ready + timedelta(minutes=random.randint(2, 15))
        t_delivered = t_pickup + timedelta(minutes=random.randint(5, 40))

        order_events = [
            make_event("ORDER_PLACED",     t_placed),
            make_event("ORDER_ACCEPTED",   t_accepted),
            make_event("PREPARING",        t_prep),
            make_event("READY_FOR_PICKUP", t_ready),
        ]

        skip_pickup = random.random() < CONFIG["prob_missing_step"]
        if not skip_pickup:
            order_events.append(make_event("PICKED_UP", t_pickup, courier["courier_id"]))

        courier_offline = random.random() < CONFIG["prob_courier_offline_mid"]
        if not courier_offline:
            order_events.append(make_event("DELIVERED", t_delivered, courier["courier_id"]))
            if random.random() < CONFIG["refund_probability"]:
                t_refund = t_delivered + timedelta(minutes=random.randint(5, 60))
                order_events.append(make_event("REFUND_ISSUED", t_refund, courier["courier_id"]))

        for e in order_events:
            events.append(maybe_late(e))
            events = maybe_duplicate(events, e)

    random.shuffle(events)
    return events

# ══════════════════════════════════════════════════════════════════════════════
# FEED 2: COURIER LOCATION / STATUS EVENTS
# ══════════════════════════════════════════════════════════════════════════════

def generate_courier_events(
    couriers: list[dict],
    sim_start: datetime,
    sim_end: datetime,
) -> list[dict]:

    events      = []
    ping_sec    = CONFIG["courier_ping_interval_sec"]
    total_sec   = int((sim_end - sim_start).total_seconds())
    num_pings   = total_sec // ping_sec

    for courier in couriers:
        status    = "AVAILABLE"
        order_id  = None
        lat, lon  = courier_rand_position(courier)
        battery   = courier["battery"]

        for i in range(num_pings):
            t = sim_start + timedelta(seconds=i * ping_sec)

            if random.random() < 0.005:
                status   = "OFFLINE"
                order_id = None
            if status == "OFFLINE" and random.random() < 0.3:
                status = "AVAILABLE"
            if status in ("EN_ROUTE_PICKUP", "EN_ROUTE_DELIVERY"):
                if random.random() < CONFIG["prob_courier_offline_mid"] / num_pings:
                    status   = "OFFLINE"
                    order_id = None

            if status == "AVAILABLE" and random.random() < 0.04:
                status   = "ASSIGNED"
                order_id = str(uuid.uuid4())
            elif status == "ASSIGNED" and random.random() < 0.4:
                status   = "EN_ROUTE_PICKUP"
            elif status == "EN_ROUTE_PICKUP" and random.random() < 0.3:
                status   = "EN_ROUTE_DELIVERY"
            elif status == "EN_ROUTE_DELIVERY" and random.random() < 0.2:
                status   = "AVAILABLE"
                order_id = None

            lat += random.uniform(-0.001, 0.001)
            lon += random.uniform(-0.001, 0.001)
            battery = max(0, battery - random.randint(0, 1))

            event = {
                "event_id":       str(uuid.uuid4()),
                "courier_id":     courier["courier_id"],
                "order_id":       order_id,
                "zone_id":        courier["home_zone"],
                "status":         status,
                "event_time":     iso(t),
                "ingestion_time": iso(t + timedelta(seconds=random.uniform(0.1, 2.0))),
                "latitude":       round(lat, 6),
                "longitude":      round(lon, 6),
                "speed_kmh":      round(random.uniform(0, 35), 1) if status != "OFFLINE" else 0.0,
                "battery_pct":    battery,
                "is_late_event":  False,
                "is_duplicate":   False,
                "schema_version": "1.0",
            }

            event = maybe_late(event, "event_time")
            events = maybe_duplicate(events, event)
            events.append(event)

    random.shuffle(events)
    return events

# ══════════════════════════════════════════════════════════════════════════════
# BATCH OUTPUT WRITERS
# ══════════════════════════════════════════════════════════════════════════════

def write_json(events: list[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"  ✓ JSON  → {path}  ({len(events)} events)")


def write_avro(events: list[dict], schema: dict, path: str) -> None:
    if not AVRO_AVAILABLE:
        print(f"  ⚠  AVRO skipped (fastavro not installed): {path}")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    parsed = fastavro.parse_schema(schema)
    with open(path, "wb") as f:
        fastavro.writer(f, parsed, events)
    print(f"  ✓ AVRO  → {path}  ({len(events)} events)")


def write_schema(schema: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"  ✓ Schema → {path}")


# ══════════════════════════════════════════════════════════════════════════════
# MODE: BATCH (write JSON + Avro samples to disk)
# ══════════════════════════════════════════════════════════════════════════════

def run_batch() -> None:
    print("\n🍕  Food Delivery Event Generator — BATCH MODE")
    print("=" * 50)

    sim_start = datetime(2024, 6, 14, 10, 0, 0, tzinfo=timezone.utc)  # Friday 10:00
    sim_end   = sim_start + timedelta(hours=CONFIG["simulation_duration_hours"])
    print(f"   Simulation: {iso(sim_start)}  →  {iso(sim_end)}")

    random.seed(42)
    restaurants = build_restaurants(CONFIG["num_restaurants"])
    couriers    = build_couriers(CONFIG["num_couriers"])
    customers   = build_customers(CONFIG["num_customers"])

    print(f"   Restaurants : {len(restaurants)}")
    print(f"   Couriers    : {len(couriers)}")
    print(f"   Customers   : {len(customers)}")
    print(f"   Zones       : {ZONES}")

    print("\n⚙  Generating events …")
    order_events   = generate_order_events(restaurants, couriers, customers, sim_start, sim_end)
    courier_events = generate_courier_events(couriers, sim_start, sim_end)

    late_orders   = sum(1 for e in order_events   if e["is_late_event"])
    dup_orders    = sum(1 for e in order_events    if e["is_duplicate"])
    late_couriers = sum(1 for e in courier_events  if e["is_late_event"])
    dup_couriers  = sum(1 for e in courier_events  if e["is_duplicate"])

    print(f"   Order events   : {len(order_events):>6}  (late: {late_orders}, dups: {dup_orders})")
    print(f"   Courier events : {len(courier_events):>6}  (late: {late_couriers}, dups: {dup_couriers})")

    out = CONFIG["output_dir"]
    print(f"\n💾  Writing output to ./{out}/")
    write_schema(ORDER_AVRO_SCHEMA,   f"{out}/schemas/order_event.avsc")
    write_schema(COURIER_AVRO_SCHEMA, f"{out}/schemas/courier_event.avsc")
    write_json(order_events,   f"{out}/json/order_events.jsonl")
    write_json(courier_events, f"{out}/json/courier_events.jsonl")
    write_avro(order_events,   ORDER_AVRO_SCHEMA,   f"{out}/avro/order_events.avro")
    write_avro(courier_events, COURIER_AVRO_SCHEMA, f"{out}/avro/courier_events.avro")

    print("\n✅  Done!\n")


# ══════════════════════════════════════════════════════════════════════════════
# MODE: STREAM (publish continuously to Azure Event Hubs)
# ══════════════════════════════════════════════════════════════════════════════

def _avro_encode(record: dict, parsed_schema) -> bytes:
    """Serialize one record to Avro bytes (schemaless — matches Spark's from_avro)."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def _build_producer(namespace: str, conn_str: str):
    """Event Hubs Kafka-API producer configuration."""
    from confluent_kafka import Producer
    return Producer({
        "bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "sasl.username":     "$ConnectionString",
        "sasl.password":     conn_str,
        "client.id":         "food-delivery-producer",
        "linger.ms":         50,
        # Note: Event Hubs' Kafka endpoint does not accept client-side compression
        # on the Basic tier — leave compression off.
    })


def _delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[ERR] Delivery failed: {err}", file=sys.stderr)


def _generate_feed(feed: str) -> list[dict]:
    """Generate a full set of events for the requested feed and sort chronologically."""
    cfg = CONFIG
    restaurants = build_restaurants(cfg["num_restaurants"])
    couriers    = build_couriers(cfg["num_couriers"])
    customers   = build_customers(cfg["num_customers"])

    sim_start = datetime.now(timezone.utc).replace(microsecond=0)
    sim_end   = sim_start + timedelta(hours=cfg["simulation_duration_hours"])

    if feed == "orders":
        events = generate_order_events(restaurants, couriers, customers, sim_start, sim_end)
    elif feed == "couriers":
        events = generate_courier_events(couriers, sim_start, sim_end)
    else:
        raise ValueError(f"Unknown feed: {feed!r}")

    # Sort chronologically so we can replay against a simulated clock
    events.sort(key=lambda e: e["event_time"])
    return events


def run_stream(
    namespace: str,
    eventhub: str,
    conn_str: str,
    feed: str,
    speedup: float,
    max_events: Optional[int],
) -> None:

    if not AVRO_AVAILABLE:
        print("[FATAL] stream mode requires fastavro. Install with: pip install fastavro",
              file=sys.stderr)
        sys.exit(1)

    try:
        from confluent_kafka import Producer  # noqa: F401
    except ImportError:
        print("[FATAL] stream mode requires confluent-kafka. "
              "Install with: pip install confluent-kafka", file=sys.stderr)
        sys.exit(1)

    schema = fastavro.parse_schema(
        ORDER_AVRO_SCHEMA if feed == "orders" else COURIER_AVRO_SCHEMA
    )

    print(f"[INFO] Generating {feed} events ...")
    events = _generate_feed(feed)
    if max_events:
        events = events[:max_events]
    print(f"[INFO] {len(events):,} events ready to stream.")

    producer = _build_producer(namespace, conn_str)
    print(f"[INFO] Connected to {namespace}.servicebus.windows.net:9093")
    print(f"[INFO] Publishing to topic '{eventhub}' with speedup={speedup}x")

    # Anchor the real clock against the first event's simulated time
    sim_anchor  = datetime.fromisoformat(events[0]["event_time"].replace("Z", "+00:00"))
    real_anchor = datetime.now(timezone.utc)

    sent = 0
    for ev in events:
        ev_time   = datetime.fromisoformat(ev["event_time"].replace("Z", "+00:00"))
        sim_delta = (ev_time - sim_anchor).total_seconds() / speedup
        target    = real_anchor + timedelta(seconds=sim_delta)
        wait      = (target - datetime.now(timezone.utc)).total_seconds()
        if wait > 0:
            time.sleep(wait)

        try:
            payload = _avro_encode(ev, schema)
            producer.produce(
                topic=eventhub,
                value=payload,
                key=ev["zone_id"].encode("utf-8"),  # partition key
                on_delivery=_delivery_report,
            )
        except BufferError:
            producer.poll(1.0)
            producer.produce(
                topic=eventhub,
                value=_avro_encode(ev, schema),
                key=ev["zone_id"].encode("utf-8"),
                on_delivery=_delivery_report,
            )

        producer.poll(0)
        sent += 1
        if sent % 100 == 0:
            print(f"[INFO] Sent {sent:,} / {len(events):,} events")

    print("[INFO] Flushing final batch ...")
    producer.flush(timeout=30)
    print(f"[DONE] {sent:,} events published to '{eventhub}'.")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="Food Delivery event generator — batch files or stream to Event Hubs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="mode", required=False)

    sub.add_parser("batch", help="Write JSON + AVRO samples to ./sample_data/")

    s = sub.add_parser("stream", help="Publish continuously to Azure Event Hubs")
    s.add_argument("--namespace", required=True,
                   help="e.g. iesstsabbadbaa-grp-06-10")
    s.add_argument("--eventhub", required=True,
                   help="e.g. group_10_orders")
    s.add_argument("--conn-str", required=True,
                   help="Producer connection string (namespace- or hub-level)")
    s.add_argument("--feed", choices=["orders", "couriers"], required=True)
    s.add_argument("--speedup", type=float, default=60.0,
                   help="Simulated seconds per real second (default: 60)")
    s.add_argument("--max-events", type=int, default=None,
                   help="Optional cap on events sent (for quick testing)")

    return parser.parse_args()


def main():
    args = parse_args()

    # Default to batch mode when no subcommand is given — preserves M1 behavior
    if args.mode is None or args.mode == "batch":
        run_batch()
        return

    if args.mode == "stream":
        run_stream(
            namespace=args.namespace,
            eventhub=args.eventhub,
            conn_str=args.conn_str,
            feed=args.feed,
            speedup=args.speedup,
            max_events=args.max_events,
        )
        return


if __name__ == "__main__":
    main()
