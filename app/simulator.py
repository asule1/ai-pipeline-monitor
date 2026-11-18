"""Simulate production ETL workflows reporting to the monitor API.

    python -m app.simulator --api http://localhost:8000 --days 7

Generates historical runs for several pipelines with realistic failures, slowdowns,
row-count drops, and one pipeline that goes stale.
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone

import httpx

PIPELINES = [
    # name, owner, schedule, runs/day, sla_min, base_duration_s, base_rows
    ("orders_ingest", "data-eng", "hourly", 24, 120, 180, 40_000),
    ("customer_sync", "data-eng", "every 4h", 6, 360, 420, 8_000),
    ("inventory_snapshot", "supply-chain", "daily", 1, 1500, 900, 250_000),
    ("payments_reconciliation", "finance-data", "every 6h", 4, 480, 600, 15_000),
    ("marketing_attribution", "analytics", "daily", 1, 1500, 1500, 1_200_000),
]

FAILURES = [
    'psycopg2.errors.UndefinedColumn: column "customer_tier" does not exist',
    "Query timed out after 1800 seconds",
    "could not connect to server: Connection refused (host=warehouse-replica:5432)",
    "google.api_core.exceptions.Forbidden: 403 Access Denied: Table analytics.events",
    'duplicate key value violates unique constraint "fact_payments_pkey"',
    "MemoryError: Unable to allocate 6.2 GiB for an array",
    "FileNotFoundError: s3://raw-landing/orders/dt=2024-01-01/ not found",
]


def simulate(api: str, days: int, seed: int | None = None) -> None:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    with httpx.Client(base_url=api, timeout=30) as c:
        for name, owner, schedule, per_day, sla, *_ in PIPELINES:
            r = c.post("/api/pipelines", json={"name": name, "owner": owner, "schedule": schedule,
                                               "freshness_sla_minutes": sla})
            if r.status_code not in (201, 409):
                r.raise_for_status()

        events = []
        for name, _, _, per_day, _, base_dur, base_rows in PIPELINES:
            interval = timedelta(hours=24 / per_day)
            t = start + timedelta(minutes=rng.randint(0, 30))
            # marketing_attribution stops running ~2 days ago (stale pipeline)
            stop = now - timedelta(days=2) if name == "marketing_attribution" else now
            while t < stop - timedelta(seconds=base_dur * 2):
                events.append((t, name, base_dur, base_rows))
                t += interval
        events.sort()

        for t, name, base_dur, base_rows in events:
            duration = rng.gauss(base_dur, base_dur * 0.08)
            rows = int(rng.gauss(base_rows, base_rows * 0.05))
            status, error = "success", None
            roll = rng.random()
            if roll < 0.06:
                status, error = "failed", rng.choice(FAILURES)
                duration *= rng.uniform(0.1, 0.6)
                rows = None
            elif roll < 0.09:
                duration *= rng.uniform(3, 5)  # slowdown
            elif roll < 0.11:
                rows = int(rows * rng.uniform(0.05, 0.3))  # partial upstream data

            run = c.post("/api/runs", json={"pipeline": name, "started_at": t.isoformat()}).json()
            c.post(f"/api/runs/{run['id']}/complete", json={
                "status": status,
                "finished_at": (t + timedelta(seconds=max(duration, 5))).isoformat(),
                "rows_processed": rows,
                "error_message": error,
            }).raise_for_status()

    print(f"Simulated {len(events)} runs across {len(PIPELINES)} pipelines.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    simulate(args.api, args.days, args.seed)


if __name__ == "__main__":
    main()
