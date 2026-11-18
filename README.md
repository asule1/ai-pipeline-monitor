# AI Data Pipeline Monitor

A central monitoring service for ETL pipelines. It tracks every run (status, runtime, row counts, data freshness), flags anomalous runs against each pipeline's own history, and uses an LLM to diagnose failures and recommend fixes.

![dashboard](docs/screenshot.png)

## What it does

- **Run tracking**: Pipelines report `start` / `complete` to a REST API. Runtime, rows processed, and errors are stored in PostgreSQL.
- **Anomaly detection**: Each successful run is compared to that pipeline's last 20 successful runs using a robust z-score (median/MAD), plus a minimum relative-change threshold. This catches slowdowns, row-count drops/spikes, and zero-row loads without being thrown off by past outliers.
- **Data freshness**: Each pipeline has a freshness SLA. Pipelines with no successful run inside the SLA are marked stale.
- **AI root cause analysis**: Failed or anomalous runs are sent (error message, metrics, recent run history) to the OpenAI API, which returns a structured diagnosis: category, root cause, recommended actions, and confidence. If no API key is set or the call fails, a rule-based classifier covers common failure types (schema drift, timeouts, connection, permissions, data quality, OOM, missing upstream data).
- **Dashboards**: A built-in operational dashboard at `/`, plus SQL reporting views and a CSV export for Power BI (see [`powerbi/`](powerbi/README.md)).
- **Simulated workloads**: `app/simulator.py` generates a week of realistic runs across 5 pipelines with injected failures, slowdowns, partial loads, and a stale pipeline.
- **Docker + CI**: Docker Compose runs Postgres, the API, and the simulator. GitHub Actions runs lint and tests on SQLite and PostgreSQL, then builds the image.

## Architecture

```
ETL jobs / simulator ──► FastAPI ──► PostgreSQL (pipelines, pipeline_runs, diagnoses)
                           │                 │
                           ├─ anomaly.py     └─ reporting views ──► Power BI
                           ├─ rca.py ──► OpenAI API (fallback: rules)
                           └─ dashboard (/)
```

| Path | Purpose |
|---|---|
| `app/main.py` | API routes, run completion flow, freshness, metrics, CSV export |
| `app/anomaly.py` | Robust z-score anomaly detection |
| `app/rca.py` | OpenAI root cause analysis + rule-based fallback |
| `app/db.py` | SQLAlchemy models |
| `app/simulator.py` | Simulated production workflows |
| `app/static/dashboard.html` | Operational dashboard |
| `sql/powerbi_views.sql` | Reporting views for Power BI |

## Running it

```bash
cp .env.example .env          # optionally add OPENAI_API_KEY
docker compose up --build
```

- Dashboard: http://localhost:8000
- API docs: http://localhost:8000/docs

The `simulator` service loads a week of sample runs on startup.

**Local dev**

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload                       # SQLite by default
python -m app.simulator --api http://localhost:8000 # in another terminal
```

## Reporting runs from a pipeline

```bash
# start
curl -X POST localhost:8000/api/runs -H 'Content-Type: application/json' \
  -d '{"pipeline": "orders_ingest"}'
# -> {"id": 42, "status": "running", ...}

# finish
curl -X POST localhost:8000/api/runs/42/complete -H 'Content-Type: application/json' \
  -d '{"status": "failed", "error_message": "column \"customer_tier\" does not exist"}'
# -> includes "diagnosis": {"category": "schema_drift", "root_cause": ..., "recommended_actions": [...]}
```

## API

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/pipelines` | Register a pipeline (name, owner, schedule, freshness SLA) |
| GET | `/api/pipelines` | Pipeline health: last status, 7-day success rate, avg runtime, staleness |
| POST | `/api/runs` | Start a run |
| POST | `/api/runs/{id}/complete` | Finish a run, triggering anomaly detection + RCA |
| GET | `/api/runs` | List runs (`pipeline`, `status`, `anomalies_only`, `limit`) |
| GET | `/api/runs/{id}` | Run detail with diagnosis |
| POST | `/api/runs/{id}/diagnose` | Re-run root cause analysis |
| GET | `/api/freshness` | Freshness vs SLA per pipeline |
| GET | `/api/metrics/summary` | Totals, success rate, anomalies, stale pipelines, issues by category |
| GET | `/api/export/runs.csv` | Flat export for Power BI / Excel |

## Tests

```bash
pytest -q
TEST_DATABASE_URL=postgresql+psycopg2://user:pass@localhost:5432/test pytest -q   # also checks the Power BI views
```

The OpenAI client is mocked in tests, so no API key is needed.
