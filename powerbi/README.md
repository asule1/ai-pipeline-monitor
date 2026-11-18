# Power BI dashboard

The API creates three reporting views in PostgreSQL on startup (`sql/powerbi_views.sql`):

| View | Grain | Use it for |
|---|---|---|
| `v_run_facts` | one row per completed run | Drill-down tables, failure/anomaly lists, RCA categories |
| `v_daily_pipeline_stats` | pipeline × day | Success-rate trend, p95 runtime trend, row volume |
| `v_pipeline_freshness` | one row per pipeline | Freshness / SLA cards |

## Connect

1. `docker compose up` (Postgres is exposed on `localhost:5432`, user/password/db = `monitor`).
2. Power BI Desktop → **Get Data → PostgreSQL database** → server `localhost`, database `monitor`.
3. Select the three views. Use **DirectQuery** for near-real-time, or **Import** with scheduled refresh.

No direct DB access? Use **Get Data → Web** with `http://localhost:8000/api/export/runs.csv?days=30`.

## Suggested report layout

**Page 1: Operations overview**
- Cards: total runs, success rate, failed runs, stale pipelines (`v_pipeline_freshness[is_stale]`)
- Line chart: `success_rate` by `run_date`, legend = `pipeline`
- Line chart: `p95_duration_seconds` by `run_date`
- Table: `v_pipeline_freshness` with conditional formatting on `is_stale`

**Page 2: Incidents & root causes**
- Bar chart: count of `run_id` by `issue_category` (`v_run_facts`)
- Stacked column: failures by `pipeline` and `issue_category`
- Table: `started_at`, `pipeline`, `status`, `issue_category`, `root_cause`, `error_message` filtered to `issue_category` not blank
- Slicers: `pipeline`, `owner`, `run_date`

Useful DAX measure:

```DAX
Success Rate = DIVIDE(
    CALCULATE(COUNTROWS(v_run_facts), v_run_facts[status] = "success"),
    COUNTROWS(v_run_facts)
)
```
