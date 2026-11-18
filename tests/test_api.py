from datetime import datetime, timedelta, timezone

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def add_pipeline(client, name="orders", sla=60):
    r = client.post("/api/pipelines", json={"name": name, "owner": "data-eng", "freshness_sla_minutes": sla})
    assert r.status_code == 201


def record(client, pipeline, start, seconds, status="success", rows=1000, error=None):
    run = client.post("/api/runs", json={"pipeline": pipeline, "started_at": start.isoformat()}).json()
    assert run["status"] == "running"
    r = client.post(f"/api/runs/{run['id']}/complete", json={
        "status": status, "finished_at": (start + timedelta(seconds=seconds)).isoformat(),
        "rows_processed": rows, "error_message": error,
    })
    assert r.status_code == 200, r.text
    return r.json()


def test_health_and_dashboard(client):
    assert client.get("/health").json() == {"status": "ok"}
    assert "Pipeline Monitor" in client.get("/").text


def test_duplicate_and_unknown_pipeline(client):
    add_pipeline(client)
    assert client.post("/api/pipelines", json={"name": "orders"}).status_code == 409
    assert client.post("/api/runs", json={"pipeline": "nope"}).status_code == 404


def test_run_lifecycle_and_duration(client):
    add_pipeline(client)
    run = record(client, "orders", T0, 120)
    assert run["duration_seconds"] == 120
    assert run["is_anomaly"] is False and run["diagnosis"] is None
    assert client.post(f"/api/runs/{run['id']}/complete", json={"status": "success"}).status_code == 409


def test_failed_run_gets_diagnosis(client):
    add_pipeline(client)
    run = record(client, "orders", T0, 30, status="failed", rows=None,
                 error='column "customer_tier" does not exist')
    assert run["diagnosis"]["category"] == "schema_drift"
    assert run["diagnosis"]["source"] == "rules"
    assert run["diagnosis"]["recommended_actions"]


def test_slow_run_flagged_as_anomaly(client):
    add_pipeline(client)
    for i in range(8):
        record(client, "orders", T0 + timedelta(hours=i), 100 + i)
    run = record(client, "orders", T0 + timedelta(hours=9), 600)
    assert run["is_anomaly"] is True
    assert run["diagnosis"]["category"] == "performance_degradation"
    anomalies = client.get("/api/runs", params={"anomalies_only": True}).json()
    assert [a["id"] for a in anomalies] == [run["id"]]


def test_freshness_and_summary(client):
    add_pipeline(client, "fresh_one", sla=60)
    add_pipeline(client, "stale_one", sla=60)
    now = datetime.now(timezone.utc)
    record(client, "fresh_one", now - timedelta(minutes=10), 60)
    record(client, "stale_one", now - timedelta(hours=5), 60)
    record(client, "stale_one", now - timedelta(minutes=20), 5, status="failed", rows=None, error="timeout")

    fresh = {f["pipeline"]: f["is_stale"] for f in client.get("/api/freshness").json()}
    assert fresh == {"fresh_one": False, "stale_one": True}

    s = client.get("/api/metrics/summary").json()
    assert s["total_runs"] == 3 and s["failed_runs"] == 1
    assert s["stale_pipelines"] == ["stale_one"]
    assert s["issues_by_category"] == {"timeout": 1}

    pipelines = {p["name"]: p for p in client.get("/api/pipelines").json()}
    assert pipelines["stale_one"]["success_rate_7d"] == 0.5
    assert pipelines["stale_one"]["last_status"] == "failed"


def test_rediagnose_and_csv_export(client):
    add_pipeline(client)
    now = datetime.now(timezone.utc)
    run = record(client, "orders", now - timedelta(hours=1), 10, status="failed", rows=None,
                 error="Connection refused")
    assert client.post(f"/api/runs/{run['id']}/diagnose").json()["diagnosis"]["category"] == "connection"
    csv_text = client.get("/api/export/runs.csv").text
    assert csv_text.splitlines()[0].startswith("run_id,pipeline,status")
    assert "connection" in csv_text
