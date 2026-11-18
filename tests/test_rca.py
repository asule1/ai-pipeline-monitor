import json
from types import SimpleNamespace

from app.rca import RunContext, diagnose, rule_based_rca


def ctx(error=None, reasons=()):
    return RunContext("orders_ingest", "failed" if error else "success", error, 100, 10, list(reasons), [])


def test_rules_classify_common_errors():
    cases = {
        'column "tier" does not exist': "schema_drift",
        "Query timed out after 1800 seconds": "timeout",
        "could not connect to server: Connection refused": "connection",
        "403 Access Denied: Table x": "permission",
        'duplicate key value violates unique constraint "pk"': "data_quality",
        "MemoryError: Unable to allocate": "resource_exhaustion",
        "FileNotFoundError: s3://bucket/key not found": "upstream_missing",
        "something weird": "unknown",
    }
    for error, expected in cases.items():
        assert rule_based_rca(ctx(error)).category == expected, error


def test_rules_for_anomalies():
    assert rule_based_rca(ctx(reasons=["Runtime 400s is unusually slow"])).category == "performance_degradation"
    assert rule_based_rca(ctx(reasons=["Row count 10 dropped sharply"])).category == "volume_anomaly"


def fake_client(payload):
    msg = SimpleNamespace(content=json.dumps(payload))
    create = lambda **kw: SimpleNamespace(choices=[SimpleNamespace(message=msg)])  # noqa: E731
    return SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))


def test_openai_result_used_and_sanitized():
    result = diagnose(ctx("boom"), client=fake_client({
        "category": "timeout", "root_cause": "Warehouse was overloaded.",
        "recommended_actions": ["a", "b", "c", "d", "e"], "confidence": 1.7,
    }))
    assert result.source == "openai"
    assert result.category == "timeout"
    assert len(result.recommended_actions) == 4
    assert result.confidence == 1.0


def test_openai_unknown_category_normalized():
    result = diagnose(ctx("boom"), client=fake_client({"category": "aliens", "root_cause": "x"}))
    assert result.category == "unknown"


def test_falls_back_to_rules_on_openai_error():
    class Broken:
        class chat:
            class completions:
                @staticmethod
                def create(**kw):
                    raise RuntimeError("API down")

    result = diagnose(ctx("Query timed out"), client=Broken())
    assert result.source == "rules"
    assert result.category == "timeout"
