"""Root cause analysis for failed or anomalous runs.

If OPENAI_API_KEY is set, the run context is sent to an OpenAI model that returns a
structured diagnosis. Otherwise (or if the call fails) a rule-based classifier is used,
so the monitor always produces a diagnosis.
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import asdict, dataclass

log = logging.getLogger(__name__)

CATEGORIES = [
    "schema_drift", "timeout", "connection", "permission", "data_quality",
    "resource_exhaustion", "upstream_missing", "performance_degradation",
    "volume_anomaly", "unknown",
]


@dataclass
class RCAResult:
    category: str
    root_cause: str
    recommended_actions: list[str]
    confidence: float
    source: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RunContext:
    pipeline: str
    status: str
    error_message: str | None
    duration_seconds: float | None
    rows_processed: int | None
    anomaly_reasons: list[str]
    recent_statuses: list[str]


RULES: list[tuple[str, re.Pattern, str, list[str]]] = [
    (
        "schema_drift",
        re.compile(r"column .* does not exist|no such column|KeyError|unknown column|schema mismatch", re.I),
        "A source column was renamed, removed, or changed type, so the transform no longer matches the source schema.",
        ["Compare the current source schema to the last successful run",
         "Update the column mapping or add a schema contract check before load",
         "Notify the upstream data owner about the breaking change"],
    ),
    (
        "timeout",
        re.compile(r"timed? ?out|timeout|deadline exceeded|statement cancel", re.I),
        "The job or a query exceeded its time limit, likely due to larger input volume or a slow upstream system.",
        ["Check query plans for full scans and add indexes or partition filters",
         "Increase the timeout or split the job into smaller batches",
         "Check whether input volume grew compared to recent runs"],
    ),
    (
        "connection",
        re.compile(r"connection (refused|reset)|could not connect|network is unreachable|host .* not found|SSL", re.I),
        "The pipeline could not reach a source or target system (network, DNS, or service outage).",
        ["Verify the source/target service is up and reachable from the worker",
         "Check credentials/secrets rotation and firewall rules",
         "Add retry with exponential backoff for transient network errors"],
    ),
    (
        "permission",
        re.compile(r"permission denied|access denied|forbidden|unauthori[sz]ed|403|401", re.I),
        "The service account lacks the permissions needed to read the source or write the target.",
        ["Check recent IAM/role changes for the pipeline's service account",
         "Grant the minimum required read/write privileges",
         "Verify the credentials have not expired"],
    ),
    (
        "data_quality",
        re.compile(r"null value|not-null constraint|duplicate key|unique constraint|violates|invalid input|cannot cast", re.I),
        "Incoming records violate a constraint (nulls, duplicates, or bad types) in the target table.",
        ["Quarantine the offending records and inspect them",
         "Add validation/deduplication in the staging layer",
         "Confirm upstream did not change data formats"],
    ),
    (
        "resource_exhaustion",
        re.compile(r"out of memory|MemoryError|OOM|disk full|no space left|killed", re.I),
        "The job ran out of memory or disk, typically from a data volume spike or an unbounded join.",
        ["Check input volume against recent runs",
         "Process in chunks or increase worker memory",
         "Look for exploding joins (many-to-many keys)"],
    ),
    (
        "upstream_missing",
        re.compile(r"no such file|file not found|does not exist|not found|404|missing partition", re.I),
        "An expected upstream file, table, or partition was not available when the job ran.",
        ["Check whether the upstream job finished before this one started",
         "Add a sensor/dependency check on upstream completion",
         "Backfill once the upstream data lands"],
    ),
]


def rule_based_rca(ctx: RunContext) -> RCAResult:
    error = ctx.error_message or ""
    for category, pattern, cause, actions in RULES:
        if pattern.search(error):
            return RCAResult(category, cause, actions, 0.75, "rules")

    reasons = " ".join(ctx.anomaly_reasons).lower()
    if "slow" in reasons:
        return RCAResult(
            "performance_degradation",
            "The run succeeded but took far longer than usual, pointing to slower upstream sources, "
            "resource contention, or a regressed query.",
            ["Compare the query plan against a recent fast run",
             "Check worker/cluster utilization during the run window",
             "Check whether input volume increased"],
            0.6, "rules",
        )
    if "row" in reasons:
        return RCAResult(
            "volume_anomaly",
            "The run processed an unusual number of rows, which often means partial upstream data, a "
            "filter bug, or a duplicate load.",
            ["Check upstream row counts for the same window",
             "Verify incremental watermarks/filters were applied correctly",
             "Hold downstream consumers until the data is validated"],
            0.6, "rules",
        )
    return RCAResult(
        "unknown",
        "The failure did not match a known pattern.",
        ["Inspect the full job logs around the failure", "Re-run the job to rule out a transient issue"],
        0.3, "rules",
    )


SYSTEM_PROMPT = (
    "You are a data reliability engineer. Given a failed or anomalous ETL pipeline run, "
    "diagnose the most likely root cause. Respond with JSON only: "
    '{"category": one of ' + json.dumps(CATEGORIES) + ', "root_cause": string (1-2 sentences), '
    '"recommended_actions": list of 2-4 short strings, "confidence": number between 0 and 1}'
)


def openai_rca(ctx: RunContext, client=None) -> RCAResult:
    if client is None:
        from openai import OpenAI
        client = OpenAI()
    response = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        response_format={"type": "json_object"},
        temperature=0.1,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(asdict(ctx), default=str)},
        ],
    )
    data = json.loads(response.choices[0].message.content)
    category = data.get("category") if data.get("category") in CATEGORIES else "unknown"
    return RCAResult(
        category=category,
        root_cause=str(data["root_cause"]),
        recommended_actions=[str(a) for a in data.get("recommended_actions", [])][:4],
        confidence=max(0.0, min(1.0, float(data.get("confidence", 0.5)))),
        source="openai",
    )


def diagnose(ctx: RunContext, client=None) -> RCAResult:
    if client is not None or os.getenv("OPENAI_API_KEY"):
        try:
            return openai_rca(ctx, client)
        except Exception as e:  # noqa: BLE001 - always fall back to rules
            log.warning("OpenAI RCA failed, falling back to rules: %s", e)
    return rule_based_rca(ctx)
