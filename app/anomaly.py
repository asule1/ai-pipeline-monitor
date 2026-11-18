"""Detect anomalous pipeline runs by comparing against that pipeline's recent history.

Uses a robust z-score (median / MAD) so a few past outliers don't skew the baseline.
"""
from __future__ import annotations

import math
from statistics import median

MIN_HISTORY = 5
Z_THRESHOLD = 3.5
# Also require a meaningful relative change so tight histories don't flag tiny wobbles.
MIN_RELATIVE_CHANGE = 0.30


def _significant(value: float, base: float) -> bool:
    return base == 0 or abs(value - base) / abs(base) >= MIN_RELATIVE_CHANGE


def robust_z(value: float, history: list[float]) -> float:
    med = median(history)
    mad = median(abs(x - med) for x in history)
    if mad == 0:
        # Flat history: any meaningful deviation (>10%) counts as extreme.
        if value == med or (med != 0 and abs(value - med) / abs(med) <= 0.10):
            return 0.0
        return math.copysign(math.inf, value - med)
    return 0.6745 * (value - med) / mad


def detect(
    duration: float | None,
    rows: int | None,
    duration_history: list[float],
    rows_history: list[int],
) -> list[str]:
    """Return human-readable anomaly reasons (empty list = normal)."""
    reasons: list[str] = []

    if duration is not None and len(duration_history) >= MIN_HISTORY:
        z = robust_z(duration, duration_history)
        base = median(duration_history)
        if not _significant(duration, base):
            pass
        elif z > Z_THRESHOLD:
            reasons.append(f"Runtime {duration:.0f}s is unusually slow (typical ~{base:.0f}s)")
        elif z < -Z_THRESHOLD:
            reasons.append(f"Runtime {duration:.0f}s is unusually fast (typical ~{base:.0f}s)")

    if rows is not None and len(rows_history) >= MIN_HISTORY:
        base = median(rows_history)
        if rows == 0 and base > 0:
            reasons.append(f"Processed 0 rows (typical ~{base:,.0f})")
        else:
            z = robust_z(rows, rows_history)
            if not _significant(rows, base):
                pass
            elif z < -Z_THRESHOLD:
                reasons.append(f"Row count {rows:,} dropped sharply (typical ~{base:,.0f})")
            elif z > Z_THRESHOLD:
                reasons.append(f"Row count {rows:,} spiked (typical ~{base:,.0f})")

    return reasons
