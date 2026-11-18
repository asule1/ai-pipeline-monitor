from app.anomaly import detect, robust_z

DUR = [100, 104, 98, 101, 97, 103, 99, 102]
ROWS = [10_000, 10_200, 9_900, 10_100, 9_800, 10_050]


def test_normal_run_has_no_reasons():
    assert detect(101, 10_000, DUR, ROWS) == []


def test_slow_run_flagged():
    [reason] = detect(400, 10_000, DUR, ROWS)
    assert "slow" in reason


def test_row_drop_and_zero_rows():
    assert "dropped" in detect(100, 1_500, DUR, ROWS)[0]
    assert "0 rows" in detect(100, 0, DUR, ROWS)[0]


def test_needs_minimum_history():
    assert detect(10_000, 0, DUR[:3], ROWS[:3]) == []


def test_flat_history():
    assert robust_z(100, [100] * 6) == 0
    assert robust_z(105, [100] * 6) == 0
    assert robust_z(200, [100] * 6) == float("inf")


def test_outliers_in_history_do_not_hide_anomaly():
    history = DUR + [900]  # one past slowdown
    assert detect(400, None, history, []) != []


def test_small_deviation_in_tight_history_ignored():
    tight = [100, 100.5, 99.8, 100.2, 99.9, 100.1]
    assert detect(108, None, tight, []) == []
