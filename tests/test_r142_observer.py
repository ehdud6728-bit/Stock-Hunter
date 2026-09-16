import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import real_full_r1c1_structure_observer_r142 as o


def _frame(n=250):
    idx = pd.bdate_range("2025-09-01", periods=n)
    close = np.linspace(100.0, 150.0, n)
    return pd.DataFrame({
        "Open": close - 0.5,
        "High": close + 1.0,
        "Low": close - 1.0,
        "Close": close,
        "Volume": np.linspace(1000.0, 1500.0, n),
    }, index=idx)


def test_structure_metrics_are_descriptive_and_finite():
    raw = _frame()
    idx = raw.index
    anchor = pd.Series({
        "wave1_low_date": idx[-30].date().isoformat(),
        "wave1_high_date": idx[-20].date().isoformat(),
        "pullback_low_date": idx[-5].date().isoformat(),
    })
    m = o._structure_metrics(raw, idx[-1].date().isoformat(), anchor)
    assert m["observer_metric_status"] == "PASS"
    assert m["sell_pressure_metrics_are_proxy_only"] is True
    assert math.isfinite(m["pb_amount_vs_wave1"])
    assert math.isfinite(m["signal_volume_vs_20d_median"])
    assert 0 <= m["signal_clv"] <= 1


def test_ma224_requires_enough_causal_runtime_history():
    raw = _frame(250)
    idx = raw.index
    anchor = pd.Series({
        "wave1_low_date": idx[-30].date().isoformat(),
        "wave1_high_date": idx[-20].date().isoformat(),
        "pullback_low_date": idx[-5].date().isoformat(),
    })
    ok = o._ma224_metrics(raw, idx[-1].date().isoformat(), anchor)
    assert ok["ma224_status"] == "PASS_EXACT_RUNTIME_CACHE"
    short = raw.tail(220)
    no = o._ma224_metrics(short, idx[-1].date().isoformat(), anchor)
    assert no["ma224_status"] == "UNAVAILABLE_NO_RUNTIME_CACHE_GE224"


def test_long_cache_never_uses_future_row():
    raw = _frame(250)
    sd = raw.index[-1].date().isoformat()
    ymd = raw.index[-1].strftime("%Y%m%d")
    cache = {f"000001_250_{ymd}": raw}
    fr, key, days = o._find_long_cache(cache, "000001", sd)
    assert days == 250 and key and fr.index.max() == raw.index.max()

    future = raw.copy()
    next_day = raw.index[-1] + pd.offsets.BDay(1)
    future.loc[next_day] = future.iloc[-1]
    cache2 = {f"000001_260_{ymd}": future}
    fr2, key2, days2 = o._find_long_cache(cache2, "000001", sd)
    assert fr2.empty and key2 == "" and days2 == 0
