#!/usr/bin/env python3
"""Pre-D0 chart context for the fixed 300 ENV10/ENV20 research cases.

No strategy search, trading simulation or live/search/watch code.  The source
is an uncertified frozen pilot, not independent point-in-time market history.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
import statistics
import sys

import pandas as pd

REVISION = "ENV10_ENV20_FROZEN_300_PRE_SIGNAL_CONTEXT_R4"
SOURCE_SHA256 = "b6d1fd1f4e4fdab6680bc78705825ff8b9775b20adb14b4aa8e05b55a004742d"
GROUPS = ("ENV10_ONLY", "ENV20_BOTH", "NO_TOUCH", "UNKNOWN")
PRE20 = list(range(-20, 0))
PRE60 = list(range(-60, 0))


def number(value: object, positive: bool = False) -> float | None:
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    if not math.isfinite(result) or (positive and result <= 0):
        return None
    return result


def good_bar(row: pd.Series) -> bool:
    values = [number(row.get(k), positive=True) for k in ("open", "high", "low", "close")]
    if any(x is None for x in values) or row.get("valid_price") != 1:
        return False
    o, h, lo, c = values
    return lo <= min(o, c) and h >= max(o, c)


def exact_history(rows: pd.DataFrame, ds: list[int], require_flow: bool = False) -> pd.DataFrame | None:
    past = rows.loc[rows.D.isin(ds)].sort_values("D")
    if past.D.tolist() != ds or not all(good_bar(x) for _, x in past.iterrows()):
        return None
    if require_flow and any(
        number(row.get(field)) is None or number(row.get(field)) < 0
        for _, row in past.iterrows() for field in ("volume", "amount")
    ):
        return None
    return past


def wave_proxy(past60: pd.DataFrame) -> dict:
    """Causal, descriptive first-wave proxy: pre-D0 trough->peak then retreat.

    Largest closing-price run-up from D-60..D-3. Peak is at least two
    sessions before D-1; candidate requires >=15% run-up and 20%-80% retrace.
    This is a research label, NOT a verified first wave or trading trigger.
    """
    old = past60.loc[past60.D.le(-3)]
    min_close = float(old.iloc[0].close)
    min_day = str(old.iloc[0].trade_date)
    best = (0.0, None, None, 0.0, 0.0)
    for row in old.iloc[1:].itertuples(index=False):
        close = float(row.close)
        advance = 100 * (close / min_close - 1)
        if advance > best[0]:
            best = (advance, min_day, str(row.trade_date), min_close, close)
        if close < min_close:
            min_close, min_day = close, str(row.trade_date)
    gain, trough_day, peak_day, trough, peak = best
    end = float(past60.loc[past60.D.eq(-1)].iloc[0].close)
    retrace = (peak - end) / (peak - trough) if peak > trough else None
    candidate = bool(gain >= 15 and retrace is not None and .20 <= retrace <= .80)
    return {
        "wave_proxy_status": "CANDIDATE" if candidate else "NO_CANDIDATE",
        "wave_proxy_gain_pct": round(gain, 6),
        "wave_proxy_retrace_fraction": round(retrace, 6) if retrace is not None else None,
        "wave_proxy_trough_date": trough_day,
        "wave_proxy_peak_date": peak_day,
    }


def describe(event: dict, panel_rows: pd.DataFrame) -> dict:
    """D0 features may depend on D<=0 only, never on D+ prices."""
    result = {k: event[k] for k in ("code", "date", "year", "signal_group")}
    result.update(context_status="UNKNOWN_D0", history60_status="UNKNOWN", wave_proxy_status="UNKNOWN")
    rows = panel_rows.loc[panel_rows.D.le(0)].copy()
    if rows.D.duplicated().any():
        result["context_status"] = "UNKNOWN_DUPLICATE_RELATIVE_DAY"
        return result
    d0 = rows.loc[rows.D.eq(0)]
    if len(d0) != 1 or not good_bar(d0.iloc[0]):
        return result
    bar = d0.iloc[0]
    pre20 = exact_history(rows, PRE20, require_flow=True)
    if pre20 is None:
        result["context_status"] = "UNKNOWN_PRE20_PRICE_OR_FLOW"
        return result
    d0_vol, d0_amt = number(bar.volume), number(bar.amount)
    if d0_vol is None or d0_amt is None or d0_vol < 0 or d0_amt < 0:
        result["context_status"] = "UNKNOWN_D0_FLOW"
        return result
    avg_vol = float(pre20.volume.mean())
    avg_amt = float(pre20.amount.mean())
    if avg_vol <= 0 or avg_amt <= 0:
        result["context_status"] = "UNKNOWN_ZERO_PRE20_FLOW_BASE"
        return result
    close, high, low, op = (float(bar[k]) for k in ("close", "high", "low", "open"))
    pre_high, pre_low = float(pre20.high.max()), float(pre20.low.min())
    prev_close = float(pre20.iloc[-1].close)
    result.update(
        context_status="KNOWN_PRE20_UNCERTIFIED_SOURCE",
        volume_d0=d0_vol, volume_pre20_avg=round(avg_vol, 6),
        volume_ratio_d0_to_pre20=round(d0_vol / avg_vol, 6),
        amount_d0=d0_amt, amount_pre20_avg=round(avg_amt, 6),
        amount_ratio_d0_to_pre20=round(d0_amt / avg_amt, 6),
        volume_pre5_to_pre20=round(float(pre20.tail(5).volume.mean()) / avg_vol, 6),
        prior20_high=pre_high, prior20_low=pre_low,
        d0_close_vs_prior20_high_pct=round(100 * (close / pre_high - 1), 6),
        d0_close_vs_prior20_low_pct=round(100 * (close / pre_low - 1), 6),
        prev_close_rank_in_prior20_range=round((prev_close - pre_low) / (pre_high - pre_low), 6)
        if pre_high > pre_low else None,
        d0_gap_from_previous_close_pct=round(100 * (op / prev_close - 1), 6),
        d0_body_pct_of_range=round((close - op) / (high - low), 6) if high > low else None,
        d0_upper_wick_fraction=round((high - max(close, op)) / (high - low), 6)
        if high > low else None,
        d0_close_rank_in_candle=round((close - low) / (high - low), 6) if high > low else None,
    )
    # MA20/MA60 can be independently recalculated ONLY within this pilot panel;
    # MA112/MA224 must retain their SOURCE_ONLY status (not enough raw history).
    for ma in (20, 60, 112, 224):
        stored = number(bar.get(f"MA{ma}"), positive=True)
        result[f"d0_close_vs_ma{ma}_pct"] = round(100 * (close / stored - 1), 6) if stored else None
        result[f"ma{ma}_basis"] = "PILOT_STORED_ONLY_UNCERTIFIED" if ma > 60 else "UNKNOWN_RAW_WINDOW"
    pre60 = exact_history(rows, PRE60, require_flow=True)
    if pre60 is None:
        result["history60_status"] = "UNKNOWN_INCOMPLETE_60_BARS"
        return result
    result["history60_status"] = "KNOWN_PRE60_UNCERTIFIED_SOURCE"
    result["prior60_high"] = float(pre60.high.max())
    result["prior60_low"] = float(pre60.low.min())
    for ma in (20, 60):
        raw = pd.concat([pre60.tail(ma - 1), d0], ignore_index=True)
        recalculated = float(raw.close.mean())
        stored = number(bar.get(f"MA{ma}"), positive=True)
        result[f"ma{ma}_basis"] = (
            "RAW_MATCH_PILOT_ONLY" if stored is not None and abs(recalculated - stored) <= .0002
            else "RAW_MISMATCH_OR_MISSING_REVIEW"
        )
    result.update(wave_proxy(pre60))
    return result


def group_summary(events: list[dict]) -> dict:
    vals = [x["volume_ratio_d0_to_pre20"] for x in events
            if x["context_status"] == "KNOWN_PRE20_UNCERTIFIED_SOURCE"]
    return {
        "events": len(events),
        "known_pre20": len(vals), "unknown_pre20": len(events) - len(vals),
        "known_pre60": sum(e["history60_status"].startswith("KNOWN_") for e in events),
        "median_d0_to_prior20_volume_ratio": round(statistics.median(vals), 6) if vals else None,
        "volume_d0_ge_2x_pre20_count": sum(v >= 2 for v in vals),
        "below_ma224_source_only_count": sum(
            e.get("d0_close_vs_ma224_pct") is not None and e["d0_close_vs_ma224_pct"] < 0
            for e in events
        ),
        "wave_proxy_candidates": sum(e["wave_proxy_status"] == "CANDIDATE" for e in events),
    }


def assemble(events: list[dict], pilot_hash: str, pool_keys: set[tuple[str, str]],
             fullmarket: dict) -> dict:
    if pilot_hash != SOURCE_SHA256:
        raise ValueError("FROZEN_SOURCE_CHANGED")
    keys = {(e["code"], e["date"]) for e in events}
    if len(events) != 300 or len(keys) != 300 or not keys.issubset(pool_keys):
        raise ValueError("FROZEN_300_OR_POOL_RECONCILIATION_FAILED")
    if fullmarket.get("data_retrieved") is not False or fullmarket.get("not_pit_certified") is not True:
        raise ValueError("FULL_MARKET_MANIFEST_CHANGED_REVIEW_REQUIRED")
    if any(e["signal_group"] not in GROUPS for e in events):
        raise ValueError("UNRECOGNIZED_SIGNAL_GROUP")
    summary = {g: group_summary([e for e in events if e["signal_group"] == g]) for g in GROUPS}
    if sum(x["events"] for x in summary.values()) != 300:
        raise AssertionError("EVENTS_LOST")
    return {
        "revision": REVISION, "research_only": True,
        "source_status": "UNCERTIFIED_FROZEN_300_PRICE_ONLY_PILOT_NOT_ALL_MARKET",
        "pilot_source_sha256": pilot_hash,
        "frozen_events": 300, "original_pool_membership": 300,
        "fullmarket_retrieved": False, "pit_certified": False,
        "ma112_ma224_independently_recomputed": False,
        "signal_grouping_frozen_from_r2": True,
        "volume_baseline": "D-20_THROUGH_D-1_EXCLUDES_D0",
        "range_baseline": "D-20_THROUGH_D-1_EXCLUDES_D0",
        "pre60_baseline": "D-60_THROUGH_D-1_STRICT_FULL_HISTORY",
        "wave_proxy": "MAX_PRE_D0_CLOSE_RUNUP_TO_D-3_GE_15PCT_D-1_RETRACE_20_TO_80PCT_DESCRIPTIVE_ONLY",
        "dplus_used_for_context_features": False,
        "by_signal_group": summary,
        "by_year": {str(y): group_summary([e for e in events if e["year"] == y])
                    for y in range(2021, 2026)},
        "strategy_winner": None, "can_publish_strategy_performance": False,
        "can_modify_live_search_watch": False,
        "breakout_line_identity_verified": False,
        "events": events,
    }


def run(repo: Path, out: Path) -> dict:
    sys.path.insert(0, str(repo / "research_projects/closebet_source_parity_20261117"))
    sys.path.insert(0, str(repo / "research_projects/closebet_env_ab_20261119"))
    from historical_case_audit import load_panel
    from frozen_300_forward_path import study

    panel, sha = load_panel(repo)
    manifest = json.loads((repo / "research_inputs/R1D27D_FULLMARKET_SOURCE_QUEUE_MANIFEST.json").read_text(encoding="utf-8"))
    original = study(panel, sha, manifest)
    with (repo / "research_inputs/R1D27R_FLOW_BLIND_FROZEN_1500.csv").open(encoding="utf-8-sig", newline="") as f:
        keys = {(x["code"], x["date"]) for x in csv.DictReader(f)}
    events = [describe(e, panel.loc[panel.code.eq(e["code"]) & panel.date.eq(e["date"])])
              for e in original["events"]]
    result = assemble(events, sha, keys, manifest)
    out.mkdir(parents=True, exist_ok=True)
    (out / "ENV10_ENV20_FROZEN_300_D0_CONTEXT.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    columns = list(dict.fromkeys(k for e in events for k in e))
    with (out / "ENV10_ENV20_FROZEN_300_D0_CONTEXT.csv").open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(events)
    print(json.dumps({k: v for k, v in result.items() if k != "events"}, indent=2, ensure_ascii=False))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--out", required=True)
    arg = parser.parse_args()
    run(Path(arg.repo), Path(arg.out))
