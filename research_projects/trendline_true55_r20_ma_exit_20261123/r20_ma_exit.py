#!/usr/bin/env python3
"""R20: compare MA5/MA10/MA20 close-break, next-open exits on frozen R19.

RESEARCH_ONLY; do not alter LIVE/SEARCH/WATCH or place orders.
The moving-average threshold is computed only after the day's close;
execution happens at the following open (never at the breached average).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from pathlib import Path

import numpy as np
import pandas as pd

KIS_RUN = "44988188271"
KIS_CSV_SHA256 = "e5edabf1b111d424a0afa8503111d7dd4d6e4827036aa7eea512ef067d122dbc"
HORIZON, COST_PCT = 60, .2
R19_METHODS = ("D0_LOW_NEXT_OPEN", "PREV5_LOW_NEXT_OPEN", "PREV10_LOW_NEXT_OPEN")
MA_METHODS = {n: f"MA{n}_CLOSE_BELOW_NEXT_OPEN" for n in (5, 10, 20)}


def ma_close_break_next_open(g: pd.DataFrame, t: int, n: int) -> dict:
    """Exit when D+i closing price < SMA(n) as of D+i, at next OPEN."""
    if n not in MA_METHODS or t < n or t + HORIZON >= len(g):
        raise ValueError("INVALID_MA_PERIOD_OR_INCOMPLETE_HORIZON")
    entry = float(g.open.iloc[t + 1])
    if entry <= 0:
        raise ValueError("INVALID_ENTRY_PRICE")
    for i in range(t + 1, t + HORIZON):
        # Current close is included in its as-of daily SMA; no future closes.
        ma = float(g.close.iloc[i - n + 1:i + 1].mean())
        if float(g.close.iloc[i]) < ma:
            return {"net_pct": round(100 * (float(g.open.iloc[i + 1]) / entry - 1) - COST_PCT, 6),
                    "exit_day": i + 1 - t, "exit_reason": "CLOSE_BELOW_ASOF_MA"}
    return {"net_pct": round(100 * (float(g.close.iloc[t + HORIZON]) / entry - 1) - COST_PCT, 6),
            "exit_day": HORIZON, "exit_reason": "WINDOW_END_CLOSE"}


def prior_low_exit(g: pd.DataFrame, t: int, n: int) -> dict:
    """Recheck the frozen R19 previous-low baselines without modifying R19."""
    assert n in (5, 10) and t + HORIZON < len(g)
    entry = float(g.open.iloc[t + 1])
    for i in range(t + 1, t + HORIZON):
        if float(g.close.iloc[i]) < float(g.low.iloc[i - n:i].min()):
            return {"net_pct": round(100 * (float(g.open.iloc[i + 1]) / entry - 1) - COST_PCT, 6),
                    "exit_day": i + 1 - t,
                    "exit_reason": "PRIOR_CLOSE_BELOW_ASOF_STRUCTURE"}
    return {"net_pct": round(100 * (float(g.close.iloc[t + HORIZON]) / entry - 1) - COST_PCT, 6),
            "exit_day": HORIZON, "exit_reason": "WINDOW_END_CLOSE"}


def describe(rows: list[dict], method: str) -> dict:
    values = [float(r["outcomes"][method]["net_pct"]) for r in rows]
    if not values:
        return {"n": 0}
    v = sorted(values)
    trim = max(1, int(len(v) * .1)) if len(v) >= 10 else 0
    mid = v[trim:-trim] if trim else v
    return {"n": len(v), "mean_net_pct": round(statistics.mean(v), 6),
            "median_net_pct": round(statistics.median(v), 6),
            "p10_net_pct": round(float(np.quantile(v, .1)), 6),
            "worst_net_pct": round(v[0], 6), "best_net_pct": round(v[-1], 6),
            "mean_excluding_best_pct": round(statistics.mean(v[:-1]), 6),
            "trimmed_10pct_mean_net_pct": round(statistics.mean(mid), 6),
            "win_rate_pct": round(100 * sum(x > 0 for x in v) / len(v), 3),
            "median_exit_day": statistics.median(
                r["outcomes"][method]["exit_day"] for r in rows)}


def paired(rows: list[dict], test: str, control: str) -> dict:
    delta = [r["outcomes"][test]["net_pct"] - r["outcomes"][control]["net_pct"]
             for r in rows]
    return {"new_method": test, "control": control, "n": len(delta),
            "mean_delta_net_pct_points": round(statistics.mean(delta), 6),
            "median_delta_net_pct_points": round(statistics.median(delta), 6),
            "new_better_n": sum(x > 1e-9 for x in delta),
            "new_worse_n": sum(x < -1e-9 for x in delta),
            "same_n": sum(abs(x) <= 1e-9 for x in delta)}


def read_sources(r16_out: Path, r19_out: Path):
    manifest = json.loads((r16_out / "R16_SOURCE_MANIFEST.json").read_text(encoding="utf-8"))
    if (manifest.get("kis_run_id") != KIS_RUN
            or manifest.get("kis_csv_sha256") != KIS_CSV_SHA256
            or manifest.get("n_valid_tickers") != 26):
        raise ValueError("WRONG_OR_UNPINNED_KIS_SOURCE")
    archive = r16_out / "R16_FROZEN_HOLDOUT_UNADJUSTED_OHLCV.csv.gz"
    if hashlib.sha256(archive.read_bytes()).hexdigest() != manifest.get("raw_archive_sha256"):
        raise ValueError("R16_ARCHIVE_SHA_MISMATCH")
    prices = pd.read_csv(archive, dtype={"code": str, "date": str})
    if len(prices) != 37736 or prices.code.nunique() != 26 or prices.duplicated(["code", "date"]).any():
        raise ValueError("R16_ARCHIVE_SHAPE_DRIFT")
    r19_summary = json.loads((r19_out / "R19_TRUE55_TRANSFER_SUMMARY.json").read_text(encoding="utf-8"))
    r19_events = json.loads((r19_out / "R19_TRUE55_EVENT_AUDIT.json").read_text(encoding="utf-8"))
    if (r19_summary.get("source_kis_run_id") != KIS_RUN
            or r19_summary.get("definition") != "D0_CLOSE_GT_MAX_PREVIOUS_55_DAILY_HIGHS"
            or r19_summary.get("entry") != "DPLUS1_OPEN_MUST_BE_ABOVE_FROZEN_PRIOR55_HIGH"
            or r19_summary.get("cost_round_trip_pct") != COST_PCT
            or r19_summary.get("new_study_55_events") != 149
            or len(r19_events) != 149
            or len({(r["code"], r["date"]) for r in r19_events}) != 149):
        raise ValueError("R19_FIXED_COHORT_MISMATCH")
    return prices, r19_events


def analyze(prices: pd.DataFrame, events: list[dict]):
    frames = {str(code): f.sort_values("date").reset_index(drop=True)
              for code, f in prices.groupby("code")}
    positions = {code: {date: i for i, date in enumerate(f.date)}
                 for code, f in frames.items()}
    rows = []
    for event in events:
        code, date = str(event["code"]), str(event["date"])
        if code not in frames or date not in positions[code]:
            raise ValueError("R19_EVENT_MISSING_IN_FROZEN_SOURCE")
        f = frames[code]
        t = positions[code][date]
        for n in (5, 10):
            if prior_low_exit(f, t, n) != event["outcomes"][f"PREV{n}_LOW_NEXT_OPEN"]:
                raise ValueError("R19_STRUCTURE_BASELINE_REPLAY_CHANGED")
        outcomes = {m: event["outcomes"][m] for m in R19_METHODS}
        outcomes.update({name: ma_close_break_next_open(f, t, n)
                         for n, name in MA_METHODS.items()})
        rows.append({"code": code, "date": date, "year": int(date[:4]),
                     "outcomes": outcomes,
                     "expost_not_tradable": event["expost_not_tradable"]})
    return rows


def run(r16_out: Path, r19_out: Path, out: Path):
    prices, events = read_sources(r16_out, r19_out)
    rows = analyze(prices, events)
    methods = (*R19_METHODS, *MA_METHODS.values())
    summary = {
        "revision": "R20_MA5_MA10_MA20_CLOSE_BREAK_NEXT_OPEN_FROZEN_TRUE55",
        "research_only": True, "same_149_r19_entries": len(rows),
        "source_kis_run_id": KIS_RUN, "cost_round_trip_pct": COST_PCT,
        "entry": "R19_DPLUS1_OPEN_UNCHANGED", "max_horizon_bars": HORIZON,
        "exits": "IF_CURRENT_CLOSE_BELOW_CURRENT_SMA_EXIT_FOLLOWING_OPEN",
        "sma_includes_current_close": True,
        "method_statistics": {m: describe(rows, m) for m in methods},
        "ma_vs_prev10_low": {name: paired(rows, name, "PREV10_LOW_NEXT_OPEN")
                             for name in MA_METHODS.values()},
        "ma20_vs_ma5_ma10": {str(n): paired(rows, MA_METHODS[20], MA_METHODS[n])
                             for n in (5, 10)},
        "annual_by_ma": {str(year): {name: describe(
            [r for r in rows if r["year"] == year], name) for name in MA_METHODS.values()}
                         for year in range(2021, 2026)},
        "no_new_signal_selection_or_percent_stop_tuning": True,
        "expost_mfe_not_used_as_exit": True,
        "sample_unrepresentative_and_review_not_pristine_oos": True,
        "orders_placed": 0, "live_search_watch_changed": False,
        "can_publish_trade_edge": False,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "R20_MA_EXIT_SUMMARY.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (out / "R20_MA_PAIRED_EVENTS.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--r16-out", type=Path, required=True)
    p.add_argument("--r19-out", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    print(json.dumps(run(args.r16_out, args.r19_out, args.out),
                     ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
