#!/usr/bin/env python3
"""R7 RESEARCH_ONLY: non-overlapping R6 breakout events and downside diagnostics.

This is an *independent, source-limited* implementation of the hypotheses in the
2026-11-19 R7 screenshot. The screenshot contains aggregate returns, not the
original formula code; new execution definitions are explicit and must NOT be
represented as byte-for-byte reproduction of that screenshot.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
R6_PATH = "research_projects/trendline_exit_timing_r6_20261119/r6_exit_timing.py"
R6_SHA256 = "53044200c87fafd114e1929f1347c9445171e61f39e72a5b1dc70f3ff1d42911"
HORIZON = 20
COST_BPS = 20
ATR_DAYS = 14
ATR_MULT = 3.0
HIGH_VOLUME_RATIO = 1.5
RULES = (
    "TIME_D20", "INITIAL_3ATR_STOP", "ATR_TRAIL_3",
    "STRUCTURE_NEXT_OPEN", "VOLUME_DOWN_NEXT_OPEN", "TWO_OF_THREE_NEXT_OPEN",
)


def r6_module(repo: Path = ROOT):
    source = repo / R6_PATH
    if hashlib.sha256(source.read_bytes()).hexdigest() != R6_SHA256:
        raise ValueError("FROZEN_R6_SOURCE_SHA256_MISMATCH")
    sys.path.insert(0, str(source.parent))
    import r6_exit_timing as r6
    return r6


def nonoverlap(events: list[dict]) -> tuple[list[dict], list[dict]]:
    """Earliest D0 per stock, then require 21 intervening index positions.

    Selection references *only* code, D0 index, R6's outcome-blind primary
    period priority; never uses returns, completion or missing line geometry.
    The 20-day D+1..D+20 exposure windows cannot share a trading bar.
    """
    last_index: dict[str, int] = {}
    selected, excluded = [], []
    for e in sorted(events, key=lambda x: (x["code"], x["idx"], x["line_id"])):
        if e["code"] in last_index and e["idx"] - last_index[e["code"]] <= HORIZON:
            excluded.append(e)
        else:
            selected.append(e)
            last_index[e["code"]] = e["idx"]
    return selected, excluded


def _atr14_at(full: pd.DataFrame, asof: int) -> float:
    """Last 14 true ranges available by asof close (simple mean ATR14)."""
    if asof < ATR_DAYS:
        raise ValueError("UNKNOWN_ATR14_HISTORY")
    tr = []
    for t in range(asof - ATR_DAYS + 1, asof + 1):
        row, prev = full.iloc[t], full.iloc[t-1]
        hi, lo, previous_close = float(row.high), float(row.low), float(prev.close)
        tr.append(max(hi-lo, abs(hi-previous_close), abs(lo-previous_close)))
    atr = statistics.mean(tr)
    if not math.isfinite(atr) or atr <= 0:
        raise ValueError("INVALID_ATR14")
    return atr


def _finish(bars: pd.DataFrame, entry: float, day: int, price: float,
            reason: str, *, gap: bool = False, votes: list[str] | None = None) -> dict:
    gross = 100 * (price / entry - 1)
    return {
        "status": "PROVISIONAL_SOURCE_LIMITED_DAILY_OHLC_NOT_OOS",
        "exit_day": day, "exit_date": str(bars.iloc[day-1].date.date()),
        "exit_price": round(price, 6), "reason": reason,
        "net_total20bps_pct": round(gross-COST_BPS/100, 6),
        "gap_through_stop_open_fill": gap, "causal_votes_after_close": votes or [],
    }


def _stop_fill(bars: pd.DataFrame, entry: float, day: int, stop: float) -> dict | None:
    bar = bars.iloc[day-1]
    if float(bar.open) <= stop:
        return _finish(bars, entry, day, float(bar.open), "GAP_STOP_AT_OPEN", gap=True)
    if float(bar.low) <= stop:
        return _finish(bars, entry, day, stop, "INTRADAY_STOP_ASSUMED_FILL")
    return None


def _causal_close_votes(full: pd.DataFrame, t: int, day: int,
                        entry: float, fixed_prior_low: float,
                        line_level_d0: float, line_slope: float) -> list[str]:
    """Three independent votes known only AFTER D+day CLOSE.

    ATR vote: close below prior-completed-high minus 3 ATR(prev completed).
    Volume vote: down close, >=1.5 x mean volume of preceding 20 sessions.
    Structure vote: close below fixed D0-known 20d support OR original frozen line.
    """
    j = t + day
    row, yesterday = full.iloc[j], full.iloc[j-1]
    prior_high = max([entry] + [float(v) for v in full.iloc[t+1:j].high])
    atr = _atr14_at(full, j-1)
    volume_ref = float(full.iloc[j-20:j].volume.mean())
    line_today = line_level_d0 + day * line_slope
    votes = []
    if float(row.close) < prior_high - ATR_MULT * atr:
        votes.append("ATR_CLOSE_LOST")
    if (float(row.close) < float(yesterday.close) and volume_ref > 0 and
            float(row.volume) >= HIGH_VOLUME_RATIO * volume_ref):
        votes.append("HIGH_VOLUME_DOWN_CLOSE")
    if (float(row.close) < fixed_prior_low or
            float(row.close) < line_today * .996):
        votes.append("FROZEN_SUPPORT_OR_LINE_RELOSS")
    return votes


def simulate(full: pd.DataFrame, t: int, event: dict, rule: str) -> dict:
    if rule not in RULES:
        raise ValueError("UNREGISTERED_EXIT_RULE")
    bars = full.iloc[t+1:t+HORIZON+1]
    if len(bars) != HORIZON:
        return {"status": "UNKNOWN_INCOMPLETE_DPLUS20_PATH"}
    entry = float(bars.iloc[0].open)
    if entry <= 0 or (bars[["open", "high", "low", "close"]] <= 0).any().any():
        return {"status": "UNKNOWN_INVALID_PRICE"}
    if rule == "TIME_D20":
        return _finish(bars, entry, HORIZON, float(bars.iloc[-1].close), "TIME_D20_CLOSE")

    if t < ATR_DAYS:
        return {"status": "UNKNOWN_ATR14_HISTORY"}

    atr0 = _atr14_at(full, t)
    if rule == "INITIAL_3ATR_STOP":
        stop = entry - ATR_MULT * atr0
        for day in range(1, HORIZON+1):
            hit = _stop_fill(bars, entry, day, stop)
            if hit:
                return hit
    if rule == "ATR_TRAIL_3":
        stop = entry - ATR_MULT * atr0
        preceding_peak = entry
        for day in range(1, HORIZON+1):
            # Yesterday's *completed* high and ATR; never today's high to stop today.
            stop = max(stop, preceding_peak - ATR_MULT * _atr14_at(full, t+day-1))
            hit = _stop_fill(bars, entry, day, stop)
            if hit:
                return hit
            preceding_peak = max(preceding_peak, float(bars.iloc[day-1].high))

    if rule in ("STRUCTURE_NEXT_OPEN", "VOLUME_DOWN_NEXT_OPEN", "TWO_OF_THREE_NEXT_OPEN"):
        line = event.get("break_line_level_d0")
        slope = event.get("line_slope_per_bar")
        if line is None or slope is None:
            return {"status": "UNKNOWN_MISSING_ORIGINAL_LINE_GEOMETRY"}
        if t < 20:
            return {"status": "UNKNOWN_PRIOR_20_DAY_CONTEXT"}
        prior_low = float(full.iloc[t-20:t].low.min())
        for day in range(1, HORIZON):
            votes = _causal_close_votes(full, t, day, entry, prior_low, float(line), float(slope))
            triggers = {
                "STRUCTURE_NEXT_OPEN": "FROZEN_SUPPORT_OR_LINE_RELOSS" in votes,
                "VOLUME_DOWN_NEXT_OPEN": "HIGH_VOLUME_DOWN_CLOSE" in votes,
                "TWO_OF_THREE_NEXT_OPEN": len(votes) >= 2,
            }
            if triggers[rule]:
                return _finish(bars, entry, day+1, float(bars.iloc[day].open),
                               "PRIOR_COMPLETE_CLOSE_NEXT_OPEN", votes=votes)
    return _finish(bars, entry, HORIZON, float(bars.iloc[-1].close), "TIME_D20_FALLBACK")


def _pctile(values: list[float], percentile: int) -> float | None:
    if not values:
        return None
    a = sorted(values)
    pos = (len(a)-1) * percentile / 100
    lo, hi = math.floor(pos), math.ceil(pos)
    return round(a[lo] + (a[hi]-a[lo]) * (pos-lo), 6)


def stats(values: list[float]) -> dict:
    if not values:
        return {"n": 0}
    n = len(values)
    sorted_values = sorted(values)
    worst_n = max(1, math.ceil(n * .1))
    return {
        "n": n, "mean_pct": round(statistics.mean(values), 6),
        "median_pct": round(statistics.median(values), 6),
        "p05_pct": _pctile(values, 5), "p10_pct": _pctile(values, 10),
        "p90_pct": _pctile(values, 90),
        "worst_10pct_mean_pct": round(statistics.mean(sorted_values[:worst_n]), 6),
        "min_pct": round(sorted_values[0], 6), "max_pct": round(sorted_values[-1], 6),
        "win_count": sum(x > 0 for x in values),
        "loss_at_least_10pct_count": sum(x <= -10 for x in values),
    }


def study(repo: Path = ROOT) -> dict:
    r6 = r6_module(repo)
    r5 = r6.r5_module(repo)
    frozen = r5.study(repo)
    # Reuse the exact R6 event simulator without re-running R5 a second time.
    # R6's slim ledger omits D0 geometry, so keep the SHA-locked R5 originals.
    original = {
        (e["code"], e["date"], e["scale"], e["line_id"]): e
        for e in frozen["events"]
    }
    prepare = r5._modules(repo)[0]
    prices = {c: prepare(r5.frozen_data(repo, c)) for c in r5.SOURCES}
    r6_events = [r6.scan_event(e, prices[e["code"]]) for e in frozen["events"]]
    primary = r6.primary_events(r6_events)
    selected, overlap_removed = nonoverlap(primary)
    assert len(r6_events) == frozen["line_events"] == 506
    assert len(primary) == frozen["unique_stock_dates"] == 368
    rows = []
    for e in selected:
        full = prices[e["code"]]
        t = int(e["idx"])
        if str(full.iloc[t].date.date()) != e["date"]:
            raise AssertionError("R6_EVENT_D0_IDENTITY_MISMATCH")
        source_event = original[(e["code"], e["date"], e["scale"], e["line_id"])]
        if int(source_event["idx"]) != t:
            raise AssertionError("R5_R6_EVENT_INDEX_MISMATCH")
        models = {rule: simulate(full, t, source_event, rule) for rule in RULES}
        if models["TIME_D20"]["status"].startswith("PROVISIONAL_"):
            r6_reference = e["exit_models"]["TIME_D20"]
            if abs(models["TIME_D20"]["net_total20bps_pct"] - r6_reference["net_total20bps_pct"]) > 1e-5:
                raise AssertionError("R6_D20_PARITY_FAILURE")
        comparable = all(m["status"].startswith("PROVISIONAL_") for m in models.values())
        rows.append({
            "code": e["code"], "d0": e["date"], "year": int(e["date"][:4]),
            "idx": t, "scale": e["scale"], "line_id": e["line_id"],
            "status": "COMPARABLE" if comparable else "NOT_COMPARABLE",
            "models": models,
        })
    fully = [x for x in rows if x["status"] == "COMPARABLE"]
    if (len(primary), len(selected), len(fully)) != (368, 94, 90):
        unknowns = [(x["code"], x["d0"], x["idx"],
                     {rule: val["status"] for rule, val in x["models"].items()
                      if not val["status"].startswith("PROVISIONAL_")})
                    for x in rows if x["status"] != "COMPARABLE"]
        raise AssertionError(f"FROZEN_NONOVERLAP_POPULATION_DRIFT "
                             f"{(len(primary),len(selected),len(fully))} {unknowns}")
    returns = {rule: [x["models"][rule]["net_total20bps_pct"] for x in fully] for rule in RULES}
    deltas = {}
    for rule in RULES[1:]:
        differences = [v-base for v, base in zip(returns[rule], returns["TIME_D20"])]
        deltas[rule] = {
            **stats(differences),
            "better_than_d20_count": sum(x > 1e-9 for x in differences),
            "worse_than_d20_count": sum(x < -1e-9 for x in differences),
            "unchanged_count": sum(abs(x) <= 1e-9 for x in differences),
            "deep_loss_10pct_to_better_count": sum(
                b <= -10 and v > b for b, v in zip(returns["TIME_D20"], returns[rule])),
            "d20_positive_but_early_exit_negative_count": sum(
                b > 0 and v < 0 for b, v in zip(returns["TIME_D20"], returns[rule])),
        }
    by_stock = {c: {rule:stats([x["models"][rule]["net_total20bps_pct"] for x in fully
                                 if x["code"] == c]) for rule in RULES}
                for c in sorted(r5.SOURCES)}
    by_year = {str(y): {rule:stats([x["models"][rule]["net_total20bps_pct"] for x in fully
                                    if x["year"] == y]) for rule in RULES}
               for y in range(2021, 2025)}
    return {
        "revision": "R7_NONOVERLAP_DOWNSIDE_DISTRIBUTION_SOURCE_LIMITED",
        "research_only": True, "source_r6_sha256": R6_SHA256,
        "r7_screenshot_rule_formulas_available": False,
        "r7_new_rules_are_independent_explicit_proxies": True,
        "selection": "FIRST_D0_PER_STOCK_THEN_NEXT_D0_INDEX_GT_PREVIOUS_PLUS_20",
        "selection_reads_future_returns": False,
        "line_events_r6": len(r6_events), "primary_stock_dates": len(primary),
        "nonoverlap_selected": len(selected), "excluded_due_to_window_overlap": len(overlap_removed),
        "same_scenario_comparable": len(fully),
        "noncomparable_reasons": dict(Counter(
            m["status"] for x in rows if x["status"] != "COMPARABLE"
            for m in x["models"].values() if not m["status"].startswith("PROVISIONAL_"))),
        "rules": list(RULES), "exit_assumptions": {
            "entry": "D+1_OPEN", "horizon_days": HORIZON, "round_trip_total_cost_bps": COST_BPS,
            "atr": "SIMPLE_MEAN_LAST_14_TRUE_RANGES_PRIOR_COMPLETED_DAY",
            "stop_multiplier": ATR_MULT, "down_volume_multiple": HIGH_VOLUME_RATIO,
            "stop_gap": "WORSE_OPEN", "close_confirmed_signal": "NEXT_OPEN_NOT_SAME_CLOSE",
            "two_of_three": ["PRIOR_HIGH_MINUS_3ATR_CLOSE_BREACH", "1_5X_PREVIOUS20_VOLUME_DOWN_CLOSE",
                             "PRE_D0_20_LOW_OR_ORIGINAL_LINE_CLOSE_BREACH"],
            "same_day_intra_bar_order_known": False,
        },
        "returns_distribution": {rule:stats(returns[rule]) for rule in RULES},
        "paired_difference_vs_D20": deltas,
        "by_stock_not_independent_market_sample": by_stock,
        "by_year_not_oos": by_year, "rows": rows,
        "source_population": "THREE_HANDPICKED_HISTORICAL_STOCKS_NOT_ALL_MARKET",
        "independent_price_source_certified": False, "full_market_pit_coverage": False,
        "survivorship_bias_resolved": False, "adjusted_price_lineage_certified": False,
        "out_of_sample": False, "market_regime_pit": "UNKNOWN",
        "strategy_winner": None, "can_publish_strategy_performance": False,
        "can_modify_live_search_watch": False, "env_lower_touch_events_merged": False,
    }


def run(repo: Path, out: Path) -> dict:
    data = study(repo)
    out.mkdir(parents=True, exist_ok=True)
    summary = {k:v for k,v in data.items() if k != "rows"}
    (out / "R7_TAIL_DISTRIBUTION_SUMMARY.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False)+"\n", encoding="utf-8")
    flat = []
    for x in data["rows"]:
        for rule, model in x["models"].items():
            flat.append({**{k:v for k,v in x.items() if k != "models"}, "rule": rule,
                         **model})
    pd.DataFrame(flat).to_csv(out / "R7_NONOVERLAP_RULES_FULL_LEDGER.csv",
                              encoding="utf-8-sig", index=False)
    return data


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", default=".")
    p.add_argument("--out", required=True)
    args = p.parse_args()
    data = run(Path(args.repo).resolve(), Path(args.out).resolve())
    print(json.dumps({k:v for k,v in data.items() if k in (
        "revision", "nonoverlap_selected", "same_scenario_comparable",
        "noncomparable_reasons", "returns_distribution", "paired_difference_vs_D20")},
        ensure_ascii=False, indent=2, allow_nan=False))
