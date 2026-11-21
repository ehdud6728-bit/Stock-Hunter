#!/usr/bin/env python3
"""R12: frozen R5 close signal -> next-session OPEN entry expectancy.

Research only. No trading orders, optimization, or LIVE/SEARCH/WATCH edits.
All exposure paths and reference-day outcomes are ex-post descriptions.
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
R5_PATH = "research_projects/trendline_breakout_quality_20261119/r5_breakout_paths.py"
R5_SHA256 = "c1245ef44d13b0252cc1215cdf4d58ee5593277546a88a6dbd35f9a15a8e6d69"
ADDITIONAL_SOURCES = {
    "053800": "2ec18a88960f0ad3340375a585e878595088da3120570fe21b6210a45f94e624",
    "220260": "1ee7582093bccb9532ed8d5dba661c8b52350390c6db2876cb5060d98e42a525",
}
HORIZONS = (5, 10, 20)
COST_BPS = 20  # TOTAL round trip, not each side
BRACKETS = ((5, 5), (10, 5), (10, 10))  # (take-profit %, stop-loss %), fixed before results
SIGNAL_FEATURES = (
    "d0_close_location", "d0_volume_vs_pre20", "d0_amount_vs_pre20",
    "d0_gap_vs_prev_close_pct", "d0_close_vs_ma20_pct",
    "d0_close_vs_frozen_line_pct", "d0_upper_wick_fraction",
)
NONOVERLAP_BARS = 20
MIN_REFERENCE_PRIOR_BARS = 60
SCALE_PRIORITY = {"MEDIUM": 0, "LONG": 1, "SHORT": 2}
COHORTS = ("FROZEN_R5_THREE", "ADDITIONAL_EXISTING_TWO")


def r5_module(repo: Path):
    source = repo / R5_PATH
    if hashlib.sha256(source.read_bytes()).hexdigest() != R5_SHA256:
        raise ValueError("R5_CANONICAL_SOURCE_MISMATCH")
    sys.path.insert(0, str(source.parent))
    import r5_breakout_paths as r5
    return r5


def extra_frozen_prices(repo: Path, code: str) -> pd.DataFrame:
    source = repo / "research_projects/r1d97/data" / f"{code}_KRX.csv"
    if code not in ADDITIONAL_SOURCES or hashlib.sha256(source.read_bytes()).hexdigest() != ADDITIONAL_SOURCES[code]:
        raise ValueError("ADDITIONAL_INPUT_SHA256_MISMATCH " + code)
    raw = pd.read_csv(source, dtype={"code": str})
    if raw.empty or set(raw.code) != {code} or raw.date.duplicated().any():
        raise AssertionError("INVALID_ADDITIONAL_STOCK_DATES " + code)
    return raw


def primary_signals(events: list[dict]) -> list[dict]:
    """One signal per stock/date, using R6's *fixed* scale priority."""
    chosen: dict[tuple[str, str], dict] = {}
    for event in sorted(events, key=lambda e: (
        e["code"], e["date"], SCALE_PRIORITY[e["scale"]], e["line_id"]
    )):
        chosen.setdefault((event["code"], event["date"]), event)
    return sorted(chosen.values(), key=lambda e: (e["code"], e["idx"]))


def nonoverlap_signals(events: list[dict], *, bars: int = NONOVERLAP_BARS) -> list[dict]:
    """Exact R7 embargo: earliest first; block next `bars` signal days."""
    if bars < 1:
        raise ValueError("INVALID_COOLDOWN")
    kept, last = [], {}
    for event in sorted(events, key=lambda e: (e["code"], e["idx"])):
        code, t = event["code"], int(event["idx"])
        if t > last.get(code, -bars - 1) + bars:
            kept.append(event)
            last[code] = t
    return kept


def first_touch_day(future: pd.DataFrame, factor: float, *, upper: bool) -> int | None:
    """Ex-post touch only; not an instruction to trade at the extreme."""
    for day, bar in enumerate(future.itertuples(index=False), start=1):
        if (_at_least(float(bar.high), factor) if upper else _at_most(float(bar.low), factor)):
            return day
    return None


def _at_least(price: float, level: float) -> bool:
    return price >= level or math.isclose(price, level, rel_tol=1e-12, abs_tol=1e-9)


def _at_most(price: float, level: float) -> bool:
    return price <= level or math.isclose(price, level, rel_tol=1e-12, abs_tol=1e-9)


def first_touch_order(target_day: int | None, stop_day: int | None) -> str:
    if target_day is None:
        return "STOP_ONLY" if stop_day is not None else "NEITHER_TOUCHED"
    if stop_day is None:
        return "TARGET_ONLY"
    if target_day == stop_day:
        return "SAME_DAILY_BAR_ORDER_UNKNOWN"
    return "TARGET_BEFORE_STOP" if target_day < stop_day else "STOP_BEFORE_TARGET"


def bracket_exit(future: pd.DataFrame, entry: float, target_pct: float, stop_pct: float) -> dict:
    """Fixed take-profit/stop; daily OHLC same-bar ambiguity is stop-first.

    Worst execution on gap-down through stop uses the OPEN. Profit on gap-up
    is filled only at the target (not a favorable imagined price).
    """
    target, stop = entry * (1 + target_pct / 100), entry * (1 - stop_pct / 100)
    for day, bar in enumerate(future.itertuples(index=False), start=1):
        if _at_most(float(bar.open), stop):
            exit_price, reason = float(bar.open), "STOP_GAP_DOWN_OPEN"
        elif _at_least(float(bar.open), target):
            exit_price, reason = target, "TARGET_GAP_UP_CONSERVATIVE_TARGET"
        elif _at_most(float(bar.low), stop):
            exit_price, reason = stop, (
                "STOP_SAME_BAR_TARGET_AMBIGUOUS_PESSIMISTIC" if _at_least(float(bar.high), target)
                else "STOP_INTRADAY"
            )
        elif _at_least(float(bar.high), target):
            exit_price, reason = target, "TARGET_INTRADAY"
        else:
            continue
        break
    else:
        day, bar = len(future), next(reversed(list(future.itertuples(index=False))))
        exit_price, reason = float(bar.close), "TIME_CLOSE_NO_BARRIER"
    gross = 100. * (exit_price / entry - 1)
    return {
        "exit_day": day, "exit_price": round(exit_price, 6), "exit_reason": reason,
        "gross_pct": round(gross, 6),
        "net_total20bps_pct": round(gross - COST_BPS / 100., 6),
        "hit_target": reason.startswith("TARGET_"),
        "hit_stop": reason.startswith("STOP_"),
        "same_bar_pessimistic": reason == "STOP_SAME_BAR_TARGET_AMBIGUOUS_PESSIMISTIC",
    }


def path_at_close(prices: pd.DataFrame, t: int, h: int) -> dict:
    """D0 after-close selection, D+1 OPEN entry, D+h CLOSE exit.

    High/low extrema are ex-post only; their intraday order is unknown.
    """
    if t < 0 or t >= len(prices) or h < 1:
        raise ValueError("INVALID_DATE_INDEX_OR_HORIZON")
    future = prices.iloc[t + 1:t + h + 1]
    if len(future) < h:
        return {"status": "UNKNOWN_RIGHT_CENSORED_FUTURE"}
    d0_close = float(prices.iloc[t].close)
    entry = float(future.iloc[0].open)
    terminal = float(future.iloc[-1].close)
    if not math.isfinite(d0_close) or d0_close <= 0 or not math.isfinite(entry) or entry <= 0:
        return {"status": "UNKNOWN_INVALID_D0_OR_ENTRY"}
    ohlc = future[["open", "high", "low", "close"]]
    if not ohlc.notna().all().all() or not (ohlc > 0).all().all():
        return {"status": "UNKNOWN_INVALID_FUTURE_OHLC"}
    gross = 100.0 * (terminal / entry - 1)
    peak_day = max(range(1, len(future) + 1),
                   key=lambda i: (float(future.iloc[i-1].high), -i))
    target_touch = {str(x): first_touch_day(future, entry * (1 + x/100), upper=True)
                    for x in (5, 10, 15)}
    stop_touch = {str(x): first_touch_day(future, entry * (1 - x/100), upper=False)
                  for x in (5, 10)}
    brackets = {f"TAKE_{target}_STOP_{stop}": bracket_exit(future, entry, target, stop)
                for target, stop in BRACKETS}
    return {
        "status": "DESCRIPTIVE_COMPLETE_NOT_LIVE_EXECUTION",
        "entry_date": future.iloc[0].date.strftime("%Y-%m-%d"),
        "exit_date": future.iloc[-1].date.strftime("%Y-%m-%d"),
        "entry_open": round(entry, 6), "exit_close": round(terminal, 6),
        "gross_pct": round(gross, 6),
        "net_total20bps_pct": round(gross - COST_BPS / 100.0, 6),
        "gap_from_signal_close_pct": round(100.0 * (entry / d0_close - 1), 6),
        "mfe_high_from_entry_pct_EXPOST": round(100.0 * (float(future.high.max()) / entry - 1), 6),
        "mae_low_from_entry_pct_EXPOST": round(100.0 * (float(future.low.min()) / entry - 1), 6),
        "peak_high_day_EXPOST": peak_day,
        "first_target_touch_day_EXPOST": target_touch,
        "first_stop_touch_day_EXPOST": stop_touch,
        "target_vs_stop_touch_order_EXPOST": {
            f"TAKE_{target}_STOP_{stop}": first_touch_order(target_touch[str(target)], stop_touch[str(stop)])
            for target, stop in BRACKETS
        },
        "predeclared_bracket_models": brackets,
        "same_day_high_low_order_known": False,
    }


def metric_row(signal: dict, prices: pd.DataFrame, cohort: str) -> dict:
    t = int(signal["idx"])
    if t < 0 or t >= len(prices) or prices.iloc[t].date.strftime("%Y-%m-%d") != signal["date"]:
        raise AssertionError("SIGNAL_PRICE_INDEX_MISMATCH")
    return {
        "cohort": cohort, "code": signal["code"], "signal_date": signal["date"],
        "year": int(signal["date"][:4]), "idx": t,
        "scale": signal["scale"], "line_id": signal["line_id"],
        "asof_d0_features": {key: signal.get(key) for key in SIGNAL_FEATURES},
        "selection_asof": "AFTER_D0_CLOSE_R5_R116", "horizons_EXPOST": {
            str(h): path_at_close(prices, t, h) for h in HORIZONS
        },
    }


def reference_days(prices: pd.DataFrame, excluded_signal_indices: set[int], code: str) -> list[dict]:
    """Every eligible non-signal trading day; no outcome-based reference selection.

    Overlapping reference windows are deliberate and NOT independent draws.
    """
    rows = []
    for t in range(MIN_REFERENCE_PRIOR_BARS, len(prices)):
        if t in excluded_signal_indices:
            continue
        day = prices.iloc[t].date.strftime("%Y-%m-%d")
        rows.append({
            "code": code, "date": day, "year": int(day[:4]), "idx": t,
            "horizons_EXPOST": {str(h): path_at_close(prices, t, h) for h in HORIZONS},
        })
    return rows


def _quantile(vals: list[float], fraction: float) -> float | None:
    if not vals:
        return None
    sorted_vals = sorted(vals)
    # Nearest rank: deterministic; especially visible for small samples.
    idx = max(0, math.ceil(len(sorted_vals) * fraction) - 1)
    return round(sorted_vals[idx], 6)


def _mean(vals: list[float]) -> float | None:
    return round(statistics.mean(vals), 6) if vals else None


def summarize(rows: list[dict], h: int) -> dict:
    paths = [e["horizons_EXPOST"][str(h)] for e in rows]
    completed = [p for p in paths if p["status"].startswith("DESCRIPTIVE_")]
    net = [p["net_total20bps_pct"] for p in completed]
    mfe = [p["mfe_high_from_entry_pct_EXPOST"] for p in completed]
    mae = [p["mae_low_from_entry_pct_EXPOST"] for p in completed]
    def touch_rate(name: str, threshold: int) -> dict:
        yes = sum(p[name][str(threshold)] is not None for p in completed)
        return {"n_touch": yes, "pct_complete": round(100 * yes / len(completed), 3) if completed else None}
    def bracket_stats(target: int, stop: int) -> dict:
        key = f"TAKE_{target}_STOP_{stop}"
        b = [p["predeclared_bracket_models"][key] for p in completed]
        returns = [x["net_total20bps_pct"] for x in b]
        return {
            "n": len(b), "mean_net_pct": _mean(returns),
            "median_net_pct": round(statistics.median(returns), 6) if returns else None,
            "net_win_rate_pct": round(100 * sum(x > 0 for x in returns) / len(b), 3) if b else None,
            "target_hit": sum(x["hit_target"] for x in b),
            "stop_hit": sum(x["hit_stop"] for x in b),
            "same_day_stop_first": sum(x["same_bar_pessimistic"] for x in b),
            "gap_stop": sum(x["exit_reason"] == "STOP_GAP_DOWN_OPEN" for x in b),
            "exit_reason_counts": dict(Counter(x["exit_reason"] for x in b)),
            "expost_touch_order_counts": dict(Counter(
                p["target_vs_stop_touch_order_EXPOST"][key] for p in completed
            )),
        }
    return {
        "n_selected": len(paths), "n_complete": len(completed),
        "n_unknown": len(paths) - len(completed),
        "mean_net_pct": _mean(net), "median_net_pct": round(statistics.median(net), 6) if net else None,
        "p10_net_pct": _quantile(net, .10), "p25_net_pct": _quantile(net, .25),
        "p75_net_pct": _quantile(net, .75), "worst_net_pct": min(net) if net else None,
        "win_count_net": sum(x > 0 for x in net),
        "win_rate_net_pct": round(100.0 * sum(x > 0 for x in net) / len(net), 3) if net else None,
        "mean_mfe_expost_pct": _mean([p["mfe_high_from_entry_pct_EXPOST"] for p in completed]),
        "mean_mae_expost_pct": _mean([p["mae_low_from_entry_pct_EXPOST"] for p in completed]),
        "median_mfe_expost_pct": round(statistics.median(mfe), 6) if mfe else None,
        "p75_mfe_expost_pct": _quantile(mfe, .75),
        "median_mae_expost_pct": round(statistics.median(mae), 6) if mae else None,
        "median_peak_day_expost": round(statistics.median([p["peak_high_day_EXPOST"] for p in completed]), 3)
        if completed else None,
        "target_touch_expost": {str(x): touch_rate("first_target_touch_day_EXPOST", x)
                                for x in (5, 10, 15)},
        "stop_touch_expost": {str(x): touch_rate("first_stop_touch_day_EXPOST", x)
                              for x in (5, 10)},
        "fixed_brackets_pessimistic_daily_ohlc": {
            f"TAKE_{target}_STOP_{stop}": bracket_stats(target, stop) for target, stop in BRACKETS
        },
        "mean_next_open_gap_pct": _mean([p["gap_from_signal_close_pct"] for p in completed]),
    }


def failure_feature_profile(rows: list[dict]) -> dict:
    """Descriptive only: pre-D0 features of fixed +10/-5 outcomes.

    Never use these ex-post labels to choose a threshold or alter R5 signals.
    """
    target, stop, other = [], [], []
    for row in rows:
        h20 = row["horizons_EXPOST"]["20"]
        if not h20["status"].startswith("DESCRIPTIVE_"):
            continue
        result = h20["predeclared_bracket_models"]["TAKE_10_STOP_5"]
        (target if result["hit_target"] else stop if result["hit_stop"] else other).append(row)
    def median_feature(sample: list[dict], name: str) -> dict:
        vals = [float(e["asof_d0_features"][name]) for e in sample
                if e["asof_d0_features"].get(name) is not None]
        return {"n_known": len(vals), "median": round(statistics.median(vals), 6) if vals else None}
    return {
        "label": "FIXED_TAKE_10_STOP_5_D20_ONLY_NO_PREDICTION_CLAIM",
        "target_first_count": len(target), "stop_first_or_same_bar_count": len(stop),
        "neither_count": len(other),
        "features": {name: {
            "target_first": median_feature(target, name),
            "stop_first": median_feature(stop, name),
            "neither": median_feature(other, name),
        } for name in SIGNAL_FEATURES},
        "descriptive_only_no_out_of_sample_classifier": True,
    }


def year_stock_reference(signals: list[dict], refs: list[dict], h: int) -> dict:
    """Event-weighted *non-signal* same stock/year reference, not a paired trial."""
    buckets: dict[tuple[str, int], list[float]] = {}
    for ref in refs:
        path = ref["horizons_EXPOST"][str(h)]
        if path["status"].startswith("DESCRIPTIVE_"):
            buckets.setdefault((ref["code"], ref["year"]), []).append(path["net_total20bps_pct"])
    eligible = [s for s in signals if s["horizons_EXPOST"][str(h)]["status"].startswith("DESCRIPTIVE_")]
    means, missing = [], []
    for signal in eligible:
        key = (signal["code"], signal["year"])
        if key in buckets:
            means.append(statistics.mean(buckets[key]))
        else:
            missing.append(f"{key[0]}:{key[1]}")
    signal_mean = _mean([s["horizons_EXPOST"][str(h)]["net_total20bps_pct"] for s in eligible])
    ref_mean = _mean(means)
    return {
        "method": "EVENT_WEIGHTED_SAME_STOCK_YEAR_ALL_ELIGIBLE_NON_SIGNAL_DAYS",
        "complete_signals": len(eligible), "matched_signals": len(means),
        "unmatched_cohorts": sorted(set(missing)),
        "reference_mean_net_pct": ref_mean,
        "signal_minus_reference_percentage_points": round(signal_mean - ref_mean, 6)
        if signal_mean is not None and ref_mean is not None and not missing else None,
        "reference_paths_are_overlapping": True,
        "is_independent_control_trial": False,
    }


def group_stats(signals: list[dict], refs: list[dict]) -> dict:
    return {str(h): {
        "signal": summarize(signals, h),
        "same_stock_year_reference": year_stock_reference(signals, refs, h),
    } for h in HORIZONS}


def study(repo: Path = ROOT) -> dict:
    r5 = r5_module(repo)
    base = r5.study(repo)
    if (base["line_events"], base["unique_stock_dates"]) != (506, 368):
        raise AssertionError("FROZEN_R5_COUNTS_DRIFTED")
    prepare = r5._modules(repo)[0]
    raw = {c: r5.frozen_data(repo, c) for c in r5.SOURCES}
    raw.update({c: extra_frozen_prices(repo, c) for c in ADDITIONAL_SOURCES})
    prices = {c: prepare(df) for c, df in raw.items()}

    extras = [e for code in ADDITIONAL_SOURCES for e in r5.scan_stock(raw[code], repo)]
    grouped = {
        "FROZEN_R5_THREE": primary_signals(base["events"]),
        "ADDITIONAL_EXISTING_TWO": primary_signals(extras),
    }
    if len(grouped["FROZEN_R5_THREE"]) != 368:
        raise AssertionError("R5_UNIQUE_DATES_DRIFTED")
    event_rows = [metric_row(e, prices[e["code"]], cohort)
                  for cohort in COHORTS for e in grouped[cohort]]
    # Reproduce R5's original D+20 net and path extremes, not a new signal system.
    for event in grouped["FROZEN_R5_THREE"]:
        legacy = event["future_paths_EXPOST"]["20"]
        observed = path_at_close(prices[event["code"]], int(event["idx"]), 20)
        if legacy["status"].startswith("PROVISIONAL_"):
            if not observed["status"].startswith("DESCRIPTIVE_"):
                raise AssertionError("R5_R12_FUTURE_COMPLETENESS_DRIFT")
            for old, new in (("net_pct_by_total_cost_bps", "net_total20bps_pct"),
                             ("mfe_from_entry_pct", "mfe_high_from_entry_pct_EXPOST"),
                             ("mae_from_entry_pct", "mae_low_from_entry_pct_EXPOST")):
                legacy_value = legacy[old]["20"] if old == "net_pct_by_total_cost_bps" else legacy[old]
                if abs(float(legacy_value) - observed[new]) > 0.00001:
                    raise AssertionError("R5_R12_OUTCOME_PARITY_DRIFT " + new)
        elif observed["status"].startswith("DESCRIPTIVE_"):
            raise AssertionError("R5_R12_UNKNOWN_FUTURE_DRIFT")
    reference_rows = [row for c, df in prices.items() for row in reference_days(
        df, {int(e["idx"]) for e in grouped["FROZEN_R5_THREE" if c in r5.SOURCES else "ADDITIONAL_EXISTING_TWO"] if e["code"] == c}, c
    )]
    results = {}
    for cohort in COHORTS:
        events = [e for e in event_rows if e["cohort"] == cohort]
        refs = [r for r in reference_rows if (r["code"] in r5.SOURCES) == (cohort == "FROZEN_R5_THREE")]
        compact = nonoverlap_signals(events)
        results[cohort] = {
            "unique_signal_days": len(events), "nonoverlap_20bar_days": len(compact),
            "all_unique_days_OVERLAPPING": group_stats(events, refs),
            "nonoverlap_20bar_PRIMARY": group_stats(compact, refs),
            "failed_vs_target_asof_feature_profile_EXPOST": failure_feature_profile(compact),
            "by_year_nonoverlap": {str(y): group_stats([e for e in compact if e["year"] == y], refs)
                                   for y in sorted({e["year"] for e in compact})},
        }
    return {
        "revision": "R12_FROZEN_R5_ENTRY_EXPECTANCY_FINITE_DECISION", "research_only": True,
        "r5_source_sha256": R5_SHA256,
        "frozen_r5_inputs_sha256": r5.SOURCES, "additional_inputs_sha256": ADDITIONAL_SOURCES,
        "signal_rule_unchanged": "R116_PREVIOUS_ACTIVE_UPPER_RESISTANCE_CROSS_AT_D0_CLOSE",
        "dedup_rule": "MEDIUM_ELSE_LONG_ELSE_SHORT_THEN_LINE_ID",
        "entry": "D_PLUS_1_OPEN", "exit": "D_PLUS_H_CLOSE", "horizons": list(HORIZONS),
        "total_roundtrip_cost_bps": COST_BPS, "nonoverlap_cooldown_bars": NONOVERLAP_BARS,
        "predeclared_daily_ohlc_brackets": [f"TAKE_{p}_STOP_{s}" for p, s in BRACKETS],
        "same_bar_target_stop_rule": "PESSIMISTIC_STOP_FIRST",
        "stop_gap_fill": "ACTUAL_NEXT_OPEN_IF_WORSE_THAN_STOP",
        "target_gap_fill": "CONSERVATIVE_TARGET_LIMIT_PRICE",
        "reference_min_prior_bars": MIN_REFERENCE_PRIOR_BARS,
        "reference_rule": "ALL_NON_SIGNAL_DAYS_SAME_STOCK_YEAR_NOT_RANDOM_MATCHING",
        "r5_frozen_line_events": base["line_events"],
        "additional_line_events": len(extras),
        "cohorts": results, "signal_event_rows": event_rows,
        "reference_daily_rows": reference_rows,
        "scope_limits": [
            "Original three tickers were handpicked; not full-market or out-of-sample.",
            "Additional two tickers preexist in historical project; NOT independent unseen OOS.",
            "Per-ticker 20-bar cooldown does not remove across-stock or market correlation.",
            "Daily high/low MFE/MAE are ex-post, not executable intraday exits.",
            "A target/stop hit on the same daily candle has unknowable intraday order; fixed bracket assumes stop first.",
            "Failure/target ex-post feature profiles must never be treated as fitted predictive rules.",
            "Same-stock/year baseline is overlapping, unrandomized and regime-confounded.",
            "Frozen KRX files have unresolved historical corporate-action/provenance caveats.",
            "Gross-to-net subtracts only assumed total 20bps, no slippage/fill guarantee.",
        ],
        "can_publish_tradable_edge": False, "strategy_winner": None,
        "can_modify_live_search_watch": False,
        "next_decision": "STOP_EXIT_TUNING; ANY_POSITIVE_HINT_NEEDS_NEW_INDEPENDENT_UNIVERSE_AND_TIME",
    }


def run(repo: Path, out: Path) -> dict:
    result = study(repo)
    out.mkdir(parents=True, exist_ok=True)
    summary = {k: v for k, v in result.items() if k not in ("signal_event_rows", "reference_daily_rows")}
    (out / "R12_SIGNAL_EXPECTANCY_SUMMARY.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    (out / "R12_SIGNAL_EXPECTANCY_FULL_AUDIT.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    records = []
    nonoverlap_keys = {(e["code"], e["signal_date"]) for c in COHORTS
                       for e in nonoverlap_signals([r for r in result["signal_event_rows"] if r["cohort"] == c])}
    for e in result["signal_event_rows"]:
        for h, path in e["horizons_EXPOST"].items():
            records.append({k: v for k, v in e.items() if k != "horizons_EXPOST"} | {
                "nonoverlap_20bar": (e["code"], e["signal_date"]) in nonoverlap_keys,
                "horizon": int(h), **path,
            })
    pd.DataFrame(records).to_csv(out / "R12_SIGNAL_EVENT_PATHS.csv", index=False, encoding="utf-8-sig")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    report = run(Path(args.repo).resolve(), Path(args.out).resolve())
    print(json.dumps({"revision": report["revision"], "cohorts": {
        c: {"unique": report["cohorts"][c]["unique_signal_days"],
            "nonoverlap": report["cohorts"][c]["nonoverlap_20bar_days"],
            "d20": report["cohorts"][c]["nonoverlap_20bar_PRIMARY"]["20"]}
        for c in COHORTS}}, ensure_ascii=False, indent=2))
