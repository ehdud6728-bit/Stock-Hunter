#!/usr/bin/env python3
"""R10 RESEARCH_ONLY: frozen point-in-time resistance-based exit hypotheses.

Historical daily trading *amount* assigned to HLC3 is a diagnostic proxy, NOT
actual volume-at-price or signed order flow. D0 parameters are never refit.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
R7_SHA256 = "d3ed459f5dd0c4c747502a450c38c08d305c3f28e5f828cdeeb0f7af65c88e6a"
LOOKBACK = 120
PIVOT_LEFT = 2
PIVOT_RIGHT = 2
PIVOT_CLUSTER_PCT = .01
MIN_TARGET_ABOVE_D0_PCT = .02
PROXY_BIN_PCT_D0 = .02
PROXY_MIN_DAYS = 3
PROXY_AMOUNT_TO_MEDIAN = 1.5
HORIZON = 20
COST_BPS = 20
ATR_MULT = 3.0
METHODS = ("PRIOR_PIVOT_120", "DAILY_AMOUNT_HLC3_PROXY_120")
RULES = ("TARGET_ONLY", "TARGET_ATR_INITIAL_3")
BASELINES = ("TIME_D20", "TARGET_5", "TARGET_10", "ATR_INITIAL_3")


def frozen_r7(repo: Path = ROOT):
    source = repo / "research_projects/trendline_multifactor_exit_r7_20261119/r7_multifactor.py"
    if hashlib.sha256(source.read_bytes()).hexdigest() != R7_SHA256:
        raise ValueError("CANONICAL_R7_SOURCE_SHA256_MISMATCH")
    sys.path.insert(0, str(source.parent))
    import r7_multifactor
    return r7_multifactor


def _positive(*values) -> bool:
    return all(math.isfinite(float(v)) and float(v) > 0 for v in values)


def resistance_at_d0(data: pd.DataFrame, t: int, method: str) -> dict:
    """Compute target using D-120..D-1 only; D0 close is a known-after-close gate.

    Pivot's right-side confirmation occurs at least two sessions before D0.
    The proxy ATTRIBUTES all of a day's trading amount to HLC3, which must
    never be represented as the true executed-price distribution.
    """
    if method not in METHODS:
        raise ValueError("UNREGISTERED_RESISTANCE_METHOD")
    if t < LOOKBACK or t >= len(data):
        return {"status": "UNKNOWN_INSUFFICIENT_PRE_D0_HISTORY", "target": None}
    d0_close = float(data.iloc[t].close)
    prior = data.iloc[t-LOOKBACK:t]
    if not _positive(d0_close):
        return {"status": "UNKNOWN_INVALID_D0_CLOSE", "target": None}
    if not all(_positive(*[float(row[k]) for k in ("high", "low", "close")])
               and float(row.high) >= float(row.low)
               for _, row in prior.iterrows()):
        return {"status": "UNKNOWN_INVALID_PRIOR_PRICE", "target": None}
    floor = d0_close * (1 + MIN_TARGET_ABOVE_D0_PCT)

    if method == "PRIOR_PIVOT_120":
        highs = [float(x) for x in prior.high]
        # Strict left/right confirmation, avoiding ties and future bars.
        pivots = [highs[k] for k in range(PIVOT_LEFT, LOOKBACK-PIVOT_RIGHT)
                  if all(highs[k] > highs[j]
                         for j in range(k-PIVOT_LEFT, k+PIVOT_RIGHT+1) if j != k)]
        eligible = sorted(x for x in pivots if x >= floor)
        # Two independently confirmed pivot highs near one another. The
        # level is the lower pivot: conservative nearest overhead boundary.
        pairs = [(a, b) for i, a in enumerate(eligible)
                 for b in eligible[i+1:] if b <= a*(1+PIVOT_CLUSTER_PCT)]
        if not pairs:
            return {"status": "UNKNOWN_NO_CONFIRMED_PRIOR_PIVOT_CLUSTER",
                    "target": None, "confirmed_pivots": len(pivots)}
        target = min(a for a, _ in pairs)
        return {"status": "KNOWN_AFTER_D0_CLOSE", "target": round(target, 6),
                "confirmed_pivots": len(pivots), "method": method}

    amounts = [float(x) for x in prior.amount]
    if not all(math.isfinite(a) and a >= 0 for a in amounts) or sum(amounts) <= 0:
        return {"status": "UNKNOWN_INVALID_OR_MISSING_TRADING_AMOUNT", "target": None}
    width = d0_close * PROXY_BIN_PCT_D0
    bins: dict[int, dict] = {}
    for row in prior.itertuples(index=False):
        key = math.floor(((float(row.high)+float(row.low)+float(row.close))/3) / width)
        bucket = bins.setdefault(key, {"days": 0, "amount": 0.})
        bucket["days"] += 1
        bucket["amount"] += float(row.amount)
    occupied = [b["amount"] for b in bins.values() if b["amount"] > 0]
    median_amount = statistics.median(occupied)
    qualified = [key*width for key, b in bins.items()
                 if key*width >= floor and b["days"] >= PROXY_MIN_DAYS
                 and b["amount"] >= PROXY_AMOUNT_TO_MEDIAN*median_amount]
    if not qualified:
        return {"status": "UNKNOWN_NO_QUALIFYING_DAILY_AMOUNT_HLC3_PROXY_BIN",
                "target": None, "occupied_bins": len(occupied)}
    return {"status": "KNOWN_AFTER_D0_CLOSE", "target": round(min(qualified), 6),
            "occupied_bins": len(occupied), "method": method,
            "actual_volume_at_price": False}


def simulate_target(bars: pd.DataFrame, entry: float, target: float,
                    atr_d0: float | None = None, *, with_atr_stop: bool = False) -> dict:
    """Fixed before entry; intraday stop-target tie resolves STOP FIRST.

    D+1 entry open already >= D0 target means there was never a usable
    forward target at entry, even if the gap later retraces.
    """
    if len(bars) != HORIZON:
        return {"status": "UNKNOWN_INCOMPLETE_DPLUS20_PATH"}
    if not _positive(entry, target) or not all(
        _positive(*[float(getattr(row, k)) for k in ("open", "high", "low", "close")])
        and float(row.low) <= min(float(row.open), float(row.close), float(row.high))
        and float(row.high) >= max(float(row.open), float(row.close))
        for row in bars.itertuples(index=False)):
        return {"status": "UNKNOWN_INVALID_DAILY_PRICE"}
    if entry >= target:
        return {"status": "UNKNOWN_ENTRY_ALREADY_AT_OR_ABOVE_FROZEN_TARGET"}
    if with_atr_stop and (atr_d0 is None or not _positive(atr_d0)):
        return {"status": "UNKNOWN_D0_ATR"}
    stop = entry - ATR_MULT*float(atr_d0) if with_atr_stop else None
    for day, row in enumerate(bars.itertuples(index=False), start=1):
        op, hi, lo = float(row.open), float(row.high), float(row.low)
        if stop is not None and op <= stop:
            return _exit(bars, entry, day, op, "STOP_GAP_DOWN_OPEN", gap_stop=True)
        if op >= target:
            return _exit(bars, entry, day, target, "FROZEN_TARGET_LIMIT_GAP_CONSERVATIVE")
        hit_stop = stop is not None and lo <= stop
        hit_target = hi >= target
        if hit_stop:
            return _exit(bars, entry, day, stop,
                         "STOP_FIRST_ON_AMBIGUOUS_DAILY_BAR" if hit_target else "INITIAL_ATR_STOP",
                         ambiguous=bool(hit_target))
        if hit_target:
            return _exit(bars, entry, day, target, "FROZEN_TARGET_LIMIT_TOUCH")
    return _exit(bars, entry, HORIZON, float(bars.iloc[-1].close), "TIME_D20_FALLBACK")


def _exit(bars: pd.DataFrame, entry: float, day: int, price: float,
          reason: str, *, ambiguous: bool = False, gap_stop: bool = False) -> dict:
    gross = 100 * (price/entry-1)
    return {"status": "PROVISIONAL_3STOCK_DAILY_OHLC_NOT_OOS",
            "exit_day": day, "exit_date": str(bars.iloc[day-1].date.date()),
            "exit_price": round(price, 6), "reason": reason,
            "net_total20bps_pct": round(gross - COST_BPS/100, 6),
            "same_bar_stop_target_ambiguous": ambiguous, "gap_stop": gap_stop}


def stats(rows: list[dict]) -> dict:
    ok = [r for r in rows if r["status"].startswith("PROVISIONAL_")]
    values = sorted(r["net_total20bps_pct"] for r in ok)
    return {"n": len(rows), "complete": len(ok), "unknown": len(rows)-len(ok),
            "mean_pct": round(statistics.mean(values), 6) if values else None,
            "median_pct": round(statistics.median(values), 6) if values else None,
            "p10_pct": round(values[int((len(values)-1)*.1)], 6) if values else None,
            "win_pct": round(100*sum(x>0 for x in values)/len(values), 4) if values else None,
            "median_exit_day": statistics.median(r.get("exit_day", r.get("day")) for r in ok) if ok else None,
            "reason_counts": dict(Counter(r["reason"] for r in ok))}


def _group(records: list[dict]) -> dict:
    result = {"n": len(records), "methods": {}}
    for method in METHODS:
        ready = [r for r in records if r["resistance"][method]["status"] == "KNOWN_AFTER_D0_CLOSE"]
        method_data = {"target_known": len(ready), "target_unknown": len(records)-len(ready),
                       "target_statuses": dict(Counter(r["resistance"][method]["status"] for r in records)),
                       "rules": {}}
        for rule in RULES:
            name = method + "/" + rule
            model = [r["models"][name] for r in records]
            common = [r for r in records if r["models"][name]["status"].startswith("PROVISIONAL_")
                      and all(r["models"][b]["status"].startswith("PROVISIONAL_") for b in BASELINES)]
            method_data["rules"][rule] = {
                "all_events": stats(model), "identical_cohort_n": len(common),
                "identical_cohort": {name: stats([r["models"][name] for r in common])
                                     for name in (name, *BASELINES)},
                "paired_mean_difference_vs_baselines_pp": {
                    b: round(statistics.mean(r["models"][name]["net_total20bps_pct"] -
                        r["models"][b]["net_total20bps_pct"] for r in common), 6)
                    if common else None for b in BASELINES},
            }
        result["methods"][method] = method_data
    return result


def study(repo: Path = ROOT) -> dict:
    r7 = frozen_r7(repo)
    r6 = r7.frozen_r6(repo)
    frozen = r6.study(repo)
    r5 = r6.r5_module(repo)
    prepare = r5._modules(repo)[0]
    prices = {code: prepare(r5.frozen_data(repo, code)) for code in r5.SOURCES}
    atr = {code: r7.wilder_atr(df) for code, df in prices.items()}
    primary = r6.primary_events(frozen["events"])
    if len(frozen["events"]) != 506 or len(primary) != 368:
        raise AssertionError("FROZEN_R5_R6_POPULATION_CHANGED")
    no_overlap = {(e["code"], e["idx"], e["line_id"])
                  for e in r7.nonoverlap_by_stock(primary, horizon=HORIZON)}
    records = []
    for e in primary:
        code, t = e["code"], int(e["idx"])
        df = prices[code]
        bars = df.iloc[t+1:t+HORIZON+1]
        entry = float(df.iloc[t+1].open) if t+1 < len(df) else math.nan
        resistances = {m: resistance_at_d0(df, t, m) for m in METHODS}
        models = {b: e["exit_models"][b] for b in BASELINES if b != "ATR_INITIAL_3"}
        models["ATR_INITIAL_3"] = r7.simulate(df, t, atr[code], "ATR_INITIAL_3")
        for method, feature in resistances.items():
            for rule in RULES:
                name = method + "/" + rule
                models[name] = (simulate_target(bars, entry, float(feature["target"]),
                                atr_d0=float(atr[code].iloc[t]),
                                with_atr_stop=(rule == "TARGET_ATR_INITIAL_3"))
                                if feature["status"] == "KNOWN_AFTER_D0_CLOSE" else
                                {"status": feature["status"]})
        records.append({"code": code, "date": e["date"], "idx": t,
                        "line_id": e["line_id"], "scale": e["scale"],
                        "nonoverlap_20d": (code, t, e["line_id"]) in no_overlap,
                        "entry_dplus1_open": entry if math.isfinite(entry) else None,
                        "resistance": resistances, "models": models})
    return {
        "revision": "R10_PIT_RESISTANCE_EXIT_RESEARCH_ONLY_3STOCK_PROVISIONAL",
        "research_only": True, "source_population": "FROZEN_R5_R6_3_HANDPICKED_STOCKS",
        "raw_line_events": len(frozen["events"]), "unique_stock_dates": len(records),
        "nonoverlap_20d_count": sum(x["nonoverlap_20d"] for x in records),
        "predeclared_parameters": {"lookback_prior_days": LOOKBACK,
            "prior_pivot_left_right_confirmed": [PIVOT_LEFT, PIVOT_RIGHT],
            "pivot_cluster_within_pct": PIVOT_CLUSTER_PCT*100,
            "min_target_above_D0_close_pct": MIN_TARGET_ABOVE_D0_PCT*100,
            "proxy_bin_width_pct_D0_close": PROXY_BIN_PCT_D0*100,
            "proxy_bin_min_daily_bars": PROXY_MIN_DAYS,
            "proxy_bin_total_amount_over_median": PROXY_AMOUNT_TO_MEDIAN,
            "entry": "DPLUS1_OPEN", "max_exit_horizon": HORIZON,
            "total_cost_bps": COST_BPS, "atr_stop": "ENTRY_MINUS_3_TIMES_D0_WILDER_ATR14",
            "stop_target_same_day": "STOP_FIRST", "gap_down_stop": "FILL_OPEN",
            "gap_up_target": "LIMIT_AT_TARGET", "incomplete": "UNKNOWN",
            "d0_target_known_asof": "AFTER_D0_CLOSE_FROM_PRE_D0_BARS_ONLY"},
        "actual_volume_at_price_available": False,
        "daily_amount_hlc3_proxy_is_real_volume_profile": False,
        "marketwide_point_in_time_universe": False,
        "independent_price_adjustment_certified": False,
        "out_of_sample": False, "strategy_winner": None,
        "can_publish_strategy_performance": False,
        "can_modify_live_search_watch": False,
        "primary": _group(records),
        "nonoverlap_20d": _group([r for r in records if r["nonoverlap_20d"]]),
        "by_code": {code: _group([r for r in records if r["code"] == code])
                    for code in sorted(r5.SOURCES)},
        "records": records,
    }


def run(repo: Path, out: Path) -> dict:
    result = study(repo)
    out.mkdir(parents=True, exist_ok=True)
    (out/"R10_FULL_AUDIT.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)+"\n", encoding="utf-8")
    (out/"R10_SUMMARY.json").write_text(
        json.dumps({k: v for k, v in result.items() if k != "records"},
                   ensure_ascii=False, indent=2, allow_nan=False)+"\n", encoding="utf-8")
    flat = []
    for e in result["records"]:
        for rule, model in e["models"].items():
            method = rule.split("/")[0] if "/" in rule else None
            flat.append({"code": e["code"], "date": e["date"], "idx": e["idx"],
                         "line_id": e["line_id"], "nonoverlap_20d": e["nonoverlap_20d"],
                         "method": method, "rule": rule,
                         "frozen_target": e["resistance"][method]["target"] if method else None,
                         "status": model["status"], "exit_day": model.get("exit_day", model.get("day")),
                         "reason": model.get("reason"), "net20bps_pct": model.get("net_total20bps_pct")})
    pd.DataFrame(flat).to_csv(out/"R10_PRIMARY_EVENTS_AND_MODELS.csv",
                              index=False, encoding="utf-8-sig")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    result = run(Path(args.repo).resolve(), Path(args.out))
    print(json.dumps({k: v for k, v in result.items() if k != "records"}, ensure_ascii=False))
