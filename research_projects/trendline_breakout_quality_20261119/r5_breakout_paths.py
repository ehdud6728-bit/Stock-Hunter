#!/usr/bin/env python3
"""R5: frozen 3-stock R116 prior-resistance breakout, causal D0 vs future paths.

Research-only discovery, NOT a representative universe, OOS or strategy test.
No ENV lower-touch events are merged into this independent trendline track.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SOURCES = {
    "006400": "d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566",
    "039840": "330ffdc5febbbf76f3f959d72181a2a37cc144ec567e6e7f691985b7dc82e3e4",
    "071840": "aba4ffb8702961fbc8d3d411138b755092551f2fd6ef1a2ac3ce02da2c221c1b",
}
ORIGINAL_CODE_SHA256 = {
    "r1d111/algorithms.py": "8e6026a21c793de7d453912696cfe8dfcf72324acd63db8fb264ff36053b803a",
    "r1d116/recent_turn.py": "c2e11143bba8382ff765bdd69e3137e4ecf87bb68bab839ff48fec211eea85ff",
}
HORIZONS = (1, 3, 5, 10, 20)
COSTS = (10, 20, 50)
BREAK_CROSS = "CROSSED_ABOVE_PREVIOUS_ACTIVE_RESISTANCE"
REPLAY_ALGORITHM = "TS_BODY_WICK_PROXY"


def frozen_data(repo: Path, code: str) -> pd.DataFrame:
    p = repo / "research_projects/r1d97/data" / f"{code}_KRX.csv"
    if code not in SOURCES or hashlib.sha256(p.read_bytes()).hexdigest() != SOURCES[code]:
        raise ValueError("FROZEN_INPUT_SHA256_MISMATCH " + code)
    data = pd.read_csv(p, dtype={"code": str})
    if not len(data) or set(data.code) != {code} or data.date.duplicated().any():
        raise AssertionError("INVALID_FROZEN_STOCK_DATES " + code)
    return data


def _modules(repo: Path):
    for relative, expected in ORIGINAL_CODE_SHA256.items():
        src = repo / "research_projects" / relative
        if hashlib.sha256(src.read_bytes()).hexdigest() != expected:
            raise ValueError("CANONICAL_TRENDLINE_CODE_CHANGED_REQUIRE_REAUDIT " + relative)
    sys.path.insert(0, str(repo / "research_projects/r1d111"))
    sys.path.insert(0, str(repo / "research_projects/r1d116"))
    from algorithms import prepare, make_anchors, generate_candidates
    from recent_turn import replay_one, line_level
    return prepare, make_anchors, generate_candidates, replay_one, line_level


def replay_stock(raw: pd.DataFrame, repo: Path = ROOT):
    prepare, make_anchors, generate_candidates, replay_one, line_level = _modules(repo)
    data = prepare(raw)
    catalogue = generate_candidates(data, REPLAY_ALGORITHM, make_anchors(data))
    marks = pd.DataFrame({
        "idx": np.arange(len(data)), "date": data.date.dt.strftime("%Y-%m-%d"),
        "close": data.close,
    })
    daily, _, _ = replay_one(str(raw.iloc[0].code), REPLAY_ALGORITHM, catalogue, marks)
    lookup = {q["line_id"]: q for q in catalogue}
    return data, daily, lookup, line_level


def _percent(a: float, b: float):
    return round(100 * (a / b - 1), 6) if b > 0 else None


def wave_proxy(past: pd.DataFrame) -> dict:
    """Strictly pre-D0 close trough to peak, then partial retracement at D-1."""
    if len(past) != 60:
        return {"wave_proxy": "UNKNOWN_PRIOR60"}
    min_price = float(past.iloc[0].close)
    best_advance, peak, trough = 0., 0., 0.
    for bar in past.iloc[1:-2].itertuples():
        c = float(bar.close)
        advance = c / min_price - 1
        if advance > best_advance:
            best_advance, peak, trough = advance, c, min_price
        min_price = min(min_price, c)
    end = float(past.iloc[-1].close)
    retrace = (peak - end) / (peak - trough) if peak > trough else None
    eligible = best_advance >= .15 and retrace is not None and .2 <= retrace <= .8
    return {
        "wave_proxy": "CANDIDATE" if eligible else "NO_CANDIDATE",
        "wave_max_gain_pct": round(100 * best_advance, 6),
        "wave_retrace_fraction": round(retrace, 6) if retrace is not None else None,
    }


def prebreak_features(data: pd.DataFrame, t: int, line: dict, line_level) -> dict:
    """No price, volume or threshold used here has an index > t."""
    if int(line["known_idx"]) >= t:
        raise AssertionError("UNCONFIRMED_LINE")
    p = data.iloc[t]
    prior = data.iloc[max(0, t-20):t]
    older = data.iloc[max(0, t-60):t]
    level = line_level(line, t)
    if not level > 0:
        raise AssertionError("NONPOSITIVE_ORIGINAL_LINE")
    op, hi, lo, close = (float(p[k]) for k in ("open", "high", "low", "close"))
    if len(prior) < 20:
        return {
            "line_anchor1": line["a1"], "line_anchor2": line["a2"],
            "line_known_date": line["known_date"],
            "line_known_idx": int(line["known_idx"]),
            "break_line_level_d0": round(level, 6),
            "d0_close_vs_frozen_line_pct": _percent(close, level),
            "context_status": "UNKNOWN_LESS_THAN_20_PRIOR_BARS",
            "features_asof": "AFTER_D0_CLOSE_NO_FUTURE_BARS",
        }
    av_vol = float(prior.volume.mean()); av_amt = float(prior.amount.mean())
    spread = hi - lo
    result = {
        "line_anchor1": line["a1"], "line_anchor2": line["a2"],
        "line_known_date": line["known_date"], "line_known_idx": int(line["known_idx"]),
        "line_raw_anchor1": float(line["v1"]), "line_raw_anchor2": float(line["v2"]),
        "line_slope_per_bar": round(float(line["m"]), 6),
        "break_line_level_d0": round(level, 6),
        "d0_close_vs_frozen_line_pct": _percent(close, level),
        "d0_open_above_line": op > level,
        "d0_open_below_line_then_high_crossed": op <= level < hi,
        "d0_gap_vs_prev_close_pct": _percent(op, float(prior.iloc[-1].close)),
        "d0_close_location": round((close - lo) / spread, 6) if spread else None,
        "d0_upper_wick_fraction": round((hi - max(op, close)) / spread, 6) if spread else None,
        "d0_volume_vs_pre20": round(float(p.volume) / av_vol, 6) if av_vol else None,
        "d0_amount_vs_pre20": round(float(p.amount) / av_amt, 6) if av_amt else None,
        "prior5_vol_vs_prior20": round(float(prior.tail(5).volume.mean()) / av_vol, 6) if av_vol else None,
        "prior20_high": float(prior.high.max()), "prior20_low": float(prior.low.min()),
        "d0_close_vs_prior20_high_pct": _percent(close, float(prior.high.max())),
        "d_minus_1_close_rank_in_prior20_range": round(
            (float(prior.iloc[-1].close) - float(prior.low.min())) /
            (float(prior.high.max()) - float(prior.low.min())), 6
        ) if float(prior.high.max()) > float(prior.low.min()) else None,
        "price_source": "PINNED_3_STOCK_KRX_2021_2024_UNCERTIFIED_INDEPENDENCE",
        "features_asof": "AFTER_D0_CLOSE_NO_FUTURE_BARS",
        "context_status": "KNOWN_PRE20" if len(older) < 60 else "KNOWN_PRE60",
    }
    for n in (20, 60, 112, 224):
        ma = float(data.iloc[t-n+1:t+1].close.mean()) if t+1 >= n else None
        result[f"d0_close_vs_ma{n}_pct"] = _percent(close, ma) if ma else None
        result[f"ma{n}_status"] = "EXACT_RAW_D0" if ma else "UNKNOWN_INSUFFICIENT_HISTORY"
    result.update(wave_proxy(older))
    return result


def future_paths(data: pd.DataFrame, t: int, line: dict, line_level) -> dict:
    """Only labels and diagnostics, NEVER D0 filters/selection."""
    results = {}
    for h in HORIZONS:
        path = data.iloc[t+1:t+h+1]
        if len(path) != h:
            results[str(h)] = {"status": "UNKNOWN_INCOMPLETE_FUTURE_PATH"}
            continue
        entry = float(path.iloc[0].open)
        terminal = float(path.iloc[-1].close)
        if min(entry, terminal) <= 0:
            results[str(h)] = {"status": "UNKNOWN_INVALID_PRICE"}
            continue
        gross = _percent(terminal, entry)
        trough, peak = float(path.low.min()), float(path.high.max())
        relost = any(
            float(row.close) < line_level(line, t+step) * .996
            for step, row in enumerate(path.itertuples(index=False), 1)
        )
        retest = any(
            float(row.low) <= line_level(line, t+step) * 1.004 and
            float(row.high) >= line_level(line, t+step) * .996
            for step, row in enumerate(path.itertuples(index=False), 1)
        )
        results[str(h)] = {
            "status": "PROVISIONAL_3_STOCK_SOURCE_NOT_OOS",
            "entry_date": str(path.iloc[0].date.date()),
            "exit_date": str(path.iloc[-1].date.date()),
            "gross_pct": gross,
            "net_pct_by_total_cost_bps": {
                str(cost): round(gross-cost/100, 6) for cost in COSTS
            },
            "mfe_from_entry_pct": _percent(peak, entry),
            "mae_from_entry_pct": _percent(trough, entry),
            "relost_same_frozen_line_by_h": bool(relost),
            "retested_same_frozen_line_by_h": bool(retest),
            "label_only": True,
        }
    return results


def scan_stock(raw: pd.DataFrame, repo: Path = ROOT):
    d, daily, lookup, line_level = replay_stock(raw, repo)
    events = []
    chosen = daily.loc[
        daily.kind.eq("UPPER") & daily.prior_line_cross.eq(BREAK_CROSS)
    ]
    for row in chosen.itertuples(index=False):
        t = int(row.idx); line_id = str(row.prior_line_id)
        if line_id not in lookup:
            raise AssertionError("PRIOR_ACTIVE_LINE_MISSING_IN_ORIGINAL_CATALOG")
        line = lookup[line_id]
        if int(line["known_idx"]) >= t-1:
            raise AssertionError("NEW_LINE_CANNOT_CAUSE_SAME_DAY_CROSS")
        prev, close = float(d.iloc[t-1].close), float(d.iloc[t].close)
        if not (
            prev <= line_level(line, t-1) + .004 * prev and
            close > line_level(line, t) + .004 * close
        ):
            raise AssertionError("R116_CROSS_INCONSISTENT_WITH_ORIGINAL_GEOMETRY")
        features = prebreak_features(d, t, line, line_level)
        events.append({
            "code": str(row.code), "date": str(row.date), "year": int(str(row.date)[:4]),
            "idx": t, "scale": str(row.scale), "kind": "UPPER",
            "line_id": line_id, "cross": BREAK_CROSS,
            **features, "future_paths_EXPOST": future_paths(d, t, line, line_level),
        })
    return events


def _summary_group(rows: list[dict]) -> dict:
    d20 = [r["future_paths_EXPOST"]["20"] for r in rows]
    available = [f for f in d20 if f["status"].startswith("PROVISIONAL_")]
    vals = [f["net_pct_by_total_cost_bps"]["20"] for f in available]
    return {
        "line_events": len(rows), "unique_stock_dates": len({(r["code"], r["date"]) for r in rows}),
        "dplus20_complete": len(available), "dplus20_unknown": len(rows)-len(available),
        "dplus20_provisional_mean_net20bps_pct": round(statistics.mean(vals), 6) if vals else None,
        "dplus20_provisional_median_net20bps_pct": round(statistics.median(vals), 6) if vals else None,
        "dplus20_mean_excluding_single_highest": round(statistics.mean(sorted(vals)[:-1]), 6)
        if len(vals) > 1 else None,
        "dplus20_same_line_relost_count": sum(x["relost_same_frozen_line_by_h"] for x in available),
    }


def study(repo: Path = ROOT) -> dict:
    all_events = []
    for code in SOURCES:
        all_events.extend(scan_stock(frozen_data(repo, code), repo))
    all_events.sort(key=lambda x: (x["date"], x["code"], x["scale"], x["line_id"]))
    identity = [(e["date"], e["code"], e["scale"], e["line_id"]) for e in all_events]
    if len(identity) != len(set(identity)):
        raise AssertionError("DUPLICATED_LINE_EVENTS")
    return {
        "revision": "R5_R116_TRENDLINE_BREAKOUT_FORWARD_PATH_THREE_STOCK_PILOT",
        "research_only": True, "source_sha256": SOURCES,
        "selection_protocol": "R116_PREVIOUS_DAY_ACTIVE_UPPER_LINE_CROSSED_AT_D0_CLOSE",
        "events": all_events, "line_events": len(all_events),
        "unique_stock_dates": len({(e["code"], e["date"]) for e in all_events}),
        "by_scale": {s: _summary_group([e for e in all_events if e["scale"] == s])
                     for s in ("SHORT", "MEDIUM", "LONG")},
        "by_year": {str(year): _summary_group([e for e in all_events if e["year"] == year])
                    for year in range(2021, 2025)},
        "by_code": {c: _summary_group([e for e in all_events if e["code"] == c])
                    for c in SOURCES},
        "source_population": "THREE_HANDPICKED_HISTORICAL_STOCKS_NOT_ALL_MARKET",
        "independent_price_source_certified": False,
        "historical_adjustment_provenance_certified": False,
        "full_market_pit_coverage": False, "out_of_sample": False,
        "market_regime_pit": "UNKNOWN", "survivorship_bias_resolved": False,
        "entry_exit": "D+1_OPEN_TO_D+h_CLOSE_ASSUMED_COST_ONLY",
        "strategy_winner": None, "can_publish_strategy_performance": False,
        "can_modify_live_search_watch": False,
        "env10_env20_lower_touch_events_merged": False,
    }


def run(repo: Path, out: Path):
    result = study(repo)
    out.mkdir(parents=True, exist_ok=True)
    (out / "R5_BREAKOUT_PILOT_AUDIT.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    summary = {k:v for k,v in result.items() if k != "events"}
    (out / "R5_BREAKOUT_PILOT_SUMMARY.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8"
    )
    rows = []
    for e in result["events"]:
        for horizon in HORIZONS:
            path = e["future_paths_EXPOST"][str(horizon)]
            rows.append(
                {k: v for k, v in e.items() if k != "future_paths_EXPOST"} |
                {"horizon": horizon, "future_status": path["status"],
                 "net20bps_pct_EXPOST": path.get("net_pct_by_total_cost_bps", {}).get("20"),
                 "mfe_pct_EXPOST": path.get("mfe_from_entry_pct"),
                 "mae_pct_EXPOST": path.get("mae_from_entry_pct"),
                 "same_line_relost_EXPOST": path.get("relost_same_frozen_line_by_h")}
            )
    pd.DataFrame(rows).to_csv(out / "R5_BREAKOUT_EVENTS_DPLUS_PATHS.csv",
                              index=False, encoding="utf-8-sig")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    result = run(Path(args.repo).resolve(), Path(args.out))
    print(json.dumps({k:v for k,v in result.items() if k!="events"}, ensure_ascii=False, indent=2))
