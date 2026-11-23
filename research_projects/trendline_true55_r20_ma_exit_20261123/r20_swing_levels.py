#!/usr/bin/env python3
"""R20 supplemental: causal confirmed swing highs/lows, frozen R19 entries.

A pivot at bar j is only known after j+2 close. The pre-entry upper target is
the most recent *already confirmed* high above D+1 opening entry. If absent,
it is UNKNOWN; never claim the old 55-day high as an achievable new target.
"""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

import pandas as pd

import r20_ma_exit as r20

LEFT = 2
RIGHT = 2


def confirmed_pivots(g: pd.DataFrame) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    """Each tuple (confirmation_bar, reference_price); no future use before confirmation."""
    highs, lows = [], []
    for j in range(LEFT, len(g) - RIGHT):
        high = float(g.high.iloc[j])
        low = float(g.low.iloc[j])
        if (high > float(g.high.iloc[j - LEFT:j].max())
                and high >= float(g.high.iloc[j + 1:j + RIGHT + 1].max())):
            highs.append((j + RIGHT, high))
        if (low < float(g.low.iloc[j - LEFT:j].min())
                and low <= float(g.low.iloc[j + 1:j + RIGHT + 1].min())):
            lows.append((j + RIGHT, low))
    return highs, lows


def frozen_upper_target(highs: list[tuple[int, float]], t: int, entry: float):
    eligible = [value for confirmed, value in highs if confirmed <= t and value > entry]
    return eligible[-1] if eligible else None


def swing_exits(g: pd.DataFrame, t: int,
                pivots: tuple[list[tuple[int, float]], list[tuple[int, float]]]) -> dict:
    """Three distinct reports: low-only, upper-target-only if available, both."""
    if t + r20.HORIZON >= len(g):
        raise ValueError("INCOMPLETE_FROZEN_R19_EVENT")
    highs, lows = pivots
    entry = float(g.open.iloc[t + 1])
    if entry <= 0:
        raise ValueError("INVALID_ENTRY")
    target = frozen_upper_target(highs, t, entry)
    last_low = None
    position = 0

    def outcome(day, price, reason):
        return {"net_pct": round(100 * (float(price) / entry - 1) - r20.COST_PCT, 6),
                "exit_day": day, "exit_reason": reason}

    stop = None
    profit = None
    combined = None
    for i in range(t + 1, t + r20.HORIZON):
        # At i's close only pivots confirmed through i may affect the stop.
        while position < len(lows) and lows[position][0] <= i:
            last_low = lows[position][1]
            position += 1
        below_low = last_low is not None and float(g.close.iloc[i]) < last_low
        above_target = target is not None and float(g.close.iloc[i]) >= target
        price = float(g.open.iloc[i + 1])
        if stop is None and below_low:
            stop = outcome(i + 1 - t, price, "CLOSE_BELOW_LAST_CONFIRMED_SWING_LOW")
        if profit is None and above_target:
            profit = outcome(i + 1 - t, price, "CLOSE_ABOVE_FROZEN_PRIOR_SWING_HIGH")
        if combined is None and (below_low or above_target):
            combined = outcome(i + 1 - t, price,
                               "FROZEN_SWING_HIGH_TARGET" if above_target else "SWING_LOW_BREAK")
        if stop is not None and (target is None or profit is not None) and combined is not None:
            break
    terminal = outcome(r20.HORIZON, float(g.close.iloc[t + r20.HORIZON]),
                       "WINDOW_END_CLOSE")
    return {"frozen_pre_entry_upper_target": target,
            "swing_low_only": stop or terminal,
            "swing_high_only_if_eligible": (profit or terminal) if target is not None else None,
            "swing_low_or_upper_target": combined or terminal,
            "target_hit": profit is not None,
            "swing_low_exit_observed": stop is not None}


def run(r16_out: Path, r19_out: Path, r20_ma_out: Path, out: Path):
    prices, frozen = r20.read_sources(r16_out, r19_out)
    ma_rows = json.loads((r20_ma_out / "R20_MA_PAIRED_EVENTS.json").read_text(encoding="utf-8"))
    if (len(ma_rows) != len(frozen) or
            [(x["code"], x["date"]) for x in ma_rows] !=
            [(x["code"], x["date"]) for x in frozen]):
        raise ValueError("R20_MA_COHORT_IS_NOT_IDENTICAL_TO_R19")
    frames = {str(code): f.sort_values("date").reset_index(drop=True)
              for code, f in prices.groupby("code")}
    locs = {code: {date: i for i, date in enumerate(frame.date)}
            for code, frame in frames.items()}
    pivots_by_code = {code: confirmed_pivots(f) for code, f in frames.items()}
    rows = []
    for e, ma in zip(frozen, ma_rows):
        code, date = e["code"], e["date"]
        if code not in locs or date not in locs[code]:
            raise ValueError("MISSING_R19_EVENT_BAR")
        result = swing_exits(frames[code], locs[code][date], pivots_by_code[code])
        rows.append({"code": code, "date": date, "year": int(date[:4]),
                     "swing": result, "ma5": ma["outcomes"][r20.MA_METHODS[5]]})
    target_rows = [r for r in rows if r["swing"]["frozen_pre_entry_upper_target"] is not None]
    eligible = len(target_rows)

    def swing_view(method: str, subset: list[dict]):
        converted = [{"outcomes": {method: r["swing"][method]}} for r in subset]
        return r20.describe(converted, method) if converted else {"n": 0}

    # Compare all 149 on a single fixed cohort; high-target-only cannot be
    # compared on 149 because absent targets are UNKNOWN, not zero returns.
    all_low = swing_view("swing_low_only", rows)
    all_both = swing_view("swing_low_or_upper_target", rows)
    target_only = swing_view("swing_high_only_if_eligible", target_rows)
    stop_subset = swing_view("swing_low_only", target_rows)
    report = {
        "revision": "R20_CONFIRMED_2LEFT_2RIGHT_SWING_LEVELS_ASOF_TRUE55",
        "fixed_r19_events": len(rows), "pivot_left_bars": LEFT,
        "pivot_right_confirmation_lag_bars": RIGHT,
        "pivot_known_only_when_j_plus_2_at_or_before_decision": True,
        "upper_target_fixed_at_entry_above_dplus1_open": True,
        "upper_target_missing_is_unknown_not_zero": True,
        "close_signal_following_open_execution": True,
        "total_with_pre_entry_eligible_upper_target": eligible,
        "total_without_pre_entry_eligible_upper_target": len(rows) - eligible,
        "target_was_breached_by_close_within_window": sum(r["swing"]["target_hit"] for r in target_rows),
        "all_149_swing_low_trail": all_low,
        "all_149_swing_low_or_preentry_high_target": all_both,
        "eligible_subset_upper_target_only": target_only,
        "eligible_subset_swing_low_only": stop_subset,
        "ma5_all_149_context": r20.describe(
            [{"outcomes": {"ma5": r["ma5"]}} for r in rows], "ma5"),
        "no_arbitrary_percent_stops_or_future_peak_target": True,
        "not_a_trading_policy_winner": True,
        "orders_placed": 0, "live_search_watch_changed": False,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "R20_SWING_LEVEL_SUMMARY.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (out / "R20_SWING_PAIRED_EVENTS.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--r16-out", type=Path, required=True)
    p.add_argument("--r19-out", type=Path, required=True)
    p.add_argument("--r20-ma-out", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    print(json.dumps(run(args.r16_out, args.r19_out, args.r20_ma_out, args.out),
                     ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
