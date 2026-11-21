#!/usr/bin/env python3
"""R13: single, frozen R5/R12 D0 volume screen. RESEARCH_ONLY.

This is a retrospective *development-set* check, not independent validation:
the R12 exploratory failure profiles motivated investigating volume. Never
promote the filter or tune its threshold on these same five tickers.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import statistics
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
R12_REL = "research_projects/trendline_signal_expectancy_r12_20261121/r12_signal_expectancy.py"
R12_SHA256 = "42d10240928169612d5ea9f97bc9f8eb3bbd5d3b14c3230255aae029612adcda"
FILTER_NAME = "D0_VOLUME_AT_LEAST_PRE20_MEAN"
FILTER_FEATURE = "d0_volume_vs_pre20"
FILTER_THRESHOLD = 1.0  # one natural, fixed threshold; no search/grid/optimization
EXIT_MODEL = "TAKE_10_STOP_5"  # from R12's fixed risk:reward study
HORIZON = 20
COHORTS = ("FROZEN_R5_THREE", "ADDITIONAL_EXISTING_TWO")


def r12_module(repo: Path = ROOT):
    source = repo / R12_REL
    if hashlib.sha256(source.read_bytes()).hexdigest() != R12_SHA256:
        raise ValueError("FROZEN_R12_SOURCE_MISMATCH_NO_SILENT_UPDATE")
    spec = importlib.util.spec_from_file_location("r12_signal_expectancy", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def passes_signal_day_screen(event: dict) -> bool:
    ratio = event["asof_d0_features"].get(FILTER_FEATURE)
    return isinstance(ratio, (float, int)) and ratio >= FILTER_THRESHOLD


def metric(events: list[dict]) -> dict:
    good = [e["horizons_EXPOST"][str(HORIZON)]["predeclared_bracket_models"][EXIT_MODEL]
            for e in events if e["horizons_EXPOST"][str(HORIZON)]["status"].startswith("DESCRIPTIVE_")]
    returns = [p["net_total20bps_pct"] for p in good]
    return {
        "n_events": len(events), "n_complete": len(good), "n_unknown": len(events)-len(good),
        "mean_net_pct": round(statistics.mean(returns), 6) if returns else None,
        "median_net_pct": round(statistics.median(returns), 6) if returns else None,
        "p10_net_pct": round(sorted(returns)[max(0, int(len(returns)*.1)-1)], 6) if returns else None,
        "net_win_count": sum(ret > 0 for ret in returns),
        "net_win_rate_pct": round(100 * sum(ret > 0 for ret in returns) / len(returns), 3) if returns else None,
        "target_hit": sum(p["hit_target"] for p in good),
        "stop_hit": sum(p["hit_stop"] for p in good),
        "time_exit": sum(not p["hit_target"] and not p["hit_stop"] for p in good),
        "gap_through_stop": sum(p["exit_reason"] == "STOP_GAP_DOWN_OPEN" for p in good),
        "exit_reason_counts": dict(Counter(p["exit_reason"] for p in good)),
    }


def compare_cohort(events: list[dict], r12) -> dict:
    """Screen *before* R7 embargo; never choose a late winning signal post hoc."""
    unique = sorted(events, key=lambda e: (e["code"], e["idx"]))
    screened = [e for e in unique if passes_signal_day_screen(e)]
    base_nonoverlap = r12.nonoverlap_signals(unique)
    screened_nonoverlap = r12.nonoverlap_signals(screened)
    base = metric(base_nonoverlap)
    after = metric(screened_nonoverlap)
    return {
        "unique_input": len(unique), "unique_screen_pass": len(screened),
        "unique_screen_rejected": len(unique) - len(screened),
        "baseline_20bar_nonoverlap": base,
        "screened_then_20bar_nonoverlap": after,
        "mean_delta_percentage_points_DESCRIPTIVE_NOT_CAUSAL": (
            round(after["mean_net_pct"]-base["mean_net_pct"], 6)
            if after["mean_net_pct"] is not None and base["mean_net_pct"] is not None else None
        ),
        "same_selected_stock_dates_after_screen": {
            (e["code"], e["signal_date"]) for e in base_nonoverlap
        } == {
            (e["code"], e["signal_date"]) for e in screened_nonoverlap
        },
        "by_year_screened": {
            str(year): metric([e for e in screened_nonoverlap if e["year"] == year])
            for year in sorted({e["year"] for e in screened_nonoverlap})
        },
        "screened_event_keys": [(e["code"], e["signal_date"]) for e in screened_nonoverlap],
    }


def study(repo: Path = ROOT) -> dict:
    r12 = r12_module(repo)
    frozen = r12.study(repo)
    results = {cohort: compare_cohort(
        [r for r in frozen["signal_event_rows"] if r["cohort"] == cohort], r12
    ) for cohort in COHORTS}
    for cohort in COHORTS:
        expected = frozen["cohorts"][cohort]["nonoverlap_20bar_PRIMARY"]["20"]["signal"]
        actual = results[cohort]["baseline_20bar_nonoverlap"]
        if (actual["n_events"] != expected["n_selected"] or
                actual["n_complete"] != expected["n_complete"]):
            raise AssertionError("R12_R13_NONOVERLAP_COHORT_DRIFT")
        original_model = expected["fixed_brackets_pessimistic_daily_ohlc"][EXIT_MODEL]
        if abs(actual["mean_net_pct"] - original_model["mean_net_pct"]) > 0.00001:
            raise AssertionError("R12_R13_EXIT_MODEL_PARITY_FAILURE")
    no_positive = any(results[c]["screened_then_20bar_nonoverlap"]["mean_net_pct"] is None or
                      results[c]["screened_then_20bar_nonoverlap"]["mean_net_pct"] <= 0
                      for c in COHORTS)
    return {
        "revision": "R13_ONE_SCREEN_RETROSPECTIVE_NO_OOS",
        "research_only": True, "filter": FILTER_NAME, "feature": FILTER_FEATURE,
        "threshold_inclusive": FILTER_THRESHOLD, "missing_feature_behavior": "REJECT",
        "filter_asof": "AFTER_D0_CLOSE_PRIOR_TO_DPLUS1_OPEN",
        "bracket_exit_model": EXIT_MODEL, "horizon": HORIZON,
        "r12_source_sha256": R12_SHA256,
        "fixed_filter_count": 1, "parameter_search_performed": False,
        "cohorts": results,
        "decision": "REJECT_AS_A_PROFITABLE_ENTRY_SCREEN" if no_positive else "REQUIRES_TRULY_NEW_UNSEEN_DATA",
        "descriptive_development_set_only": True,
        "independent_out_of_sample": False,
        "screen_origin": "INSPIRED_BY_R12_FEATURE_PROFILES_ON_THE_SAME_FIVE_STOCKS_NOT_AN_UNTOUCHED_HOLDOUT",
        "screen_changes_which_early_signals_survive_20BAR_EMBARGO": True,
        "no_prediction_claim": True,
        "can_modify_live_search_watch": False,
        "can_publish_trade_edge": False,
        "stop_policy": "DO_NOT_ADJUST_THRESHOLD_TO_RESCUE_RESULT; STOP_THIS_FILTER_IF_NET_NONPOSITIVE",
    }


def run(repo: Path, out: Path) -> dict:
    result = study(repo)
    out.mkdir(parents=True, exist_ok=True)
    (out / "R13_SINGLE_VOLUME_SCREEN_SUMMARY.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    report = run(Path(args.repo).resolve(), Path(args.out).resolve())
    print(json.dumps({"decision": report["decision"], "cohorts": {
        c: {"base": report["cohorts"][c]["baseline_20bar_nonoverlap"],
            "screen": report["cohorts"][c]["screened_then_20bar_nonoverlap"]}
        for c in COHORTS}}, ensure_ascii=False, indent=2))
