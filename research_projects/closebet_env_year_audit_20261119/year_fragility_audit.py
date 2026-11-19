#!/usr/bin/env python3
"""Year and single-case fragility of the frozen ENV10/ENV20 pilot.

RESEARCH_ONLY: consumes the existing frozen pilot; never selects strategies.
All results remain provisional until independent PIT/full-market validation.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

REVISION = "ENV10_ENV20_FROZEN_300_YEAR_FRAGILITY_R3"
GROUPS = ("ENV10_ONLY", "ENV20_BOTH", "NO_TOUCH", "UNKNOWN")
YEARS = (2021, 2022, 2023, 2024, 2025)
EXPECTED_SOURCE_SHA256 = "b6d1fd1f4e4fdab6680bc78705825ff8b9775b20adb14b4aa8e05b55a004742d"
VALID_PATH_PREFIX = "PROVISIONAL_"


def path_net20(event: dict) -> float | None:
    """Unknown or uncertified future paths must never turn into zero."""
    path = event.get("forward_paths", {}).get("20", {})
    if not str(path.get("status", "")).startswith(VALID_PATH_PREFIX):
        return None
    value = path.get("net_return_pct_by_total_cost_bps", {}).get("20")
    return float(value) if value is not None else None


def summarize(events: list[dict]) -> dict:
    """One-observation deletion is a sensitivity test, not an exclusion rule."""
    known = [(e, path_net20(e)) for e in events]
    valid = [(e, v) for e, v in known if v is not None]
    vals = sorted(v for _, v in valid)
    n = len(vals)
    mean = sum(vals) / n if n else None
    median = ((vals[(n - 1) // 2] + vals[n // 2]) / 2) if n else None
    without_best = (sum(vals[:-1]) / (n - 1)) if n > 1 else None
    without_worst = (sum(vals[1:]) / (n - 1)) if n > 1 else None
    best_event = max(valid, key=lambda pair: (pair[1], pair[0]["date"], pair[0]["code"]))[0] if valid else None
    return {
        "event_count": len(events),
        "known_d20_count": n,
        "unknown_d20_count": len(events) - n,
        "unique_tickers": len({e["code"] for e in events}),
        "unique_signal_dates": len({e["date"] for e in events}),
        "mean_net20bps_d20_pct": round(mean, 6) if mean is not None else None,
        "median_net20bps_d20_pct": round(median, 6) if median is not None else None,
        "positive_net20bps_count": sum(v > 0 for v in vals),
        "minimum_net20bps_pct": round(vals[0], 6) if vals else None,
        "maximum_net20bps_pct": round(vals[-1], 6) if vals else None,
        "mean_without_single_best_case_pct": round(without_best, 6) if without_best is not None else None,
        "mean_without_single_worst_case_pct": round(without_worst, 6) if without_worst is not None else None,
        "best_case_code": best_event["code"] if best_event else None,
        "best_case_date": best_event["date"] if best_event else None,
        "best_case_removal_changes_mean_sign": (
            mean * without_best < 0 if mean is not None and without_best is not None else None
        ),
    }


def make_audit(events: list[dict], pilot_sha: str, fullmarket: dict,
               index_columns: list[str], frozen_pool_keys: set[tuple[str, str]]) -> dict:
    if pilot_sha != EXPECTED_SOURCE_SHA256:
        raise ValueError("FROZEN_SOURCE_CHANGED")
    if len(events) != 300 or len({(e["code"], e["date"]) for e in events}) != 300:
        raise ValueError("EXPECTED_DISTINCT_FROZEN_300")
    if any((e["code"], e["date"]) not in frozen_pool_keys for e in events):
        raise ValueError("PILOT_EVENT_NOT_IN_PRESELECTED_1500")
    if fullmarket.get("data_retrieved") is not False or fullmarket.get("not_pit_certified") is not True:
        raise ValueError("FULLMARKET_SOURCE_READINESS_CHANGED_REVIEW_REQUIRED")
    if any(e["signal_group"] not in GROUPS for e in events):
        raise ValueError("UNKNOWN_SIGNAL_GROUP_SCHEMA")

    by_year = {
        str(year): {
            "all_pilot_events": summarize([e for e in events if e["year"] == year]),
            "groups": {
                group: summarize([e for e in events if e["year"] == year and e["signal_group"] == group])
                for group in GROUPS
            },
        } for year in YEARS
    }
    if sum(d["all_pilot_events"]["event_count"] for d in by_year.values()) != 300:
        raise ValueError("FROZEN_EVENTS_OUTSIDE_PREDECLARED_YEARS")
    overall = {group: summarize([e for e in events if e["signal_group"] == group]) for group in GROUPS}
    if sum(e["event_count"] for e in overall.values()) != 300:
        raise ValueError("GROUP_RECONCILIATION_FAILED")

    return {
        "revision": REVISION,
        "research_only": True,
        "sample_status": "FROZEN_300_UNCERTIFIED_PRICE_PILOT_NOT_ALL_MARKET",
        "pilot_sha256": pilot_sha,
        "pilot_events": 300,
        "pilot_unique_tickers": len({e["code"] for e in events}),
        "pilot_repeated_ticker_event_surplus": 300 - len({e["code"] for e in events}),
        "pilot_unique_dates": len({e["date"] for e in events}),
        "events_verified_in_frozen_preselected_1500": 300,
        "market_regime_status": (
            "UNKNOWN_CALENDAR_ONLY_NO_INDEX_PRICE_LEVELS"
            if set(index_columns) == {"date"} else "UNKNOWN_INDEX_PRICE_SOURCE_NOT_PIT_CERTIFIED"
        ),
        "original_index_columns": index_columns,
        "fullmarket_queue_requests": fullmarket.get("n_source_requests"),
        "fullmarket_source_retrieved": False,
        "fullmarket_pit_certified": False,
        "independent_price_validation": False,
        "out_of_sample_validation": False,
        "trading_fills_modeled": False,
        "entry_exit": "D+1_OPEN_TO_D+20_CLOSE_ASSUMED_TOTAL_COST_20BPS",
        "group_descriptive_stats": overall,
        "by_year": by_year,
        "strategy_winner": None,
        "can_publish_strategy_performance": False,
        "can_modify_live_search_watch": False,
        "interpretation": (
            "DESCRIPTIVE_OUTLIER_SENSITIVITY_ONLY_NO_CAUSAL_OR_LIVE_DECISION; "
            "MEAN_WITHOUT_BEST_IS_NOT_AN_ESTIMATOR_OF_TRADING_PERFORMANCE"
        ),
    }


def run(repo: Path, out: Path) -> dict:
    sys.path.insert(0, str(repo / "research_projects/closebet_source_parity_20261117"))
    sys.path.insert(0, str(repo / "research_projects/closebet_env_ab_20261119"))
    from historical_case_audit import load_panel
    from frozen_300_forward_path import study

    panel, sha = load_panel(repo)
    manifest = json.loads((repo / "research_inputs/R1D27D_FULLMARKET_SOURCE_QUEUE_MANIFEST.json").read_text(encoding="utf-8"))
    original = study(panel, sha, manifest)
    pool_path = repo / "research_inputs/R1D27R_FLOW_BLIND_FROZEN_1500.csv"
    with pool_path.open(encoding="utf-8-sig", newline="") as f:
        pool_keys = {(row["code"], row["date"]) for row in csv.DictReader(f)}
    with (repo / "research_inputs/R1D27D_ORIGINAL_KRX_INDEX_1907_DAYS.csv").open(encoding="utf-8-sig", newline="") as f:
        index_columns = list(csv.DictReader(f).fieldnames or [])
    result = make_audit(original["events"], sha, manifest, index_columns, pool_keys)
    out.mkdir(parents=True, exist_ok=True)
    (out / "ENV10_ENV20_FROZEN_300_YEAR_FRAGILITY.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    flattened = []
    for year, entry in result["by_year"].items():
        for group, data in entry["groups"].items():
            flattened.append({"year": year, "signal_group": group, **data})
    with (out / "ENV10_ENV20_FROZEN_300_YEAR_GROUPS.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(flattened[0]))
        writer.writeheader()
        writer.writerows(flattened)
    print(json.dumps({k: v for k, v in result.items() if k not in ("by_year",)}, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", default=".")
    p.add_argument("--out", required=True)
    args = p.parse_args()
    run(Path(args.repo), Path(args.out))
