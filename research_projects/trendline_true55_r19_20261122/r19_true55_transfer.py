#!/usr/bin/env python3
"""R19: actual previous-55-high breakout, structural exits, R18 risk transfer.

RESEARCH_ONLY. Never reads KIS credentials or modifies LIVE/SEARCH/WATCH.
No fixed percent stops, hindsight entry, or in-sample winner declaration.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import statistics
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

KIS_RUN = "44988188271"
KIS_CSV_SHA256 = "e5edabf1b111d424a0afa8503111d7dd4d6e4827036aa7eea512ef067d122dbc"
HORIZON = 60
EMBARGO = 60
FEATURES = ("close_location", "amount_vs_prev20", "volume_vs_prev20",
            "close_vs_ma224_pct", "return_prev20_pct", "breakout_vs_prior55_high_pct")
EXITS = ("D0_LOW_NEXT_OPEN", "PREV5_LOW_NEXT_OPEN", "PREV10_LOW_NEXT_OPEN")


def load_r17(repo: Path):
    file = repo / "research_projects/trendline_kis_failure_r17_20261122/r17_failure_exit.py"
    spec = importlib.util.spec_from_file_location("r17_frozen_for_r19", file)
    if not spec or not spec.loader:
        raise ValueError("MISSING_FROZEN_R17_MODULE")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def prior55_candidates(frame: pd.DataFrame) -> tuple[list[int], pd.Series]:
    """D0 decision uses only highs strictly before D0 and D0 closing price."""
    prior = frame.high.shift(1).rolling(55, min_periods=55).max()
    indexes = [i for i in range(55, len(frame))
               if str(frame.date.iloc[i]) >= "2021-01-01"
               and float(frame.close.iloc[i]) > float(prior.iloc[i])]
    return indexes, prior


def collect_signals(data: pd.DataFrame, r17) -> tuple[list[dict], dict]:
    rows = []
    audit = {"raw_d0_breakouts": 0, "after_60bar_embargo": 0,
             "incomplete_d60": 0, "dplus1_open_below_or_equal_breakout": 0,
             "evaluated": 0}
    for code, frame in data.groupby("code", sort=True):
        g = frame.sort_values("date").reset_index(drop=True)
        candidates, prior = prior55_candidates(g)
        audit["raw_d0_breakouts"] += len(candidates)
        last = -EMBARGO
        for t in candidates:
            if t - last < EMBARGO:
                continue
            # Embargo precedes all future-bar checks, so future returns cannot
            # change the independent signal identities.
            last = t
            audit["after_60bar_embargo"] += 1
            if t + HORIZON >= len(g):
                audit["incomplete_d60"] += 1
                continue
            # An entry gate evaluated only when D+1 actually opens.
            if float(g.open.iloc[t + 1]) <= float(prior.iloc[t]):
                audit["dplus1_open_below_or_equal_breakout"] += 1
                continue
            outcomes = {kind: r17.exit_close_next_open(g, t, HORIZON, kind)
                        for kind in EXITS}
            path = g.iloc[t + 1:t + HORIZON + 1]
            entry = float(path.iloc[0].open)
            rows.append({
                "code": str(code), "date": str(g.date.iloc[t]),
                "year": int(str(g.date.iloc[t])[:4]),
                "features": r17.make_features(g, t), "outcomes": outcomes,
                "expost_not_tradable": {
                    "max_favorable_60_pct": round(100 * (float(path.high.max()) / entry - 1), 6),
                    "max_adverse_60_pct": round(100 * (float(path.low.min()) / entry - 1), 6),
                    "peak_day": int(path.high.to_numpy().argmax() + 1),
                },
            })
            audit["evaluated"] += 1
    return rows, audit


def policy_stats(rows: list[dict], kind: str) -> dict:
    values = [float(r["outcomes"][kind]["net_pct"]) for r in rows]
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    trim = max(1, int(.1 * len(values))) if len(values) >= 10 else 0
    middle = ordered[trim:-trim] if trim else ordered
    best = max(rows, key=lambda r: r["outcomes"][kind]["net_pct"])
    return {
        "n": len(values), "mean_net_pct": round(statistics.mean(values), 6),
        "median_net_pct": round(statistics.median(values), 6),
        "p10_net_pct": round(float(np.quantile(values, .10)), 6),
        "mean_excluding_best_pct": round(statistics.mean(ordered[:-1]), 6)
        if len(values) > 1 else None,
        "trimmed_10pct_mean_net_pct": round(statistics.mean(middle), 6),
        "win_rate_pct": round(100 * sum(v > 0 for v in values) / len(values), 3),
        "best_return_pct": round(max(values), 6), "best_code": best["code"],
        "best_date": best["date"], "worst_return_pct": round(min(values), 6),
    }


def feature_matrix(rows: list[dict]) -> np.ndarray:
    return np.array([[np.nan if r["features"][f] is None
                      else float(r["features"][f]) for f in FEATURES]
                     for r in rows], dtype=float)


def train_warning(rows: list[dict], kind: str):
    """Fit all transformations and thresholds on 2021-2023 only."""
    train = [r for r in rows if r["date"] <= "2023-12-31"]
    if len(train) < 20:
        raise ValueError("INSUFFICIENT_EARLY_TRAIN")
    q10 = float(np.quantile([r["outcomes"][kind]["net_pct"] for r in train], .10))
    y = np.array([int(r["outcomes"][kind]["net_pct"] <= q10) for r in train])
    if len(set(y)) != 2:
        raise ValueError("SINGLE_TRAIN_CLASS")
    classifier = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(),
                               LogisticRegression(C=.25, solver="liblinear", max_iter=1000))
    classifier.fit(feature_matrix(train), y)
    q75 = float(np.quantile(classifier.predict_proba(feature_matrix(train))[:, 1], .75))
    return classifier, q10, q75, len(train)


def review_warning(rows: list[dict], kind: str, fitted) -> tuple[dict, list[dict]]:
    classifier, q10, q75, ntrain = fitted
    review = [r for r in rows if r["date"] >= "2024-01-01"]
    if not review:
        return {"n": 0}, []
    probabilities = classifier.predict_proba(feature_matrix(review))[:, 1]
    y = np.array([int(r["outcomes"][kind]["net_pct"] <= q10) for r in review])
    high = probabilities >= q75
    net = np.array([r["outcomes"][kind]["net_pct"] for r in review], dtype=float)

    def stats(mask):
        v = net[mask]
        return {"n": len(v), "tail_n": int(y[mask].sum()),
                "mean_net_pct": round(float(np.mean(v)), 6) if len(v) else None,
                "median_net_pct": round(float(np.median(v)), 6) if len(v) else None,
                "p10_net_pct": round(float(np.quantile(v, .10)), 6) if len(v) else None}

    scores = [{"code": row["code"], "date": row["date"],
               "risk_score": round(float(p), 8), "risk_group": "HIGH" if flag else "RETAINED",
               "actual_net_pct": float(v), "actual_early_train_tail": int(tail)}
              for row, p, flag, v, tail in zip(review, probabilities, high, net, y)]
    return {"n": len(review), "n_training": ntrain,
            "training_tail_threshold_pct": round(q10, 6),
            "auc": round(float(roc_auc_score(y, probabilities)), 6)
            if len(set(y)) == 2 else None,
            "high_risk": stats(high), "retained": stats(~high),
            "all": stats(np.ones(len(review), dtype=bool))}, scores


def read_frozen_sources(r16_out: Path, r17_out: Path) -> tuple[pd.DataFrame, list[dict], dict]:
    manifest = json.loads((r16_out / "R16_SOURCE_MANIFEST.json").read_text(encoding="utf-8"))
    archive = r16_out / "R16_FROZEN_HOLDOUT_UNADJUSTED_OHLCV.csv.gz"
    if manifest.get("kis_run_id") != KIS_RUN or manifest.get("kis_csv_sha256") != KIS_CSV_SHA256:
        raise ValueError("UNPINNED_OR_WRONG_KIS_SOURCE")
    if hashlib.sha256(archive.read_bytes()).hexdigest() != manifest.get("raw_archive_sha256"):
        raise ValueError("R16_PRICE_ARCHIVE_SHA_MISMATCH")
    data = pd.read_csv(archive, dtype={"code": str, "date": str})
    if len(data) != 37736 or data.code.nunique() != 26 or data.duplicated(["code", "date"]).any():
        raise ValueError("R16_PRICE_ARCHIVE_UNEXPECTED_SHAPE")
    old = json.loads((r17_out / "R17_EVENTS.json").read_text(encoding="utf-8"))
    if len(old) != 425 or len({(r["code"], r["date"]) for r in old}) != 425:
        raise ValueError("R17_FROZEN_EVENTS_UNEXPECTED")
    return data, old, manifest


def run(repo: Path, r16_out: Path, r17_out: Path, out: Path) -> dict:
    data, older, manifest = read_frozen_sources(r16_out, r17_out)
    genuine, audit = collect_signals(data, load_r17(repo))
    if (audit["raw_d0_breakouts"], audit["after_60bar_embargo"],
            audit["incomplete_d60"], audit["dplus1_open_below_or_equal_breakout"],
            audit["evaluated"]) != (500, 175, 7, 19, 149):
        raise ValueError("R19_SIGNAL_REPLAY_DRIFT")
    pairs = {(r["code"], r["date"]) for r in genuine}
    overlap = len(pairs & {(r["code"], r["date"]) for r in older})
    frozen_warning, frozen_scores = review_warning(genuine, EXITS[0], train_warning(older, EXITS[0]))
    own_warning, own_scores = review_warning(genuine, EXITS[0], train_warning(genuine, EXITS[0]))
    summary = {
        "revision": "R19_GENUINE_PRIOR55_HIGH_CLOSE_BREAKOUT_R18_RISK_TRANSFER_EXPLORATORY",
        "definition": "D0_CLOSE_GT_MAX_PREVIOUS_55_DAILY_HIGHS",
        "entry": "DPLUS1_OPEN_MUST_BE_ABOVE_FROZEN_PRIOR55_HIGH",
        "same_ticker_nonoverlap_60_bars": True, "cost_round_trip_pct": 0.2,
        "signal_audit": audit, "r116_overlap": {"code_date_overlap": overlap, "r116_total": len(older)},
        "new_55_signals_by_year": {str(y): sum(r["year"] == y for r in genuine)
                                   for y in range(2021, 2026)},
        "by_exit_method": {kind: policy_stats(genuine, kind) for kind in EXITS},
        "by_year_d0_low": {str(y): policy_stats([r for r in genuine if r["year"] == y], EXITS[0])
                           for y in range(2021, 2026)},
        "expost_60_median_offered_mfe_pct": round(statistics.median(
            r["expost_not_tradable"]["max_favorable_60_pct"] for r in genuine), 6),
        "expost_60_median_offered_mae_pct": round(statistics.median(
            r["expost_not_tradable"]["max_adverse_60_pct"] for r in genuine), 6),
        "expost_60_median_peak_day": statistics.median(
            r["expost_not_tradable"]["peak_day"] for r in genuine),
        "source_kis_run_id": KIS_RUN,
        "source_26_valid_codes": manifest["n_valid_tickers"] == 26,
        "new_study_55_events": len(genuine),
        "r116_d0_low_training_to_genuine_55_review": frozen_warning,
        "genuine_55_training_to_genuine_55_review": own_warning,
        "no_hyperparameter_or_percent_stop_tuning": True,
        "review_2024_2025_previously_seen_not_pristine": True,
        "source_corporate_actions_certified": False,
        "full_market_random_sample": False,
        "orders_placed": 0, "live_search_watch_changed": False,
        "can_publish_trade_edge": False,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "R19_TRUE55_TRANSFER_SUMMARY.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    (out / "R19_TRUE55_EVENT_AUDIT.json").write_text(
        json.dumps(genuine, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    (out / "R19_REVIEW_RISK_SCORES.json").write_text(
        json.dumps({"frozen_r116_model": frozen_scores, "true55_model": own_scores},
                   indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--r16-out", type=Path, required=True)
    parser.add_argument("--r17-out", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.repo, args.r16_out, args.r17_out, args.out),
                     indent=2, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
