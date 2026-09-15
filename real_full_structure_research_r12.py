#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
REAL_FULL Structure Research R1.2 — V72 saturation / outcome4 / blind fidelity.

Research-only postprocessor. It consumes the already-materialized R1 outputs.
It MUST NOT change the REAL_FULL search formula, score, rank, order logic, CORE224,
TRIANGLE1PB, or LOW224.

R1.2 rules:
- Identity fail-closed.
- Fixed four outcome labels: CLEAN_WIN / GIVEBACK / DEEP_MAE_WIN / NO_HIT.
- Score==100 cohort is analyzed separately.
- V72 debug parsing is idempotent: existing parsed columns are preserved and only
  missing values are backfilled. This prevents duplicate-column failures.
- The true-blind pack created by the upgraded base R1 is reused verbatim. R1.2
  never rebuilds a blind pack from the legacy manual-review bars.
- No cutoff search, no optimizer, no same-sample rule selection.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import shutil
from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd

RESEARCH_ID = "REAL_FULL_STRUCTURE_RESEARCH_R1_2"
RESEARCH_REVISION = "R1_2_V72_SCORE100_CAUSAL_AUDIT_HOTFIX_20260915"
OUTCOME_ORDER = ["CLEAN_WIN", "GIVEBACK", "DEEP_MAE_WIN", "NO_HIT"]
EXPECTED_IDENTITY = (24, 115, 115, 13)


def read_csv(path: str | Path, **kwargs) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def numeric(s) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def finite_median(s) -> float:
    x = numeric(s).dropna()
    return float(x.median()) if len(x) else np.nan


def q(s, p: float) -> float:
    x = numeric(s).dropna()
    return float(x.quantile(p)) if len(x) else np.nan


def fmt(x, digits=2):
    try:
        v = float(x)
        if not math.isfinite(v):
            return "NA"
        return f"{v:.{digits}f}"
    except Exception:
        return "NA"


def pct(x, digits=1):
    try:
        v = float(x)
        if not math.isfinite(v):
            return "NA"
        return f"{v * 100:.{digits}f}%"
    except Exception:
        return "NA"


def parse_v72_debug(text) -> Dict[str, float]:
    txt = str(text or "")
    out: Dict[str, float] = {}
    pats = {
        "v72_debug_score": r"(?:^|,\s*)score=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_impulse": r"(?:^|,\s*)impulse=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_support": r"(?:^|,\s*)support=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_rsi": r"(?:^|,\s*)rsi=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_disp": r"(?:^|,\s*)disp=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_wick": r"(?:^|,\s*)wick=([+-]?\d+(?:\.\d+)?)",
    }
    for name, pat in pats.items():
        m = re.search(pat, txt)
        out[name] = float(m.group(1)) if m else np.nan
    m = re.search(r"(?:^|,\s*)k=([+-]?\d+(?:\.\d+)?)/([+-]?\d+(?:\.\d+)?)", txt)
    if m:
        k, d = float(m.group(1)), float(m.group(2))
        out["v72_debug_k"] = k
        out["v72_debug_d"] = d
        out["v72_debug_k_minus_d"] = k - d
    else:
        out["v72_debug_k"] = np.nan
        out["v72_debug_d"] = np.nan
        out["v72_debug_k_minus_d"] = np.nan
    return out


def backfill_debug_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Idempotent merge: never creates duplicate column labels."""
    x = df.reset_index(drop=True).copy()
    src = x.get("v72_pullback_restart_debug", pd.Series([""] * len(x), index=x.index))
    parsed = pd.DataFrame([parse_v72_debug(v) for v in src]).reset_index(drop=True)
    for c in parsed.columns:
        p = numeric(parsed[c])
        if c in x.columns:
            e = numeric(x[c])
            x[c] = e.where(e.notna(), p)
        else:
            x[c] = p
    if x.columns.duplicated().any():
        dups = x.columns[x.columns.duplicated()].tolist()
        raise RuntimeError(f"R12_DUPLICATE_COLUMNS_AFTER_BACKFILL:{dups}")
    return x


def assign_outcome4(df: pd.DataFrame) -> pd.Series:
    hit = numeric(df.get("origin_d5_hit_plus5", pd.Series(index=df.index, dtype=float))).fillna(0).eq(1)
    close = numeric(df.get("origin_d5_close_ret_pct", pd.Series(index=df.index, dtype=float)))
    mae = numeric(df.get("origin_d5_mae_pct", pd.Series(index=df.index, dtype=float)))
    conds = [
        hit & close.gt(0) & mae.gt(-5),
        hit & close.le(0),
        hit & close.gt(0) & mae.le(-5),
        ~hit,
    ]
    return pd.Series(
        np.select(conds, OUTCOME_ORDER, default="UNCLASSIFIED"),
        index=df.index,
        dtype="object",
    )


def identity_tuple(identity: pd.DataFrame):
    r = identity.iloc[-1]
    return (
        int(numeric(pd.Series([r.get("snapshot_dates")])).iloc[0]),
        int(numeric(pd.Series([r.get("top15_events")])).iloc[0]),
        int(numeric(pd.Series([r.get("d5_mature_events")])).iloc[0]),
        int(numeric(pd.Series([r.get("context_pattern_matrices")])).iloc[0]),
    )


def cliffs_delta(a: Iterable[float], b: Iterable[float]) -> float:
    """Descriptive effect only. Positive means values in a tend to exceed b."""
    x = numeric(pd.Series(list(a))).dropna().to_numpy()
    y = numeric(pd.Series(list(b))).dropna().to_numpy()
    if len(x) == 0 or len(y) == 0:
        return np.nan
    gt = 0
    lt = 0
    for xv in x:
        gt += int(np.sum(xv > y))
        lt += int(np.sum(xv < y))
    return float((gt - lt) / (len(x) * len(y)))


def feature_long(df: pd.DataFrame, features: List[str], cohort: str) -> pd.DataFrame:
    rows = []
    for feature in features:
        if feature not in df.columns:
            continue
        clean = numeric(df.loc[df["outcome4"].eq("CLEAN_WIN"), feature]).dropna()
        for outcome in OUTCOME_ORDER:
            s = numeric(df.loc[df["outcome4"].eq(outcome), feature]).dropna()
            rows.append({
                "cohort": cohort,
                "feature": feature,
                "outcome4": outcome,
                "n": int(len(s)),
                "median": float(s.median()) if len(s) else np.nan,
                "q25": float(s.quantile(.25)) if len(s) else np.nan,
                "q75": float(s.quantile(.75)) if len(s) else np.nan,
                # adverse-vs-clean: positive => adverse has larger feature value
                "cliffs_delta_vs_clean": cliffs_delta(s, clean) if outcome != "CLEAN_WIN" else 0.0,
            })
    return pd.DataFrame(rows)


def outcome_summary(df: pd.DataFrame, cohort: str) -> pd.DataFrame:
    rows = []
    for outcome in OUTCOME_ORDER:
        g = df[df["outcome4"].eq(outcome)]
        rows.append({
            "cohort": cohort,
            "outcome4": outcome,
            "events": int(len(g)),
            "rate": float(len(g) / len(df)) if len(df) else np.nan,
            "d5_close_median": finite_median(g.get("origin_d5_close_ret_pct", pd.Series(dtype=float))),
            "d5_mfe_median": finite_median(g.get("origin_d5_mfe_pct", pd.Series(dtype=float))),
            "d5_mae_median": finite_median(g.get("origin_d5_mae_pct", pd.Series(dtype=float))),
            "v72_raw_score_median": finite_median(g.get("v72_pullback_restart_score_raw", pd.Series(dtype=float))),
        })
    return pd.DataFrame(rows)


def ma224_path_table(score100: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if "ma224_context" not in score100.columns:
        return pd.DataFrame()
    for ctx, g in score100.groupby("ma224_context", dropna=False):
        hits = g[~g["outcome4"].eq("NO_HIT")]
        rows.append({
            "ma224_context": str(ctx),
            "events": int(len(g)),
            "clean_win": int(g["outcome4"].eq("CLEAN_WIN").sum()),
            "giveback": int(g["outcome4"].eq("GIVEBACK").sum()),
            "deep_mae_win": int(g["outcome4"].eq("DEEP_MAE_WIN").sum()),
            "no_hit": int(g["outcome4"].eq("NO_HIT").sum()),
            "plus5_hit_rate": float(len(hits) / len(g)) if len(g) else np.nan,
            "clean_among_hits_rate": float(hits["outcome4"].eq("CLEAN_WIN").mean()) if len(hits) else np.nan,
            "path_damage_among_hits_rate": float(hits["outcome4"].isin(["GIVEBACK", "DEEP_MAE_WIN"]).mean()) if len(hits) else np.nan,
        })
    return pd.DataFrame(rows)


def amount_audit(df: pd.DataFrame) -> pd.DataFrame:
    src = df.get("amount_source", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str)
    rows = [
        {
            "item": "price_history_signal_pre20_amount",
            "coverage": int(src.ne("").sum()),
            "total": int(len(df)),
            "authority": "PROXY" if src.str.contains("proxy|close.*volume", case=False, regex=True).all() else "MIXED_OR_UNKNOWN",
            "detail": ";".join(sorted(src[src.ne("")].unique().tolist()))[:500],
        }
    ]
    for c, detail in [
        ("universe_avg_amount20", "D-1 universe 20D amount context"),
        ("universe_amount_ratio_prev_vs20", "D-1 amount / prior20 context ratio"),
        ("universe_volume_ratio_prev_vs20", "D-1 volume / prior20 context ratio"),
    ]:
        coverage = int(numeric(df[c]).notna().sum()) if c in df.columns else 0
        rows.append({
            "item": c,
            "coverage": coverage,
            "total": int(len(df)),
            "authority": "PRESERVED_CONTEXT_FIELD" if coverage else "MISSING",
            "detail": detail,
        })
    return pd.DataFrame(rows)


def anchor_audit(df: pd.DataFrame) -> pd.DataFrame:
    exact_patterns = [
        r"(?:^|_)wave1_high(?:_|$)", r"(?:^|_)wave1_low(?:_|$)",
        r"(?:^|_)wave1_start(?:_|$)", r"(?:^|_)pb_low(?:_|$)",
        r"(?:^|_)pullback_low(?:_|$)",
    ]
    exact_cols = []
    for c in df.columns:
        lc = c.lower()
        if "inferred" in lc:
            continue
        if any(re.search(p, lc) for p in exact_patterns):
            exact_cols.append(c)
    inferred = [c for c in df.columns if "inferred" in c.lower() and any(k in c.lower() for k in ["wave", "pb", "pullback"])]
    rows = []
    for name, cols, exact in [
        ("exact_wave1_pb_anchor_columns", exact_cols, 1),
        ("inferred_wave1_pb_columns", inferred, 0),
    ]:
        coverage = 0
        if cols:
            coverage = int(df[cols].notna().any(axis=1).sum())
        rows.append({
            "item": name,
            "column_count": int(len(cols)),
            "event_coverage": int(coverage),
            "total": int(len(df)),
            "is_exact_strategy_anchor": exact,
            "columns": ";".join(cols),
        })
    return pd.DataFrame(rows)


def validate_and_copy_blind(sample_path, bars_path, key_path, outdir: Path, expected=40):
    sample = read_csv(sample_path, dtype={"code": str})
    bars = read_csv(bars_path, dtype={"code": str})
    key = read_csv(key_path, dtype={"code": str})
    if sample.empty or bars.empty or key.empty:
        raise RuntimeError("R12_TRUE_BLIND_INPUT_MISSING")
    forbidden = {
        "outcome4", "primary_outcome_group", "sample_stratum",
        "MFE5_CLOSE_NONPOS", "MFE5_CLOSE_POS", "DEEP_MAE_D5",
    }
    future_prefixes = ("origin_d1_", "origin_d3_", "origin_d5_", "origin_d10_", "origin_d15_")
    leaked = [c for c in sample.columns if c in forbidden or c.startswith(future_prefixes)]
    if leaked:
        raise RuntimeError(f"R12_TRUE_BLIND_LABEL_LEAK:{leaked}")
    if "blind_id" not in sample.columns or "blind_id" not in bars.columns or "blind_id" not in key.columns:
        raise RuntimeError("R12_TRUE_BLIND_ID_MISSING")
    sids = set(sample["blind_id"].astype(str))
    bids = set(bars["blind_id"].astype(str))
    kids = set(key["blind_id"].astype(str))
    if len(sids) != expected or not sids.issubset(bids) or not sids.issubset(kids):
        raise RuntimeError(f"R12_TRUE_BLIND_COVERAGE_FAIL:sample={len(sids)} bars={len(bids)} key={len(kids)}")
    if "bar_offset" not in bars.columns or numeric(bars["bar_offset"]).max() > 0:
        raise RuntimeError("R12_TRUE_BLIND_FUTURE_BAR_LEAK")
    shutil.copy2(sample_path, outdir / "blind_review_sample.csv")
    shutil.copy2(bars_path, outdir / "blind_review_bars.csv")
    shutil.copy2(key_path, outdir / "blind_review_key_DO_NOT_OPEN_UNTIL_REVIEW.csv")
    return {
        "blind_sample_events": len(sids),
        "blind_bar_rows": int(len(bars)),
        "blind_max_bar_offset": int(numeric(bars["bar_offset"]).max()),
    }


def self_test() -> int:
    d = pd.DataFrame({
        "origin_d5_hit_plus5": [1, 1, 1, 0],
        "origin_d5_close_ret_pct": [3, -1, 2, -4],
        "origin_d5_mae_pct": [-2, -3, -7, -8],
        "v72_pullback_restart_debug": [
            "score=112, impulse=45.8, support=4, k=88.1/46.6, rsi=58.1, disp=106.9, wick=0.17"
        ] * 4,
        # Existing parsed field exercises idempotent backfill.
        "v72_debug_rsi": [58.1, np.nan, 58.1, 58.1],
    })
    x = backfill_debug_columns(d)
    got = assign_outcome4(x).tolist()
    exp = ["CLEAN_WIN", "GIVEBACK", "DEEP_MAE_WIN", "NO_HIT"]
    assert got == exp, (got, exp)
    assert not x.columns.duplicated().any()
    assert float(x.loc[1, "v72_debug_rsi"]) == 58.1
    print("REAL_FULL_STRUCTURE_RESEARCH_R12_SELF_TEST_PASS")
    return 0


def run(a) -> int:
    outdir = Path(a.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    identity = read_csv(a.r1_identity)
    if identity.empty or "identity_pass" not in identity.columns or int(numeric(identity["identity_pass"]).fillna(0).iloc[-1]) != 1:
        raise SystemExit("REAL_FULL_R12_IDENTITY_FAIL_CLOSED")
    ident = identity_tuple(identity)
    if ident != EXPECTED_IDENTITY:
        raise SystemExit(f"REAL_FULL_R12_IDENTITY_MISMATCH:{ident}")

    base = read_csv(a.r1_ledger, dtype={"code": str})
    if base.empty:
        raise SystemExit("REAL_FULL_R12_R1_LEDGER_EMPTY")
    if len(base) != EXPECTED_IDENTITY[1]:
        raise SystemExit(f"REAL_FULL_R12_EVENT_COUNT_MISMATCH:{len(base)}")

    x = backfill_debug_columns(base)
    x["outcome4"] = assign_outcome4(x)
    if x["outcome4"].eq("UNCLASSIFIED").any():
        raise SystemExit("REAL_FULL_R12_OUTCOME4_UNCLASSIFIED")
    x["v72_score100"] = numeric(x.get("v72_pullback_restart_score", pd.Series(index=x.index, dtype=float))).eq(100).astype(int)
    x.to_csv(outdir / "r12_event_ledger.csv", index=False, encoding="utf-8-sig")

    score100 = x[x["v72_score100"].eq(1)].copy()
    outcome_summary(x, "ALL115").to_csv(outdir / "outcome4_summary_all.csv", index=False, encoding="utf-8-sig")
    outcome_summary(score100, "V72_SCORE100").to_csv(outdir / "outcome4_summary_score100.csv", index=False, encoding="utf-8-sig")

    features = [
        "v72_pullback_restart_score_raw", "v72_impulse_pct", "v72_pullback_days", "v72_support_count",
        "v72_volume_ratio20", "v72_headroom_pct", "v72_stop_distance_pct",
        "v72_debug_k", "v72_debug_d", "v72_debug_k_minus_d", "v72_debug_rsi", "v72_debug_disp", "v72_debug_wick",
        "ma224_distance_pct", "prior20_below_ma224_rate", "range60_position_pct",
        "pre20_gradual_pulse_days_1p2_2x", "pre20_spike_days_ge2x",
        "signal_amount_vs_pre20_median", "signal_volume_vs_pre20_median",
        "universe_avg_amount20", "universe_amount_ratio_prev_vs20", "universe_volume_ratio_prev_vs20",
        "origin_bb40", "origin_obv_slope",
        "inferred_pb_drawdown_from_wave_high_pct", "inferred_pb_amount_vs_wave_peak", "inferred_pb_volume_vs_wave_peak",
    ]
    feature_long(x, features, "ALL115").to_csv(outdir / "feature_outcome4_all_long.csv", index=False, encoding="utf-8-sig")
    feature_long(score100, features, "V72_SCORE100").to_csv(outdir / "feature_outcome4_score100_long.csv", index=False, encoding="utf-8-sig")

    ma = ma224_path_table(score100)
    ma.to_csv(outdir / "score100_ma224_path_quality.csv", index=False, encoding="utf-8-sig")
    aa = amount_audit(x)
    aa.to_csv(outdir / "actual_amount_audit.csv", index=False, encoding="utf-8-sig")
    wa = anchor_audit(x)
    wa.to_csv(outdir / "wave1_anchor_audit.csv", index=False, encoding="utf-8-sig")

    blind = validate_and_copy_blind(
        a.r1_blind_sample, a.r1_blind_bars, a.r1_blind_key, outdir, expected=a.blind_sample
    )

    prov = {
        "research_id": RESEARCH_ID,
        "research_revision": RESEARCH_REVISION,
        "identity": {"snapshots": ident[0], "events": ident[1], "d5": ident[2], "matrices": ident[3], "pass": 1},
        "score100_events": int(len(score100)),
        "search_logic_changed": 0,
        "score_logic_changed": 0,
        "ranking_changed": 0,
        "order_changed": 0,
        "v72_detector_changed": 0,
        "core224_changed": 0,
        "triangle1pb_changed": 0,
        "low224_changed": 0,
        "same_sample_tuning_allowed": 0,
        "shadow_frozen": 0,
        "blind": blind,
    }
    (outdir / "r12_provenance.json").write_text(json.dumps(prov, ensure_ascii=False, indent=2), encoding="utf-8")

    counts = score100["outcome4"].value_counts().to_dict()
    ma_lines = []
    for _, r in ma.iterrows():
        ma_lines.append(
            f"- {r['ma224_context']}: n={int(r['events'])}, +5 hit={pct(r['plus5_hit_rate'])}, "
            f"clean among hits={pct(r['clean_among_hits_rate'])}, path-damage among hits={pct(r['path_damage_among_hits_rate'])}"
        )
    proxy_events = int(x.get("amount_source", pd.Series([""] * len(x))).fillna("").astype(str).str.contains("proxy|close.*volume", case=False, regex=True).sum())
    d1_amount_cov = int(numeric(x.get("universe_amount_ratio_prev_vs20", pd.Series(index=x.index, dtype=float))).notna().sum())
    exact_anchor_row = wa[wa["item"].eq("exact_wave1_pb_anchor_columns")]
    exact_anchor_cov = int(exact_anchor_row["event_coverage"].iloc[0]) if len(exact_anchor_row) else 0

    report = [
        "🧪 [REAL_FULL STRUCTURE RESEARCH R1.2 · V72 SCORE=100 CAUSAL AUDIT]",
        f"revision={RESEARCH_REVISION}",
        f"identity={ident[0]}/{ident[1]}/{ident[2]}/{ident[3]} PASS",
        "authority=DESCRIPTIVE_HISTORICAL_RESEARCH_ONLY · exact WAIT→NEAR→READY replay unavailable",
        "production/search/score/rank/order changes=0 · same-sample tuning=PROHIBITED",
        "",
        "[Fixed outcome4 · V72 score=100]",
        f"score100 n={len(score100)} / {len(x)}",
        f"CLEAN_WIN={counts.get('CLEAN_WIN',0)} · GIVEBACK={counts.get('GIVEBACK',0)} · DEEP_MAE_WIN={counts.get('DEEP_MAE_WIN',0)} · NO_HIT={counts.get('NO_HIT',0)}",
        "",
        "[MA224 context · path quality, not a gate]",
        *ma_lines,
        "Interpretation: compare hit probability separately from post-hit path damage. No threshold selection is performed.",
        "",
        "[Amount authority audit]",
        f"signal/pre20 path proxy amount events={proxy_events}/{len(x)}",
        f"D-1 universe amount-ratio context coverage={d1_amount_cov}/{len(x)}",
        "Do not reinterpret Close×Volume proxy as actual KRX trading value.",
        "",
        "[Wave1/PB anchor audit]",
        f"exact Wave1/PB anchor event coverage={exact_anchor_cov}/{len(x)}",
        "Inferred Wave1/PB fields remain audit-only and cannot define a reclaim/pullback gate.",
        "",
        "[Blind fidelity]",
        f"blind events={blind['blind_sample_events']} · causal bar rows={blind['blind_bar_rows']} · max bar_offset={blind['blind_max_bar_offset']}",
        "outcome/stratum key is isolated in blind_review_key_DO_NOT_OPEN_UNTIL_REVIEW.csv",
        "",
        "[R1C1 Shadow decision]",
        "shadow_frozen=NO",
        "Reason: current differences are from the same 115-event historical population. A structural candidate may be named, but no production gate/cutoff may be selected before prospective/OOS append-only confirmation.",
    ]
    (outdir / "real_full_structure_research_r12_report.txt").write_text("\n".join(report) + "\n", encoding="utf-8")
    print("\n".join(report))
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--r1-ledger")
    p.add_argument("--r1-identity")
    p.add_argument("--r1-blind-sample")
    p.add_argument("--r1-blind-bars")
    p.add_argument("--r1-blind-key")
    p.add_argument("--output-dir", default="reports/real_full_structure_research_r12")
    p.add_argument("--blind-sample", type=int, default=40)
    p.add_argument("--self-test", action="store_true")
    a = p.parse_args()
    if a.self_test:
        return self_test()
    required = [a.r1_ledger, a.r1_identity, a.r1_blind_sample, a.r1_blind_bars, a.r1_blind_key]
    if any(not v for v in required):
        p.error("R1 ledger/identity/true-blind sample/bars/key are required")
    return run(a)


if __name__ == "__main__":
    raise SystemExit(main())
