#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

RESEARCH_ID = "REAL_FULL_STRUCTURE_RESEARCH_R1_2"
RESEARCH_REVISION = "R1_2_V72_SATURATION_OUTCOME4_BLIND_FIDELITY"
BLIND_SEED = "REAL_FULL_R1_2_BLIND_V1"


def num(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else np.nan
    except Exception:
        return np.nan


def read_csv(path: str | Path, **kwargs) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def med(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return float(x.median()) if len(x) else np.nan


def pct_true(s: pd.Series) -> float:
    if s is None:
        return np.nan
    x = pd.Series(s).dropna()
    if x.empty:
        return np.nan
    if x.dtype == bool:
        return float(x.mean())
    z = pd.to_numeric(x, errors="coerce").dropna()
    return float((z != 0).mean()) if len(z) else np.nan


def fmt(v, suffix=""):
    x = num(v)
    return "NA" if not math.isfinite(x) else f"{x:.2f}{suffix}"


def fmt_rate(v):
    x = num(v)
    return "NA" if not math.isfinite(x) else f"{x*100:.1f}%"


def parse_v72_debug(s: str) -> Dict[str, float]:
    txt = str(s or "")
    out: Dict[str, float] = {}
    pats = {
        "v72_debug_score": r"(?:^|,\s*)score=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_impulse": r"(?:^|,\s*)impulse=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_support": r"(?:^|,\s*)support=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_rsi": r"(?:^|,\s*)rsi=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_disp": r"(?:^|,\s*)disp=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_wick": r"(?:^|,\s*)wick=([+-]?\d+(?:\.\d+)?)",
    }
    for k, pat in pats.items():
        m = re.search(pat, txt)
        out[k] = float(m.group(1)) if m else np.nan
    m = re.search(r"(?:^|,\s*)k=([+-]?\d+(?:\.\d+)?)/([+-]?\d+(?:\.\d+)?)", txt)
    if m:
        out["v72_debug_k"] = float(m.group(1))
        out["v72_debug_d"] = float(m.group(2))
        out["v72_debug_k_minus_d"] = float(m.group(1)) - float(m.group(2))
    else:
        out["v72_debug_k"] = np.nan
        out["v72_debug_d"] = np.nan
        out["v72_debug_k_minus_d"] = np.nan
    return out


def assign_outcome4(df: pd.DataFrame) -> pd.Series:
    hit = pd.to_numeric(df.get("origin_d5_hit_plus5"), errors="coerce").fillna(0).eq(1)
    close = pd.to_numeric(df.get("origin_d5_close_ret_pct"), errors="coerce")
    mae = pd.to_numeric(df.get("origin_d5_mae_pct"), errors="coerce")
    conds = [
        ~hit,
        hit & close.le(0),
        hit & close.gt(0) & mae.le(-5),
        hit & close.gt(0) & mae.gt(-5),
    ]
    labels = ["NO_HIT", "GIVEBACK", "DEEP_MAE_WIN", "CLEAN_WIN"]
    return pd.Series(np.select(conds, labels, default="UNCLASSIFIED"), index=df.index)


def outcome_row(g: pd.DataFrame, label: str) -> Dict[str, object]:
    return {
        "group": label,
        "events": int(len(g)),
        "d5_close_median": med(g.get("origin_d5_close_ret_pct", pd.Series(dtype=float))),
        "d5_mfe_median": med(g.get("origin_d5_mfe_pct", pd.Series(dtype=float))),
        "d5_mae_median": med(g.get("origin_d5_mae_pct", pd.Series(dtype=float))),
        "v72_prc_rate": pct_true(g.get("v72_pullback_restart", pd.Series(dtype=float))),
        "v72_score100_rate": float(pd.to_numeric(g.get("v72_pullback_restart_score"), errors="coerce").eq(100).mean()) if len(g) else np.nan,
        "v72_grade_a_rate": float(g.get("v72_pullback_restart_grade", pd.Series(index=g.index, dtype=str)).fillna("").astype(str).str.upper().eq("A").mean()) if len(g) else np.nan,
        "v72_raw_score_median": med(g.get("v72_pullback_restart_score_raw", pd.Series(dtype=float))),
    }


def feature_compare(df: pd.DataFrame, features: List[str]) -> pd.DataFrame:
    rows = []
    order = ["CLEAN_WIN", "GIVEBACK", "DEEP_MAE_WIN", "NO_HIT"]
    for grp in order:
        g = df[df["outcome4"].eq(grp)]
        rec = outcome_row(g, grp)
        for c in features:
            if c not in g.columns:
                continue
            vals = pd.to_numeric(g[c], errors="coerce")
            rec[f"{c}__n"] = int(vals.notna().sum())
            rec[f"{c}__median"] = med(vals)
        rows.append(rec)
    return pd.DataFrame(rows)


def deterministic_blind_sample(df: pd.DataFrame, limit: int = 40) -> pd.DataFrame:
    # Balanced enough to cover all four paths, but final ordering is globally shuffled.
    quotas = {"CLEAN_WIN": 12, "GIVEBACK": 10, "DEEP_MAE_WIN": 8, "NO_HIT": 10}
    picks = []
    for grp, n in quotas.items():
        z = df[df["outcome4"].eq(grp)].copy()
        if z.empty:
            continue
        z["_pick_hash"] = z.apply(lambda r: hashlib.sha256((BLIND_SEED+"|pick|"+str(r.get("event_id") or f"{r.get('origin_date')}|{r.get('code')}|{r.get('origin_rank')}" )).encode()).hexdigest(), axis=1)
        z = z.sort_values("_pick_hash").head(n)
        picks.append(z.drop(columns=["_pick_hash"], errors="ignore"))
    if not picks:
        return pd.DataFrame()
    out = pd.concat(picks, ignore_index=True).copy()
    out["_shuffle_hash"] = out.apply(lambda r: hashlib.sha256((BLIND_SEED+"|shuffle|"+str(r.get("event_id") or f"{r.get('origin_date')}|{r.get('code')}|{r.get('origin_rank')}" )).encode()).hexdigest(), axis=1)
    out = out.sort_values("_shuffle_hash").drop(columns=["_shuffle_hash"]).head(limit).reset_index(drop=True)
    out.insert(0, "blind_id", [f"B{i:03d}" for i in range(1, len(out)+1)])
    return out


def build_blind_files(sample: pd.DataFrame, r1_bars: pd.DataFrame, outdir: Path) -> None:
    if sample.empty:
        pd.DataFrame().to_csv(outdir/"blind_review_sample.csv", index=False)
        pd.DataFrame().to_csv(outdir/"blind_review_key.csv", index=False)
        pd.DataFrame().to_csv(outdir/"blind_review_bars.csv", index=False)
        return

    # Key preserves outcome labels privately for reveal after chart classification.
    key_cols = [c for c in ["blind_id","event_id","origin_date","code","name","origin_rank","outcome4","origin_d5_hit_plus5","origin_d5_close_ret_pct","origin_d5_mfe_pct","origin_d5_mae_pct"] if c in sample.columns]
    sample[key_cols].to_csv(outdir/"blind_review_key.csv", index=False, encoding="utf-8-sig")

    # Blind sample excludes all outcome/future columns and outcome labels.
    hidden_prefixes = ("origin_d1_","origin_d3_","origin_d5_","origin_d10_","origin_d15_")
    hidden_exact = {"outcome4","primary_outcome_group","MFE5_CLOSE_NONPOS","MFE5_CLOSE_POS","DEEP_MAE_D5","sample_stratum"}
    keep = [c for c in sample.columns if c not in hidden_exact and not c.startswith(hidden_prefixes)]
    blind = sample[keep].copy()
    blind.to_csv(outdir/"blind_review_sample.csv", index=False, encoding="utf-8-sig")

    # Build causal bars only: bar_offset <= 0. No post-signal path is exposed.
    if r1_bars.empty:
        pd.DataFrame().to_csv(outdir/"blind_review_bars.csv", index=False)
        return
    bars = r1_bars.copy()
    bars["event_id"] = bars["event_id"].astype(str)
    map_id = sample[["event_id","blind_id"]].copy()
    map_id["event_id"] = map_id["event_id"].astype(str)
    bars = bars.merge(map_id, on="event_id", how="inner")
    bars = bars[pd.to_numeric(bars.get("bar_offset"), errors="coerce").le(0)].copy()
    # Remove outcome labels from bar file as well.
    for c in ["primary_outcome_group"]:
        if c in bars.columns:
            bars = bars.drop(columns=[c])
    cols = ["blind_id"] + [c for c in bars.columns if c != "blind_id"]
    bars = bars[cols].sort_values(["blind_id","bar_offset"])
    bars.to_csv(outdir/"blind_review_bars.csv", index=False, encoding="utf-8-sig")


def run(a) -> int:
    outdir = Path(a.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    df = read_csv(a.r1_ledger, dtype={"code": str})
    if df.empty:
        raise SystemExit("REAL_FULL_R12_R1_LEDGER_EMPTY")

    identity = read_csv(a.r1_identity)
    if identity.empty or int(pd.to_numeric(identity.get("identity_pass"), errors="coerce").fillna(0).iloc[-1]) != 1:
        (outdir/"real_full_structure_research_r12_report.txt").write_text(
            "🧪 [REAL_FULL STRUCTURE RESEARCH R1.2]\nIDENTITY FAIL-CLOSED · R1 identity_pass != 1\n성과 해석 중단. 검색식/점수/랭킹 변경 0.\n", encoding="utf-8")
        return 41

    dbg = pd.DataFrame([parse_v72_debug(x) for x in df.get("v72_pullback_restart_debug", pd.Series([""]*len(df)))], index=df.index)
    x = pd.concat([df.reset_index(drop=True), dbg.reset_index(drop=True)], axis=1)
    x["outcome4"] = assign_outcome4(x)
    x["v72_score100"] = pd.to_numeric(x.get("v72_pullback_restart_score"), errors="coerce").eq(100).astype(int)
    x["v72_raw_ge100"] = pd.to_numeric(x.get("v72_pullback_restart_score_raw"), errors="coerce").ge(100).astype(int)
    x["v72_grade_a"] = x.get("v72_pullback_restart_grade", pd.Series(index=x.index, dtype=str)).fillna("").astype(str).str.upper().eq("A").astype(int)
    x.to_csv(outdir/"r12_event_ledger.csv", index=False, encoding="utf-8-sig")

    counts = x["outcome4"].value_counts().to_dict()
    prc_rate = pct_true(x.get("v72_pullback_restart"))
    score100_rate = float(x["v72_score100"].mean())
    grade_a_rate = float(x["v72_grade_a"].mean())
    raw_ge100_rate = float(x["v72_raw_ge100"].mean())

    saturation = pd.DataFrame([
        {"metric":"v72_pullback_restart_true","count":int(pd.Series(x.get("v72_pullback_restart")).fillna(False).astype(bool).sum()),"total":len(x),"rate":prc_rate},
        {"metric":"v72_score_eq_100","count":int(x["v72_score100"].sum()),"total":len(x),"rate":score100_rate},
        {"metric":"v72_grade_A","count":int(x["v72_grade_a"].sum()),"total":len(x),"rate":grade_a_rate},
        {"metric":"v72_raw_score_ge_100","count":int(x["v72_raw_ge100"].sum()),"total":len(x),"rate":raw_ge100_rate},
    ])
    saturation.to_csv(outdir/"v72_saturation_audit.csv", index=False, encoding="utf-8-sig")

    features = [
        "v72_pullback_restart_score_raw","v72_impulse_pct","v72_pullback_days","v72_support_count","v72_volume_ratio20","v72_headroom_pct","v72_stop_distance_pct",
        "v72_debug_k","v72_debug_d","v72_debug_k_minus_d","v72_debug_rsi","v72_debug_disp","v72_debug_wick",
        "ma224_distance_pct","prior20_below_ma224_rate","range60_position_pct","pre20_gradual_pulse_days_1p2_2x","pre20_spike_days_ge2x",
        "signal_amount_vs_pre20_median","signal_volume_vs_pre20_median","origin_bb40","origin_obv_slope",
    ]
    comp_all = feature_compare(x, features)
    comp_all.to_csv(outdir/"outcome4_structure_comparison.csv", index=False, encoding="utf-8-sig")

    score100 = x[x["v72_score100"].eq(1)].copy()
    comp100 = feature_compare(score100, features)
    comp100.to_csv(outdir/"v72_score100_outcome4_comparison.csv", index=False, encoding="utf-8-sig")

    # Fixed raw-score bins for descriptive saturation review only.
    raw = pd.to_numeric(x.get("v72_pullback_restart_score_raw"), errors="coerce")
    x["v72_raw_score_bin"] = pd.cut(raw, bins=[-np.inf,69,94,104,np.inf], labels=["<70","70-94","95-104","105+"])
    rows=[]
    for b,g in x.groupby("v72_raw_score_bin", observed=False, dropna=False):
        if not len(g):
            continue
        r=outcome_row(g,str(b))
        for name in ["CLEAN_WIN","GIVEBACK","DEEP_MAE_WIN","NO_HIT"]:
            r[f"{name}_rate"] = float(g["outcome4"].eq(name).mean())
        rows.append(r)
    pd.DataFrame(rows).to_csv(outdir/"v72_raw_score_bin_outcomes.csv", index=False, encoding="utf-8-sig")

    # Fully blinded, globally shuffled review pack. Source R1 bars are reused but post-origin bars are removed.
    r1_bars = read_csv(a.r1_review_bars, dtype={"code": str})
    blind = deterministic_blind_sample(x, limit=a.blind_sample)
    build_blind_files(blind, r1_bars, outdir)

    # Minimal signal-quality observation: do not select thresholds or a shadow condition here.
    clean = x[x["outcome4"].eq("CLEAN_WIN")]
    give = x[x["outcome4"].eq("GIVEBACK")]
    deep = x[x["outcome4"].eq("DEEP_MAE_WIN")]
    nohit = x[x["outcome4"].eq("NO_HIT")]
    score100_counts = score100["outcome4"].value_counts().to_dict()

    report = "\n".join([
        "🧪 [REAL_FULL STRUCTURE RESEARCH R1.2]",
        "RESEARCH ONLY · 본 검색식/점수/랭킹/주문 변경 0 · same-sample threshold tuning 금지",
        f"revision={RESEARCH_REVISION}",
        f"source events={len(x)} · R1 identity PASS",
        "",
        "🎯 [Mutually-exclusive D+5 Outcome4]",
        f"CLEAN_WIN n={len(clean)} · +5% hit + D5 Close>0 + MAE>-5%",
        f"GIVEBACK n={len(give)} · +5% hit + D5 Close≤0",
        f"DEEP_MAE_WIN n={len(deep)} · +5% hit + D5 Close>0 + MAE≤-5%",
        f"NO_HIT n={len(nohit)} · D5 +5% 미도달",
        f"coverage={sum(int(counts.get(k,0)) for k in ['CLEAN_WIN','GIVEBACK','DEEP_MAE_WIN','NO_HIT'])}/{len(x)}",
        "",
        "🧯 [V72 Saturation Audit]",
        f"pullback_restart TRUE={fmt_rate(prc_rate)}",
        f"score=100={fmt_rate(score100_rate)} · Grade A={fmt_rate(grade_a_rate)} · raw_score≥100={fmt_rate(raw_ge100_rate)}",
        f"score100 n={len(score100)} outcome: CLEAN={score100_counts.get('CLEAN_WIN',0)} / GIVEBACK={score100_counts.get('GIVEBACK',0)} / DEEP_MAE_WIN={score100_counts.get('DEEP_MAE_WIN',0)} / NO_HIT={score100_counts.get('NO_HIT',0)}",
        "※ score100 자체의 분별력은 outcome4_structure_comparison.csv와 v72_score100_outcome4_comparison.csv에서 구조 변수별로 감사.",
        "",
        "🧬 [V72 Debug Decomposition]",
        "score/raw_score뿐 아니라 impulse, support, stochastic K/D, RSI, disp, wick, volume_ratio20, headroom, stop_distance를 분리 저장.",
        "이번 표본 결과로 threshold를 생성하지 않음. 차이는 가설 후보일 뿐.",
        "",
        "🕶️ [Blind Fidelity Pack]",
        f"blind sample={len(blind)} · outcome 라벨/미래 D+경로 숨김 · 전체 순서 global deterministic shuffle",
        "blind_review_bars.csv는 bar_offset≤0만 포함하여 signal 이후 미래 가격을 차단.",
        "blind_review_key.csv는 판정 종료 후 reveal 전용.",
        "",
        "🔒 [Decision]",
        "R1.2는 Shadow 생성 단계가 아님. 먼저 V72=100 포화 내부에서 반복되는 causal 구조 차이 + blind chart fidelity를 확인.",
        "정확한 historical Amount와 exact Wave1 anchor가 없으면 관련 feature는 계측 보강 후보로만 유지.",
        "CORE224/TRIANGLE1PB/LOW224와 독립. REAL_FULL 본 검색 로직 변경 0.",
        "➡️ NEXT: blind 40개를 outcome 미공개 상태로 구조 판정 → key reveal → 반복되는 차이 1개만 R1C1 Shadow 후보 검토.",
    ])
    (outdir/"real_full_structure_research_r12_report.txt").write_text(report, encoding="utf-8")
    provenance = {
        "research_id":RESEARCH_ID,"revision":RESEARCH_REVISION,"research_only":1,
        "search_logic_changed":0,"score_changed":0,"ranking_changed":0,"order_changed":0,
        "same_sample_threshold_tuning_allowed":0,"shadow_created":0,
        "outcome4_definition":{"CLEAN_WIN":"hit+5 & close>0 & MAE>-5","GIVEBACK":"hit+5 & close<=0","DEEP_MAE_WIN":"hit+5 & close>0 & MAE<=-5","NO_HIT":"no +5 hit"},
        "blind_seed":BLIND_SEED,"blind_future_bars_included":0,
    }
    (outdir/"provenance.json").write_text(json.dumps(provenance, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report)
    return 0


def self_test() -> int:
    z = pd.DataFrame([
        {"origin_d5_hit_plus5":1,"origin_d5_close_ret_pct":2,"origin_d5_mae_pct":-2},
        {"origin_d5_hit_plus5":1,"origin_d5_close_ret_pct":-1,"origin_d5_mae_pct":-7},
        {"origin_d5_hit_plus5":1,"origin_d5_close_ret_pct":2,"origin_d5_mae_pct":-7},
        {"origin_d5_hit_plus5":0,"origin_d5_close_ret_pct":1,"origin_d5_mae_pct":-1},
    ])
    assert assign_outcome4(z).tolist()==["CLEAN_WIN","GIVEBACK","DEEP_MAE_WIN","NO_HIT"]
    d=parse_v72_debug("score=112, impulse=45.8, support=4, k=88.1/46.6, rsi=58.1, disp=106.9, wick=0.17")
    assert abs(d["v72_debug_k_minus_d"]-41.5)<1e-9 and d["v72_debug_support"]==4
    print("REAL_FULL_STRUCTURE_RESEARCH_R1_2_SELF_TEST PASS")
    return 0


def main() -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument("--r1-ledger", default="reports/real_full_structure_research_r1/structure_event_ledger.csv")
    ap.add_argument("--r1-identity", default="reports/real_full_structure_research_r1/identity_audit.csv")
    ap.add_argument("--r1-review-bars", default="reports/real_full_structure_research_r1/manual_review_bars.csv")
    ap.add_argument("--output-dir", default="reports/real_full_structure_research_r12")
    ap.add_argument("--blind-sample", type=int, default=40)
    ap.add_argument("--self-test", action="store_true")
    a=ap.parse_args()
    return self_test() if a.self_test else run(a)

if __name__ == "__main__":
    raise SystemExit(main())
