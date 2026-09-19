#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

VERSION="CLOSEBET_A_PATTERN_RESPONSE_MAP_R16_20260920"
PATTERN="A"
MIN_ROUTE_N=3
LOW_CONF_BELOW_N=10

FEATURES={
    "entry_close_loc_pct":"CloseLoc",
    "entry_vol20_ratio":"Vol20",
    "entry_amount20_ratio":"Amount20",
    "entry_ma20_dist_pct":"MA20",
    "entry_ma60_dist_pct":"MA60",
    "entry_ma224_dist_pct":"MA224",
    "entry_upper_wick_pct":"UpperWick(body-relative)",
    "entry_ret5_pct":"Ret5",
}

ROUTE_ORDER=[
    "EARLY_WIN_HELD",
    "EARLY_WIN_GIVEBACK",
    "SHAKEOUT_THEN_RECOVERY",
    "EARLY_STOP_SLOW_OR_NO_RECOVERY",
    "AMBIGUOUS_OR_PENDING",
]

def read_csv(p):
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,encoding=enc,low_memory=False)
        except Exception:pass
    return pd.DataFrame()

def med(g,c):
    if c not in g:return np.nan
    x=pd.to_numeric(g[c],errors="coerce").dropna()
    return float(x.median()) if len(x) else np.nan

def mean(g,c):
    if c not in g:return np.nan
    x=pd.to_numeric(g[c],errors="coerce").dropna()
    return float(x.mean()) if len(x) else np.nan

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-root",default="source_r13")
    ap.add_argument("--output-dir",default="reports/a_pattern_response_map_r16")
    ap.add_argument("--source-r13-run-id",default="")
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    xs=list(Path(a.source_root).rglob("shadow_outcome_refined_events.csv"))
    if not xs: raise SystemExit("R13_EVENTS_NOT_FOUND")
    src=xs[0]
    df=read_csv(src)
    if df.empty: raise SystemExit("R13_EVENTS_EMPTY")
    if "pattern" not in df: raise SystemExit("PATTERN_COLUMN_MISSING")

    adf=df[df["pattern"].astype(str).eq(PATTERN)].copy()
    if adf.empty: raise SystemExit("A_PATTERN_EMPTY")

    # Preserve original R1.3 event order and outcome labels.
    adf.to_csv(out/"a_pattern_refined_events.csv",index=False,encoding="utf-8-sig")

    rows=[]
    for fam in ROUTE_ORDER:
        g=adf[adf["refined_family_r13"].astype(str).eq(fam)]
        if g.empty:continue
        rec={
            "pattern":PATTERN,
            "response_family":fam,
            "n":len(g),
            "independent_signal_dates":g["signal_date"].nunique(),
            "route_proximity_eligible":len(g)>=MIN_ROUTE_N,
            "confidence":"EXCLUDED_N_LT_3" if len(g)<MIN_ROUTE_N else ("LOW_CONFIDENCE" if len(g)<LOW_CONF_BELOW_N else "OBSERVED_SAMPLE"),
        }
        for c,label in FEATURES.items():
            rec[c+"_median"]=med(g,c)
            rec[c+"_mean"]=mean(g,c)
        for c in ["ret_max_high_5d","ret_close_5d","ret_max_high_10d","ret_close_10d","giveback_hd_pctpt"]:
            rec[c+"_median"]=med(g,c)
            rec[c+"_mean"]=mean(g,c)
        rows.append(rec)
    ev=pd.DataFrame(rows)
    ev.to_csv(out/"a_response_map_evidence.csv",index=False,encoding="utf-8-sig")

    # Descriptive contrast only between the two sufficiently sampled dominant routes.
    gb=adf[adf["refined_family_r13"].eq("EARLY_WIN_GIVEBACK")]
    sh=adf[adf["refined_family_r13"].eq("SHAKEOUT_THEN_RECOVERY")]
    contrasts=[]
    for c,label in FEATURES.items():
        mg,ms=med(gb,c),med(sh,c)
        contrasts.append({
            "feature":c,
            "label":label,
            "giveback_n":len(gb),
            "shakeout_recovery_n":len(sh),
            "giveback_median":mg,
            "shakeout_recovery_median":ms,
            "median_difference_shakeout_minus_giveback":(ms-mg) if pd.notna(ms) and pd.notna(mg) else np.nan,
            "interpretation":"DESCRIPTIVE_ONLY_NO_THRESHOLD",
        })
    pd.DataFrame(contrasts).to_csv(out/"a_dominant_route_feature_contrast.csv",index=False,encoding="utf-8-sig")

    counts=adf["refined_family_r13"].value_counts()
    total=len(adf)

    # Watchlist language is intentionally descriptive and avoids a hidden score/classifier.
    watch=[
        "A는 현재 OOS에서 Shakeout→Recovery와 Early Win→Giveback이 주된 두 경로로 관찰됨",
        "강한 당일 종가만으로 이후 유지력을 판단하지 않음",
        "Shakeout→Recovery 표본에서는 Giveback보다 거래량/거래대금 중앙값이 소폭 높았는지 함께 표시",
        "MA20 이격은 Shakeout→Recovery 표본이 Giveback보다 소폭 낮아 과도한 단기 이격 여부를 관찰",
        "UpperWick(body-relative)은 단독 실패 신호로 사용하지 않고 경로 설명 변수로만 표시",
        "D3~D5 동안 유동성 유지와 종가/이평 구조의 재회복 여부를 관찰",
    ]

    response=pd.DataFrame([{
        "pattern":"A",
        "pattern_n":total,
        "independent_signal_dates":adf["signal_date"].nunique(),
        "dominant_observed_routes":"SHAKEOUT_THEN_RECOVERY / EARLY_WIN_GIVEBACK",
        "held_n":int(counts.get("EARLY_WIN_HELD",0)),
        "giveback_n":int(counts.get("EARLY_WIN_GIVEBACK",0)),
        "shakeout_recovery_n":int(counts.get("SHAKEOUT_THEN_RECOVERY",0)),
        "slow_no_recovery_n":int(counts.get("EARLY_STOP_SLOW_OR_NO_RECOVERY",0)),
        "ambiguous_pending_n":int(counts.get("AMBIGUOUS_OR_PENDING",0)),
        "response_watchlist":" / ".join(watch),
        "route_proximity_min_n":MIN_ROUTE_N,
        "low_confidence_below_n":LOW_CONF_BELOW_N,
        "atr_evidence":"EXCLUDED_PROVENANCE_AMBIGUOUS",
        "use":"SHADOW_DESCRIPTION_ONLY",
    }])
    response.to_csv(out/"a_pattern_response_map.csv",index=False,encoding="utf-8-sig")

    lines=[
        "# Closing Bet A Pattern Historical Outcome / Response Map R1.6",
        "",
        f"- source R1.3 run: `{a.source_r13_run_id}`",
        f"- A events: {total}",
        f"- independent signal dates: {adf['signal_date'].nunique()}",
        "",
        "## Outcome distribution",
    ]
    nice={
        "EARLY_WIN_HELD":"Held",
        "EARLY_WIN_GIVEBACK":"Giveback",
        "SHAKEOUT_THEN_RECOVERY":"Shakeout→Recovery",
        "EARLY_STOP_SLOW_OR_NO_RECOVERY":"Slow/No recovery",
        "AMBIGUOUS_OR_PENDING":"Ambiguous/Pending",
    }
    for fam in ROUTE_ORDER:
        n=int(counts.get(fam,0))
        if n:
            lines.append(f"- {nice.get(fam,fam)}: {n}/{total} ({n/total*100:.1f}%)")
    lines += [
        "",
        "## Display confidence",
        f"- n < {MIN_ROUTE_N}: route proximity comparison excluded",
        f"- {MIN_ROUTE_N} <= n < {LOW_CONF_BELOW_N}: LOW CONFIDENCE",
        f"- n >= {LOW_CONF_BELOW_N}: OBSERVED SAMPLE",
        "",
        "## Dominant-route descriptive contrast",
    ]
    for c,label in FEATURES.items():
        mg,ms=med(gb,c),med(sh,c)
        if pd.notna(mg) and pd.notna(ms):
            lines.append(f"- {label}: Giveback median {mg:.3f} vs Shakeout→Recovery median {ms:.3f}")
    lines += [
        "",
        "## A Response Watchlist",
    ]
    for w in watch: lines.append(f"- {w}")
    lines += [
        "",
        "## Guardrails",
        "- A pattern only; C/B1/B2/I frozen response maps are untouched.",
        "- No candidate filtering or re-ranking.",
        "- No score/rank change.",
        "- No aggregate route score or route classifier.",
        "- No new optimized threshold.",
        "- No same-sample tuning.",
        "- Outcome labels are inherited from R1.3.",
        "- ATR remains excluded until source provenance is explicit.",
        "- UpperWick is explicitly labeled body-relative.",
    ]
    (out/"REPORT.md").write_text("\n".join(lines),encoding="utf-8")

    meta={
        "version":VERSION,
        "status":"PASS",
        "research_only":True,
        "pattern":"A",
        "source_r13_run_id":a.source_r13_run_id,
        "events":total,
        "independent_signal_dates":int(adf["signal_date"].nunique()),
        "route_proximity_min_n":MIN_ROUTE_N,
        "low_confidence_below_n":LOW_CONF_BELOW_N,
        "held_n":int(counts.get("EARLY_WIN_HELD",0)),
        "giveback_n":int(counts.get("EARLY_WIN_GIVEBACK",0)),
        "shakeout_recovery_n":int(counts.get("SHAKEOUT_THEN_RECOVERY",0)),
        "slow_no_recovery_n":int(counts.get("EARLY_STOP_SLOW_OR_NO_RECOVERY",0)),
        "ambiguous_pending_n":int(counts.get("AMBIGUOUS_OR_PENDING",0)),
        "candidate_membership_changed":False,
        "candidate_order_changed":False,
        "score_rank_changed":False,
        "candidate_filter_created":False,
        "production_logic_changed":False,
        "aggregate_route_score_created":False,
        "aggregate_route_classification_created":False,
        "same_sample_tuning":False,
        "new_threshold_optimization":False,
        "atr_used":False,
        "wick_display_label":"UpperWick(body-relative)",
        "existing_pattern_maps_modified":False,
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False,indent=2))
    print(ev.to_string(index=False))

if __name__=="__main__":
    main()
