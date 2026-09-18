#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="CLOSEBET_ENTRY_EXIT_BEHAVIOR_OOS_R110_20260919"
FOCUS_PATTERNS={"C","B1","B2","I"}
DISCOVERY_END="2026-08-18"

FEATURE_ALIASES={
    "close_location":["entry_close_location","close_location","signal_close_location","close_loc"],
    "vol20_ratio":["entry_volume_vs_20d","volume_vs_20d","vol20_ratio","volume_ratio_20d"],
    "amt20_ratio":["entry_amount_vs_20d","amount_vs_20d","amt20_ratio","amount_ratio_20d"],
    "pullback_depth_pct":["pullback_depth_pct","pb_depth_pct"],
    "restart_score":["restart_score"],
    "atr_pct":["entry_atr_pct","atr_pct","signal_atr_pct"],
    "ma20_dist_pct":["entry_price_to_ma20_pct","price_to_ma20_pct","ma20_dist_pct"],
    "ma60_dist_pct":["entry_price_to_ma60_pct","price_to_ma60_pct","ma60_dist_pct"],
    "ma224_dist_pct":["entry_price_to_ma224_pct","price_to_ma224_pct","ma224_dist_pct"],
    "ret5":["entry_stock_ret_5d","stock_ret_5d","ret_5d"],
    "ret20":["entry_stock_ret_20d","stock_ret_20d","ret_20d"],
    "upper_wick":["entry_upper_wick_ratio","upper_wick_ratio","upper_wick"],
}

def read_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:
        return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:
            return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:
            pass
    return pd.DataFrame()

def find(root,name):
    xs=list(Path(root).rglob(name))
    xs.sort(key=lambda p:(len(p.parts),str(p)))
    return xs[0] if xs else None

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():
        s=s[:-2]
    if len(s)==7 and s.startswith("A"):
        s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def get_first(df,names):
    for c in names:
        if c in df.columns:
            return c
    return None

def classify_behavior(r):
    mfe=pd.to_numeric(pd.Series([r.get("mfe20")]),errors="coerce").iloc[0]
    d20=pd.to_numeric(pd.Series([r.get("d20_ret")]),errors="coerce").iloc[0]
    ma5=pd.to_numeric(pd.Series([r.get("ma5_exit_ret")]),errors="coerce").iloc[0]
    if pd.notna(mfe) and mfe>=10 and pd.notna(d20) and d20<=0:
        return "BIG_GIVEBACK"
    if pd.notna(mfe) and mfe>=10 and pd.notna(d20) and d20>0:
        return "SUSTAINED_WINNER"
    if pd.notna(mfe) and 5<=mfe<10 and pd.notna(d20) and d20>0:
        return "MODERATE_SUSTAIN"
    if pd.notna(mfe) and mfe>=5 and pd.notna(d20) and d20<=0:
        return "GIVEBACK"
    if pd.notna(mfe) and mfe<5 and pd.notna(d20) and d20<=0:
        return "FAILED_OR_WEAK"
    if pd.notna(ma5) and pd.notna(d20) and ma5>0 and d20<ma5-5:
        return "EARLY_EXIT_HELPED"
    return "OTHER"

def freeze_bins(discovery):
    rows=[]
    for p,g in discovery.groupby("primary_formula"):
        for feat in FEATURE_ALIASES:
            if feat not in g.columns:
                continue
            s=pd.to_numeric(g[feat],errors="coerce").dropna()
            if len(s)<10:
                continue
            rows.append({"pattern":p,"feature":feat,"n":len(s),
                         "q33":s.quantile(.33),"q67":s.quantile(.67),"median":s.median()})
    return pd.DataFrame(rows)

def apply_bin(v,q33,q67):
    if pd.isna(v):
        return "UNKNOWN"
    if v<=q33:
        return "LOW"
    if v>=q67:
        return "HIGH"
    return "MID"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r105-root",default="r105_artifacts")
    ap.add_argument("--r1091-root",default="r1091_artifacts")
    ap.add_argument("--output-dir",default="reports/closebet_entry_exit_behavior_oos_r110")
    a=ap.parse_args()

    out=Path(a.output_dir)
    out.mkdir(parents=True,exist_ok=True)

    p105=find(a.r105_root,"event_master_with_outcomes_and_shadow_tags.csv")
    p109=find(a.r1091_root,"oos_dynamic_exit_event_ledger.csv")
    if p105 is None or p109 is None:
        raise SystemExit(f"R110_INPUT_MISSING r105={p105} r1091={p109}")

    e=read_csv(p105)
    l=read_csv(p109)
    e["code"]=e["code"].map(norm_code)
    l["code"]=l["code"].map(norm_code)
    e["signal_date"]=pd.to_datetime(e["signal_date"],errors="coerce").dt.normalize()
    l["signal_date"]=pd.to_datetime(l["signal_date"],errors="coerce").dt.normalize()

    for canon,aliases in FEATURE_ALIASES.items():
        c=get_first(e,aliases)
        if c is not None:
            e[canon]=pd.to_numeric(e[c],errors="coerce")

    disc=e[e.signal_date<=pd.Timestamp(DISCOVERY_END)].copy()
    bins=freeze_bins(disc)

    base=l[["signal_date","code","name","primary_formula","mfe20","mae20"]].drop_duplicates(["signal_date","code"]).copy()

    def merge_policy(base,pol,newname):
        z=l[l["policy"].eq(pol)][["signal_date","code","exit_ret","exit_day"]].copy()
        z=z.rename(columns={"exit_ret":newname,"exit_day":newname.replace("_ret","_day")})
        return base.merge(z,on=["signal_date","code"],how="left")

    for pol,nm in [
        ("HOLD_D20","d20_ret"),
        ("MA5_CLOSE_BREAK_AFTER_D3","ma5_exit_ret"),
        ("MA10_CLOSE_BREAK_AFTER_D5","ma10_exit_ret"),
        ("ACT5_GIVEBACK3_CLOSE","gb3_exit_ret"),
        ("ACT10_GIVEBACK5_CLOSE","gb5_exit_ret"),
    ]:
        base=merge_policy(base,pol,nm)

    base["behavior_class"]=base.apply(classify_behavior,axis=1)

    keep=["signal_date","code","primary_formula"]+[c for c in FEATURE_ALIASES if c in e.columns]
    for c in e.columns:
        if c.startswith("TAG_") and c not in keep:
            keep.append(c)

    m=base.merge(e[keep].drop_duplicates(["signal_date","code","primary_formula"]),
                 on=["signal_date","code","primary_formula"],how="left")
    m=m[m.primary_formula.astype(str).isin(FOCUS_PATTERNS)].copy()

    # Behavior counts
    counts=(m.groupby(["primary_formula","behavior_class"]).size().rename("n").reset_index())
    counts["share_pct"]=counts.groupby("primary_formula")["n"].transform(lambda x:x/x.sum()*100)

    # Feature summaries by behavior class
    rows=[]
    for (p,b),g in m.groupby(["primary_formula","behavior_class"]):
        for feat in FEATURE_ALIASES:
            if feat not in g.columns:
                continue
            s=pd.to_numeric(g[feat],errors="coerce").dropna()
            if len(s)<2:
                continue
            rows.append({"pattern":p,"behavior_class":b,"feature":feat,"n":len(s),
                         "mean":s.mean(),"median":s.median(),"p25":s.quantile(.25),"p75":s.quantile(.75)})
    feature_summary=pd.DataFrame(rows)

    # Frozen-bin behavior distribution
    brow=[]
    if not bins.empty:
        key={(r.pattern,r.feature):(r.q33,r.q67) for _,r in bins.iterrows()}
        for p,g in m.groupby("primary_formula"):
            for feat in FEATURE_ALIASES:
                if feat not in g.columns or (p,feat) not in key:
                    continue
                q33,q67=key[(p,feat)]
                z=g.copy()
                vals=pd.to_numeric(z[feat],errors="coerce")
                z["feature_bin"]=[apply_bin(v,q33,q67) for v in vals]
                for (fb,b),h in z.groupby(["feature_bin","behavior_class"]):
                    denom=len(z[z.feature_bin.eq(fb)])
                    brow.append({"pattern":p,"feature":feat,"feature_bin":fb,"behavior_class":b,
                                 "n":len(h),"behavior_share_within_bin_pct":len(h)/denom*100 if denom else np.nan})
    bin_behavior=pd.DataFrame(brow)

    # Family contrast, descriptive only
    family_map={
        "SUSTAIN_FAMILY":{"SUSTAINED_WINNER","MODERATE_SUSTAIN"},
        "GIVEBACK_FAMILY":{"BIG_GIVEBACK","GIVEBACK"},
        "FAIL_FAMILY":{"FAILED_OR_WEAK"},
    }
    crow=[]
    for p,g in m.groupby("primary_formula"):
        for fam,classes in family_map.items():
            z=g[g.behavior_class.isin(classes)]
            if len(z)<2:
                continue
            for feat in FEATURE_ALIASES:
                if feat not in z.columns:
                    continue
                s=pd.to_numeric(z[feat],errors="coerce").dropna()
                if len(s)<2:
                    continue
                crow.append({"pattern":p,"behavior_family":fam,"feature":feat,"n":len(s),
                             "mean":s.mean(),"median":s.median()})
    contrast=pd.DataFrame(crow)

    bins.to_csv(out/"discovery_frozen_feature_bins.csv",index=False,encoding="utf-8-sig")
    m.to_csv(out/"oos_entry_exit_behavior_event_master.csv",index=False,encoding="utf-8-sig")
    counts.to_csv(out/"oos_behavior_class_counts.csv",index=False,encoding="utf-8-sig")
    feature_summary.to_csv(out/"oos_behavior_feature_summary.csv",index=False,encoding="utf-8-sig")
    bin_behavior.to_csv(out/"oos_feature_bin_behavior_distribution.csv",index=False,encoding="utf-8-sig")
    contrast.to_csv(out/"oos_behavior_family_feature_contrast.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REVISION,"status":"PASS","focus_patterns":sorted(FOCUS_PATTERNS),
        "oos_rows":len(m),"discovery_end":DISCOVERY_END,
        "research_only":True,"production_eligible":False,
        "selection_logic_changed":False,"score_rank_changed":False,"order_logic_changed":False,
        "same_sample_retuning":False,
        "feature_bins_frozen_from_discovery_only":True,
        "behavior_labels_use_oos_outcomes_for_description_only":True,
        "composite_score_created":False,"candidate_filter_created":False,
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "🔗 [CLOSING BET · ENTRY QUALITY × EXIT BEHAVIOR OOS R1.0.10]",
        f"status=PASS | focus OOS rows={len(m)}",
        "- discovery는 feature bin(q33/q67) 동결에만 사용",
        "- OOS outcome으로 threshold 재조정 금지",
        "- composite score/rank/filter 생성 금지",
        "- production/order 변경 0",
    ]
    (out/"report.txt").write_text("\n".join(report),encoding="utf-8")
    print("\n".join(report))

if __name__=="__main__":
    main()
