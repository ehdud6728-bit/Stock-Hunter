#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_A_CONTEXT_R2_3_GOOD_PULLBACK_20260925"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_marcap(root, years):
    parts=[]
    for y in sorted(years):
        p=Path(root)/"data"/f"marcap-{y}.parquet"
        if not p.exists(): raise SystemExit(f"MISSING_MARCAP:{p}")
        q=pd.read_parquet(p)
        if "Date" not in q.columns: q=q.reset_index()
        q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
        q["Code"]=q["Code"].map(norm_code)
        parts.append(q)
    return pd.concat(parts,ignore_index=True)

def safe_mean(x):
    x=pd.to_numeric(x,errors="coerce").dropna()
    return float(x.mean()) if len(x) else np.nan

def descriptors(g, sigdate):
    g=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
    hits=g.index[g["Date"].eq(sigdate)]
    if len(hits)!=1:return None
    i=int(hits[0])
    if i<25:return None

    pre20=g.iloc[i-20:i].copy()
    pre10=g.iloc[i-10:i].copy()
    pre5=g.iloc[i-5:i].copy()
    pre3=g.iloc[i-3:i].copy()
    prior10=g.iloc[i-15:i-5].copy()
    sig=g.iloc[i]

    high20=pd.to_numeric(pre20["High"],errors="coerce").max()
    low20=pd.to_numeric(pre20["Low"],errors="coerce").min()
    high10=pd.to_numeric(pre10["High"],errors="coerce").max()
    low10=pd.to_numeric(pre10["Low"],errors="coerce").min()
    sig_close=float(sig["Close"])

    lows5=pd.to_numeric(pre5["Low"],errors="coerce").to_numpy()
    highs5=pd.to_numeric(pre5["High"],errors="coerce").to_numpy()
    closes5=pd.to_numeric(pre5["Close"],errors="coerce").to_numpy()
    opens5=pd.to_numeric(pre5["Open"],errors="coerce").to_numpy()

    higher_low_count=int(np.sum(np.diff(lows5)>0)) if len(lows5)>=2 else 0
    lower_low_count=int(np.sum(np.diff(lows5)<0)) if len(lows5)>=2 else 0

    ranges3=(pd.to_numeric(pre3["High"],errors="coerce")-pd.to_numeric(pre3["Low"],errors="coerce"))
    ranges_prior=(pd.to_numeric(prior10["High"],errors="coerce")-pd.to_numeric(prior10["Low"],errors="coerce"))
    range_contract_3v10=safe_mean(ranges3)/safe_mean(ranges_prior) if safe_mean(ranges_prior)>0 else np.nan

    bodies3=(pd.to_numeric(pre3["Close"],errors="coerce")-pd.to_numeric(pre3["Open"],errors="coerce")).abs()
    bodies_prior=(pd.to_numeric(prior10["Close"],errors="coerce")-pd.to_numeric(prior10["Open"],errors="coerce")).abs()
    body_contract_3v10=safe_mean(bodies3)/safe_mean(bodies_prior) if safe_mean(bodies_prior)>0 else np.nan

    # down/up day participation
    p10=pre10.copy()
    p10["ret"]=pd.to_numeric(p10["Close"],errors="coerce").pct_change()
    down=p10[p10["ret"]<0]
    up=p10[p10["ret"]>0]
    down_amt=safe_mean(down["Amount"])
    up_amt=safe_mean(up["Amount"])
    down_vol=safe_mean(down["Volume"])
    up_vol=safe_mean(up["Volume"])

    # peak -> trough -> signal recovery geometry
    peak_idx=int(pre10.index[pd.to_numeric(pre10["High"],errors="coerce").eq(high10)][-1])
    after_peak=g.loc[peak_idx:i-1].copy()
    trough=float(pd.to_numeric(after_peak["Low"],errors="coerce").min()) if len(after_peak) else np.nan
    pullback_depth=(trough/high10-1)*100 if high10>0 and np.isfinite(trough) else np.nan
    recovery_ratio=((sig_close-trough)/(high10-trough)) if high10>trough and np.isfinite(trough) else np.nan

    # close retention / compression
    pre3_high=float(pd.to_numeric(pre3["High"],errors="coerce").max())
    pre3_low=float(pd.to_numeric(pre3["Low"],errors="coerce").min())
    signal_reclaim_pre3_high=(sig_close/pre3_high-1)*100 if pre3_high>0 else np.nan
    pre3_position=((float(pd.to_numeric(pre3["Close"],errors="coerce").iloc[-1])-pre3_low)/(pre3_high-pre3_low)
                   if pre3_high>pre3_low else np.nan)

    # moving averages computed from preceding closes including signal
    hist=g.iloc[:i+1].copy()
    close=pd.to_numeric(hist["Close"],errors="coerce")
    ma5=float(close.rolling(5).mean().iloc[-1])
    ma10=float(close.rolling(10).mean().iloc[-1])
    ma20=float(close.rolling(20).mean().iloc[-1])

    return {
      "higher_low_count_5d":higher_low_count,
      "lower_low_count_5d":lower_low_count,
      "range_contract_3v10":range_contract_3v10,
      "body_contract_3v10":body_contract_3v10,
      "down_up_amount_ratio_10d":down_amt/up_amt if up_amt and up_amt>0 else np.nan,
      "down_up_volume_ratio_10d":down_vol/up_vol if up_vol and up_vol>0 else np.nan,
      "pullback_depth_pct":pullback_depth,
      "recovery_ratio_to_pre10_high":recovery_ratio,
      "signal_reclaim_pre3_high_pct":signal_reclaim_pre3_high,
      "pre3_position_in_range":pre3_position,
      "signal_vs_pre20_high_pct":(sig_close/high20-1)*100 if high20>0 else np.nan,
      "signal_vs_pre20_low_pct":(sig_close/low20-1)*100 if low20>0 else np.nan,
      "ma5_ma10_gap_pct_r23":(ma5/ma10-1)*100 if ma10>0 else np.nan,
      "ma10_ma20_gap_pct_r23":(ma10/ma20-1)*100 if ma20>0 else np.nan,
    }

def qbucket(s):
    x=pd.to_numeric(s,errors="coerce")
    try:
        q,bins=pd.qcut(x,5,labels=False,retbins=True,duplicates="drop")
        return q+1,bins
    except Exception:
        return pd.Series(np.nan,index=s.index),np.array([])

def complete_bool(s):
    return s.astype(str).str.lower().isin(["true","1"])

def summ(q,label):
    d=q[complete_bool(q["d5_complete"])].copy()
    if d.empty:return {"group":label,"n":len(q),"d5_complete_n":0}
    mfe=pd.to_numeric(d["d5_mfe_pct"],errors="coerce")
    mae=pd.to_numeric(d["d5_mae_pct"],errors="coerce")
    close=pd.to_numeric(d["d5_close_ret_pct"],errors="coerce")
    held=d["odoli_path_r1"].eq("EARLY_WIN_HELD")
    give=d["odoli_path_r1"].eq("EARLY_WIN_GIVEBACK")
    fail=d["odoli_path_r1"].eq("NO_RECOVERY_BY_D5")
    return {
      "group":label,"n":len(q),"d5_complete_n":len(d),
      "d5_plus5_rate_pct":float(mfe.ge(5).mean()*100),
      "d5_positive_close_rate_pct":float(close.gt(0).mean()*100),
      "d5_close_median_pct":float(close.median()),
      "d5_mfe_median_pct":float(mfe.median()),
      "d5_mae_median_pct":float(mae.median()),
      "held_rate_pct":float(held.mean()*100),
      "giveback_rate_pct":float(give.mean()*100),
      "no_recovery_rate_pct":float(fail.mean()*100),
      "held_minus_giveback_pp":float((held.mean()-give.mean())*100),
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--odoli-root",required=True)
    ap.add_argument("--r13-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    od=pd.read_csv(find_one(a.odoli_root,"odoli_all_market_events.csv"),dtype={"code":str},low_memory=False)
    r13=pd.read_csv(find_one(a.r13_root,"shadow_outcome_refined_events.csv"),dtype={"code":str},low_memory=False)
    od["code"]=od["code"].map(norm_code)
    r13["code"]=r13["code"].map(norm_code)
    od["signal_date"]=pd.to_datetime(od["signal_date"]).dt.normalize()
    r13["signal_date"]=pd.to_datetime(r13["signal_date"]).dt.normalize()

    A=r13[r13["pattern"].eq("A")].copy()
    A["key"]=list(zip(A["signal_date"],A["code"]))
    okeys=set(zip(od["signal_date"],od["code"]))
    A["odoli_overlap"]=A["key"].isin(okeys)

    mar=load_marcap(a.marcap_root,{2023,2024,2025,2026})
    mar=mar[mar["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()
    bycode={c:g.copy() for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    for k,r in A.reset_index(drop=True).iterrows():
        g=bycode.get(r["code"])
        d=descriptors(g,r["signal_date"]) if g is not None else None
        rec={"_row":k}
        if d:rec.update(d)
        rows.append(rec)
    desc=pd.DataFrame(rows).set_index("_row")
    A2=A.reset_index(drop=True).join(desc)

    # Bring ODOLI path labels/outcomes onto the 21 overlap rows.
    od_key=od.set_index(["signal_date","code"])
    for c in ["odoli_path_r1","d5_complete","d5_mfe_pct","d5_mae_pct","d5_close_ret_pct"]:
        A2[c]=[
            od_key.at[(r["signal_date"],r["code"]),c] if (r["signal_date"],r["code"]) in od_key.index else np.nan
            for _,r in A2.iterrows()
        ]

    A2.to_csv(out/"a97_good_pullback_descriptors.csv",index=False,encoding="utf-8-sig")
    A2[A2["odoli_overlap"]].to_csv(out/"a21_odoli_good_pullback_descriptors.csv",index=False,encoding="utf-8-sig")

    feats=[
      "higher_low_count_5d","lower_low_count_5d","range_contract_3v10","body_contract_3v10",
      "down_up_amount_ratio_10d","down_up_volume_ratio_10d","pullback_depth_pct",
      "recovery_ratio_to_pre10_high","signal_reclaim_pre3_high_pct","pre3_position_in_range",
      "signal_vs_pre20_high_pct","signal_vs_pre20_low_pct",
      "ma5_ma10_gap_pct_r23","ma10_ma20_gap_pct_r23"
    ]

    # A-population outcome-blind quintile bins only.
    bins={}
    qrows=[]
    for c in feats:
        q,b=qbucket(A2[c]); A2[c+"_q"]=q
        bins[c]=[float(x) for x in b] if len(b) else []
        # Only evaluate outcome response where ODOLI outcomes exist (21 overlap).
        ao=A2[A2["odoli_overlap"]].copy()
        for qn,g in ao.groupby(c+"_q",dropna=True):
            rr=summ(g,f"{c}:Q{int(qn)}")
            rr["feature"]=c; rr["quintile"]=int(qn)
            qrows.append(rr)
    pd.DataFrame(qrows).to_csv(out/"a_context_quintile_response_on_21.csv",index=False,encoding="utf-8-sig")
    (out/"a_context_bins.json").write_text(json.dumps(bins,ensure_ascii=False,indent=2),encoding="utf-8")

    # Structural, non-fitted definitions.
    ao=A2[A2["odoli_overlap"]].copy()
    def mask(name):
        if name=="ALL_A_ODOLI": return pd.Series(True,index=ao.index)
        if name=="HIGHER_LOW_3PLUS": return pd.to_numeric(ao["higher_low_count_5d"],errors="coerce").ge(3)
        if name=="LOWER_LOW_1MAX": return pd.to_numeric(ao["lower_low_count_5d"],errors="coerce").le(1)
        if name=="MA5_GT_MA10": return pd.to_numeric(ao["ma5_ma10_gap_pct_r23"],errors="coerce").gt(0)
        if name=="MA10_GT_MA20": return pd.to_numeric(ao["ma10_ma20_gap_pct_r23"],errors="coerce").gt(0)
        if name=="RANGE_CONTRACT_Q12": return pd.to_numeric(ao["range_contract_3v10_q"],errors="coerce").le(2)
        if name=="DOWN_AMOUNT_WEAK_Q12": return pd.to_numeric(ao["down_up_amount_ratio_10d_q"],errors="coerce").le(2)
        if name=="RECOVERY_Q45": return pd.to_numeric(ao["recovery_ratio_to_pre10_high_q"],errors="coerce").ge(4)
        if name=="STRUCTURE_CORE":
            return mask("LOWER_LOW_1MAX") & mask("MA5_GT_MA10") & mask("RANGE_CONTRACT_Q12")
        if name=="STRUCTURE_CORE_PLUS_PARTICIPATION":
            return mask("STRUCTURE_CORE") & mask("DOWN_AMOUNT_WEAK_Q12")
        raise KeyError(name)

    names=["ALL_A_ODOLI","HIGHER_LOW_3PLUS","LOWER_LOW_1MAX","MA5_GT_MA10","MA10_GT_MA20",
           "RANGE_CONTRACT_Q12","DOWN_AMOUNT_WEAK_Q12","RECOVERY_Q45",
           "STRUCTURE_CORE","STRUCTURE_CORE_PLUS_PARTICIPATION"]
    screen=pd.DataFrame([summ(ao[mask(n)].copy(),n) for n in names])
    screen.to_csv(out/"a_context_structural_screen.csv",index=False,encoding="utf-8-sig")

    # descriptive medians by resolved path
    med=[]
    resolved=ao[ao["odoli_path_r1"].isin(["EARLY_WIN_HELD","EARLY_WIN_GIVEBACK","NO_RECOVERY_BY_D5"])]
    for p,g in resolved.groupby("odoli_path_r1"):
        rr={"path":p,"n":len(g)}
        for c in feats:
            rr[c+"_median"]=float(pd.to_numeric(g[c],errors="coerce").median())
        med.append(rr)
    pd.DataFrame(med).to_csv(out/"a21_resolved_path_structure_medians.csv",index=False,encoding="utf-8-sig")

    report=[
      "# A-context good pullback R2.3",
      "",
      "- A population: 97 frozen R1.3 A events.",
      "- A×ODOLI exact overlap: 21.",
      "- 21-event outcomes are used only for descriptive response, never for fitting cutpoints.",
      "- Quintile boundaries are computed from all 97 A events, outcome-blind.",
      "- Structural cutpoints are non-fitted (e.g. MA5>MA10, <=1 lower-low step).",
      "- Same historical sample: discovery only, not OOS.",
      "",
      "## Structural screen on A×ODOLI overlap",
      "```",
      screen.to_string(index=False),
      "```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")
    meta={
      "revision":REV,"research_only":True,"production_logic_changed":False,
      "same_sample_threshold_tuning":False,"a_population_n":len(A2),
      "a_odoli_n":int(A2["odoli_overlap"].sum()),
      "descriptor_complete_n":int(pd.to_numeric(A2["range_contract_3v10"],errors="coerce").notna().sum()),
      "true_oos_validation":False
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
