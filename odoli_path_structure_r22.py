#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_PATH_STRUCTURE_R2_2_WHOLE_MARKET_20260925"

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

def event_descriptors(g, sigdate):
    g=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
    hits=g.index[g["Date"].eq(sigdate)]
    if len(hits)!=1:return None
    i=int(hits[0])
    if i<20:return None
    pre20=g.iloc[i-20:i].copy()
    pre10=g.iloc[i-10:i].copy()
    pre5=g.iloc[i-5:i].copy()
    pre3=g.iloc[i-3:i].copy()
    sig=g.iloc[i]
    early=pre20.iloc[:15]  # D-20..D-6
    if early.empty or pre10.empty:return None

    early_low=pd.to_numeric(early["Low"],errors="coerce").min()
    pre10_high=pd.to_numeric(pre10["High"],errors="coerce").max()
    peak_rows=pre10.index[pd.to_numeric(pre10["High"],errors="coerce").eq(pre10_high)]
    peak_idx=int(peak_rows[-1])
    after_peak=g.loc[peak_idx:i-1].copy()

    a20=pd.to_numeric(pre20["Amount"],errors="coerce")
    v20=pd.to_numeric(pre20["Volume"],errors="coerce")
    a3=pd.to_numeric(pre3["Amount"],errors="coerce")
    v3=pd.to_numeric(pre3["Volume"],errors="coerce")
    a5=pd.to_numeric(pre5["Amount"],errors="coerce")
    v5=pd.to_numeric(pre5["Volume"],errors="coerce")
    close3=pd.to_numeric(pre3["Close"],errors="coerce")

    sigclose=float(sig["Close"])
    d={
      "first_wave_proxy_pct":(pre10_high/early_low-1)*100 if early_low>0 else np.nan,
      "signal_vs_pre10_high_pct":(sigclose/pre10_high-1)*100 if pre10_high>0 else np.nan,
      "pullback_after_pre10_peak_pct":
          (pd.to_numeric(after_peak["Low"],errors="coerce").min()/pre10_high-1)*100 if len(after_peak) and pre10_high>0 else np.nan,
      "days_since_pre10_high":float(i-peak_idx),
      "pre5_range_pct":(pd.to_numeric(pre5["High"],errors="coerce").max()/pd.to_numeric(pre5["Low"],errors="coerce").min()-1)*100,
      "pre3_range_pct":(pd.to_numeric(pre3["High"],errors="coerce").max()/pd.to_numeric(pre3["Low"],errors="coerce").min()-1)*100,
      "pre3_close_change_pct":(close3.iloc[-1]/close3.iloc[0]-1)*100 if len(close3)>=2 and close3.iloc[0]>0 else np.nan,
      "signal_vs_pre20_high_pct":(sigclose/pd.to_numeric(pre20["High"],errors="coerce").max()-1)*100,
      "signal_vs_pre20_low_pct":(sigclose/pd.to_numeric(pre20["Low"],errors="coerce").min()-1)*100,
      "pre3_amount_vs_pre20":a3.mean()/a20.mean() if a20.mean()>0 else np.nan,
      "pre5_amount_vs_pre20":a5.mean()/a20.mean() if a20.mean()>0 else np.nan,
      "pre3_volume_vs_pre20":v3.mean()/v20.mean() if v20.mean()>0 else np.nan,
      "pre5_volume_vs_pre20":v5.mean()/v20.mean() if v20.mean()>0 else np.nan,
    }
    return d

def qbucket(s):
    x=pd.to_numeric(s,errors="coerce")
    try:
        q,bins=pd.qcut(x,5,labels=False,retbins=True,duplicates="drop")
        return q+1,bins
    except Exception:
        return pd.Series(np.nan,index=s.index),np.array([])

def summarize(q,label):
    d=q[q["d5_complete"].astype(str).str.lower().isin(["true","1"])].copy()
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
      "held_minus_giveback_pp":float((held.mean()-give.mean())*100)
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--odoli-root",required=True)
    ap.add_argument("--r21-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    od=pd.read_csv(find_one(a.odoli_root,"odoli_all_market_events.csv"),dtype={"code":str},low_memory=False)
    od["code"]=od["code"].map(norm_code)
    od["signal_date"]=pd.to_datetime(od["signal_date"]).dt.normalize()

    # Load 2023-2026 to guarantee D-20 warmup for early-2024 events.
    m=load_marcap(a.marcap_root,{2023,2024,2025,2026})
    m=m[m["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()

    bycode={c:g.copy() for c,g in m.groupby("Code",sort=False)}
    desc=[]
    for k,row in od.iterrows():
        g=bycode.get(row["code"])
        d=event_descriptors(g,row["signal_date"]) if g is not None else None
        rec={"_idx":k}
        if d:rec.update(d)
        desc.append(rec)
        if (k+1)%5000==0: print("PATH_DESC",k+1,"/",len(od))
    ds=pd.DataFrame(desc).set_index("_idx")
    od2=od.join(ds)
    od2.to_csv(out/"odoli_65054_with_path_descriptors.csv",index=False,encoding="utf-8-sig")

    features=[
      "first_wave_proxy_pct","signal_vs_pre10_high_pct","pullback_after_pre10_peak_pct",
      "days_since_pre10_high","pre5_range_pct","pre3_range_pct","pre3_close_change_pct",
      "signal_vs_pre20_high_pct","signal_vs_pre20_low_pct",
      "pre3_amount_vs_pre20","pre5_amount_vs_pre20",
      "pre3_volume_vs_pre20","pre5_volume_vs_pre20"
    ]

    # Full response for every global quintile; quintile boundaries depend only on feature distribution.
    bins={}
    rows=[]
    for c in features:
        q,b=qbucket(od2[c]); od2[c+"_q"]=q
        bins[c]=[float(x) for x in b] if len(b) else []
        for qn,g in od2.groupby(c+"_q",dropna=True):
            r=summarize(g,f"{c}:Q{int(qn)}")
            r["feature"]=c;r["quintile"]=int(qn)
            rows.append(r)
    pd.DataFrame(rows).to_csv(out/"path_descriptor_quintile_response.csv",index=False,encoding="utf-8-sig")
    (out/"path_descriptor_bins.json").write_text(json.dumps(bins,ensure_ascii=False,indent=2),encoding="utf-8")

    # Predeclared structural hypotheses from R2.1 qualitative review:
    # strong first wave + dry-up before restart. No cutpoint learned from the 21 cases:
    # use global feature quintiles only.
    def mask(name):
        if name=="ALL":return pd.Series(True,index=od2.index)
        if name=="FIRST_WAVE_Q45":return pd.to_numeric(od2["first_wave_proxy_pct_q"],errors="coerce").ge(4)
        if name=="DRY_AMOUNT_Q12":return pd.to_numeric(od2["pre3_amount_vs_pre20_q"],errors="coerce").le(2)
        if name=="DRY_VOLUME_Q12":return pd.to_numeric(od2["pre3_volume_vs_pre20_q"],errors="coerce").le(2)
        if name=="PULLBACK_DEEP_Q12":
            # more negative values live in the lower quintiles
            return pd.to_numeric(od2["pullback_after_pre10_peak_pct_q"],errors="coerce").le(2)
        if name=="FIRST_WAVE_Q45__DRY_AMOUNT_Q12":
            return mask("FIRST_WAVE_Q45") & mask("DRY_AMOUNT_Q12")
        if name=="FIRST_WAVE_Q45__DRY_VOLUME_Q12":
            return mask("FIRST_WAVE_Q45") & mask("DRY_VOLUME_Q12")
        if name=="FIRST_WAVE_Q45__PULLBACK_DEEP_Q12__DRY_AMOUNT_Q12":
            return mask("FIRST_WAVE_Q45") & mask("PULLBACK_DEEP_Q12") & mask("DRY_AMOUNT_Q12")
        raise KeyError(name)

    names=[
      "ALL","FIRST_WAVE_Q45","DRY_AMOUNT_Q12","DRY_VOLUME_Q12","PULLBACK_DEEP_Q12",
      "FIRST_WAVE_Q45__DRY_AMOUNT_Q12",
      "FIRST_WAVE_Q45__DRY_VOLUME_Q12",
      "FIRST_WAVE_Q45__PULLBACK_DEEP_Q12__DRY_AMOUNT_Q12"
    ]
    screen=pd.DataFrame([summarize(od2[mask(n)].copy(),n) for n in names])
    screen.to_csv(out/"path_structure_hypothesis_screen.csv",index=False,encoding="utf-8-sig")

    # A×ODOLI 21 subset with new descriptors for traceability.
    a21=od2[od2["r13_pattern"].eq("A")].copy()
    a21.to_csv(out/"a_odoli_21_path_descriptors.csv",index=False,encoding="utf-8-sig")

    report=[
      "# ODOLI R2.2 path-structure whole-market expansion",
      "",
      "- Frozen ODOLI PIT authority: run 36129293163.",
      "- R2.1 qualitative source: run 36140416395.",
      "- 65,054 events receive D-20..D-1 structural descriptors from PIT marcap.",
      "- No cutpoint is learned from the 21 A×ODOLI outcomes.",
      "- Hypothesis screen uses only global distribution quintiles.",
      "- Full Q1..Q5 response is emitted for every descriptor.",
      "- Same historical period: discovery/expansion, NOT true OOS.",
      "",
      "## Predeclared whole-market path hypotheses",
      "```",
      screen.to_string(index=False),
      "```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")
    meta={
      "revision":REV,"research_only":True,"production_logic_changed":False,
      "same_sample_threshold_tuning":False,"odoli_n":len(od2),
      "path_descriptor_complete_n":int(pd.to_numeric(od2["first_wave_proxy_pct"],errors="coerce").notna().sum()),
      "true_oos_validation":False
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
