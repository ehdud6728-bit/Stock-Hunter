#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math, re
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_A_PATH_R2_1_DISCOVERY_EXPANSION_20260925"
RESOLVED_PATHS={"EARLY_WIN_HELD","EARLY_WIN_GIVEBACK","NO_RECOVERY_BY_D5"}

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_marcap_years(root, years):
    parts=[]
    diag=[]
    for y in sorted(years):
        p=Path(root)/"data"/f"marcap-{y}.parquet"
        if not p.exists():
            diag.append({"year":y,"exists":False,"rows":0})
            continue
        q=pd.read_parquet(p)
        if "Date" not in q.columns: q=q.reset_index()
        if "Date" not in q.columns:
            diag.append({"year":y,"exists":True,"rows":len(q),"error":"Date missing"})
            continue
        q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
        q["Code"]=q["Code"].map(norm_code)
        parts.append(q)
        diag.append({"year":y,"exists":True,"rows":len(q),"error":""})
    if not parts: raise SystemExit("NO_MARCAP_DATA")
    return pd.concat(parts,ignore_index=True),pd.DataFrame(diag)

def event_window(g, signal_date, pre=20, post=10):
    g=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
    hit=g.index[g["Date"].eq(pd.Timestamp(signal_date).normalize())]
    if len(hit)!=1: return None
    i=int(hit[0])
    lo=max(0,i-pre); hi=min(len(g),i+post+1)
    w=g.iloc[lo:hi].copy()
    w["event_day"]=np.arange(lo-i,hi-i)
    c=pd.to_numeric(w["Close"],errors="coerce")
    w["ma5"]=c.rolling(5,min_periods=1).mean()
    w["ma10"]=c.rolling(10,min_periods=1).mean()
    sig=float(g.loc[i,"Close"])
    for c0 in ["Open","High","Low","Close","ma5","ma10"]:
        w[c0+"_norm"]=pd.to_numeric(w[c0],errors="coerce")/sig*100
    return w

def make_chart(w, title, out_png):
    import matplotlib.pyplot as plt
    fig=plt.figure(figsize=(10,5.5))
    ax=fig.add_subplot(111)
    ax.plot(w["event_day"],w["Close_norm"],label="Close")
    ax.plot(w["event_day"],w["ma5_norm"],label="MA5")
    ax.plot(w["event_day"],w["ma10_norm"],label="MA10")
    ax.axvline(0,linestyle="--",linewidth=1)
    ax.axhline(100,linestyle=":",linewidth=1)
    ax.set_title(title)
    ax.set_xlabel("Trading day from signal")
    ax.set_ylabel("Signal close = 100")
    ax.grid(True,alpha=.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png,dpi=150)
    plt.close(fig)

def qlabels(s):
    x=pd.to_numeric(s,errors="coerce")
    try:
        q,bins=pd.qcut(x,5,labels=False,retbins=True,duplicates="drop")
        return q+1,bins
    except Exception:
        return pd.Series(np.nan,index=s.index),np.array([])

def outcome_summary(q,label):
    d5c=q[pd.to_numeric(q["d5_complete"],errors="coerce").fillna(0).astype(bool)].copy()
    if len(d5c):
        hit=pd.to_numeric(d5c["d5_mfe_pct"],errors="coerce").ge(5)
        close=pd.to_numeric(d5c["d5_close_ret_pct"],errors="coerce")
        held=d5c["odoli_path_r1"].eq("EARLY_WIN_HELD")
        give=d5c["odoli_path_r1"].eq("EARLY_WIN_GIVEBACK")
        fail=d5c["odoli_path_r1"].eq("NO_RECOVERY_BY_D5")
        return {
            "group":label,"n":len(q),"d5_complete_n":len(d5c),
            "d5_plus5_rate_pct":float(hit.mean()*100),
            "d5_positive_close_rate_pct":float(close.gt(0).mean()*100),
            "d5_close_median_pct":float(close.median()),
            "d5_mfe_median_pct":float(pd.to_numeric(d5c["d5_mfe_pct"],errors="coerce").median()),
            "d5_mae_median_pct":float(pd.to_numeric(d5c["d5_mae_pct"],errors="coerce").median()),
            "held_rate_pct":float(held.mean()*100),
            "giveback_rate_pct":float(give.mean()*100),
            "no_recovery_rate_pct":float(fail.mean()*100),
            "held_minus_giveback_pp":float((held.mean()-give.mean())*100),
        }
    return {"group":label,"n":len(q),"d5_complete_n":0}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--odoli-root",required=True)
    ap.add_argument("--a-audit-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    charts=out/"event_charts"; paths=out/"event_paths"
    charts.mkdir(exist_ok=True); paths.mkdir(exist_ok=True)

    allod=pd.read_csv(find_one(a.odoli_root,"odoli_all_market_events.csv"),dtype={"code":str},low_memory=False)
    a21=pd.read_csv(find_one(a.a_audit_root,"a_odoli_21_events.csv"),dtype={"code":str},low_memory=False)
    for q in (allod,a21):
        q["code"]=q["code"].map(norm_code)
        q["signal_date"]=pd.to_datetime(q["signal_date"]).dt.strftime("%Y-%m-%d")

    # Data needed for 21 event paths only.
    sdates=pd.to_datetime(a21["signal_date"])
    years=set(sdates.dt.year.tolist())
    years |= {min(years)-1}  # safe warmup across year boundary
    marcap,mdiag=load_marcap_years(a.marcap_root,years)
    mdiag.to_csv(out/"marcap_path_source_diagnostics.csv",index=False,encoding="utf-8-sig")
    marcap=marcap[marcap["Code"].isin(set(a21["code"]))].copy()

    index_rows=[]
    for _,ev in a21.sort_values(["odoli_path_r1","signal_date","code"]).iterrows():
        code=ev["code"]; d=ev["signal_date"]; name=str(ev.get("name",""))
        g=marcap[marcap["Code"].eq(code)].copy()
        w=event_window(g,d,20,10)
        safe=re.sub(r"[^0-9A-Za-z가-힣_-]+","_",name)[:30]
        stem=f"{ev['odoli_path_r1']}__{d}__{code}__{safe}"
        if w is None:
            index_rows.append({"signal_date":d,"code":code,"name":name,"path":ev["odoli_path_r1"],
                               "chart":"","csv":"","status":"SIGNAL_DATE_NOT_FOUND"})
            continue
        csvp=paths/f"{stem}.csv"
        pngp=charts/f"{stem}.png"
        w.to_csv(csvp,index=False,encoding="utf-8-sig")
        make_chart(w,f"{name} {code} | {d} | {ev['odoli_path_r1']}",pngp)
        index_rows.append({"signal_date":d,"code":code,"name":name,"path":ev["odoli_path_r1"],
                           "chart":str(pngp.relative_to(out)),"csv":str(csvp.relative_to(out)),"status":"OK"})
    idx=pd.DataFrame(index_rows)
    idx.to_csv(out/"event_chart_index.csv",index=False,encoding="utf-8-sig")

    # Resolved 20-event discovery comparison. Pending retained separately.
    resolved=a21[a21["odoli_path_r1"].isin(RESOLVED_PATHS)].copy()
    feats=["ma5_slope_1d_pct","ma5_ma10_gap_pct","vol20_ratio","amount20_ratio",
           "close_loc_pct","upper_wick_pct","gap_pct","ret5_pct"]
    rows=[]
    for fam,q in resolved.groupby("odoli_path_r1"):
        r={"path":fam,"n":len(q)}
        for c in feats:
            x=pd.to_numeric(q[c],errors="coerce")
            r[c+"_median"]=float(x.median())
            r[c+"_mean"]=float(x.mean())
        rows.append(r)
    pd.DataFrame(rows).to_csv(out/"resolved20_feature_comparison.csv",index=False,encoding="utf-8-sig")

    # Feature-distribution-only global quintiles. These are NOT fitted to outcomes.
    bins={}
    for c in ["ma5_slope_1d_pct","amount20_ratio","vol20_ratio","ma5_ma10_gap_pct","close_loc_pct","upper_wick_pct","ret5_pct"]:
        lab,b=qlabels(allod[c])
        allod[c+"_q"]=lab
        bins[c]=[float(v) for v in b] if len(b) else []
        if len(b):
            a21[c+"_q"]=pd.cut(pd.to_numeric(a21[c],errors="coerce"),bins=b,labels=False,include_lowest=True)+1

    # Discovery hypotheses: all thresholds are structural (zero) or feature-distribution quintiles,
    # never optimized on 21-event outcomes.
    def bmask(name):
        if name=="ALL": return pd.Series(True,index=allod.index)
        if name=="MA5_GT_MA10": return pd.to_numeric(allod["ma5_ma10_gap_pct"],errors="coerce").gt(0)
        if name=="SLOPE_Q45": return pd.to_numeric(allod["ma5_slope_1d_pct_q"],errors="coerce").ge(4)
        if name=="AMOUNT_Q45": return pd.to_numeric(allod["amount20_ratio_q"],errors="coerce").ge(4)
        if name=="VOL_Q45": return pd.to_numeric(allod["vol20_ratio_q"],errors="coerce").ge(4)
        if name=="CLOSELOC_Q5": return pd.to_numeric(allod["close_loc_pct_q"],errors="coerce").eq(5)
        if name=="MA5_GT_MA10__SLOPE_Q45": return bmask("MA5_GT_MA10") & bmask("SLOPE_Q45")
        if name=="MA5_GT_MA10__AMOUNT_Q45": return bmask("MA5_GT_MA10") & bmask("AMOUNT_Q45")
        if name=="SLOPE_Q45__AMOUNT_Q45": return bmask("SLOPE_Q45") & bmask("AMOUNT_Q45")
        if name=="TREND_PARTICIPATION": return bmask("MA5_GT_MA10") & bmask("SLOPE_Q45") & bmask("AMOUNT_Q45")
        raise KeyError(name)

    hnames=["ALL","MA5_GT_MA10","SLOPE_Q45","AMOUNT_Q45","VOL_Q45","CLOSELOC_Q5",
            "MA5_GT_MA10__SLOPE_Q45","MA5_GT_MA10__AMOUNT_Q45",
            "SLOPE_Q45__AMOUNT_Q45","TREND_PARTICIPATION"]
    hrows=[]
    for h in hnames:
        hrows.append(outcome_summary(allod[bmask(h)].copy(),h))
    hs=pd.DataFrame(hrows)
    hs.to_csv(out/"whole_market_hypothesis_screen.csv",index=False,encoding="utf-8-sig")

    # Full quintile response surfaces, so no cherry-picking of only a preferred bucket.
    qr=[]
    for c in ["ma5_slope_1d_pct","amount20_ratio","vol20_ratio","ma5_ma10_gap_pct","close_loc_pct","upper_wick_pct","ret5_pct"]:
        qc=c+"_q"
        for qn,g in allod.groupby(qc,dropna=True):
            r=outcome_summary(g,f"{c}:Q{int(qn)}")
            r["feature"]=c; r["quintile"]=int(qn)
            qr.append(r)
    pd.DataFrame(qr).to_csv(out/"whole_market_quintile_response.csv",index=False,encoding="utf-8-sig")

    a21.to_csv(out/"a_odoli_21_with_global_buckets.csv",index=False,encoding="utf-8-sig")
    (out/"global_feature_bins.json").write_text(json.dumps(bins,ensure_ascii=False,indent=2),encoding="utf-8")

    # Human-readable report.
    fcmp=pd.DataFrame(rows)
    lines=[
        "# A×ODOLI 21-event path review → 65,054-event expansion",
        "",
        "## Authority / safeguards",
        "- Frozen ODOLI PIT population: run 36129293163 (65,054 events).",
        "- Frozen A×ODOLI overlap audit: run 36137759740 (21 events).",
        "- Resolved discovery cases: HELD 7 / GIVEBACK 10 / NO_RECOVERY 3. Pending 1 retained but excluded from resolved comparison.",
        "- No numeric threshold is optimized from the 21 cases.",
        "- Whole-market buckets are distribution-only quintiles or structural zero-crossing (MA5 vs MA10).",
        "- This is discovery/expansion on the same historical period, not true OOS validation.",
        "",
        "## Resolved 20 feature comparison",
        "```",
        fcmp.to_string(index=False),
        "```",
        "",
        "## Whole-market hypothesis screen",
        "```",
        hs.to_string(index=False),
        "```",
        "",
        "## Intraday limitation",
        "- GAP_FAIL_BOX_RESTART cannot be established from daily marcap bars.",
        "- A separate intraday dataset/lane is required before that hypothesis can be tested.",
        "",
        "## Generated path material",
        f"- Individual event charts: {int((idx.status=='OK').sum())}/21",
        "- Each chart covers approximately D-20 to D+10 trading days and normalizes signal close to 100.",
        "- Matching raw path CSVs are included for manual inspection.",
    ]
    (out/"REPORT.md").write_text("\n".join(lines),encoding="utf-8")

    meta={
        "revision":REV,"research_only":True,"production_logic_changed":False,
        "same_sample_threshold_tuning":False,
        "odoli_population_n":len(allod),"a_odoli_n":len(a21),
        "resolved_n":len(resolved),"held_n":int((resolved.odoli_path_r1=="EARLY_WIN_HELD").sum()),
        "giveback_n":int((resolved.odoli_path_r1=="EARLY_WIN_GIVEBACK").sum()),
        "failure_n":int((resolved.odoli_path_r1=="NO_RECOVERY_BY_D5").sum()),
        "pending_n":int((a21.odoli_path_r1=="PENDING_D5").sum()),
        "event_charts_ok":int((idx.status=="OK").sum()),
        "true_oos_validation":False,
        "intraday_gap_fail_box_restart_tested":False,
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
