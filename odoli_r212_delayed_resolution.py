#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_12_DELAYED_RESOLUTION_20260926"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_marcap(root):
    p=Path(root)/"data"/"marcap-2026.parquet"
    if not p.exists(): raise SystemExit(f"MISSING:{p}")
    q=pd.read_parquet(p)
    if "Date" not in q.columns: q=q.reset_index()
    q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
    q["Code"]=q["Code"].map(norm_code)
    q=q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()
    return q

def ichimoku(df):
    h=pd.to_numeric(df["High"],errors="coerce")
    l=pd.to_numeric(df["Low"],errors="coerce")
    tenkan=(h.rolling(9).max()+l.rolling(9).min())/2
    kijun=(h.rolling(26).max()+l.rolling(26).min())/2
    span_a=((tenkan+kijun)/2).shift(26)
    span_b=((h.rolling(52).max()+l.rolling(52).min())/2).shift(26)
    out=df.copy()
    out["tenkan"]=tenkan
    out["kijun"]=kijun
    out["span_a"]=span_a
    out["span_b"]=span_b
    out["cloud_top"]=pd.concat([span_a,span_b],axis=1).max(axis=1)
    out["cloud_bottom"]=pd.concat([span_a,span_b],axis=1).min(axis=1)
    return out

def cloud_state(row):
    c=float(row["Close"])
    top=row.get("cloud_top",np.nan); bot=row.get("cloud_bottom",np.nan)
    if not np.isfinite(top) or not np.isfinite(bot): return "NO_CLOUD"
    if c>top: return "ABOVE_CLOUD"
    if c>=bot: return "IN_CLOUD"
    return "BELOW_CLOUD"

def vp_resistance_proxy(hist, current_close, bins=24, lookback=120):
    q=hist.tail(lookback).copy()
    if q.empty: return {}
    tp=(pd.to_numeric(q["High"],errors="coerce")+pd.to_numeric(q["Low"],errors="coerce")+pd.to_numeric(q["Close"],errors="coerce"))/3
    vol=pd.to_numeric(q["Volume"],errors="coerce").fillna(0)
    mn,mx=float(tp.min()),float(tp.max())
    if not np.isfinite(mn) or not np.isfinite(mx) or mx<=mn: return {}
    edges=np.linspace(mn,mx,bins+1)
    idx=np.clip(np.digitize(tp,edges)-1,0,bins-1)
    vv=np.zeros(bins)
    for i,v in zip(idx,vol): vv[int(i)]+=float(v)
    mids=(edges[:-1]+edges[1:])/2
    above=np.where(mids>current_close)[0]
    total=float(vv.sum())
    above_share=float(vv[above].sum()/total*100) if total>0 and len(above) else 0.0
    if len(above):
        j=above[np.argmax(vv[above])]
        node=float(mids[j])
        dist=(node/current_close-1)*100
        node_share=float(vv[j]/total*100) if total>0 else np.nan
    else:
        node=np.nan; dist=np.nan; node_share=0.0
    return {
        "vp_above_volume_share_pct":above_share,
        "vp_strongest_overhead_node":node,
        "vp_strongest_overhead_node_dist_pct":dist,
        "vp_strongest_overhead_node_share_pct":node_share
    }

def first_touch(path, target=10):
    q=path[path["high_ret_pct"]>=target]
    return int(q.iloc[0]["day"]) if len(q) else None

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r211-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    audit=pd.read_csv(find_one(a.r211_root,"r211_accumulation_x_characteristics.csv"),dtype={"code":str},low_memory=False)
    audit["code"]=audit["code"].map(norm_code)
    audit["signal_date"]=pd.to_datetime(audit["signal_date"],errors="coerce").dt.normalize()

    mar=load_marcap(a.marcap_root)
    bycode={c:ichimoku(g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True))
            for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    paths=[]
    for _,r in audit.iterrows():
        g=bycode.get(r["code"])
        if g is None: continue
        hit=g.index[g["Date"].eq(r["signal_date"])]
        if len(hit)!=1: continue
        i=int(hit[0]); sig=float(g.loc[i,"Close"])

        rec=r.to_dict()
        rec["signal_close"]=sig

        # path D+1..D+20
        pp=[]
        for d in range(1,21):
            if i+d>=len(g): break
            rr=g.loc[i+d]
            pp.append({
                "day":d,"date":rr["Date"],
                "high_ret_pct":(float(rr["High"])/sig-1)*100,
                "low_ret_pct":(float(rr["Low"])/sig-1)*100,
                "close_ret_pct":(float(rr["Close"])/sig-1)*100,
                "cloud_state":cloud_state(rr),
                "close":float(rr["Close"])
            })
        p=pd.DataFrame(pp)
        if p.empty: continue
        ft10=first_touch(p,10)
        rec["first_plus10_day_20"]=ft10 if ft10 is not None else np.nan
        rec["d10_touch10"]=bool((p[p["day"]<=10]["high_ret_pct"]>=10).any())
        rec["d15_touch10"]=bool((p[p["day"]<=15]["high_ret_pct"]>=10).any())
        rec["d20_touch10"]=bool((p[p["day"]<=20]["high_ret_pct"]>=10).any())
        rec["d20_complete"]=len(p)>=20

        # D5 state
        d5=p[p["day"]<=5]
        if len(d5)>=5:
            row5=g.loc[i+5]
            rec["d5_cloud_state"]=cloud_state(row5)
            rec["d5_close_ret_pct_refresh"]=float(d5.iloc[-1]["close_ret_pct"])
            rec["d5_mfe_pct_refresh"]=float(d5["high_ret_pct"].max())
            rec["d5_mae_pct_refresh"]=float(d5["low_ret_pct"].min())
            vp=vp_resistance_proxy(g.iloc[:i+6],float(row5["Close"]))
            rec.update(vp)

        # Accumulation-level hold / second pullback
        acc_low=pd.to_numeric(pd.Series([r.get("best_accum_low")]),errors="coerce").iloc[0]
        acc_high=pd.to_numeric(pd.Series([r.get("best_accum_high")]),errors="coerce").iloc[0]
        if np.isfinite(acc_low) and np.isfinite(acc_high):
            acc_mid=(acc_low+acc_high)/2
            rec["accum_mid"]=acc_mid
            rec["d5_holds_accum_low"]=bool(float(g.loc[min(i+5,len(g)-1),"Close"])>=acc_low)
            rec["d5_holds_accum_mid"]=bool(float(g.loc[min(i+5,len(g)-1),"Close"])>=acc_mid)
            # post-D5 second pullback then recovery to +10 by D20
            q6=p[(p["day"]>=6)&(p["day"]<=20)]
            rec["post_d5_min_ret_pct"]=float(q6["low_ret_pct"].min()) if len(q6) else np.nan
            rec["post_d5_recovered_plus10"]=bool((q6["high_ret_pct"]>=10).any()) if len(q6) else False

        # Resolution taxonomy: descriptive, not predictive.
        d5success=bool((p[p["day"]<=5]["high_ret_pct"]>=10).any())
        if d5success:
            cls="EARLY_SUCCESS_D5"
        elif ft10 is not None and ft10<=10:
            cls="DELAYED_SUCCESS_D6_10"
        elif ft10 is not None and ft10<=20:
            cls="DELAYED_SUCCESS_D11_20"
        else:
            cs=rec.get("d5_cloud_state","NO_CLOUD")
            holdmid=bool(rec.get("d5_holds_accum_mid",False))
            holdlow=bool(rec.get("d5_holds_accum_low",False))
            overhead=rec.get("vp_above_volume_share_pct",np.nan)
            if cs in ("ABOVE_CLOUD","IN_CLOUD") and holdmid:
                cls="UNRESOLVED_SUPPORTED"
            elif holdlow and np.isfinite(overhead) and overhead>=35:
                cls="UNRESOLVED_OVERHEAD_SUPPLY"
            elif holdlow:
                cls="UNRESOLVED_SECOND_PULLBACK"
            else:
                cls="STRUCTURAL_FAILURE"
        rec["resolution_class"]=cls

        for x in pp:
            x.update({"signal_date":r["signal_date"],"code":r["code"],"name":r.get("name","")})
            paths.append(x)
        rows.append(rec)

    z=pd.DataFrame(rows)
    pd.DataFrame(paths).to_csv(out/"r212_d1_d20_paths.csv",index=False,encoding="utf-8-sig")
    z.to_csv(out/"r212_delayed_resolution_events.csv",index=False,encoding="utf-8-sig")

    # Focus on original D5 failures
    orig_fail=z[z["outcome10"].eq("FAIL_NO_10P_D5")].copy()
    cls=(orig_fail.groupby("resolution_class").size().reset_index(name="n")
         .sort_values("n",ascending=False))
    cls.to_csv(out/"r212_original_fail_resolution_summary.csv",index=False,encoding="utf-8-sig")

    # Strong accumulation subset
    strong=orig_fail[pd.to_numeric(orig_fail["best_accum_score"],errors="coerce").ge(70)].copy()
    strong.to_csv(out/"r212_strong_accum_original_failures.csv",index=False,encoding="utf-8-sig")

    report=[
        "# ODOLI R2.12 — Delayed Resolution / Resistance Anatomy","",
        "- Re-examines the 9 original D+5 +10% non-touches.",
        "- A D+5 miss is NOT automatically treated as permanent failure.",
        "- Checks later +10% touch through D+10/D+15/D+20.",
        "- Adds Ichimoku cloud state at D+5.",
        "- Adds a daily-OHLCV price-volume overhead-supply proxy (NOT true investor-position data).",
        "- Checks whether accumulation candle low/mid remains held.",
        "- Separates delayed success, supported unresolved, overhead-supply unresolved, second-pullback unresolved, and structural failure.",
        "- Research only; no production gate.","",
        "## Original D5 failures reclassified",
        "```",cls.to_string(index=False),"```","",
        "## Strong-accumulation original failures",
        "```",
        strong[[c for c in ["signal_date","code","name","best_accum_score","resolution_class",
                            "first_plus10_day_20","d5_cloud_state",
                            "vp_above_volume_share_pct","d5_holds_accum_mid",
                            "d5_holds_accum_low","post_d5_min_ret_pct"] if c in strong.columns]].to_string(index=False),
        "```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

    meta={
        "revision":REV,"research_only":True,"production_logic_changed":False,
        "new_gate_created":False,"same_sample_tuning":False,
        "original_d5_failure_n":int(len(orig_fail)),
        "d20_complete_n":int(orig_fail["d20_complete"].fillna(False).astype(bool).sum()),
        "volume_profile_semantics":"DAILY_OHLCV_PRICE_VOLUME_PROXY_NOT_TRUE_HOLDER_COST_BASIS",
        "ichimoku_semantics":"STANDARD_9_26_52_SHIFTED_26",
        "resolution_classes":cls.to_dict(orient="records")
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")

if __name__=="__main__":
    main()
