#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="CLOSEBET_EXIT_WINDOW_OOS_R1081_FULL_EXIT_WINDOW_FIX_20260918"
OOS_START="2026-08-19"
OOS_END="2026-09-18"
EXIT_DAYS=[2,3,5,7,10,15,20]
FOCUS_PATTERNS={"C","B1","B2","I"}

def read_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:pass
    return pd.DataFrame()

def find(root,name):
    xs=list(Path(root).rglob(name));xs.sort(key=lambda p:(len(p.parts),str(p)))
    return xs[0] if xs else None

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():s=s[:-2]
    if len(s)==7 and s.startswith("A"):s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def prep_hist(h):
    q=h.copy()
    q["code"]=q["code"].map(norm_code)
    q["date"]=pd.to_datetime(q["date"],errors="coerce").dt.normalize()
    for c in ["Open","High","Low","Close","Volume","Amount"]:
        if c in q:q[c]=pd.to_numeric(q[c],errors="coerce")
    return q[q.code.ne("") & q.date.notna()].sort_values(["code","date"]).copy()

def exact_exit_from_hist(row,g,day):
    if g is None or g.empty:return np.nan
    dt=pd.Timestamp(row.signal_date).normalize()
    z=g[g.date>=dt].sort_values("date").reset_index(drop=True)
    if len(z)<=day:return np.nan
    entry=row.get("entry_price",np.nan)
    if not fin(entry) or float(entry)<=0:
        entry=z.loc[0,"Close"]
    if not fin(entry) or float(entry)<=0:return np.nan
    px=z.loc[day,"Close"]
    if not fin(px):return np.nan
    return (float(px)/float(entry)-1)*100.0

def fin(x):
    try:return math.isfinite(float(x))
    except:return False

def qnum(s):
    return pd.to_numeric(s,errors="coerce")

def build_event_metrics(ev,hgroups=None):
    q=ev.copy()
    rows=[]
    for _,r in q.iterrows():
        out={
            "signal_date":r.get("signal_date"),
            "code":r.get("code"),
            "name":r.get("name",""),
            "primary_formula":str(r.get("primary_formula","UNCLASSIFIED")),
            "swing_class":str(r.get("swing_class","UNRESOLVED")),
        }
        for c in q.columns:
            if c.startswith("TAG_"): out[c]=r.get(c,False)

        # D20 mature is the main one-month comparison horizon.
        mature20=bool(r.get("D20_mature",False))
        out["D20_mature"]=mature20
        if mature20:
            peak_day=r.get("D20_peak_day",np.nan)
            mfe=r.get("D20_mfe",np.nan)
            d20=r.get("D20_close_ret",np.nan)
            giveback=r.get("D20_peak_giveback_to_horizon",np.nan)
            out["peak_day_20"]=peak_day
            out["mfe_20"]=mfe
            out["close_ret_20"]=d20
            out["giveback_to_D20"]=giveback
            out["capture_ratio_D20_vs_MFE"]=(d20/mfe) if fin(mfe) and float(mfe)>0 and fin(d20) else np.nan
            out["giveback_pct_of_MFE"]=((mfe-d20)/mfe*100) if fin(mfe) and float(mfe)>0 and fin(d20) else np.nan
            out["peak_bucket"]=(
                "D1_3" if fin(peak_day) and peak_day<=3 else
                "D4_5" if fin(peak_day) and peak_day<=5 else
                "D6_10" if fin(peak_day) and peak_day<=10 else
                "D11_15" if fin(peak_day) and peak_day<=15 else
                "D16_20" if fin(peak_day) and peak_day<=20 else "UNKNOWN"
            )
        else:
            out.update({"peak_day_20":np.nan,"mfe_20":np.nan,"close_ret_20":np.nan,
                        "giveback_to_D20":np.nan,"capture_ratio_D20_vs_MFE":np.nan,
                        "giveback_pct_of_MFE":np.nan,"peak_bucket":"IMMATURE"})

        g = hgroups.get(norm_code(r.get("code",""))) if isinstance(hgroups,dict) else None
        for d in EXIT_DAYS:
            source_mature = bool(r.get(f"D{d}_mature",False))
            source_ret = r.get(f"D{d}_close_ret",np.nan) if source_mature else np.nan

            # R1.0.7 did not emit D2/D3/D7. Reconstruct those from the same causal OHLCV.
            if d in (2,3,7):
                hist_ret = exact_exit_from_hist(r,g,d)
                mature = fin(hist_ret)
                rr = hist_ret if mature else np.nan
                out[f"D{d}_source"]="R102_CAUSAL_OHLCV_RECONSTRUCTED"
            else:
                mature = source_mature
                rr = source_ret if mature else np.nan
                out[f"D{d}_source"]="R107_EVENT_MASTER"

            out[f"D{d}_mature"]=bool(mature)
            out[f"D{d}_close_ret"]=rr

            mfe20=out.get("mfe_20",np.nan)
            out[f"D{d}_capture_of_D20_MFE"]=(rr/mfe20) if mature20 and fin(mfe20) and float(mfe20)>0 and fin(rr) else np.nan
        rows.append(out)
    return pd.DataFrame(rows)

def peak_summary(m):
    rows=[]
    for p,g in [("ALL",m)]+[(str(k),v) for k,v in m.groupby("primary_formula")]:
        z=g[g.D20_mature.eq(True)]
        if z.empty:continue
        peak=qnum(z.peak_day_20); mfe=qnum(z.mfe_20); gb=qnum(z.giveback_to_D20); gp=qnum(z.giveback_pct_of_MFE)
        rows.append({
            "pattern":p,"mature_n":len(z),
            "peak_day_median":peak.median(),"peak_day_mean":peak.mean(),
            "peak_by_D5_rate":peak.le(5).mean()*100,
            "peak_by_D10_rate":peak.le(10).mean()*100,
            "mfe20_median":mfe.median(),
            "giveback_to_D20_median":gb.median(),
            "giveback_pct_of_MFE_median":gp.median(),
            "D20_positive_close_rate":qnum(z.close_ret_20).gt(0).mean()*100,
        })
    return pd.DataFrame(rows)

def peak_bucket_summary(m):
    z=m[m.D20_mature.eq(True)]
    if z.empty:return pd.DataFrame()
    return (z.groupby(["primary_formula","peak_bucket"],dropna=False)
            .agg(n=("code","size"),
                 mfe20_median=("mfe_20","median"),
                 d20_median=("close_ret_20","median"),
                 giveback_median=("giveback_to_D20","median"))
            .reset_index())

def fixed_exit_summary(m):
    rows=[]
    scopes=[("ALL",m)]+[(str(k),v) for k,v in m.groupby("primary_formula")]
    for p,g in scopes:
        # fair comparison: same D20-mature cohort for all fixed exits
        z=g[g.D20_mature.eq(True)].copy()
        if z.empty:continue
        for d in EXIT_DAYS:
            zz=z[z[f"D{d}_mature"].eq(True)]
            if zz.empty:continue
            r=qnum(zz[f"D{d}_close_ret"])
            cap=qnum(zz[f"D{d}_capture_of_D20_MFE"])
            rows.append({
                "pattern":p,"exit_day":d,"same_cohort":"D20_MATURE_ONLY",
                "n":len(zz),"mean_ret":r.mean(),"median_ret":r.median(),
                "positive_rate":r.gt(0).mean()*100,
                "p25":r.quantile(.25),"p75":r.quantile(.75),
                "capture_of_D20_MFE_median":cap.median(),
            })
    return pd.DataFrame(rows)

def retention_summary(m):
    # Descriptive retention after reaching +5/+10 using first-hit day from R107.
    rows=[]
    # Source columns may not survive build_event_metrics, so this function uses original event master externally.
    return pd.DataFrame(rows)

def tag_exit_summary(m):
    tagcols=[c for c in m.columns if c.startswith("TAG_")]
    rows=[]
    for p,g in m.groupby("primary_formula"):
        z=g[g.D20_mature.eq(True)]
        if len(z)<5:continue
        for tag in tagcols:
            for val in [True,False]:
                zz=z[z[tag].eq(val)]
                if len(zz)<3:continue
                for d in [3,5,10,15,20]:
                    r=qnum(zz[f"D{d}_close_ret"])
                    if r.notna().sum()<3:continue
                    rows.append({
                        "pattern":p,"tag":tag,"tag_value":val,"exit_day":d,"n":r.notna().sum(),
                        "mean_ret":r.mean(),"median_ret":r.median(),
                        "positive_rate":r.gt(0).mean()*100
                    })
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r107-root",default="r107_artifacts")
    ap.add_argument("--history-cache",default=".cache/closebet_structure_env_oos_r102/v49_76_research_raw_history.csv")
    ap.add_argument("--output-dir",default="reports/closebet_exit_window_oos_r108")
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    ep=find(a.r107_root,"oos_swing_event_master.csv")
    if ep is None: raise SystemExit("R108_INPUT_MISSING oos_swing_event_master.csv")
    ev=read_csv(ep)
    if ev.empty: raise SystemExit("R108_EMPTY_INPUT")
    hp=Path(a.history_cache)
    if not hp.exists(): raise SystemExit("R1081_HISTORY_MISSING")
    hist=prep_hist(read_csv(hp))
    hgroups={c:g.copy() for c,g in hist.groupby("code")}
    ev["code"]=ev["code"].map(norm_code)

    metrics=build_event_metrics(ev,hgroups)
    focus=metrics[metrics.primary_formula.astype(str).isin(FOCUS_PATTERNS)].copy()

    peaks=peak_summary(metrics)
    peakb=peak_bucket_summary(metrics)
    exits=fixed_exit_summary(metrics)
    tagexit=tag_exit_summary(metrics)

    # same-cohort focused comparison table for C/B1/B2/I
    focus_rows=[]
    for p,g in focus.groupby("primary_formula"):
        z=g[g.D20_mature.eq(True)]
        for d in EXIT_DAYS:
            zz=z[z[f"D{d}_mature"].eq(True)]
            if zz.empty:continue
            r=qnum(zz[f"D{d}_close_ret"])
            focus_rows.append({
                "pattern":p,"exit_day":d,"n":len(zz),
                "mean_ret":r.mean(),"median_ret":r.median(),
                "positive_rate":r.gt(0).mean()*100,
                "capture_of_D20_MFE_median":qnum(zz[f"D{d}_capture_of_D20_MFE"]).median()
            })
    focus_exit=pd.DataFrame(focus_rows)

    metrics.to_csv(out/"oos_exit_event_master.csv",index=False,encoding="utf-8-sig")
    peaks.to_csv(out/"oos_time_to_peak_summary.csv",index=False,encoding="utf-8-sig")
    peakb.to_csv(out/"oos_peak_bucket_summary.csv",index=False,encoding="utf-8-sig")
    exits.to_csv(out/"oos_fixed_exit_window_same_cohort.csv",index=False,encoding="utf-8-sig")
    focus_exit.to_csv(out/"oos_focus_pattern_exit_window.csv",index=False,encoding="utf-8-sig")
    tagexit.to_csv(out/"oos_shadow_tag_exit_window.csv",index=False,encoding="utf-8-sig")

    d20n=int(metrics.D20_mature.eq(True).sum())
    meta={
        "revision":REVISION,"status":"PASS","event_rows":len(metrics),"d20_mature_rows":d20n,
        "focus_patterns":sorted(FOCUS_PATTERNS),"exit_days":EXIT_DAYS,
        "research_only":True,"production_eligible":False,
        "selection_logic_changed":False,"score_rank_changed":False,"order_logic_changed":False,
        "same_sample_retuning":False,
        "same_cohort_exit_comparison":True,
        "uses_hindsight_optimized_exit":False,
        "full_exit_window_complete":True,
        "d2_d3_d7_reconstructed_from_causal_ohlcv":True,
        "notes":[
            "Fixed exit windows are predeclared and compared on the same D20-mature cohort.",
            "Time-to-peak and giveback are descriptive post-signal diagnostics, not entry filters.",
            "No best-exit rule is promoted from this sample.",
            "D2/D3/D7 are reconstructed from R102 causal OHLCV; D5/D10/D15/D20 come from R107.",
            "D30/D40 remain outside the current mature comparison."
        ]
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    lines=[
        "⏱️ [CLOSING BET · TIME-TO-PEAK + GIVEBACK + EXIT WINDOW OOS R1.0.8]",
        f"status=PASS | events={len(metrics)} | D20 mature={d20n}",
        "",
        "핵심:",
        "- D20까지 성숙한 동일 코호트에서 D2/3/5/7/10/15/20 고정 출구를 비교",
        "- D2/D3/D7은 R102 causal OHLCV에서 직접 재구성",
        "- 패턴별 peak day / MFE / D20 giveback / MFE capture 비율 측정",
        "- C/B1/B2/I를 별도 focus table로 출력",
        "- 사후적으로 최적 exit를 선택하지 않음",
        "- 검색식·점수·랭킹·후보제거·주문 변경 0",
    ]
    (out/"report.txt").write_text("\n".join(lines),encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    main()
