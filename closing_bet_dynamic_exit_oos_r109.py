#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="CLOSEBET_DYNAMIC_EXIT_OOS_R109_20260918"
FOCUS_PATTERNS={"C","B1","B2","I"}
MAX_HOLD=20

# Predeclared, non-optimized dynamic policies.
POLICIES=[
    "HOLD_D5","HOLD_D7","HOLD_D10","HOLD_D15","HOLD_D20",
    "MA5_CLOSE_BREAK_AFTER_D3",
    "MA10_CLOSE_BREAK_AFTER_D5",
    "PRIOR3_LOW_CLOSE_BREAK_AFTER_D3",
    "ACT5_GIVEBACK3_CLOSE",
    "ACT10_GIVEBACK5_CLOSE",
]

def read_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:pass
    return pd.DataFrame()

def find(root,name):
    xs=list(Path(root).rglob(name)); xs.sort(key=lambda p:(len(p.parts),str(p)))
    return xs[0] if xs else None

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():s=s[:-2]
    if len(s)==7 and s.startswith("A"):s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def fin(x):
    try:return math.isfinite(float(x))
    except:return False

def prep_hist(h):
    q=h.copy()
    q["code"]=q["code"].map(norm_code)
    q["date"]=pd.to_datetime(q["date"],errors="coerce").dt.normalize()
    for c in ["Open","High","Low","Close","Volume","Amount"]:
        if c in q:q[c]=pd.to_numeric(q[c],errors="coerce")
    return q[q.code.ne("") & q.date.notna()].sort_values(["code","date"]).copy()

def event_frame(r,g):
    dt=pd.Timestamp(r.signal_date).normalize()
    z=g[g.date>=dt].sort_values("date").reset_index(drop=True).copy()
    if len(z)<MAX_HOLD+1:return None

    entry=r.get("entry_price",np.nan)
    if not fin(entry) or float(entry)<=0:
        entry=z.loc[0,"Close"]
    if not fin(entry) or float(entry)<=0:return None

    # Need enough prior history for MA5/10 at signal and thereafter.
    full=g[g.date<=z.loc[MAX_HOLD,"date"]].sort_values("date").copy()
    full["ma5"]=full.Close.rolling(5,min_periods=5).mean()
    full["ma10"]=full.Close.rolling(10,min_periods=10).mean()
    z=full[full.date>=dt].head(MAX_HOLD+1).reset_index(drop=True)
    if len(z)<MAX_HOLD+1:return None

    z["day"]=np.arange(len(z))
    z["close_ret"]=(z.Close/float(entry)-1)*100
    z["high_ret"]=(z.High/float(entry)-1)*100
    z["low_ret"]=(z.Low/float(entry)-1)*100
    z["prior3_low"]=z.Low.shift(1).rolling(3,min_periods=3).min()
    return z,float(entry)

def fixed_exit(z,d):
    return d,float(z.loc[d,"close_ret"]),f"D{d}_FIXED"

def dynamic_exit(z,policy):
    if policy.startswith("HOLD_D"):
        d=int(policy.split("D")[1]); return fixed_exit(z,d)

    if policy=="MA5_CLOSE_BREAK_AFTER_D3":
        for d in range(3,MAX_HOLD+1):
            ma=z.loc[d,"ma5"]; c=z.loc[d,"Close"]
            if fin(ma) and fin(c) and float(c)<float(ma):
                return d,float(z.loc[d,"close_ret"]),"MA5_CLOSE_BREAK"
        return fixed_exit(z,MAX_HOLD)

    if policy=="MA10_CLOSE_BREAK_AFTER_D5":
        for d in range(5,MAX_HOLD+1):
            ma=z.loc[d,"ma10"]; c=z.loc[d,"Close"]
            if fin(ma) and fin(c) and float(c)<float(ma):
                return d,float(z.loc[d,"close_ret"]),"MA10_CLOSE_BREAK"
        return fixed_exit(z,MAX_HOLD)

    if policy=="PRIOR3_LOW_CLOSE_BREAK_AFTER_D3":
        for d in range(3,MAX_HOLD+1):
            pl=z.loc[d,"prior3_low"]; c=z.loc[d,"Close"]
            if fin(pl) and fin(c) and float(c)<float(pl):
                return d,float(z.loc[d,"close_ret"]),"PRIOR3_LOW_CLOSE_BREAK"
        return fixed_exit(z,MAX_HOLD)

    if policy=="ACT5_GIVEBACK3_CLOSE":
        peak=-1e9; active=False
        for d in range(1,MAX_HOLD+1):
            peak=max(peak,float(z.loc[d,"high_ret"]))
            if peak>=5: active=True
            if active and float(z.loc[d,"close_ret"]) <= peak-3:
                return d,float(z.loc[d,"close_ret"]),"ACT5_GIVEBACK3"
        return fixed_exit(z,MAX_HOLD)

    if policy=="ACT10_GIVEBACK5_CLOSE":
        peak=-1e9; active=False
        for d in range(1,MAX_HOLD+1):
            peak=max(peak,float(z.loc[d,"high_ret"]))
            if peak>=10: active=True
            if active and float(z.loc[d,"close_ret"]) <= peak-5:
                return d,float(z.loc[d,"close_ret"]),"ACT10_GIVEBACK5"
        return fixed_exit(z,MAX_HOLD)

    raise ValueError(policy)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r1081-root",default="r1081_artifacts")
    ap.add_argument("--history-cache",default=".cache/closebet_structure_env_oos_r102/v49_76_research_raw_history.csv")
    ap.add_argument("--output-dir",default="reports/closebet_dynamic_exit_oos_r109")
    a=ap.parse_args()

    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    ep=find(a.r1081_root,"oos_exit_event_master.csv")
    if ep is None: raise SystemExit("R109_INPUT_MISSING oos_exit_event_master.csv")
    hp=Path(a.history_cache)
    if not hp.exists(): raise SystemExit("R109_HISTORY_MISSING")

    ev=read_csv(ep)
    hist=prep_hist(read_csv(hp))
    ev["code"]=ev["code"].map(norm_code)
    ev["signal_date"]=pd.to_datetime(ev.signal_date,errors="coerce").dt.normalize()

    # Fair cohort: only D20-mature rows, same as R1.0.8.1.
    ev=ev[ev.D20_mature.eq(True)].copy()
    hgroups={c:g.copy() for c,g in hist.groupby("code")}

    ledger=[]
    skipped=0
    for _,r in ev.iterrows():
        g=hgroups.get(r.code)
        if g is None:
            skipped+=1; continue
        fr=event_frame(r,g)
        if fr is None:
            skipped+=1; continue
        z,entry=fr
        mfe20=float(z.loc[1:MAX_HOLD,"high_ret"].max())
        mae20=float(z.loc[1:MAX_HOLD,"low_ret"].min())

        for pol in POLICIES:
            d,ret,reason=dynamic_exit(z,pol)
            ledger.append({
                "signal_date":r.signal_date,"code":r.code,"name":r.get("name",""),
                "primary_formula":str(r.get("primary_formula","UNCLASSIFIED")),
                "policy":pol,"exit_day":d,"exit_ret":ret,"exit_reason":reason,
                "entry_price":entry,"mfe20":mfe20,"mae20":mae20,
                "capture_of_mfe20":ret/mfe20 if mfe20>0 else np.nan,
                "avoided_giveback_vs_D20":ret-float(r.get("D20_close_ret",np.nan))
                    if fin(r.get("D20_close_ret",np.nan)) else np.nan,
            })

    led=pd.DataFrame(ledger)
    if led.empty: raise SystemExit("R109_NO_LEDGER")

    rows=[]
    scopes=[("ALL",led)]+[(str(k),v) for k,v in led.groupby("primary_formula")]
    for p,g in scopes:
        for pol,z in g.groupby("policy"):
            r=pd.to_numeric(z.exit_ret,errors="coerce")
            cap=pd.to_numeric(z.capture_of_mfe20,errors="coerce")
            av=pd.to_numeric(z.avoided_giveback_vs_D20,errors="coerce")
            rows.append({
                "pattern":p,"policy":pol,"n":len(z),
                "mean_ret":r.mean(),"median_ret":r.median(),
                "positive_rate":r.gt(0).mean()*100,
                "p25":r.quantile(.25),"p75":r.quantile(.75),
                "exit_day_mean":pd.to_numeric(z.exit_day,errors="coerce").mean(),
                "exit_day_median":pd.to_numeric(z.exit_day,errors="coerce").median(),
                "capture_of_mfe20_median":cap.median(),
                "avoided_giveback_vs_D20_median":av.median(),
            })
    summary=pd.DataFrame(rows)

    focus=summary[summary.pattern.isin(FOCUS_PATTERNS)].copy()
    reasons=(led.groupby(["primary_formula","policy","exit_reason"],dropna=False)
             .size().rename("n").reset_index())

    led.to_csv(out/"oos_dynamic_exit_event_ledger.csv",index=False,encoding="utf-8-sig")
    summary.to_csv(out/"oos_dynamic_exit_summary.csv",index=False,encoding="utf-8-sig")
    focus.to_csv(out/"oos_focus_pattern_dynamic_exit.csv",index=False,encoding="utf-8-sig")
    reasons.to_csv(out/"oos_dynamic_exit_reason_counts.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REVISION,"status":"PASS","d20_input_rows":len(ev),
        "evaluated_events":int(led[["signal_date","code"]].drop_duplicates().shape[0]),
        "ledger_rows":len(led),"skipped_events":skipped,
        "policies":POLICIES,
        "research_only":True,"production_eligible":False,
        "selection_logic_changed":False,"score_rank_changed":False,
        "order_logic_changed":False,"same_sample_retuning":False,
        "same_d20_mature_cohort":True,"policy_thresholds_predeclared":True,
        "hindsight_best_policy_promotion":False,
        "notes":[
            "Dynamic exits use only information available through each exit day's close.",
            "MA exits use causal rolling averages built from historical OHLCV.",
            "Giveback exits activate only after predeclared MFE thresholds and exit at the observed daily close.",
            "No intraday stop ordering assumption is used.",
            "Results are descriptive OOS comparisons; no production rule is promoted."
        ]
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    lines=[
        "🧭 [CLOSING BET · DYNAMIC EXIT OOS R1.0.9]",
        f"status=PASS | D20 cohort={len(ev)} | evaluated={meta['evaluated_events']} | ledger={len(led)}",
        "",
        "Policies:",
        *[f"- {p}" for p in POLICIES],
        "",
        "Guardrails:",
        "- same D20-mature cohort",
        "- post-signal causal data only",
        "- no same-sample threshold tuning",
        "- no scanner/score/rank/candidate/order change",
        "- no hindsight best-policy promotion",
    ]
    (out/"report.txt").write_text("\n".join(lines),encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    main()
