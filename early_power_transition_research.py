#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EARLY POWER / MA transition research (RESEARCH_ONLY)

Purpose
-------
Test whether an inverse-MA regime that relaxes into early alignment contains
forward edge, without changing production selection/ranking/order.

This is deliberately a descriptive state-transition study, not a tuned model.
Thresholds that are used as labels are frozen from the project's existing
research conventions where available:
  - inverse context: MA5 < MA20 < MA60 < MA112
  - MA20/60/112 convergence <= 8% (existing project diagnostic)
  - BB40 width <= 10% (existing project 'energy compression' convention)
  - MA20 disparity 98~106 (existing project preferred zone)

New quantities such as distance-to-high are exported continuously and are NOT
used to fit/tune production rules.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

REVISION = "EARLY_POWER_MA_TRANSITION_R1_20261003"
RESEARCH_ONLY = True


def norm_code(v) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-6:].zfill(6) if s else ""


def load_marcap(paths: List[str]) -> pd.DataFrame:
    parts=[]
    for p in paths:
        q=pd.read_parquet(p)
        if "Date" not in q.columns:
            q=q.reset_index()
        parts.append(q)
    if not parts:
        raise SystemExit("NO_MARCAP_INPUT")
    x=pd.concat(parts,ignore_index=True,sort=False)
    x["Date"]=pd.to_datetime(x["Date"],errors="coerce").dt.normalize()
    x["Code"]=x["Code"].map(norm_code)
    for c in ["Open","High","Low","Close","Volume","Amount","Marcap"]:
        if c in x.columns:
            x[c]=pd.to_numeric(x[c],errors="coerce")
    x=x.dropna(subset=["Date","Code","Open","High","Low","Close"])
    x=x[(x.Open>0)&(x.High>0)&(x.Low>0)&(x.Close>0)]
    x=x.sort_values(["Code","Date"],kind="stable").drop_duplicates(["Code","Date"],keep="last")
    return x.reset_index(drop=True)


def rsi14(close: pd.Series) -> pd.Series:
    d=close.diff()
    up=d.clip(lower=0).rolling(14,min_periods=14).mean()
    dn=(-d.clip(upper=0)).rolling(14,min_periods=14).mean()
    rs=up/dn.replace(0,np.nan)
    out=100-(100/(1+rs))
    return out.fillna(50.0)


def add_features(g: pd.DataFrame) -> pd.DataFrame:
    g=g.sort_values("Date").copy()
    c=g["Close"].astype(float)
    v=g["Volume"].fillna(0).astype(float)
    for n in [5,10,20,40,60,112,224]:
        g[f"ma{n}"]=c.rolling(n,min_periods=n).mean()
    std40=c.rolling(40,min_periods=40).std()
    g["bb40_width"]=(std40*4/g["ma40"])*100
    g["rsi14"]=rsi14(c)
    g["ret_5d"]=(c/c.shift(5)-1)*100
    g["ret_20d"]=(c/c.shift(20)-1)*100
    g["disp_ma20"]=(c/g["ma20"])*100
    g["dist_ma112_pct"]=(c/g["ma112"]-1)*100
    g["dist_ma224_pct"]=(c/g["ma224"]-1)*100
    g["high60"]=g["High"].rolling(60,min_periods=20).max()
    g["high120"]=g["High"].rolling(120,min_periods=40).max()
    g["space_high60_pct"]=(g["high60"]/c-1)*100
    g["space_high120_pct"]=(g["high120"]/c-1)*100
    g["vol20_mean"]=v.rolling(20,min_periods=10).mean()
    g["vol20_ratio"]=v/g["vol20_mean"].replace(0,np.nan)

    # OBV and causal slope proxy.
    sign=np.sign(c.diff()).fillna(0)
    g["obv"]=(sign*v).cumsum()
    g["obv_delta20"]=g["obv"]-g["obv"].shift(20)

    # slopes expressed as percent change over five sessions.
    for n in [5,20,60,112,224]:
        m=g[f"ma{n}"]
        g[f"ma{n}_slope5_pct"]=(m/m.shift(5)-1)*100

    # Core states.
    g["inverse_4"]=(g.ma5<g.ma20)&(g.ma20<g.ma60)&(g.ma60<g.ma112)
    g["inverse_5"]=g["inverse_4"]&(g.ma112<g.ma224)
    g["inverse_days_120"]=g["inverse_4"].astype(int).rolling(120,min_periods=20).sum()
    g["inverse_recent20"]=g["inverse_4"].astype(int).rolling(20,min_periods=1).max().astype(bool)
    g["inverse_recent60"]=g["inverse_4"].astype(int).rolling(60,min_periods=1).max().astype(bool)

    g["ma5_turn_up"]=(g.ma5>g.ma5.shift(1))&(g.ma5.shift(1)<=g.ma5.shift(2))
    g["cross_5_20"]=(g.ma5>g.ma20)&(g.ma5.shift(1)<=g.ma20.shift(1))
    g["cross_close_112"]=(c>g.ma112)&(c.shift(1)<=g.ma112.shift(1))
    g["cross_close_224"]=(c>g.ma224)&(c.shift(1)<=g.ma224.shift(1))
    g["aligned_4"]=(g.ma5>g.ma20)&(g.ma20>g.ma60)&(g.ma60>g.ma112)
    g["aligned_4_first"] = g["aligned_4"] & (~g["aligned_4"].shift(1, fill_value=False))

    vals=pd.concat([g.ma20,g.ma60,g.ma112],axis=1)
    g["ma20_60_112_conv_pct"]=(vals.max(axis=1)-vals.min(axis=1))/vals.max(axis=1)*100
    g["conv8"] = g["ma20_60_112_conv_pct"] <= 8.0
    g["bb40_tight10"] = g["bb40_width"] <= 10.0
    g["disp_preferred"] = g["disp_ma20"].between(98,106,inclusive="both")
    g["obv_hold_up"] = g["obv_delta20"] >= 0

    # Project-aligned first-wave / pullback context, exported descriptively.
    # No optimized threshold is used for production; this is a research tag only.
    low40=g["Low"].rolling(40,min_periods=20).min()
    hi15=g["High"].rolling(15,min_periods=8).max()
    impulse=(hi15/low40-1)*100
    retrace=(hi15-c)/(hi15-low40).replace(0,np.nan)*100
    g["impulse40_pct"]=impulse
    g["retrace_from_15h_pct"]=retrace
    g["first_wave_context"]=(impulse>=8)&(impulse<=35)&retrace.between(0,65,inclusive="both")

    # Non-tuned overheat diagnostics; raw components are preserved.
    g["overheat_rsi68"] = g.rsi14 >= 68
    g["overheat_disp112"] = g.disp_ma20 >= 112
    g["overheat_ret5_12"] = g.ret_5d >= 12
    g["overheat_count"] = g[["overheat_rsi68","overheat_disp112","overheat_ret5_12"]].sum(axis=1)

    # Stage events. Requirement: a real inverse context must have existed recently.
    inv60=g["inverse_recent60"]
    g["stage_S1_MA5_TURN"] = g.ma5_turn_up & inv60
    g["stage_S2_CROSS_5_20"] = g.cross_5_20 & inv60
    g["stage_S3_CONVERGENCE"] = (g.ma5>g.ma20) & g.conv8 & inv60
    g["stage_S4_RECLAIM_112"] = g.cross_close_112 & inv60
    g["stage_S5_RECLAIM_224"] = g.cross_close_224 & inv60
    g["stage_S6_EARLY_ALIGNED"] = g.aligned_4_first & inv60
    return g


STAGES = [
    ("S1_MA5_TURN","stage_S1_MA5_TURN"),
    ("S2_CROSS_5_20","stage_S2_CROSS_5_20"),
    ("S3_CONVERGENCE","stage_S3_CONVERGENCE"),
    ("S4_RECLAIM_112","stage_S4_RECLAIM_112"),
    ("S5_RECLAIM_224","stage_S5_RECLAIM_224"),
    ("S6_EARLY_ALIGNED","stage_S6_EARLY_ALIGNED"),
]


def event_rows(g: pd.DataFrame, study_start: pd.Timestamp, study_end: pd.Timestamp, cooldown: int) -> List[dict]:
    rows=[]
    dates=list(g.Date)
    last_by_stage={}
    code=str(g.Code.iloc[0])
    name=str(g.Name.iloc[-1]) if "Name" in g.columns else ""
    for i,r in g.iterrows():
        d=pd.Timestamp(r.Date)
        if d<study_start or d>study_end:
            continue
        for stage,col in STAGES:
            if not bool(r.get(col,False)):
                continue
            pos=g.index.get_loc(i)
            lp=last_by_stage.get(stage,-10**9)
            if pos-lp < cooldown:
                continue
            last_by_stage[stage]=pos
            rec={
                "revision":REVISION,"research_only":True,
                "signal_date":d.date().isoformat(),"code":code,"name":name,
                "stage":stage,"entry_close":float(r.Close),
            }
            keep=[
                "inverse_days_120","inverse_4","inverse_5","ma5_turn_up","cross_5_20",
                "ma20_60_112_conv_pct","conv8","bb40_width","bb40_tight10",
                "disp_ma20","disp_preferred","rsi14","ret_5d","ret_20d",
                "vol20_ratio","obv_delta20","obv_hold_up","dist_ma112_pct","dist_ma224_pct",
                "space_high60_pct","space_high120_pct","impulse40_pct","retrace_from_15h_pct",
                "first_wave_context","overheat_count","ma5_slope5_pct","ma20_slope5_pct",
                "ma60_slope5_pct","ma112_slope5_pct","ma224_slope5_pct",
            ]
            for k in keep: rec[k]=r.get(k,np.nan)
            # Frozen, transparent context count. This is NOT a fitted model.
            favorable=[
                float(r.get("inverse_days_120",0) or 0)>=30,
                bool(r.get("conv8",False)),
                bool(r.get("bb40_tight10",False)),
                bool(r.get("disp_preferred",False)),
                bool(r.get("obv_hold_up",False)),
                float(r.get("ma20_slope5_pct",-999) or -999)>=0,
                float(r.get("overheat_count",99) or 99)==0,
            ]
            rec["context_hits_7"] = int(sum(favorable))
            rec["context_definition"] = "inverse30,conv8,bb40<=10,disp98-106,obv20>=0,ma20slope>=0,no_overheat"

            # Forward outcomes from signal close, signal day excluded.
            for h in [1,3,5,10,20]:
                if pos+h < len(g):
                    w=g.iloc[pos+1:pos+h+1]
                    rec[f"d{h}_date"]=pd.Timestamp(g.iloc[pos+h].Date).date().isoformat()
                    rec[f"d{h}_close_ret_pct"]=(float(g.iloc[pos+h].Close)/float(r.Close)-1)*100
                    rec[f"mfe{h}_pct"]=(float(w.High.max())/float(r.Close)-1)*100
                    rec[f"mae{h}_pct"]=(float(w.Low.min())/float(r.Close)-1)*100
                    # First +5/+10 and -5 touch within window, using intraday H/L.
                    for thr in [5,10]:
                        hits=np.where((w.High.values/float(r.Close)-1)*100>=thr)[0]
                        rec[f"first_plus{thr}_day_h{h}"]=int(hits[0]+1) if len(hits) else np.nan
                    stops=np.where((w.Low.values/float(r.Close)-1)*100<=-5)[0]
                    rec[f"first_minus5_day_h{h}"]=int(stops[0]+1) if len(stops) else np.nan
                else:
                    rec[f"d{h}_date"]=""
                    rec[f"d{h}_close_ret_pct"]=np.nan
                    rec[f"mfe{h}_pct"]=np.nan
                    rec[f"mae{h}_pct"]=np.nan
            rows.append(rec)
    return rows


def summarize(led: pd.DataFrame) -> pd.DataFrame:
    out=[]
    if led.empty:return pd.DataFrame()
    # Overall stage plus context hit strata; no selection is promoted.
    group_specs=[("STAGE",["stage"]),("STAGE_X_CONTEXT",["stage","context_hits_7"])]
    for view,keys in group_specs:
        for vals,g in led.groupby(keys,dropna=False):
            if not isinstance(vals,tuple): vals=(vals,)
            base={"view":view,"n":len(g),"signal_days":g.signal_date.nunique()}
            for k,v in zip(keys,vals):base[k]=v
            for h in [1,3,5,10,20]:
                s=pd.to_numeric(g[f"d{h}_close_ret_pct"],errors="coerce")
                mfe=pd.to_numeric(g[f"mfe{h}_pct"],errors="coerce")
                mae=pd.to_numeric(g[f"mae{h}_pct"],errors="coerce")
                ok=s.notna()
                base[f"d{h}_n"]=int(ok.sum())
                base[f"d{h}_mean"]=float(s[ok].mean()) if ok.any() else np.nan
                base[f"d{h}_median"]=float(s[ok].median()) if ok.any() else np.nan
                base[f"d{h}_positive_pct"]=float((s[ok]>0).mean()*100) if ok.any() else np.nan
                base[f"mfe{h}_median"]=float(mfe.dropna().median()) if mfe.notna().any() else np.nan
                base[f"mae{h}_median"]=float(mae.dropna().median()) if mae.notna().any() else np.nan
                base[f"mfe{h}_ge5_pct"]=float((mfe.dropna()>=5).mean()*100) if mfe.notna().any() else np.nan
                base[f"mfe{h}_ge10_pct"]=float((mfe.dropna()>=10).mean()*100) if mfe.notna().any() else np.nan
            out.append(base)
    return pd.DataFrame(out)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--marcap",nargs="+",required=True)
    ap.add_argument("--out",default="reports/early_power_transition")
    ap.add_argument("--start",default="2026-01-01")
    ap.add_argument("--end",default="2099-12-31")
    ap.add_argument("--cooldown",type=int,default=20)
    ap.add_argument("--min-amount-b",type=float,default=20.0,
                    help="descriptive liquidity floor in KRW billions using rolling 20d median Amount; 0 disables")
    args=ap.parse_args()
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    x=load_marcap(args.marcap)
    start=pd.Timestamp(args.start); end=pd.Timestamp(args.end)
    allrows=[]; diag=[]
    for code,g0 in x.groupby("Code",sort=False):
        try:
            g=add_features(g0.reset_index(drop=True))
            if "Amount" in g.columns and args.min_amount_b>0:
                g["amount20_med_b"]=g.Amount.rolling(20,min_periods=10).median()/1e9
                # liquidity is an explicit study-population guard, not a ranking score
                g=g[g.amount20_med_b>=args.min_amount_b].copy()
                if g.empty: continue
                # Reindex after population guard; event outcomes still come from remaining sessions only
                # so DO NOT use filtered frame for outcomes. Recompute events from original feature frame and
                # require signal-day liquidity inside the event predicate below instead.
                g=add_features(g0.reset_index(drop=True))
                g["amount20_med_b"]=g.Amount.rolling(20,min_periods=10).median()/1e9
                mask_liq=g.amount20_med_b>=args.min_amount_b
                for _,col in STAGES:g[col]=g[col]&mask_liq.fillna(False)
            rr=event_rows(g,start,end,args.cooldown)
            allrows.extend(rr)
        except Exception as e:
            diag.append({"code":code,"error":f"{type(e).__name__}:{str(e)[:200]}"})
    led=pd.DataFrame(allrows)
    if led.empty:raise SystemExit("EARLY_POWER_NO_EVENTS")
    led=led.sort_values(["signal_date","stage","code"]).reset_index(drop=True)
    led.to_csv(out/"early_power_event_ledger.csv",index=False,encoding="utf-8-sig")
    sm=summarize(led)
    sm.to_csv(out/"early_power_stage_summary.csv",index=False,encoding="utf-8-sig")
    pd.DataFrame(diag).to_csv(out/"diagnostics.csv",index=False,encoding="utf-8-sig")

    # Candidate shadow board from latest study date only. It is intentionally not production authority.
    latest=led.signal_date.max()
    board=led[led.signal_date.eq(latest)].copy()
    # Frozen descriptive ordering: context_hits only, then lower overheat, then more upside space.
    # This is a prospective shadow order, not a backfit to future returns.
    board=board.sort_values(["context_hits_7","overheat_count","space_high120_pct","code"],
                            ascending=[False,True,False,True],kind="stable")
    board.insert(0,"shadow_rank",range(1,len(board)+1))
    board.to_csv(out/"early_power_latest_shadow_board.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REVISION,"research_only":True,
        "production_changed":False,"production_search_changed":False,
        "production_score_changed":False,"production_rank_changed":False,
        "production_order_changed":False,"automatic_orders":False,
        "same_sample_retuning":False,
        "study_start":args.start,"study_end":args.end,"cooldown_sessions":args.cooldown,
        "min_amount20_median_b":args.min_amount_b,
        "event_rows":len(led),"signal_days":int(led.signal_date.nunique()),
        "codes":int(led.code.nunique()),"latest_signal_date":latest,
        "stage_definitions":{k:v for k,v in STAGES},
        "project_frozen_labels":{
            "inverse":"MA5 < MA20 < MA60 < MA112",
            "convergence":"MA20/60/112 max-min <= 8% of max",
            "bb40_tight":"BB40 width <= 10%",
            "preferred_disparity":"Close/MA20 * 100 in 98..106"
        },
        "note":"Descriptive state-transition validation. Shadow order is prospective and must not be promoted from same-sample results."
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    lines=["# EARLY POWER MA Transition R1","",json.dumps(meta,ensure_ascii=False,indent=2),"",
           "## Stage summary",sm.to_string(index=False),"","## Latest shadow board",
           board.head(20).to_string(index=False)]
    (out/"REPORT.txt").write_text("\n".join(lines),encoding="utf-8")
    print("EARLY_POWER_MA_TRANSITION_PASS")
    print(json.dumps(meta,ensure_ascii=False))
    print(sm.to_string(index=False))

if __name__=="__main__":
    main()
