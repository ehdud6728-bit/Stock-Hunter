#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from real_full_decision_engine import code, text, num
from real_full_trust_audit import load_price_frames

ANALYZER_ID="REAL_FULL_CONTEXT_PATTERN_RISK_R1"
ANALYZER_REVISION="REAL_FULL_CONTEXT_PATTERN_RISK_R1_1_REALIZED_R_MULTIPLE"
EXPLORATORY_ONLY=1

HORIZONS=(1,3,5,10)
TARGET_RS=(1,2,3)

def readcsv(p,**kw):
    p=Path(p)
    if not p.exists(): return pd.DataFrame()
    try:return pd.read_csv(p,**kw)
    except pd.errors.EmptyDataError:return pd.DataFrame()

def sample_tier(n:int)->str:
    if n<5:return "TOO_SMALL_LT5"
    if n<10:return "EARLY_5_9"
    if n<30:return "DESCRIPTIVE_10_29"
    return "STRONGER_DESCRIPTIVE_30PLUS"

def frame_after(frame:pd.DataFrame,date:str,h:int)->pd.DataFrame:
    if frame is None or frame.empty:return pd.DataFrame()
    f=frame.copy()
    f["date"]=pd.to_datetime(f["date"],errors="coerce").dt.normalize()
    d=pd.Timestamp(date).normalize()
    return f[f["date"].gt(d)].sort_values("date").iloc[:h].copy()

def path_result(path:pd.DataFrame, entry:float, stop:float, r_multiple:int)->str:
    if path is None or path.empty:return "UNMATURED"
    risk=entry-stop
    if not (math.isfinite(risk) and risk>0):return "INVALID_RISK"
    target=entry+r_multiple*risk
    for _,bar in path.iterrows():
        hi=num(bar.get("high")); lo=num(bar.get("low"))
        hit=math.isfinite(hi) and hi>=target
        stp=math.isfinite(lo) and lo<=stop
        if hit and stp:return "AMBIGUOUS_SAME_BAR"
        if hit:return "TARGET_FIRST"
        if stp:return "STOP_FIRST"
    return "OPEN"

def enrich(events:pd.DataFrame,frames:Dict[str,pd.DataFrame])->pd.DataFrame:
    if events.empty:return events
    out=events.copy()
    for i,r in out.iterrows():
        entry=num(r.get("origin_price")); stop=num(r.get("frozen_stop"))
        risk=entry-stop if math.isfinite(entry) and math.isfinite(stop) else np.nan
        risk_pct=(risk/entry*100) if math.isfinite(risk) and risk>0 and entry>0 else np.nan
        out.at[i,"initial_risk_abs"]=risk
        out.at[i,"initial_risk_pct"]=risk_pct
        out.at[i,"static_source_rr_ratio"]=num(r.get("origin_source_rr_ratio"))
        c=code(r.get("code"))
        f=frames.get(c)
        for h in HORIZONS:
            cr=num(r.get(f"origin_d{h}_close_ret_pct"))
            mfe=num(r.get(f"origin_d{h}_mfe_pct"))
            mae=num(r.get(f"origin_d{h}_mae_pct"))
            out.at[i,f"d{h}_close_r"]=(cr/risk_pct) if math.isfinite(cr) and math.isfinite(risk_pct) and risk_pct>0 else np.nan
            out.at[i,f"d{h}_mfe_r"]=(mfe/risk_pct) if math.isfinite(mfe) and math.isfinite(risk_pct) and risk_pct>0 else np.nan
            out.at[i,f"d{h}_mae_r"]=(mae/risk_pct) if math.isfinite(mae) and math.isfinite(risk_pct) and risk_pct>0 else np.nan
            path=frame_after(f,r.get("origin_date"),h)
            for rr in TARGET_RS:
                out.at[i,f"d{h}_{rr}r_path"]=path_result(path,entry,stop,rr)
    # Context signatures use source categorical descriptions, not optimized thresholds.
    out["trend_context"]=(
        out.get("origin_medium_wave_direction","").fillna("").astype(str)+" / "+
        out.get("origin_medium_wave_position","").fillna("").astype(str)
    ).str.strip(" /")
    out["micro_context"]=(
        out.get("origin_small_wave_direction","").fillna("").astype(str)+" / "+
        out.get("origin_small_wave_position","").fillna("").astype(str)
    ).str.strip(" /")
    out["structure_context"]=(
        out.get("origin_watermelon_state","").fillna("").astype(str)+" / "+
        out.get("origin_cloud_state","").fillna("").astype(str)
    ).str.strip(" /")
    out["phase_context"]=(
        out.get("origin_wave_state","").fillna("").astype(str)+" / "+
        out.get("origin_stage_status","").fillna("").astype(str)
    ).str.strip(" /")
    out["chart_context_signature"]=(
        out["trend_context"]+" || "+out["micro_context"]+" || "+out["phase_context"]
    )
    return out

def metric_row(g:pd.DataFrame)->Dict[str,Any]:
    rec={"events":int(len(g)),"sample_tier":sample_tier(len(g))}
    risk=pd.to_numeric(g.get("initial_risk_pct"),errors="coerce").dropna()
    rec["median_initial_risk_pct"]=float(risk.median()) if len(risk) else np.nan
    src_rr=pd.to_numeric(g.get("static_source_rr_ratio"),errors="coerce").dropna()
    rec["median_static_source_rr"]=float(src_rr.median()) if len(src_rr) else np.nan
    for h in HORIZONS:
        comp_col = g[f"origin_d{h}_complete"] if f"origin_d{h}_complete" in g.columns else pd.Series(0, index=g.index)
        mature=pd.to_numeric(comp_col,errors="coerce").fillna(0).eq(1)
        z=g[mature]
        rec[f"d{h}_mature"]=int(len(z))
        for m in ("close_r","mfe_r","mae_r"):
            vals=pd.to_numeric(z.get(f"d{h}_{m}"),errors="coerce").dropna()
            rec[f"d{h}_{m}_median"]=float(vals.median()) if len(vals) else np.nan
        # Realized path RR is explicit and keeps same-day ambiguity separate.
        for rr in TARGET_RS:
            s=z.get(f"d{h}_{rr}r_path",pd.Series(dtype=str)).astype(str)
            denom=int(s.isin(["TARGET_FIRST","STOP_FIRST","AMBIGUOUS_SAME_BAR","OPEN"]).sum())
            rec[f"d{h}_{rr}r_target_first_rate"]=float((s=="TARGET_FIRST").sum()/denom) if denom else np.nan
            rec[f"d{h}_{rr}r_stop_first_rate"]=float((s=="STOP_FIRST").sum()/denom) if denom else np.nan
            rec[f"d{h}_{rr}r_ambiguous_rate"]=float((s=="AMBIGUOUS_SAME_BAR").sum()/denom) if denom else np.nan
    return rec

def make_matrix(events:pd.DataFrame,dims:List[str])->pd.DataFrame:
    if events.empty:return pd.DataFrame()
    rows=[]
    for keys,g in events.groupby(dims,dropna=False):
        if not isinstance(keys,tuple):keys=(keys,)
        rec={"analyzer_id":ANALYZER_ID,"analyzer_revision":ANALYZER_REVISION,"exploratory_only":1}
        for d,v in zip(dims,keys):rec[d]=str(v)
        rec.update(metric_row(g))
        rows.append(rec)
    return pd.DataFrame(rows)

def exploratory_top(matrix:pd.DataFrame)->pd.DataFrame:
    if matrix.empty:return matrix
    d=matrix.copy()
    # No composite score. Surface only descriptive rows with enough sample,
    # sorted by realized 2R-before-stop, then lower stop-first, then MFE_R.
    d=d[pd.to_numeric(d["events"],errors="coerce").ge(10)].copy()
    if d.empty:return d
    for c in ("d5_2r_target_first_rate","d5_2r_stop_first_rate","d5_mfe_r_median","d5_mae_r_median"):
        d[c]=pd.to_numeric(d.get(c),errors="coerce")
    return d.sort_values(
        ["d5_2r_target_first_rate","d5_2r_stop_first_rate","d5_mfe_r_median"],
        ascending=[False,True,False],na_position="last"
    ).head(30)

def self_test():
    e=pd.DataFrame([{
        "origin_date":"2026-09-01","origin_price":100,"frozen_stop":95,"code":"005930",
        "origin_d5_complete":1,"origin_d5_close_ret_pct":5,"origin_d5_mfe_pct":12,"origin_d5_mae_pct":-3,
        "origin_search_pattern":"P","origin_medium_wave_direction":"완만상향","origin_medium_wave_position":"중단권",
        "origin_small_wave_direction":"강한상향","origin_small_wave_position":"중단권",
        "origin_wave_state":"1차눌림","origin_stage_status":"PASS_A",
        "origin_watermelon_state":"눌림수박","origin_cloud_state":"저항돌파",
    }])
    f=pd.DataFrame([
        {"date":"2026-09-02","open":100,"high":104,"low":98,"close":102},
        {"date":"2026-09-03","open":102,"high":111,"low":101,"close":108},
        {"date":"2026-09-04","open":108,"high":112,"low":106,"close":110},
        {"date":"2026-09-07","open":110,"high":113,"low":109,"close":111},
        {"date":"2026-09-08","open":111,"high":114,"low":110,"close":112},
    ])
    x=enrich(e,{"005930":f})
    assert abs(float(x.iloc[0]["d5_mfe_r"])-2.4)<1e-9
    assert x.iloc[0]["d5_2r_path"]=="TARGET_FIRST"
    m=make_matrix(x,["origin_search_pattern","trend_context"])
    assert len(m)==1
    print("REAL_FULL_CONTEXT_PATTERN_RISK_SELF_TEST PASS")
    return 0

def run(a):
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    events=readcsv(a.event_ledger,dtype={"code":str})
    if events.empty:
        print("REAL_FULL_CONTEXT_PATTERN_RISK no events")
        return 0
    needed=set(events["code"].map(code))
    frames={}
    try:
        frames,_,_=load_price_frames(Path(a.price_cache_dir),Path(a.amount_cache_dir),Path(a.asof_cache_dir),needed_codes=needed)
    except Exception as ex:
        print("price cache warning",type(ex).__name__,ex)
    x=enrich(events,frames)
    x.to_csv(out/"context_pattern_event_risk_ledger.csv",index=False,encoding="utf-8-sig")

    matrices={
        "pattern_x_trend":["origin_search_pattern","trend_context"],
        "pattern_x_micro":["origin_search_pattern","micro_context"],
        "pattern_x_wave":["origin_search_pattern","origin_wave_state"],
        "pattern_x_structure":["origin_search_pattern","structure_context"],
        "pattern_x_correction":["origin_search_pattern","origin_correction_label"],
        "wave_x_trend":["origin_wave_state","trend_context"],
        "watermelon_x_trend":["origin_watermelon_state","trend_context"],
        "cloud_x_trend":["origin_cloud_state","trend_context"],
        "action_x_trend":["origin_action","trend_context"],
        "full_context":["chart_context_signature"],
    }
    combined=[]
    for name,dims in matrices.items():
        m=make_matrix(x,dims)
        m.insert(0,"matrix",name)
        m.to_csv(out/f"context_matrix_{name}.csv",index=False,encoding="utf-8-sig")
        if len(m):combined.append(m)
    allm=pd.concat(combined,ignore_index=True) if combined else pd.DataFrame()
    allm.to_csv(out/"context_matrix_all.csv",index=False,encoding="utf-8-sig")
    top=exploratory_top(allm)
    top.to_csv(out/"context_pattern_exploratory_top.csv",index=False,encoding="utf-8-sig")

    report=[
        "🧠 [REAL_FULL CONTEXT × PATTERN × RISK R1]",
        f"events={len(x)} · matrices={len(matrices)}",
        "핵심 지표: 손절거리=1R · D+5 MFE_R/MAE_R · +1R/+2R/+3R이 손절보다 먼저 도달한 비율",
        "맥락축: 중파동 방향/위치 · 소파동 방향/위치 · 파동상태 · 수박/저항구름 · 조정률라벨 · 검색식",
        "표본등급: <5 TOO_SMALL / 5~9 EARLY / 10~29 DESCRIPTIVE / 30+ STRONGER_DESCRIPTIVE",
        "주의: exploratory_top은 발견용이며 선택/랭킹 authority가 아닙니다. 같은 표본으로 조건 수정 금지.",
    ]
    if len(top):
        report+=["","[D+5 2R-before-stop 탐색 상위 · n>=10]"]
        for _,r in top.head(10).iterrows():
            dims=[]
            for c in ["origin_search_pattern","trend_context","micro_context","origin_wave_state","structure_context","origin_correction_label","chart_context_signature"]:
                if c in r and text(r[c]):dims.append(f"{c}={r[c]}")
            report.append(
                f"- {' | '.join(dims[:3])} | n={int(r['events'])} | "
                f"2R-first={float(r['d5_2r_target_first_rate'])*100:.1f}% | "
                f"stop-first={float(r['d5_2r_stop_first_rate'])*100:.1f}% | "
                f"MFE={float(r['d5_mfe_r_median']):.2f}R | MAE={float(r['d5_mae_r_median']):.2f}R"
            )
    (out/"context_pattern_risk_report.txt").write_text("\n".join(report),encoding="utf-8")
    print("\n".join(report))
    return 0

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--event-ledger",default="reports/real_full_validation_backtest/analysis/pattern_backtest_event_ledger.csv")
    ap.add_argument("--output-dir",default="reports/real_full_validation_backtest/context_risk")
    ap.add_argument("--price-cache-dir",default="reports/.cache/v20_price_history")
    ap.add_argument("--amount-cache-dir",default="reports/.cache/v25_actual_amount_history")
    ap.add_argument("--asof-cache-dir",default="reports/.cache/v20_asof_snapshots")
    ap.add_argument("--self-test",action="store_true")
    a=ap.parse_args()
    return self_test() if a.self_test else run(a)
if __name__=="__main__":raise SystemExit(main())
