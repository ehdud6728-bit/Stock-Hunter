#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="CLOSEBET_SWING_HORIZON_OOS_R107_20260918"
DISCOVERY_END="2026-08-18"
OOS_START="2026-08-19"
OOS_END="2026-09-18"
HORIZONS=[5,10,15,20,22,30,40]
THRESHOLDS_UP=[3,5,10,15,20]
THRESHOLDS_DN=[-3,-5,-8,-10]

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
    q=q[q.code.ne("")&q.date.notna()].sort_values(["code","date"]).copy()
    return q

def first_hit(arr,cond):
    for i,v in enumerate(arr, start=1):
        try:
            if cond(float(v)): return i
        except: pass
    return np.nan

def event_path(row,g):
    dt=pd.Timestamp(row.signal_date).normalize()
    gg=g[g.date>=dt].sort_values("date").copy()
    # signal bar must exist
    pos=gg.index[gg.date.eq(dt)].tolist()
    if not pos:return None
    # re-slice from signal date with positional reset
    gg=gg[gg.date>=dt].reset_index(drop=True)
    if gg.empty:return None
    entry=float(row.get("entry_price",np.nan)) if "entry_price" in row else np.nan
    if not math.isfinite(entry):
        entry=float(gg.loc[0,"Close"]) if pd.notna(gg.loc[0,"Close"]) else np.nan
    if not math.isfinite(entry) or entry<=0:return None

    future=gg.iloc[1:].copy().reset_index(drop=True)
    rec={
        "signal_date":dt,"code":row.code,"name":row.get("name",""),
        "primary_formula":str(row.get("primary_formula","UNCLASSIFIED")),
        "core_pattern_label":str(row.get("core_pattern_label","UNKNOWN")),
        "shadow_tag_combo":str(row.get("shadow_tag_combo","")),
        "entry_price":entry,
        "available_future_trading_days":len(future),
    }

    # Full available-path threshold hit dates, capped naturally by available history.
    if len(future):
        high_ret=(future.High/entry-1)*100
        low_ret=(future.Low/entry-1)*100
        close_ret=(future.Close/entry-1)*100
        for t in THRESHOLDS_UP:
            rec[f"first_hit_plus{t}_day"]=first_hit(high_ret.values,lambda x,t=t:x>=t)
        for t in THRESHOLDS_DN:
            rec[f"first_hit_minus{abs(t)}_day"]=first_hit(low_ret.values,lambda x,t=t:x<=t)
    else:
        for t in THRESHOLDS_UP:rec[f"first_hit_plus{t}_day"]=np.nan
        for t in THRESHOLDS_DN:rec[f"first_hit_minus{abs(t)}_day"]=np.nan

    for h in HORIZONS:
        mature=len(future)>=h
        rec[f"D{h}_mature"]=bool(mature)
        if not mature:
            for k in ["close_ret","mfe","mae","peak_day","trough_day","peak_close_ret","peak_giveback_to_horizon"]:
                rec[f"D{h}_{k}"]=np.nan
            continue
        z=future.iloc[:h].copy()
        close_ret=(z.Close/entry-1)*100
        high_ret=(z.High/entry-1)*100
        low_ret=(z.Low/entry-1)*100
        peak_i=int(np.nanargmax(high_ret.values))
        trough_i=int(np.nanargmin(low_ret.values))
        peak=float(high_ret.iloc[peak_i])
        trough=float(low_ret.iloc[trough_i])
        end=float(close_ret.iloc[-1])
        rec[f"D{h}_close_ret"]=end
        rec[f"D{h}_mfe"]=peak
        rec[f"D{h}_mae"]=trough
        rec[f"D{h}_peak_day"]=peak_i+1
        rec[f"D{h}_trough_day"]=trough_i+1
        rec[f"D{h}_peak_close_ret"]=float(close_ret.max())
        rec[f"D{h}_peak_giveback_to_horizon"]=peak-end

    return rec

def classify_swing(r):
    # Descriptive only. Uses only mature horizons.
    d5=r.get("D5_close_ret",np.nan); d10=r.get("D10_close_ret",np.nan)
    d20=r.get("D20_close_ret",np.nan); d22=r.get("D22_close_ret",np.nan)
    mfe5=r.get("D5_mfe",np.nan); mfe10=r.get("D10_mfe",np.nan)
    mfe20=r.get("D20_mfe",np.nan); mfe22=r.get("D22_mfe",np.nan)
    mae5=r.get("D5_mae",np.nan); mae10=r.get("D10_mae",np.nan)
    final20=d22 if math.isfinite(float(d22)) else d20
    mfe_long=mfe22 if math.isfinite(float(mfe22)) else mfe20

    def fin(x): 
        try:return math.isfinite(float(x))
        except:return False

    if fin(mae5) and mae5<=-5 and fin(mfe_long) and mfe_long>=10:
        return "SHAKEOUT_SWING_WINNER"
    if fin(mfe5) and mfe5>=5 and fin(d5) and d5>0:
        return "EARLY_WINNER"
    if fin(mfe10) and mfe10>=10 and fin(d10) and d10>0:
        return "MEDIUM_SWING_WINNER"
    if fin(mfe_long) and mfe_long>=10 and fin(final20) and final20>0:
        return "MONTH_SWING_WINNER"
    if fin(mfe10) and mfe10>=5 and fin(d10) and d10<=0:
        return "GIVEBACK"
    if fin(mae5) and mae5<=-5 and (not fin(mfe10) or mfe10<5):
        return "EARLY_FAILURE"
    if fin(d20) and abs(d20)<3 and fin(mfe20) and mfe20<5 and fin(r.get("D20_mae",np.nan)) and r.get("D20_mae",0)>-5:
        return "DEAD_MONEY"
    return "UNRESOLVED_OR_OTHER"

def summary_by_horizon(ev):
    rows=[]
    scopes=[("ALL","ALL",ev)]
    for p,g in ev.groupby("primary_formula",dropna=False):
        scopes.append(("PATTERN",str(p),g))
    for h in HORIZONS:
        for st,label,g in scopes:
            m=g[g[f"D{h}_mature"].eq(True)].copy()
            if m.empty:
                rows.append({"scope_type":st,"scope":label,"horizon":h,"mature_n":0,
                             "status":"PENDING_OR_NO_MATURE"})
                continue
            c=pd.to_numeric(m[f"D{h}_close_ret"],errors="coerce")
            mfe=pd.to_numeric(m[f"D{h}_mfe"],errors="coerce")
            mae=pd.to_numeric(m[f"D{h}_mae"],errors="coerce")
            rows.append({
                "scope_type":st,"scope":label,"horizon":h,"mature_n":len(m),"status":"MATURE",
                "close_mean":c.mean(),"close_median":c.median(),
                "mfe_mean":mfe.mean(),"mfe_median":mfe.median(),
                "mae_mean":mae.mean(),"mae_median":mae.median(),
                "plus5_reach_rate":mfe.ge(5).mean()*100,
                "plus10_reach_rate":mfe.ge(10).mean()*100,
                "minus5_touch_rate":mae.le(-5).mean()*100,
                "positive_close_rate":c.gt(0).mean()*100,
                "peak_day_median":pd.to_numeric(m[f"D{h}_peak_day"],errors="coerce").median(),
                "giveback_median":pd.to_numeric(m[f"D{h}_peak_giveback_to_horizon"],errors="coerce").median(),
            })
    return pd.DataFrame(rows)

def maturity_table(ev):
    rows=[]
    for h in HORIZONS:
        m=ev[f"D{h}_mature"].eq(True)
        rows.append({
            "horizon":h,"oos_rows":len(ev),"mature_n":int(m.sum()),
            "mature_pct":float(m.mean()*100) if len(ev) else 0,
            "immature_n":int((~m).sum()),
            "interpretation":"OOS_EVALUABLE" if m.sum()>=30 else ("SMALL_MATURE_SAMPLE" if m.sum()>0 else "PENDING")
        })
    return pd.DataFrame(rows)

def threshold_table(ev):
    rows=[]
    for p,g in [("ALL",ev)]+[(str(k),v) for k,v in ev.groupby("primary_formula")]:
        for t in THRESHOLDS_UP:
            c=f"first_hit_plus{t}_day"; s=pd.to_numeric(g[c],errors="coerce")
            rows.append({"pattern":p,"threshold":f"+{t}%","n":len(g),"hit_n":int(s.notna().sum()),
                         "hit_rate_available_path":s.notna().mean()*100,
                         "first_hit_day_median":s.median()})
        for t in THRESHOLDS_DN:
            c=f"first_hit_minus{abs(t)}_day"; s=pd.to_numeric(g[c],errors="coerce")
            rows.append({"pattern":p,"threshold":f"{t}%","n":len(g),"hit_n":int(s.notna().sum()),
                         "hit_rate_available_path":s.notna().mean()*100,
                         "first_hit_day_median":s.median()})
    return pd.DataFrame(rows)

def tag_horizon(ev):
    tagcols=[c for c in ev.columns if c.startswith("TAG_")]
    rows=[]
    for p,g in ev.groupby("primary_formula"):
        for tag in tagcols:
            for val in [True,False]:
                z=g[g[tag].eq(val)]
                if z.empty:continue
                for h in [5,10,15,20,22]:
                    m=z[z[f"D{h}_mature"].eq(True)]
                    if len(m)<3:continue
                    c=pd.to_numeric(m[f"D{h}_close_ret"],errors="coerce")
                    mfe=pd.to_numeric(m[f"D{h}_mfe"],errors="coerce")
                    rows.append({"pattern":p,"tag":tag,"tag_value":val,"horizon":h,"mature_n":len(m),
                                 "close_mean":c.mean(),"close_median":c.median(),
                                 "plus5_reach_rate":mfe.ge(5).mean()*100,
                                 "plus10_reach_rate":mfe.ge(10).mean()*100})
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r105-root",default="r105_artifacts")
    ap.add_argument("--history-cache",default=".cache/closebet_structure_env_oos_r102/v49_76_research_raw_history.csv")
    ap.add_argument("--output-dir",default="reports/closebet_swing_horizon_oos_r107")
    ap.add_argument("--discovery-end",default=DISCOVERY_END)
    ap.add_argument("--oos-start",default=OOS_START)
    ap.add_argument("--oos-end",default=OOS_END)
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    ep=find(a.r105_root,"event_master_with_outcomes_and_shadow_tags.csv")
    hp=Path(a.history_cache)
    if ep is None or not hp.exists():
        raise SystemExit(f"R107_INPUT_MISSING event={ep} history={hp.exists()}")

    e=read_csv(ep)
    h=prep_hist(read_csv(hp))
    e["code"]=e.code.map(norm_code)
    e["signal_date"]=pd.to_datetime(e.signal_date,errors="coerce").dt.normalize()
    if "entry_price" not in e.columns:
        for c in ["signal_close","Close","close"]:
            if c in e.columns:
                e["entry_price"]=pd.to_numeric(e[c],errors="coerce");break
    osd=pd.Timestamp(a.oos_start);oed=pd.Timestamp(a.oos_end)
    oos=e[(e.signal_date>=osd)&(e.signal_date<=oed)].copy()

    hgroups={c:g.copy() for c,g in h.groupby("code")}
    rows=[]
    for _,r in oos.iterrows():
        g=hgroups.get(r.code)
        if g is None:continue
        rec=event_path(r,g)
        if rec is None:continue
        # preserve shadow tags for research-only horizon stratification
        for c in oos.columns:
            if c.startswith("TAG_"):
                rec[c]=r.get(c,False)
        rec["swing_class"]=classify_swing(rec)
        rows.append(rec)

    ev=pd.DataFrame(rows)
    if ev.empty:raise SystemExit("R107_NO_EVENTS")

    summary=summary_by_horizon(ev)
    maturity=maturity_table(ev)
    thresholds=threshold_table(ev)
    classes=(ev.groupby(["primary_formula","swing_class"],dropna=False).size()
             .rename("n").reset_index())
    tagperf=tag_horizon(ev)

    ev.to_csv(out/"oos_swing_event_master.csv",index=False,encoding="utf-8-sig")
    summary.to_csv(out/"oos_swing_horizon_summary.csv",index=False,encoding="utf-8-sig")
    maturity.to_csv(out/"oos_swing_maturity.csv",index=False,encoding="utf-8-sig")
    thresholds.to_csv(out/"oos_swing_threshold_first_hit.csv",index=False,encoding="utf-8-sig")
    classes.to_csv(out/"oos_swing_class_summary.csv",index=False,encoding="utf-8-sig")
    tagperf.to_csv(out/"oos_shadow_tag_by_swing_horizon.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REVISION,"status":"PASS","oos_start":a.oos_start,"oos_end":a.oos_end,
        "oos_input_rows":len(oos),"event_rows":len(ev),"horizons":HORIZONS,
        "maturity":{str(int(r.horizon)):int(r.mature_n) for _,r in maturity.iterrows()},
        "research_only":True,"production_eligible":False,
        "selection_logic_changed":False,"score_rank_changed":False,"order_logic_changed":False,
        "same_sample_retuning":False,"immature_rows_excluded_from_horizon_performance":True,
        "future_data_imputation":False,"swing_classes_descriptive_only":True,
        "notes":[
            "Each horizon uses only events with enough subsequent trading days.",
            "D30/D40 remain pending until enough OOS history exists.",
            "Threshold first-hit rates use currently available post-signal path and must be interpreted with maturity context.",
            "No scanner tuning or production filtering is performed."
        ]
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    lines=[
        "📈 [CLOSING BET · SWING HORIZON OOS R1.0.7]",
        f"status=PASS | OOS {a.oos_start}~{a.oos_end} | events={len(ev)}",
        "maturity: "+", ".join([f"D{int(r.horizon)}={int(r.mature_n)}" for _,r in maturity.iterrows()]),
        "",
        "핵심:",
        "- D+5/10/15/20/22/30/40을 거래일 기준으로 분리",
        "- 각 horizon은 충분한 미래 거래일이 실제 존재하는 사건만 평가",
        "- 종가수익률 + MFE/MAE + 최고점 도달일 + peak giveback + +5/+10 등 최초 도달일",
        "- EARLY / MEDIUM / MONTH / SHAKEOUT / GIVEBACK / EARLY_FAILURE / DEAD_MONEY 분류",
        "- D30/D40은 성숙 표본이 없으면 PENDING; 미래값을 0으로 채우지 않음",
        "- 검색식·점수·랭킹·후보제거·주문 변경 0",
    ]
    (out/"report.txt").write_text("\n".join(lines),encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    main()
