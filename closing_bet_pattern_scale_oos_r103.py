#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math, re
from pathlib import Path
from typing import Any, Callable
import numpy as np
import pandas as pd

REVISION="CLOSEBET_PATTERN_SCALE_OOS_R103_20260918"
RESEARCH_ONLY=True
DISCOVERY_END="2026-08-18"
OOS_START="2026-08-19"
OOS_END="2026-09-18"

POLICIES={
    "LUMP_100":[(0.0,1.0)],
    "SHALLOW_60_40":[(0.0,0.60),(-1.0,0.40)],
    "BALANCED_40_30_30":[(0.0,0.40),(-1.5,0.30),(-3.0,0.30)],
    "DEEP_30_30_40":[(0.0,0.30),(-2.0,0.30),(-4.0,0.40)],
    "STRUCTURE_40_30_30":[(0.0,0.40),("STRUCT1",0.30),("STRUCT2",0.30)],
}

MACRO_FEATURES=[
    "USDKRW_ret5_pct","VIX_ret5_pct","US10Y_ret5_pct","SOX_ret5_pct",
    "NASDAQ_ret5_pct","KOSPI_ret5_pct","KOSDAQ_ret5_pct","DXY_ret5_pct","WTI_ret5_pct"
]

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit(): s=s[:-2]
    if len(s)==7 and s.startswith("A"): s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def read_csv(p:Path):
    if not p or not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:pass
    return pd.DataFrame()

def find(root:Path,name:str):
    h=list(root.rglob(name)); h.sort(key=lambda p:(len(p.parts),str(p)))
    return h[0] if h else None

def truth(v):
    if isinstance(v,(bool,np.bool_)): return bool(v)
    s=str(v or "").strip().lower()
    if s in {"1","true","yes","y","on","t","pass","ok"}: return True
    if s in {"0","false","no","n","off","f","fail","none","nan",""}: return False
    return None

def num(v):
    try:
        x=float(v); return x if math.isfinite(x) else np.nan
    except:return np.nan

def known_bool(row,col):
    if col not in row.index or pd.isna(row.get(col)): return None
    return truth(row.get(col))

def numeric_check(row,col,fn):
    v=num(row.get(col))
    if not math.isfinite(v): return None
    try:return bool(fn(v))
    except:return None

def any_known(vals):
    z=[v for v in vals if v is not None]
    return (any(z) if z else None)

def not_bool(v):
    return None if v is None else (not v)

# Each checklist mirrors the CURRENT search_spec intent using already-emitted scanner diagnostics.
# These are audit semantics, not replacement predicates.
def semantic_checks(row, strategy):
    s=str(strategy or "").upper()
    checks=[]
    add=lambda key,val,desc: checks.append((key,val,desc))

    if s=="C":
        add("support",any_known([known_bool(row,"ymgp_support_hold"),known_bool(row,"ymgp_near_support")]),"눌림 지지/근접")
        add("volume_dry",known_bool(row,"ymgp_volume_dry"),"거래량 수축")
        add("restart",any_known([known_bool(row,"ymgp_reversal_signal"),known_bool(row,"ymgp_strong_reentry_signal"),known_bool(row,"ymgp_pullback_reentry")]),"재상승/재유입")
        add("ma_reclaim",known_bool(row,"ymgp_reclaim_ma5"),"단기 이평 회복")
        add("candle",known_bool(row,"ymgp_bullish_candle"),"종가/양봉 품질")
    elif s=="LP":
        add("gap_zone",known_bool(row,"lp_gap_zone_hold"),"리더갭 구간 지지")
        add("ma_hold",known_bool(row,"lp_ma_hold"),"MA5/10 지지")
        add("volume_cool",numeric_check(row,"lp_volume_vs_gap",lambda x:x<1.0),"갭봉 대비 거래량 냉각")
        add("normal_pullback",numeric_check(row,"lp_pullback_pct",lambda x:-12<=x<=0),"과도하지 않은 눌림")
        add("gap_quality",numeric_check(row,"lp_gap_close_loc_pct",lambda x:x>=50),"갭봉 종가 품질")
    elif s=="H":
        add("dry",any_known([known_bool(row,"high_dryup_volume_dry"),known_bool(row,"high_dryup_volume_dry_prev5"),known_bool(row,"high_dryup_volume_dry_vs_breakout")]),"거래량 건조")
        add("proper_pb",known_bool(row,"high_dryup_proper_pullback"),"정상 눌림")
        add("ma_support",any_known([known_bool(row,"high_dryup_ma_support"),known_bool(row,"high_dryup_ma5_close_hold")]),"이평 지지")
        add("zone_hold",known_bool(row,"high_dryup_zone_hold"),"고점/전고점 구간 유지")
        add("close_quality",known_bool(row,"high_dryup_entry_close_loc_ok"),"종가 품질")
    elif s=="G":
        add("gap_range",numeric_check(row,"gap_pct",lambda x:2<=x<=12),"2~12% 갭")
        add("vol50",numeric_check(row,"vol50_ratio",lambda x:x>=1.5),"Vol50 1.5배+")
        add("unfilled",known_bool(row,"gap_unfilled"),"갭 미메움")
        add("support",known_bool(row,"close_support"),"시가/갭 지지")
        add("breakout",any_known([known_bool(row,"leader_gap_new_high_120"),known_bool(row,"leader_gap_box_breakout")]),"박스/120일 돌파")
        add("not_overheat",not_bool(known_bool(row,"leader_gap_overheat_flag")),"과열 제외")
    elif s=="L":
        add("leader",any_known([known_bool(row,"leader_gap_core"),known_bool(row,"leader_gap_mega"),known_bool(row,"leader_gap_watch")]),"대형/주도 리더")
        add("support",known_bool(row,"close_support"),"갭/시가 지지")
        add("close_strength",truth(row.get("close_strength")) if "close_strength" in row.index else None,"종가 강도")
        add("breakout",any_known([known_bool(row,"leader_gap_new_high_120"),known_bool(row,"leader_gap_new_high_52w"),known_bool(row,"leader_gap_box_breakout")]),"고점/박스 돌파")
        add("not_overheat",not_bool(known_bool(row,"leader_gap_overheat_flag")),"과열 제외")
    elif s in {"I","IT"}:
        add("i_core",any_known([known_bool(row,"i_core"),known_bool(row,"i_core_main_candidate")]),"I-MAIN/코어")
        add("long_ma",numeric_check(row,"i_long_ma_dist_pct",lambda x:abs(x)<=20),"장기선 근접 구조")
        add("ma_reclaim",known_bool(row,"i_ma5_reclaim_long"),"단기선 재회복")
        add("monthly",known_bool(row,"i_monthly_ok"),"월봉 구조")
        add("volume_rebuild",any_known([known_bool(row,"i_monthly_vol_rebuild"),known_bool(row,"i_amount20_rebuild")]),"거래대금/거래량 재구축")
        if s=="IT": add("trigger",str(row.get("i_trigger_class","")).strip() not in {"","nan","None"} if "i_trigger_class" in row.index else None,"I 촉발 클래스")
    elif s in {"S","SLOCK"}:
        q=str(row.get("s_quality","")).upper()
        add("quality",q not in {"","NAN","NONE","FAIL","WEAK"} if "s_quality" in row.index else None,"고점권 품질")
        add("close_loc",numeric_check(row,"close_loc_pct",lambda x:x>=55),"종가 위치")
        add("volume",numeric_check(row,"vol_ratio",lambda x:x>=1.0),"거래량 재유입")
        add("not_wick",numeric_check(row,"wick_pct",lambda x:x<=35),"윗꼬리 제한")
        add("not_overextended",numeric_check(row,"runup20",lambda x:x<=35),"과열 제한")
        if s=="SLOCK": add("lock",str(row.get("s_type","")).upper() not in {"","NAN","NONE"} if "s_type" in row.index else None,"SLOCK 파생 구조")
    elif s=="A":
        add("breakout",any_known([known_bool(row,"a_reclaim_prev_high"),known_bool(row,"a_close_above_prev_high")]),"전고/돌파 구조")
        add("support",known_bool(row,"a_support_hold"),"지지 유지")
        add("money",known_bool(row,"a_amount_hold"),"거래대금 유지")
        add("bull",known_bool(row,"a_bullish_today"),"종가/양봉 강도")
        add("no_bear_expand",known_bool(row,"a_no_bear_expand"),"하락확대 없음")
    elif s=="B1":
        add("env_zone",numeric_check(row,"env20_pct",lambda x:x<=5),"엔벨 하단/재안착")
        add("close_loc",numeric_check(row,"close_loc_pct",lambda x:x>=50),"종가 위치")
        add("volume",numeric_check(row,"vol_ratio",lambda x:x>=0.8),"거래량")
        add("not_wick",numeric_check(row,"wick_pct",lambda x:x<=35),"윗꼬리 제한")
        add("ma20",numeric_check(row,"ma20_dist_pct",lambda x:abs(x)<=12),"추세/이평 거리")
    elif s=="B2":
        add("bb_zone",numeric_check(row,"bb40_pct",lambda x:math.isfinite(x)),"볼린저 구조 존재")
        add("volume",numeric_check(row,"vol_ratio",lambda x:x>=0.8),"거래량 확대")
        add("close_loc",numeric_check(row,"close_loc_pct",lambda x:x>=50),"종가 위치")
        add("not_wick",numeric_check(row,"wick_pct",lambda x:x<=35),"윗꼬리 제한")
        add("ma20",numeric_check(row,"ma20_dist_pct",lambda x:abs(x)<=15),"이평/추세")
    else:
        add("generic_close",numeric_check(row,"entry_close_loc_pct",lambda x:x>=50),"종가 품질")
        add("generic_volume",numeric_check(row,"entry_vol20_ratio",lambda x:x>=1),"거래량")
        add("generic_ma",numeric_check(row,"entry_ma20_dist_pct",lambda x:abs(x)<=15),"MA20 거리")

    return checks

def semantic_result(row):
    strategy=str(row.get("primary_formula",row.get("primary_strategy",row.get("strategy","")))).upper()
    checks=semantic_checks(row,strategy)
    known=[x for x in checks if x[1] is not None]
    passed=[x for x in known if x[1] is True]
    score=(len(passed)/len(known)) if known else np.nan
    if len(known)<2: label="UNKNOWN"
    elif score>=0.75: label="MATCH"
    elif score>=0.50: label="PARTIAL"
    else: label="MISMATCH"
    return strategy,len(known),len(passed),score,label,"|".join(k for k,v,d in known if v is False)

def freeze_profile_cuts(disc):
    rows=[]; cuts={}
    for pat,g in disc.groupby("primary_formula"):
        s=pd.to_numeric(g["semantic_score"],errors="coerce").dropna()
        if len(s)>=10 and s.nunique()>=3:
            q33=float(s.quantile(1/3)); q67=float(s.quantile(2/3))
            cuts[str(pat)]={"q33":q33,"q67":q67,"n":len(s)}
            rows.append({"pattern":pat,"q33":q33,"q67":q67,"n":len(s)})
    return cuts,pd.DataFrame(rows)

def profile_label(row,cuts):
    p=str(row["primary_formula"]); x=num(row["semantic_score"])
    if not math.isfinite(x) or p not in cuts:return "NEUTRAL_UNRESOLVED"
    c=cuts[p]
    if x>=c["q67"]:return "CONSERVATIVE"
    if x<c["q33"]:return "AGGRESSIVE"
    return "NEUTRAL"

def macro_cuts(disc):
    cuts={}; rows=[]
    for f in MACRO_FEATURES:
        if f not in disc:continue
        s=pd.to_numeric(disc[f],errors="coerce").dropna()
        if len(s)>=10:
            c={"q33":float(s.quantile(1/3)),"q67":float(s.quantile(2/3)),"n":len(s)}
            cuts[f]=c; rows.append({"feature":f,**c})
    return cuts,pd.DataFrame(rows)

def bucket(v,c):
    x=num(v)
    if not math.isfinite(x):return "UNKNOWN"
    return "LOW" if x<c["q33"] else ("MID" if x<=c["q67"] else "HIGH")

def prepare_history(h):
    q=h.copy()
    q["code"]=q["code"].map(norm_code)
    q["date"]=pd.to_datetime(q["date"],errors="coerce").dt.normalize()
    for c in ["Open","High","Low","Close","Volume","Amount"]:
        if c in q:q[c]=pd.to_numeric(q[c],errors="coerce")
    return q[q["code"].ne("")&q["date"].notna()].sort_values(["code","date"])

def future_path(h,code,signal_date,horizon=10):
    q=h[(h.code==code)&(h.date>signal_date)].sort_values("date").head(horizon).copy()
    return q

def structure_levels(row,entry):
    vals=[]
    def add(name,v):
        x=num(v)
        if math.isfinite(x) and 0<x<entry: vals.append((name,x))
    p=str(row.get("primary_formula","")).upper()
    if p=="C":
        add("YMGP_SUPPORT",row.get("ymgp_support_level")); add("MA5",row.get("MA5")); add("MA10",row.get("MA10"))
    elif p=="LP":
        add("GAP_LOW",row.get("lp_gap_low")); add("PREV_CLOSE",row.get("lp_prev_close")); add("MA5",row.get("MA5")); add("MA10",row.get("MA10"))
    elif p=="H":
        add("MA5",row.get("MA5")); add("MA10",row.get("MA10")); add("MA20",row.get("MA20"))
    elif p in {"I","IT"}:
        add("MA150",row.get("MA150")); add("MA200",row.get("MA200")); add("MA120",row.get("MA120"))
    elif p=="B1":
        add("ENV20_LOWER",row.get("lower20")); add("ENV40_LOWER",row.get("lower40")); add("MA20",row.get("MA20"))
    elif p=="B2":
        add("BB40_LOWER",row.get("bb40_lower")); add("MA20",row.get("MA20")); add("MA10",row.get("MA10"))
    elif p in {"A","G","L"}:
        add("PREV_HIGH",row.get("a_prev_high")); add("GAP_LOW",row.get("gap_low")); add("PREV_CLOSE",row.get("prev_close")); add("MA20",row.get("MA20"))
    else:
        add("MA5",row.get("MA5")); add("MA10",row.get("MA10")); add("MA20",row.get("MA20"))
    # closest supports first, unique, at least 0.25% below entry
    vals=sorted(vals,key=lambda z:z[1],reverse=True)
    out=[]
    for name,x in vals:
        if (entry/x-1)*100 < 0.25: continue
        if all(abs(x-y)>entry*0.002 for _,y in out): out.append((name,x))
        if len(out)>=2: break
    return out

def stop_price(row,entry):
    x=num(row.get("stoploss"))
    if math.isfinite(x) and 0<x<entry and (x/entry-1)*100>=-15:return x,"OFFICIAL"
    return entry*0.95,"FALLBACK_-5"

def policy_limits(row,policy,entry):
    legs=POLICIES[policy]; supports=structure_levels(row,entry)
    out=[]
    for level,w in legs:
        if level=="STRUCT1":
            if len(supports)>=1: out.append((supports[0][1],w,supports[0][0]))
        elif level=="STRUCT2":
            if len(supports)>=2: out.append((supports[1][1],w,supports[1][0]))
        else:
            out.append((entry*(1+float(level)/100),w,f"{level:+.1f}%"))
    return out

def simulate(row,h,policy):
    entry=num(row.get("entry_price",row.get("close")))
    if not math.isfinite(entry) or entry<=0:return None
    code=norm_code(row["code"]); sd=pd.Timestamp(row["signal_date"]).normalize()
    fut=future_path(h,code,sd,10)
    if fut.empty:return None
    stop,stop_src=stop_price(row,entry)
    legs=policy_limits(row,policy,entry)
    legs=[z for z in legs if z[0]>stop]
    if not legs:return None

    fills=[]; units=0.; capital=0.; stop_hit=False; stop_day=np.nan
    # signal close first leg is always filled
    px0,w0,label0=legs[0]; fills.append((0,entry,w0,label0)); units+=w0/entry; capital+=w0
    next_leg=1
    conflict=False
    for di,(_,d) in enumerate(fut.iterrows(),start=1):
        o,hig,lo,cl=[num(d.get(c)) for c in ["Open","High","Low","Close"]]
        if not all(math.isfinite(x) for x in [o,hig,lo,cl]):continue
        # gap below stop = stopped before any new add.
        if o<=stop:
            stop_hit=True; stop_day=di; break
        # fill all limits crossed above stop. Descending price reaches higher limits before stop.
        while next_leg<len(legs) and lo<=legs[next_leg][0]:
            lp,w,lab=legs[next_leg]
            fills.append((di,lp,w,lab)); units+=w/lp; capital+=w; next_leg+=1
        if lo<=stop:
            stop_hit=True; stop_day=di; break

    avg=capital/units if units>0 else np.nan
    last3=fut.iloc[min(2,len(fut)-1)] if len(fut) else None
    last5=fut.iloc[min(4,len(fut)-1)] if len(fut) else None
    last10=fut.iloc[min(9,len(fut)-1)] if len(fut) else None
    def ret_at(d):
        if d is None or not math.isfinite(avg):return np.nan
        c=num(d.get("Close")); return (c/avg-1)*100 if math.isfinite(c) else np.nan
    # after final fill, measure excursions to D10; if stopped, cap adverse at stop.
    final_fill_day=max(x[0] for x in fills)
    post=fut.iloc[max(0,final_fill_day-1):]
    mfe=max(((num(x)/avg-1)*100 for x in post["High"] if math.isfinite(num(x))),default=np.nan)
    mae=min(((num(x)/avg-1)*100 for x in post["Low"] if math.isfinite(num(x))),default=np.nan)
    hit3=bool((post["High"]>=avg*1.03).any()) if len(post) else False
    hit5=bool((post["High"]>=avg*1.05).any()) if len(post) else False
    breakeven=bool((post["High"]>=avg).any()) if len(post) else False
    invested=capital
    improvement=(1-avg/entry)*100 if math.isfinite(avg) else np.nan
    return {
        "signal_date":sd,"code":code,"name":row.get("name",""),"pattern":row.get("primary_formula",""),
        "semantic_label":row.get("semantic_label",""),"entry_profile":row.get("entry_profile",""),
        "policy":policy,"entry_price":entry,"weighted_avg_entry":avg,"avg_entry_improvement_pct":improvement,
        "planned_legs":len(legs),"filled_legs":len(fills),"invested_weight":invested,
        "deepest_fill_pct":min(((p/entry-1)*100 for _,p,_,_ in fills),default=0),
        "stop_price":stop,"stop_source":stop_src,"stop_hit":stop_hit,"stop_day":stop_day,
        "d3_ret_from_avg":ret_at(last3),"d5_ret_from_avg":ret_at(last5),"d10_ret_from_avg":ret_at(last10),
        "mfe_from_avg":mfe,"mae_from_avg":mae,"hit3_from_avg":hit3,"hit5_from_avg":hit5,
        "breakeven_after_final_fill":breakeven,
        "fill_ledger":"|".join(f"D{d}:{lab}@{p:.2f}({w:.2f})" for d,p,w,lab in fills),
        "research_only":True
    }

def perf(g,retcol="d5_ret_from_avg"):
    r=pd.to_numeric(g[retcol],errors="coerce").dropna()
    return {
        "n":len(g),"eval_n":len(r),
        "mean":r.mean() if len(r) else np.nan,"median":r.median() if len(r) else np.nan,
        "win_rate":(r>0).mean()*100 if len(r) else np.nan,
        "hit3_rate":g["hit3_from_avg"].mean()*100 if "hit3_from_avg" in g and len(g) else np.nan,
        "hit5_rate":g["hit5_from_avg"].mean()*100 if "hit5_from_avg" in g and len(g) else np.nan,
        "stop_rate":g["stop_hit"].mean()*100 if "stop_hit" in g and len(g) else np.nan,
        "avg_entry_improvement":pd.to_numeric(g["avg_entry_improvement_pct"],errors="coerce").mean() if len(g) else np.nan,
        "avg_invested_weight":pd.to_numeric(g["invested_weight"],errors="coerce").mean() if len(g) else np.nan,
        "mfe_median":pd.to_numeric(g["mfe_from_avg"],errors="coerce").median() if len(g) else np.nan,
        "mae_median":pd.to_numeric(g["mae_from_avg"],errors="coerce").median() if len(g) else np.nan,
    }

def group_perf(df,cols,label,retcol="d5_ret_from_avg"):
    rows=[]
    for keys,g in df.groupby(cols,dropna=False):
        if not isinstance(keys,tuple):keys=(keys,)
        z={"group_type":label,**{c:str(v) for c,v in zip(cols,keys)}}; z.update(perf(g,retcol)); rows.append(z)
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r102-root",default="r102_artifacts")
    ap.add_argument("--history-cache",default=".cache/closebet_structure_env_oos_r102/v49_76_research_raw_history.csv")
    ap.add_argument("--output-dir",default="reports/closebet_pattern_scale_oos_r103")
    ap.add_argument("--discovery-end",default=DISCOVERY_END)
    ap.add_argument("--oos-start",default=OOS_START)
    ap.add_argument("--oos-end",default=OOS_END)
    a=ap.parse_args()

    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    ep=find(Path(a.r102_root),"event_master_enriched.csv")
    hp=Path(a.history_cache)
    if ep is None or not hp.exists():
        meta={"revision":REVISION,"status":"FAIL_CLOSED","event_source":str(ep),"history_source":str(hp),
              "research_only":True,"production_eligible":False}
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        raise SystemExit(f"INPUT_MISSING event={ep} history_exists={hp.exists()}")

    e=read_csv(ep); h=prepare_history(read_csv(hp))
    e["code"]=e["code"].map(norm_code); e["signal_date"]=pd.to_datetime(e["signal_date"],errors="coerce").dt.normalize()
    if "primary_formula" not in e: e["primary_formula"]=e.get("primary_strategy",e.get("strategy","UNCLASSIFIED"))
    sem=e.apply(semantic_result,axis=1,result_type="expand")
    sem.columns=["semantic_pattern","semantic_known","semantic_pass","semantic_score","semantic_label","semantic_failed_checks"]
    e=pd.concat([e.reset_index(drop=True),sem.reset_index(drop=True)],axis=1)

    de=pd.Timestamp(a.discovery_end); osd=pd.Timestamp(a.oos_start); oed=pd.Timestamp(a.oos_end)
    disc=e[e.signal_date<=de].copy(); oos=e[(e.signal_date>=osd)&(e.signal_date<=oed)].copy()
    pcuts,pcutdf=freeze_profile_cuts(disc)
    e["entry_profile"]=e.apply(lambda r:profile_label(r,pcuts),axis=1)
    disc=e[e.signal_date<=de].copy(); oos=e[(e.signal_date>=osd)&(e.signal_date<=oed)].copy()

    mcuts,mcutdf=macro_cuts(disc)
    for f,c in mcuts.items():
        e[f+"_bucket"]=e[f].map(lambda v:bucket(v,c))
    oos=e[(e.signal_date>=osd)&(e.signal_date<=oed)].copy()

    semantic_summary=[]
    for (p,l),g in oos.groupby(["primary_formula","semantic_label"],dropna=False):
        r=pd.to_numeric(g["evaluation_ret"],errors="coerce").dropna()
        semantic_summary.append({"pattern":p,"semantic_label":l,"n":len(g),"eval_n":len(r),
                                 "d5_mean":r.mean() if len(r) else np.nan,"d5_median":r.median() if len(r) else np.nan,
                                 "win_rate":(r>0).mean()*100 if len(r) else np.nan,
                                 "semantic_score_mean":pd.to_numeric(g["semantic_score"],errors="coerce").mean()})
    semantic_summary=pd.DataFrame(semantic_summary)

    profile_summary=[]
    for (p,prof),g in oos.groupby(["primary_formula","entry_profile"],dropna=False):
        r=pd.to_numeric(g["evaluation_ret"],errors="coerce").dropna()
        profile_summary.append({"pattern":p,"entry_profile":prof,"n":len(g),"eval_n":len(r),
                                "d5_mean":r.mean() if len(r) else np.nan,"d5_median":r.median() if len(r) else np.nan,
                                "win_rate":(r>0).mean()*100 if len(r) else np.nan,
                                "mfe_median":pd.to_numeric(g["mfe"],errors="coerce").median(),
                                "mae_median":pd.to_numeric(g["mae"],errors="coerce").median()})
    profile_summary=pd.DataFrame(profile_summary)

    sims=[]
    for _,r in oos.iterrows():
        for p in POLICIES:
            z=simulate(r,h,p)
            if z is not None:sims.append(z)
    sims=pd.DataFrame(sims)

    scale_summary=group_perf(sims,["pattern","policy"],"PATTERN_X_SCALE") if len(sims) else pd.DataFrame()
    semantic_scale=group_perf(sims,["pattern","semantic_label","policy"],"PATTERN_X_SEMANTIC_X_SCALE") if len(sims) else pd.DataFrame()
    profile_scale=group_perf(sims,["pattern","entry_profile","policy"],"PATTERN_X_PROFILE_X_SCALE") if len(sims) else pd.DataFrame()

    envrows=[]
    for f,c in mcuts.items():
        bc=f+"_bucket"
        for (p,b),g in oos.groupby(["primary_formula",bc],dropna=False):
            r=pd.to_numeric(g["evaluation_ret"],errors="coerce").dropna()
            envrows.append({"pattern":p,"feature":f,"bucket":b,"n":len(g),"eval_n":len(r),
                            "d5_mean":r.mean() if len(r) else np.nan,"d5_median":r.median() if len(r) else np.nan,
                            "win_rate":(r>0).mean()*100 if len(r) else np.nan,
                            "q33":c["q33"],"q67":c["q67"]})
    env=pd.DataFrame(envrows)

    e.to_csv(out/"pattern_semantic_event_master.csv",index=False,encoding="utf-8-sig")
    semantic_summary.to_csv(out/"oos_pattern_semantic_summary.csv",index=False,encoding="utf-8-sig")
    pcutdf.to_csv(out/"discovery_frozen_confirmation_profile_bins.csv",index=False,encoding="utf-8-sig")
    profile_summary.to_csv(out/"oos_pattern_entry_profile_summary.csv",index=False,encoding="utf-8-sig")
    sims.to_csv(out/"oos_scale_in_event.csv",index=False,encoding="utf-8-sig")
    scale_summary.to_csv(out/"oos_pattern_scale_in_summary.csv",index=False,encoding="utf-8-sig")
    semantic_scale.to_csv(out/"oos_pattern_semantic_scale_in_summary.csv",index=False,encoding="utf-8-sig")
    profile_scale.to_csv(out/"oos_pattern_profile_scale_in_summary.csv",index=False,encoding="utf-8-sig")
    mcutdf.to_csv(out/"discovery_frozen_environment_bins.csv",index=False,encoding="utf-8-sig")
    env.to_csv(out/"oos_pattern_environment_summary.csv",index=False,encoding="utf-8-sig")

    criteria=[]
    for p in sorted(e["primary_formula"].dropna().astype(str).unique()):
        sample=e[e.primary_formula.astype(str).eq(p)].iloc[0]
        for k,v,d in semantic_checks(sample,p):
            criteria.append({"pattern":p,"check":k,"description":d,"role":"SEMANTIC_AUDIT_ONLY"})
    pd.DataFrame(criteria).to_csv(out/"pattern_semantic_checklist.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REVISION,"status":"PASS","discovery_end":a.discovery_end,"oos_start":a.oos_start,"oos_end":a.oos_end,
        "event_rows":len(e),"discovery_rows":len(disc),"oos_rows":len(oos),"scale_event_rows":len(sims),
        "patterns":int(e.primary_formula.nunique()),"profile_cut_patterns":len(pcuts),"environment_features":len(mcuts),
        "research_only":True,"production_eligible":False,"selection_logic_changed":False,"score_rank_changed":False,
        "order_logic_changed":False,"same_sample_retuning":False,
        "semantic_note":"MATCH/PARTIAL/MISMATCH audits emitted diagnostics against search_spec intent; not replacement predicates.",
        "profile_note":"CONSERVATIVE/NEUTRAL/AGGRESSIVE are frozen by discovery semantic-confirmation quantiles per pattern; not outcome tuned.",
        "scale_note":"Scale-in uses same fixed stop, never widens stop, and never adds below stop. Fixed-depth and structure-support policies are compared."
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")

    report=[
        "🧪 [CLOSING BET · PATTERN SEMANTIC × PROFILE × SCALE-IN × ENVIRONMENT OOS R1.0.3]",
        f"status=PASS | discovery<={a.discovery_end} n={len(disc)} | OOS {a.oos_start}~{a.oos_end} n={len(oos)}",
        f"patterns={meta['patterns']} | scale simulations={len(sims)} | macro frozen features={len(mcuts)}",
        "",
        "[1] Pattern Semantic Audit",
        "- 검색 라벨과 실제 검색식 의도 필드가 얼마나 일치하는지 MATCH/PARTIAL/MISMATCH로 분리.",
        "[2] Entry Confirmation Profile",
        "- 패턴별 discovery confirmation score q33/q67을 고정하여 AGGRESSIVE/NEUTRAL/CONSERVATIVE OOS 비교.",
        "[3] Scale-in Depth",
        "- LUMP / -1% / -1.5,-3% / -2,-4% / 패턴 구조지지선 매집 비교.",
        "- 손절선은 추가매수 후에도 절대 확대하지 않으며 손절 아래 추가매수 금지.",
        "[4] Environment",
        "- USDKRW/VIX/US10Y/SOX/NASDAQ/KOSPI/KOSDAQ/DXY/WTI는 discovery q33/q67 고정 후 OOS 교차분석.",
        "",
        "RESEARCH_ONLY. LIVE 검색식·점수·랭킹·주문 변경 0."
    ]
    (out/"report.txt").write_text("\n".join(report),encoding="utf-8")
    print("\n".join(report))

if __name__=="__main__":
    main()
