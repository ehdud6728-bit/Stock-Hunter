#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, re
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="CLOSEBET_MARKET_ENV_EXPECTATION_OOS_R111_20260919"
OOS_START="2026-08-19"
OOS_END="2026-09-18"

MACRO={
    "USDKRW":("KRW=X","GLOBAL_DAILY_PRIOR"),
    "KOSPI":("^KS11","KRX_CLOSE"),
    "KOSDAQ":("^KQ11","KRX_CLOSE"),
    "NASDAQ":("^IXIC","US_CLOSE_PRIOR"),
    "SOX":("^SOX","US_CLOSE_PRIOR"),
    "VIX":("^VIX","US_CLOSE_PRIOR"),
    "US10Y":("^TNX","US_CLOSE_PRIOR"),
    "WTI":("CL=F","GLOBAL_DAILY_PRIOR"),
    "DXY":("DX-Y.NYB","GLOBAL_DAILY_PRIOR"),
}

TEXT_FIELDS=[
    "material_hint","news_hint","reason","issue","theme","theme_name","tags",
    "sector_label","sector","sector_name","catalyst_state","context_alignment"
]

EXPECTATION_RULES=[
    ("GOV_POLICY",["정부","대통령","장관","정책","지원","규제","법안","국회","보조금","세제","정상회담","협상"]),
    ("COMPANY_GUIDANCE",["대표","ceo","회사","사측","가이던스","전망","목표","실적","매출","영업이익","생산능력","증설"]),
    ("ORDER_SUPPLY",["수주","계약","공급","납품","고객사","채택","양산","승인","허가","임상","수출"]),
    ("GLOBAL_LEADER_STATEMENT",["엔비디아","nvidia","테슬라","tesla","마이크로소프트","microsoft","구글","google","애플","apple","오픈ai","openai"]),
    ("BROKER_MEDIA_EXPECTATION",["증권사","리포트","목표가","상향","전망","언론","보도","단독"]),
    ("GEOPOLITICAL_POLICY",["관세","제재","전쟁","분쟁","휴전","수출규제","무역","중동","우크라","중국","미국"]),
    ("THEME_NARRATIVE",["ai","인공지능","로봇","반도체","hbm","2차전지","원전","전력","방산","바이오","재건"]),
]

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

def infer_signal_clock(row):
    for c in ["signal_timestamp","signal_datetime","observation_at","captured_at"]:
        if c in row.index and pd.notna(row[c]):
            ts=pd.to_datetime(row[c],errors="coerce")
            if pd.notna(ts):return ts,c
    blob=" ".join(str(row.get(c,"")) for c in [
        "lane","slot","signal_slot","run_slot","capture_slot","phase","final_stage","source_stage"
    ] if c in row.index).upper()
    dt=pd.Timestamp(row.signal_date).normalize()
    if any(k in blob for k in ["15:40","FINAL","AFTER_FINAL"]):
        return dt+pd.Timedelta(hours=15,minutes=40),"INFERRED_FINAL_1540"
    return dt+pd.Timedelta(hours=15,minutes=3),"CONSERVATIVE_1503"

def fetch_macro(start,end):
    import yfinance as yf
    st=(pd.Timestamp(start)-pd.Timedelta(days=45)).strftime("%Y-%m-%d")
    en=(pd.Timestamp(end)+pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    parts=[];audit=[]
    for name,(sym,rule) in MACRO.items():
        try:
            d=yf.download(sym,start=st,end=en,progress=False,auto_adjust=False,threads=False)
            if d is None or d.empty:
                audit.append({"macro":name,"symbol":sym,"status":"EMPTY","rows":0});continue
            if isinstance(d.columns,pd.MultiIndex):d.columns=[c[0] for c in d.columns]
            z=pd.DataFrame({
                "date":pd.to_datetime(d.index).tz_localize(None).normalize(),
                "macro":name,"close":pd.to_numeric(d.Close,errors="coerce").values,
                "rule":rule
            }).dropna(subset=["date","close"])
            z["ret1_pct"]=z.close.pct_change()*100
            z["ret5_pct"]=z.close.pct_change(5)*100
            parts.append(z)
            audit.append({"macro":name,"symbol":sym,"status":"OK","rows":len(z)})
        except Exception as e:
            audit.append({"macro":name,"symbol":sym,"status":f"ERROR:{type(e).__name__}","rows":0})
    return (pd.concat(parts,ignore_index=True) if parts else pd.DataFrame()),pd.DataFrame(audit)

def choose(g,ts,rule):
    d=ts.normalize()
    if rule=="KRX_CLOSE":
        allow_same=(ts.hour>15 or (ts.hour==15 and ts.minute>=30))
        cutoff=d if allow_same else d-pd.Timedelta(days=1)
    else:
        cutoff=d-pd.Timedelta(days=1)
    q=g[g.date<=cutoff].sort_values("date")
    return None if q.empty else q.iloc[-1]

def macro_snapshot(events,macro):
    groups={k:g for k,g in macro.groupby("macro")} if not macro.empty else {}
    rows=[]
    for _,r in events.iterrows():
        ts,src=infer_signal_clock(r)
        rec={"signal_date":r.signal_date,"code":r.code,
             "pit_signal_timestamp":ts,"pit_signal_clock_source":src}
        for name,(sym,rule) in MACRO.items():
            g=groups.get(name)
            mr=choose(g,ts,rule) if g is not None else None
            if mr is None:
                rec[f"pit_{name}_source_date"]=pd.NaT
            else:
                rec[f"pit_{name}_source_date"]=mr.date
                rec[f"pit_{name}_ret1_pct"]=mr.ret1_pct
                rec[f"pit_{name}_ret5_pct"]=mr.ret5_pct
                rec[f"pit_{name}_close"]=mr.close
        rows.append(rec)
    return pd.DataFrame(rows)

def build_text_source(source):
    q=source.copy()
    for c in TEXT_FIELDS:
        if c not in q:q[c]=""
    q["expectation_text"]=q.apply(
        lambda r:" | ".join(f"{c}:{str(r[c]).strip()}" for c in TEXT_FIELDS
                            if str(r[c]).strip() not in {"","nan","None","UNKNOWN","UNKNOWN_CURRENT_V4976_ARTIFACT"}),
        axis=1)
    q["expectation_text_present"]=q.expectation_text.str.len().gt(0)
    return q

def classify_expectation(text):
    s=str(text or "").lower()
    hits=[]
    for label,kws in EXPECTATION_RULES:
        if any(str(k).lower() in s for k in kws):hits.append(label)
    return "|".join(hits) if hits else ("TEXT_UNCLASSIFIED" if s.strip() else "UNKNOWN")

def classify_d5(row):
    oc=str(row.get("outcome_class",""))
    if oc in {"BIG_WIN","NORMAL_WIN","QUICK_WIN","SHAKEOUT_WIN"}:return "D5_WIN_FAMILY"
    if oc=="GIVEBACK":return "D5_GIVEBACK"
    if oc=="STOP_FIRST":return "D5_STOP_FIRST"
    if oc in {"WEAK_LOSS","SLOW_LOSS"}:return "D5_WEAK_LOSS"
    return "D5_OTHER"

def macro_contrast(df):
    feats=[c for c in df if c.startswith("pit_") and (c.endswith("_ret1_pct") or c.endswith("_ret5_pct"))]
    rows=[]
    for (p,b),g in df.groupby(["primary_formula","d5_behavior"]):
        for f in feats:
            s=pd.to_numeric(g[f],errors="coerce").dropna()
            if len(s)<2:continue
            rows.append({"pattern":p,"d5_behavior":b,"feature":f,
                         "events":len(g),"signal_dates":g.signal_date.nunique(),
                         "available_n":len(s),"median":s.median(),"mean":s.mean()})
    return pd.DataFrame(rows)

def expectation_summary(df):
    rows=[]
    for (p,cat,b),g in df.groupby(["primary_formula","expectation_category","d5_behavior"],dropna=False):
        rows.append({"pattern":p,"expectation_category":cat,"d5_behavior":b,
                     "events":len(g),"signal_dates":g.signal_date.nunique(),
                     "stocks":g.code.nunique()})
    return pd.DataFrame(rows)

def date_cluster_summary(df):
    rows=[]
    for (p,d),g in df.groupby(["primary_formula","signal_date"]):
        vc=g.d5_behavior.value_counts()
        rows.append({"pattern":p,"signal_date":d,"events":len(g),
                     "win":int(vc.get("D5_WIN_FAMILY",0)),
                     "stop_first":int(vc.get("D5_STOP_FIRST",0)),
                     "giveback":int(vc.get("D5_GIVEBACK",0)),
                     "weak_loss":int(vc.get("D5_WEAK_LOSS",0))})
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r105-root",default="r105_artifacts")
    ap.add_argument("--source-root",default="source_artifacts")
    ap.add_argument("--output-dir",default="reports/closebet_env_expectation_oos_r111")
    a=ap.parse_args()
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)

    p105=find(a.r105_root,"event_master_with_outcomes_and_shadow_tags.csv")
    src=None
    for n in ["v49_76_selected_enriched_outcomes.csv","v49_76_global_canonical.csv","v49_76_search_intent_enriched.csv"]:
        src=find(a.source_root,n)
        if src is not None:break
    if p105 is None or src is None:
        raise SystemExit(f"R111_INPUT_MISSING r105={p105} source={src}")

    e=read_csv(p105)
    s=read_csv(src)
    for q in [e,s]:
        q["code"]=q["code"].map(norm_code)
        q["signal_date"]=pd.to_datetime(q["signal_date"],errors="coerce").dt.normalize()

    e=e[(e.signal_date>=pd.Timestamp(OOS_START))&(e.signal_date<=pd.Timestamp(OOS_END))].copy()
    e["d5_behavior"]=e.apply(classify_d5,axis=1)

    st=build_text_source(s)
    st=st[["signal_date","code","expectation_text","expectation_text_present"]].drop_duplicates(["signal_date","code"])
    x=e.merge(st,on=["signal_date","code"],how="left")
    x["expectation_text"]=x.expectation_text.fillna("")
    x["expectation_text_present"]=x.expectation_text_present.fillna(False)
    x["expectation_category"]=x.expectation_text.map(classify_expectation)

    # PIT macro on expanded OOS event set.
    evtcols=[c for c in e.columns if c in ["signal_date","code","lane","slot","signal_slot","run_slot","capture_slot","phase","final_stage","source_stage","signal_timestamp","signal_datetime","observation_at","captured_at"]]
    ev=e[evtcols].drop_duplicates(["signal_date","code"]).copy()
    macro,audit=fetch_macro(OOS_START,OOS_END)
    ms=macro_snapshot(ev,macro)
    x=x.merge(ms,on=["signal_date","code"],how="left")

    # Point-in-time audit.
    pits=[]
    for name,(sym,rule) in MACRO.items():
        sc=f"pit_{name}_source_date"
        if sc not in x:continue
        for _,r in x[["signal_date","code","pit_signal_timestamp",sc]].dropna().iterrows():
            srcd=pd.Timestamp(r[sc]).normalize()
            ts=pd.Timestamp(r.pit_signal_timestamp)
            if rule=="KRX_CLOSE":
                allow=(ts.hour>15 or (ts.hour==15 and ts.minute>=30))
                valid=srcd<=ts.normalize() if allow else srcd<ts.normalize()
            else:
                valid=srcd<ts.normalize()
            pits.append({"signal_date":r.signal_date,"code":r.code,"macro":name,
                         "source_date":srcd,"pit_valid":bool(valid)})
    pita=pd.DataFrame(pits)
    pit_fail=int((~pita.pit_valid).sum()) if len(pita) else 0

    mc=macro_contrast(x)
    es=expectation_summary(x)
    ds=date_cluster_summary(x)

    # Coverage with independent date counts.
    cov=[]
    for c in ["expectation_text_present"]+[f"pit_{n}_ret5_pct" for n in MACRO]:
        if c not in x:continue
        if c=="expectation_text_present":
            mask=x[c].eq(True)
        else:
            mask=x[c].notna()
        cov.append({"feature":c,"events_available":int(mask.sum()),"events_total":len(x),
                    "coverage_pct":mask.mean()*100 if len(x) else 0,
                    "signal_dates_available":x.loc[mask,"signal_date"].nunique(),
                    "signal_dates_total":x.signal_date.nunique()})
    cov=pd.DataFrame(cov)

    x.to_csv(out/"oos_expanded_event_master.csv",index=False,encoding="utf-8-sig")
    ms.to_csv(out/"macro_pit_snapshot.csv",index=False,encoding="utf-8-sig")
    audit.to_csv(out/"macro_source_audit.csv",index=False,encoding="utf-8-sig")
    pita.to_csv(out/"pit_availability_audit.csv",index=False,encoding="utf-8-sig")
    mc.to_csv(out/"oos_macro_behavior_contrast.csv",index=False,encoding="utf-8-sig")
    es.to_csv(out/"oos_expectation_behavior_summary.csv",index=False,encoding="utf-8-sig")
    ds.to_csv(out/"oos_date_cluster_summary.csv",index=False,encoding="utf-8-sig")
    cov.to_csv(out/"coverage_events_and_signal_dates.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REVISION,"status":"PASS" if pit_fail==0 else "FAIL_CLOSED_PIT",
        "oos_rows":len(x),"signal_dates":int(x.signal_date.nunique()),
        "patterns":int(x.primary_formula.nunique()),
        "pit_audit_rows":len(pita),"pit_fail_rows":pit_fail,
        "expectation_text_rows":int(x.expectation_text_present.sum()),
        "expectation_text_coverage_pct":float(x.expectation_text_present.mean()*100 if len(x) else 0),
        "expectation_source":"SCANNER_PAYLOAD_ONLY_NO_RETROACTIVE_NEWS_BACKFILL",
        "research_only":True,"production_eligible":False,
        "external_context_shadow_only":True,"expectation_context_shadow_only":True,
        "same_sample_retuning":False,"selection_logic_changed":False,
        "score_rank_changed":False,"candidate_filter_created":False,"order_logic_changed":False,
        "independent_signal_date_count_reported":True,
        "no_hindsight_news_backfill":True,
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    if pit_fail:raise SystemExit(f"R111_PIT_FAIL {pit_fail}")

    report=[
        "🗣️ [CLOSING BET · MARKET ENV + EXPECTATION/CATALYST PIT OOS R1.0.11]",
        f"status=PASS | events={len(x)} | independent signal_dates={x.signal_date.nunique()} | patterns={x.primary_formula.nunique()}",
        f"expectation text coverage={meta['expectation_text_rows']}/{len(x)} ({meta['expectation_text_coverage_pct']:.1f}%)",
        "- D5 behavior로 OOS 전체를 확장해 시장환경 날짜 수 확보",
        "- macro 결과는 events와 signal_dates를 함께 표기",
        "- 기대감 텍스트는 당시 scanner payload에 존재한 필드만 사용",
        "- 과거 뉴스 사후검색으로 재료를 채우지 않음",
        "- expectation category는 SHADOW 설명용, 점수/랭킹/필터 아님",
        "- production/order 변경 0",
    ]
    (out/"report.txt").write_text("\n".join(report),encoding="utf-8")
    print("\n".join(report))

if __name__=="__main__":
    main()
