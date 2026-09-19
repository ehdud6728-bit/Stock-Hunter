#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, re
from pathlib import Path
import pandas as pd
import numpy as np

REVISION="CLOSEBET_EXPECTATION_CAPTURE_AUDIT_R112_20260919"
OOS_START="2026-08-19"
OOS_END="2026-09-18"

POSITIVE_TOKENS=[
    "catalyst","material","news","headline","reason","issue","event","disclosure",
    "공시","재료","뉴스","기사","이슈","발언","멘트","수주","계약","공급","가이던스",
    "정책","정부","대통령","장관","ceo","대표","speaker","statement","source","report",
    "theme","테마","keyword","키워드"
]
NEGATIVE_METADATA_TOKENS=[
    "sector","market","exchange","industry","universe","code","ticker","name","label",
    "state","mode","strategy","formula","score","rank","flag","bucket","group","type",
    "ret","return","price","close","open","high","low","volume","amount","ma","atr",
    "date","time","day","count","pct","ratio","source_date","source_stage"
]
LIKELY_TEXT_NAMES={
    "material_hint","news_hint","reason","issue","headline","news","catalyst","catalyst_text",
    "event_text","disclosure_text","report_text","theme","theme_name","tags","keywords"
}

def read_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:pass
    return pd.DataFrame()

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():s=s[:-2]
    if len(s)==7 and s.startswith("A"):s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def nonempty_series(s):
    q=s.astype(str).str.strip()
    bad={"","nan","none","null","unknown","n/a","na","-"}
    return ~q.str.lower().isin(bad)

def text_stats(df,col):
    s=df[col]
    mask=nonempty_series(s)
    vals=s[mask].astype(str).str.strip()
    return {
        "rows":len(df),
        "nonempty_rows":int(mask.sum()),
        "coverage_pct":float(mask.mean()*100 if len(df) else 0),
        "unique_nonempty":int(vals.nunique()),
        "median_len":float(vals.str.len().median()) if len(vals) else 0.0,
        "p90_len":float(vals.str.len().quantile(.90)) if len(vals) else 0.0,
        "max_len":int(vals.str.len().max()) if len(vals) else 0,
    }

def score_column_name(col):
    lc=str(col).lower()
    pos=[t for t in POSITIVE_TOKENS if t in lc]
    neg=[t for t in NEGATIVE_METADATA_TOKENS if t in lc]
    exact=lc in LIKELY_TEXT_NAMES
    return (3 if exact else 0)+len(pos)-len(neg),pos,neg

def classify_field(col,stats):
    lc=str(col).lower()
    score,_,_=score_column_name(col)
    if any(t in lc for t in ["sector","market_label","exchange","industry","universe"]):
        return "METADATA_ONLY"
    if stats["coverage_pct"]==0:return "EMPTY"
    if score>=3 and stats["median_len"]>=8:return "HIGH_CATALYST_CANDIDATE"
    if score>=1 and stats["median_len"]>=5:return "POSSIBLE_CATALYST_TEXT"
    if score>=1:return "KEYWORD_OR_TAG_CANDIDATE"
    return "NOT_CATALYST"

def find_event_file(root):
    for n in ["v49_76_selected_enriched_outcomes.csv","v49_76_global_canonical.csv",
              "v49_76_search_intent_enriched.csv","v49_76_global_raw.csv"]:
        xs=list(Path(root).rglob(n))
        if xs:
            xs.sort(key=lambda p:(len(p.parts),str(p)))
            return xs[0]
    return None

def find_r105(root):
    xs=list(Path(root).rglob("event_master_with_outcomes_and_shadow_tags.csv"))
    xs.sort(key=lambda p:(len(p.parts),str(p)))
    return xs[0] if xs else None

def classify_d5(row):
    oc=str(row.get("outcome_class",""))
    if oc in {"BIG_WIN","NORMAL_WIN","QUICK_WIN","SHAKEOUT_WIN"}:return "D5_WIN_FAMILY"
    if oc=="GIVEBACK":return "D5_GIVEBACK"
    if oc=="STOP_FIRST":return "D5_STOP_FIRST"
    if oc in {"WEAK_LOSS","SLOW_LOSS"}:return "D5_WEAK_LOSS"
    return "D5_OTHER"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-root",default="source_artifacts")
    ap.add_argument("--r105-root",default="r105_artifacts")
    ap.add_argument("--output-dir",default="reports/closebet_expectation_capture_audit_r112")
    a=ap.parse_args()
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)

    src=find_event_file(a.source_root)
    r105p=find_r105(a.r105_root)
    if src is None:raise SystemExit("R112_SOURCE_EVENT_FILE_MISSING")

    df=read_csv(src)
    if df.empty:raise SystemExit(f"R112_SOURCE_EMPTY {src}")
    if "code" in df:df["code"]=df["code"].map(norm_code)
    if "signal_date" in df:df["signal_date"]=pd.to_datetime(df["signal_date"],errors="coerce").dt.normalize()

    inv=[];examples=[]
    for c in df.columns:
        st=text_stats(df,c)
        score,pos,neg=score_column_name(c)
        cls=classify_field(c,st)
        inv.append({"column":c,"dtype":str(df[c].dtype),**st,"name_score":score,
                    "positive_name_tokens":"|".join(pos),"negative_name_tokens":"|".join(neg),
                    "field_class":cls})
        if cls in {"HIGH_CATALYST_CANDIDATE","POSSIBLE_CATALYST_TEXT","KEYWORD_OR_TAG_CANDIDATE"}:
            vals=df.loc[nonempty_series(df[c]),c].astype(str).str.strip().drop_duplicates().head(12)
            for i,v in enumerate(vals,1):
                examples.append({"column":c,"field_class":cls,"example_rank":i,"example_text":v[:1000]})

    invdf=pd.DataFrame(inv).sort_values(["field_class","name_score","coverage_pct","median_len"],
                                        ascending=[True,False,False,False])
    exdf=pd.DataFrame(examples)
    candidate_fields=invdf[invdf.field_class.isin(
        ["HIGH_CATALYST_CANDIDATE","POSSIBLE_CATALYST_TEXT"])]["column"].tolist()
    candidate_fields=[c for c in candidate_fields if not any(t in c.lower() for t in
        ["sector","market_label","market_state","context_state","source_date","source_stage",
         "universe","benchmark"])]

    event=df.copy()
    if "signal_date" in event:
        event=event[(event.signal_date>=pd.Timestamp(OOS_START))&
                    (event.signal_date<=pd.Timestamp(OOS_END))].copy()

    def combine_row(r):
        parts=[];used=[]
        for c in candidate_fields:
            if c not in r.index:continue
            v=str(r[c]).strip()
            if v.lower() in {"","nan","none","null","unknown","n/a","na","-"}:continue
            parts.append(f"{c}:{v}");used.append(c)
        return pd.Series({
            "captured_expectation_text":" | ".join(parts),
            "captured_expectation_fields":"|".join(used),
            "captured_expectation_field_count":len(used),
            "captured_expectation_present":len(used)>0,
        })

    if len(event):
        cap=event.apply(combine_row,axis=1)
        event=pd.concat([event,cap],axis=1)
    else:
        event["captured_expectation_present"]=False
        event["captured_expectation_field_count"]=0
        event["captured_expectation_text"]=""
        event["captured_expectation_fields"]=""

    joined=event
    if r105p is not None and {"code","signal_date"}.issubset(event.columns):
        r105=read_csv(r105p)
        r105["code"]=r105["code"].map(norm_code)
        r105["signal_date"]=pd.to_datetime(r105["signal_date"],errors="coerce").dt.normalize()
        r105=r105[(r105.signal_date>=pd.Timestamp(OOS_START))&
                  (r105.signal_date<=pd.Timestamp(OOS_END))].copy()
        r105["d5_behavior"]=r105.apply(classify_d5,axis=1)
        keep=[c for c in ["signal_date","code","primary_formula","outcome_class","d5_behavior"] if c in r105]
        joined=r105[keep].merge(
            event[["signal_date","code","captured_expectation_present",
                   "captured_expectation_field_count","captured_expectation_fields",
                   "captured_expectation_text"]].drop_duplicates(["signal_date","code"]),
            on=["signal_date","code"],how="left")
        joined["captured_expectation_present"]=joined["captured_expectation_present"].fillna(False)

    covrows=[]
    if len(joined) and "primary_formula" in joined:
        for (p,b),g in joined.groupby(["primary_formula","d5_behavior"],dropna=False):
            n=int(g.captured_expectation_present.sum())
            covrows.append({"pattern":p,"d5_behavior":b,"events":len(g),
                            "signal_dates":g.signal_date.nunique() if "signal_date" in g else np.nan,
                            "captured_expectation_events":n,
                            "capture_coverage_pct":n/len(g)*100 if len(g) else 0})
    covdf=pd.DataFrame(covrows)

    family_rows=[]
    for _,r in invdf.iterrows():
        c=str(r["column"]);lc=c.lower();family="OTHER"
        if any(k in lc for k in ["news","headline","기사","뉴스"]):family="NEWS"
        elif any(k in lc for k in ["material","재료"]):family="MATERIAL"
        elif any(k in lc for k in ["reason","issue","이슈"]):family="REASON_ISSUE"
        elif any(k in lc for k in ["catalyst","event"]):family="CATALYST_EVENT"
        elif any(k in lc for k in ["disclosure","공시"]):family="DISCLOSURE"
        elif any(k in lc for k in ["statement","speaker","발언","ceo","대표"]):family="STATEMENT"
        elif any(k in lc for k in ["theme","테마","tag","keyword"]):family="THEME_TAG"
        elif any(k in lc for k in ["sector","market","industry"]):family="METADATA"
        family_rows.append({"column":c,"family":family,"field_class":r["field_class"],
                            "coverage_pct":r["coverage_pct"],"unique_nonempty":r["unique_nonempty"],
                            "median_len":r["median_len"],"name_score":r["name_score"]})
    famdf=pd.DataFrame(family_rows)

    overall_capture=int(event.captured_expectation_present.sum()) if len(event) else 0
    event_rows=len(event)
    meta={
        "revision":REVISION,"status":"PASS","source_event_file":str(src),
        "source_columns":len(df.columns),"candidate_catalyst_fields":candidate_fields,
        "candidate_catalyst_field_count":len(candidate_fields),
        "oos_event_rows_in_source":event_rows,"captured_expectation_rows":overall_capture,
        "captured_expectation_coverage_pct":overall_capture/max(1,event_rows)*100,
        "sector_market_metadata_excluded":True,"retroactive_news_backfill_used":False,
        "research_only":True,"production_eligible":False,"selection_logic_changed":False,
        "score_rank_changed":False,"candidate_filter_created":False,"order_logic_changed":False,
        "same_sample_retuning":False,
    }

    invdf.to_csv(out/"all_column_expectation_inventory.csv",index=False,encoding="utf-8-sig")
    exdf.to_csv(out/"candidate_field_examples.csv",index=False,encoding="utf-8-sig")
    famdf.to_csv(out/"column_family_audit.csv",index=False,encoding="utf-8-sig")
    event.to_csv(out/"oos_source_expectation_capture.csv",index=False,encoding="utf-8-sig")
    joined.to_csv(out/"oos_expectation_capture_joined_r105.csv",index=False,encoding="utf-8-sig")
    covdf.to_csv(out/"oos_expectation_capture_by_pattern_behavior.csv",index=False,encoding="utf-8-sig")
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")

    lines=[
        "🔎 [CLOSING BET · EXPECTATION / CATALYST CAPTURE AUDIT R1.0.12]",
        f"status=PASS | source columns={len(df.columns)} | candidate catalyst fields={len(candidate_fields)}",
        f"OOS source events={event_rows} | genuine expectation capture={overall_capture} ({meta['captured_expectation_coverage_pct']:.1f}%)",
        "- sector/market/industry metadata는 기대감 텍스트에서 제외",
        "- source artifact 모든 컬럼 전수조사",
        "- candidate field별 coverage/unique/text length/example 저장",
        "- R105 결합은 coverage 확인용; score/rank/filter 생성 없음",
        "- 과거 뉴스 사후검색/backfill 없음",
        "- production/order 변경 0",
    ]
    (out/"report.txt").write_text("\n".join(lines),encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    main()
