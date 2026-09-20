#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math, os, re
from pathlib import Path
import numpy as np
import pandas as pd

VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R161_A_INTEGRATION_20260920"
RESEARCH_ONLY=True
ROUTE_PROX_MIN_N=3
ROUTE_PROX_LOW_CONF_N=10

ALIASES={
    "entry_close_loc_pct":["entry_close_loc_pct","close_loc_pct","close_location_pct"],
    "entry_vol20_ratio":["entry_vol20_ratio","vol20_ratio","volume20_ratio"],
    "entry_amount20_ratio":["entry_amount20_ratio","amount20_ratio"],
    "entry_ma20_dist_pct":["entry_ma20_dist_pct","ma20_dist_pct"],
    "entry_ma60_dist_pct":["entry_ma60_dist_pct","ma60_dist_pct"],
    "entry_ma224_dist_pct":["entry_ma224_dist_pct","ma224_dist_pct","close_vs_ma224_pct"],
    "entry_upper_wick_pct":["entry_upper_wick_pct","upper_wick_pct"],
    "entry_ret5_pct":["entry_ret5_pct","ret5_pct","entry_stock_ret_5d"],
}
FEATURE_LABELS={
    "entry_close_loc_pct":"CloseLoc",
    "entry_vol20_ratio":"Vol20",
    "entry_amount20_ratio":"Amount20",
    "entry_ma20_dist_pct":"MA20",
    "entry_ma60_dist_pct":"MA60",
    "entry_ma224_dist_pct":"MA224",
    "entry_upper_wick_pct":"UpperWick(body-relative)",
    "entry_ret5_pct":"Ret5",
}
ROUTE_NAMES={
    "EARLY_WIN_HELD":"Held",
    "EARLY_WIN_GIVEBACK":"Giveback",
    "SHAKEOUT_THEN_RECOVERY":"Shakeout→Recovery",
    "EARLY_STOP_SLOW_OR_NO_RECOVERY":"Slow/No recovery",
    "AMBIGUOUS_OR_PENDING":"Ambiguous/Pending",
}

def read_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,encoding=enc,low_memory=False)
        except Exception:pass
    return pd.DataFrame()

def first(row,names,default=np.nan):
    for c in names:
        if c in row.index:
            v=row[c]
            if pd.notna(v) and str(v).strip()!="": return v
    return default

def nval(row,key):
    v=first(row,ALIASES.get(key,[key]),np.nan)
    return pd.to_numeric(pd.Series([v]),errors="coerce").iloc[0]

def sval(row,names,default=""):
    v=first(row,names,default)
    return "" if pd.isna(v) else str(v).strip()

def pattern_of(r):
    for c in ["primary_formula","primary_strategy","mode","strategy","formula"]:
        if c in r.index and str(r[c]).strip():
            x=str(r[c]).strip().upper()
            for p in ["B1","B2","C","I","A"]:
                if x==p or x.startswith(p+"-") or x.startswith(p+"_"): return p
            return x
    return "UNKNOWN"

def code_of(r):
    s=sval(r,["code","Code","종목코드","ticker"],"")
    s=re.sub(r"\.0$","",s)
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def name_of(r):
    return sval(r,["name","Name","종목명","stock_name"],"")

def find_candidate(root):
    root=Path(root)
    for patt in ["live_shadow_execution_*.csv","live_shadow_hits_*.csv"]:
        xs=sorted(root.rglob(patt),key=lambda p:p.name,reverse=True)
        for p in xs:
            q=read_csv(p)
            if not q.empty:return p
    for name in [
        "v49_76_pit_marcap_current_selected_enriched.csv",
        "v49_76_selected_enriched_outcomes.csv",
        "v49_76_global_canonical.csv",
        "v49_76_search_intent_enriched.csv",
    ]:
        for p in sorted(root.rglob(name),key=lambda p:(len(p.parts),str(p))):
            q=read_csv(p)
            if not q.empty:return p
    return None

def find_file(root,name):
    xs=list(Path(root).rglob(name))
    return xs[0] if xs else None

def fmt(v,d=2,suffix=""):
    try:
        x=float(v)
        if not math.isfinite(x):return "-"
        return f"{x:.{d}f}{suffix}"
    except:return "-"

def current_line(r):
    bits=[]
    for key,label,suf in [
        ("entry_close_loc_pct","CloseLoc","%"),
        ("entry_vol20_ratio","Vol20","x"),
        ("entry_amount20_ratio","Amount20","x"),
        ("entry_ma20_dist_pct","MA20","%"),
        ("entry_ma60_dist_pct","MA60","%"),
        ("entry_ma224_dist_pct","MA224","%"),
        ("entry_upper_wick_pct","UpperWick(body-relative)","%"),
        ("entry_ret5_pct","Ret5","%"),
    ]:
        v=nval(r,key)
        if pd.notna(v):bits.append(f"{label} {fmt(v,2,suf)}")
    return " · ".join(bits) if bits else "현재 구조 필드 부족"

def normalize_core_evidence(df):
    return df.copy()

def normalize_a_evidence(df):
    q=df.copy()
    m={
        "entry_close_loc_pct_median":"close_loc_median",
        "entry_vol20_ratio_median":"vol20_median",
        "entry_amount20_ratio_median":"amount20_median",
        "entry_ma20_dist_pct_median":"ma20_median",
        "entry_ma60_dist_pct_median":"ma60_median",
        "entry_ma224_dist_pct_median":"ma224_median",
        "entry_upper_wick_pct_median":"wick_median",
        "entry_ret5_pct_median":"ret5_median",
    }
    for src,dst in m.items():
        if src in q.columns:q[dst]=q[src]
    return q

def normalize_a_map(df):
    if df.empty:return pd.DataFrame()
    r=df.iloc[0]
    counts=[
        ("EARLY_WIN_HELD",r.get("held_n",0)),
        ("EARLY_WIN_GIVEBACK",r.get("giveback_n",0)),
        ("SHAKEOUT_THEN_RECOVERY",r.get("shakeout_recovery_n",0)),
        ("EARLY_STOP_SLOW_OR_NO_RECOVERY",r.get("slow_no_recovery_n",0)),
        ("AMBIGUOUS_OR_PENDING",r.get("ambiguous_pending_n",0)),
    ]
    rows=[]
    for fam,n in counts:
        rows.append({
            "pattern":"A","response_family":fam,"pattern_n":r.get("pattern_n",np.nan),
            "family_n":n,"dominant_observed_routes":r.get("dominant_observed_routes",""),
            "response_watchlist":r.get("response_watchlist",""),
            "atr_evidence":r.get("atr_evidence","EXCLUDED_PROVENANCE_AMBIGUOUS"),
            "use":r.get("use","SHADOW_DESCRIPTION_ONLY"),
        })
    return pd.DataFrame(rows)

def route_counts(response_map,pat):
    q=response_map[response_map["pattern"].astype(str).eq(str(pat))]
    out=[];seen=set()
    for _,r in q.iterrows():
        fam=str(r.get("response_family",""))
        n=pd.to_numeric(pd.Series([r.get("family_n")]),errors="coerce").iloc[0]
        if fam and fam not in seen:
            out.append((fam,int(n) if pd.notna(n) else 0));seen.add(fam)
    return out

def response_row(response_map,pat):
    q=response_map[response_map["pattern"].astype(str).eq(str(pat))]
    return None if q.empty else q.iloc[0]

def routes_for(evidence,pat):
    q=evidence[evidence["pattern"].astype(str).eq(str(pat))]
    out={}
    for _,r in q.iterrows():
        fam=str(r.get("response_family",""))
        if fam:out[fam]=r.to_dict()
    return out

def nearest_feature_routes(r,routes):
    cols={
        "entry_close_loc_pct":"close_loc_median",
        "entry_vol20_ratio":"vol20_median",
        "entry_amount20_ratio":"amount20_median",
        "entry_ma20_dist_pct":"ma20_median",
        "entry_ma60_dist_pct":"ma60_median",
        "entry_ma224_dist_pct":"ma224_median",
        "entry_upper_wick_pct":"wick_median",
        "entry_ret5_pct":"ret5_median",
    }
    out=[]
    for key,mcol in cols.items():
        x=nval(r,key)
        if pd.isna(x):continue
        vals=[]
        for fam,rr in routes.items():
            m=pd.to_numeric(pd.Series([rr.get(mcol)]),errors="coerce").iloc[0]
            n=pd.to_numeric(pd.Series([rr.get("n")]),errors="coerce").iloc[0]
            if pd.isna(m) or pd.isna(n):continue
            n=int(n)
            if n<ROUTE_PROX_MIN_N:continue
            vals.append((abs(float(x)-float(m)),fam,float(m),n))
        if not vals:continue
        vals.sort(key=lambda z:z[0])
        _,fam,m,n=vals[0]
        conf="LOW CONFIDENCE" if n<ROUTE_PROX_LOW_CONF_N else "OBSERVED SAMPLE"
        out.append((FEATURE_LABELS[key],fam,n,conf))
    return out

def catalyst_text(r):
    fields=["headline","news_hint","material_hint","issue","news","catalyst_text","disclosure_text","statement"]
    vals=[]
    for c in fields:
        if c in r.index:
            v=str(r[c]).strip()
            if v and v.lower() not in {"nan","none","unknown","other_or_unknown",""}:
                vals.append(v)
    return " | ".join(vals[:2]) if vals else "UNKNOWN · forward capture 필요"

def macro_snapshot():
    try: import yfinance as yf
    except Exception:return {}
    mp={"KOSPI":"^KS11","KOSDAQ":"^KQ11","SOX":"^SOX","NASDAQ":"^IXIC","USDKRW":"KRW=X","VIX":"^VIX","US10Y":"^TNX","DXY":"DX-Y.NYB","WTI":"CL=F"}
    out={}
    for k,sym in mp.items():
        try:
            d=yf.download(sym,period="15d",progress=False,auto_adjust=False,threads=False)
            if d is None or d.empty:continue
            if isinstance(d.columns,pd.MultiIndex):d.columns=[c[0] for c in d.columns]
            c=pd.to_numeric(d["Close"],errors="coerce").dropna()
            if len(c)>=6:out[k]=(c.iloc[-1]/c.iloc[-6]-1)*100
        except Exception:pass
    return out

def macro_text(m):
    return " · ".join(f"{k} 5D {fmt(m[k],2,'%')}" for k in ["KOSPI","KOSDAQ","SOX","NASDAQ","USDKRW","VIX","US10Y","DXY","WTI"] if k in m) or "macro unavailable"

def board_for_row(r,evidence,response_map,macro):
    pat=pattern_of(r); name=name_of(r) or code_of(r)
    lines=[f"🔬 {name} · {pat}",f"CURRENT | {current_line(r)}"]
    rc=route_counts(response_map,pat)
    if rc:
        total=sum(n for _,n in rc)
        bits=[f"{ROUTE_NAMES.get(f,f)} {n}/{total} ({n/total*100:.1f}%)" for f,n in rc if n]
        lines.append("HISTORICAL PATH | "+" · ".join(bits))
        prox=nearest_feature_routes(r,routes_for(evidence,pat))
        if prox:
            pp=[]
            for label,fam,n,conf in prox[:6]:
                c=" · LOW CONFIDENCE" if conf=="LOW CONFIDENCE" else ""
                pp.append(f"{label}→{ROUTE_NAMES.get(fam,fam)} median(n={n}{c})")
            lines.append("FEATURE ROUTE PROXIMITY | "+" · ".join(pp))
            lines.append("※ n<3 비교 제외 · 3≤n<10 LOW CONFIDENCE · 종합 경로 판정/확률 아님")
        rr=response_row(response_map,pat)
        if rr is not None:
            dom=str(rr.get("dominant_observed_routes","")).strip()
            watch=str(rr.get("response_watchlist","")).strip()
            if dom:lines.append("DOMINANT OBSERVED ROUTES | "+dom)
            if watch:lines.append("RESPONSE WATCHLIST | "+watch)
    else:
        lines.append("HISTORICAL PATH | response map 미구축 패턴")
    lines += [
        "ATR | UNAVAILABLE · provenance 명확해질 때까지 제외",
        "MARKET | "+macro_text(macro),
        "CATALYST | "+catalyst_text(r),
    ]
    return "\n".join(lines)

def telegram_send(text):
    import requests
    token=(os.environ.get("CLOSING_BET_TOKEN") or "").strip()
    chat=(os.environ.get("CLOSING_BET_CHAT_ID") or "").strip()
    if not token or not chat:return {"status":"SKIPPED_NO_ROUTE"}
    parts=[];cur=""
    for block in text.split("\n\n"):
        add=block+"\n\n"
        if len(cur)+len(add)>3600 and cur:
            parts.append(cur.rstrip());cur=add
        else:cur+=add
    if cur.strip():parts.append(cur.rstrip())
    ok=0;errors=[]
    for part in parts:
        try:
            rr=requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             data={"chat_id":chat,"text":part,"disable_web_page_preview":"true"},timeout=20)
            if rr.ok:ok+=1
            else:errors.append(f"{rr.status_code}:{rr.text[:120]}")
        except Exception as e:errors.append(type(e).__name__)
    return {"status":"DELIVERED" if parts and ok==len(parts) else "PARTIAL_OR_FAILED",
            "parts":len(parts),"success":ok,"errors":errors}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-root",default="source_run")
    ap.add_argument("--r14-root",default="source_r14")
    ap.add_argument("--r16-a-root",default="source_r16_a")
    ap.add_argument("--output-dir",default="reports/live_shadow_board_r161")
    ap.add_argument("--top-n",type=int,default=10)
    ap.add_argument("--send-telegram",action="store_true")
    ap.add_argument("--source-run-id",default="")
    ap.add_argument("--r14-run-id",default="")
    ap.add_argument("--r16-a-run-id",default="")
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    src=find_candidate(a.source_root)
    if src is None:
        lane=""
        metas=sorted(Path(a.source_root).rglob("schedule_slot_authority_*.json"))
        if metas:
            try:lane=str(json.loads(metas[-1].read_text(encoding="utf-8")).get("effective_lane",""))
            except Exception:pass
        status="SKIP_NON_CANDIDATE_LANE" if ("RESTORE_ONLY" in lane or "INTRADAY" in lane) else "NO_LIVE_SIDECAR"
        meta={"version":VERSION,"status":status,"research_only":True,
              "source_run_id":a.source_run_id,"r14_run_id":a.r14_run_id,"r16_a_run_id":a.r16_a_run_id,
              "source_lane":lane,"production_changed":False,"score_rank_changed":False,
              "candidate_filter_created":False,"candidate_order_changed":False}
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        (out/"shadow_board.txt").write_text(status,encoding="utf-8")
        print(status);return

    p14e=find_file(a.r14_root,"response_map_evidence.csv")
    p14m=find_file(a.r14_root,"pattern_response_map.csv")
    p16e=find_file(a.r16_a_root,"a_response_map_evidence.csv")
    p16m=find_file(a.r16_a_root,"a_pattern_response_map.csv")
    if not all([p14e,p14m,p16e,p16m]):raise SystemExit("REFERENCE_ARTIFACT_MISSING")

    df=read_csv(src); core_e=read_csv(p14e); core_m=read_csv(p14m); a_e=read_csv(p16e); a_m=read_csv(p16m)
    if any(x.empty for x in [df,core_e,core_m,a_e,a_m]):raise SystemExit("EMPTY_SOURCE_OR_REFERENCE")

    evidence=pd.concat([normalize_core_evidence(core_e),normalize_a_evidence(a_e)],ignore_index=True,sort=False)
    response_map=pd.concat([core_m,normalize_a_map(a_m)],ignore_index=True,sort=False)

    if "signal_date" in df:
        dd=pd.to_datetime(df["signal_date"],errors="coerce")
        if dd.notna().any():
            df=df[dd.dt.normalize().eq(dd.max().normalize())].copy()
    df=df.head(max(1,a.top_n)).copy()  # preserve source order

    macro=macro_snapshot()
    boards=[board_for_row(r,evidence,response_map,macro) for _,r in df.iterrows()]
    header=[
        "🧪 [v49.76 LIVE SHADOW BOARD R1.6.1 · A INTEGRATED · RESEARCH ONLY]",
        f"source={src.name} · rows={len(df)} · source_run={a.source_run_id or '-'}",
        f"references: C/B1/B2/I=r14:{a.r14_run_id or '-'} · A=r16:{a.r16_a_run_id or '-'}",
        "※ 검색식/점수/랭킹/후보순서/주문 로직 변경 0",
        "※ FEATURE ROUTE PROXIMITY는 항목별 과거 중앙값 근접도일 뿐 종합 경로 점수/확률/판정이 아님",
    ]
    text="\n".join(header)+"\n\n"+"\n\n".join(boards)
    (out/"shadow_board.txt").write_text(text,encoding="utf-8")
    df.to_csv(out/"shadow_board_source_candidates.csv",index=False,encoding="utf-8-sig")

    structured=[]
    for i,(_,r) in enumerate(df.iterrows(),1):
        pat=pattern_of(r)
        rr=response_row(response_map,pat)
        rec={"source_order":i,"code":code_of(r),"name":name_of(r),"pattern":pat}
        for k in ALIASES:rec[k]=nval(r,k)
        rec["response_watchlist"]=str(rr.get("response_watchlist","")) if rr is not None else ""
        rec["dominant_observed_routes"]=str(rr.get("dominant_observed_routes","")) if rr is not None else ""
        rec["atr_response_evidence"]="EXCLUDED_PROVENANCE_AMBIGUOUS"
        structured.append(rec)
    pd.DataFrame(structured).to_csv(out/"shadow_board_structured.csv",index=False,encoding="utf-8-sig")

    tg={"status":"NOT_REQUESTED"}
    if a.send_telegram:tg=telegram_send(text)
    (out/"telegram_delivery.json").write_text(json.dumps(tg,ensure_ascii=False,indent=2),encoding="utf-8")

    meta={
        "version":VERSION,"status":"PASS","research_only":True,
        "source":str(src),"rows":len(df),"source_run_id":a.source_run_id,
        "r14_run_id":a.r14_run_id,"r16_a_run_id":a.r16_a_run_id,
        "a_pattern_response_map_integrated":True,
        "a_reference_only_from_r16":True,
        "candidate_membership_changed":False,"candidate_order_changed":False,
        "score_rank_changed":False,"candidate_filter_created":False,
        "order_logic_changed":False,"production_changed":False,"production_logic_changed":False,
        "aggregate_route_score_created":False,"aggregate_route_classification_created":False,
        "feature_level_route_proximity_only":True,
        "route_proximity_min_n":ROUTE_PROX_MIN_N,
        "route_proximity_low_confidence_below_n":ROUTE_PROX_LOW_CONF_N,
        "small_sample_route_excluded_from_proximity":True,
        "wick_display_label":"UpperWick(body-relative)",
        "atr_used_in_response_map":False,
        "same_sample_tuning":False,"new_threshold_optimization":False,
        "macro_semantics":"OBSERVER_TIME_CURRENT_CONTEXT_ONLY",
        "catalyst_semantics":"FORWARD_FIELDS_ONLY_IF_PRESENT",
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print(text)
    print("TELEGRAM",tg)
    print(json.dumps(meta,ensure_ascii=False,indent=2))

if __name__=="__main__":
    main()
