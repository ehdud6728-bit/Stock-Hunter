#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math, os, re
from pathlib import Path
import numpy as np
import pandas as pd

VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R15_RESPONSE_MAP_20260919"
RESEARCH_ONLY=True

ALIASES={
    "entry_close_loc_pct":["entry_close_loc_pct","close_loc_pct","close_location_pct"],
    "entry_vol20_ratio":["entry_vol20_ratio","vol20_ratio","volume20_ratio"],
    "entry_amount20_ratio":["entry_amount20_ratio","amount20_ratio"],
    "entry_ma20_dist_pct":["entry_ma20_dist_pct","ma20_dist_pct"],
    "entry_ma60_dist_pct":["entry_ma60_dist_pct","ma60_dist_pct"],
    "entry_ma224_dist_pct":["entry_ma224_dist_pct","ma224_dist_pct","close_vs_ma224_pct"],
    "entry_upper_wick_pct":["entry_upper_wick_pct","upper_wick_pct"],
    "entry_ret5_pct":["entry_ret5_pct","ret5_pct","entry_stock_ret_5d"],
    "entry_ret20_pct":["entry_ret20_pct","ret20_pct","entry_stock_ret_20d"],
    "entry_amount_b":["entry_amount_b","amount_b"],
}

FEATURE_LABELS={
    "entry_close_loc_pct":"CloseLoc",
    "entry_vol20_ratio":"Vol20",
    "entry_amount20_ratio":"Amount20",
    "entry_ma20_dist_pct":"MA20",
    "entry_ma60_dist_pct":"MA60",
    "entry_ma224_dist_pct":"MA224",
    "entry_upper_wick_pct":"Wick",
    "entry_ret5_pct":"Ret5",
}

def read_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,encoding=enc,low_memory=False)
        except Exception:pass
    return pd.DataFrame()

def find_candidate(root):
    root=Path(root)
    # Exact copy-only live sidecars first.
    ex=sorted(root.rglob("live_shadow_execution_*.csv"),key=lambda p:p.name,reverse=True)
    for p in ex:
        q=read_csv(p)
        if not q.empty:return p
    hs=sorted(root.rglob("live_shadow_hits_*.csv"),key=lambda p:p.name,reverse=True)
    for p in hs:
        q=read_csv(p)
        if not q.empty:return p

    preferred=[
        "v49_76_pit_marcap_current_selected_enriched.csv",
        "v49_76_selected_enriched_outcomes.csv",
        "v49_76_global_canonical.csv",
        "v49_76_search_intent_enriched.csv",
    ]
    for name in preferred:
        xs=list(root.rglob(name))
        if xs:
            xs.sort(key=lambda p:(len(p.parts),str(p)))
            for p in xs:
                q=read_csv(p)
                if not q.empty:return p
    xs=[p for p in root.rglob("*.csv") if any(k in p.name.lower() for k in ["selected","candidate","canonical"])]
    xs.sort(key=lambda p:(0 if "selected" in p.name.lower() else 1,len(p.parts),str(p)))
    return xs[0] if xs else None

def find_r14(root):
    root=Path(root)
    ev=list(root.rglob("response_map_evidence.csv"))
    rm=list(root.rglob("pattern_response_map.csv"))
    if not ev or not rm:return None,None
    return ev[0],rm[0]

def first(row,names,default=np.nan):
    for c in names:
        if c in row.index:
            v=row[c]
            if pd.notna(v) and str(v).strip()!="":
                return v
    return default

def nval(row,key):
    v=first(row,ALIASES.get(key,[key]),np.nan)
    return pd.to_numeric(pd.Series([v]),errors="coerce").iloc[0]

def sval(row,names,default=""):
    v=first(row,names,default)
    return "" if pd.isna(v) else str(v).strip()

def mode_of(r):
    for c in ["primary_formula","primary_strategy","mode","strategy","formula"]:
        if c in r.index and str(r[c]).strip():
            x=str(r[c]).strip().upper()
            for p in ["B1","B2","C","I"]:
                if x==p or x.startswith(p+"-") or x.startswith(p+"_"):
                    return p
            return x
    return "UNKNOWN"

def code_of(r):
    s=sval(r,["code","Code","종목코드","ticker"],"")
    s=re.sub(r"\.0$","",s)
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def name_of(r):
    return sval(r,["name","Name","종목명","stock_name"],"")

def fmt(v,d=2,suffix=""):
    try:
        x=float(v)
        if not math.isfinite(x): return "-"
        return f"{x:.{d}f}{suffix}"
    except Exception:
        return "-"

def current_line(r):
    bits=[]
    for key,label,suf in [
        ("entry_close_loc_pct","CloseLoc","%"),
        ("entry_vol20_ratio","Vol20","x"),
        ("entry_amount20_ratio","Amount20","x"),
        ("entry_ma20_dist_pct","MA20","%"),
        ("entry_ma60_dist_pct","MA60","%"),
        ("entry_ma224_dist_pct","MA224","%"),
        ("entry_upper_wick_pct","Wick","%"),
        ("entry_ret5_pct","Ret5","%"),
    ]:
        v=nval(r,key)
        if pd.notna(v):bits.append(f"{label} {fmt(v,2,suf)}")
    return " · ".join(bits) if bits else "현재 구조 필드 부족"

def route_table(evidence,pat):
    q=evidence[evidence["pattern"].astype(str).eq(str(pat))].copy()
    if q.empty:return {}
    out={}
    for _,r in q.iterrows():
        fam=str(r.get("response_family",""))
        if not fam:continue
        out[fam]={k:r.get(k,np.nan) for k in q.columns}
    return out

def response_row(response_map,pat):
    q=response_map[response_map["pattern"].astype(str).eq(str(pat))]
    if q.empty:return None
    return q.iloc[0]

def route_counts(response_map,pat):
    q=response_map[response_map["pattern"].astype(str).eq(str(pat))]
    if q.empty:return []
    seen=set(); out=[]
    for _,r in q.iterrows():
        fam=str(r.get("response_family",""))
        n=pd.to_numeric(pd.Series([r.get("family_n")]),errors="coerce").iloc[0]
        if fam and fam not in seen:
            out.append((fam,int(n) if pd.notna(n) else 0));seen.add(fam)
    return out

def nearest_route_per_feature(r, routes):
    """Feature-level descriptive proximity only. No aggregate route score."""
    if not routes:return []
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
    results=[]
    for fkey,medcol in cols.items():
        x=nval(r,fkey)
        if pd.isna(x):continue
        vals=[]
        for fam,rr in routes.items():
            m=pd.to_numeric(pd.Series([rr.get(medcol)]),errors="coerce").iloc[0]
            n=pd.to_numeric(pd.Series([rr.get("n")]),errors="coerce").iloc[0]
            if pd.notna(m):
                vals.append((abs(float(x)-float(m)),fam,float(m),int(n) if pd.notna(n) else 0))
        if not vals:continue
        vals.sort(key=lambda z:z[0])
        d,fam,m,n=vals[0]
        # No thresholded verdict; only nearest observed route median.
        results.append((FEATURE_LABELS.get(fkey,fkey),fam,m,n))
    return results

def macro_snapshot():
    try:
        import yfinance as yf
    except Exception:
        return {}
    mp={"USDKRW":"KRW=X","KOSPI":"^KS11","KOSDAQ":"^KQ11","NASDAQ":"^IXIC","SOX":"^SOX","VIX":"^VIX","US10Y":"^TNX","WTI":"CL=F","DXY":"DX-Y.NYB"}
    out={}
    for k,sym in mp.items():
        try:
            d=yf.download(sym,period="15d",progress=False,auto_adjust=False,threads=False)
            if d is None or d.empty:continue
            if isinstance(d.columns,pd.MultiIndex):d.columns=[c[0] for c in d.columns]
            c=pd.to_numeric(d["Close"],errors="coerce").dropna()
            if len(c)>=2:
                out[k]={"ret5":(c.iloc[-1]/c.iloc[-6]-1)*100 if len(c)>=6 else np.nan}
        except Exception:
            pass
    return out

def macro_text(m):
    bits=[]
    for k in ["KOSPI","KOSDAQ","SOX","NASDAQ","USDKRW","VIX","US10Y","DXY","WTI"]:
        if k in m:bits.append(f"{k} 5D {fmt(m[k].get('ret5'),2,'%')}")
    return " · ".join(bits) if bits else "macro unavailable"

def catalyst_text(r):
    fields=["headline","news_hint","material_hint","issue","news","catalyst_text","disclosure_text","statement"]
    parts=[]
    for c in fields:
        if c in r.index:
            v=str(r[c]).strip()
            if v and v.lower() not in {"nan","none","unknown","other_or_unknown",""}:
                parts.append(v)
    return " | ".join(parts[:2]) if parts else "UNKNOWN · forward capture 필요"

def board_for_row(r,evidence,response_map,macro):
    pat=mode_of(r)
    name=name_of(r) or code_of(r)
    lines=[f"🔬 {name} · {pat}", f"CURRENT | {current_line(r)}"]

    rc=route_counts(response_map,pat)
    if rc:
        total=sum(n for _,n in rc)
        nice=[]
        names={
            "EARLY_WIN_HELD":"Held",
            "EARLY_WIN_GIVEBACK":"Giveback",
            "SHAKEOUT_THEN_RECOVERY":"Shakeout→Recovery",
            "EARLY_STOP_SLOW_OR_NO_RECOVERY":"Slow/No recovery",
            "AMBIGUOUS_OR_PENDING":"Ambiguous/Pending",
        }
        for fam,n in rc:
            if n:
                pct=(n/total*100) if total else 0
                nice.append(f"{names.get(fam,fam)} {n}/{total} ({pct:.1f}%)")
        if nice:lines.append("HISTORICAL PATH | "+" · ".join(nice))

        routes=route_table(evidence,pat)
        prox=nearest_route_per_feature(r,routes)
        if prox:
            pp=[]
            nmap={
                "EARLY_WIN_HELD":"Held",
                "EARLY_WIN_GIVEBACK":"Giveback",
                "SHAKEOUT_THEN_RECOVERY":"Shakeout→Recovery",
                "EARLY_STOP_SLOW_OR_NO_RECOVERY":"Slow/No recovery",
            }
            for label,fam,m,n in prox:
                pp.append(f"{label}→{nmap.get(fam,fam)} median(n={n})")
            lines.append("FEATURE ROUTE PROXIMITY | "+" · ".join(pp[:6]))
            lines.append("※ 항목별 가장 가까운 과거 중앙값 표시일 뿐, 종합 점수/확률/판정이 아님")

        rr=response_row(response_map,pat)
        if rr is not None:
            w=str(rr.get("response_watchlist","")).strip()
            dom=str(rr.get("dominant_observed_routes","")).strip()
            if dom:lines.append("DOMINANT OBSERVED ROUTES | "+dom)
            if w:lines.append("RESPONSE WATCHLIST | "+w)
    else:
        lines.append("HISTORICAL PATH | frozen R1.4 response map 미구축 패턴")

    lines += [
        "ATR | UNAVAILABLE · source provenance 명확해질 때까지 Response Map에서 제외",
        "MARKET | "+macro_text(macro),
        "CATALYST | "+catalyst_text(r),
    ]
    return "\n".join(lines)

def telegram_send(text):
    import requests
    token=(os.environ.get("CLOSING_BET_TOKEN") or os.environ.get("TELEGRAM_CLOSEBET_TOKEN") or os.environ.get("TELEGRAM_TOKEN") or "").strip()
    chat=(os.environ.get("CLOSING_BET_CHAT_ID") or os.environ.get("TELEGRAM_CLOSEBET_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat:return {"status":"SKIPPED_NO_ROUTE"}
    parts=[];cur=""
    for block in text.split("\n\n"):
        add=block+"\n\n"
        if len(cur)+len(add)>3600 and cur:
            parts.append(cur.rstrip());cur=add
        else:cur+=add
    if cur.strip():parts.append(cur.rstrip())
    ok=0;errs=[]
    for p in parts:
        try:
            rr=requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             data={"chat_id":chat,"text":p,"disable_web_page_preview":"true"},timeout=20)
            if rr.ok:ok+=1
            else:errs.append(f"{rr.status_code}:{rr.text[:120]}")
        except Exception as e:errs.append(type(e).__name__)
    return {"status":"DELIVERED" if parts and ok==len(parts) else "PARTIAL_OR_FAILED",
            "parts":len(parts),"success":ok,"errors":errs}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-root",default="source_run")
    ap.add_argument("--r14-root",default="source_r14")
    ap.add_argument("--output-dir",default="reports/live_shadow_board_r15")
    ap.add_argument("--top-n",type=int,default=10)
    ap.add_argument("--send-telegram",action="store_true")
    ap.add_argument("--source-run-id",default="")
    ap.add_argument("--r14-run-id",default="")
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    src=find_candidate(a.source_root)
    evp,rmp=find_r14(a.r14_root)
    if src is None:
        lane=""
        metas=sorted(Path(a.source_root).rglob("schedule_slot_authority_*.json"))
        if metas:
            try:lane=str(json.loads(metas[-1].read_text(encoding="utf-8")).get("effective_lane",""))
            except Exception:pass
        status="SKIP_NON_CANDIDATE_LANE" if ("RESTORE_ONLY" in lane or "INTRADAY" in lane) else "NO_LIVE_SIDECAR"
        meta={"version":VERSION,"status":status,"research_only":True,
              "source_run_id":a.source_run_id,"r14_run_id":a.r14_run_id,
              "source_lane":lane,"production_changed":False,"score_rank_changed":False,
              "candidate_filter_created":False,"candidate_order_changed":False}
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        (out/"shadow_board.txt").write_text(status,encoding="utf-8")
        print(status);return
    if evp is None or rmp is None:
        raise SystemExit("R14_REFERENCE_ARTIFACT_MISSING")

    df=read_csv(src)
    evidence=read_csv(evp)
    response_map=read_csv(rmp)
    if df.empty or evidence.empty or response_map.empty:
        raise SystemExit("EMPTY_SOURCE_OR_R14")

    if "signal_date" in df:
        dd=pd.to_datetime(df["signal_date"],errors="coerce")
        if dd.notna().any():
            latest=dd.max().normalize()
            df=df[dd.dt.normalize().eq(latest)].copy()

    # Strictly preserve incoming candidate order.
    df=df.head(max(1,a.top_n)).copy()
    macro=macro_snapshot()

    boards=[board_for_row(r,evidence,response_map,macro) for _,r in df.iterrows()]
    header=[
        "🧪 [v49.76 LIVE SHADOW BOARD R1.5 · RESPONSE MAP · RESEARCH ONLY]",
        f"source={src.name} · rows={len(df)} · source_run={a.source_run_id or '-'} · r14={a.r14_run_id or '-'}",
        "※ 검색식/점수/랭킹/후보순서/주문 로직 변경 0",
        "※ Historical Path는 과거 OOS 523건의 기술통계이며 확률·추천점수·매매판정이 아님",
        "※ FEATURE ROUTE PROXIMITY는 특징별 중앙값 근접도만 표시하며 종합 경로 분류를 만들지 않음",
    ]
    text="\n".join(header)+"\n\n"+"\n\n".join(boards)
    (out/"shadow_board.txt").write_text(text,encoding="utf-8")
    df.to_csv(out/"shadow_board_source_candidates.csv",index=False,encoding="utf-8-sig")

    # Structured copy with observer-only fields; no sort/filter/rank.
    rows=[]
    for i,(_,r) in enumerate(df.iterrows(),1):
        pat=mode_of(r)
        rec={"source_order":i,"code":code_of(r),"name":name_of(r),"pattern":pat}
        for k in ALIASES:rec[k]=nval(r,k)
        rec["atr_response_evidence"]="EXCLUDED_PROVENANCE_AMBIGUOUS"
        rr=response_row(response_map,pat)
        rec["response_watchlist"]=str(rr.get("response_watchlist","")) if rr is not None else ""
        rec["dominant_observed_routes"]=str(rr.get("dominant_observed_routes","")) if rr is not None else ""
        rows.append(rec)
    pd.DataFrame(rows).to_csv(out/"shadow_board_structured.csv",index=False,encoding="utf-8-sig")

    tg={"status":"NOT_REQUESTED"}
    if a.send_telegram:tg=telegram_send(text)
    (out/"telegram_delivery.json").write_text(json.dumps(tg,ensure_ascii=False,indent=2),encoding="utf-8")

    meta={
        "version":VERSION,"status":"PASS","research_only":True,
        "source":str(src),"rows":len(df),"source_run_id":a.source_run_id,"r14_run_id":a.r14_run_id,
        "r14_evidence_source":str(evp),"r14_response_map_source":str(rmp),
        "candidate_membership_changed":False,"candidate_order_changed":False,
        "score_rank_changed":False,"candidate_filter_created":False,"order_logic_changed":False,
        "production_changed":False,"production_logic_changed":False,
        "aggregate_route_score_created":False,"aggregate_route_classification_created":False,
        "feature_level_route_proximity_only":True,
        "atr_used_in_response_map":False,
        "atr_semantics":"EXCLUDED_PROVENANCE_AMBIGUOUS",
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
