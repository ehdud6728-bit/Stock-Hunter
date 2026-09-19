#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math, os, re
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R1_20260919"
RESEARCH_ONLY=True

# Frozen descriptive references from completed R1062/R1081/R1091/R110/R1104 research.
# They are DISPLAY REFERENCES only. They never alter candidate membership, score, rank, filter, or order.
REF = {
    "C":{
        "winner_n":3, "comparison":"SUSTAINED vs GIVEBACK",
        "winner":{"entry_close_loc_pct":76.5,"entry_vol20_ratio":6.07,"entry_amount20_ratio":7.14,
                  "entry_atr_pct":6.58,"entry_ma20_dist_pct":16.0,"entry_ma60_dist_pct":27.7,
                  "entry_ma224_dist_pct":12.3,"entry_upper_wick_pct":23.5,"entry_ret5_pct":11.3},
        "risk":{"entry_close_loc_pct":57.4,"entry_vol20_ratio":0.83,"entry_amount20_ratio":0.99,
                "entry_atr_pct":7.55,"entry_ma20_dist_pct":7.35,"entry_ma60_dist_pct":17.8,
                "entry_ma224_dist_pct":7.2,"entry_upper_wick_pct":36.4,"entry_ret5_pct":-0.7},
        "characteristics":[
            "Sustained 표본에서 유동성 재유입·강한 종가·작은 wick이 더 두드러짐",
            "Giveback 표본은 종가 위치가 약하고 변동성이 더 큰 경우가 관찰됨",
            "상승 파동이 빠른 대신 이후 이익 반납도 컸던 표본이 존재"
        ],
        "watch":[
            "거래대금/거래량 재유입이 유지되는지",
            "종가 위치가 무너지거나 윗꼬리가 급증하는지",
            "빠른 상승 뒤 giveback이 커지는지",
            "15:03→15:40에서 구조와 유동성이 강화되는지"
        ],
        "exit":"D20 성숙표본 n=13 · peak day 중앙값 D7 · giveback 중앙값 약 16.1%p"
    },
    "B1":{
        "winner_n":9, "comparison":"SUSTAINED vs FAILED",
        "winner":{"entry_vol20_ratio":1.66,"entry_amount20_ratio":1.44,"entry_atr_pct":6.28,
                  "entry_ma60_dist_pct":-16.8,"entry_ma224_dist_pct":-17.1,"entry_ret5_pct":-11.8},
        "risk":{"entry_vol20_ratio":1.61,"entry_amount20_ratio":1.40,"entry_atr_pct":8.42,
                "entry_ma60_dist_pct":-23.1,"entry_ma224_dist_pct":-32.8,"entry_ret5_pct":-15.9},
        "characteristics":[
            "Sustained 표본은 실패군보다 ATR이 낮고 장기이평 훼손이 상대적으로 작았음",
            "단순히 많이 떨어진 후보보다 '복구 가능한 눌림'인지가 중요하게 보였음",
            "거래량 자체보다 구조 손상 정도가 더 구분력 있게 관찰됨"
        ],
        "watch":[
            "ATR 과대 여부",
            "MA60/224에서 지나치게 멀어진 깊은 훼손 여부",
            "유동성 재진입과 종가 회복이 실제로 동반되는지",
            "싸다는 이유만으로 구조 손상을 무시하지 않기"
        ],
        "exit":"D20 성숙표본 n=14 · peak day 중앙값 D7.5 · 중간 구간 지속성이 상대적으로 관찰됨"
    },
    "B2":{
        "winner_n":3, "comparison":"SUSTAINED vs GIVEBACK",
        "winner":{"entry_vol20_ratio":1.48,"entry_amount20_ratio":1.47,
                  "entry_ma60_dist_pct":-7.5,"entry_ma224_dist_pct":-6.3},
        "risk":{"entry_vol20_ratio":0.98,"entry_amount20_ratio":0.93,
                "entry_ma60_dist_pct":-12.1,"entry_ma224_dist_pct":-10.1},
        "characteristics":[
            "작은 표본에서 Sustained 쪽은 거래량/거래대금 재유입이 더 강했음",
            "장기이평 훼손이 상대적으로 덜한 쪽이 양호하게 관찰됨",
            "현재 표본이 작아 방향성 확인 단계"
        ],
        "watch":[
            "거래량/거래대금이 기준 이상으로 재유입되는지",
            "MA60/224 훼손 확대 여부",
            "반등 직후 이익 반납이 커지는지"
        ],
        "exit":"D20 성숙표본 n=10 · peak day 중앙값 D7.5 · 표본 작음"
    },
    "I":{
        "winner_n":8, "comparison":"SUSTAINED vs GIVEBACK/FAILED",
        "winner":{"entry_close_loc_pct":85.3,"entry_vol20_ratio":1.06,"entry_amount20_ratio":1.09,
                  "entry_atr_pct":4.01,"entry_ma20_dist_pct":0.17,"entry_ma60_dist_pct":2.22,
                  "entry_ma224_dist_pct":17.0,"entry_upper_wick_pct":14.7,"entry_ret5_pct":-0.14},
        "risk":{"entry_close_loc_pct":72.6,"entry_vol20_ratio":0.61,"entry_amount20_ratio":0.61,
                "entry_atr_pct":4.89,"entry_ma20_dist_pct":2.67,"entry_ma60_dist_pct":0.06,
                "entry_ma224_dist_pct":11.0,"entry_upper_wick_pct":25.7,"entry_ret5_pct":-1.55},
        "giveback":{"entry_close_loc_pct":75.5,"entry_vol20_ratio":1.47,"entry_amount20_ratio":1.54,
                    "entry_atr_pct":6.11,"entry_ma20_dist_pct":4.09,"entry_ma60_dist_pct":4.26,
                    "entry_ma224_dist_pct":21.9,"entry_upper_wick_pct":14.5,"entry_ret5_pct":1.42},
        "characteristics":[
            "Sustained 표본은 적당한 유동성·낮은 ATR·MA20 근처·강한 종가가 특징",
            "Failed 표본은 유동성 부족과 큰 wick이 상대적으로 두드러짐",
            "Giveback 표본은 유동성과 이격이 더 강해 과열된 반등 형태가 관찰됨"
        ],
        "watch":[
            "유동성이 너무 약하지도, 과도하게 폭발하지도 않는지",
            "MA20 부근의 조용한 재시동인지",
            "ATR 급증과 과열 반등 여부",
            "종가 강도가 유지되고 wick이 커지지 않는지"
        ],
        "exit":"D20 성숙표본 n=18 · peak day 중앙값 D9 · 빠른 MA5 대응은 정상 흔들림까지 자를 가능성 관찰"
    }
}

ALIASES={
    "entry_close_loc_pct":["entry_close_loc_pct","close_loc_pct","close_location_pct"],
    "entry_vol20_ratio":["entry_vol20_ratio","vol20_ratio","volume20_ratio"],
    "entry_amount20_ratio":["entry_amount20_ratio","amount20_ratio"],
    "entry_atr_pct":["entry_atr_pct","atr_pct","atr14_pct"],
    "entry_ma20_dist_pct":["entry_ma20_dist_pct","ma20_dist_pct"],
    "entry_ma60_dist_pct":["entry_ma60_dist_pct","ma60_dist_pct"],
    "entry_ma224_dist_pct":["entry_ma224_dist_pct","ma224_dist_pct","close_vs_ma224_pct"],
    "entry_upper_wick_pct":["entry_upper_wick_pct","upper_wick_pct"],
    "entry_ret5_pct":["entry_ret5_pct","ret5_pct","entry_stock_ret_5d"],
    "entry_ret20_pct":["entry_ret20_pct","ret20_pct","entry_stock_ret_20d"],
    "entry_amount_b":["entry_amount_b","amount_b"],
}

def read_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:pass
    return pd.DataFrame()

def find_candidate(root):
    root=Path(root)
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
            return xs[0]
    # fallback: any current/selected candidate-like CSV
    xs=[p for p in root.rglob("*.csv") if any(k in p.name.lower() for k in ["selected","candidate","canonical"])]
    xs.sort(key=lambda p:(0 if "selected" in p.name.lower() else 1,len(p.parts),str(p)))
    return xs[0] if xs else None

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

def name_of(r):
    return sval(r,["name","Name","종목명","stock_name"],"")

def code_of(r):
    s=sval(r,["code","Code","종목코드","ticker"],"")
    s=re.sub(r"\.0$","",s)
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def fmt(v,d=2,suffix=""):
    try:
        x=float(v)
        if not math.isfinite(x):return "-"
        return f"{x:.{d}f}{suffix}"
    except:return "-"

def match_direction(cur,winner,risk,key):
    if not (pd.notna(cur) and key in winner and key in risk):return "?"
    w=float(winner[key]); b=float(risk[key]); x=float(cur)
    if abs(w-b)<1e-12:return "≈"
    # closer in absolute distance to descriptive group median
    dw=abs(x-w); db=abs(x-b)
    if dw < db*0.75:return "✓ winner-like"
    if db < dw*0.75:return "⚠ risk-like"
    return "△ mixed"

def catalyst_text(r):
    # Strict genuine-catalyst fields only; R112 showed current historical artifact has almost no genuine coverage.
    fields=["headline","news_hint","material_hint","issue","news","catalyst_text","disclosure_text","statement"]
    parts=[]
    for c in fields:
        if c in r.index:
            v=str(r[c]).strip()
            if v and v.lower() not in {"nan","none","unknown","other_or_unknown",""}:
                parts.append(v)
    return " | ".join(parts[:2]) if parts else "UNKNOWN · forward capture 필요"

def current_lines(r,p):
    fields=[
        ("CloseLoc","entry_close_loc_pct","%"),
        ("Vol20","entry_vol20_ratio","x"),
        ("Amount20","entry_amount20_ratio","x"),
        ("ATR","entry_atr_pct","%"),
        ("MA20","entry_ma20_dist_pct","%"),
        ("MA60","entry_ma60_dist_pct","%"),
        ("MA224","entry_ma224_dist_pct","%"),
        ("UpperWick","entry_upper_wick_pct","%"),
        ("Ret5","entry_ret5_pct","%"),
    ]
    out=[]
    for label,key,suf in fields:
        v=nval(r,key)
        if pd.notna(v):out.append(f"{label} {fmt(v,2,suf)}")
    return " · ".join(out) if out else "현재 구조 필드 부족"

def match_lines(r,p):
    ref=REF.get(p)
    if not ref:return ["해당 패턴의 frozen Winner reference 아직 없음"]
    w=ref["winner"]; risk=ref["risk"]
    labels={
        "entry_close_loc_pct":"종가위치","entry_vol20_ratio":"거래량",
        "entry_amount20_ratio":"거래대금","entry_atr_pct":"ATR",
        "entry_ma20_dist_pct":"MA20","entry_ma60_dist_pct":"MA60",
        "entry_ma224_dist_pct":"MA224","entry_upper_wick_pct":"Wick",
        "entry_ret5_pct":"5D"
    }
    rows=[]
    for key in w:
        if key not in risk:continue
        cur=nval(r,key)
        if pd.isna(cur):continue
        rows.append(f"{labels.get(key,key)} {match_direction(cur,w,risk,key)}")
    return rows[:7] if rows else ["현재 후보에서 비교 가능한 frozen feature 부족"]

def reference_line(p):
    r=REF.get(p)
    if not r:return ""
    w=r["winner"]; pieces=[]
    for key,label,suf in [
        ("entry_close_loc_pct","CloseLoc","%"),
        ("entry_vol20_ratio","Vol20","x"),
        ("entry_amount20_ratio","Amount20","x"),
        ("entry_atr_pct","ATR","%"),
        ("entry_ma224_dist_pct","MA224","%"),
    ]:
        if key in w: pieces.append(f"{label} {fmt(w[key],2,suf)}")
    return " · ".join(pieces)

def macro_snapshot():
    # Current environment display only; never used for selection/rank.
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
            if len(c)<2:continue
            out[k]={"ret1":(c.iloc[-1]/c.iloc[-2]-1)*100,
                    "ret5":(c.iloc[-1]/c.iloc[-6]-1)*100 if len(c)>=6 else np.nan,
                    "date":str(pd.Timestamp(c.index[-1]).date())}
        except Exception:
            pass
    return out

def macro_text(m):
    order=["KOSPI","KOSDAQ","SOX","NASDAQ","USDKRW","VIX","US10Y","DXY","WTI"]
    bits=[]
    for k in order:
        if k in m:
            bits.append(f"{k} 5D {fmt(m[k].get('ret5'),2,'%')}")
    return " · ".join(bits) if bits else "macro unavailable"

def board_for_row(r,macro):
    p=mode_of(r); code=code_of(r); name=name_of(r)
    title=f"🔬 {name or code} · {p}"
    lines=[title,
           f"CURRENT | {current_lines(r,p)}"]
    ref=REF.get(p)
    if ref:
        lines += [
            "WINNER CHARACTERISTICS | " + " / ".join(ref["characteristics"][:2]),
            "CURRENT vs WINNER/RISK | " + " · ".join(match_lines(r,p)),
            "WINNER REF | " + reference_line(p),
            "RESPONSE WATCHLIST | " + " / ".join(ref["watch"][:3]),
            "EXIT MORPHOLOGY | " + ref["exit"],
            f"SAMPLE | winner reference n={ref['winner_n']} · {ref['comparison']} · SHADOW ONLY",
        ]
    else:
        lines += ["RESEARCH | frozen Winner/Failure reference 미구축 패턴 · 일반 구조만 표시"]
    lines += [
        "MARKET | "+macro_text(macro),
        "CATALYST | "+catalyst_text(r),
    ]
    return "\n".join(lines)

def telegram_send(text):
    import requests
    token=(os.environ.get("CLOSING_BET_TOKEN") or os.environ.get("TELEGRAM_CLOSEBET_TOKEN") or
           os.environ.get("TELEGRAM_TOKEN") or "").strip()
    chat=(os.environ.get("CLOSING_BET_CHAT_ID") or os.environ.get("TELEGRAM_CLOSEBET_CHAT_ID") or
          os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat:
        return {"status":"SKIPPED_NO_ROUTE"}
    url=f"https://api.telegram.org/bot{token}/sendMessage"
    parts=[]; cur=""
    for block in text.split("\n\n"):
        add=block+"\n\n"
        if len(cur)+len(add)>3600 and cur:
            parts.append(cur.rstrip());cur=add
        else:cur+=add
    if cur.strip():parts.append(cur.rstrip())
    ok=0; errs=[]
    for p in parts:
        try:
            rr=requests.post(url,data={"chat_id":chat,"text":p,"disable_web_page_preview":"true"},timeout=20)
            if rr.ok:ok+=1
            else:errs.append(f"{rr.status_code}:{rr.text[:120]}")
        except Exception as e:errs.append(type(e).__name__)
    return {"status":"DELIVERED" if ok==len(parts) and parts else "PARTIAL_OR_FAILED","parts":len(parts),"success":ok,"errors":errs}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-root",default="source_run")
    ap.add_argument("--output-dir",default="reports/live_shadow_board_r1")
    ap.add_argument("--top-n",type=int,default=10)
    ap.add_argument("--send-telegram",action="store_true")
    ap.add_argument("--source-run-id",default="")
    a=ap.parse_args()
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)

    src=find_candidate(a.source_root)
    if src is None:
        meta={"version":VERSION,"status":"NO_CANDIDATE_SOURCE","research_only":True,
              "production_changed":False,"score_rank_changed":False,"candidate_filter_created":False,
              "source_run_id":a.source_run_id}
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        (out/"shadow_board.txt").write_text("NO_CANDIDATE_SOURCE",encoding="utf-8")
        print("NO_CANDIDATE_SOURCE")
        return

    df=read_csv(src)
    if df.empty:
        raise SystemExit(f"EMPTY_CANDIDATE_SOURCE {src}")

    # Current-day/live source may contain historical rows; keep latest signal_date if available.
    if "signal_date" in df:
        dd=pd.to_datetime(df["signal_date"],errors="coerce")
        if dd.notna().any():
            latest=dd.max().normalize()
            df=df[dd.dt.normalize().eq(latest)].copy()

    # Preserve incoming row order. No sorting/re-ranking is permitted.
    df=df.head(max(1,a.top_n)).copy()
    macro=macro_snapshot()

    boards=[board_for_row(r,macro) for _,r in df.iterrows()]
    header=[
        "🧪 [v49.76 LIVE SHADOW BOARD R1 · RESEARCH ONLY]",
        f"source={src.name} · rows={len(df)} · source_run={a.source_run_id or '-'}",
        "※ 기존 검색식/점수/랭킹/후보순서/주문 로직 변경 0",
        "※ Winner/Failure 특징은 과거 OOS 소표본의 설명용 frozen reference이며 확률·추천점수가 아님",
    ]
    text="\n".join(header)+"\n\n"+"\n\n".join(boards)
    (out/"shadow_board.txt").write_text(text,encoding="utf-8")
    df.to_csv(out/"shadow_board_source_candidates.csv",index=False,encoding="utf-8-sig")

    rows=[]
    for i,(_,r) in enumerate(df.iterrows(),1):
        p=mode_of(r)
        rec={"source_order":i,"code":code_of(r),"name":name_of(r),"pattern":p,
             "catalyst":catalyst_text(r)}
        for k in ALIASES:rec[k]=nval(r,k)
        if p in REF:
            rec["winner_reference_n"]=REF[p]["winner_n"]
            rec["research_comparison"]=REF[p]["comparison"]
            rec["exit_morphology"]=REF[p]["exit"]
        rows.append(rec)
    pd.DataFrame(rows).to_csv(out/"shadow_board_structured.csv",index=False,encoding="utf-8-sig")

    tg={"status":"NOT_REQUESTED"}
    if a.send_telegram:
        tg=telegram_send(text)
    (out/"telegram_delivery.json").write_text(json.dumps(tg,ensure_ascii=False,indent=2),encoding="utf-8")

    meta={
        "version":VERSION,"status":"PASS","source":str(src),"rows":len(df),
        "source_run_id":a.source_run_id,"research_only":True,"production_eligible":False,
        "candidate_membership_changed":False,"candidate_order_changed":False,
        "score_rank_changed":False,"candidate_filter_created":False,"order_logic_changed":False,
        "winner_reference_patterns":sorted(REF),"macro_shadow_only":True,
        "catalyst_shadow_only":True,"telegram_status":tg.get("status"),
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    print(text)
    print("TELEGRAM",tg)

if __name__=="__main__":
    main()
