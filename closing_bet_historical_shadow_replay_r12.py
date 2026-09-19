#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math, os, re
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

VERSION="CLOSEBET_HISTORICAL_SHADOW_REPLAY_R12_20260919"
RESEARCH_ONLY=True

# Frozen references copied from the already-established LIVE SHADOW BOARD R1/R1.1.
# DO NOT tune these values inside this replay.
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
            "거래대금/거래량 재유입 유지 여부",
            "종가 위치 붕괴 또는 윗꼬리 급증 여부",
            "빠른 상승 후 giveback 확대 여부",
            "15:03→15:40 구조/유동성 강화 여부(스냅샷 있을 때만)"
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
            "단순히 많이 떨어진 후보보다 복구 가능한 눌림인지가 중요하게 관찰됨",
            "거래량 자체보다 구조 손상 정도가 더 구분력 있게 관찰됨"
        ],
        "watch":[
            "ATR 과대 여부",
            "MA60/224에서 지나치게 멀어진 깊은 훼손 여부",
            "유동성 재진입과 종가 회복 동반 여부",
            "낙폭만 보고 구조 손상을 무시하지 않기"
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
            "거래량/거래대금 재유입 여부",
            "MA60/224 훼손 확대 여부",
            "반등 직후 이익 반납 확대 여부"
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
            "유동성이 너무 약하지도 과도하게 폭발하지도 않는지",
            "MA20 부근의 조용한 재시동인지",
            "ATR 급증과 과열 반등 여부",
            "종가 강도 유지와 wick 확대 여부"
        ],
        "exit":"D20 성숙표본 n=18 · peak day 중앙값 D9 · 빠른 MA5 대응은 정상 흔들림까지 자를 가능성 관찰"
    }
}

ALIASES={
    "entry_close_loc_pct":["entry_close_loc_pct","close_loc_pct"],
    "entry_vol20_ratio":["entry_vol20_ratio","vol_ratio","vol20_ratio"],
    "entry_amount20_ratio":["entry_amount20_ratio","amount20_ratio"],
    "entry_atr_pct":["entry_atr_pct","atr_pct","atr"],
    "entry_ma20_dist_pct":["entry_ma20_dist_pct","ma20_dist_pct"],
    "entry_ma60_dist_pct":["entry_ma60_dist_pct","ma60_dist_pct"],
    "entry_ma224_dist_pct":["entry_ma224_dist_pct","ma224_dist_pct"],
    "entry_upper_wick_pct":["entry_upper_wick_pct","wick_pct","_upper_wick_body"],
    "entry_ret5_pct":["entry_ret5_pct","entry_stock_ret_5d"],
    "entry_ret20_pct":["entry_ret20_pct","entry_stock_ret_20d"],
}

def read_csv(p):
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,encoding=enc,low_memory=False)
        except Exception:pass
    return pd.DataFrame()

def find_source(root):
    root=Path(root)
    # Prefer outcome-enriched event files for historical replay.
    preferred=[
        "v49_76_selected_enriched_outcomes.csv",
        "v49_76_global_canonical.csv",
        "v49_76_pit_marcap_current_selected_enriched.csv",
        "v49_76_search_intent_enriched.csv",
    ]
    for n in preferred:
        xs=list(root.rglob(n))
        if xs:
            xs.sort(key=lambda p:(len(p.parts),str(p)))
            for p in xs:
                q=read_csv(p)
                if not q.empty:return p
    # Last-resort candidate-like files.
    xs=[p for p in root.rglob("*.csv") if any(x in p.name.lower() for x in ["selected_enriched","global_canonical"])]
    for p in sorted(xs):
        q=read_csv(p)
        if not q.empty:return p
    return None

def num(row,names):
    for c in names:
        if c in row.index:
            v=pd.to_numeric(pd.Series([row[c]]),errors="coerce").iloc[0]
            if pd.notna(v):return float(v)
    return np.nan

def txt(row,names,default=""):
    for c in names:
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip():
            return str(row[c]).strip()
    return default

def feature(row,key):
    return num(row,ALIASES.get(key,[key]))

def pattern(row):
    x=txt(row,["primary_strategy","primary_formula","mode","strategy"],"UNKNOWN").upper()
    for p in ["B1","B2","C","I"]:
        if x==p or x.startswith(p+"-") or x.startswith(p+"_"):return p
    # explicit A is retained but has no frozen winner reference yet.
    if x=="A" or x.startswith("A-") or x.startswith("A_"): return "A"
    return x

def code(row):
    x=txt(row,["code","ticker","종목코드"],"")
    x=re.sub(r"\.0$","",x)
    return x.zfill(6) if x.isdigit() and len(x)<=6 else x

def event_key(row):
    return "|".join([
        txt(row,["signal_date","date"],""),
        code(row),
        txt(row,["primary_strategy","strategy","mode"],"")
    ])

def closer_label(x,w,b):
    """Frozen R1.1 display heuristic; not a learned classifier."""
    if not (pd.notna(x) and pd.notna(w) and pd.notna(b)):return "UNAVAILABLE"
    dw,db=abs(x-w),abs(x-b)
    if abs(w-b)<1e-12:return "MIXED"
    if dw < db*0.75:return "WINNER_LIKE"
    if db < dw*0.75:return "RISK_LIKE"
    return "MIXED"

def actual_outcome(row):
    # Causal label uses only FUTURE columns and is therefore for retrospective validation ONLY.
    # It is never fed back into candidate selection.
    avail=num(row,["eval_available_days"])
    pstop=num(row,["path_first_stop_day"])
    p5=num(row,["path_first_plus5_day"])
    max5=num(row,["ret_max_high_5d"])
    close5=num(row,["ret_close_5d"])
    max10=num(row,["ret_max_high_10d"])
    close10=num(row,["ret_close_10d"])
    maxhd=num(row,["ret_max_high_hd","path_max_high_ret"])
    closehd=num(row,["ret_close_hd"])
    stopbefore=num(row,["stop_before_3"])
    if pd.notna(stopbefore) and stopbefore>=1:
        cls="STOP_FIRST"
    elif pd.notna(pstop) and pd.notna(p5) and pstop < p5:
        cls="STOP_FIRST"
    elif pd.notna(p5):
        cls="PLUS5_FIRST"
    elif pd.notna(max5) and max5>=5:
        cls="PLUS5_REACHED"
    elif pd.notna(avail) and avail>=5:
        cls="NO_PLUS5_WITHIN_5D"
    else:
        cls="MATURE_PENDING"

    # Descriptive giveback, only if enough path information exists.
    gb=np.nan
    if pd.notna(maxhd) and pd.notna(closehd): gb=maxhd-closehd
    return cls, max5, close5, max10, close10, maxhd, closehd, gb

def macro_pit(signal_dates):
    """Strict prior-close macro reconstruction.
    For every Korean signal date, use the latest market close strictly BEFORE signal_date.
    This intentionally gives up same-day KRX information to avoid historical look-ahead.
    """
    try:
        import yfinance as yf
    except Exception:
        return pd.DataFrame(), {"status":"YFINANCE_UNAVAILABLE"}

    dates=sorted(pd.to_datetime(pd.Series(list(signal_dates)),errors="coerce").dropna().dt.normalize().unique())
    if not dates:return pd.DataFrame(), {"status":"NO_DATES"}
    start=(pd.Timestamp(min(dates))-pd.Timedelta(days=20)).strftime("%Y-%m-%d")
    end=(pd.Timestamp(max(dates))+pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    mp={"KOSPI":"^KS11","KOSDAQ":"^KQ11","SOX":"^SOX","NASDAQ":"^IXIC",
        "SP500":"^GSPC","USDKRW":"KRW=X","VIX":"^VIX","US10Y":"^TNX",
        "DXY":"DX-Y.NYB","WTI":"CL=F"}
    histories={}
    for k,sym in mp.items():
        try:
            d=yf.download(sym,start=start,end=end,progress=False,auto_adjust=False,threads=False)
            if isinstance(d.columns,pd.MultiIndex):d.columns=[c[0] for c in d.columns]
            c=pd.to_numeric(d["Close"],errors="coerce").dropna()
            c.index=pd.to_datetime(c.index).tz_localize(None).normalize()
            histories[k]=c
        except Exception:
            histories[k]=pd.Series(dtype=float)

    out=[]
    for sd in dates:
        sd=pd.Timestamp(sd).normalize()
        rec={"signal_date":sd.strftime("%Y-%m-%d"),"macro_semantics":"STRICT_PRIOR_CLOSE"}
        for k,c in histories.items():
            prev=c[c.index < sd]
            if len(prev)>=1:
                rec[f"{k}_close"]=float(prev.iloc[-1])
                rec[f"{k}_source_date"]=prev.index[-1].strftime("%Y-%m-%d")
                rec[f"{k}_ret1_pct"]=float((prev.iloc[-1]/prev.iloc[-2]-1)*100) if len(prev)>=2 else np.nan
                rec[f"{k}_ret5_pct"]=float((prev.iloc[-1]/prev.iloc[-6]-1)*100) if len(prev)>=6 else np.nan
            else:
                rec[f"{k}_close"]=np.nan
                rec[f"{k}_source_date"]=""
                rec[f"{k}_ret1_pct"]=np.nan
                rec[f"{k}_ret5_pct"]=np.nan
        out.append(rec)
    return pd.DataFrame(out), {"status":"OK","dates":len(out),"semantics":"STRICT_PRIOR_CLOSE"}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-root",default="source_run")
    ap.add_argument("--output-dir",default="reports/historical_shadow_replay_r12")
    ap.add_argument("--source-run-id",default="")
    ap.add_argument("--oos-start",default="2026-08-19")
    ap.add_argument("--oos-end",default="")
    ap.add_argument("--max-events",type=int,default=0)
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    src=find_source(a.source_root)
    if src is None:
        meta={"version":VERSION,"status":"NO_SOURCE","research_only":True,"source_run_id":a.source_run_id}
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        raise SystemExit("NO_SOURCE")

    df=read_csv(src)
    if "signal_date" not in df:
        raise SystemExit("SOURCE_HAS_NO_SIGNAL_DATE")
    dt=pd.to_datetime(df["signal_date"],errors="coerce")
    keep=dt.notna()
    if a.oos_start:
        keep &= dt>=pd.Timestamp(a.oos_start)
    if a.oos_end:
        keep &= dt<=pd.Timestamp(a.oos_end)
    df=df.loc[keep].copy()
    if a.max_events and len(df)>a.max_events:
        df=df.iloc[:a.max_events].copy()  # preserve source order; never rank
    if df.empty:
        raise SystemExit("EMPTY_AFTER_OOS_FILTER")

    # Stable event de-dup only; source order preserved.
    df["_event_key"]=[event_key(r) for _,r in df.iterrows()]
    before=len(df)
    df=df.drop_duplicates("_event_key",keep="first").copy()
    deduped=before-len(df)

    rows=[]
    match_long=[]
    for _,r in df.iterrows():
        p=pattern(r)
        cls,max5,close5,max10,close10,maxhd,closehd,giveback=actual_outcome(r)
        rec={
            "event_key":event_key(r),
            "signal_date":txt(r,["signal_date"]),
            "code":code(r),
            "name":txt(r,["name","종목명"]),
            "pattern":p,
            "actual_retrospective_class":cls,
            "ret_max_high_5d":max5,"ret_close_5d":close5,
            "ret_max_high_10d":max10,"ret_close_10d":close10,
            "ret_max_high_hd":maxhd,"ret_close_hd":closehd,
            "giveback_hd_pctpt":giveback,
            "path_first_stop_day":num(r,["path_first_stop_day"]),
            "path_first_plus5_day":num(r,["path_first_plus5_day"]),
            "eval_available_days":num(r,["eval_available_days"]),
            "snapshot_1503_status":"UNAVAILABLE_UNLESS_EXPLICIT_CAPTURE",
            "snapshot_1540_status":"FINAL_DAILY_OR_EVENT_SOURCE_ONLY",
            "catalyst_status":"RETROACTIVE_NOT_RECONSTRUCTED",
            "frozen_reference_available":p in REF,
        }
        # Entry features
        for k in ALIASES:
            rec[k]=feature(r,k)

        if p in REF:
            w,b=REF[p]["winner"],REF[p]["risk"]
            vals=[]
            for k in sorted(set(w)&set(b)):
                x=feature(r,k)
                lab=closer_label(x,float(w[k]),float(b[k]))
                vals.append(lab)
                match_long.append({
                    "event_key":rec["event_key"],"signal_date":rec["signal_date"],"code":rec["code"],
                    "name":rec["name"],"pattern":p,"actual_retrospective_class":cls,
                    "feature":k,"current_value":x,"winner_ref":w[k],"risk_ref":b[k],
                    "frozen_r11_alignment":lab
                })
            rec["winner_like_feature_n"]=sum(v=="WINNER_LIKE" for v in vals)
            rec["risk_like_feature_n"]=sum(v=="RISK_LIKE" for v in vals)
            rec["mixed_feature_n"]=sum(v=="MIXED" for v in vals)
            rec["unavailable_feature_n"]=sum(v=="UNAVAILABLE" for v in vals)
            rec["research_characteristics"]=" / ".join(REF[p]["characteristics"])
            rec["response_watchlist"]=" / ".join(REF[p]["watch"])
            rec["exit_morphology_reference"]=REF[p]["exit"]
            rec["reference_sample_n"]=REF[p]["winner_n"]
            rec["reference_comparison"]=REF[p]["comparison"]
        else:
            rec["winner_like_feature_n"]=np.nan
            rec["risk_like_feature_n"]=np.nan
            rec["mixed_feature_n"]=np.nan
            rec["unavailable_feature_n"]=np.nan
            rec["research_characteristics"]="NO_FROZEN_REFERENCE"
            rec["response_watchlist"]="GENERIC_STRUCTURE_ONLY"
            rec["exit_morphology_reference"]="NO_FROZEN_REFERENCE"
            rec["reference_sample_n"]=np.nan
            rec["reference_comparison"]="NO_FROZEN_REFERENCE"
        rows.append(rec)

    replay=pd.DataFrame(rows)
    match=pd.DataFrame(match_long)

    macro,macro_meta=macro_pit(replay["signal_date"].unique())
    if not macro.empty:
        replay=replay.merge(macro,on="signal_date",how="left",validate="many_to_one")

    replay.to_csv(out/"historical_shadow_replay_events.csv",index=False,encoding="utf-8-sig")
    match.to_csv(out/"historical_shadow_trait_alignment_long.csv",index=False,encoding="utf-8-sig")
    macro.to_csv(out/"historical_macro_strict_prior_close.csv",index=False,encoding="utf-8-sig")

    # Descriptive cohort summary only. No score/ranking/promotion.
    agg=[]
    for (p,cls),g in replay.groupby(["pattern","actual_retrospective_class"],dropna=False):
        rec={"pattern":p,"actual_retrospective_class":cls,"n":len(g),
             "independent_signal_dates":g["signal_date"].nunique()}
        for c in ["ret_max_high_5d","ret_close_5d","ret_max_high_10d","ret_close_10d",
                  "giveback_hd_pctpt","winner_like_feature_n","risk_like_feature_n",
                  "entry_close_loc_pct","entry_vol20_ratio","entry_amount20_ratio",
                  "entry_atr_pct","entry_ma20_dist_pct","entry_ma60_dist_pct",
                  "entry_ma224_dist_pct","entry_upper_wick_pct","entry_ret5_pct"]:
            if c in g:
                x=pd.to_numeric(g[c],errors="coerce").dropna()
                rec[c+"_median"]=float(x.median()) if len(x) else np.nan
                rec[c+"_mean"]=float(x.mean()) if len(x) else np.nan
        agg.append(rec)
    pd.DataFrame(agg).to_csv(out/"historical_shadow_cohort_summary.csv",index=False,encoding="utf-8-sig")

    # Alignment x realized class frequencies.
    if not match.empty:
        t=(match.groupby(["pattern","feature","frozen_r11_alignment","actual_retrospective_class"])
                .size().reset_index(name="n"))
        t.to_csv(out/"historical_shadow_alignment_vs_outcome.csv",index=False,encoding="utf-8-sig")
    else:
        pd.DataFrame().to_csv(out/"historical_shadow_alignment_vs_outcome.csv",index=False,encoding="utf-8-sig")

    # Human-readable report
    lines=[
        "# Closing Bet Historical Shadow Replay R1.2",
        "",
        f"- source: `{src.name}`",
        f"- source_run_id: `{a.source_run_id}`",
        f"- OOS window: {a.oos_start} ~ {a.oos_end or 'source end'}",
        f"- unique events: {len(replay)} (deduped {deduped})",
        f"- independent signal dates: {replay['signal_date'].nunique()}",
        f"- patterns: {', '.join(sorted(map(str,replay['pattern'].dropna().unique())))}",
        f"- macro reconstruction: {macro_meta.get('status')} / {macro_meta.get('semantics','')}",
        "",
        "## Guardrails",
        "- Frozen R1.1 references are replayed unchanged.",
        "- Future outcome fields are used only for retrospective validation.",
        "- No candidate filtering, re-ranking, scoring, or production promotion is created.",
        "- 15:03 state is NOT reconstructed from final daily OHLCV.",
        "- Historical macro uses strict prior-close semantics to avoid same-day look-ahead.",
        "- Catalyst is not backfilled here; it remains RETROACTIVE_NOT_RECONSTRUCTED.",
        "- Pattern A and other patterns without frozen references are shown as NO_FROZEN_REFERENCE, not silently inferred.",
        "",
        "## Retrospective outcome counts",
    ]
    counts=(replay.groupby(["pattern","actual_retrospective_class"]).size()
                  .reset_index(name="n").sort_values(["pattern","actual_retrospective_class"]))
    for _,r in counts.iterrows():
        lines.append(f"- {r['pattern']} / {r['actual_retrospective_class']}: n={int(r['n'])}")
    lines += [
        "",
        "## Interpretation",
        "This replay tests whether previously frozen descriptive characteristics remain visibly associated with later outcomes.",
        "It is not a classifier backtest and does not establish causal or trading rules.",
    ]
    (out/"REPORT.md").write_text("\n".join(lines),encoding="utf-8")

    meta={
        "version":VERSION,"status":"PASS","research_only":True,
        "source":str(src),"source_run_id":a.source_run_id,
        "oos_start":a.oos_start,"oos_end":a.oos_end,
        "events":len(replay),"independent_signal_dates":int(replay["signal_date"].nunique()),
        "deduped_events":deduped,
        "frozen_reference_patterns":sorted(REF),
        "same_sample_tuning":False,
        "new_threshold_optimization":False,
        "candidate_membership_changed":False,
        "candidate_order_changed":False,
        "score_rank_changed":False,
        "production_logic_changed":False,
        "snapshot_1503_reconstructed_from_final_bar":False,
        "historical_macro_semantics":"STRICT_PRIOR_CLOSE",
        "catalyst_semantics":"RETROACTIVE_NOT_RECONSTRUCTED",
        "macro_meta":macro_meta,
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False,indent=2))
    print(counts.to_string(index=False))

if __name__=="__main__":
    main()
