#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
R1.0.2 causal-history bridge for Closing Bet Structure + Environment OOS research.

Why:
- Current v49.76 artifact exports history authority summaries, not raw OHLCV.
- This research-only bridge reconstructs OHLCV causally from public KRX history,
  one ticker at a time, then feeds it to the existing R1 analyzer.
- Production search/score/rank/order code is NOT modified.
"""
from __future__ import annotations

import argparse, json, math, os, re, sys, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np

import closing_bet_structure_env_oos_r1 as r1

REVISION = "CLOSEBET_STRUCTURE_ENV_OOS_R102_CAUSAL_HISTORY_20260918"

def _arg_value(flag: str, default: str) -> str:
    a = sys.argv[1:]
    if flag in a:
        i = a.index(flag)
        if i + 1 < len(a):
            return a[i+1]
    return default

SOURCE_ROOT = Path(_arg_value("--source-root", "source_artifacts"))
OOS_END = _arg_value("--oos-end", "2026-09-18")
CACHE_DIR = Path(".cache/closebet_structure_env_oos_r102")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
RAW_HISTORY = CACHE_DIR / "v49_76_research_raw_history.csv"
HISTORY_META = CACHE_DIR / "history_rebuild_meta.json"

_orig_find = r1.find
_orig_prep_evt = r1.prep_evt

def _pick_event_source(root: Path) -> Path | None:
    for name in (
        "v73_backtest_event_master.csv",
        "v49_76_selected_enriched_outcomes.csv",
        "v49_76_global_canonical.csv",
    ):
        p = _orig_find(root, name)
        if p is not None:
            return p
    return None

def _copy_if_missing(q: pd.DataFrame, target: str, sources: list[str], default=np.nan):
    if target in q.columns and q[target].notna().any():
        return
    for s in sources:
        if s in q.columns:
            q[target] = q[s]
            return
    if target not in q.columns:
        q[target] = default

def _prep_evt_bridge(df: pd.DataFrame) -> pd.DataFrame:
    q=df.copy()
    _copy_if_missing(q,"ret1",["ret_close_1d","ret_next_close"])
    _copy_if_missing(q,"ret3",["ret_close_3d"])
    _copy_if_missing(q,"ret5",["ret_close_5d","ret_close_hd"])
    _copy_if_missing(q,"ret10",["ret_close_10d"])
    _copy_if_missing(q,"mfe",["ret_max_high_5d","ret_max_high_hd","path_max_high_ret"])
    _copy_if_missing(q,"mae",["ret_min_low_5d","ret_min_low_hd","path_min_low_ret"])
    _copy_if_missing(q,"evaluation_ret",["ret_close_5d","ret_close_hd","rule35_pnl"])
    _copy_if_missing(q,"primary_formula",["primary_strategy","strategy","mode"],"UNCLASSIFIED")
    _copy_if_missing(q,"formula_list",["all_matched_strategies","primary_strategy","strategy"],"")
    _copy_if_missing(q,"formula_count",["matched_strategy_count"],1)
    _copy_if_missing(q,"market_state",["market_m5_state_calc_t1","market_m5_state_calc","market_down_context"],"UNKNOWN")
    _copy_if_missing(q,"sector_state",["sector_label"],"UNKNOWN")
    _copy_if_missing(q,"context_alignment",["event_theme_bucket"],"UNKNOWN")
    _copy_if_missing(q,"market_turnover_ratio",["entry_amount20_ratio"])
    _copy_if_missing(q,"sector_turnover_ratio",["sector_peer_amount_b"])
    _copy_if_missing(q,"sector_breadth",["sector_peer_positive_pct"])
    if "catalyst_state" not in q.columns:
        q["catalyst_state"]="UNKNOWN_CURRENT_V4976_ARTIFACT"
    return _orig_prep_evt(q)

def _norm_code(v) -> str:
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit(): s=s[:-2]
    if s.startswith("A") and len(s)==7: s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def _load_one_pykrx(code: str, start: str, end: str):
    from pykrx import stock
    # pykrx accepts YYYYMMDD
    q=stock.get_market_ohlcv_by_date(start.replace("-",""), end.replace("-",""), code)
    if q is None or q.empty:
        return None, "PYKRX_EMPTY"
    q=q.reset_index()
    date_col=q.columns[0]
    ren={}
    for c in q.columns:
        cs=str(c)
        if "시가"==cs: ren[c]="Open"
        elif "고가"==cs: ren[c]="High"
        elif "저가"==cs: ren[c]="Low"
        elif "종가"==cs: ren[c]="Close"
        elif "거래량"==cs: ren[c]="Volume"
        elif "거래대금"==cs: ren[c]="Amount"
    q=q.rename(columns=ren)
    q["date"]=pd.to_datetime(q[date_col],errors="coerce").dt.normalize()
    q["code"]=code
    need=["Open","High","Low","Close","Volume"]
    if any(c not in q.columns for c in need):
        return None, f"PYKRX_SCHEMA_{list(q.columns)}"
    if "Amount" not in q.columns:
        q["Amount"]=np.nan
    return q[["code","date","Open","High","Low","Close","Volume","Amount"]], "PYKRX"

def _load_one_fdr(code: str, start: str, end: str):
    import FinanceDataReader as fdr
    q=fdr.DataReader(code,start,end)
    if q is None or q.empty:
        return None, "FDR_EMPTY"
    q=q.reset_index()
    dc=q.columns[0]
    q["date"]=pd.to_datetime(q[dc],errors="coerce").dt.normalize()
    q["code"]=code
    for c in ["Open","High","Low","Close","Volume"]:
        if c not in q.columns:
            return None, f"FDR_SCHEMA_{list(q.columns)}"
    q["Amount"]=np.nan
    return q[["code","date","Open","High","Low","Close","Volume","Amount"]], "FDR"

def _fetch_code(code: str, start: str, end: str):
    errs=[]
    for attempt in range(2):
        try:
            q,src=_load_one_pykrx(code,start,end)
            if q is not None and len(q)>=224:
                return code,q,src,""
            errs.append(src)
        except Exception as e:
            errs.append(f"PYKRX_{type(e).__name__}:{e}")
        time.sleep(0.2*(attempt+1))
    try:
        q,src=_load_one_fdr(code,start,end)
        if q is not None and len(q)>=224:
            return code,q,src,""
        errs.append(src)
    except Exception as e:
        errs.append(f"FDR_{type(e).__name__}:{e}")
    return code,None,"FAIL"," | ".join(errs)[-1000:]

def _rebuild_history():
    evp=_pick_event_source(SOURCE_ROOT)
    if evp is None:
        raise SystemExit("R102_EVENT_SOURCE_MISSING")
    ev=pd.read_csv(evp,low_memory=False,encoding="utf-8-sig")
    cc=None
    for c in ("code","Code","종목코드"):
        if c in ev.columns: cc=c; break
    dc=None
    for c in ("signal_date","date","신호일"):
        if c in ev.columns: dc=c; break
    if cc is None or dc is None:
        raise SystemExit(f"R102_EVENT_SCHEMA_MISSING cols={list(ev.columns)[:80]}")
    ev["_code"]=ev[cc].map(_norm_code)
    ev["_date"]=pd.to_datetime(ev[dc],errors="coerce").dt.normalize()
    ev=ev[ev["_code"].ne("") & ev["_date"].notna()]
    codes=sorted(ev["_code"].unique())
    earliest=ev["_date"].min()
    # 700 calendar days gives comfortably >224 trading days before first 2026 signal.
    start=(earliest-pd.Timedelta(days=700)).strftime("%Y-%m-%d")
    end=pd.Timestamp(OOS_END).strftime("%Y-%m-%d")

    print(f"R102_CAUSAL_HISTORY_REBUILD codes={len(codes)} start={start} end={end} event_source={evp}")
    parts=[]; rows=[]
    workers=min(6,max(1,int(os.environ.get("R102_HISTORY_WORKERS","6"))))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs={ex.submit(_fetch_code,c,start,end):c for c in codes}
        done=0
        for fut in as_completed(futs):
            c,q,src,err=fut.result()
            done+=1
            if q is not None:
                parts.append(q)
                rows.append({"code":c,"status":"OK","source":src,"rows":len(q),"error":""})
            else:
                rows.append({"code":c,"status":"FAIL","source":src,"rows":0,"error":err})
            if done%50==0 or done==len(codes):
                ok=sum(r["status"]=="OK" for r in rows)
                print(f"R102_HISTORY_PROGRESS {done}/{len(codes)} ok={ok}")

    audit=pd.DataFrame(rows)
    ok=int((audit["status"]=="OK").sum()) if len(audit) else 0
    coverage=ok/max(1,len(codes))*100
    if not parts:
        raise SystemExit("R102_HISTORY_REBUILD_ZERO_SUCCESS")
    hist=pd.concat(parts,ignore_index=True).drop_duplicates(["code","date"],keep="last")
    hist.to_csv(RAW_HISTORY,index=False,encoding="utf-8-sig")
    audit.to_csv(CACHE_DIR/"history_rebuild_audit.csv",index=False,encoding="utf-8-sig")
    meta={
        "revision":REVISION,"event_source":str(evp),"codes":len(codes),"ok_codes":ok,
        "coverage_pct":coverage,"start":start,"end":end,"raw_rows":len(hist),
        "research_only":True,"production_eligible":False
    }
    HISTORY_META.write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    # Fail closed only if coverage is unusably low; partial misses remain visible.
    if coverage < 85:
        raise SystemExit(f"R102_HISTORY_COVERAGE_TOO_LOW {coverage:.1f}%")
    print(f"R102_HISTORY_READY rows={len(hist)} codes={ok}/{len(codes)} coverage={coverage:.1f}%")

def _find_bridge(root: Path, name: str):
    if name=="v73_backtest_event_master.csv":
        p=_pick_event_source(root)
        if p is not None:
            print(f"R102_EVENT_SOURCE_BRIDGE -> {p.name}")
            return p
    if name=="v49_76_history_authority_global.csv":
        if RAW_HISTORY.exists():
            print(f"R102_HISTORY_SOURCE_BRIDGE -> {RAW_HISTORY}")
            return RAW_HISTORY
    return _orig_find(root,name)

def main():
    _rebuild_history()
    r1.find=_find_bridge
    r1.prep_evt=_prep_evt_bridge
    print("CLOSEBET_STRUCTURE_ENV_OOS_R102_CAUSAL_HISTORY active")
    r1.main()

if __name__=="__main__":
    main()
