#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math, time, os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd

VERSION="ODOLI_RETROSPECTIVE_ALL_MARKET_R1_FIX2_MEMBERSHIP_PER_TICKER_20260925"
DEFINITION="STRICT_ODOLI_R1"
RESEARCH_ONLY=True

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit(): s=s[:-2]
    if s.startswith("A") and len(s)==7: s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def _read_csv(p):
    try:return pd.read_csv(p,dtype={"code":str},low_memory=False)
    except Exception:return pd.DataFrame()

def fetch_membership(stock,start,end,outdir,sleep_s=0.05):
    rows=[]; diag=[]
    for d in pd.date_range(start,end,freq="D"):
        if d.weekday()>=5: continue
        ymd=d.strftime("%Y%m%d")
        day_codes=[]
        for market in ("KOSPI","KOSDAQ"):
            try:
                try:
                    xs=list(stock.get_market_ticker_list(date=ymd,market=market) or [])
                except TypeError:
                    xs=list(stock.get_market_ticker_list(ymd,market=market) or [])
                codes=[norm_code(x) for x in xs if norm_code(x)]
                diag.append({"date":d.strftime("%Y-%m-%d"),"market":market,"rows":len(codes),"status":"OK","error":""})
                day_codes.extend((c,market) for c in codes)
            except Exception as e:
                diag.append({"date":d.strftime("%Y-%m-%d"),"market":market,"rows":0,"status":"FAIL",
                             "error":f"{type(e).__name__}:{e}"})
            time.sleep(max(0,sleep_s))
        for c,m in day_codes:
            rows.append({"date":d.strftime("%Y-%m-%d"),"code":c,"market":m})
    mem=pd.DataFrame(rows).drop_duplicates(["date","code"],keep="last") if rows else pd.DataFrame()
    pd.DataFrame(diag).to_csv(outdir/"membership_diagnostics.csv",index=False,encoding="utf-8-sig")
    mem.to_csv(outdir/"historical_membership.csv",index=False,encoding="utf-8-sig")
    return mem

def load_one_pykrx(code,start,end):
    from pykrx import stock
    q=stock.get_market_ohlcv_by_date(start.replace("-",""),end.replace("-",""),code)
    if q is None or q.empty:return None,"PYKRX_EMPTY"
    q=q.reset_index()
    dc=q.columns[0]
    ren={}
    for c in q.columns:
        s=str(c)
        if s=="시가":ren[c]="open"
        elif s=="고가":ren[c]="high"
        elif s=="저가":ren[c]="low"
        elif s=="종가":ren[c]="close"
        elif s=="거래량":ren[c]="volume"
        elif s=="거래대금":ren[c]="amount"
    q=q.rename(columns=ren)
    q["date"]=pd.to_datetime(q[dc],errors="coerce").dt.normalize()
    q["code"]=code
    need=["open","high","low","close","volume"]
    if any(c not in q.columns for c in need):
        return None,f"PYKRX_SCHEMA_{list(q.columns)}"
    if "amount" not in q.columns:q["amount"]=np.nan
    return q[["code","date","open","high","low","close","volume","amount"]],"PYKRX"

def load_one_fdr(code,start,end):
    import FinanceDataReader as fdr
    q=fdr.DataReader(code,start,end)
    if q is None or q.empty:return None,"FDR_EMPTY"
    q=q.reset_index()
    dc=q.columns[0]
    q["date"]=pd.to_datetime(q[dc],errors="coerce").dt.normalize()
    q["code"]=code
    q=q.rename(columns={c:str(c).lower() for c in q.columns})
    need=["open","high","low","close","volume"]
    if any(c not in q.columns for c in need):
        return None,f"FDR_SCHEMA_{list(q.columns)}"
    q["amount"]=np.nan
    return q[["code","date","open","high","low","close","volume","amount"]],"FDR"

def fetch_code(code,start,end,cache):
    p=cache/f"{code}.csv.gz"
    if p.exists():
        q=_read_csv(p)
        if not q.empty:
            q["date"]=pd.to_datetime(q["date"],errors="coerce").dt.normalize()
            return code,q,"CACHE",""
    errs=[]
    for attempt in range(2):
        try:
            q,src=load_one_pykrx(code,start,end)
            if q is not None and not q.empty:
                p.parent.mkdir(parents=True,exist_ok=True)
                q.to_csv(p,index=False,compression="gzip")
                return code,q,src,""
            errs.append(src)
        except Exception as e:
            errs.append(f"PYKRX_{type(e).__name__}:{e}")
        time.sleep(0.25*(attempt+1))
    try:
        q,src=load_one_fdr(code,start,end)
        if q is not None and not q.empty:
            p.parent.mkdir(parents=True,exist_ok=True)
            q.to_csv(p,index=False,compression="gzip")
            return code,q,src,""
        errs.append(src)
    except Exception as e:
        errs.append(f"FDR_{type(e).__name__}:{e}")
    return code,None,"FAIL"," | ".join(errs)[-1200:]

def add_features(g):
    g=g.sort_values("date").copy()
    c=g["close"].astype(float)
    g["ma5"]=c.rolling(5,min_periods=5).mean()
    g["ma10"]=c.rolling(10,min_periods=10).mean()
    g["prev_close"]=c.shift(1); g["prev_ma5"]=g["ma5"].shift(1)
    g["prev2_close"]=c.shift(2); g["prev2_ma5"]=g["ma5"].shift(2)
    g["prev3_close"]=c.shift(3); g["prev3_ma5"]=g["ma5"].shift(3)
    g["pre3_below_ma5_n"]=(
        (g["prev_close"]<g["prev_ma5"]).astype(float)
        +(g["prev2_close"]<g["prev2_ma5"]).astype(float)
        +(g["prev3_close"]<g["prev3_ma5"]).astype(float)
    )
    g["ma5_slope_1d_pct"]=(g["ma5"]/g["prev_ma5"]-1)*100
    g["ma5_ma10_gap_pct"]=(g["ma5"]/g["ma10"]-1)*100
    g["ret5_pct"]=(c/c.shift(5)-1)*100
    g["vol20_ratio"]=g["volume"]/g["volume"].shift(1).rolling(20,min_periods=10).mean()
    g["amount20_ratio"]=g["amount"]/g["amount"].shift(1).rolling(20,min_periods=10).mean()
    den=(g["high"]-g["low"]).replace(0,np.nan)
    g["close_loc_pct"]=(g["close"]-g["low"])/den*100
    g["upper_wick_pct"]=(g["high"]-g[["open","close"]].max(axis=1))/den*100
    g["gap_pct"]=(g["open"]/g["prev_close"]-1)*100
    g["bull_candle"]=g["close"]>g["open"]
    g["odoli_r1"]=(
        g["bull_candle"]
        & (g["prev_close"]<g["prev_ma5"])
        & (g["close"]>g["ma5"])
        & (g["ma5_slope_1d_pct"]>0)
        & (g["pre3_below_ma5_n"]>=2)
    )
    return g

def outcomes(g,idx):
    pos=g.index.get_loc(idx); entry=float(g.loc[idx,"close"]); out={}
    for n in (1,3,5,10):
        j=pos+n
        out[f"d{n}_complete"]=bool(j<len(g))
        out[f"d{n}_close_ret_pct"]=((float(g.iloc[j]["close"])/entry-1)*100) if j<len(g) else np.nan
        sl=g.iloc[pos+1:min(len(g),pos+n+1)]
        if len(sl):
            out[f"d{n}_mfe_pct"]=(float(sl["high"].max())/entry-1)*100
            out[f"d{n}_mae_pct"]=(float(sl["low"].min())/entry-1)*100
            out[f"d{n}_hit_plus5"]=bool(out[f"d{n}_mfe_pct"]>=5)
        else:
            out[f"d{n}_mfe_pct"]=np.nan; out[f"d{n}_mae_pct"]=np.nan; out[f"d{n}_hit_plus5"]=False
    if out["d5_complete"]:
        hit5=bool(out["d5_hit_plus5"]); d5=float(out["d5_close_ret_pct"])
        if hit5 and d5>=5:fam="EARLY_WIN_HELD"
        elif hit5 and d5<5:fam="EARLY_WIN_GIVEBACK"
        elif (not hit5) and d5>0:fam="SLOW_POSITIVE_NO_PLUS5"
        else:fam="NO_RECOVERY_BY_D5"
    else:fam="PENDING_D5"
    out["odoli_path_r1"]=fam
    return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--start",required=True); ap.add_argument("--end",required=True)
    ap.add_argument("--output-dir",required=True)
    ap.add_argument("--cache-dir",default="reports/.cache/odoli_all_market_r1_fix2")
    ap.add_argument("--workers",type=int,default=4)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    cache=Path(a.cache_dir); cache.mkdir(parents=True,exist_ok=True)
    scan_start=pd.Timestamp(a.start); scan_end=pd.Timestamp(a.end)
    fetch_start=(scan_start-pd.Timedelta(days=45)).normalize()
    fetch_end=(scan_end+pd.Timedelta(days=25)).normalize()

    from pykrx import stock
    mem=fetch_membership(stock,fetch_start,fetch_end,out)
    if mem.empty:
        meta={"version":VERSION,"definition":DEFINITION,"research_only":True,"status":"NO_MEMBERSHIP",
              "start":a.start,"end":a.end,"events":0,"production_logic_changed":False,
              "same_sample_threshold_tuning":False}
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        raise SystemExit("ODOLI_R1_NO_MEMBERSHIP")

    codes=sorted(mem["code"].dropna().astype(str).unique())
    start_s=fetch_start.strftime("%Y-%m-%d"); end_s=fetch_end.strftime("%Y-%m-%d")
    audit=[]; hist_parts=[]
    workers=max(1,min(int(a.workers),6))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs={ex.submit(fetch_code,c,start_s,end_s,cache/"ticker_history"):c for c in codes}
        done=0
        for fut in as_completed(futs):
            c,q,src,err=fut.result(); done+=1
            if q is not None and not q.empty:
                hist_parts.append(q)
                audit.append({"code":c,"status":"OK","source":src,"rows":len(q),"error":""})
            else:
                audit.append({"code":c,"status":"FAIL","source":src,"rows":0,"error":err})
            if done%100==0 or done==len(codes):
                print(f"ODOLI_R1_HISTORY_PROGRESS {done}/{len(codes)} ok={sum(x['status']=='OK' for x in audit)}")

    aud=pd.DataFrame(audit)
    aud.to_csv(out/"history_fetch_audit.csv",index=False,encoding="utf-8-sig")
    ok_codes=int((aud["status"]=="OK").sum()) if len(aud) else 0
    coverage=ok_codes/max(1,len(codes))*100
    if not hist_parts or coverage<85:
        meta={"version":VERSION,"definition":DEFINITION,"research_only":True,"status":"HISTORY_COVERAGE_TOO_LOW",
              "start":a.start,"end":a.end,"events":0,"membership_codes":len(codes),
              "history_ok_codes":ok_codes,"history_coverage_pct":coverage,
              "production_logic_changed":False,"same_sample_threshold_tuning":False}
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        raise SystemExit(f"ODOLI_R1_HISTORY_COVERAGE_TOO_LOW {coverage:.1f}%")

    hist=pd.concat(hist_parts,ignore_index=True)
    hist["date"]=pd.to_datetime(hist["date"]).dt.normalize()
    mem["date"]=pd.to_datetime(mem["date"]).dt.normalize()
    membership_key=set(zip(mem["date"],mem["code"]))

    events=[]
    for code,g0 in hist.groupby("code",sort=False):
        g=add_features(g0).reset_index(drop=True)
        mask=g["odoli_r1"] & g["date"].between(scan_start,scan_end)
        for idx in g.index[mask]:
            r=g.loc[idx]
            # Exact historical membership on signal date: fail closed.
            if (r["date"],code) not in membership_key: continue
            mm=mem[(mem["date"].eq(r["date"])) & (mem["code"].eq(code))]
            market=str(mm.iloc[0]["market"]) if not mm.empty else ""
            row={
                "version":VERSION,"definition":DEFINITION,"research_only":True,
                "signal_date":r["date"].strftime("%Y-%m-%d"),"code":code,"market":market,
                "open":r["open"],"high":r["high"],"low":r["low"],"close":r["close"],
                "volume":r["volume"],"amount":r["amount"],
                "ma5":r["ma5"],"ma10":r["ma10"],
                "ma5_slope_1d_pct":r["ma5_slope_1d_pct"],
                "ma5_ma10_gap_pct":r["ma5_ma10_gap_pct"],
                "pre3_below_ma5_n":r["pre3_below_ma5_n"],
                "close_loc_pct":r["close_loc_pct"],
                "upper_wick_pct":r["upper_wick_pct"],
                "gap_pct":r["gap_pct"],
                "vol20_ratio":r["vol20_ratio"],
                "amount20_ratio":r["amount20_ratio"],
                "ret5_pct":r["ret5_pct"],
            }
            row.update(outcomes(g,idx)); events.append(row)

    ev=pd.DataFrame(events)
    ev.to_csv(out/"odoli_events.csv",index=False,encoding="utf-8-sig")
    meta={
        "version":VERSION,"definition":DEFINITION,"research_only":True,"status":"PASS",
        "start":a.start,"end":a.end,"events":len(ev),
        "membership_rows":len(mem),"membership_codes":len(codes),
        "history_ok_codes":ok_codes,"history_coverage_pct":coverage,
        "historical_membership_enforced":True,
        "cross_section_ohlcv_endpoint_used":False,
        "production_logic_changed":False,"same_sample_threshold_tuning":False,
        "event_rule":{
            "bull_candle":True,"prev_close_below_prev_ma5":True,
            "d0_close_above_ma5":True,"ma5_slope_positive":True,
            "prior_3_below_ma5_min_count":2,
            "volume_is_gate":False,"amount_is_gate":False,"ma5_ma10_gap_is_gate":False,
        },
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False))

if __name__=="__main__":
    main()
