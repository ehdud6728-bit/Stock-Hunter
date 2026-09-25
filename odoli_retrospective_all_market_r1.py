#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math, time
from pathlib import Path
import numpy as np
import pandas as pd

VERSION="ODOLI_RETROSPECTIVE_ALL_MARKET_R1_FIX1_20260925"
DEFINITION="STRICT_ODOLI_R1"
RESEARCH_ONLY=True

def norm_code(v):
    s=str(v or "").strip().upper().replace(".KS","").replace(".KQ","")
    if s.endswith(".0") and s[:-2].isdigit(): s=s[:-2]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def pick(df,names):
    for n in names:
        if n in df.columns:return n
    return None

def normalize(raw,market,date):
    if raw is None or raw.empty:return pd.DataFrame()
    q=raw.copy()
    if not isinstance(q.index,pd.RangeIndex) or q.index.name is not None:q=q.reset_index()
    cc=pick(q,["티커","ticker","종목코드","Code","code","index"]) or q.columns[0]
    out=pd.DataFrame({"date":pd.Timestamp(date).normalize(),"code":q[cc].map(norm_code),"market":market})
    mp={
        "open":["시가","Open","open"],"high":["고가","High","high"],"low":["저가","Low","low"],
        "close":["종가","Close","close"],"volume":["거래량","Volume","volume"],
        "amount":["거래대금","Amount","amount","거래대금(원)"],
    }
    for dst,names in mp.items():
        c=pick(q,names); out[dst]=pd.to_numeric(q[c],errors="coerce") if c else np.nan
    # ODOLI and MFE/MAE need real OHLC, not a close-only fallback.
    need=["open","high","low","close","volume"]
    ok=pd.Series(True,index=out.index)
    for c in need: ok &= out[c].notna()
    out=out[ok & out["code"].ne("") & out["close"].gt(0)].copy()
    return out.drop_duplicates("code",keep="last")

def _cache_path(cache,ymd):
    return cache/"ALL"/f"{ymd}.csv.gz"

def fetch_day(stock,ymd,cache):
    p=_cache_path(cache,ymd)
    if p.exists():
        try:
            q=pd.read_csv(p,dtype={"code":str},parse_dates=["date"])
            if not q.empty:return q,"CACHE"
        except Exception:pass

    attempts=[]
    candidates=[]

    # 1) Current pykrx preferred API: one all-market OHLCV cross-section.
    f=getattr(stock,"get_market_ohlcv",None)
    if callable(f):
        for args,kwargs,label in [
            ((ymd,),{"market":"ALL"},"get_market_ohlcv:ALL"),
            ((ymd,"ALL"),{},"get_market_ohlcv:ALL-positional"),
        ]:
            try:
                raw=f(*args,**kwargs)
                z=normalize(raw,"ALL",ymd)
                attempts.append({"source":label,"rows":len(z),"error":""})
                if not z.empty:
                    candidates.append((z,label)); break
            except Exception as e:
                attempts.append({"source":label,"rows":0,"error":f"{type(e).__name__}:{e}"})

    # 2) Compatibility alias: all-market by-ticker.
    if not candidates:
        f=getattr(stock,"get_market_ohlcv_by_ticker",None)
        if callable(f):
            for args,kwargs,label in [
                ((ymd,),{"market":"ALL"},"get_market_ohlcv_by_ticker:ALL"),
                ((),{"date":ymd,"market":"ALL"},"get_market_ohlcv_by_ticker:ALL-date"),
            ]:
                try:
                    raw=f(*args,**kwargs)
                    z=normalize(raw,"ALL",ymd)
                    attempts.append({"source":label,"rows":len(z),"error":""})
                    if not z.empty:
                        candidates.append((z,label)); break
                except Exception as e:
                    attempts.append({"source":label,"rows":0,"error":f"{type(e).__name__}:{e}"})

    # 3) Market-specific current API, only if ALL failed.
    if not candidates:
        frames=[]; labels=[]
        f=getattr(stock,"get_market_ohlcv",None)
        if callable(f):
            for market in ("KOSPI","KOSDAQ"):
                try:
                    raw=f(ymd,market=market)
                    z=normalize(raw,market,ymd)
                    attempts.append({"source":f"get_market_ohlcv:{market}","rows":len(z),"error":""})
                    if not z.empty:
                        frames.append(z); labels.append(market)
                except Exception as e:
                    attempts.append({"source":f"get_market_ohlcv:{market}","rows":0,"error":f"{type(e).__name__}:{e}"})
        if frames:
            candidates.append((pd.concat(frames,ignore_index=True).drop_duplicates("code",keep="last"),
                               "get_market_ohlcv:KOSPI+KOSDAQ"))

    # 4) Market-specific legacy alias last.
    if not candidates:
        frames=[]
        f=getattr(stock,"get_market_ohlcv_by_ticker",None)
        if callable(f):
            for market in ("KOSPI","KOSDAQ"):
                try:
                    raw=f(ymd,market=market)
                    z=normalize(raw,market,ymd)
                    attempts.append({"source":f"get_market_ohlcv_by_ticker:{market}","rows":len(z),"error":""})
                    if not z.empty:frames.append(z)
                except Exception as e:
                    attempts.append({"source":f"get_market_ohlcv_by_ticker:{market}","rows":0,"error":f"{type(e).__name__}:{e}"})
        if frames:
            candidates.append((pd.concat(frames,ignore_index=True).drop_duplicates("code",keep="last"),
                               "get_market_ohlcv_by_ticker:KOSPI+KOSDAQ"))

    if not candidates:return pd.DataFrame(), "NO_DATA", attempts

    z,src=candidates[0]
    p.parent.mkdir(parents=True,exist_ok=True)
    z.to_csv(p,index=False,compression="gzip")
    return z,src,attempts

def add_features(g):
    g=g.sort_values("date").copy()
    c=g["close"].astype(float)
    g["ma5"]=c.rolling(5,min_periods=5).mean()
    g["ma10"]=c.rolling(10,min_periods=10).mean()
    g["prev_close"]=c.shift(1)
    g["prev_ma5"]=g["ma5"].shift(1)
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
    pos=g.index.get_loc(idx)
    entry=float(g.loc[idx,"close"])
    out={}
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
    ap.add_argument("--cache-dir",default="reports/.cache/odoli_all_market_r1")
    ap.add_argument("--sleep",type=float,default=0.08)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    cache=Path(a.cache_dir); cache.mkdir(parents=True,exist_ok=True)
    scan_start=pd.Timestamp(a.start); scan_end=pd.Timestamp(a.end)
    fetch_start=scan_start-pd.Timedelta(days=45)
    fetch_end=scan_end+pd.Timedelta(days=25)

    from pykrx import stock
    frames=[]; day_diag=[]; fetch_attempts=[]
    for d in pd.date_range(fetch_start,fetch_end,freq="D"):
        # weekends are known non-trading days: avoid pointless network calls.
        if d.weekday()>=5:continue
        y=d.strftime("%Y%m%d")
        z,src,attempts=fetch_day(stock,y,cache)
        fetch_attempts.extend([{"date":d.strftime("%Y-%m-%d"),**x} for x in attempts])
        if not z.empty:
            frames.append(z)
            day_diag.append({"date":d.strftime("%Y-%m-%d"),"rows":len(z),"source":src})
        time.sleep(max(0,a.sleep))

    pd.DataFrame(day_diag).to_csv(out/"day_coverage.csv",index=False,encoding="utf-8-sig")
    pd.DataFrame(fetch_attempts).to_csv(out/"fetch_diagnostics.csv",index=False,encoding="utf-8-sig")

    if not frames:
        meta={
            "version":VERSION,"definition":DEFINITION,"research_only":True,
            "start":a.start,"end":a.end,"events":0,
            "status":"NO_MARKET_DATA","production_logic_changed":False,
            "same_sample_threshold_tuning":False,
        }
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        raise SystemExit("ODOLI_R1_NO_MARKET_DATA")

    panel=pd.concat(frames,ignore_index=True)
    panel["date"]=pd.to_datetime(panel["date"])
    events=[]
    for code,g0 in panel.groupby("code",sort=False):
        g=add_features(g0).reset_index(drop=True)
        mask=g["odoli_r1"] & g["date"].between(scan_start,scan_end)
        for idx in g.index[mask]:
            r=g.loc[idx]
            row={
                "version":VERSION,"definition":DEFINITION,"research_only":True,
                "signal_date":r["date"].strftime("%Y-%m-%d"),"code":code,"market":r["market"],
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
        "version":VERSION,"definition":DEFINITION,"research_only":True,
        "start":a.start,"end":a.end,"events":len(ev),"status":"PASS",
        "market_days_loaded":len(day_diag),
        "production_logic_changed":False,
        "same_sample_threshold_tuning":False,
        "event_rule":{
            "bull_candle":True,"prev_close_below_prev_ma5":True,
            "d0_close_above_ma5":True,"ma5_slope_positive":True,
            "prior_3_below_ma5_min_count":2,
            "volume_is_gate":False,"amount_is_gate":False,"ma5_ma10_gap_is_gate":False,
        },
        "notes":[
            "All-market OHLCV is attempted first to reduce endpoint pressure.",
            "No close-only fallback is allowed because ODOLI and MFE/MAE require actual OHLC.",
            "No current-list survivorship universe is substituted.",
            "Outcome metrics are descriptive research outputs only."
        ],
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False))

if __name__=="__main__":
    main()
