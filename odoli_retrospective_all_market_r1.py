#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
import numpy as np
import pandas as pd

VERSION="ODOLI_RETROSPECTIVE_ALL_MARKET_R1_FIX6_MARCAP_PIT_20260925"
DEFINITION="STRICT_ODOLI_R1"
RESEARCH_ONLY=True

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit(): s=s[:-2]
    if s.startswith("A") and len(s)==7: s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def sanitize(q):
    q=q.copy()
    for c in ["Open","High","Low","Close","Volume","Amount"]:
        q[c]=pd.to_numeric(q[c],errors="coerce")
    valid=(q["Open"]>0)&(q["High"]>0)&(q["Low"]>0)&(q["Close"]>0)&(q["Volume"]>=0)
    valid &= q["High"].ge(q[["Open","Close"]].max(axis=1))
    valid &= q["Low"].le(q[["Open","Close"]].min(axis=1))
    valid &= q["High"].ge(q["Low"])
    return q[valid].copy(), int((~valid).sum())

def add_features(g):
    g=g.sort_values("Date").copy()
    c=g["Close"].astype(float)
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
    g["vol20_ratio"]=g["Volume"]/g["Volume"].shift(1).rolling(20,min_periods=10).mean()
    g["amount20_ratio"]=g["Amount"]/g["Amount"].shift(1).rolling(20,min_periods=10).mean()
    den=(g["High"]-g["Low"]).replace(0,np.nan)
    g["close_loc_pct"]=(g["Close"]-g["Low"])/den*100
    g["upper_wick_pct"]=(g["High"]-g[["Open","Close"]].max(axis=1))/den*100
    g["gap_pct"]=(g["Open"]/g["prev_close"]-1)*100
    g["bull_candle"]=g["Close"]>g["Open"]
    g["odoli_r1"]=(
        g["bull_candle"]
        & (g["prev_close"]<g["prev_ma5"])
        & (g["Close"]>g["ma5"])
        & (g["ma5_slope_1d_pct"]>0)
        & (g["pre3_below_ma5_n"]>=2)
    )
    return g

def outcomes(g,idx):
    pos=g.index.get_loc(idx); entry=float(g.loc[idx,"Close"]); out={}
    for n in (1,3,5,10):
        j=pos+n
        out[f"d{n}_complete"]=bool(j<len(g))
        out[f"d{n}_close_ret_pct"]=((float(g.iloc[j]["Close"])/entry-1)*100) if j<len(g) else np.nan
        sl=g.iloc[pos+1:min(len(g),pos+n+1)]
        if len(sl):
            out[f"d{n}_mfe_pct"]=(float(sl["High"].max())/entry-1)*100
            out[f"d{n}_mae_pct"]=(float(sl["Low"].min())/entry-1)*100
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
    ap.add_argument("--marcap-root",default="vendor")
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    scan_start=pd.Timestamp(a.start).normalize()
    scan_end=pd.Timestamp(a.end).normalize()
    fetch_start=(scan_start-pd.Timedelta(days=45)).normalize()
    fetch_end=(scan_end+pd.Timedelta(days=25)).normalize()

    sys.path.insert(0,str(Path(a.marcap_root).resolve()))
    from marcap import marcap_data

    q=marcap_data(fetch_start.strftime("%Y-%m-%d"),fetch_end.strftime("%Y-%m-%d"))
    if q is None or q.empty:
        raise SystemExit("ODOLI_R1_MARCAP_EMPTY")
    q=q.reset_index() if "Date" not in q.columns else q.copy()
    if "Date" not in q.columns:
        q=q.rename(columns={q.columns[0]:"Date"})
    need=["Date","Code","Open","High","Low","Close","Volume","Amount","Market"]
    missing=[c for c in need if c not in q.columns]
    if missing:
        raise SystemExit(f"ODOLI_R1_MARCAP_SCHEMA_MISSING {missing}")
    q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
    q["Code"]=q["Code"].map(norm_code)
    q["Market"]=q["Market"].astype(str).str.upper()
    q=q[q["Market"].isin(["KOSPI","KOSDAQ"]) & q["Code"].ne("") & q["Date"].notna()].copy()
    raw_rows=len(q)
    q,bad=sanitize(q)

    coverage=pd.DataFrame({
        "date":sorted(q["Date"].dropna().unique())
    })
    coverage["rows"]=coverage["date"].map(q.groupby("Date").size())
    coverage.to_csv(out/"day_coverage.csv",index=False,encoding="utf-8-sig")

    events=[]
    for code,g0 in q.groupby("Code",sort=False):
        g=add_features(g0).reset_index(drop=True)
        mask=g["odoli_r1"] & g["Date"].between(scan_start,scan_end)
        for idx in g.index[mask]:
            r=g.loc[idx]
            row={
                "version":VERSION,"definition":DEFINITION,"research_only":True,
                "signal_date":r["Date"].strftime("%Y-%m-%d"),"code":code,
                "name":r.get("Name",""),"market":r["Market"],
                "open":r["Open"],"high":r["High"],"low":r["Low"],"close":r["Close"],
                "volume":r["Volume"],"amount":r["Amount"],
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
        "historical_membership_authority":"FINANCEDATA_MARCAP_DAILY_PIT_ROWS",
        "point_in_time_universe":True,
        "current_only_universe_used":False,
        "cross_section_network_endpoint_used":False,
        "source_raw_rows":raw_rows,
        "source_valid_rows":len(q),
        "invalid_ohlcv_rows_excluded":bad,
        "unique_codes":int(q["Code"].nunique()),
        "market_days":int(q["Date"].nunique()),
        "amount_nonnull_pct":float(pd.to_numeric(q["Amount"],errors="coerce").notna().mean()*100),
        "amount20_event_nonnull_pct":float(pd.to_numeric(ev.get("amount20_ratio"),errors="coerce").notna().mean()*100) if not ev.empty else 0.0,
        "production_logic_changed":False,
        "same_sample_threshold_tuning":False,
        "event_rule":{
            "bull_candle":True,"prev_close_below_prev_ma5":True,
            "d0_close_above_ma5":True,"ma5_slope_positive":True,
            "prior_3_below_ma5_min_count":2,
            "volume_is_gate":False,"amount_is_gate":False,"ma5_ma10_gap_is_gate":False,
        }
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False))

if __name__=="__main__":
    main()
