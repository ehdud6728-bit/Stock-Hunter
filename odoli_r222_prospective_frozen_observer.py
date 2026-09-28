#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_22_FIX1_PROSPECTIVE_FROZEN_OBSERVER_20260928"
OBSERVER_VERSION="C2_C3_C4_FROZEN_V1"
REQ=["signal_date","code"]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_signals(path):
    p=Path(path)
    if not p.exists():
        return pd.DataFrame(columns=REQ), True
    q=pd.read_csv(p,dtype={"code":str},low_memory=False)
    miss=[c for c in REQ if c not in q.columns]
    if miss:
        raise SystemExit(f"MISSING_SIGNAL_COLUMNS:{miss}")
    if len(q):
        q["signal_date"]=pd.to_datetime(q["signal_date"],errors="coerce").dt.normalize()
        q["code"]=q["code"].map(norm_code)
    return q, False

def load_marcap(root):
    fs=[]
    for p in sorted(Path(root).glob("data/marcap-*.parquet")):
        q=pd.read_parquet(p)
        if "Date" not in q.columns:
            q=q.reset_index()
        q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
        q["Code"]=q["Code"].map(norm_code)
        q=q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()
        fs.append(q)
    if not fs:
        raise SystemExit("NO_MARCAP_FILES")
    return pd.concat(fs,ignore_index=True)

def first_confirm(g,i,mode):
    close=pd.to_numeric(g["Close"],errors="coerce")
    ma5=close.rolling(5).mean()
    amount=pd.to_numeric(g["Amount"],errors="coerce")
    sig_low=float(g.iloc[i]["Low"])
    for j in range(i+4,min(len(g),i+11)):
        c=float(g.iloc[j]["Close"])
        lo=float(g.iloc[j]["Low"])
        prev_lo=float(g.iloc[j-1]["Low"])
        above=bool(pd.notna(ma5.iloc[j]) and c>float(ma5.iloc[j]))
        hl=bool(lo>prev_lo)
        pre20=amount.iloc[max(0,j-20):j]
        ama=float(pre20.mean()) if len(pre20) else np.nan
        ar=float(amount.iloc[j]/ama) if pd.notna(ama) and ama>0 else np.nan
        hist=g.iloc[i+1:j+1]
        low_survive=bool(pd.to_numeric(hist["Low"],errors="coerce").min()>=sig_low) if len(hist) else False
        ok=False
        if mode=="C2":
            ok=above and hl and low_survive
        elif mode=="C3":
            ok=above and pd.notna(ar) and ar>=1.0 and low_survive
        elif mode=="C4":
            ok=above and hl and pd.notna(ar) and ar>=1.0 and low_survive
        if ok:
            return {"confirmed":True,"confirm_day":j-i,"confirm_date":g.iloc[j]["Date"],
                    "confirm_close":c,"confirm_amount_ratio20":ar,
                    "signal_low_survived":low_survive}
    return {"confirmed":False,"confirm_day":np.nan,"confirm_date":pd.NaT,
            "confirm_close":np.nan,"confirm_amount_ratio20":np.nan,
            "signal_low_survived":False}

def forward_outcomes(g,i):
    sig=float(g.iloc[i]["Close"])
    out={}
    for h in [5,10,20,40,60]:
        q=g.iloc[i+1:min(len(g),i+h+1)].copy()
        complete=len(q)>=h
        out[f"d{h}_complete"]=complete
        if not complete:
            for t in [5,10,15]:
                out[f"d{h}_touch_{t}"]=np.nan
            out[f"d{h}_mfe_pct"]=np.nan
            out[f"d{h}_mae_pct"]=np.nan
            continue
        hr=(pd.to_numeric(q["High"],errors="coerce")/sig-1)*100
        lr=(pd.to_numeric(q["Low"],errors="coerce")/sig-1)*100
        out[f"d{h}_mfe_pct"]=float(hr.max())
        out[f"d{h}_mae_pct"]=float(lr.min())
        for t in [5,10,15]:
            out[f"d{h}_touch_{t}"]=bool(hr.ge(t).any())
    return out

def empty_summary():
    cols=["observer","ok_signal_n","confirmed_n","confirm_rate_pct","confirm_day_median"]
    for h in [20,40,60]:
        cols.append(f"d{h}_complete_n")
        for t in [10,15]:
            cols.append(f"d{h}_touch{t}_rate_pct")
    return pd.DataFrame(columns=cols)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--signals-csv",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()

    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    s,missing_file=load_signals(a.signals_csv)
    mar=load_marcap(a.marcap_root)

    if len(s)==0:
        ledger=pd.DataFrame(columns=REQ+["observer_version","status"])
        ledger.to_csv(out/"r222_prospective_observer_ledger.csv",index=False,encoding="utf-8-sig")
        summary=empty_summary()
        summary.to_csv(out/"r222_observer_summary.csv",index=False,encoding="utf-8-sig")
        meta={
            "revision":REV,"observer_version":OBSERVER_VERSION,
            "research_only":True,"production_logic_changed":False,
            "same_sample_tuning":False,"new_gate_created":False,
            "signals_source":a.signals_csv,
            "signals_file_missing":missing_file,
            "bootstrap_empty":True,"prospective_rows":0,
            "profit_authority":"INTRAPERIOD_HIGH_TOUCH_MFE",
            "promotion_policy":"NO_PROMOTION_UNTIL_PROSPECTIVE_SAMPLE_ACCUMULATES",
            "no_backfill_as_prospective":True
        }
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        (out/"REPORT.md").write_text(
            "# ODOLI R2.22 FIX1 — Prospective Frozen Observer\n\n"
            "- Observer initialized successfully.\n"
            "- Prospective rows: 0\n"
            f"- Signal source: `{a.signals_csv}`\n"
            "- No retrospective backfill was inserted.\n"
            "- Waiting for genuinely new frozen prospective signals.\n",
            encoding="utf-8"
        )
        return

    bycode={c:g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
            for c,g in mar.groupby("Code",sort=False)}
    rows=[]
    for _,r in s.iterrows():
        rec=r.to_dict();rec["observer_version"]=OBSERVER_VERSION
        g=bycode.get(r["code"])
        if g is None:
            rec["status"]="NO_HISTORY";rows.append(rec);continue
        hit=g.index[g["Date"].eq(r["signal_date"])]
        if len(hit)!=1:
            rec["status"]="NO_SIGNAL_DATE";rows.append(rec);continue
        i=int(hit[0]);rec["status"]="OK"
        for mode in ["C2","C3","C4"]:
            x=first_confirm(g,i,mode)
            for k,v in x.items():
                rec[f"{mode}_{k}"]=v
        rec.update(forward_outcomes(g,i))
        rows.append(rec)

    z=pd.DataFrame(rows)
    z.to_csv(out/"r222_prospective_observer_ledger.csv",index=False,encoding="utf-8-sig")

    rr=[]
    for mode in ["C2","C3","C4"]:
        ok=z[z["status"].eq("OK")].copy()
        flag=ok[f"{mode}_confirmed"].fillna(False).astype(bool)
        conf=ok[flag].copy()
        row={"observer":mode,"ok_signal_n":len(ok),"confirmed_n":len(conf),
             "confirm_rate_pct":float(flag.mean()*100) if len(ok) else np.nan,
             "confirm_day_median":float(pd.to_numeric(conf[f"{mode}_confirm_day"],errors="coerce").median()) if len(conf) else np.nan}
        for h in [20,40,60]:
            complete=conf[conf[f"d{h}_complete"].fillna(False).astype(bool)]
            row[f"d{h}_complete_n"]=len(complete)
            for t in [10,15]:
                c=f"d{h}_touch_{t}"
                x=complete[c].dropna().astype(bool) if c in complete.columns else pd.Series(dtype=bool)
                row[f"d{h}_touch{t}_rate_pct"]=float(x.mean()*100) if len(x) else np.nan
        rr.append(row)
    summary=pd.DataFrame(rr)
    summary.to_csv(out/"r222_observer_summary.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REV,"observer_version":OBSERVER_VERSION,
        "research_only":True,"production_logic_changed":False,
        "same_sample_tuning":False,"new_gate_created":False,
        "signals_source":a.signals_csv,"signals_file_missing":False,
        "bootstrap_empty":False,"prospective_rows":len(z),
        "profit_authority":"INTRAPERIOD_HIGH_TOUCH_MFE",
        "promotion_policy":"NO_PROMOTION_UNTIL_PROSPECTIVE_SAMPLE_ACCUMULATES",
        "no_backfill_as_prospective":True
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    (out/"REPORT.md").write_text(
        "# ODOLI R2.22 FIX1 — Prospective Frozen Observer\n\n"
        f"- Prospective rows: {len(z)}\n"
        f"- Signal source: `{a.signals_csv}`\n"
        "- C2/C3/C4 remain frozen.\n",
        encoding="utf-8"
    )

if __name__=="__main__":
    main()
