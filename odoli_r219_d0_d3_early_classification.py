#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_19_D0_D3_EARLY_CLASSIFICATION_ANATOMY_20260928"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs:
        raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_marcap(root):
    fs=[]
    for y in [2024,2025,2026]:
        p=Path(root)/"data"/f"marcap-{y}.parquet"
        if not p.exists():
            raise SystemExit(f"MISSING_MARCAP:{p}")
        q=pd.read_parquet(p)
        if "Date" not in q.columns:
            q=q.reset_index()
        q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
        q["Code"]=q["Code"].map(norm_code)
        q=q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()
        fs.append(q)
    return pd.concat(fs,ignore_index=True)

def pct(a,b):
    if pd.isna(a) or pd.isna(b) or b==0:return np.nan
    return (a/b-1)*100

def early_path(g,i):
    sig=g.iloc[i]
    sig_close=float(sig["Close"]); sig_low=float(sig["Low"]); sig_high=float(sig["High"])
    close=pd.to_numeric(g["Close"],errors="coerce")
    ma5=close.rolling(5).mean()
    pre20=g.iloc[max(0,i-20):i]
    v20=float(pd.to_numeric(pre20["Volume"],errors="coerce").mean()) if len(pre20) else np.nan
    a20=float(pd.to_numeric(pre20["Amount"],errors="coerce").mean()) if "Amount" in pre20 and len(pre20) else np.nan

    rows=[]
    for d in [1,2,3]:
        j=i+d
        if j>=len(g): break
        r=g.iloc[j]
        o,h,l,c=[float(r[x]) for x in ["Open","High","Low","Close"]]
        rng=max(h-l,1e-9)
        vr=float(r["Volume"])/v20 if pd.notna(v20) and v20>0 else np.nan
        ar=float(r["Amount"])/a20 if "Amount" in r.index and pd.notna(a20) and a20>0 else np.nan
        rows.append({
            "d":d,
            "open_gap_pct":pct(o,sig_close),
            "high_ret_pct":pct(h,sig_close),
            "low_ret_pct":pct(l,sig_close),
            "close_ret_pct":pct(c,sig_close),
            "close_loc":(c-l)/rng,
            "volume_ratio20":vr,
            "amount_ratio20":ar,
            "low":l,
            "close":c,
            "above_ma5":bool(c>ma5.iloc[j]) if pd.notna(ma5.iloc[j]) else np.nan
        })

    p=pd.DataFrame(rows)
    out={}
    for d in [1,2,3]:
        q=p[p["d"].eq(d)] if len(p) else p
        if len(q)==1:
            r=q.iloc[0]
            for c in ["open_gap_pct","high_ret_pct","low_ret_pct","close_ret_pct","close_loc",
                      "volume_ratio20","amount_ratio20"]:
                out[f"d{d}_{c}"]=r[c]
            out[f"d{d}_holds_signal_low"]=bool(r["low"]>=sig_low)
            out[f"d{d}_close_above_signal"]=bool(r["close"]>sig_close)
            out[f"d{d}_high_break_signal_high"]=bool((sig_high>0) and
                                                       (sig_high <= sig_high) and
                                                       (float(g.iloc[i+d]["High"])>sig_high))
            out[f"d{d}_above_ma5"]=r["above_ma5"]
        else:
            for c in ["open_gap_pct","high_ret_pct","low_ret_pct","close_ret_pct","close_loc",
                      "volume_ratio20","amount_ratio20"]:
                out[f"d{d}_{c}"]=np.nan
            for c in ["holds_signal_low","close_above_signal","high_break_signal_high","above_ma5"]:
                out[f"d{d}_{c}"]=np.nan

    if len(p)>=2:
        out["d1_to_d2_higher_low"]=bool(float(p.iloc[1]["low"])>float(p.iloc[0]["low"]))
    else:
        out["d1_to_d2_higher_low"]=np.nan
    if len(p)>=3:
        out["d2_to_d3_higher_low"]=bool(float(p.iloc[2]["low"])>float(p.iloc[1]["low"]))
        out["d1_d3_all_hold_signal_low"]=bool((p.iloc[:3]["low"]>=sig_low).all())
        x=p.iloc[:3]["above_ma5"].dropna()
        out["d1_d3_all_above_ma5"]=bool(x.astype(bool).all()) if len(x)==3 else np.nan
        out["d1_d3_mfe_pct"]=float(p.iloc[:3]["high_ret_pct"].max())
        out["d1_d3_mae_pct"]=float(p.iloc[:3]["low_ret_pct"].min())
    else:
        out["d2_to_d3_higher_low"]=np.nan
        out["d1_d3_all_hold_signal_low"]=np.nan
        out["d1_d3_all_above_ma5"]=np.nan
        out["d1_d3_mfe_pct"]=np.nan
        out["d1_d3_mae_pct"]=np.nan
    return out

def med(g,c):
    if c not in g.columns:return np.nan
    x=pd.to_numeric(g[c],errors="coerce").dropna()
    return float(x.median()) if len(x) else np.nan

def mean(g,c):
    if c not in g.columns:return np.nan
    x=pd.to_numeric(g[c],errors="coerce").dropna()
    return float(x.mean()) if len(x) else np.nan

def brate(g,c):
    if c not in g.columns:return np.nan
    x=g[c].dropna().astype(bool)
    return float(x.mean()*100) if len(x) else np.nan

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r218-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)

    z=pd.read_csv(find_one(a.r218_root,"r218_event_speed_classes.csv"),dtype={"code":str},low_memory=False)
    if len(z)!=100:
        raise SystemExit(f"COHORT_MISMATCH:{len(z)}")
    z["code"]=z["code"].map(norm_code)
    z["signal_date"]=pd.to_datetime(z["signal_date"],errors="coerce").dt.normalize()

    mar=load_marcap(a.marcap_root)
    bycode={c:g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
            for c,g in mar.groupby("Code",sort=False)}

    extras=[]
    for _,r in z.iterrows():
        rec={"signal_date":r["signal_date"],"code":r["code"]}
        g=bycode.get(r["code"])
        if g is None:
            rec["early_status"]="NO_HISTORY";extras.append(rec);continue
        hit=g.index[g["Date"].eq(r["signal_date"])]
        if len(hit)!=1:
            rec["early_status"]="NO_SIGNAL_DATE";extras.append(rec);continue
        rec["early_status"]="OK"
        rec.update(early_path(g,int(hit[0])))
        extras.append(rec)

    e=pd.DataFrame(extras)
    z=z.merge(e,on=["signal_date","code"],how="left",validate="one_to_one")
    z.to_csv(out/"r219_event_d0_d3_features.csv",index=False,encoding="utf-8-sig")

    groups=["FAST_D1_10","MID_D11_20","SLOW_D21_60","NO_PLUS15_D60","PENDING_D60"]
    d0_num=[
        "candle_only_score","accum_volume_ratio20","accum_amount_ratio20",
        "pullback_volume_vs_accum","pullback_amount_vs_accum","range_contract_3v10",
        "signal_volume_ratio20","signal_amount_ratio20","sequence_component_count"
    ]
    early_num=[]
    for d in [1,2,3]:
        early_num += [
            f"d{d}_open_gap_pct",f"d{d}_high_ret_pct",f"d{d}_low_ret_pct",
            f"d{d}_close_ret_pct",f"d{d}_close_loc",
            f"d{d}_volume_ratio20",f"d{d}_amount_ratio20"
        ]
    early_num += ["d1_d3_mfe_pct","d1_d3_mae_pct"]
    early_bool=[
        "d1_holds_signal_low","d2_holds_signal_low","d3_holds_signal_low",
        "d1_close_above_signal","d2_close_above_signal","d3_close_above_signal",
        "d1_high_break_signal_high","d2_high_break_signal_high","d3_high_break_signal_high",
        "d1_above_ma5","d2_above_ma5","d3_above_ma5",
        "d1_to_d2_higher_low","d2_to_d3_higher_low",
        "d1_d3_all_hold_signal_low","d1_d3_all_above_ma5"
    ]

    rows=[]
    for label in groups:
        g=z[z["plus15_speed_class"].eq(label)].copy()
        row={"group":label,"n":len(g),"rate_pct":float(len(g)/len(z)*100)}
        for c in d0_num+early_num:
            row[c+"_median"]=med(g,c)
            row[c+"_mean"]=mean(g,c)
        for c in early_bool:
            row[c+"_rate_pct"]=brate(g,c)
        rows.append(row)
    summary=pd.DataFrame(rows)
    summary.to_csv(out/"r219_group_d0_d3_anatomy.csv",index=False,encoding="utf-8-sig")

    # Stage-specific compact board for easier interpretation.
    compact=[]
    for r in rows:
        compact.append({
            "group":r["group"],"n":r["n"],
            "D0_accum_amount_x_med":r.get("accum_amount_ratio20_median"),
            "D0_pullback_amount_vs_accum_med":r.get("pullback_amount_vs_accum_median"),
            "D0_signal_amount_x_med":r.get("signal_amount_ratio20_median"),
            "D1_close_ret_med":r.get("d1_close_ret_pct_median"),
            "D1_low_ret_med":r.get("d1_low_ret_pct_median"),
            "D1_amount_x_med":r.get("d1_amount_ratio20_median"),
            "D2_close_ret_med":r.get("d2_close_ret_pct_median"),
            "D2_low_ret_med":r.get("d2_low_ret_pct_median"),
            "D1_D2_higher_low_rate":r.get("d1_to_d2_higher_low_rate_pct"),
            "D2_above_MA5_rate":r.get("d2_above_ma5_rate_pct"),
            "D1_D3_all_MA5_rate":r.get("d1_d3_all_above_ma5_rate_pct"),
            "D1_D3_MAE_med":r.get("d1_d3_mae_pct_median"),
            "D1_D3_MFE_med":r.get("d1_d3_mfe_pct_median"),
        })
    compact=pd.DataFrame(compact)
    compact.to_csv(out/"r219_stage_compact_board.csv",index=False,encoding="utf-8-sig")

    # Pairwise descriptive deltas, not thresholds.
    by={r["group"]:r for r in rows}
    pairs=[
        ("FAST_D1_10","NO_PLUS15_D60"),
        ("SLOW_D21_60","NO_PLUS15_D60"),
        ("FAST_D1_10","SLOW_D21_60"),
        ("MID_D11_20","NO_PLUS15_D60")
    ]
    focus=[
        "signal_amount_ratio20_median",
        "pullback_amount_vs_accum_median",
        "d1_close_ret_pct_median","d1_low_ret_pct_median","d1_amount_ratio20_median",
        "d2_close_ret_pct_median","d2_low_ret_pct_median",
        "d1_to_d2_higher_low_rate_pct","d2_above_ma5_rate_pct",
        "d1_d3_all_above_ma5_rate_pct","d1_d3_mae_pct_median"
    ]
    dr=[]
    for ga,gb in pairs:
        arow=by.get(ga,{});brow=by.get(gb,{})
        rec={"group_a":ga,"group_b":gb}
        for c in focus:
            va=arow.get(c,np.nan);vb=brow.get(c,np.nan)
            rec[c+"_delta_a_minus_b"]=float(va-vb) if pd.notna(va) and pd.notna(vb) else np.nan
        dr.append(rec)
    pd.DataFrame(dr).to_csv(out/"r219_pairwise_deltas.csv",index=False,encoding="utf-8-sig")

    # Manual review ledger: only information available by D3 plus later class label.
    led=["signal_date","code","name","period","plus15_speed_class","plus15_first_touch_day"]
    led += [c for c in d0_num+early_num+early_bool if c in z.columns]
    z[led].sort_values(["plus15_speed_class","signal_date"]).to_csv(
        out/"r219_manual_review_ledger.csv",index=False,encoding="utf-8-sig"
    )

    meta={
        "revision":REV,
        "research_only":True,
        "production_logic_changed":False,
        "same_sample_tuning":False,
        "new_gate_created":False,
        "cohort_n":len(z),
        "future_label":"PLUS15_SPEED_CLASS",
        "feature_cutoff":"D3_ONLY",
        "profit_authority":"INTRAPERIOD_HIGH_TOUCH_MFE",
        "purpose":"DESCRIPTIVE_EARLY_CLASSIFICATION_ANATOMY_NOT_MODEL_TRAINING",
        "warning":"No same-sample classifier thresholds may be promoted from this output."
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "# ODOLI R2.19 — D0~D3 Early Classification Anatomy","",
        "- Frozen 100-event cohort from R2.18.",
        "- Future label: +15 High-touch speed class.",
        "- Features are limited to information available by D3.",
        "- No classifier is trained; no thresholds are optimized.",
        "- Goal: determine whether FAST / SLOW / NO show distinct early behavior.","",
        "## Compact stage board","```",compact.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
