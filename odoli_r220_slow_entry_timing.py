#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_20_SLOW_VS_NO_STRUCTURAL_SURVIVAL_ENTRY_TIMING_20260928"
TARGETS=[5,10,15]

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

def find_confirmations(g,i):
    close=pd.to_numeric(g["Close"],errors="coerce")
    ma5=close.rolling(5).mean()
    amount=pd.to_numeric(g["Amount"],errors="coerce")
    out={}
    for variant in ["MA5_RECLAIM","HL_MA5","AMOUNT_MA5"]:
        out[variant]=None
    for j in range(i+4, min(len(g),i+11)):
        c=float(g.iloc[j]["Close"])
        lo=float(g.iloc[j]["Low"])
        prev_lo=float(g.iloc[j-1]["Low"])
        above=bool(pd.notna(ma5.iloc[j]) and c>float(ma5.iloc[j]))
        hl=bool(lo>prev_lo)
        pre20=amount.iloc[max(0,j-20):j]
        ama=float(pre20.mean()) if len(pre20) else np.nan
        ar=float(amount.iloc[j]/ama) if pd.notna(ama) and ama>0 else np.nan
        if out["MA5_RECLAIM"] is None and above:
            out["MA5_RECLAIM"]=(j,j-i,ar)
        if out["HL_MA5"] is None and above and hl:
            out["HL_MA5"]=(j,j-i,ar)
        if out["AMOUNT_MA5"] is None and above and pd.notna(ar) and ar>=1.0:
            out["AMOUNT_MA5"]=(j,j-i,ar)
    return out

def post_entry_metrics(g,entry_i,entry_price,end_i):
    # Strictly after the close used for entry. No same-day high/low look-ahead.
    q=g.iloc[entry_i+1:min(len(g),end_i+1)].copy()
    out={"bars_after_entry":len(q)}
    if not len(q):
        for c in ["mfe_pct","mae_pct","last_close_ret_pct"]:
            out[c]=np.nan
        for t in TARGETS:
            out[f"touch_{t}"]=np.nan
            out[f"first_touch_bars_{t}"]=np.nan
        return out
    hr=(pd.to_numeric(q["High"],errors="coerce")/entry_price-1)*100
    lr=(pd.to_numeric(q["Low"],errors="coerce")/entry_price-1)*100
    cr=(pd.to_numeric(q["Close"],errors="coerce")/entry_price-1)*100
    out["mfe_pct"]=float(hr.max())
    out["mae_pct"]=float(lr.min())
    out["last_close_ret_pct"]=float(cr.iloc[-1])
    for t in TARGETS:
        hit=hr.ge(t)
        out[f"touch_{t}"]=bool(hit.any())
        out[f"first_touch_bars_{t}"]=int(np.argmax(hit.to_numpy())+1) if hit.any() else np.nan
    return out

def d0_metrics(g,i,end_i):
    p=float(g.iloc[i]["Close"])
    return post_entry_metrics(g,i,p,end_i)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r219-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)

    z=pd.read_csv(find_one(a.r219_root,"r219_event_d0_d3_features.csv"),dtype={"code":str},low_memory=False)
    if len(z)!=100:
        raise SystemExit(f"COHORT_MISMATCH:{len(z)}")
    z["code"]=z["code"].map(norm_code)
    z["signal_date"]=pd.to_datetime(z["signal_date"],errors="coerce").dt.normalize()

    # Primary comparison only SLOW vs NO. FAST/MID retained in event ledger for context.
    mar=load_marcap(a.marcap_root)
    bycode={c:g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
            for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    for _,r in z.iterrows():
        rec={
            "signal_date":r["signal_date"],"code":r["code"],"name":r.get("name"),
            "period":r.get("period"),"plus15_speed_class":r.get("plus15_speed_class")
        }
        g=bycode.get(r["code"])
        if g is None:
            rec["status"]="NO_HISTORY";rows.append(rec);continue
        hit=g.index[g["Date"].eq(r["signal_date"])]
        if len(hit)!=1:
            rec["status"]="NO_SIGNAL_DATE";rows.append(rec);continue
        i=int(hit[0]); rec["status"]="OK"
        end_i=min(len(g)-1,i+60)
        sig_close=float(g.iloc[i]["Close"])
        sig_low=float(g.iloc[i]["Low"])

        base=d0_metrics(g,i,end_i)
        for k,v in base.items():
            rec["D0_FULL_"+k]=v

        cons=find_confirmations(g,i)
        for variant,x in cons.items():
            pref=variant
            if x is None:
                rec[pref+"_confirmed"]=False
                rec[pref+"_confirm_day"]=np.nan
                rec[pref+"_confirm_close"]=np.nan
                rec[pref+"_confirm_amount_ratio20"]=np.nan
                for k in ["bars_after_entry","mfe_pct","mae_pct","last_close_ret_pct"]:
                    rec[pref+"_DELAY_"+k]=np.nan
                    rec[pref+"_SPLIT_"+k]=np.nan
                for t in TARGETS:
                    rec[pref+"_DELAY_touch_"+str(t)]=np.nan
                    rec[pref+"_DELAY_first_touch_bars_"+str(t)]=np.nan
                    rec[pref+"_SPLIT_touch_"+str(t)]=np.nan
                    rec[pref+"_SPLIT_first_touch_bars_"+str(t)]=np.nan
                rec[pref+"_split_avg_entry"]=np.nan
                rec[pref+"_preconfirm_low_ret_from_d0"]=np.nan
                continue

            j,day,ar=x
            cp=float(g.iloc[j]["Close"])
            rec[pref+"_confirmed"]=True
            rec[pref+"_confirm_day"]=day
            rec[pref+"_confirm_close"]=cp
            rec[pref+"_confirm_amount_ratio20"]=ar

            # Delayed full entry at confirmation close.
            dm=post_entry_metrics(g,j,cp,end_i)
            for k,v in dm.items():
                rec[pref+"_DELAY_"+k]=v

            # Split: 1/3 at D0 close, 2/3 at confirmation close.
            avg=(sig_close+2*cp)/3.0
            rec[pref+"_split_avg_entry"]=avg
            sm=post_entry_metrics(g,j,avg,end_i)
            for k,v in sm.items():
                rec[pref+"_SPLIT_"+k]=v

            pre=g.iloc[i+1:j+1]
            rec[pref+"_preconfirm_low_ret_from_d0"]=float(
                (pd.to_numeric(pre["Low"],errors="coerce").min()/sig_close-1)*100
            ) if len(pre) else np.nan
            rec[pref+"_signal_low_survived_to_confirm"]=bool(
                pd.to_numeric(pre["Low"],errors="coerce").min()>=sig_low
            ) if len(pre) else np.nan

        rows.append(rec)

    e=pd.DataFrame(rows)
    e.to_csv(out/"r220_entry_simulation_events.csv",index=False,encoding="utf-8-sig")

    # Summary by future class and confirmation variant.
    ss=[]
    for cls in ["SLOW_D21_60","NO_PLUS15_D60","FAST_D1_10","MID_D11_20"]:
        gc=e[e["plus15_speed_class"].eq(cls)].copy()
        for variant in ["MA5_RECLAIM","HL_MA5","AMOUNT_MA5"]:
            conf=gc[gc[variant+"_confirmed"].fillna(False).astype(bool)].copy()
            row={
                "class":cls,"variant":variant,"class_n":len(gc),
                "confirmed_n":len(conf),
                "confirm_rate_pct":float(len(conf)/len(gc)*100) if len(gc) else np.nan,
                "confirm_day_median":float(pd.to_numeric(conf[variant+"_confirm_day"],errors="coerce").median()) if len(conf) else np.nan,
                "preconfirm_low_ret_from_d0_median":float(pd.to_numeric(conf[variant+"_preconfirm_low_ret_from_d0"],errors="coerce").median()) if len(conf) else np.nan,
                "d0_full_mfe_median":float(pd.to_numeric(gc["D0_FULL_mfe_pct"],errors="coerce").median()) if len(gc) else np.nan,
                "d0_full_mae_median":float(pd.to_numeric(gc["D0_FULL_mae_pct"],errors="coerce").median()) if len(gc) else np.nan,
            }
            for mode in ["DELAY","SPLIT"]:
                for metric in ["mfe_pct","mae_pct","last_close_ret_pct"]:
                    c=f"{variant}_{mode}_{metric}"
                    row[f"{mode.lower()}_{metric}_median"]=float(pd.to_numeric(conf[c],errors="coerce").median()) if len(conf) else np.nan
                for t in TARGETS:
                    c=f"{variant}_{mode}_touch_{t}"
                    x=conf[c].dropna().astype(bool) if c in conf.columns else pd.Series(dtype=bool)
                    row[f"{mode.lower()}_touch{t}_rate_pct"]=float(x.mean()*100) if len(x) else np.nan
            ss.append(row)
    summary=pd.DataFrame(ss)
    summary.to_csv(out/"r220_entry_strategy_summary.csv",index=False,encoding="utf-8-sig")

    # Primary SLOW vs NO survival board.
    board=[]
    for cls in ["SLOW_D21_60","NO_PLUS15_D60"]:
        gc=e[e["plus15_speed_class"].eq(cls)].copy()
        rec={"class":cls,"n":len(gc)}
        for variant in ["MA5_RECLAIM","HL_MA5","AMOUNT_MA5"]:
            x=gc[variant+"_confirmed"].fillna(False).astype(bool)
            rec[variant+"_confirm_rate_pct"]=float(x.mean()*100) if len(x) else np.nan
            conf=gc[x]
            rec[variant+"_confirm_day_med"]=float(pd.to_numeric(conf[variant+"_confirm_day"],errors="coerce").median()) if len(conf) else np.nan
            rec[variant+"_delay_mae_med"]=float(pd.to_numeric(conf[variant+"_DELAY_mae_pct"],errors="coerce").median()) if len(conf) else np.nan
            rec[variant+"_split_mae_med"]=float(pd.to_numeric(conf[variant+"_SPLIT_mae_pct"],errors="coerce").median()) if len(conf) else np.nan
            rec[variant+"_delay_touch15_rate"]=float(conf[variant+"_DELAY_touch_15"].dropna().astype(bool).mean()*100) if len(conf) else np.nan
            rec[variant+"_split_touch15_rate"]=float(conf[variant+"_SPLIT_touch_15"].dropna().astype(bool).mean()*100) if len(conf) else np.nan
        board.append(rec)
    board=pd.DataFrame(board)
    board.to_csv(out/"r220_slow_vs_no_survival_board.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REV,
        "research_only":True,
        "production_logic_changed":False,
        "same_sample_tuning":False,
        "new_gate_created":False,
        "cohort_n":len(e),
        "primary_classes":["SLOW_D21_60","NO_PLUS15_D60"],
        "entry_modes":["D0_FULL","D0_ONE_THIRD_PLUS_CONFIRM_TWO_THIRDS","CONFIRMATION_CLOSE_FULL"],
        "confirmation_variants":{
            "MA5_RECLAIM":"first D4-D10 close above contemporaneous MA5",
            "HL_MA5":"first D4-D10 close above MA5 and low above prior-day low",
            "AMOUNT_MA5":"first D4-D10 close above MA5 and trading amount >= prior-20d mean"
        },
        "execution_assumption":"confirmation entries occur at confirmation-day close; target/MAE/MFE measurement starts next trading day",
        "warning":"All variants are predeclared parallel research comparisons. Do not select the best same-sample variant for production."
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "# ODOLI R2.20 — SLOW vs NO Structural Survival + Entry Timing","",
        "- Frozen R2.19 100-event cohort.",
        "- Primary comparison: SLOW_D21_60 vs NO_PLUS15_D60.",
        "- Three predeclared confirmation variants are reported in parallel.",
        "- Delayed/split confirmation entry uses confirmation-day CLOSE.",
        "- Same-day High/Low after a confirmation is not credited; outcome measurement begins next trading day.",
        "- No best variant is promoted from this same sample.","",
        "## SLOW vs NO survival board","```",board.to_string(index=False),"```","",
        "## Entry strategy summary","```",summary.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
