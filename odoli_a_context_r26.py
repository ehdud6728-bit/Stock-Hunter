#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_A_CONTEXT_R2_6_FORWARD_REFRESH_20260925"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs:
        raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_marcap(root, years):
    parts=[]
    for y in sorted(years):
        p=Path(root)/"data"/f"marcap-{y}.parquet"
        if not p.exists():
            raise SystemExit(f"MISSING_MARCAP:{p}")
        q=pd.read_parquet(p)
        if "Date" not in q.columns:
            q=q.reset_index()
        q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
        q["Code"]=q["Code"].map(norm_code)
        parts.append(q)
    return pd.concat(parts,ignore_index=True)

def forward_metrics(g, sigdate):
    g=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
    hit=g.index[g["Date"].eq(sigdate)]
    if len(hit)!=1:
        return None
    i=int(hit[0])
    sig_close=float(g.loc[i,"Close"])
    out={"signal_close":sig_close}

    for h in [1,3,5,10]:
        if i+h < len(g):
            q=g.iloc[i+1:i+h+1].copy()
            out[f"d{h}_complete"]=len(q)==h
            if len(q)==h:
                close_h=float(q.iloc[-1]["Close"])
                high_h=float(pd.to_numeric(q["High"],errors="coerce").max())
                low_h=float(pd.to_numeric(q["Low"],errors="coerce").min())
                out[f"d{h}_close_ret_pct"]=(close_h/sig_close-1)*100
                out[f"d{h}_mfe_pct"]=(high_h/sig_close-1)*100
                out[f"d{h}_mae_pct"]=(low_h/sig_close-1)*100
                out[f"d{h}_plus5"]=out[f"d{h}_mfe_pct"]>=5
                out[f"d{h}_positive_close"]=out[f"d{h}_close_ret_pct"]>0
            else:
                out[f"d{h}_complete"]=False
        else:
            out[f"d{h}_complete"]=False
    return out

def summarize(q,label,h=5):
    d=q[q[f"d{h}_complete"].fillna(False).astype(bool)].copy()
    if d.empty:
        return {"group":label,"horizon":h,"n":len(q),"complete_n":0}
    mfe=pd.to_numeric(d[f"d{h}_mfe_pct"],errors="coerce")
    mae=pd.to_numeric(d[f"d{h}_mae_pct"],errors="coerce")
    close=pd.to_numeric(d[f"d{h}_close_ret_pct"],errors="coerce")
    hit=mfe.ge(5)
    pos=close.gt(0)
    held=hit & close.ge(5)
    give=hit & close.lt(5)
    nohit=~hit
    return {
        "group":label,"horizon":h,"n":len(q),"complete_n":len(d),
        "plus5_rate_pct":float(hit.mean()*100),
        "positive_close_rate_pct":float(pos.mean()*100),
        "close_median_pct":float(close.median()),
        "mfe_median_pct":float(mfe.median()),
        "mae_median_pct":float(mae.median()),
        "held_ge5_close_rate_pct":float(held.mean()*100),
        "giveback_after_plus5_rate_pct":float(give.mean()*100),
        "no_plus5_rate_pct":float(nohit.mean()*100),
        "held_minus_giveback_pp":float((held.mean()-give.mean())*100),
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r25-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    core=pd.read_csv(find_one(a.r25_root,"core21_event_trace.csv"),dtype={"code":str},low_memory=False)
    core["code"]=core["code"].map(norm_code)
    core["signal_date"]=pd.to_datetime(core["signal_date"]).dt.normalize()

    mar=load_marcap(a.marcap_root,{2026})
    bycode={c:g.copy() for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    for idx,r in core.iterrows():
        g=bycode.get(r["code"])
        fm=forward_metrics(g,r["signal_date"]) if g is not None else None
        rec=r.to_dict()
        rec["_row"]=idx
        if fm:
            rec.update(fm)
        else:
            rec["path_status"]="MISSING_SIGNAL_OR_CODE"
        rows.append(rec)
    z=pd.DataFrame(rows)

    if "odoli_overlap" not in z.columns:
        raise SystemExit("MISSING_ODOLI_OVERLAP")
    z["odoli_overlap"]=z["odoli_overlap"].astype(str).str.lower().isin(["true","1"])

    z.to_csv(out/"core21_forward_refreshed.csv",index=False,encoding="utf-8-sig")

    groups=[
        ("CORE_ALL",z),
        ("CORE_AND_ODOLI",z[z["odoli_overlap"]]),
        ("CORE_WITHOUT_ODOLI",z[~z["odoli_overlap"]]),
    ]
    rows=[]
    for h in [3,5,10]:
        for name,g in groups:
            rows.append(summarize(g,name,h))
    summary=pd.DataFrame(rows)
    summary.to_csv(out/"core21_forward_summary.csv",index=False,encoding="utf-8-sig")

    # Event-level completed status for recent ODOLI 4.
    od=z[z["odoli_overlap"]].copy()
    od.to_csv(out/"core_odoli4_forward_detail.csv",index=False,encoding="utf-8-sig")

    # Compare refreshed D5 to frozen R1.3 D5 only where both complete.
    cmp=[]
    for _,r in z.iterrows():
        frozen_close=pd.to_numeric(pd.Series([r.get("ret_close_5d")]),errors="coerce").iloc[0]
        frozen_mfe=pd.to_numeric(pd.Series([r.get("ret_max_high_5d")]),errors="coerce").iloc[0]
        fresh_close=r.get("d5_close_ret_pct",np.nan)
        fresh_mfe=r.get("d5_mfe_pct",np.nan)
        cmp.append({
            "signal_date":r["signal_date"],"code":r["code"],"name":r.get("name",""),
            "odoli_overlap":r["odoli_overlap"],
            "frozen_d5_close_pct":frozen_close,
            "fresh_d5_close_pct":fresh_close,
            "d5_close_diff_pp":fresh_close-frozen_close if pd.notna(fresh_close) and pd.notna(frozen_close) else np.nan,
            "frozen_d5_mfe_pct":frozen_mfe,
            "fresh_d5_mfe_pct":fresh_mfe,
            "d5_mfe_diff_pp":fresh_mfe-frozen_mfe if pd.notna(fresh_mfe) and pd.notna(frozen_mfe) else np.nan,
        })
    pd.DataFrame(cmp).to_csv(out/"frozen_vs_refreshed_d5_check.csv",index=False,encoding="utf-8-sig")

    d5od=od[od["d5_complete"].fillna(False).astype(bool)]
    meta={
        "revision":REV,
        "research_only":True,
        "production_logic_changed":False,
        "same_sample_threshold_tuning":False,
        "signal_definition_frozen":True,
        "core_definition_frozen":True,
        "core_n":len(z),
        "core_odoli_n":int(z["odoli_overlap"].sum()),
        "core_without_odoli_n":int((~z["odoli_overlap"]).sum()),
        "core_odoli_d5_complete_n":len(d5od),
        "asof_data_date":str(mar["Date"].max().date()) if len(mar) else "",
        "true_oos_validation":False
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "# ODOLI R2.6 — Frozen-core forward outcome refresh",
        "",
        "- No signal/threshold changes.",
        "- Frozen cohort source: R2.5 run 36148837491.",
        "- Forward prices refreshed from FinanceData marcap 2026 PIT rows.",
        f"- Data max date: {meta['asof_data_date']}.",
        f"- Core total: {len(z)} / Core+ODOLI: {int(z['odoli_overlap'].sum())} / Core-only: {int((~z['odoli_overlap']).sum())}.",
        f"- Core+ODOLI D+5 complete: {len(d5od)}/4.",
        "- D+3, D+5, D+10 are reported with complete-only denominators.",
        "- This is forward-outcome completion on frozen historical signals, not a new OOS signal-generation test.",
        "",
        "## Refreshed summary",
        "```",
        summary.to_string(index=False),
        "```",
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
