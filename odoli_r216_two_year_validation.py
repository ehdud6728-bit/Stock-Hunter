#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,sys
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_16_TWO_YEAR_VALIDATION_20260927"
START=pd.Timestamp("2024-01-01")
END=pd.Timestamp("2025-12-31")

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def b(s):
    return s.astype(str).str.lower().isin(["true","1"])

def safe_mean(x):
    x=pd.to_numeric(x,errors="coerce").dropna()
    return float(x.mean()) if len(x) else np.nan

def period(d):
    y=d.year
    return f"{y}H1" if d.month<=6 else f"{y}H2"

def load_marcap(root):
    frames=[]
    for y in [2024,2025]:
        p=Path(root)/"data"/f"marcap-{y}.parquet"
        if not p.exists(): raise SystemExit(f"MISSING_MARCAP:{p}")
        q=pd.read_parquet(p)
        if "Date" not in q.columns:q=q.reset_index()
        q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
        q["Code"]=q["Code"].map(norm_code)
        q=q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()
        frames.append(q)
    return pd.concat(frames,ignore_index=True)

def descriptor(g,sigdate,bins):
    g=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
    hit=g.index[g["Date"].eq(sigdate)]
    if len(hit)!=1:return None
    i=int(hit[0])
    if i<25:return None
    hist=g.iloc[:i+1].copy()
    pre10=g.iloc[max(0,i-10):i].copy()
    close=pd.to_numeric(hist["Close"],errors="coerce")
    ma5=float(close.rolling(5).mean().iloc[-1])
    ma10=float(close.rolling(10).mean().iloc[-1])

    p10=pre10.copy()
    p10["ret"]=pd.to_numeric(p10["Close"],errors="coerce").pct_change()
    down=p10[p10["ret"]<0]; up=p10[p10["ret"]>0]
    da=safe_mean(down["Amount"]); ua=safe_mean(up["Amount"])
    dv=safe_mean(down["Volume"]); uv=safe_mean(up["Volume"])
    ar=da/ua if ua and ua>0 else np.nan
    vr=dv/uv if uv and uv>0 else np.nan

    def legacy_q(val,feature):
        edges=np.array(bins.get(feature,[]),dtype=float)
        if len(edges)<2 or not np.isfinite(val):return np.nan
        q=pd.cut(pd.Series([val]),bins=edges,labels=False,include_lowest=True).iloc[0]
        return float(q+1) if pd.notna(q) else np.nan

    def frozen_q2_weak(val,feature):
        edges=np.array(bins.get(feature,[]),dtype=float)
        # Discovery used five qcut buckets. Q1+Q2 means <= upper edge of Q2.
        if len(edges)<3 or not np.isfinite(val): return False
        return bool(val <= float(edges[2]))

    aq=legacy_q(ar,"down_up_amount_ratio_10d")
    vq=legacy_q(vr,"down_up_volume_ratio_10d")
    amount_weak_legacy=bool(pd.notna(aq) and aq<=2)
    volume_weak_legacy=bool(pd.notna(vq) and vq<=2)
    amount_weak_boundary=frozen_q2_weak(ar,"down_up_amount_ratio_10d")
    volume_weak_boundary=frozen_q2_weak(vr,"down_up_volume_ratio_10d")

    return {
        "ma5":ma5,"ma10":ma10,
        "ma5_ma10_gap_pct":(ma5/ma10-1)*100 if ma10 else np.nan,
        "down_up_amount_ratio_10d":ar,
        "down_up_volume_ratio_10d":vr,
        "amount_q_legacy":aq,"volume_q_legacy":vq,
        "ma5_gt_ma10":bool(ma5>ma10),
        "weak_participation_legacy":amount_weak_legacy or volume_weak_legacy,
        "weak_participation_boundary":amount_weak_boundary or volume_weak_boundary,
    }

def path(g,sigdate):
    g=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
    hit=g.index[g["Date"].eq(sigdate)]
    if len(hit)!=1:return None
    i=int(hit[0]); sig=float(g.loc[i,"Close"]); siglow=float(g.loc[i,"Low"]); sighigh=float(g.loc[i,"High"])
    ma5=pd.to_numeric(g["Close"],errors="coerce").rolling(5).mean()
    rows=[]
    for d in range(1,11):
        if i+d>=len(g):break
        r=g.loc[i+d]
        rows.append({
            "day":d,
            "high_ret_pct":(float(r["High"])/sig-1)*100,
            "low_ret_pct":(float(r["Low"])/sig-1)*100,
            "close_ret_pct":(float(r["Close"])/sig-1)*100,
            "low":float(r["Low"]),"close":float(r["Close"]),
            "above_ma5":bool(float(r["Close"])>float(ma5.iloc[i+d]))
        })
    p=pd.DataFrame(rows)
    out={"signal_close":sig,"signal_low":siglow,"signal_high":sighigh}
    for h in [1,3,5,10]:
        q=p[p.day<=h]
        out[f"d{h}_complete"]=len(q)>=h
        if len(q)>=h:
            out[f"d{h}_mfe_pct"]=float(q.high_ret_pct.max())
            out[f"d{h}_mae_pct"]=float(q.low_ret_pct.min())
            out[f"d{h}_close_ret_pct"]=float(q.iloc[-1].close_ret_pct)
            out[f"d{h}_touch10"]=bool((q.high_ret_pct>=10).any())
    if len(p)>=2:
        out["d1_2_higher_low"]=bool(float(p.iloc[1].low)>float(p.iloc[0].low))
        out["d2_holds_signal_low"]=bool(float(p.iloc[1].low)>=siglow)
        out["d2_close_above_ma5"]=bool(p.iloc[1].above_ma5)
    if len(p)>=3:
        out["d1_3_all_hold_signal_low"]=bool((p.iloc[:3].low>=siglow).all())
        out["d1_3_all_above_ma5"]=bool(p.iloc[:3].above_ma5.all())
        out["d1_3_mfe_pct"]=float(p.iloc[:3].high_ret_pct.max())
        out["d1_3_mae_pct"]=float(p.iloc[:3].low_ret_pct.min())
    return out

def summ(g,label,period_label):
    d=g[g["d5_complete"].fillna(False).astype(bool)].copy()
    r={"period":period_label,"group":label,"n":len(g),"d5_complete_n":len(d)}
    if not len(d):return r
    r.update({
        "d5_touch10_n":int(d["d5_touch10"].fillna(False).astype(bool).sum()),
        "d5_touch10_rate_pct":float(d["d5_touch10"].fillna(False).astype(bool).mean()*100),
        "d5_mfe_median_pct":float(pd.to_numeric(d["d5_mfe_pct"],errors="coerce").median()),
        "d5_mae_median_pct":float(pd.to_numeric(d["d5_mae_pct"],errors="coerce").median()),
        "d5_close_median_pct":float(pd.to_numeric(d["d5_close_ret_pct"],errors="coerce").median()),
    })
    return r

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--v4976-root",required=True)
    ap.add_argument("--odoli-root",required=True)
    ap.add_argument("--r23-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)

    src=pd.read_csv(find_one(a.v4976_root,"v49_76_selected_enriched_outcomes.csv"),dtype={"code":str},low_memory=False)
    src["code"]=src["code"].map(norm_code)
    src["signal_date"]=pd.to_datetime(src["signal_date"],errors="coerce").dt.normalize()
    src["primary_strategy"]=src["primary_strategy"].astype(str).str.upper().str.strip()
    A=src[(src.signal_date>=START)&(src.signal_date<=END)&src.primary_strategy.eq("A")].copy()
    A=A.sort_values(["signal_date","code"]).drop_duplicates(["signal_date","code"],keep="first")
    if len(A)!=2464:
        raise SystemExit(f"A_AUTHORITY_COUNT_MISMATCH:{len(A)}")

    tf=pd.read_csv(find_one(a.v4976_root,"v49_76_technical_false_negative.csv"),dtype={"code":str},low_memory=False)
    tf["code"]=tf["code"].map(norm_code)
    tf["signal_date"]=pd.to_datetime(tf["signal_date"],errors="coerce").dt.normalize()
    tf_a=tf[(tf["gate_missing_modes"].astype(str).str.contains("A",na=False)) &
            (pd.to_numeric(tf["a_authority_replay_hit"],errors="coerce").fillna(0).eq(1))].copy()
    tf_a.to_csv(out/"technical_fn_a_audit.csv",index=False,encoding="utf-8-sig")
    if len(tf_a)!=1: raise SystemExit(f"EXPECTED_ONE_A_TECH_FN_GOT:{len(tf_a)}")

    od=pd.read_csv(find_one(a.odoli_root,"odoli_all_market_events.csv"),dtype={"code":str},low_memory=False)
    od["code"]=od["code"].map(norm_code)
    od["signal_date"]=pd.to_datetime(od["signal_date"],errors="coerce").dt.normalize()
    odkeys=set(zip(od.signal_date,od.code))

    bins=json.loads(find_one(a.r23_root,"a_context_bins.json").read_text(encoding="utf-8"))
    mar=load_marcap(a.marcap_root)
    bycode={c:g.copy() for c,g in mar.groupby("Code",sort=False)}

    # Existing accumulation authority, unchanged.
    sys.path.insert(0,str(Path.cwd()))
    from scanner.accumulation_wave_complete import calc_accum_candle_score

    rows=[]
    for _,r in A.iterrows():
        rec={"signal_date":r.signal_date,"code":r.code,"name":r.get("name",""),
             "primary_strategy":"A","period":period(r.signal_date),
             "odoli_overlap":bool((r.signal_date,r.code) in odkeys)}
        g=bycode.get(r.code)
        d=descriptor(g,r.signal_date,bins) if g is not None else None
        p=path(g,r.signal_date) if g is not None else None
        if d:rec.update(d)
        if p:rec.update(p)
        rec["is_core_legacy"]=bool(rec.get("ma5_gt_ma10",False) and rec.get("weak_participation_legacy",False))
        rec["is_core_boundary"]=bool(rec.get("ma5_gt_ma10",False) and rec.get("weak_participation_boundary",False))
        rec["is_core_odoli"]=bool(rec["is_core_boundary"] and rec["odoli_overlap"])

        # Accumulation score only for frozen CORE+ODOLI.
        if rec["is_core_odoli"] and g is not None:
            gg=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
            hit=gg.index[gg.Date.eq(r.signal_date)]
            best=-1
            if len(hit)==1:
                i=int(hit[0]); hist=gg.iloc[:i+1].reset_index(drop=True)
                for j in range(max(1,len(hist)-21),len(hist)-1):
                    info=calc_accum_candle_score(hist,j)
                    best=max(best,float(info.get("score",0) or 0))
            rec["best_accum_score"]=best if best>=0 else np.nan
            rec["has_accum40"]=bool(best>=40)
            rec["has_strong_accum70"]=bool(best>=70)
        rows.append(rec)

    z=pd.DataFrame(rows)
    z.to_csv(out/"r216_a_events.csv",index=False,encoding="utf-8-sig")
    co=z[z.is_core_odoli].copy()
    co.to_csv(out/"r216_core_odoli_events.csv",index=False,encoding="utf-8-sig")

    # CORE semantics parity audit.
    parity=pd.DataFrame([{
        "a_n":len(z),
        "legacy_core_n":int(z.is_core_legacy.sum()),
        "boundary_core_n":int(z.is_core_boundary.sum()),
        "core_membership_diff_n":int((z.is_core_legacy != z.is_core_boundary).sum()),
        "legacy_core_odoli_n":int((z.is_core_legacy & z.odoli_overlap).sum()),
        "boundary_core_odoli_n":int(z.is_core_odoli.sum()),
    }])
    parity.to_csv(out/"r216_core_semantics_audit.csv",index=False,encoding="utf-8-sig")

    s=[]
    for per in ["2024H1","2024H2","2025H1","2025H2","ALL_2024_2025"]:
        q=z if per.startswith("ALL") else z[z.period.eq(per)]
        groups=[
            ("A_ALL",q),
            ("A_ODOLI",q[q.odoli_overlap]),
            ("CORE_ALL",q[q.is_core_boundary]),
            ("CORE_ODOLI",q[q.is_core_odoli]),
            ("CORE_ONLY",q[q.is_core_boundary & ~q.odoli_overlap]),
        ]
        for label,g in groups:s.append(summ(g,label,per))
    sm=pd.DataFrame(s)
    sm.to_csv(out/"r216_period_summary.csv",index=False,encoding="utf-8-sig")

    # Early-reaction validation inside CORE+ODOLI, outcome defined before this run.
    mature=co[co.d5_complete.fillna(False).astype(bool)].copy()
    mature["success_d5_touch10"]=mature.d5_touch10.fillna(False).astype(bool)
    br=[]
    for feat in ["d1_2_higher_low","d2_holds_signal_low","d2_close_above_ma5",
                 "d1_3_all_hold_signal_low","d1_3_all_above_ma5"]:
        if feat not in mature:continue
        for outcome,g in [("SUCCESS",mature[mature.success_d5_touch10]),
                          ("FAIL",mature[~mature.success_d5_touch10])]:
            x=g[feat].dropna().astype(bool)
            br.append({"feature":feat,"outcome":outcome,"n":len(x),
                       "true_n":int(x.sum()),"true_rate_pct":float(x.mean()*100) if len(x) else np.nan})
    pd.DataFrame(br).to_csv(out/"r216_early_reaction_validation.csv",index=False,encoding="utf-8-sig")

    # Accumulation comparison.
    ar=[]
    for outcome,g in [("SUCCESS",mature[mature.success_d5_touch10]),
                      ("FAIL",mature[~mature.success_d5_touch10])]:
        score=pd.to_numeric(g.get("best_accum_score"),errors="coerce")
        ar.append({"outcome":outcome,"n":len(g),
                   "score_median":float(score.median()) if len(score.dropna()) else np.nan,
                   "accum40_rate_pct":float(g.has_accum40.fillna(False).mean()*100) if len(g) else np.nan,
                   "strong70_rate_pct":float(g.has_strong_accum70.fillna(False).mean()*100) if len(g) else np.nan})
    pd.DataFrame(ar).to_csv(out/"r216_accumulation_validation.csv",index=False,encoding="utf-8-sig")

    counts={
        "selected_a":len(z),
        "a_signal_dates":int(z.signal_date.nunique()),
        "a_odoli":int(z.odoli_overlap.sum()),
        "core_boundary":int(z.is_core_boundary.sum()),
        "core_odoli":int(z.is_core_odoli.sum()),
        "technical_fn_a_audit":len(tf_a)
    }
    meta={
        "revision":REV,"research_only":True,"production_logic_changed":False,
        "same_sample_tuning":False,"new_gate_created":False,
        "source_v4976_run":36318449010,
        "source_v4976_merge_status":"DEGRADED_ONE_TECHNICAL_FALSE_NEGATIVE",
        "selected_a_authority":"v49_76_selected_enriched_outcomes primary_strategy==A",
        "technical_fn_repaired_into_selected_population":False,
        "technical_fn_reason":"rank/selected status not proven; audit separately",
        "odoli_definition":"STRICT_ODOLI_R1",
        "core_semantics_primary":"FROZEN_Q2_UPPER_BOUNDARY",
        "core_legacy_semantics_also_reported":True,
        **counts
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "# ODOLI R2.16 — Two-year frozen validation","",
        "- Window: 2024-01-01 ~ 2025-12-31.",
        "- v49.76 source run had 8/8 valid shards but merge fail-closed on exactly one technical false negative.",
        "- Baseline selected A population remains 2,464 rows; the one A technical-FN case is audited separately and is NOT silently inserted.",
        "- STRICT_ODOLI_R1 unchanged.",
        "- CORE uses frozen R2.3 Q2 upper-boundary semantics; legacy R2.9 pd.cut semantics also reported for parity.",
        "- D+5 +10% success definition unchanged.",
        "- R2.14 early-reaction features are validation-only descriptors, not gates.","",
        "## Counts","```",json.dumps(counts,ensure_ascii=False,indent=2),"```","",
        "## CORE semantics audit","```",parity.to_string(index=False),"```","",
        "## Period summary","```",sm.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
