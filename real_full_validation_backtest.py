#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math, re
from pathlib import Path
import numpy as np
import pandas as pd
from real_full_decision_engine import code,text,num,extract_row,make_event,mature_from_ready
from real_full_trust_audit import load_price_frames

VALIDATION_ID="REAL_FULL_VALIDATION_BACKTEST_R1"
VALIDATION_REVISION="REAL_FULL_VALIDATION_BACKTEST_R1_1_PATTERN_OVERALL_GATE"

def readcsv(p,**kw):
    p=Path(p)
    if not p.exists():return pd.DataFrame()
    try:return pd.read_csv(p,**kw)
    except pd.errors.EmptyDataError:return pd.DataFrame()

def load_jsonl(p):
    p=Path(p); rows=[]
    if not p.exists():return pd.DataFrame()
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():continue
        try:rows.append(json.loads(line))
        except:pass
    return pd.DataFrame(rows)

def selected_snapshots(manifest,rows):
    if manifest.empty:return {},pd.DataFrame()
    m=manifest.copy()
    m["snapshot_date"]=m["snapshot_date"].fillna("").astype(str)
    unresolved=int(m["snapshot_date"].eq("").sum())
    selected={}
    ambiguity=[]
    for d,g in m[m["snapshot_date"].ne("")].groupby("snapshot_date"):
        g=g.sort_values("call_id")
        ids=list(g["call_id"].astype(int))
        # last builder call is the only deterministic choice; record same-date multi-call ambiguity.
        sel=ids[-1]
        selected[d]=sel
        ambiguity.append({"snapshot_date":d,"calls":len(ids),"selected_call_id":sel,"ambiguity":int(len(ids)>1)})
    return selected,pd.DataFrame(ambiguity)

def get_price_on(frame,date):
    if frame is None or frame.empty:return np.nan
    d=pd.Timestamp(date).normalize()
    f=frame.copy(); f["date"]=pd.to_datetime(f["date"],errors="coerce").dt.normalize()
    z=f[f["date"].eq(d)]
    return num(z.iloc[-1]["close"]) if len(z) else np.nan

def pattern_score(events,group):
    if events.empty or group not in events.columns:return pd.DataFrame()
    rows=[]
    for k,g in events.groupby(group,dropna=False):
        rec={"validation_id":VALIDATION_ID,"group_type":group,"group":str(k),"events":len(g)}
        for h in (1,3,5,10):
            comp=pd.to_numeric(g.get(f"origin_d{h}_complete"),errors="coerce").fillna(0).eq(1)
            z=g[comp]
            rec[f"d{h}_mature"]=len(z)
            for m in ("close_ret_pct","mfe_pct","mae_pct"):
                s=pd.to_numeric(z.get(f"origin_d{h}_{m}"),errors="coerce").dropna()
                rec[f"d{h}_{m}_median"]=float(s.median()) if len(s) else np.nan
            for m in ("hit_plus3","hit_plus5"):
                s=pd.to_numeric(z.get(f"origin_d{h}_{m}"),errors="coerce").dropna()
                rec[f"d{h}_{m}_rate"]=float(s.mean()) if len(s) else np.nan
        rows.append(rec)
    return pd.DataFrame(rows)

def mature_origin(event,frame):
    e=dict(event); e["ready_date"]=e["origin_date"]; e["ready_price"]=e["origin_price"]
    vals=mature_from_ready(e,frame); out={}
    for k,v in vals.items(): out[k.replace("ready_","origin_")]=v
    return out

def overall_gate(report_dir):
    root=Path(report_dir)
    required=[
        "v73_backtest_event_master.csv",
        "v73_search_formula_scorecard.csv",
        "v73_formula_stability_matrix.csv",
        "v73_formula_stability_reconciliation.csv",
        "v73_universe_rank_bucket_coverage.csv",
        "v73_direct_replay_performance_audit.csv",
    ]
    rows=[]; ready=True
    for fn in required:
        p=root/fn
        if not p.exists():
            n=-1; status="MISSING"; ready=False
        else:
            q=readcsv(p)
            n=len(q); status="READY" if n>0 else "EMPTY"
            if n<=0: ready=False
        rows.append({"file":fn,"rows":n,"status":status})
    return pd.DataFrame(rows),("READY_OVERALL_BACKTEST" if ready else "INCOMPLETE_OVERALL_BACKTEST")

def self_test():
    e={"origin_date":"2026-09-01","origin_price":100}
    f=pd.DataFrame([
        {"date":"2026-09-02","open":100,"high":104,"low":98,"close":102},
        {"date":"2026-09-03","open":102,"high":106,"low":101,"close":105},
        {"date":"2026-09-04","open":105,"high":107,"low":103,"close":106},
        {"date":"2026-09-07","open":106,"high":108,"low":104,"close":107},
        {"date":"2026-09-08","open":107,"high":109,"low":105,"close":108},
    ])
    x=mature_origin(e,f)
    assert x["origin_d1_complete"]==1 and x["origin_d5_hit_plus5"]==1
    print("REAL_FULL_VALIDATION_BACKTEST_SELF_TEST PASS")
    return 0

def run(a):
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    manifest=readcsv(Path(a.capture_dir)/"capture_manifest.csv")
    rows=load_jsonl(Path(a.capture_dir)/"candidate_snapshots.jsonl")
    selected,amb=selected_snapshots(manifest,rows)

    source_mode="BUILDER_CAPTURE"
    materialized_selection=readcsv(Path(a.materialized_adapter_dir)/"materialized_source_selection.csv")
    if not selected:
        mrows=load_jsonl(Path(a.materialized_adapter_dir)/"materialized_candidate_snapshots.jsonl")
        if not mrows.empty and "_snapshot_date" in mrows.columns:
            rows=mrows.copy()
            rows["_capture_call_id"]=pd.factorize(rows["_snapshot_date"].astype(str))[0]+1
            map_ids=(rows[["_snapshot_date","_capture_call_id"]]
                     .drop_duplicates()
                     .rename(columns={"_snapshot_date":"snapshot_date","_capture_call_id":"call_id"}))
            selected={str(r.snapshot_date):int(r.call_id) for r in map_ids.itertuples(index=False)}
            amb=pd.DataFrame([{"snapshot_date":d,"calls":1,"selected_call_id":cid,"ambiguity":0}
                              for d,cid in selected.items()])
            source_mode="V23_MATERIALIZED_ADAPTER"
    amb.to_csv(out/"historical_snapshot_call_audit.csv",index=False,encoding="utf-8-sig")
    if not rows.empty:
        rows["_capture_call_id"]=pd.to_numeric(rows["_capture_call_id"],errors="coerce")
    needed=set()
    if not rows.empty:
        cc="code" if "code" in rows.columns else "종목코드" if "종목코드" in rows.columns else None
        if cc: needed=set(rows[cc].map(code))
    frames={}
    price_meta={}
    if needed:
        try:
            frames,_,price_meta=load_price_frames(Path(a.price_cache_dir),Path(a.amount_cache_dir),Path(a.asof_cache_dir),needed_codes=needed)
        except Exception as ex:
            price_meta={"error":f"{type(ex).__name__}:{ex}"}

    event_rows=[]
    snapshot_rows=[]
    for d,cid in sorted(selected.items()):
        q=rows[rows["_capture_call_id"].eq(cid)].copy() if not rows.empty else pd.DataFrame()
        if not q.empty:q=q.sort_values("_source_order")
        snapshot_rows.append({"snapshot_date":d,"selected_call_id":cid,"candidate_rows":len(q),"top15_rows":min(15,len(q))})
        for rank,(_,r) in enumerate(q.head(15).iterrows(),1):
            e=make_event(r,d,rank)
            c=e["code"]
            # Backtest candidate price should be as-of source price; price cache is fallback only.
            if not math.isfinite(num(e["origin_price"])):
                e["origin_price"]=get_price_on(frames.get(c),d)
            vals=mature_origin(e,frames.get(c))
            event_rows.append({**e,**vals})
    events=pd.DataFrame(event_rows)
    pd.DataFrame(snapshot_rows).to_csv(out/"pattern_backtest_snapshot_summary.csv",index=False,encoding="utf-8-sig")
    events.to_csv(out/"pattern_backtest_event_ledger.csv",index=False,encoding="utf-8-sig")

    groups=["origin_action","origin_wave_state","origin_watermelon_state","origin_recommendation_stage",
            "origin_stage_status","origin_cloud_state","origin_refine_state","origin_search_pattern"]
    for g in groups:
        pattern_score(events,g).to_csv(out/f"pattern_backtest_scorecard_{g}.csv",index=False,encoding="utf-8-sig")

    gate,overall_status=overall_gate(a.report_dir)
    gate.to_csv(out/"overall_backtest_gate.csv",index=False,encoding="utf-8-sig")
    dates=len(selected); ev=len(events)
    d5=int(pd.to_numeric(events.get("origin_d5_complete",pd.Series(dtype=float)),errors="coerce").fillna(0).eq(1).sum()) if ev else 0
    multi=int(amb.get("ambiguity",pd.Series(dtype=int)).sum()) if not amb.empty else 0

    # Exact WAIT->NEAR->READY replay requires daily classifier snapshots.
    # WEEK_LAST historical snapshots cannot prove an intra-week D+1..D+5 trigger.
    freq_status="NO_CAPTURE"
    exact="NOT_READY_DAILY_CLASSIFIER_SOURCE"
    if dates>=2:
        ds=sorted(pd.to_datetime(list(selected.keys())))
        gaps=[(ds[i]-ds[i-1]).days for i in range(1,len(ds))]
        med=float(np.median(gaps)) if gaps else np.nan
        freq_status=f"median_calendar_gap={med:.1f}d"
        if med<=2.0:
            exact="DAILY_SOURCE_CANDIDATE_REVIEW_REQUIRED"
    pattern_status="READY_DESCRIPTIVE_PATTERN_BACKTEST" if dates>=10 and ev>=30 and d5>=30 else "WARMUP_PATTERN_BACKTEST"
    if multi>0:
        pattern_status += "_CALL_AMBIGUITY"

    readiness=pd.DataFrame([{
        "validation_id":VALIDATION_ID,"validation_revision":VALIDATION_REVISION,
        "snapshot_dates":dates,"top15_events":ev,"d5_mature_events":d5,
        "historical_source_mode":source_mode,
        "same_date_multi_call_dates":multi,"pattern_status":pattern_status,
        "overall_status":overall_status,"exact_state_machine_replay_status":exact,
        "capture_frequency":freq_status,
        "same_sample_tuning_allowed":0,"auto_order_allowed":0,
    }])
    readiness.to_csv(out/"real_full_validation_readiness.csv",index=False,encoding="utf-8-sig")
    report="\n".join([
        "🧪 [REAL_FULL VALIDATION BACKTEST R1]",
        f"historical source={source_mode}",
        f"historical snapshots={dates} · Top15 events={ev} · D+5 mature={d5} · multi-call dates={multi}",
        f"pattern={pattern_status}",
        f"overall={overall_status}",
        f"exact WAIT→NEAR→READY replay={exact} ({freq_status})",
        "해석: 패턴/판정명 성과와 전체 Direct Replay는 과거검증. 3/5거래일 READY 전환은 일별 classifier snapshot 없이는 과거 exact 판정 금지.",
        "동일 표본 결과를 보고 R1 대기기간/조건을 튜닝하지 않습니다.",
    ])
    (out/"real_full_validation_backtest_report.txt").write_text(report,encoding="utf-8")
    print(report)
    return 0

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--capture-dir",default="reports/real_full_validation_backtest")
    ap.add_argument("--report-dir",default="reports")
    ap.add_argument("--materialized-adapter-dir",default="reports/real_full_validation_backtest/materialized_adapter")
    ap.add_argument("--output-dir",default="reports/real_full_validation_backtest/analysis")
    ap.add_argument("--price-cache-dir",default="reports/.cache/v20_price_history")
    ap.add_argument("--amount-cache-dir",default="reports/.cache/v25_actual_amount_history")
    ap.add_argument("--asof-cache-dir",default="reports/.cache/v20_asof_snapshots")
    ap.add_argument("--self-test",action="store_true")
    a=ap.parse_args()
    return self_test() if a.self_test else run(a)
if __name__=="__main__": raise SystemExit(main())
