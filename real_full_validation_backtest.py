#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math, re
from pathlib import Path
import numpy as np
import pandas as pd
from real_full_decision_engine import code,text,num,extract_row,make_event,mature_from_ready
from real_full_trust_audit import load_price_frames

VALIDATION_ID="REAL_FULL_VALIDATION_BACKTEST_R1"
VALIDATION_REVISION="REAL_FULL_VALIDATION_BACKTEST_R1_2_MATERIALIZED_OVERALL_AUTHORITY"

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

def overall_gate(report_dir, events=None, snapshot_dates=0, d5_mature=0):
    root=Path(report_dir)
    rows=[]

    # 1) Candidate-level historical backtest from V23 materialized authority.
    cand_ready=(int(snapshot_dates)>=10 and events is not None and len(events)>=30 and int(d5_mature)>=30)
    rows.append({
        "lane":"MATERIALIZED_CANDIDATE_BACKTEST",
        "status":"READY_DESCRIPTIVE" if cand_ready else "NOT_READY",
        "rows":0 if events is None else len(events),
        "detail":f"snapshot_dates={snapshot_dates};d5_mature={d5_mature}",
    })

    # 2) Full-denominator formula truth/performance lane.
    fs=readcsv(root/"v73_v24_full_denominator_formula_summary.csv")
    fu=readcsv(root/"v73_v24_full_universe_attempt_outcomes.csv")
    cov=readcsv(root/"v73_v24_full_denominator_coverage.csv")
    formula_count=len(fs)
    attempt_rows=len(fu)
    known_cells=0; truth_cells=0; known_pct=np.nan
    if not cov.empty:
        cr=cov.iloc[-1]
        known_cells=int(pd.to_numeric(pd.Series([cr.get("known_cells")]),errors="coerce").fillna(0).iloc[0])
        truth_cells=int(pd.to_numeric(pd.Series([cr.get("truth_cells")]),errors="coerce").fillna(0).iloc[0])
        known_pct=float(pd.to_numeric(pd.Series([cr.get("known_cell_coverage_pct")]),errors="coerce").iloc[0])
    formula_ready=(formula_count==66 and attempt_rows>0 and known_cells>0)
    rows.append({
        "lane":"FULL_DENOMINATOR_FORMULA_BACKTEST",
        "status":"READY_PARTIAL_TRUTH_COVERAGE" if formula_ready else "NOT_READY",
        "rows":attempt_rows,
        "detail":f"formulas={formula_count};known_cells={known_cells}/{truth_cells};known_pct={known_pct}",
    })

    # 3) Historical universe authority coverage.
    ua=readcsv(root/"v73_universe_data_availability.csv")
    complete_days=fallback_days=0
    if not ua.empty:
        complete_days=int(pd.to_numeric(ua.get("complete"),errors="coerce").fillna(0).eq(1).sum())
        fallback_days=int(pd.to_numeric(ua.get("fallback_used"),errors="coerce").fillna(0).eq(1).sum())
    rows.append({
        "lane":"HISTORICAL_UNIVERSE_AUTHORITY",
        "status":"LIMITED_FALLBACK_PRESENT" if fallback_days>0 else ("READY" if complete_days>0 else "NOT_READY"),
        "rows":len(ua),
        "detail":f"complete_days={complete_days};fallback_days={fallback_days}",
    })

    # 4) Legacy V72 path is diagnostic only under V23 zero-recompute validation.
    legacy=readcsv(root/"v72_backtest_fail_closed_audit.csv")
    legacy_reason=""
    if not legacy.empty:
        legacy_reason=str(legacy.iloc[-1].get("reason") or "")
    rows.append({
        "lane":"LEGACY_V72_WEEKLY_DIAGNOSTIC",
        "status":"INCOMPATIBLE_WITH_ZERO_RECOMPUTE_NOT_AUTHORITY" if legacy_reason=="UNIVERSE_EMPTY_OR_LISTING_FAILURE" else "DIAGNOSTIC",
        "rows":len(legacy),
        "detail":legacy_reason,
    })

    if cand_ready and formula_ready:
        overall="READY_DESCRIPTIVE_WITH_AUTHORITY_LIMITS" if fallback_days>0 else "READY_DESCRIPTIVE"
    else:
        overall="INCOMPLETE_MATERIALIZED_OVERALL_BACKTEST"
    return pd.DataFrame(rows),overall

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

    dates=len(selected); ev=len(events)
    d5=int(pd.to_numeric(events.get("origin_d5_complete",pd.Series(dtype=float)),errors="coerce").fillna(0).eq(1).sum()) if ev else 0
    gate,overall_status=overall_gate(a.report_dir,events=events,snapshot_dates=dates,d5_mature=d5)
    gate.to_csv(out/"overall_backtest_gate.csv",index=False,encoding="utf-8-sig")
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

    overall_metrics={}
    if ev:
        valid=pd.to_numeric(events.get("origin_price"),errors="coerce").gt(0)
        for h in (1,3,5,10):
            for m in ("close_ret_pct","mfe_pct","mae_pct"):
                col=f"origin_d{h}_{m}"
                vals=pd.to_numeric(events.loc[valid,col],errors="coerce").dropna() if col in events.columns else pd.Series(dtype=float)
                overall_metrics[f"d{h}_{m}_median"]=float(vals.median()) if len(vals) else np.nan
                overall_metrics[f"d{h}_{m}_mean"]=float(vals.mean()) if len(vals) else np.nan

    readiness=pd.DataFrame([{
        "validation_id":VALIDATION_ID,"validation_revision":VALIDATION_REVISION,
        "snapshot_dates":dates,"top15_events":ev,"d5_mature_events":d5,
        "historical_source_mode":source_mode,
        "same_date_multi_call_dates":multi,"pattern_status":pattern_status,
        "overall_status":overall_status,"exact_state_machine_replay_status":exact,
        "capture_frequency":freq_status,
        "same_sample_tuning_allowed":0,"auto_order_allowed":0,
        **overall_metrics,
    }])
    readiness.to_csv(out/"real_full_validation_readiness.csv",index=False,encoding="utf-8-sig")
    report="\n".join([
        "🧪 [REAL_FULL VALIDATION BACKTEST R1]",
        f"historical source={source_mode}",
        f"historical snapshots={dates} · Top15 events={ev} · D+5 mature={d5} · multi-call dates={multi}",
        f"pattern={pattern_status}",
        f"overall={overall_status}",
        f"overall D+5 median close={overall_metrics.get('d5_close_ret_pct_median',np.nan):+.2f}% · "
        f"MFE={overall_metrics.get('d5_mfe_pct_median',np.nan):+.2f}% · "
        f"MAE={overall_metrics.get('d5_mae_pct_median',np.nan):+.2f}%",
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
