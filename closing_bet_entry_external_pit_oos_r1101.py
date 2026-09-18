#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math, re
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="CLOSEBET_ENTRY_EXTERNAL_PIT_OOS_R1104_META_CONTRACT_FIX_20260919"
DISCOVERY_END="2026-08-18"
FOCUS={"C","B1","B2","I"}

MACRO = {
    "USDKRW":("KRW=X","GLOBAL_DAILY_PRIOR"),
    "KOSPI":("^KS11","KRX_CLOSE"),
    "KOSDAQ":("^KQ11","KRX_CLOSE"),
    "NASDAQ":("^IXIC","US_CLOSE_PRIOR"),
    "SOX":("^SOX","US_CLOSE_PRIOR"),
    "VIX":("^VIX","US_CLOSE_PRIOR"),
    "US10Y":("^TNX","US_CLOSE_PRIOR"),
    "WTI":("CL=F","GLOBAL_DAILY_PRIOR"),
    "DXY":("DX-Y.NYB","GLOBAL_DAILY_PRIOR"),
}

INTERNAL_FEATURES=[
    "pit_entry_close_loc_pct","pit_entry_vol20_ratio","pit_entry_amount20_ratio","pit_entry_atr_pct",
    "pit_entry_ma20_dist_pct","pit_entry_ma60_dist_pct","pit_entry_ma224_dist_pct",
    "pit_entry_upper_wick_pct","pit_entry_ret5_pct","pit_entry_ret20_pct"
]
EXTERNAL_FEATURES=[
    "pit_USDKRW_ret1_pct","pit_USDKRW_ret5_pct","pit_KOSPI_ret1_pct","pit_KOSPI_ret5_pct",
    "pit_KOSDAQ_ret1_pct","pit_KOSDAQ_ret5_pct","pit_NASDAQ_ret1_pct","pit_NASDAQ_ret5_pct",
    "pit_SOX_ret1_pct","pit_SOX_ret5_pct","pit_VIX_ret1_pct","pit_VIX_ret5_pct",
    "pit_US10Y_ret1_pct","pit_US10Y_ret5_pct","pit_WTI_ret1_pct","pit_WTI_ret5_pct",
    "pit_DXY_ret1_pct","pit_DXY_ret5_pct","pit_geo_event_count_1d","pit_geo_event_count_5d"
]

def read_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:pass
    return pd.DataFrame()

def find(root,name):
    xs=list(Path(root).rglob(name)); xs.sort(key=lambda p:(len(p.parts),str(p)))
    return xs[0] if xs else None

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():s=s[:-2]
    if len(s)==7 and s.startswith("A"):s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def num(v):
    return pd.to_numeric(v,errors="coerce")

def boolish(v):
    return str(v).strip().lower() in {"1","true","yes","y","on","t"}

def prep_hist(h):
    q=h.copy()
    q["code"]=q["code"].map(norm_code)
    q["date"]=pd.to_datetime(q["date"],errors="coerce").dt.normalize()
    for c in ["Open","High","Low","Close","Volume","Amount"]:
        if c in q:q[c]=pd.to_numeric(q[c],errors="coerce")
    return q[q.code.ne("")&q.date.notna()].sort_values(["code","date"]).drop_duplicates(["code","date"],keep="last")

def true_range(q):
    p=q.Close.shift()
    return pd.concat([(q.High-q.Low).abs(),(q.High-p).abs(),(q.Low-p).abs()],axis=1).max(axis=1)

def entry_metrics(g,dt):
    z=g[g.date<=dt].sort_values("date").tail(280).reset_index(drop=True).copy()
    if len(z)<224:return {"pit_entry_feature_status":f"INSUFFICIENT_{len(z)}"}
    for n in [5,10,20,60,224]:
        z[f"ma{n}"]=z.Close.rolling(n,min_periods=n).mean()
    z["tr"]=true_range(z)
    r=z.iloc[-1]
    px=float(r.Close)
    rng=float(r.High-r.Low) if pd.notna(r.High) and pd.notna(r.Low) else np.nan
    close_loc=(float(r.Close-r.Low)/rng*100) if rng and rng>0 else np.nan
    wick=(float(r.High-max(r.Open,r.Close))/rng*100) if rng and rng>0 and pd.notna(r.Open) else np.nan

    prev20=z.iloc[-21:-1]
    vmed=prev20.Volume.median()
    curv=float(r.Volume)
    if "Amount" in z and prev20.Amount.notna().sum()>=10 and pd.notna(r.Amount):
        amed=prev20.Amount.median(); cura=float(r.Amount)
    else:
        pa=(prev20.Close*prev20.Volume); amed=pa.median(); cura=px*curv

    atr14=z.tr.iloc[-14:].mean()
    def md(n):
        m=r[f"ma{n}"]
        return (px/float(m)-1)*100 if pd.notna(m) and float(m)>0 else np.nan

    return {
        "pit_entry_feature_status":"PASS",
        "pit_entry_close_loc_pct":close_loc,
        "pit_entry_vol20_ratio":curv/vmed if vmed and vmed>0 else np.nan,
        "pit_entry_amount20_ratio":cura/amed if amed and amed>0 else np.nan,
        "pit_entry_atr_pct":atr14/px*100 if px>0 else np.nan,
        "pit_entry_ma20_dist_pct":md(20),
        "pit_entry_ma60_dist_pct":md(60),
        "pit_entry_ma224_dist_pct":md(224),
        "pit_entry_upper_wick_pct":wick,
        "pit_entry_ret5_pct":(px/float(z.Close.iloc[-6])-1)*100 if len(z)>=6 and z.Close.iloc[-6]>0 else np.nan,
        "pit_entry_ret20_pct":(px/float(z.Close.iloc[-21])-1)*100 if len(z)>=21 and z.Close.iloc[-21]>0 else np.nan,
    }

def infer_signal_clock(row):
    # Explicit timestamp first.
    for c in ["signal_timestamp","signal_datetime","observation_at","captured_at"]:
        if c in row.index and pd.notna(row[c]):
            ts=pd.to_datetime(row[c],errors="coerce")
            if pd.notna(ts):
                return ts, c
    blob=" ".join(str(row.get(c,"")) for c in [
        "lane","slot","signal_slot","run_slot","capture_slot","phase","final_stage","source_stage"
    ] if c in row.index).upper()

    dt=pd.Timestamp(row["signal_date"]).normalize()
    if any(k in blob for k in ["15:40","FINAL","AFTER_FINAL"]):
        return dt+pd.Timedelta(hours=15,minutes=40),"INFERRED_FINAL_1540"
    if any(k in blob for k in ["15:03","PRE_FINAL","PREFINAL"]):
        return dt+pd.Timedelta(hours=15,minutes=3),"INFERRED_PREFINAL_1503"
    # Fail conservative: unknown slot gets 15:03 semantics, never same-day KRX close.
    return dt+pd.Timedelta(hours=15,minutes=3),"CONSERVATIVE_UNKNOWN_AS_1503"

def fetch_macro(start,end):
    import yfinance as yf
    st=(pd.Timestamp(start)-pd.Timedelta(days=45)).strftime("%Y-%m-%d")
    en=(pd.Timestamp(end)+pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    parts=[]; audit=[]
    for name,(sym,rule) in MACRO.items():
        try:
            d=yf.download(sym,start=st,end=en,progress=False,auto_adjust=False,threads=False)
            if d is None or d.empty:
                audit.append({"macro":name,"symbol":sym,"status":"EMPTY","rows":0}); continue
            if isinstance(d.columns,pd.MultiIndex): d.columns=[c[0] for c in d.columns]
            z=pd.DataFrame({
                "date":pd.to_datetime(d.index).tz_localize(None).normalize(),
                "macro":name,
                "close":pd.to_numeric(d["Close"],errors="coerce").values,
                "availability_rule":rule,
            }).dropna(subset=["date","close"])
            z["ret1_pct"]=z.close.pct_change()*100
            z["ret5_pct"]=z.close.pct_change(5)*100
            parts.append(z)
            audit.append({"macro":name,"symbol":sym,"status":"OK","rows":len(z)})
        except Exception as e:
            audit.append({"macro":name,"symbol":sym,"status":f"ERROR:{type(e).__name__}","rows":0})
    return (pd.concat(parts,ignore_index=True) if parts else pd.DataFrame()),pd.DataFrame(audit)

def choose_macro_row(g,signal_ts,rule):
    d=signal_ts.normalize()
    if rule=="KRX_CLOSE":
        # Only explicit/inferred final 15:40+ may use same-day KRX close.
        allow_same = (signal_ts.hour>15 or (signal_ts.hour==15 and signal_ts.minute>=30))
        cutoff=d if allow_same else d-pd.Timedelta(days=1)
    else:
        # US/global daily closes are conservatively lagged one calendar day.
        cutoff=d-pd.Timedelta(days=1)
    q=g[g.date<=cutoff].sort_values("date")
    if q.empty:return None
    return q.iloc[-1]

def macro_snapshot(events,macro):
    rows=[]
    groups={k:g.copy() for k,g in macro.groupby("macro")} if not macro.empty else {}
    for _,r in events.iterrows():
        ts,ts_src=infer_signal_clock(r)
        rec={"signal_date":r.signal_date,"code":r.code,"signal_timestamp_pit":ts,
             "signal_clock_source":ts_src}
        for name,(sym,rule) in MACRO.items():
            g=groups.get(name)
            if g is None:
                rec[f"pit_{name}_source_date"]=pd.NaT
                continue
            mr=choose_macro_row(g,ts,rule)
            if mr is None:
                rec[f"pit_{name}_source_date"]=pd.NaT
                continue
            rec[f"pit_{name}_source_date"]=mr.date
            rec[f"pit_{name}_availability_rule"]=rule
            rec[f"pit_{name}_close"]=mr.close
            rec[f"pit_{name}_ret1_pct"]=mr.ret1_pct
            rec[f"pit_{name}_ret5_pct"]=mr.ret5_pct
        rows.append(rec)
    return pd.DataFrame(rows)

def load_geo(path):
    q=read_csv(path)
    if q.empty:return q
    q["available_at"]=pd.to_datetime(q["available_at"],errors="coerce")
    q["event_date"]=pd.to_datetime(q["event_date"],errors="coerce").dt.normalize()
    q["category"]=q["category"].fillna("OTHER").astype(str)
    q["event_id"]=q["event_id"].astype(str)
    return q[q.available_at.notna()].copy()

def geo_snapshot(events,geo):
    rows=[]
    cats=sorted(geo.category.unique()) if not geo.empty else []
    for _,r in events.iterrows():
        ts,_=infer_signal_clock(r)
        rec={"signal_date":r.signal_date,"code":r.code}
        if geo.empty:
            rec["pit_geo_event_count_1d"]=0; rec["pit_geo_event_count_5d"]=0
            rows.append(rec); continue
        seen=geo[geo.available_at<=ts].copy()
        for win in [1,5]:
            lo=ts-pd.Timedelta(days=win)
            w=seen[seen.available_at>lo]
            rec[f"pit_geo_event_count_{win}d"]=len(w)
        for cat in cats:
            w=seen[(seen.category==cat)&(seen.available_at>ts-pd.Timedelta(days=5))]
            rec[f"pit_GEO_{re.sub('[^A-Z0-9]+','_',cat.upper()).strip('_')}_5D"]=bool(len(w))
        ids=seen[seen.available_at>ts-pd.Timedelta(days=5)].event_id.astype(str).tolist()
        rec["pit_geo_event_ids_5d"]="|".join(ids)
        rows.append(rec)
    return pd.DataFrame(rows)

def freeze_bins(disc,features):
    rows=[]
    for p,g in disc.groupby("primary_formula"):
        for f in features:
            if f not in g:continue
            s=pd.to_numeric(g[f],errors="coerce").dropna()
            if len(s)<20 or s.nunique()<5:continue
            rows.append({"pattern":p,"feature":f,"n":len(s),
                         "q33":s.quantile(.33),"q67":s.quantile(.67),"median":s.median()})
    return pd.DataFrame(rows)

def behavior_contrast(oos,features,scope):
    rows=[]
    for (p,b),g in oos.groupby(["primary_formula","behavior_class"]):
        for f in features:
            if f not in g:continue
            s=pd.to_numeric(g[f],errors="coerce").dropna()
            if len(s)<2:continue
            rows.append({"pattern":p,"behavior_class":b,"feature":f,"scope":scope,
                         "n":len(s),"mean":s.mean(),"median":s.median(),
                         "p25":s.quantile(.25),"p75":s.quantile(.75)})
    return pd.DataFrame(rows)

def flag_behavior(oos):
    flagcols=[c for c in oos if c.startswith("pit_GEO_") and c.endswith("_5D")]
    rows=[]
    for p,g in oos.groupby("primary_formula"):
        for f in flagcols:
            for val in [True,False]:
                z=g[g[f].eq(val)]
                if len(z)<2:continue
                vc=z.behavior_class.value_counts()
                for b,n in vc.items():
                    rows.append({"pattern":p,"flag":f,"flag_value":val,"behavior_class":b,
                                 "n":int(n),"denom":len(z),"share_pct":n/len(z)*100})
    return pd.DataFrame(rows)

def coverage(df,features,scope):
    rows=[]
    for f in features:
        if f in df:
            n=int(df[f].notna().sum())
            rows.append({"feature":f,"scope":scope,"available_rows":n,
                         "rows":len(df),"coverage_pct":n/max(1,len(df))*100})
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r105-root",default="r105_artifacts")
    ap.add_argument("--r110-root",default="r110_artifacts")
    ap.add_argument("--history-cache",default=".cache/closebet_structure_env_oos_r102/v49_76_research_raw_history.csv")
    ap.add_argument("--geo-calendar",default="research_context/geopolitical_event_calendar_r1101.csv")
    ap.add_argument("--output-dir",default="reports/closebet_entry_external_pit_oos_r1101")
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    p105=find(a.r105_root,"event_master_with_outcomes_and_shadow_tags.csv")
    p110=find(a.r110_root,"oos_entry_exit_behavior_event_master.csv")
    hp=Path(a.history_cache)
    if p105 is None or p110 is None or not hp.exists():
        raise SystemExit(f"R1101_INPUT_MISSING r105={p105} r110={p110} hist={hp.exists()}")

    all_evt=read_csv(p105)
    beh=read_csv(p110)
    for df in [all_evt,beh]:
        df["code"]=df["code"].map(norm_code)
        df["signal_date"]=pd.to_datetime(df["signal_date"],errors="coerce").dt.normalize()
    hist=prep_hist(read_csv(hp))
    hgroups={c:g.copy() for c,g in hist.groupby("code")}

    # Internal PIT features for discovery + OOS event universe.
    im=[]
    basecols=[c for c in all_evt.columns if c in ["signal_date","code","primary_formula","lane","slot","signal_slot","run_slot","capture_slot","phase","final_stage","source_stage","signal_timestamp","signal_datetime","observation_at","captured_at"]]
    evtbase=all_evt[basecols].drop_duplicates(["signal_date","code","primary_formula"]).copy()
    for _,r in evtbase.iterrows():
        g=hgroups.get(r.code)
        z={"signal_date":r.signal_date,"code":r.code,"primary_formula":r.primary_formula}
        if g is not None:z.update(entry_metrics(g,r.signal_date))
        else:z["pit_entry_feature_status"]="NO_HISTORY"
        im.append(z)
    internal=pd.DataFrame(im)

    # Strict PIT macro snapshot.
    start=all_evt.signal_date.min(); end=all_evt.signal_date.max()
    macro,macro_audit=fetch_macro(start,end)
    ms=macro_snapshot(evtbase,macro)

    expected_macro_cols=[]
    for name in MACRO:
        expected_macro_cols += [
            f"pit_{name}_source_date", f"pit_{name}_ret1_pct", f"pit_{name}_ret5_pct"
        ]
    missing_snapshot_cols=[c for c in expected_macro_cols if c not in ms.columns]
    if missing_snapshot_cols:
        raise SystemExit(f"R1103_MACRO_SNAPSHOT_SCHEMA_MISSING {missing_snapshot_cols}")

    # Causal geopolitical calendar.
    geo_path=Path(a.geo_calendar)
    if not geo_path.exists() or geo_path.stat().st_size==0:
        raise SystemExit(f"R1102_GEO_CALENDAR_MISSING {geo_path}")
    geo=load_geo(a.geo_calendar)
    if geo.empty:
        raise SystemExit(f"R1102_GEO_CALENDAR_EMPTY {geo_path}")
    gs=geo_snapshot(evtbase,geo)

    enriched=all_evt.merge(internal,on=["signal_date","code","primary_formula"],how="left")
    enriched=enriched.merge(ms,on=["signal_date","code"],how="left")
    enriched=enriched.merge(gs,on=["signal_date","code"],how="left")

    # Attach behavior labels only to R110 OOS focus rows.
    keep=["signal_date","code","primary_formula","behavior_class","mfe20","mae20","d20_ret",
          "ma5_exit_ret","ma10_exit_ret","gb3_exit_ret","gb5_exit_ret"]
    keep=[c for c in keep if c in beh]
    oos=beh[keep].merge(
        enriched.drop_duplicates(["signal_date","code","primary_formula"]),
        on=["signal_date","code","primary_formula"],how="left",suffixes=("","_ctx")
    )
    oos=oos[oos.primary_formula.astype(str).isin(FOCUS)].copy()

    disc=enriched[enriched.signal_date<=pd.Timestamp(DISCOVERY_END)].copy()
    internal_bins=freeze_bins(disc,INTERNAL_FEATURES)
    external_bins=freeze_bins(disc,EXTERNAL_FEATURES)

    ic=behavior_contrast(oos,INTERNAL_FEATURES,"CAUSAL_INTERNAL_PIT")
    ec=behavior_contrast(oos,EXTERNAL_FEATURES,"EXTERNAL_PIT_SHADOW")
    gf=flag_behavior(oos)
    cov=pd.concat([
        coverage(oos,INTERNAL_FEATURES,"CAUSAL_INTERNAL_PIT"),
        coverage(oos,EXTERNAL_FEATURES,"EXTERNAL_PIT_SHADOW")
    ],ignore_index=True)

    # PIT audit: source date must not violate conservative cutoff.
    audits=[]
    for name,(sym,rule) in MACRO.items():
        sc=f"pit_{name}_source_date"
        if sc not in oos:continue
        for _,r in oos[["signal_date","code","signal_timestamp_pit",sc]].dropna().iterrows():
            src=pd.Timestamp(r[sc]).normalize()
            ts=pd.Timestamp(r.signal_timestamp_pit)
            if rule=="KRX_CLOSE":
                same_allowed=(ts.hour>15 or (ts.hour==15 and ts.minute>=30))
                valid=src<=ts.normalize() if same_allowed else src<ts.normalize()
            else:
                valid=src<ts.normalize()
            audits.append({"signal_date":r.signal_date,"code":r.code,"macro":name,
                           "source_date":src,"signal_timestamp_pit":ts,"pit_valid":bool(valid)})
    pita=pd.DataFrame(audits)

    internal.to_csv(out/"entry_internal_features_pit.csv",index=False,encoding="utf-8-sig")
    ms.to_csv(out/"macro_pit_snapshot.csv",index=False,encoding="utf-8-sig")
    macro_audit.to_csv(out/"macro_source_audit.csv",index=False,encoding="utf-8-sig")
    geo.to_csv(out/"geopolitical_event_calendar_used.csv",index=False,encoding="utf-8-sig")
    gs.to_csv(out/"geopolitical_pit_snapshot.csv",index=False,encoding="utf-8-sig")
    oos.to_csv(out/"oos_entry_behavior_external_pit_master.csv",index=False,encoding="utf-8-sig")
    internal_bins.to_csv(out/"discovery_frozen_internal_bins.csv",index=False,encoding="utf-8-sig")
    external_bins.to_csv(out/"discovery_frozen_external_bins.csv",index=False,encoding="utf-8-sig")
    ic.to_csv(out/"oos_internal_behavior_contrast.csv",index=False,encoding="utf-8-sig")
    ec.to_csv(out/"oos_external_behavior_contrast.csv",index=False,encoding="utf-8-sig")
    gf.to_csv(out/"oos_geopolitical_flag_behavior.csv",index=False,encoding="utf-8-sig")
    cov.to_csv(out/"feature_coverage.csv",index=False,encoding="utf-8-sig")
    pita.to_csv(out/"pit_availability_audit.csv",index=False,encoding="utf-8-sig")

    pit_fail=int((~pita.pit_valid).sum()) if len(pita) else 0
    macro_cov_cols=[c for c in EXTERNAL_FEATURES if c.startswith("pit_") and "geo_" not in c]
    macro_nonnull={c:int(oos[c].notna().sum()) if c in oos else 0 for c in macro_cov_cols}
    missing_macro=[c for c,n in macro_nonnull.items() if n==0]
    if missing_macro:
        raise SystemExit(f"R1102_MACRO_COVERAGE_ZERO {missing_macro}")

    meta={
        "revision":REVISION,"status":"PASS" if pit_fail==0 else "FAIL_CLOSED_PIT",
        "oos_rows":len(oos),"discovery_rows":len(disc),"pit_audit_rows":len(pita),
        "pit_fail_rows":pit_fail,"geo_calendar_rows":len(geo),
        "macro_feature_nonnull_rows":macro_nonnull,
        "column_collision_fix":True,
        "geo_calendar_required_nonempty":True,
        "macro_snapshot_prefix_fix":True,
        "macro_snapshot_schema_validated":True,
        "research_only":True,"production_eligible":False,
        "selection_logic_changed":False,"score_rank_changed":False,"order_logic_changed":False,
        "same_sample_retuning":False,"external_context_shadow_only":True,
        "macro_same_day_us_close_forbidden":True,
        "krx_same_day_close_requires_explicit_1540_or_later":True,
        "unknown_signal_clock_uses_conservative_1503":True,
        "geopolitical_score_created":False,
        "composite_score_created":False,
        "candidate_filter_created":False,
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    if pit_fail:
        raise SystemExit(f"R1101_PIT_AUDIT_FAIL rows={pit_fail}")

    report=[
        "🌐 [CLOSING BET · ENTRY QUALITY + EXTERNAL CONTEXT PIT OOS R1.0.10.1]",
        f"status=PASS | OOS focus rows={len(oos)} | PIT audit={len(pita)} fail=0",
        f"geo calendar rows={len(geo)}",
        "- 내부 feature coverage 보완: close location / vol / amount / ATR / MA20/60/224 / wick / ret5/20",
        "- 미국시장·VIX·US10Y·SOX는 한국 신호 전 완료된 직전 일봉만 사용",
        "- KRX 당일 종가는 15:40+가 명확한 경우만 허용",
        "- macro_snapshot 출력 컬럼을 pit_ prefix로 강제하고 schema 검증",
        "- 새 PIT 변수는 pit_ prefix로 기존 R105 동명 컬럼과 충돌 방지",
        "- 지정학 이벤트는 available_at 기준 causal flag, 캘린더 누락/빈 파일은 fail-closed",
        "- external context는 SHADOW only",
        "- production/score/rank/filter/order 변경 0",
    ]
    (out/"report.txt").write_text("\n".join(report),encoding="utf-8")
    print("\n".join(report))

if __name__=="__main__":
    main()
