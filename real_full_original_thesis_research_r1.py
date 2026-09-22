#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse, glob, hashlib, json, math, os, re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

RESEARCH_ID = "REAL_FULL_ORIGINAL_THESIS_RESEARCH_R1"
REVISION = "R1_0_FROZEN_20260922"
DISCOVERY_END = pd.Timestamp("2026-08-28")
HOLDOUT_START = pd.Timestamp("2026-09-01")
HOLDOUT_END = pd.Timestamp("2026-09-30")
HORIZONS = (1, 3, 5, 10, 15, 20)
OUT_DIR_DEFAULT = Path("reports/real_full_original_thesis_r1")


def num(v: Any) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else float("nan")
    except Exception:
        return float("nan")


def text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "none", "nat"} else s


def norm_code(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-6:].zfill(6) if s else ""


def read_csv(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, dtype={"code": str}, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def sha_obj(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def find_price_frame(cache_dir: Path, code: str) -> pd.DataFrame:
    pats = [str(cache_dir / f"{code}_*.pkl.gz"), str(cache_dir / f"{code}*.pkl*")]
    files: List[str] = []
    for p in pats:
        files += glob.glob(p)
    best = None
    for f in sorted(set(files)):
        try:
            o = pd.read_pickle(f)
            fr = o.get("frame") if isinstance(o, dict) else o
            if not isinstance(fr, pd.DataFrame) or fr.empty:
                continue
            if best is None or len(fr) > len(best):
                best = fr.copy()
        except Exception:
            continue
    if best is None:
        return pd.DataFrame()
    q = best.copy()
    if "Date" in q.columns:
        q["date"] = pd.to_datetime(q["Date"], errors="coerce")
    elif "date" in q.columns:
        q["date"] = pd.to_datetime(q["date"], errors="coerce")
    else:
        q["date"] = pd.to_datetime(q.index, errors="coerce")
    ren = {}
    for c in q.columns:
        lc = str(c).lower()
        if lc == "open": ren[c] = "open"
        elif lc == "high": ren[c] = "high"
        elif lc == "low": ren[c] = "low"
        elif lc == "close": ren[c] = "close"
        elif lc == "volume": ren[c] = "volume"
        elif lc in {"amount", "value", "tradingvalue", "trading_value", "거래대금"}: ren[c] = "amount"
    q = q.rename(columns=ren)
    keep = [c for c in ["date", "open", "high", "low", "close", "volume", "amount"] if c in q.columns]
    q = q[keep].dropna(subset=["date"]).copy()
    q["date"] = pd.to_datetime(q["date"], errors="coerce").dt.normalize()
    q = q.sort_values("date").drop_duplicates("date", keep="last")
    for c in ["open", "high", "low", "close", "volume", "amount"]:
        if c in q.columns: q[c] = pd.to_numeric(q[c], errors="coerce")
    if "amount" not in q.columns or not q["amount"].notna().any():
        q["amount"] = q.get("close") * q.get("volume")
        q["amount_source"] = "CLOSE_X_VOLUME_PROXY"
    else:
        q["amount_source"] = "ACTUAL_AMOUNT"
    q["ma5"] = q["close"].rolling(5, min_periods=5).mean()
    q["ma20"] = q["close"].rolling(20, min_periods=20).mean()
    q["ma60"] = q["close"].rolling(60, min_periods=60).mean()
    q["ma112"] = q["close"].rolling(112, min_periods=112).mean()
    q["ma224"] = q["close"].rolling(224, min_periods=224).mean()
    return q.reset_index(drop=True)


def first_num(r: pd.Series, *keys: str) -> float:
    for k in keys:
        if k in r.index:
            x = num(r.get(k))
            if math.isfinite(x): return x
    return float("nan")


def first_text(r: pd.Series, *keys: str) -> str:
    for k in keys:
        if k in r.index:
            s = text(r.get(k))
            if s: return s
    return ""


def cohort_for_date(d: pd.Timestamp) -> str:
    d = pd.Timestamp(d).normalize()
    if d <= DISCOVERY_END: return "DISCOVERY"
    if HOLDOUT_START <= d <= HOLDOUT_END: return "HOLDOUT_SEPTEMBER"
    return "OUTSIDE"


def signal_features(fr: pd.DataFrame, sd: pd.Timestamp) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    hist = fr[fr["date"].le(sd)].copy()
    if hist.empty or not hist["date"].eq(sd).any():
        return {"price_history_status":"SIGNAL_DATE_MISSING"}
    cur = hist[hist["date"].eq(sd)].iloc[-1]
    close = num(cur.get("close")); out["price_history_status"] = "READY"
    out["signal_close_cache"] = close
    for ma in (20,60,112,224):
        mv = num(cur.get(f"ma{ma}")); out[f"ma{ma}"] = mv
        out[f"close_vs_ma{ma}_pct"] = (close/mv-1)*100 if math.isfinite(close) and math.isfinite(mv) and mv>0 else np.nan
    ma_vals = [num(cur.get(f"ma{x}")) for x in (20,60,112,224)]
    finite = [x for x in ma_vals if math.isfinite(x) and x>0]
    out["ma_cluster_width_pct"] = ((max(finite)/min(finite))-1)*100 if len(finite)>=3 else np.nan
    p20 = hist.iloc[:-1].tail(20)
    p60 = hist.iloc[:-1].tail(60)
    med20v = pd.to_numeric(p20.get("volume"), errors="coerce").median() if len(p20) else np.nan
    med20a = pd.to_numeric(p20.get("amount"), errors="coerce").median() if len(p20) else np.nan
    out["signal_volume_vs_pre20"] = num(cur.get("volume"))/med20v if math.isfinite(num(cur.get("volume"))) and math.isfinite(num(med20v)) and med20v>0 else np.nan
    out["signal_amount_vs_pre20"] = num(cur.get("amount"))/med20a if math.isfinite(num(cur.get("amount"))) and math.isfinite(num(med20a)) and med20a>0 else np.nan
    if len(p60):
        basea = pd.to_numeric(p60.get("amount"), errors="coerce").median()
        p20a = pd.to_numeric(p20.get("amount"), errors="coerce")
        ratios = p20a/basea if math.isfinite(num(basea)) and basea>0 else pd.Series(dtype=float)
        out["pre20_gradual_amount_days_1p2_2x"] = int(((ratios>=1.2)&(ratios<2.0)).sum()) if len(ratios) else np.nan
        out["pre20_spike_amount_days_ge2x"] = int((ratios>=2.0).sum()) if len(ratios) else np.nan
    return out


def path_metrics(fr: pd.DataFrame, sd: pd.Timestamp, entry: float, h: int, wave_high: float, pb_low: float) -> Dict[str, Any]:
    fut = fr[fr["date"].gt(sd)].sort_values("date").reset_index(drop=True)
    pfx = f"d{h}_"
    if len(fut) < h or not math.isfinite(entry) or entry<=0:
        return {pfx+"complete":0}
    p = fut.iloc[:h].copy(); last = p.iloc[-1]
    high_i = pd.to_numeric(p["high"], errors="coerce").idxmax(); low_i = pd.to_numeric(p["low"], errors="coerce").idxmin()
    hi = num(p.loc[high_i,"high"]); lo = num(p.loc[low_i,"low"]); cl = num(last.get("close"))
    out = {
        pfx+"complete":1,
        pfx+"date":pd.Timestamp(last["date"]).date().isoformat(),
        pfx+"close_ret_pct":(cl/entry-1)*100,
        pfx+"mfe_pct":(hi/entry-1)*100,
        pfx+"mae_pct":(lo/entry-1)*100,
        pfx+"high_date":pd.Timestamp(p.loc[high_i,"date"]).date().isoformat(),
        pfx+"low_date":pd.Timestamp(p.loc[low_i,"date"]).date().isoformat(),
        pfx+"hit_plus3":int(hi>=entry*1.03), pfx+"hit_plus5":int(hi>=entry*1.05),
        pfx+"hit_plus10":int(hi>=entry*1.10), pfx+"hit_plus15":int(hi>=entry*1.15),
        pfx+"wave1_high_retested":int(math.isfinite(wave_high) and hi>=wave_high) if math.isfinite(wave_high) else np.nan,
        pfx+"pullback_low_breached":int(math.isfinite(pb_low) and lo<pb_low) if math.isfinite(pb_low) else np.nan,
    }
    return out


def first_hit_date(fr: pd.DataFrame, sd: pd.Timestamp, level: float, max_h: int=20) -> str:
    fut = fr[fr["date"].gt(sd)].sort_values("date").head(max_h)
    z = fut[pd.to_numeric(fut["high"],errors="coerce").ge(level)]
    return pd.Timestamp(z.iloc[0]["date"]).date().isoformat() if len(z) else ""


def classify_path(r: pd.Series) -> str:
    if int(num(r.get("d20_complete")) or 0) != 1: return "IMMATURE_D20"
    d5hit = int(num(r.get("d5_hit_plus5")) or 0)==1
    d20hit = int(num(r.get("d20_hit_plus5")) or 0)==1
    d5c = num(r.get("d5_close_ret_pct")); d20c = num(r.get("d20_close_ret_pct")); mae = num(r.get("d20_mae_pct"))
    br = num(r.get("d20_pullback_low_breached"))
    if d5hit and math.isfinite(d20c) and d20c<=0: return "EARLY_SPIKE_GIVEBACK"
    if br==1 and math.isfinite(d20c) and d20c<=0: return "STRUCTURE_BREAK"
    if d20hit and math.isfinite(d20c) and d20c>0 and math.isfinite(mae) and mae<=-3: return "SHAKEOUT_THEN_GO"
    if d5hit and math.isfinite(d5c) and d5c>0: return "FAST_SUCCESS"
    if (not d5hit) and d20hit and math.isfinite(d20c) and d20c>0: return "DELAYED_SWING"
    if (not d20hit) and math.isfinite(d20c) and -3<d20c<3: return "TIME_FAILURE"
    return "FAILURE"


def split_sim(fr: pd.DataFrame, sd: pd.Timestamp, entry: float, d20_close: float, structural_level: float) -> Dict[str, Any]:
    fut = fr[fr["date"].gt(sd)].sort_values("date").head(20).copy()
    out = {"split_one_shot_ret_d20":np.nan,"split_50_50_3pct_ret_d20":np.nan,"split_50_50_structure_ret_d20":np.nan,
           "split_3pct_second_filled":0,"split_structure_second_filled":0}
    if not math.isfinite(entry) or entry<=0 or not math.isfinite(d20_close): return out
    out["split_one_shot_ret_d20"]=(d20_close/entry-1)*100
    level3=entry*0.97; hit3 = len(fut[pd.to_numeric(fut["low"],errors="coerce").le(level3)])>0
    avg3=(entry+level3)/2 if hit3 else entry
    out["split_3pct_second_filled"]=int(hit3); out["split_50_50_3pct_ret_d20"]=(d20_close/avg3-1)*100
    if math.isfinite(structural_level) and 0<structural_level<entry:
        hs=len(fut[pd.to_numeric(fut["low"],errors="coerce").le(structural_level)])>0
        avgs=(entry+structural_level)/2 if hs else entry
        out["split_structure_second_filled"]=int(hs); out["split_50_50_structure_ret_d20"]=(d20_close/avgs-1)*100
    return out


def availability_audit(events: pd.DataFrame) -> pd.DataFrame:
    groups = {
        "scanner_pattern":["origin_search_pattern","origin_search_matches","origin_pattern_exact_combo","origin_pattern_overlap_count"],
        "market_context":["origin_market_context_status","origin_market_context_polarity","origin_rotation_bucket"],
        "sector_context":["sector","industry","sector_context","sector_strength","sector_breadth"],
        "attention":["attention_level","attention_acceleration","news_count","search_trend","turnover_rank"],
        "fundamental_pit":["sales_yoy","operating_profit_yoy","sales_consensus","op_consensus","sales_revision_1m","op_revision_1m"],
        "kki_repeat":["repeat_appearance_count","prior_wave_count","days_since_last_wave","familiar_repeat_count"],
    }
    rows=[]
    for grp, cols in groups.items():
        present=[c for c in cols if c in events.columns]
        nonnull=sum(int(events[c].notna().sum()) for c in present)
        rows.append({"group":grp,"candidate_fields":" | ".join(cols),"present_fields":" | ".join(present),"present_field_count":len(present),"nonnull_cells":nonnull,
                     "status":"AVAILABLE_PARTIAL" if present and nonnull else "UNAVAILABLE_POINT_IN_TIME_REQUIRED"})
    return pd.DataFrame(rows)


def build_blind_review(df: pd.DataFrame) -> Tuple[pd.DataFrame,pd.DataFrame]:
    if df.empty: return pd.DataFrame(),pd.DataFrame()
    z=df.copy(); z["_h"]=z.apply(lambda r: sha_obj([r.get("origin_date"),r.get("code"),r.get("origin_rank")]),axis=1); z=z.sort_values("_h").reset_index(drop=True)
    z["blind_id"]=[f"P{i:04d}" for i in range(1,len(z)+1)]
    key_cols=["blind_id","origin_date","code","name","cohort","path_class","d5_close_ret_pct","d20_close_ret_pct","d20_mfe_pct","d20_mae_pct"]
    key=z[[c for c in key_cols if c in z.columns]].copy()
    deny=("d1_","d3_","d5_","d10_","d15_","d20_","split_")
    cols=[c for c in z.columns if not c.startswith(deny) and c not in {"path_class","_h"}]
    review=z[cols].copy(); review["manual_pattern_fidelity"]=""; review["manual_observed_structure"]=""; review["manual_notes"]=""
    return review,key


def summarize(df: pd.DataFrame, by: str) -> pd.DataFrame:
    if by not in df.columns or df.empty: return pd.DataFrame()
    rows=[]
    for k,g in df.groupby(by,dropna=False):
        d20=g[pd.to_numeric(g.get("d20_complete"),errors="coerce").eq(1)] if "d20_complete" in g else pd.DataFrame()
        rows.append({by:str(k),"events":len(g),"d20_mature":len(d20),"d20_close_median":pd.to_numeric(d20.get("d20_close_ret_pct"),errors="coerce").median() if len(d20) else np.nan,
                     "d20_mfe_median":pd.to_numeric(d20.get("d20_mfe_pct"),errors="coerce").median() if len(d20) else np.nan,
                     "d20_mae_median":pd.to_numeric(d20.get("d20_mae_pct"),errors="coerce").median() if len(d20) else np.nan,
                     "d20_plus5_rate":pd.to_numeric(d20.get("d20_hit_plus5"),errors="coerce").mean() if len(d20) else np.nan})
    return pd.DataFrame(rows)


def run(args) -> int:
    out=Path(args.output_dir); out.mkdir(parents=True,exist_ok=True)
    ev=read_csv(args.event_ledger)
    if ev.empty: raise SystemExit("EVENT_LEDGER_EMPTY_OR_MISSING")
    date_col="origin_date" if "origin_date" in ev.columns else "signal_date"
    ev[date_col]=pd.to_datetime(ev[date_col],errors="coerce").dt.normalize(); ev=ev[ev[date_col].notna()].copy(); ev["origin_date"]=ev[date_col]
    ev["code"]=ev.get("code",pd.Series("",index=ev.index)).map(norm_code)
    ev["cohort"]=ev["origin_date"].map(cohort_for_date)
    ev=ev[ev["cohort"].isin(["DISCOVERY","HOLDOUT_SEPTEMBER"])].copy()
    results=[]; missing=[]
    for _,r in ev.iterrows():
        code=norm_code(r.get("code")); sd=pd.Timestamp(r["origin_date"]); fr=find_price_frame(Path(args.price_cache_dir),code)
        base=r.to_dict(); base["origin_date"]=sd.date().isoformat(); base["code"]=code
        if fr.empty:
            base["price_history_status"]="MISSING"; missing.append(f"{base['origin_date']}|{code}"); results.append(base); continue
        sf=signal_features(fr,sd); base.update(sf)
        entry=first_num(r,"origin_price","entry_price","snapshot_price","source_entry_price","현재가")
        if not math.isfinite(entry): entry=num(sf.get("signal_close_cache"))
        base["research_entry_price"]=entry
        wave_high=first_num(r,"wave1_high_price","inferred_wave1_high")
        pb_low=first_num(r,"pullback_low_price","frozen_optimal_low","inferred_pb_low")
        base["research_wave1_high"]=wave_high; base["research_pullback_low"]=pb_low
        for h in HORIZONS: base.update(path_metrics(fr,sd,entry,h,wave_high,pb_low))
        for pct in (3,5,10,15): base[f"first_plus{pct}_date_d20"]=first_hit_date(fr,sd,entry*(1+pct/100),20) if math.isfinite(entry) else ""
        results.append(base)
    res=pd.DataFrame(results)
    if not res.empty:
        res["path_class"]=res.apply(classify_path,axis=1)
        sims=[]
        for _,r in res.iterrows():
            fr=find_price_frame(Path(args.price_cache_dir),norm_code(r.get("code"))); sd=pd.Timestamp(r["origin_date"]); entry=num(r.get("research_entry_price")); d20c=np.nan
            if not fr.empty and int(num(r.get("d20_complete")) or 0)==1:
                fut=fr[fr["date"].gt(sd)].sort_values("date").head(20); d20c=num(fut.iloc[-1].get("close")) if len(fut)>=20 else np.nan
            sims.append(split_sim(fr,sd,entry,d20c,num(r.get("research_pullback_low"))))
        res=pd.concat([res.reset_index(drop=True),pd.DataFrame(sims)],axis=1)
    res.to_csv(out/"real_full_original_thesis_event_master.csv",index=False,encoding="utf-8-sig")
    availability_audit(res).to_csv(out/"real_full_original_thesis_data_availability.csv",index=False,encoding="utf-8-sig")
    for by in ["cohort","path_class","origin_search_pattern","origin_pattern_exact_combo","origin_market_context_polarity","origin_rotation_bucket"]:
        s=summarize(res,by)
        if not s.empty: s.to_csv(out/f"summary_{by}.csv",index=False,encoding="utf-8-sig")
    review,key=build_blind_review(res)
    review.to_csv(out/"pattern_fidelity_blind_review.csv",index=False,encoding="utf-8-sig"); key.to_csv(out/"pattern_fidelity_blind_key.csv",index=False,encoding="utf-8-sig")
    discovery=res[res["cohort"].eq("DISCOVERY")].copy(); holdout=res[res["cohort"].eq("HOLDOUT_SEPTEMBER")].copy()
    discovery.to_csv(out/"discovery_through_20260828.csv",index=False,encoding="utf-8-sig"); holdout.to_csv(out/"holdout_september_2026.csv",index=False,encoding="utf-8-sig")
    meta={
      "research_id":RESEARCH_ID,"revision":REVISION,"status":"PASS","discovery_end":"2026-08-28","holdout_start":"2026-09-01","holdout_end":"2026-09-30",
      "horizons_trading_days":list(HORIZONS),"events":len(res),"discovery_events":len(discovery),"holdout_events":len(holdout),"missing_price_events":len(set(missing)),
      "research_only":True,"production_eligible":False,"selection_logic_changed":False,"score_rank_changed":False,"order_logic_changed":False,"new_gate_added":False,
      "same_sample_retuning":False,"lookahead_predictor_allowed":False,"future_bars_used_for":"OUTCOME_AND_SHADOW_EXECUTION_ONLY",
      "design_hash":sha_obj({"revision":REVISION,"discovery_end":"2026-08-28","holdout":"2026-09","horizons":HORIZONS,"path_taxonomy":"FROZEN_R1","split_rules":"FROZEN_R1"})}
    (out/"real_full_original_thesis_meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    rep=["🧪 [REAL_FULL ORIGINAL THESIS RESEARCH R1]",f"status=PASS revision={REVISION}",f"events={len(res)} discovery={len(discovery)} holdout={len(holdout)} missing_price={len(set(missing))}",
         "discovery<=2026-08-28 | holdout=2026-09-01..2026-09-30",f"horizons={list(HORIZONS)}","research_only=1 production_eligible=0 selection/score/rank/order change=0",
         "attention/sector/fundamental fields require point-in-time proof; unavailable data are never backfilled from current values."]
    (out/"real_full_original_thesis_report.txt").write_text("\n".join(rep)+"\n",encoding="utf-8")
    print("\n".join(rep)); return 0


def self_test() -> int:
    dates=pd.bdate_range("2026-07-01",periods=250); close=np.linspace(90,110,len(dates)); fr=pd.DataFrame({"date":dates,"open":close,"high":close*1.01,"low":close*.99,"close":close,"volume":1000,"amount":close*1000})
    sd=dates[224]; entry=float(fr.loc[224,"close"]); fr.loc[225:229,"high"]=entry*1.06; fr.loc[225:244,"close"]=entry*1.02
    d={};
    for h in HORIZONS:d.update(path_metrics(fr,sd,entry,h,float("nan"),float("nan")))
    s=pd.Series(d); assert s["d5_complete"]==1 and s["d20_complete"]==1 and s["d5_hit_plus5"]==1
    s["d20_close_ret_pct"]=2.0; assert classify_path(s)=="FAST_SUCCESS"
    assert cohort_for_date(pd.Timestamp("2026-08-28"))=="DISCOVERY" and cohort_for_date(pd.Timestamp("2026-09-01"))=="HOLDOUT_SEPTEMBER"
    print("REAL_FULL_ORIGINAL_THESIS_R1_SELF_TEST PASS"); return 0


def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--event-ledger",default="reports/real_full_validation_backtest/analysis/pattern_backtest_event_ledger.csv"); ap.add_argument("--price-cache-dir",default="reports/.cache/v20_price_history"); ap.add_argument("--output-dir",default=str(OUT_DIR_DEFAULT)); ap.add_argument("--self-test",action="store_true"); a=ap.parse_args()
    return self_test() if a.self_test else run(a)

if __name__=="__main__": raise SystemExit(main())
