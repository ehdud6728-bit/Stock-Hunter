#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse, glob, hashlib, json, math, re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

RESEARCH_ID = "REAL_FULL_PATTERN_TRUTH_UNKNOWN_DISCOVERY_R1"
REVISION = "R1_PATTERN_TRUTH_UNKNOWN_DISCOVERY_20260923"
DISCOVERY_END = pd.Timestamp("2026-08-28")
HOLDOUT_START = pd.Timestamp("2026-09-01")
HOLDOUT_END = pd.Timestamp("2026-09-30")
BLIND_SAMPLE_N = 40
BAR_LOOKBACK = 50


def num(v: Any) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else np.nan
    except Exception:
        return np.nan


def txt(v: Any) -> str:
    try:
        if v is None or pd.isna(v): return ""
    except Exception:
        pass
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "none", "nat"} else s


def code(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-6:].zfill(6) if s else ""


def sha_obj(v: Any) -> str:
    return hashlib.sha256(json.dumps(v, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def read_csv(p: str | Path) -> pd.DataFrame:
    p = Path(p)
    if not p.exists() or p.stat().st_size == 0: return pd.DataFrame()
    try: return pd.read_csv(p, dtype={"code": str}, low_memory=False)
    except pd.errors.EmptyDataError: return pd.DataFrame()


def load_price(cache: Path, c: str, sd: pd.Timestamp, allow_refetch: bool) -> pd.DataFrame:
    files: List[str] = []
    for pat in (f"{c}_*.pkl.gz", f"{c}*.pkl*"):
        files.extend(glob.glob(str(cache / pat)))
    best = None; source = "EXISTING_CACHE"
    for f in sorted(set(files)):
        try:
            o = pd.read_pickle(f); fr = o.get("frame") if isinstance(o, dict) else o
            if isinstance(fr, pd.DataFrame) and not fr.empty and (best is None or len(fr) > len(best)): best = fr.copy()
        except Exception: pass
    if best is None and allow_refetch:
        try:
            import FinanceDataReader as fdr
            start = (sd - pd.Timedelta(days=750)).strftime("%Y-%m-%d")
            end = (sd + pd.Timedelta(days=10)).strftime("%Y-%m-%d")
            fr = fdr.DataReader(c, start, end)
            if isinstance(fr, pd.DataFrame) and not fr.empty:
                best = fr.copy(); source = "FDR_HISTORICAL_REFETCH_RESEARCH_ONLY"
                cache.mkdir(parents=True, exist_ok=True)
                pd.to_pickle({"frame": best, "source": source}, cache / f"{c}_pattern_truth_r1.pkl.gz", compression="gzip")
        except Exception: best = None
    if best is None: return pd.DataFrame()
    q = best.copy()
    if "Date" in q.columns: q["date"] = pd.to_datetime(q["Date"], errors="coerce")
    elif "date" in q.columns: q["date"] = pd.to_datetime(q["date"], errors="coerce")
    else: q["date"] = pd.to_datetime(q.index, errors="coerce")
    ren = {}
    for col in q.columns:
        lc = str(col).lower()
        if lc in {"open", "high", "low", "close", "volume"}: ren[col] = lc
        elif lc in {"amount", "value", "tradingvalue", "trading_value", "거래대금"}: ren[col] = "amount"
    q = q.rename(columns=ren)
    q = q[[x for x in ["date", "open", "high", "low", "close", "volume", "amount"] if x in q]].dropna(subset=["date"]).copy()
    q["date"] = pd.to_datetime(q["date"], errors="coerce").dt.normalize()
    q = q.sort_values("date").drop_duplicates("date", keep="last")
    for x in ["open", "high", "low", "close", "volume", "amount"]:
        if x in q: q[x] = pd.to_numeric(q[x], errors="coerce")
    if "amount" not in q or not q["amount"].notna().any(): q["amount"] = q["close"] * q["volume"]
    q["ma5"] = q["close"].rolling(5, min_periods=5).mean()
    q["ma20"] = q["close"].rolling(20, min_periods=20).mean()
    q["ma40"] = q["close"].rolling(40, min_periods=40).mean()
    v = q["volume"].replace(0, np.nan)
    q["vwma40"] = (q["close"] * v).rolling(40, min_periods=40).sum() / v.rolling(40, min_periods=40).sum()
    sd40 = q["close"].rolling(40, min_periods=40).std(ddof=0)
    q["bb40_width"] = (4 * sd40 / q["ma40"]).replace([np.inf, -np.inf], np.nan)
    q.attrs["source"] = source
    return q.reset_index(drop=True)


def raw_family(r: pd.Series) -> str:
    tags = set(x for x in txt(r.get("structure_tags")).split("|") if x and x != "UNCLASSIFIED")
    fam = []
    if {"strong_wave1", "shallow_pullback"}.issubset(tags): fam.append("STRONG_WAVE1_SHALLOW_PULLBACK")
    if "deep_pullback" in tags: fam.append("DEEP_PULLBACK_SHAKEOUT")
    if "ma224_base_or_reclaim" in tags: fam.append("MA224_BASE_RECLAIM_CONTEXT")
    if "ma_compression" in tags: fam.append("MA_COMPRESSION")
    if "gradual_accumulation" in tags: fam.append("GRADUAL_ACCUMULATION")
    if "supply_drying" in tags: fam.append("SUPPLY_DRYING")
    if "liquidity_retained" in tags: fam.append("LIQUIDITY_RETAINED")
    if "reacceleration" in tags: fam.append("REACCELERATION")
    if "prior_kki" in tags: fam.append("PRIOR_KKI")
    if "overextended" in tags: fam.append("OVEREXTENDED")
    return "|".join(fam) if fam else "RAW_UNCLASSIFIED"


def semantic_contract() -> pd.DataFrame:
    # Only claims grounded in repository definitions are auto-checked. Others are manual.
    rows = [
        {"contract_id":"FIRST_PULLBACK_RESTART", "match_regex":r"첫\s*눌림|재양봉|PULLBACK_RESTART_CLOSE|PRC-SHADOW|V72-PRC",
         "source_basis":"search_formula_truth_audit.py PRC_SCOPE_TOKENS + familiar_research.py FIRST_LONG_BULL_PULLBACK",
         "auto_check_scope":"WAVE1_PLUS_PULLBACK; REACCELERATION_IF_REBULL_TEXT", "authority":"PARTIAL_SEMANTIC_CONTRACT"},
        {"contract_id":"BLUE_DOTTED_RECLAIM", "match_regex":r"파란점선|BLUE_DOTTED_RECLAIM|blue[_ ]line",
         "source_basis":"pattern_ai_cross_research.py BLUE_DOTTED_RECLAIM + scanner/pattern_overhaul_complete.py common resistance/reclaim trigger",
         "auto_check_scope":"FROZEN_BLUE_LEVEL_IF_AVAILABLE", "authority":"PARTIAL_SEMANTIC_CONTRACT"},
        {"contract_id":"WATERMELON_STATE", "match_regex":r"수박|WATERMELON",
         "source_basis":"multiple state-machine generations; indicator_engine.py/main*.py/render_blocks.py",
         "auto_check_scope":"NONE", "authority":"SEMANTIC_MAPPING_REQUIRED"},
        {"contract_id":"MA5_RECLAIM", "match_regex":r"5일선.*재안착|MA5_PULLBACK_RECLAIM|ma5.*reclaim",
         "source_basis":"pattern_ai_cross_research.py MA5_PULLBACK_RECLAIM",
         "auto_check_scope":"MA5_LEVEL_IF_PRICE_AVAILABLE", "authority":"PARTIAL_SEMANTIC_CONTRACT"},
    ]
    return pd.DataFrame(rows)


def contract_evidence(r: pd.Series, fr: pd.DataFrame, sd: pd.Timestamp) -> Dict[str, Any]:
    label_blob = " | ".join(txt(r.get(k)) for k in ["origin_search_pattern","origin_search_matches","origin_canonical_pattern","origin_pattern_exact_combo"])
    out: Dict[str, Any] = {"semantic_contract_ids":"", "auto_pattern_truth_state":"SEMANTIC_MAPPING_REQUIRED", "auto_pattern_truth_reason":"NO_SUPPORTED_CONTRACT_MATCH"}
    contracts = semantic_contract(); hits=[]; verdicts=[]; reasons=[]
    sig = fr[fr.date.eq(sd)].iloc[-1] if (not fr.empty and fr.date.eq(sd).any()) else None
    for _,c in contracts.iterrows():
        if not re.search(c.match_regex, label_blob, flags=re.I): continue
        hits.append(c.contract_id)
        if c.authority == "SEMANTIC_MAPPING_REQUIRED":
            verdicts.append("MANUAL"); reasons.append(f"{c.contract_id}:SEMANTIC_MAPPING_REQUIRED"); continue
        if c.contract_id == "FIRST_PULLBACK_RESTART":
            wready = int(num(r.get("wave1_ready")) or 0) == 1
            pdays = num(r.get("pullback_days")); pb = math.isfinite(pdays) and pdays >= 1
            base_ok = wready and pb
            wants_rebull = bool(re.search(r"재양봉|RESTART", label_blob, flags=re.I))
            sigret = num(r.get("signal_ret_pct")); reacc = int(num(r.get("tag_reacceleration")) or 0) == 1 or (math.isfinite(sigret) and sigret > 0)
            ok = base_ok and ((not wants_rebull) or reacc)
            verdicts.append("SUPPORT" if ok else "CONTRADICT")
            reasons.append(f"FIRST_PULLBACK_RESTART:wave1={int(wready)},pullback={int(pb)},rebull_required={int(wants_rebull)},rebull={int(reacc)}")
        elif c.contract_id == "BLUE_DOTTED_RECLAIM":
            blue = num(r.get("frozen_blue")); close = num(sig.get("close")) if sig is not None else np.nan
            if math.isfinite(blue) and blue > 0 and math.isfinite(close):
                diff=(close/blue-1)*100
                ok=diff >= -1.0
                verdicts.append("SUPPORT" if ok else "CONTRADICT"); reasons.append(f"BLUE_DOTTED_RECLAIM:close_vs_blue_pct={diff:.3f}")
            else:
                verdicts.append("MANUAL"); reasons.append("BLUE_DOTTED_RECLAIM:FROZEN_BLUE_OR_PRICE_UNAVAILABLE")
        elif c.contract_id == "MA5_RECLAIM":
            if sig is not None:
                close=num(sig.get("close")); ma5=num(sig.get("ma5")); ok=math.isfinite(close) and math.isfinite(ma5) and close>=ma5*0.995
                verdicts.append("SUPPORT" if ok else "CONTRADICT"); reasons.append(f"MA5_RECLAIM:close_vs_ma5_pct={((close/ma5-1)*100) if ma5>0 else np.nan:.3f}")
            else:
                verdicts.append("MANUAL"); reasons.append("MA5_RECLAIM:PRICE_UNAVAILABLE")
    out["semantic_contract_ids"]="|".join(hits)
    if not hits: return out
    if "CONTRADICT" in verdicts:
        out["auto_pattern_truth_state"]="AUTO_EVIDENCE_CONTRADICTS"
    elif verdicts and all(v=="SUPPORT" for v in verdicts):
        out["auto_pattern_truth_state"]="AUTO_EVIDENCE_SUPPORTS"
    elif "SUPPORT" in verdicts:
        out["auto_pattern_truth_state"]="AUTO_PARTIAL_SUPPORT_MANUAL_REVIEW"
    else:
        out["auto_pattern_truth_state"]="SEMANTIC_MAPPING_REQUIRED"
    out["auto_pattern_truth_reason"]=" || ".join(reasons)
    return out


def make_blind_sample(df: pd.DataFrame, n: int) -> Tuple[pd.DataFrame,pd.DataFrame]:
    disc=df[pd.to_datetime(df.origin_date).le(DISCOVERY_END)].copy()
    if disc.empty: return pd.DataFrame(),pd.DataFrame()
    target=min(n,len(disc)); quotas={"FAST_SUCCESS":10,"EARLY_SPIKE_GIVEBACK":10,"FAILURE":10,"SHAKEOUT_THEN_GO":10}
    picks=[]; used=set()
    for cls,q in quotas.items():
        z=disc[disc.path_class.eq(cls)].copy(); z["_h"]=z.apply(lambda r: sha_obj(["SEL",r.get("origin_date"),r.get("code"),r.get("origin_rank")]),axis=1)
        for _,r in z.sort_values("_h").head(q).iterrows():
            eid=f"{r.get('origin_date')}|{r.get('code')}|{r.get('origin_rank')}"
            if eid not in used: used.add(eid);picks.append(r.drop(labels=["_h"],errors="ignore"))
    if len(picks)<target:
        z=disc.copy();z["_eid"]=z.apply(lambda r:f"{r.get('origin_date')}|{r.get('code')}|{r.get('origin_rank')}",axis=1);z=z[~z._eid.isin(used)];z["_h"]=z._eid.map(lambda x:sha_obj(["FILL",x]))
        picks += [r.drop(labels=["_eid","_h"],errors="ignore") for _,r in z.sort_values("_h").head(target-len(picks)).iterrows()]
    s=pd.DataFrame(picks).head(target).copy();s["_h"]=s.apply(lambda r:sha_obj(["ORDER",r.get("origin_date"),r.get("code"),r.get("origin_rank")]),axis=1);s=s.sort_values("_h").reset_index(drop=True);s["blind_id"]=[f"PT{i:03d}" for i in range(1,len(s)+1)]
    key_cols=["blind_id","origin_date","code","name","path_class","forensic_group","d5_close_ret_pct","d20_close_ret_pct","d20_mfe_pct","d20_mae_pct"]
    key=s[[c for c in key_cols if c in s]].copy()
    deny_prefix=("d1_","d3_","d5_","d10_","d15_","d20_","ew_","split_")
    deny={"path_class","forensic_group","early_warning_count","early_warning_tags","_h"}
    cols=[c for c in s.columns if c not in deny and not c.startswith(deny_prefix)]
    review=s[cols].copy()
    review["manual_pattern_fidelity"]="" # TRUE_MATCH/PARTIAL_MATCH/MISLABEL/MULTI_PATTERN/UNKNOWN_PATTERN
    review["manual_observed_structure"]=""
    review["manual_secondary_patterns"]=""
    review["manual_confidence"]=""
    review["manual_notes"]=""
    return review,key


def bars_for_blind(review: pd.DataFrame, price_frames: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    rows=[]
    for _,r in review.iterrows():
        c=code(r.code);sd=pd.Timestamp(r.origin_date);fr=price_frames.get(f"{c}|{sd.date()}",pd.DataFrame())
        if fr.empty: continue
        h=fr[fr.date.le(sd)].tail(BAR_LOOKBACK).copy();
        if h.empty:continue
        h["bar_offset"]=np.arange(-len(h)+1,1)
        for _,b in h.iterrows():
            rows.append({"blind_id":r.blind_id,"bar_offset":int(b.bar_offset),"date":pd.Timestamp(b.date).date().isoformat(),"open":b.get("open"),"high":b.get("high"),"low":b.get("low"),"close":b.get("close"),"volume":b.get("volume"),"amount":b.get("amount"),"ma5":b.get("ma5"),"ma20":b.get("ma20"),"vwma40":b.get("vwma40"),"bb40_width":b.get("bb40_width")})
    return pd.DataFrame(rows)


def discovery_signatures(df: pd.DataFrame) -> Tuple[pd.DataFrame,pd.DataFrame]:
    d=df[pd.to_datetime(df.origin_date).le(DISCOVERY_END)].copy();m=d[pd.to_numeric(d.get("d20_complete"),errors="coerce").eq(1)].copy()
    rows=[]
    for sig,g in m.groupby("raw_structure_family",dropna=False):
        if len(g)<3:continue
        rows.append({"raw_structure_family":sig,"events":len(g),"success_rate":g.forensic_group.eq("SUCCESS").mean(),"failure_rate":g.forensic_group.eq("FAILURE").mean(),"giveback_rate":g.path_class.eq("EARLY_SPIKE_GIVEBACK").mean(),"d20_close_median":pd.to_numeric(g.d20_close_ret_pct,errors="coerce").median(),"scanner_label_nunique":g.origin_search_pattern.fillna("").astype(str).nunique(),"scanner_labels":" | ".join(sorted(set(x for x in g.origin_search_pattern.fillna("").astype(str) if x))[:8])})
    summ=pd.DataFrame(rows).sort_values(["events","success_rate"],ascending=[False,False]) if rows else pd.DataFrame()
    # Novel candidate = repeated successful raw signature where scanner exact taxonomy is mostly missing/gapped.
    cand=[]
    for sig,g in m.groupby("raw_structure_family",dropna=False):
        succ=g[g.forensic_group.eq("SUCCESS")]
        gap=pd.to_numeric(g.get("taxonomy_gap_candidate"),errors="coerce").fillna(0).eq(1).mean() if len(g) else np.nan
        if len(g)>=3 and len(succ)>=2 and gap>=0.5 and sig!="RAW_UNCLASSIFIED":
            cand.append({"candidate_signature":sig,"events":len(g),"success_events":len(succ),"success_rate":len(succ)/len(g),"taxonomy_gap_rate":gap,"status":"DISCOVERY_ONLY_NEEDS_FREEZE_THEN_OOS","do_not_name_as_production_pattern":1})
    return summ,pd.DataFrame(cand)


def run(a) -> int:
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    src=read_csv(a.r121_master)
    if src.empty: raise SystemExit("R121_MASTER_MISSING_OR_EMPTY")
    src["origin_date"]=pd.to_datetime(src.origin_date,errors="coerce").dt.normalize();src["code"]=src.code.map(code)
    cache=Path(a.price_cache_dir);frames={};rows=[];missing=[]
    for _,r in src.iterrows():
        sd=pd.Timestamp(r.origin_date);c=code(r.code);fr=load_price(cache,c,sd,a.allow_price_refetch);frames[f"{c}|{sd.date()}"]=fr
        d=r.to_dict();d["origin_date"]=sd.date().isoformat();d["raw_structure_family"]=raw_family(r)
        if fr.empty:
            missing.append(f"{sd.date()}|{c}");d.update({"pattern_truth_price_status":"MISSING","semantic_contract_ids":"","auto_pattern_truth_state":"PRICE_MISSING","auto_pattern_truth_reason":"PRICE_MISSING"})
        else:
            d["pattern_truth_price_status"]="READY";d.update(contract_evidence(r,fr,sd))
        rows.append(d)
    z=pd.DataFrame(rows);z.to_csv(out/"pattern_truth_event_master.csv",index=False,encoding="utf-8-sig")
    semantic_contract().to_csv(out/"pattern_semantic_contract.csv",index=False,encoding="utf-8-sig")
    auto=z.groupby("auto_pattern_truth_state",dropna=False).size().reset_index(name="events");auto.to_csv(out/"auto_pattern_truth_state_summary.csv",index=False,encoding="utf-8-sig")
    # Scanner label x raw family cross-audit (descriptive, no outcomes required)
    cross=z.groupby(["origin_search_pattern","raw_structure_family"],dropna=False).size().reset_index(name="events").sort_values("events",ascending=False);cross.to_csv(out/"scanner_label_x_raw_structure.csv",index=False,encoding="utf-8-sig")
    review,key=make_blind_sample(z,BLIND_SAMPLE_N);review.to_csv(out/"pattern_truth_blind_review.csv",index=False,encoding="utf-8-sig");key.to_csv(out/"pattern_truth_blind_key_DO_NOT_OPEN_UNTIL_REVIEW.csv",index=False,encoding="utf-8-sig")
    bars=bars_for_blind(review,frames);bars.to_csv(out/"pattern_truth_blind_bars.csv",index=False,encoding="utf-8-sig")
    summ,cand=discovery_signatures(z);summ.to_csv(out/"discovery_raw_structure_signature_summary.csv",index=False,encoding="utf-8-sig");cand.to_csv(out/"candidate_novel_structure_signatures.csv",index=False,encoding="utf-8-sig")
    disc=z[pd.to_datetime(z.origin_date).le(DISCOVERY_END)].copy();hold=z[(pd.to_datetime(z.origin_date).ge(HOLDOUT_START))&(pd.to_datetime(z.origin_date).le(HOLDOUT_END))].copy();disc.to_csv(out/"discovery_pattern_truth.csv",index=False,encoding="utf-8-sig");hold.to_csv(out/"holdout_pattern_truth_FROZEN_NO_RETUNING.csv",index=False,encoding="utf-8-sig")
    design={"revision":REVISION,"discovery_end":"2026-08-28","holdout_start":"2026-09-01","blind_sample_n":BLIND_SAMPLE_N,"blind_bar_lookback":BAR_LOOKBACK,"manual_fidelity_labels":["TRUE_MATCH","PARTIAL_MATCH","MISLABEL","MULTI_PATTERN","UNKNOWN_PATTERN"],"semantic_contract_auto_check":"ONLY_REPOSITORY_GROUNDED_PARTIAL_CONTRACTS","outcomes_hidden_from_blind_review":True,"new_signature_status":"DISCOVERY_ONLY_NEEDS_FREEZE_THEN_OOS","research_only":True}
    meta={"research_id":RESEARCH_ID,"revision":REVISION,"status":"PASS","events":len(z),"discovery_events":len(disc),"holdout_events":len(hold),"blind_review_events":len(review),"blind_bar_rows":len(bars),"missing_price":len(missing),"novel_signature_candidates":len(cand),"research_only":True,"production_eligible":False,"selection_logic_changed":False,"score_rank_changed":False,"order_logic_changed":False,"same_sample_retuning":False,"blind_review_outcomes_exposed":False,"design_hash":sha_obj(design),"design":design}
    (out/"pattern_truth_meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    rep=[f"🧪 [{RESEARCH_ID}]",f"status=PASS revision={REVISION}",f"events={len(z)} discovery={len(disc)} holdout={len(hold)} missing_price={len(missing)}",f"blind_review={len(review)} blind_bar_rows={len(bars)}",f"novel_signature_candidates={len(cand)}","Auto truth is limited to repository-grounded partial semantic contracts; ambiguous labels require manual review.","Blind review contains signal-date-and-earlier evidence only; outcome key is separate.","Discovery signatures are exploratory only and cannot become a production gate without freeze + OOS validation.","research_only=1 production changes=0 same_sample_retuning=0"]
    (out/"pattern_truth_report.txt").write_text("\n".join(rep)+"\n",encoding="utf-8");print("\n".join(rep));return 0


def self_test() -> int:
    r=pd.Series({"structure_tags":"strong_wave1|shallow_pullback|ma_compression","origin_search_pattern":"첫눌림재양봉종베","wave1_ready":1,"pullback_days":4,"signal_ret_pct":2,"tag_reacceleration":1})
    assert "STRONG_WAVE1_SHALLOW_PULLBACK" in raw_family(r)
    dates=pd.bdate_range("2025-01-01",periods=260);c=np.linspace(90,110,len(dates));fr=pd.DataFrame({"date":dates,"open":c,"high":c*1.01,"low":c*.99,"close":c,"volume":1000.,"amount":c*1000});fr["ma5"]=fr.close.rolling(5).mean();fr["ma20"]=fr.close.rolling(20).mean();fr["ma40"]=fr.close.rolling(40).mean();fr["vwma40"]=fr.close.rolling(40).mean();fr["bb40_width"]=0.05
    e=contract_evidence(r,fr,dates[-1]);assert e["auto_pattern_truth_state"] in {"AUTO_EVIDENCE_SUPPORTS","AUTO_PARTIAL_SUPPORT_MANUAL_REVIEW"}
    c=semantic_contract();assert len(c)>=4 and "SEMANTIC_MAPPING_REQUIRED" in set(c.authority)
    print("REAL_FULL_PATTERN_TRUTH_UNKNOWN_R1_SELF_TEST PASS");return 0


def main() -> int:
    ap=argparse.ArgumentParser();ap.add_argument("--r121-master",default="source_r121/reports/real_full_original_thesis_forensic_r121/forensic_r12_event_master.csv");ap.add_argument("--price-cache-dir",default="reports/.cache/real_full_pattern_truth_unknown_r1/price_history");ap.add_argument("--output-dir",default="reports/real_full_pattern_truth_unknown_r1");ap.add_argument("--allow-price-refetch",action="store_true");ap.add_argument("--self-test",action="store_true");a=ap.parse_args();return self_test() if a.self_test else run(a)

if __name__=="__main__": raise SystemExit(main())
