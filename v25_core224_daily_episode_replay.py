from __future__ import annotations

"""V25.4.7 CORE224 daily episode replay (research-only, cache-first).

Purpose
-------
The 2-year ALL workflow samples signal dates weekly for runtime reasons.  That is ideal for
finding candidate CORE224 episodes, but a short-lived HEALTHY_PULLBACK -> RESTART transition
can happen between two weekly snapshots.  This module does **not** widen the CORE224 seed universe or tune a threshold. It takes only
names already seeded by the weekly CORE224 sidecar/base lens, replays their existing cached
daily price + verified trading-value history, and records any chronologically valid daily
RESTART transitions plus the same structural scale-in lifecycle used by V25.3.1. After the
RESTART set is frozen it may perform a narrowly targeted, D-1-only Historical-AsOf authority
reconstruction for those event dates. That authority lane is cache-first, bounded, fail-closed,
and never changes CORE224 membership or signal generation.

Daily replay is therefore an episode-resolution audit, not proof that the stock belonged to the
causal TOP500 on every recovered day.  Exact daily universe authority is reported separately and
never imputed from a neighboring weekly snapshot.
"""

import os
import time
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import original_thesis_reconstruction as thesis
import historical_asof_universe as hist_asof

VERSION = "V73.3.6.6.25.4.7"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🔬 [CORE224 주간 Seed → 일별 Episode 확대 Replay · RESEARCH_ONLY]"

SEED_FILE = "v73_v25_core224_daily_episode_seed.csv"
STATE_FILE = "v73_v25_core224_daily_state_ledger.csv"
TRANSITION_FILE = "v73_v25_core224_daily_transition_ledger.csv"
RESTART_FILE = "v73_v25_core224_daily_restart_ledger.csv"
INVARIANT_FILE = "v73_v25_core224_daily_invariant_audit.csv"
MANUAL_FILE = "v73_v25_core224_daily_manual_review.csv"
LIFECYCLE_SIGNAL_FILE = "v73_v25_core224_daily_lifecycle_signal_ledger.csv"
LIFECYCLE_POLICY_FILE = "v73_v25_core224_daily_lifecycle_policy_ledger.csv"
LIFECYCLE_FILL_FILE = "v73_v25_core224_daily_lifecycle_fill_ledger.csv"
LIFECYCLE_HORIZON_FILE = "v73_v25_core224_daily_lifecycle_horizon_ledger.csv"
LIFECYCLE_STOP_FILE = "v73_v25_core224_daily_lifecycle_stop_summary.csv"
READINESS_FILE = "v73_v25_core224_daily_episode_readiness.csv"
RAW_RESTART_FILE = "v73_v25_core224_daily_restart_raw.csv"
DEDUP_AUDIT_FILE = "v73_v25_core224_daily_restart_event_dedup.csv"
RECON_FILE = "v73_v25_core224_daily_restart_reconciliation.csv"
ORDER_FILE = "v73_v25_core224_daily_lifecycle_event_order.csv"
ORDER_SUMMARY_FILE = "v73_v25_core224_daily_lifecycle_event_order_summary.csv"
SINGLE_POLICY_FILE = "v73_v25_core224_daily_single_entry_policy_ledger.csv"
RISK_PARITY_FILE = "v73_v25_core224_daily_single_vs_scale_risk_parity.csv"
RISK_PARITY_SUMMARY_FILE = "v73_v25_core224_daily_single_vs_scale_risk_parity_summary.csv"
EPISODE_OVERLAP_FILE = "v73_v25_core224_daily_episode_overlap_audit.csv"
EPISODE_FAMILY_FILE = "v73_v25_core224_daily_episode_family_summary.csv"
UNRESOLVED_ROOTCAUSE_FILE = "v73_v25_core224_weekly_daily_unresolved_rootcause.csv"
TARGET_AUTHORITY_FILE = "v73_v25_core224_targeted_asof_authority.csv"
TARGET_AUTHORITY_DATE_FILE = "v73_v25_core224_targeted_asof_date_audit.csv"
PATH_CLASS_FILE = "v73_v25_core224_daily_lifecycle_path_classification.csv"
PATH_CLASS_SUMMARY_FILE = "v73_v25_core224_daily_lifecycle_path_summary.csv"
STOP_LENS_PATH_COMPARE_FILE = "v73_v25_core224_daily_stop_lens_path_compare.csv"
RISK_PARITY_FILL_GROUP_FILE = "v73_v25_core224_daily_single_vs_scale_fill_group_summary.csv"
TARGET_PROGRESS_AUDIT_FILE = "v73_v25_core224_targeted_asof_progress_audit.csv"
TARGET_PROGRESS_CACHE_FILE = "v25_targeted_authority_progress.json"
EXIT_SHADOW_FILE = "v73_v25_core224_daily_exit_policy_shadow.csv"
EXIT_SHADOW_SUMMARY_FILE = "v73_v25_core224_daily_exit_policy_shadow_summary.csv"
REPORT_FILE = "v73_v25_core224_daily_episode_report.txt"
CONTEXT_PARITY_FILE = "v73_v25_core224_weekly_daily_context_parity_audit.csv"
INPUT_FINGERPRINT_FILE = "v73_v25_core224_input_fingerprint_regression.csv"
STOP_LENS_RISK_TRADEOFF_FILE = "v73_v25_core224_stop_lens_risk_tradeoff.csv"
INPUT_FINGERPRINT_CACHE_FILE = "v25_core224_input_fingerprint_last.json"
CONTEXT_STATE_TRACE_FILE = "v73_v25_core224_weekly_daily_state_trace.csv"
CONTEXT_STATE_TRACE_SUMMARY_FILE = "v73_v25_core224_weekly_daily_state_trace_summary.csv"
TARGET_AUTHORITY_CLASS_SUMMARY_FILE = "v73_v25_core224_targeted_asof_class_summary.csv"
STOP_EXIT_POLICY_MATRIX_FILE = "v73_v25_core224_stop_exit_policy_matrix.csv"
EXECUTION_CAUSALITY_FILE = "v73_v25_core224_execution_causality_shadow.csv"
EXECUTION_CAUSALITY_SUMMARY_FILE = "v73_v25_core224_execution_causality_summary.csv"
WEEKLY_900BAR_PARITY_FILE = "v73_v25_core224_weekly_daily_900bar_contract_parity.csv"
WEEKLY_SEED_AUTHORITY_FILE = "v73_v25_core224_weekly_seed_authority.csv"
DAILY_SEED_CAUSALITY_FILE = "v73_v25_core224_daily_seed_causality_audit.csv"
SHARD_RESTART_INPUT_PROOF_FILE = "v73_v25_core224_weekly_restart_exact_shard_input_proof.csv"
POLICY_LOCK_MANIFEST_FILE = "v73_v25_core224_policy_lock_manifest.csv"
POLICY_LOCK_CACHE_FILE = "v25_core224_policy_lock.json"
FORWARD_OOS_EVENT_FILE = "v73_v25_core224_forward_oos_event_ledger.csv"
FORWARD_OOS_POLICY_FILE = "v73_v25_core224_forward_oos_policy_ledger.csv"
FORWARD_OOS_SUMMARY_FILE = "v73_v25_core224_forward_oos_summary.csv"
FORWARD_OOS_IMMUTABILITY_FILE = "v73_v25_core224_forward_oos_immutability_audit.csv"
FINGERPRINT_SCHEMA_VERSION = "V25.4.7_CANONICAL_SEMANTIC_2"
POLICY_LOCK_SCHEMA_VERSION = "V25.4.7_POLICY_LOCK_2"
POLICY_LOCK_CUTOFF_DEFAULT = "2026-08-10"
POLICY_LOCK_CREATED_DEFAULT = "2026-08-25"

CORE_STATES = {
    "CORE224_BASE", "CORE224_ACCUMULATION", "CORE224_WAVE1",
    "CORE224_FIRST_PULLBACK", "CORE224_HEALTHY_PULLBACK", "CORE224_RESTART",
}


def _num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def _norm_code(v: Any) -> str:
    return thesis._norm_code(v)


def _fmt_date(v: Any) -> str:
    return thesis._fmt_date(v)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_csv(path, index=False, encoding="utf-8-sig")


def _cohort_bounds() -> Tuple[Dict[str, Any], pd.Timestamp, pd.Timestamp]:
    cohort = thesis.resolve_cohort_window()
    st = pd.to_datetime(cohort.get("requested_start"), errors="coerce")
    en = pd.to_datetime(cohort.get("requested_end"), errors="coerce")
    if pd.isna(st) or pd.isna(en):
        raise ValueError("DAILY_EPISODE_REPLAY_REQUIRES_EXPLICIT_COHORT_WINDOW")
    return cohort, pd.Timestamp(st).normalize(), pd.Timestamp(en).normalize()


def _build_seed_ledger(state: pd.DataFrame, st: pd.Timestamp, en: pd.Timestamp) -> pd.DataFrame:
    if state is None or state.empty:
        return pd.DataFrame()
    q = state.copy()
    q["signal_date"] = pd.to_datetime(q.get("signal_date"), errors="coerce").dt.normalize()
    q["code"] = q.get("code", pd.Series("", index=q.index)).map(_norm_code)
    q = q[q["signal_date"].between(st, en, inclusive="both") & q["code"].ne("")].copy()
    if q.empty:
        return pd.DataFrame()
    state_s = q.get("core224_state", pd.Series("", index=q.index)).fillna("").astype(str)
    state_seed = state_s.isin(CORE_STATES)
    structural_seed = (
        _num(q, "base_lens_structural", 0).eq(1)
        & (_num(q, "base_lens_strict224", 0).eq(1) | _num(q, "base_lens_near224", 0).eq(1))
        & _num(q, "actual_amount_history_ready20", 0).eq(1)
    )
    accum_seed = _num(q, "accum_ok", 0).eq(1) & _num(q, "actual_amount_history_ready20", 0).eq(1)
    q = q[state_seed | structural_seed | accum_seed].copy()
    if q.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for code, g in q.groupby("code", sort=True):
        states = sorted({x for x in g.get("core224_state", pd.Series(dtype=str)).fillna("").astype(str) if x in CORE_STATES})
        reasons: List[str] = []
        if states: reasons.append("WEEKLY_CORE_STATE")
        if (_num(g, "base_lens_structural", 0).eq(1) & (_num(g, "base_lens_strict224", 0).eq(1) | _num(g, "base_lens_near224", 0).eq(1))).any():
            reasons.append("WEEKLY_STRUCTURAL_BASE_LENS")
        if _num(g, "accum_ok", 0).eq(1).any(): reasons.append("WEEKLY_ACCUM_LENS")
        name = str(g.get("name", pd.Series("", index=g.index)).dropna().astype(str).iloc[-1]) if "name" in g.columns and g["name"].notna().any() else ""
        market = str(g.get("market", pd.Series("", index=g.index)).dropna().astype(str).iloc[-1]) if "market" in g.columns and g["market"].notna().any() else ""
        rows.append({
            "version": VERSION, "code": code, "name": name, "market": market,
            "seed_reason": "+".join(reasons), "weekly_seed_rows": len(g),
            "weekly_non_none_rows": int(g.get("core224_state", pd.Series("", index=g.index)).fillna("").astype(str).isin(CORE_STATES).sum()),
            "weekly_states_seen": "|".join(states),
            "first_seed_date": _fmt_date(g["signal_date"].min()), "last_seed_date": _fmt_date(g["signal_date"].max()),
            "weekly_restart_rows": int(g.get("core224_state", pd.Series("", index=g.index)).fillna("").astype(str).eq("CORE224_RESTART").sum()),
            "research_only": True,
        })
    return pd.DataFrame(rows).sort_values(["first_seed_date", "code"], kind="stable")


def _weekly_seed_qual_mask(q: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """Return row-level weekly watch-list eligibility and a causal reason string.

    The policy lane may only inspect a Daily RESTART after the latest available weekly snapshot
    has already placed that code on the watch list.  This closes the retrospective-seed lookahead
    that is acceptable for structure discovery but not for policy training/OOS.
    """
    if q is None or q.empty:
        return pd.Series(dtype=bool), pd.Series(dtype=str)
    state_s = q.get("core224_state", pd.Series("", index=q.index)).fillna("").astype(str)
    state_seed = state_s.isin(CORE_STATES)
    structural_seed = (
        _num(q, "base_lens_structural", 0).eq(1)
        & (_num(q, "base_lens_strict224", 0).eq(1) | _num(q, "base_lens_near224", 0).eq(1))
        & _num(q, "actual_amount_history_ready20", 0).eq(1)
    )
    accum_seed = _num(q, "accum_ok", 0).eq(1) & _num(q, "actual_amount_history_ready20", 0).eq(1)
    qual = state_seed | structural_seed | accum_seed
    reasons=[]
    for i in q.index:
        rr=[]
        if bool(state_seed.loc[i]): rr.append("WEEKLY_CORE_STATE")
        if bool(structural_seed.loc[i]): rr.append("WEEKLY_STRUCTURAL_BASE_LENS")
        if bool(accum_seed.loc[i]): rr.append("WEEKLY_ACCUM_LENS")
        reasons.append("+".join(rr))
    return qual.astype(bool), pd.Series(reasons,index=q.index,dtype=str)


def _weekly_seed_authority(state: pd.DataFrame, st: pd.Timestamp, en: pd.Timestamp) -> pd.DataFrame:
    """Materialize the dated watch-list contract, never a retrospective code-only seed."""
    if state is None or state.empty:
        return pd.DataFrame()
    q=state.copy()
    q["signal_date"]=pd.to_datetime(q.get("signal_date"),errors="coerce").dt.normalize()
    q["code"]=q.get("code",pd.Series("",index=q.index)).map(_norm_code)
    q=q[q["signal_date"].between(st,en,inclusive="both") & q["code"].ne("")].copy()
    if q.empty: return pd.DataFrame()
    qual, reason=_weekly_seed_qual_mask(q); q["weekly_seed_qualified"]=qual.astype(int); q["weekly_seed_reason"]=reason
    snaps=sorted(pd.Timestamp(x).normalize() for x in q["signal_date"].dropna().unique())
    next_map={d:(snaps[i+1] if i+1<len(snaps) else en+pd.Timedelta(days=1)) for i,d in enumerate(snaps)}
    z=q[q["weekly_seed_qualified"].eq(1)].copy()
    if z.empty: return pd.DataFrame()
    z["watch_start_date"]=z["signal_date"]
    z["watch_end_exclusive"]=z["signal_date"].map(next_map)
    keep=[c for c in ["signal_date","code","name","market","core224_state","weekly_seed_reason","watch_start_date","watch_end_exclusive"] if c in z.columns]
    z=z[keep].copy()
    z.insert(0,"version",VERSION); z["research_only"]=True
    for c in ["signal_date","watch_start_date","watch_end_exclusive"]:
        if c in z.columns: z[c]=pd.to_datetime(z[c],errors="coerce").dt.strftime("%Y-%m-%d")
    return z.sort_values(["signal_date","code"],kind="stable").drop_duplicates(["signal_date","code"],keep="last")


def _daily_seed_causality_audit(restarts: pd.DataFrame, state: pd.DataFrame, st: pd.Timestamp, en: pd.Timestamp) -> pd.DataFrame:
    """Classify each frozen Daily RESTART using only the latest weekly snapshot known by then."""
    if restarts is None or restarts.empty:
        return pd.DataFrame()
    sq=state.copy() if isinstance(state,pd.DataFrame) else pd.DataFrame()
    if sq.empty:
        return pd.DataFrame([{"version":VERSION,"event_id":r.get("event_id",""),"code":_norm_code(r.get("code","")),"restart_date":_fmt_date(r.get("restart_date")),"weekly_seed_causal_eligible":0,"seed_causality_status":"NO_WEEKLY_STATE_AUTHORITY","research_only":True} for _,r in restarts.iterrows()])
    sq["signal_date"]=pd.to_datetime(sq.get("signal_date"),errors="coerce").dt.normalize(); sq["code"]=sq.get("code",pd.Series("",index=sq.index)).map(_norm_code)
    sq=sq[sq["signal_date"].between(st,en,inclusive="both") & sq["code"].ne("")].copy()
    qual, reason=_weekly_seed_qual_mask(sq); sq["weekly_seed_qualified"]=qual.astype(int); sq["weekly_seed_reason"]=reason
    sq=sq.sort_values(["signal_date","code"],kind="stable").drop_duplicates(["signal_date","code"],keep="last")
    snapshots=sorted(pd.Timestamp(x).normalize() for x in sq["signal_date"].dropna().unique())
    by_key={(pd.Timestamp(r["signal_date"]).normalize(),str(r["code"])):r.to_dict() for _,r in sq.iterrows() if pd.notna(r.get("signal_date"))}
    rows=[]
    for _,rr in restarts.iterrows():
        rec=rr.to_dict(); code=_norm_code(rec.get("code","")); d=pd.to_datetime(rec.get("restart_date",rec.get("date")),errors="coerce")
        prior=[x for x in snapshots if pd.notna(d) and x<=pd.Timestamp(d).normalize()]
        latest=prior[-1] if prior else None
        wr=by_key.get((latest,code),{}) if latest is not None else {}
        elig=int(bool(wr) and int(float(wr.get("weekly_seed_qualified",0) or 0))==1)
        if latest is None: status="NO_PRIOR_WEEKLY_SNAPSHOT"
        elif not wr: status="CODE_NOT_IN_LATEST_WEEKLY_SNAPSHOT"
        elif elig: status="CAUSAL_WEEKLY_SEED_ACTIVE"
        else: status="LATEST_WEEKLY_SNAPSHOT_NOT_SEEDED"
        next_snap=next((x for x in snapshots if latest is not None and x>latest),None)
        rows.append({
            "version":VERSION,"event_id":rec.get("event_id",rec.get("cycle_id","")),"cycle_id":rec.get("cycle_id",""),"code":code,"name":rec.get("name",""),
            "restart_date":_fmt_date(d),"latest_weekly_snapshot":_fmt_date(latest),"next_weekly_snapshot":_fmt_date(next_snap),
            "weekly_seed_causal_eligible":elig,"weekly_seed_reason":wr.get("weekly_seed_reason","") if wr else "",
            "weekly_state_at_authorization":wr.get("core224_state","") if wr else "","seed_causality_status":status,
            "causal_rule":"LATEST_WEEKLY_SNAPSHOT_AT_OR_BEFORE_RESTART_MUST_SEED_CODE","research_only":True,
        })
    return pd.DataFrame(rows)


def _weekly_restart_dates(state: pd.DataFrame, st: pd.Timestamp, en: pd.Timestamp) -> set[Tuple[str, str]]:
    if state is None or state.empty:
        return set()
    q = state.copy()
    q["signal_date"] = pd.to_datetime(q.get("signal_date"), errors="coerce").dt.normalize()
    q["code"] = q.get("code", pd.Series("", index=q.index)).map(_norm_code)
    q = q[q["signal_date"].between(st, en, inclusive="both")]
    out: set[Tuple[str, str]] = set()
    if "restart_date" in q.columns:
        rd = pd.to_datetime(q["restart_date"], errors="coerce").dt.normalize()
        for code, d in zip(q["code"], rd):
            if code and pd.notna(d) and st <= d <= en:
                out.add((code, pd.Timestamp(d).strftime("%Y-%m-%d")))
    # Defensive fallback when an old row did not serialize restart_date.
    z = q[q.get("core224_state", pd.Series("", index=q.index)).fillna("").astype(str).eq("CORE224_RESTART")]
    for _, r in z.iterrows():
        d = pd.to_datetime(r.get("signal_date"), errors="coerce")
        if pd.notna(d): out.add((_norm_code(r.get("code")), pd.Timestamp(d).strftime("%Y-%m-%d")))
    return out


def _exact_authority_map(out: Path) -> Dict[str, Dict[str, Any]]:
    p = out / thesis.UNIVERSE_RECON_FILE
    if not p.exists(): return {}
    try:
        q = pd.read_csv(p)
    except Exception:
        return {}
    if q.empty or "signal_date" not in q.columns: return {}
    q["signal_date"] = pd.to_datetime(q["signal_date"], errors="coerce").dt.normalize()
    ans: Dict[str, Dict[str, Any]] = {}
    for _, r in q[q["signal_date"].notna()].iterrows():
        ds = pd.Timestamp(r["signal_date"]).strftime("%Y-%m-%d")
        ans[ds] = {
            "complete": int(float(r.get("complete", 0) or 0)),
            "fallback_used": int(float(r.get("fallback_used", 0) or 0)),
            "status": str(r.get("status", "UNKNOWN")),
        }
    return ans


def _merge_amount_authority(out: Path, code: str, global_panel: pd.DataFrame) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    if isinstance(global_panel, pd.DataFrame) and not global_panel.empty:
        z = global_panel[global_panel["code"].eq(code)].copy()
        if not z.empty: parts.append(z)
    try:
        t = thesis._read_ticker_amount_cache(out, code)
        if isinstance(t, pd.DataFrame) and not t.empty:
            parts.append(t[[c for c in ["date", "code", "actual_amount", "actual_volume_snapshot"] if c in t.columns]].copy())
    except Exception:
        pass
    if not parts:
        return pd.DataFrame(columns=["date", "code", "actual_amount", "actual_volume_snapshot"])
    q = pd.concat(parts, ignore_index=True, sort=False)
    q["date"] = pd.to_datetime(q["date"], errors="coerce").dt.normalize()
    q["code"] = q.get("code", pd.Series(code, index=q.index)).map(_norm_code)
    q["actual_amount"] = pd.to_numeric(q.get("actual_amount"), errors="coerce")
    return q[q["date"].notna() & q["actual_amount"].notna()].drop_duplicates(["date", "code"], keep="last").sort_values("date", kind="stable")


def _decorate_event(df: pd.DataFrame, seed: Dict[str, Any]) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    q = df.copy()
    q.insert(0, "version", VERSION)
    q.insert(1, "code", seed.get("code", ""))
    q.insert(2, "name", seed.get("name", ""))
    q["seed_reason"] = seed.get("seed_reason", "")
    q["research_only"] = True
    return q


def _daily_lifecycle_for_restart(
    out: Path, restart: Dict[str, Any], px: pd.DataFrame, cohort: Dict[str, Any],
    cfg: thesis.Core224LifecycleConfig,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    code = _norm_code(restart.get("code", "")); sig_date = pd.to_datetime(restart.get("date"), errors="coerce")
    if pd.isna(sig_date): return {}, [], [], []
    sig_date = pd.Timestamp(sig_date).normalize()
    l0 = float(pd.to_numeric(pd.Series([restart.get("l0_low")]), errors="coerce").iloc[0])
    h1 = float(pd.to_numeric(pd.Series([restart.get("h1_high")]), errors="coerce").iloc[0])
    pb_low = thesis._low_between(px, restart.get("pullback_date"), sig_date)
    owner = thesis._signal_cohort_meta(sig_date, cohort)
    sig = {
        "version": VERSION, "cohort_id": owner.get("cohort_id", cohort.get("cohort_id", "")),
        "cohort_start": owner.get("requested_start", cohort.get("requested_start", "")),
        "cohort_end": owner.get("requested_end", cohort.get("requested_end", "")),
        "aggregate_cohort_id": cohort.get("cohort_id", ""),
        "aggregate_cohort_start": cohort.get("requested_start", ""), "aggregate_cohort_end": cohort.get("requested_end", ""),
        "code": code, "name": restart.get("name", ""), "signal_date": sig_date.strftime("%Y-%m-%d"),
        "source_state": "CORE224_RESTART", "l0_date": restart.get("l0_date", ""), "l0_low": l0,
        "h1_date": restart.get("h1_date", ""), "h1_high": h1, "pullback_date": restart.get("pullback_date", ""),
        "healthy_date": restart.get("healthy_date", ""), "restart_date": sig_date.strftime("%Y-%m-%d"),
        "pullback_low": float(pb_low) if pb_low is not None else np.nan,
        "daily_episode_replay": 1, "research_only": True,
    }
    lenses = thesis._stop_lenses(l0, h1, float(pb_low) if pb_low is not None else np.nan, cfg)
    sig["stop_lens_count"] = len(lenses)
    sig["lifecycle_eligible"] = int(bool(lenses) and not px.empty and (px.index.normalize() == sig_date).any())
    policy: List[Dict[str, Any]] = []; fills: List[Dict[str, Any]] = []; horizons: List[Dict[str, Any]] = []
    if sig["lifecycle_eligible"]:
        for lens_name, stop_price in lenses.items():
            rec, ff, hh = thesis._simulate_lifecycle_one(dict(sig), px, lens_name, stop_price, cfg)
            rec["version"] = VERSION; rec["daily_episode_replay"] = 1
            for x in ff: x["version"] = VERSION; x["daily_episode_replay"] = 1
            for x in hh: x["version"] = VERSION; x["daily_episode_replay"] = 1
            policy.append(rec); fills.extend(ff); horizons.extend(hh)
    return sig, policy, fills, horizons



def _cycle_key(rec: Dict[str, Any]) -> str:
    """Stable CORE224 episode identity from chronology anchors, never from returns."""
    code = _norm_code(rec.get("code", ""))
    anchors = [
        _fmt_date(rec.get("l0_date")), _fmt_date(rec.get("accum_date")),
        _fmt_date(rec.get("h1_date")), _fmt_date(rec.get("pullback_date")),
    ]
    if not any(anchors):
        anchors = [_fmt_date(rec.get("restart_date") or rec.get("date"))]
    return "|".join([code] + anchors)


def _cycle_id(rec: Dict[str, Any]) -> str:
    return hashlib.sha256(_cycle_key(rec).encode("utf-8")).hexdigest()[:20]


def _weekly_restart_ledger(state: pd.DataFrame, st: pd.Timestamp, en: pd.Timestamp) -> pd.DataFrame:
    if state is None or state.empty:
        return pd.DataFrame()
    q = state.copy()
    q["signal_date"] = pd.to_datetime(q.get("signal_date"), errors="coerce").dt.normalize()
    q["code"] = q.get("code", pd.Series("", index=q.index)).map(_norm_code)
    q = q[q["signal_date"].between(st, en, inclusive="both") & q["code"].ne("")].copy()
    q = q[q.get("core224_state", pd.Series("", index=q.index)).fillna("").astype(str).eq("CORE224_RESTART")]
    rows: List[Dict[str, Any]] = []
    for _, r in q.iterrows():
        rec = r.to_dict()
        restart_date = _fmt_date(rec.get("restart_date") or rec.get("signal_date"))
        row = {
            "version": VERSION, "code": _norm_code(rec.get("code")), "name": str(rec.get("name", "") or ""),
            "weekly_observation_date": _fmt_date(rec.get("signal_date")), "weekly_restart_date": restart_date,
            "l0_date": _fmt_date(rec.get("l0_date")), "accum_date": _fmt_date(rec.get("accum_date")),
            "h1_date": _fmt_date(rec.get("h1_date")), "pullback_date": _fmt_date(rec.get("pullback_date")),
            "healthy_date": _fmt_date(rec.get("healthy_date")), "research_only": True,
        }
        row["cycle_key"] = _cycle_key(row); row["cycle_id"] = _cycle_id(row)
        rows.append(row)
    z = pd.DataFrame(rows)
    if z.empty: return z
    return z.sort_values(["weekly_restart_date", "code"], kind="stable").drop_duplicates(["cycle_id"], keep="first")


def _dedupe_restart_events(raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if raw is None or raw.empty:
        return pd.DataFrame(), pd.DataFrame()
    q = raw.copy()
    q["restart_date"] = pd.to_datetime(q.get("restart_date"), errors="coerce").dt.strftime("%Y-%m-%d")
    q["cycle_key"] = q.apply(lambda r: _cycle_key(r.to_dict()), axis=1)
    q["cycle_id"] = q.apply(lambda r: _cycle_id(r.to_dict()), axis=1)
    q = q.sort_values(["restart_date", "code"], kind="stable")
    audit_rows: List[Dict[str, Any]] = []
    keep_idx: List[Any] = []
    for cid, g in q.groupby("cycle_id", sort=False, dropna=False):
        gs = g.sort_values(["restart_date", "code"], kind="stable")
        keep_idx.append(gs.index[0])
        dates = [str(x) for x in gs["restart_date"].tolist()]
        audit_rows.append({
            "version": VERSION, "cycle_id": cid, "cycle_key": str(gs.iloc[0].get("cycle_key", "")),
            "code": str(gs.iloc[0].get("code", "")), "name": str(gs.iloc[0].get("name", "")),
            "raw_restart_rows": len(gs), "unique_first_restart_rows": 1,
            "first_restart_date": dates[0] if dates else "", "all_restart_dates": "|".join(dates),
            "suppressed_repeat_rows": max(0, len(gs) - 1), "research_only": True,
        })
    unique = q.loc[keep_idx].copy().sort_values(["restart_date", "code"], kind="stable")
    unique["event_id"] = unique["cycle_id"]
    unique["event_is_first_restart"] = 1
    return unique.reset_index(drop=True), pd.DataFrame(audit_rows)


def _reconcile_weekly_daily(weekly: pd.DataFrame, daily_unique: pd.DataFrame) -> pd.DataFrame:
    """Explain weekly↔daily differences without pretending a shifted date is an exact match."""
    rows: List[Dict[str, Any]] = []
    if weekly is None: weekly = pd.DataFrame()
    if daily_unique is None: daily_unique = pd.DataFrame()
    used_daily: set[str] = set()
    for _, wr in weekly.iterrows():
        w = wr.to_dict(); code = str(w.get("code", "")); wdate = str(w.get("weekly_restart_date", "")); cid = str(w.get("cycle_id", ""))
        exact = daily_unique[(daily_unique.get("code", pd.Series(dtype=str)).astype(str) == code) & (daily_unique.get("restart_date", pd.Series(dtype=str)).astype(str) == wdate)] if not daily_unique.empty else pd.DataFrame()
        cyc = daily_unique[daily_unique.get("cycle_id", pd.Series(dtype=str)).astype(str).eq(cid)] if not daily_unique.empty and cid else pd.DataFrame()
        if not exact.empty:
            dr = exact.iloc[0].to_dict(); status = "EXACT_DATE_MATCH"
        elif not cyc.empty:
            dr = cyc.sort_values("restart_date", kind="stable").iloc[0].to_dict(); status = "SAME_CYCLE_DATE_SHIFT"
        else:
            dr = {}; status = "WEEKLY_ONLY_UNRECONCILED"
        did = str(dr.get("event_id", dr.get("cycle_id", "")))
        if did: used_daily.add(did)
        dd = pd.to_datetime(dr.get("restart_date"), errors="coerce"); wd = pd.to_datetime(wdate, errors="coerce")
        rows.append({
            "version": VERSION, "reconciliation_status": status, "code": code, "name": str(w.get("name", "")),
            "weekly_cycle_id": cid, "daily_cycle_id": str(dr.get("cycle_id", "")),
            "weekly_restart_date": wdate, "daily_restart_date": str(dr.get("restart_date", "")),
            "calendar_day_shift": int((dd - wd).days) if pd.notna(dd) and pd.notna(wd) else np.nan,
            "weekly_l0_date": w.get("l0_date", ""), "daily_l0_date": dr.get("l0_date", ""),
            "weekly_h1_date": w.get("h1_date", ""), "daily_h1_date": dr.get("h1_date", ""),
            "weekly_pullback_date": w.get("pullback_date", ""), "daily_pullback_date": dr.get("pullback_date", ""),
            "research_only": True,
        })
    if not daily_unique.empty:
        for _, dr in daily_unique.iterrows():
            d = dr.to_dict(); did = str(d.get("event_id", d.get("cycle_id", "")))
            if did and did in used_daily: continue
            rows.append({
                "version": VERSION, "reconciliation_status": "DAILY_ONLY_RECOVERED", "code": str(d.get("code", "")), "name": str(d.get("name", "")),
                "weekly_cycle_id": "", "daily_cycle_id": str(d.get("cycle_id", "")),
                "weekly_restart_date": "", "daily_restart_date": str(d.get("restart_date", "")),
                "calendar_day_shift": np.nan, "weekly_l0_date": "", "daily_l0_date": d.get("l0_date", ""),
                "weekly_h1_date": "", "daily_h1_date": d.get("h1_date", ""),
                "weekly_pullback_date": "", "daily_pullback_date": d.get("pullback_date", ""),
                "research_only": True,
            })
    return pd.DataFrame(rows)


def _event_relation(event_day: Any, stop_day: Any, censored: bool) -> str:
    e = pd.to_numeric(pd.Series([event_day]), errors="coerce").iloc[0]
    s = pd.to_numeric(pd.Series([stop_day]), errors="coerce").iloc[0]
    if pd.notna(e):
        if pd.notna(s):
            return "EVENT_BEFORE_STOP" if float(e) < float(s) else "STOP_FIRST_SAME_OR_EARLIER"
        return "EVENT_BEFORE_OBSERVATION_END"
    if pd.notna(s): return "STOP_BEFORE_EVENT"
    return "RIGHT_CENSORED_EVENT_UNKNOWN" if censored else "NOT_REACHED_WITHIN_60D"


def _build_event_order(policy_df: pd.DataFrame, out: Path) -> pd.DataFrame:
    if policy_df is None or policy_df.empty: return pd.DataFrame()
    px_cache: Dict[str, pd.DataFrame] = {}
    rows: List[Dict[str, Any]] = []
    for _, r in policy_df.iterrows():
        rec = r.to_dict(); code = _norm_code(rec.get("code", "")); status = str(rec.get("lifecycle_status", ""))
        stop_day = rec.get("end_day") if status == "STRUCTURE_STOP" else np.nan
        censored = status == "OPEN_RIGHT_CENSORED"
        row = {
            "version": VERSION, "event_id": rec.get("event_id", rec.get("cycle_id", "")), "cycle_id": rec.get("cycle_id", ""),
            "code": code, "name": rec.get("name", ""), "signal_date": rec.get("signal_date", ""), "stop_lens": rec.get("stop_lens", ""),
            "lifecycle_status": status, "stop_day": stop_day, "stop_date": rec.get("end_date", "") if status == "STRUCTURE_STOP" else "",
            "daily_universe_authority": rec.get("daily_universe_authority", ""),
            "daily_universe_membership_proven": int(float(rec.get("daily_universe_membership_proven", 0) or 0)),
            "same_day_order_policy": "STOP_FIRST_CONSERVATIVE_DAILY_OHLC", "research_only": True,
        }
        for key, fld in [
            ("avg_recovery", "avg_recovery_day"), ("h1_high_rebreak", "h1_rebreak_high_day"),
            ("h1_close_rebreak", "h1_rebreak_close_day"), ("profit3_high", "profit3_high_day"),
            ("profit5_high", "profit5_high_day"), ("profit10_high", "profit10_high_day"),
            ("profit3_close", "profit3_close_day"), ("profit5_close", "profit5_close_day"),
            ("profit10_close", "profit10_close_day"),
        ]:
            row[f"{key}_day"] = rec.get(fld, np.nan)
            row[f"{key}_vs_stop"] = _event_relation(rec.get(fld), stop_day, censored)
            row[f"{key}_before_stop"] = int(row[f"{key}_vs_stop"] in {"EVENT_BEFORE_STOP", "EVENT_BEFORE_OBSERVATION_END"})
            row[f"stop_before_{key}"] = int(row[f"{key}_vs_stop"] in {"STOP_BEFORE_EVENT", "STOP_FIRST_SAME_OR_EARLIER"})
        # Daily OHLC cannot order a stop-touch against an intraday target touch. Keep the strategy
        # conservative (stop first) but surface the collision rather than hiding it.
        for c in ["same_day_collision_h1_high", "same_day_collision_h1_close", "same_day_collision_profit3_high", "same_day_collision_profit5_high", "same_day_collision_profit10_high", "same_day_collision_avg_recovery_close"]:
            row[c] = 0
        if status == "STRUCTURE_STOP" and rec.get("end_date"):
            if code not in px_cache:
                pr, _ = thesis._read_price_cache_for_code(out, code); px_cache[code] = thesis._normalize_lifecycle_price(pr)
            px = px_cache.get(code, pd.DataFrame()); sd = pd.to_datetime(rec.get("end_date"), errors="coerce")
            if pd.notna(sd) and not px.empty:
                hit = px[px.index.normalize() == pd.Timestamp(sd).normalize()]
                if not hit.empty:
                    bar = hit.iloc[-1]; avg = float(pd.to_numeric(pd.Series([rec.get("avg_cost_final")]), errors="coerce").iloc[0]); h1 = float(pd.to_numeric(pd.Series([rec.get("h1_high")]), errors="coerce").iloc[0])
                    if np.isfinite(h1):
                        row["same_day_collision_h1_high"] = int(float(bar["high"]) >= h1)
                        row["same_day_collision_h1_close"] = int(float(bar["close"]) >= h1)
                    if np.isfinite(avg) and avg > 0:
                        row["same_day_collision_profit3_high"] = int(float(bar["high"]) >= avg * 1.03)
                        row["same_day_collision_profit5_high"] = int(float(bar["high"]) >= avg * 1.05)
                        row["same_day_collision_profit10_high"] = int(float(bar["high"]) >= avg * 1.10)
                        ever_below_v = pd.to_numeric(pd.Series([rec.get("ever_below_avg_close", 0)]), errors="coerce").fillna(0).iloc[0]
                        row["same_day_collision_avg_recovery_close"] = int(int(ever_below_v) == 1 and float(bar["close"]) >= avg)
        row["any_same_day_collision"] = int(any(int(row.get(c, 0) or 0) for c in row if c.startswith("same_day_collision_")))
        rows.append(row)
    return pd.DataFrame(rows)


def _order_summary(order_df: pd.DataFrame) -> pd.DataFrame:
    if order_df is None or order_df.empty: return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    scopes = [("ALL_RESEARCH", order_df), ("EXACT_CAUSAL_ASOF", order_df[_num(order_df, "daily_universe_membership_proven", 0).eq(1)])]
    for scope, base in scopes:
        if base.empty: continue
        for lens, g in base.groupby("stop_lens", dropna=False):
            rows.append({
                "version": VERSION, "scope": scope, "stop_lens": lens, "events": len(g),
                "structure_stop_rate_pct": float(g["lifecycle_status"].astype(str).eq("STRUCTURE_STOP").mean() * 100.0),
                "avg_recovery_before_stop_pct": float(_num(g, "avg_recovery_before_stop", 0).eq(1).mean() * 100.0),
                "h1_close_before_stop_pct": float(_num(g, "h1_close_rebreak_before_stop", 0).eq(1).mean() * 100.0),
                "profit3_high_before_stop_pct": float(_num(g, "profit3_high_before_stop", 0).eq(1).mean() * 100.0),
                "profit5_high_before_stop_pct": float(_num(g, "profit5_high_before_stop", 0).eq(1).mean() * 100.0),
                "profit10_high_before_stop_pct": float(_num(g, "profit10_high_before_stop", 0).eq(1).mean() * 100.0),
                "stop_before_recovery_pct": float(_num(g, "stop_before_avg_recovery", 0).eq(1).mean() * 100.0),
                "stop_before_h1_close_pct": float(_num(g, "stop_before_h1_close_rebreak", 0).eq(1).mean() * 100.0),
                "stop_before_profit5_pct": float(_num(g, "stop_before_profit5_high", 0).eq(1).mean() * 100.0),
                "same_day_collision_pct": float(_num(g, "any_same_day_collision", 0).eq(1).mean() * 100.0),
                "research_only": True,
            })
    return pd.DataFrame(rows)


def _simulate_single_entry_one(restart: Dict[str, Any], px: pd.DataFrame, stop_name: str, stop_price: float, cfg: thesis.Core224LifecycleConfig) -> Dict[str, Any]:
    sig_date = pd.to_datetime(restart.get("restart_date") or restart.get("date"), errors="coerce")
    code = _norm_code(restart.get("code", "")); event_id = str(restart.get("event_id", restart.get("cycle_id", "")))
    base = {"version": VERSION, "event_id": event_id, "cycle_id": restart.get("cycle_id", ""), "code": code, "name": restart.get("name", ""), "stop_lens": stop_name,
            "weekly_seed_causal_eligible": int(float(restart.get("weekly_seed_causal_eligible", 0) or 0)),
            "policy_training_eligible": int(float(restart.get("policy_training_eligible", 0) or 0)), "research_only": True}
    if pd.isna(sig_date) or px is None or px.empty:
        return {**base, "single_status": "NO_PRICE_FOLLOWUP"}
    sig_date = pd.Timestamp(sig_date).normalize(); start_idx = thesis._first_bar_index_on_or_after(px, sig_date)
    if start_idx is None or px.index[start_idx].normalize() != sig_date:
        return {**base, "signal_date": sig_date.strftime("%Y-%m-%d"), "single_status": "SIGNAL_DATE_PRICE_MISSING"}
    entry = float(px.iloc[start_idx]["close"]); h1 = float(pd.to_numeric(pd.Series([restart.get("h1_high")]), errors="coerce").iloc[0])
    risk_frac = (entry - float(stop_price)) / entry if entry > 0 and np.isfinite(stop_price) else np.nan
    risk_valid = int(np.isfinite(risk_frac) and risk_frac > 0)
    max_idx = min(len(px) - 1, start_idx + cfg.max_follow_days); available = max_idx - start_idx
    stopped = False; stop_day = np.nan; stop_date = ""; final_price = float(px.iloc[max_idx]["close"])
    ever_below = False; recovery_day = np.nan; h1_close_day = np.nan; h1_high_day = np.nan
    p3 = p5 = p10 = np.nan; mfe = -np.inf; mae = np.inf
    for idx in range(start_idx, max_idx + 1):
        day = idx - start_idx; bar = px.iloc[idx]
        if idx > start_idx and float(bar["low"]) <= float(stop_price):
            stopped = True; stop_day = day; stop_date = _fmt_date(px.index[idx]); final_price = float(bar["open"]) if float(bar["open"]) < float(stop_price) else float(stop_price); break
        high_ret = float(bar["high"]) / entry - 1.0; low_ret = float(bar["low"]) / entry - 1.0; close_ret = float(bar["close"]) / entry - 1.0
        mfe = max(mfe, high_ret); mae = min(mae, low_ret)
        if close_ret < 0: ever_below = True
        if day > 0 and ever_below and pd.isna(recovery_day) and close_ret >= 0: recovery_day = day
        if pd.isna(h1_high_day) and np.isfinite(h1) and float(bar["high"]) >= h1: h1_high_day = day
        if pd.isna(h1_close_day) and np.isfinite(h1) and float(bar["close"]) >= h1: h1_close_day = day
        if pd.isna(p3) and high_ret >= 0.03: p3 = day
        if pd.isna(p5) and high_ret >= 0.05: p5 = day
        if pd.isna(p10) and high_ret >= 0.10: p10 = day
    pnl_frac = final_price / entry - 1.0 if entry > 0 else np.nan
    r_mult = pnl_frac / risk_frac if risk_valid else np.nan
    return {
        **base, "signal_date": sig_date.strftime("%Y-%m-%d"), "entry_price": entry, "stop_price": float(stop_price),
        "single_status": "STRUCTURE_STOP" if stopped else ("SURVIVED_60D_OBSERVATION_END" if available >= cfg.max_follow_days else "OPEN_RIGHT_CENSORED"),
        "stop_day": stop_day, "stop_date": stop_date, "available_follow_days": available, "final_price": final_price,
        "final_capital_pnl_pct": pnl_frac * 100.0 if np.isfinite(pnl_frac) else np.nan,
        "planned_risk_pct": risk_frac * 100.0 if np.isfinite(risk_frac) else np.nan, "risk_valid": risk_valid,
        "final_r_multiple": r_mult, "mfe_pct": mfe * 100.0 if np.isfinite(mfe) else np.nan, "mae_pct": mae * 100.0 if np.isfinite(mae) else np.nan,
        "avg_recovery_day": recovery_day, "h1_high_rebreak_day": h1_high_day, "h1_close_rebreak_day": h1_close_day,
        "profit3_high_day": p3, "profit5_high_day": p5, "profit10_high_day": p10,
        "daily_universe_authority": restart.get("daily_universe_authority", ""),
        "daily_universe_membership_proven": int(float(restart.get("daily_universe_membership_proven", 0) or 0)),
    }


def _risk_parity(scale_policy: pd.DataFrame, fills: pd.DataFrame, single: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if scale_policy is None or scale_policy.empty or single is None or single.empty: return pd.DataFrame(), pd.DataFrame()
    fill_risk: Dict[Tuple[str, str], float] = {}
    if fills is not None and not fills.empty:
        for (eid, lens), g in fills.groupby(["event_id", "stop_lens"], dropna=False):
            stop_vals = scale_policy[(scale_policy["event_id"].astype(str) == str(eid)) & (scale_policy["stop_lens"].astype(str) == str(lens))].get("stop_price", pd.Series(dtype=float))
            stop = float(pd.to_numeric(stop_vals, errors="coerce").dropna().iloc[0]) if pd.to_numeric(stop_vals, errors="coerce").dropna().size else np.nan
            risk = 0.0
            for _, f in g.iterrows():
                price = float(pd.to_numeric(pd.Series([f.get("fill_price")]), errors="coerce").iloc[0]); w = float(pd.to_numeric(pd.Series([f.get("planned_weight")]), errors="coerce").fillna(0).iloc[0])
                if np.isfinite(price) and price > 0 and np.isfinite(stop): risk += w * max((price - stop) / price, 0.0)
            fill_risk[(str(eid), str(lens))] = risk
    sidx = single.set_index([single["event_id"].astype(str), single["stop_lens"].astype(str)], drop=False)
    rows: List[Dict[str, Any]] = []
    for _, r in scale_policy.iterrows():
        eid = str(r.get("event_id", "")); lens = str(r.get("stop_lens", "")); key = (eid, lens)
        if key not in sidx.index: continue
        sr = sidx.loc[key]
        if isinstance(sr, pd.DataFrame): sr = sr.iloc[0]
        scale_risk = float(fill_risk.get(key, np.nan)); scale_pnl = float(pd.to_numeric(pd.Series([r.get("final_planned_capital_pnl_pct")]), errors="coerce").iloc[0]) / 100.0
        scale_r = scale_pnl / scale_risk if np.isfinite(scale_risk) and scale_risk > 0 else np.nan
        single_r = float(pd.to_numeric(pd.Series([sr.get("final_r_multiple")]), errors="coerce").iloc[0])
        entry1 = float(pd.to_numeric(pd.Series([r.get("entry1_price")]), errors="coerce").iloc[0]); avg = float(pd.to_numeric(pd.Series([r.get("avg_cost_final")]), errors="coerce").iloc[0])
        rows.append({
            "version": VERSION, "event_id": eid, "cycle_id": r.get("cycle_id", ""), "code": r.get("code", ""), "name": r.get("name", ""), "signal_date": r.get("signal_date", ""), "stop_lens": lens,
            "daily_universe_authority": r.get("daily_universe_authority", ""), "daily_universe_membership_proven": int(float(r.get("daily_universe_membership_proven", 0) or 0)),
            "weekly_seed_causal_eligible": int(float(r.get("weekly_seed_causal_eligible", 0) or 0)),
            "policy_training_eligible": int(float(r.get("policy_training_eligible", 0) or 0)),
            "single_status": sr.get("single_status", ""), "scale_status": r.get("lifecycle_status", ""),
            "single_planned_risk_pct": sr.get("planned_risk_pct", np.nan), "scale_planned_risk_pct": scale_risk * 100.0 if np.isfinite(scale_risk) else np.nan,
            "single_final_r_multiple": single_r, "scale_final_r_multiple": scale_r,
            "scale_minus_single_r": scale_r - single_r if np.isfinite(scale_r) and np.isfinite(single_r) else np.nan,
            "entry_count": r.get("entry_count", np.nan), "deployed_weight": r.get("deployed_weight", np.nan),
            "single_holding_days": (sr.get("stop_day", np.nan) if str(sr.get("single_status", "")) == "STRUCTURE_STOP" else sr.get("available_follow_days", np.nan)),
            "scale_holding_days": r.get("end_day", np.nan),
            "single_mae_pct": sr.get("mae_pct", np.nan), "single_mfe_pct": sr.get("mfe_pct", np.nan),
            "scale_mae_pct": r.get("mae_pct", np.nan), "scale_mfe_pct": r.get("mfe_pct", np.nan),
            "avg_cost_improvement_vs_entry1_pct": (1.0 - avg / entry1) * 100.0 if np.isfinite(avg) and np.isfinite(entry1) and entry1 > 0 else np.nan,
            "single_capital_per_1R": (100.0 / float(pd.to_numeric(pd.Series([sr.get("planned_risk_pct")]), errors="coerce").iloc[0])) if pd.notna(pd.to_numeric(pd.Series([sr.get("planned_risk_pct")]), errors="coerce").iloc[0]) and float(pd.to_numeric(pd.Series([sr.get("planned_risk_pct")]), errors="coerce").iloc[0]) > 0 else np.nan,
            "scale_capital_per_1R": 1.0 / scale_risk if np.isfinite(scale_risk) and scale_risk > 0 else np.nan,
            "risk_parity_note": "R_MULTIPLE_ONLY_NOT_POSITION_SIZING_RECOMMENDATION", "research_only": True,
        })
    df = pd.DataFrame(rows)
    sums: List[Dict[str, Any]] = []
    if not df.empty:
        scopes = [
            ("ALL_RESEARCH", df),
            ("EXACT_CAUSAL_ASOF", df[_num(df, "daily_universe_membership_proven", 0).eq(1)]),
            ("POLICY_TRAINING_CAUSAL", df[_num(df, "policy_training_eligible", 0).eq(1)]),
        ]
        for scope, base in scopes:
            if base.empty: continue
            for lens, g in base.groupby("stop_lens", dropna=False):
                edge = pd.to_numeric(g["scale_minus_single_r"], errors="coerce").dropna()
                sums.append({
                    "version": VERSION, "scope": scope, "stop_lens": lens, "events": len(g),
                    "median_single_r": float(pd.to_numeric(g["single_final_r_multiple"], errors="coerce").median()),
                    "median_scale_r": float(pd.to_numeric(g["scale_final_r_multiple"], errors="coerce").median()),
                    "median_scale_minus_single_r": float(edge.median()) if len(edge) else np.nan,
                    "scale_r_better_rate_pct": float((edge > 0).mean() * 100.0) if len(edge) else np.nan,
                    "median_avg_cost_improvement_pct": float(pd.to_numeric(g["avg_cost_improvement_vs_entry1_pct"], errors="coerce").median()),
                    "median_deployed_weight": float(pd.to_numeric(g["deployed_weight"], errors="coerce").median()),
                    "research_only": True,
                })
    return df, pd.DataFrame(sums)



def _trimmed_mean(values: pd.Series, frac: float = 0.10) -> float:
    q = pd.to_numeric(values, errors="coerce").dropna().sort_values(kind="stable")
    if q.empty:
        return np.nan
    k = int(len(q) * max(0.0, min(0.45, frac)))
    if k > 0 and len(q) > 2 * k:
        q = q.iloc[k:-k]
    return float(q.mean()) if len(q) else np.nan


def _risk_parity_fill_group_summary(risk_df: pd.DataFrame) -> pd.DataFrame:
    """Compare SINGLE vs 30/30/40 only where add opportunities actually occurred."""
    if risk_df is None or risk_df.empty:
        return pd.DataFrame()
    q = risk_df.copy()
    ec = pd.to_numeric(q.get("entry_count"), errors="coerce").fillna(0)
    q["fill_group"] = np.select(
        [ec.ge(3), ec.eq(2), ec.le(1)],
        ["ENTRY3_FILLED", "ENTRY2_FILLED", "ENTRY1_ONLY"],
        default="UNKNOWN",
    )
    rows: List[Dict[str, Any]] = []
    scopes = [("ALL_RESEARCH", q), ("EXACT_CAUSAL_ASOF", q[_num(q, "daily_universe_membership_proven", 0).eq(1)]),
              ("POLICY_TRAINING_CAUSAL", q[_num(q, "policy_training_eligible", 0).eq(1)])]
    for scope, base in scopes:
        if base.empty:
            continue
        for (lens, fg), g in base.groupby(["stop_lens", "fill_group"], dropna=False):
            s1 = pd.to_numeric(g.get("single_final_r_multiple"), errors="coerce")
            sc = pd.to_numeric(g.get("scale_final_r_multiple"), errors="coerce")
            ed = pd.to_numeric(g.get("scale_minus_single_r"), errors="coerce")
            rows.append({
                "version": VERSION, "scope": scope, "stop_lens": lens, "fill_group": fg,
                "events": len(g),
                "mean_single_r": float(s1.mean()) if s1.notna().any() else np.nan,
                "median_single_r": float(s1.median()) if s1.notna().any() else np.nan,
                "trim10_single_r": _trimmed_mean(s1),
                "mean_scale_r": float(sc.mean()) if sc.notna().any() else np.nan,
                "median_scale_r": float(sc.median()) if sc.notna().any() else np.nan,
                "trim10_scale_r": _trimmed_mean(sc),
                "mean_scale_minus_single_r": float(ed.mean()) if ed.notna().any() else np.nan,
                "median_scale_minus_single_r": float(ed.median()) if ed.notna().any() else np.nan,
                "trim10_scale_minus_single_r": _trimmed_mean(ed),
                "scale_r_better_rate_pct": float((ed.dropna() > 0).mean() * 100.0) if ed.notna().any() else np.nan,
                "median_avg_cost_improvement_pct": float(pd.to_numeric(g.get("avg_cost_improvement_vs_entry1_pct"), errors="coerce").median()),
                "median_deployed_weight": float(pd.to_numeric(g.get("deployed_weight"), errors="coerce").median()),
                "median_single_holding_days": float(pd.to_numeric(g.get("single_holding_days"), errors="coerce").median()),
                "median_scale_holding_days": float(pd.to_numeric(g.get("scale_holding_days"), errors="coerce").median()),
                "research_only": True,
            })
    return pd.DataFrame(rows)


def _stop_lens_path_compare(path_summary: pd.DataFrame) -> pd.DataFrame:
    if path_summary is None or path_summary.empty:
        return pd.DataFrame()
    classes = ["CLEAN_WIN", "PROFIT_THEN_BREAK", "EARLY_STOP_RECOVERY", "TRUE_FAILURE", "CAPITAL_LOCK", "RIGHT_CENSORED"]
    rows: List[Dict[str, Any]] = []
    for (scope, lens), g in path_summary.groupby(["scope", "stop_lens"], dropna=False):
        rec: Dict[str, Any] = {"version": VERSION, "scope": scope, "stop_lens": lens, "research_only": True}
        rec["total_events"] = int(pd.to_numeric(g.get("total_events"), errors="coerce").max()) if len(g) else 0
        for cls in classes:
            z = g[g.get("path_class", pd.Series(dtype=str)).astype(str).eq(cls)]
            rec[f"{cls.lower()}_pct"] = float(pd.to_numeric(z.get("pct"), errors="coerce").iloc[0]) if not z.empty else 0.0
            rec[f"{cls.lower()}_events"] = int(pd.to_numeric(z.get("events"), errors="coerce").iloc[0]) if not z.empty else 0
        rec["pre_stop_success_pct"] = rec["clean_win_pct"] + rec["profit_then_break_pct"]
        rec["premature_stop_recovery_pct"] = rec["early_stop_recovery_pct"]
        rec["true_failure_pct"] = rec["true_failure_pct"]
        rows.append(rec)
    order = {"PB_LOW":0, "FIB_61_8":1, "FIB_78_6":2, "L0_STRUCTURE":3, "HYBRID_TIGHTER":4}
    out = pd.DataFrame(rows)
    if not out.empty:
        out["_ord"] = out["stop_lens"].map(order).fillna(99)
        out = out.sort_values(["scope", "_ord", "stop_lens"], kind="stable").drop(columns=["_ord"])
    return out


def _simulate_exit_shadow_one(
    restart: Dict[str, Any], px: pd.DataFrame, stop_name: str, stop_price: float,
    cfg: thesis.Core224LifecycleConfig, exit_policy: str,
) -> Dict[str, Any]:
    """Fixed, return-blind exit-policy shadow using the same 30/30/40 add rules.

    Policies are deliberately few and locked:
      STRUCTURE_HOLD       - current lifecycle behavior, no profit-forced exit.
      PLUS5_FULL_EXIT      - full exit at +5% of the *current pre-close-fill* average cost.
      H1_CLOSE_PB_TRAIL    - legacy parity: after an H1 close rebreak, tighten to PB_LOW.
      WIDE_H1_PB_TRAIL     - meaningful wide-stop experiment: start from Fib/L0 structural stop,
                             then tighten to PB_LOW only after an H1 close rebreak.

    Daily-bar collisions are STOP_FIRST. A close-confirmed add cannot retroactively lower the
    target for a high that occurred earlier on the same bar.
    """
    code = _norm_code(restart.get("code", "")); name = str(restart.get("name", "") or "")
    sig = pd.to_datetime(restart.get("restart_date", restart.get("signal_date")), errors="coerce")
    base = {
        "version": VERSION, "event_id": restart.get("event_id", restart.get("cycle_id", "")),
        "cycle_id": restart.get("cycle_id", ""), "code": code, "name": name,
        "signal_date": _fmt_date(sig), "stop_lens": stop_name, "exit_policy": exit_policy,
        "daily_universe_membership_proven": int(float(restart.get("daily_universe_membership_proven", 0) or 0)),
        "weekly_seed_causal_eligible": int(float(restart.get("weekly_seed_causal_eligible", 0) or 0)),
        "policy_training_eligible": int(float(restart.get("policy_training_eligible", 0) or 0)),
        "research_only": True,
    }
    if pd.isna(sig) or px is None or px.empty:
        return {**base, "status": "NO_PRICE_FOLLOWUP"}
    sig = pd.Timestamp(sig).normalize()
    px = thesis._normalize_lifecycle_price(px)
    start_idx = thesis._first_bar_index_on_or_after(px, sig)
    if start_idx is None or px.index[start_idx].normalize() != sig:
        return {**base, "status": "SIGNAL_DATE_PRICE_MISSING"}
    l0 = float(pd.to_numeric(pd.Series([restart.get("l0_low")]), errors="coerce").iloc[0])
    h1 = float(pd.to_numeric(pd.Series([restart.get("h1_high")]), errors="coerce").iloc[0])
    pb_low = thesis._low_between(px, restart.get("pullback_date"), sig)
    if pb_low is None or not np.isfinite(l0) or not np.isfinite(h1) or h1 <= l0:
        return {**base, "status": "STRUCTURE_INPUT_MISSING"}
    lenses = thesis._stop_lenses(l0, h1, float(pb_low), cfg)
    pb_stop = float(lenses.get("PB_LOW", stop_price))
    rng = h1 - l0; fib382 = h1 - 0.382 * rng; fib618 = h1 - 0.618 * rng
    support2 = fib382; support3 = max(float(pb_low), fib618)
    if support3 >= support2 * 0.99:
        support3 = np.nan
    weights = list(cfg.entry_weights); fills: List[Tuple[float,float,int]] = []
    shares = invested = deployed = 0.0
    def add(stage: int, idx: int) -> None:
        nonlocal shares, invested, deployed
        price=float(px.iloc[idx]["close"]); w=float(weights[stage-1]); shares += w/price; invested += w; deployed += w; fills.append((price,w,idx-start_idx))
    add(1,start_idx); last_fill_day=0; trail_active=False
    initial_risk=0.0
    # risk is finalized from actual fills at exit/end; all fills use the original structural stop.
    max_idx=min(len(px)-1,start_idx+cfg.max_follow_days)
    exit_idx=max_idx; exit_price=float(px.iloc[max_idx]["close"]); status="OBSERVATION_END"; reason="60D_MARK_OR_CENSORED"; collision=0
    for idx in range(start_idx+1,max_idx+1):
        day=idx-start_idx; bar=px.iloc[idx]
        avg_pre=invested/shares if shares>0 else np.nan
        trail_policy = exit_policy in {"H1_CLOSE_PB_TRAIL", "WIDE_H1_PB_TRAIL"}
        effective_stop=max(float(stop_price), pb_stop) if (trail_policy and trail_active) else float(stop_price)
        target5=(avg_pre*1.05) if np.isfinite(avg_pre) else np.nan
        stop_hit=float(bar["low"]) <= effective_stop
        target_hit=exit_policy=="PLUS5_FULL_EXIT" and np.isfinite(target5) and float(bar["high"]) >= target5
        if stop_hit and target_hit:
            collision=1
        if stop_hit:
            exit_idx=idx; exit_price=float(bar["open"]) if float(bar["open"]) < effective_stop else effective_stop
            status="STRUCTURE_STOP"; reason="PB_TRAIL_STOP" if trail_active and effective_stop==pb_stop and pb_stop>float(stop_price) else "ORIGINAL_STRUCTURE_STOP"; break
        if target_hit:
            exit_idx=idx; exit_price=float(target5); status="PLUS5_FULL_EXIT"; reason="PLUS5_TARGET_BEFORE_CLOSE_FILL"; break
        prev_close=float(px.iloc[idx-1]["close"]); confirmation=(float(bar["close"])>float(bar["open"])) or (float(bar["close"])>=prev_close)
        if confirmation and float(bar["close"])>effective_stop:
            if len(fills)==1 and np.isfinite(support2) and float(bar["low"]) <= support2*(1.0+cfg.support_touch_tolerance):
                add(2,idx); last_fill_day=day
            elif len(fills)==2 and np.isfinite(support3) and day-last_fill_day>=cfg.min_gap_between_adds_days and float(bar["low"]) <= support3*(1.0+cfg.support_touch_tolerance):
                add(3,idx); last_fill_day=day
        if exit_policy in {"H1_CLOSE_PB_TRAIL", "WIDE_H1_PB_TRAIL"} and (not trail_active) and np.isfinite(h1) and float(bar["close"])>=h1:
            trail_active=True
    avg=invested/shares if shares>0 else np.nan
    pnl=shares*exit_price-invested if shares>0 else np.nan
    for price,w,_day in fills:
        if np.isfinite(price) and price>0:
            initial_risk += w*max((price-float(stop_price))/price,0.0)
    r_mult=(pnl/initial_risk) if np.isfinite(pnl) and initial_risk>0 else np.nan
    return {
        **base, "status": status, "exit_reason": reason, "exit_date": _fmt_date(px.index[exit_idx]),
        "holding_days": int(exit_idx-start_idx), "exit_price": exit_price, "entry_count": len(fills),
        "deployed_weight": deployed, "avg_cost_final": avg,
        "avg_cost_improvement_vs_entry1_pct": (1.0-avg/float(px.iloc[start_idx]["close"]))*100.0 if np.isfinite(avg) else np.nan,
        "planned_initial_risk_pct": initial_risk*100.0, "final_planned_capital_pnl_pct": pnl*100.0 if np.isfinite(pnl) else np.nan,
        "final_r_multiple": r_mult, "trail_activated": int(trail_active), "same_day_stop_plus5_collision": collision,
        "time_forced_exit": 0, "observation_end_only": int(status=="OBSERVATION_END"),
    }


def _exit_policy_shadow(
    restart_df: pd.DataFrame, px_by_code: Dict[str, pd.DataFrame], cfg: thesis.Core224LifecycleConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if restart_df is None or restart_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    base_policies = ["STRUCTURE_HOLD", "PLUS5_FULL_EXIT", "H1_CLOSE_PB_TRAIL"]
    wide_lenses = {"FIB_61_8", "FIB_78_6", "L0_STRUCTURE"}
    rows: List[Dict[str, Any]] = []
    for _, rr in restart_df.iterrows():
        rec=rr.to_dict(); code=_norm_code(rec.get("code", "")); px=px_by_code.get(code, pd.DataFrame())
        pb=thesis._low_between(px, rec.get("pullback_date"), rec.get("restart_date")) if isinstance(px,pd.DataFrame) and not px.empty else None
        l0=float(pd.to_numeric(pd.Series([rec.get("l0_low")]),errors="coerce").iloc[0]); h1=float(pd.to_numeric(pd.Series([rec.get("h1_high")]),errors="coerce").iloc[0])
        if pb is None: continue
        lenses=thesis._stop_lenses(l0,h1,float(pb),cfg)
        for lens, stop in lenses.items():
            policies = list(base_policies)
            if str(lens) in wide_lenses:
                policies.append("WIDE_H1_PB_TRAIL")
            for policy in policies:
                rows.append(_simulate_exit_shadow_one(rec,px,lens,float(stop),cfg,policy))
    df=pd.DataFrame(rows); sums:List[Dict[str,Any]]=[]
    if not df.empty:
        scopes=[
            ("ALL_RESEARCH",df),
            ("EXACT_CAUSAL_ASOF",df[_num(df,"daily_universe_membership_proven",0).eq(1)]),
            ("POLICY_TRAINING_CAUSAL",df[_num(df,"policy_training_eligible",0).eq(1)]),
        ]
        for scope,base in scopes:
            if base.empty: continue
            for (lens,policy),g in base.groupby(["stop_lens","exit_policy"],dropna=False):
                r=pd.to_numeric(g.get("final_r_multiple"),errors="coerce")
                sums.append({
                    "version":VERSION,"scope":scope,"stop_lens":lens,"exit_policy":policy,"events":len(g),
                    "mean_r":float(r.mean()) if r.notna().any() else np.nan,"median_r":float(r.median()) if r.notna().any() else np.nan,"trim10_r":_trimmed_mean(r),
                    "positive_r_rate_pct":float((r.dropna()>0).mean()*100.0) if r.notna().any() else np.nan,
                    "structure_stop_rate_pct":float(g.get("status",pd.Series(dtype=str)).astype(str).eq("STRUCTURE_STOP").mean()*100.0),
                    "plus5_exit_rate_pct":float(g.get("status",pd.Series(dtype=str)).astype(str).eq("PLUS5_FULL_EXIT").mean()*100.0),
                    "pb_trail_stop_rate_pct":float(g.get("exit_reason",pd.Series(dtype=str)).astype(str).eq("PB_TRAIL_STOP").mean()*100.0),
                    "observation_end_rate_pct":float(g.get("status",pd.Series(dtype=str)).astype(str).eq("OBSERVATION_END").mean()*100.0),
                    "median_holding_days":float(pd.to_numeric(g.get("holding_days"),errors="coerce").median()),
                    "median_entry_count":float(pd.to_numeric(g.get("entry_count"),errors="coerce").median()),
                    "median_avg_cost_improvement_pct":float(pd.to_numeric(g.get("avg_cost_improvement_vs_entry1_pct"),errors="coerce").median()),
                    "same_day_stop_plus5_collision_rate_pct":float(_num(g,"same_day_stop_plus5_collision",0).mean()*100.0),
                    "research_only":True,
                })
    return df,pd.DataFrame(sums)



def _target_authority_class_summary(targeted_events: pd.DataFrame) -> pd.DataFrame:
    if targeted_events is None or targeted_events.empty:
        return pd.DataFrame()
    q = targeted_events.copy()
    cls = q.get("targeted_authority_class", pd.Series("AUTHORITY_MISSING", index=q.index)).fillna("AUTHORITY_MISSING").astype(str)
    q["targeted_authority_class"] = cls
    def tier(v: str) -> str:
        if v == "EXACT_CAUSAL_PRIOR_WEEKLY_PROOF": return "PRIOR_WEEKLY_EXACT_PROOF"
        if v.endswith("_CONSERVATIVE"): return "CONSERVATIVE_POSITIVE_PROOF"
        if v in {"EXACT_CAUSAL_TOP500", "EXACT_CAUSAL_EVENT_EXPANSION"}: return "FULL_NAME_EXACT_PROOF"
        if v == "NOT_IN_CAUSAL_UNIVERSE": return "PROVEN_NEGATIVE"
        return "UNRESOLVED_AUTHORITY"
    q["authority_proof_tier"] = q["targeted_authority_class"].map(tier)
    rows: List[Dict[str, Any]] = []
    total = max(1, len(q))
    for (pt, ac), g in q.groupby(["authority_proof_tier", "targeted_authority_class"], dropna=False):
        rows.append({
            "version": VERSION, "authority_proof_tier": pt, "targeted_authority_class": ac,
            "events": len(g), "pct_of_all_events": len(g) / total * 100.0,
            "membership_proven_events": int(_num(g, "targeted_membership_proven", 0).eq(1).sum()),
            "research_only": True,
        })
    return pd.DataFrame(rows).sort_values(["authority_proof_tier", "targeted_authority_class"], kind="stable")


def _stop_exit_policy_matrix(exit_summary: pd.DataFrame) -> pd.DataFrame:
    """Locked 5-stop x 3-exit matrix for ALL and exact-causal scopes."""
    if exit_summary is None or exit_summary.empty:
        return pd.DataFrame()
    stop_order = ["PB_LOW", "FIB_61_8", "FIB_78_6", "L0_STRUCTURE", "HYBRID_TIGHTER"]
    rows: List[Dict[str, Any]] = []
    for scope in ["ALL_RESEARCH", "EXACT_CAUSAL_ASOF", "POLICY_TRAINING_CAUSAL"]:
        base = exit_summary[exit_summary.get("scope", pd.Series(dtype=str)).astype(str).eq(scope)]
        if base.empty: continue
        for lens in stop_order:
            z = base[base.get("stop_lens", pd.Series(dtype=str)).astype(str).eq(lens)]
            if z.empty: continue
            selections = [
                ("STRUCTURE_HOLD", "STRUCTURE_HOLD", 0),
                ("PLUS5_FULL_EXIT", "PLUS5_FULL_EXIT", 0),
                ("H1_TO_PB_TRAIL", "WIDE_H1_PB_TRAIL" if lens in {"FIB_61_8", "FIB_78_6", "L0_STRUCTURE"} else "H1_CLOSE_PB_TRAIL", int(lens in {"PB_LOW", "HYBRID_TIGHTER"})),
            ]
            for normalized, source_policy, degenerate in selections:
                x = z[z.get("exit_policy", pd.Series(dtype=str)).astype(str).eq(source_policy)]
                if x.empty: continue
                r = x.iloc[0].to_dict()
                rows.append({
                    "version": VERSION, "scope": scope, "stop_lens": lens,
                    "normalized_exit_policy": normalized, "source_exit_policy": source_policy,
                    "trail_degenerate_same_as_initial_stop": degenerate,
                    "events": r.get("events", 0), "mean_r": r.get("mean_r", np.nan),
                    "median_r": r.get("median_r", np.nan), "trim10_r": r.get("trim10_r", np.nan),
                    "positive_r_rate_pct": r.get("positive_r_rate_pct", np.nan),
                    "structure_stop_rate_pct": r.get("structure_stop_rate_pct", np.nan),
                    "plus5_exit_rate_pct": r.get("plus5_exit_rate_pct", np.nan),
                    "pb_trail_stop_rate_pct": r.get("pb_trail_stop_rate_pct", np.nan),
                    "observation_end_rate_pct": r.get("observation_end_rate_pct", np.nan),
                    "median_holding_days": r.get("median_holding_days", np.nan),
                    "same_day_stop_plus5_collision_rate_pct": r.get("same_day_stop_plus5_collision_rate_pct", np.nan),
                    "research_only": True,
                })
    out = pd.DataFrame(rows)
    if not out.empty:
        smap = {x:i for i,x in enumerate(stop_order)}; pmap = {"STRUCTURE_HOLD":0,"PLUS5_FULL_EXIT":1,"H1_TO_PB_TRAIL":2}
        out["_s"] = out["stop_lens"].map(smap).fillna(99); out["_p"] = out["normalized_exit_policy"].map(pmap).fillna(99)
        out = out.sort_values(["scope","_s","_p"], kind="stable").drop(columns=["_s","_p"])
    return out


def _execution_cost_bps() -> float:
    # Fixed research assumption, not broker/tax authority. Override explicitly when desired.
    try: return max(0.0, float(os.getenv("V25_EXECUTION_ROUNDTRIP_COST_BPS", "20.0")))
    except Exception: return 20.0


def _simulate_execution_causality_one(
    restart: Dict[str, Any], px: pd.DataFrame, stop_name: str, stop_price: float,
    cfg: thesis.Core224LifecycleConfig, entry_mode: str, exit_policy: str, roundtrip_cost_bps: float,
) -> Dict[str, Any]:
    """Single-entry timing benchmark. D+1 open is the strict causal entry lane."""
    code = _norm_code(restart.get("code", "")); sig = pd.to_datetime(restart.get("restart_date") or restart.get("date"), errors="coerce")
    base = {
        "version": VERSION, "event_id": restart.get("event_id", restart.get("cycle_id", "")),
        "cycle_id": restart.get("cycle_id", ""), "code": code, "name": restart.get("name", ""),
        "signal_date": _fmt_date(sig), "entry_mode": entry_mode, "entry_style": "SINGLE",
        "stop_lens": stop_name, "exit_policy": exit_policy,
        "daily_universe_authority": restart.get("daily_universe_authority", ""),
        "daily_universe_membership_proven": int(float(restart.get("daily_universe_membership_proven", 0) or 0)),
        "weekly_seed_causal_eligible": int(float(restart.get("weekly_seed_causal_eligible", 0) or 0)),
        "policy_training_eligible": int(float(restart.get("policy_training_eligible", 0) or 0)),
        "targeted_authority_class": restart.get("targeted_authority_class", ""),
        "roundtrip_cost_bps_assumption": float(roundtrip_cost_bps),
        "cost_model": "FIXED_RESEARCH_ASSUMPTION_NOT_BROKER_OR_TAX_AUTHORITY",
        "research_only": True,
    }
    if pd.isna(sig) or px is None or px.empty:
        return {**base, "execution_status": "NO_PRICE_FOLLOWUP", "trade_executed": 0}
    sig = pd.Timestamp(sig).normalize(); p = thesis._normalize_lifecycle_price(px)
    sig_idx = thesis._first_bar_index_on_or_after(p, sig)
    if sig_idx is None or p.index[sig_idx].normalize() != sig:
        return {**base, "execution_status": "SIGNAL_DATE_PRICE_MISSING", "trade_executed": 0}
    if entry_mode == "D1_OPEN_CAUSAL":
        entry_idx = sig_idx + 1
        if entry_idx >= len(p):
            return {**base, "execution_status": "NO_D1_BAR_RIGHT_CENSORED", "trade_executed": 0}
        entry_price = float(p.iloc[entry_idx]["open"]); first_eval_idx = entry_idx
        if not np.isfinite(entry_price) or entry_price <= 0:
            return {**base, "execution_status": "D1_OPEN_INVALID", "trade_executed": 0}
        if entry_price <= float(stop_price):
            sc = float(p.iloc[sig_idx]["close"])
            return {
                **base, "entry_date": _fmt_date(p.index[entry_idx]), "entry_price": entry_price,
                "execution_status": "ENTRY_CANCEL_GAP_AT_OR_BELOW_STOP", "trade_executed": 0,
                "entry_cancelled_gap_below_stop": 1,
                "d1_open_gap_vs_signal_close_pct": (entry_price / sc - 1.0) * 100.0 if sc > 0 else np.nan,
            }
    else:
        entry_idx = sig_idx; entry_price = float(p.iloc[entry_idx]["close"]); first_eval_idx = entry_idx + 1
    if not np.isfinite(entry_price) or entry_price <= 0:
        return {**base, "execution_status": "ENTRY_PRICE_INVALID", "trade_executed": 0}

    h1 = pd.to_numeric(pd.Series([restart.get("h1_high")]), errors="coerce").iloc[0]
    pb = thesis._low_between(p, restart.get("pullback_date"), sig)
    pb_stop = float(pb) if pb is not None and np.isfinite(pb) else float(stop_price)
    initial_risk = (entry_price - float(stop_price)) / entry_price
    if not np.isfinite(initial_risk) or initial_risk <= 0:
        return {**base, "entry_date": _fmt_date(p.index[entry_idx]), "entry_price": entry_price, "execution_status": "INVALID_NONPOSITIVE_INITIAL_RISK", "trade_executed": 0}

    max_idx = min(len(p)-1, entry_idx + int(cfg.max_follow_days)); target5 = entry_price * 1.05
    trail_active = False; collision = 0; gap_through_stop = 0
    exit_idx = max_idx; exit_price = float(p.iloc[max_idx]["close"])
    execution_status = "OBSERVATION_END" if (max_idx-entry_idx) >= int(cfg.max_follow_days) else "OPEN_RIGHT_CENSORED"
    exit_reason = "60D_MARK" if execution_status == "OBSERVATION_END" else "INSUFFICIENT_RIGHT_FOLLOWUP"
    mfe = -np.inf; mae = np.inf
    for idx in range(first_eval_idx, max_idx + 1):
        bar = p.iloc[idx]
        effective_stop = max(float(stop_price), pb_stop) if (exit_policy == "H1_TO_PB_TRAIL" and trail_active) else float(stop_price)
        stop_hit = float(bar["low"]) <= effective_stop
        target_hit = exit_policy == "PLUS5_FULL_EXIT" and float(bar["high"]) >= target5
        if stop_hit and target_hit: collision = 1
        if stop_hit:
            op = float(bar["open"]); gap_through_stop = int(op < effective_stop)
            exit_price = op if gap_through_stop else effective_stop; exit_idx = idx
            execution_status = "STRUCTURE_STOP"; exit_reason = "GAP_THROUGH_STOP_AT_OPEN" if gap_through_stop else ("PB_TRAIL_STOP" if trail_active and effective_stop > float(stop_price) else "ORIGINAL_STRUCTURE_STOP")
            break
        if target_hit:
            exit_price = target5; exit_idx = idx; execution_status = "PLUS5_FULL_EXIT"; exit_reason = "PLUS5_TARGET"; break
        hi = float(bar["high"]) / entry_price - 1.0; lo = float(bar["low"]) / entry_price - 1.0
        mfe = max(mfe, hi); mae = min(mae, lo)
        if exit_policy == "H1_TO_PB_TRAIL" and (not trail_active) and pd.notna(h1) and float(bar["close"]) >= float(h1):
            trail_active = True

    gross_ret = exit_price / entry_price - 1.0; total_cost = float(roundtrip_cost_bps) / 10000.0
    buy_cost = total_cost / 2.0; sell_cost = total_cost / 2.0
    net_ret = (exit_price / entry_price) * (1.0 - sell_cost) - (1.0 + buy_cost)
    signal_close = float(p.iloc[sig_idx]["close"])
    return {
        **base, "entry_date": _fmt_date(p.index[entry_idx]), "entry_price": entry_price, "signal_close": signal_close,
        "d1_open_gap_vs_signal_close_pct": (entry_price / signal_close - 1.0) * 100.0 if entry_mode == "D1_OPEN_CAUSAL" and signal_close > 0 else 0.0,
        "stop_price": float(stop_price), "initial_risk_pct": initial_risk * 100.0,
        "trade_executed": 1, "entry_cancelled_gap_below_stop": 0,
        "execution_status": execution_status, "exit_reason": exit_reason,
        "exit_date": _fmt_date(p.index[exit_idx]), "exit_price": exit_price,
        "holding_days": int(exit_idx-entry_idx), "trail_activated": int(trail_active),
        "gap_through_stop": gap_through_stop, "same_day_stop_plus5_collision": collision,
        "gross_return_pct": gross_ret * 100.0, "net_return_pct_after_cost_assumption": net_ret * 100.0,
        "gross_r_multiple": gross_ret / initial_risk, "net_r_multiple": net_ret / initial_risk,
        "mfe_pct": mfe * 100.0 if np.isfinite(mfe) else np.nan, "mae_pct": mae * 100.0 if np.isfinite(mae) else np.nan,
    }


def _execution_causality_shadow(
    restart_df: pd.DataFrame, px_by_code: Dict[str, pd.DataFrame], cfg: thesis.Core224LifecycleConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if restart_df is None or restart_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    cost_bps = _execution_cost_bps(); rows: List[Dict[str, Any]] = []
    allowed_lenses = ["PB_LOW", "FIB_61_8", "FIB_78_6", "L0_STRUCTURE", "HYBRID_TIGHTER"]
    policies = ["STRUCTURE_HOLD", "PLUS5_FULL_EXIT", "H1_TO_PB_TRAIL"]
    entry_modes = ["RESTART_CLOSE_RESEARCH", "D1_OPEN_CAUSAL"]
    for _, rr in restart_df.iterrows():
        rec = rr.to_dict(); code = _norm_code(rec.get("code", "")); px = px_by_code.get(code, pd.DataFrame())
        if px is None or px.empty: continue
        pb = thesis._low_between(px, rec.get("pullback_date"), rec.get("restart_date"))
        l0 = pd.to_numeric(pd.Series([rec.get("l0_low")]), errors="coerce").iloc[0]; h1 = pd.to_numeric(pd.Series([rec.get("h1_high")]), errors="coerce").iloc[0]
        if pb is None or pd.isna(l0) or pd.isna(h1): continue
        lenses = thesis._stop_lenses(float(l0), float(h1), float(pb), cfg)
        for lens in allowed_lenses:
            if lens not in lenses: continue
            for em in entry_modes:
                for policy in policies:
                    rows.append(_simulate_execution_causality_one(rec, px, lens, float(lenses[lens]), cfg, em, policy, cost_bps))
    df = pd.DataFrame(rows); sums: List[Dict[str, Any]] = []
    if not df.empty:
        scopes = [("ALL_RESEARCH", df), ("EXACT_CAUSAL_ASOF", df[_num(df, "daily_universe_membership_proven", 0).eq(1)])]
        for scope, base in scopes:
            if base.empty: continue
            for (em, lens, policy), g0 in base.groupby(["entry_mode", "stop_lens", "exit_policy"], dropna=False):
                executed = g0[_num(g0, "trade_executed", 0).eq(1)].copy()
                nr = pd.to_numeric(executed.get("net_r_multiple"), errors="coerce") if not executed.empty else pd.Series(dtype=float)
                gr = pd.to_numeric(executed.get("gross_r_multiple"), errors="coerce") if not executed.empty else pd.Series(dtype=float)
                sums.append({
                    "version": VERSION, "scope": scope, "entry_mode": em, "entry_style": "SINGLE", "stop_lens": lens, "exit_policy": policy,
                    "signals": len(g0), "executed_trades": len(executed),
                    "entry_cancel_gap_below_stop_events": int(_num(g0, "entry_cancelled_gap_below_stop", 0).eq(1).sum()),
                    "entry_cancel_gap_below_stop_rate_pct": float(_num(g0, "entry_cancelled_gap_below_stop", 0).mean()*100.0),
                    "mean_gross_r": float(gr.mean()) if gr.notna().any() else np.nan,
                    "median_gross_r": float(gr.median()) if gr.notna().any() else np.nan,
                    "median_net_r": float(nr.median()) if nr.notna().any() else np.nan, "trim10_net_r": _trimmed_mean(nr),
                    "positive_net_r_rate_pct": float((nr.dropna()>0).mean()*100.0) if nr.notna().any() else np.nan,
                    "median_holding_days": float(pd.to_numeric(executed.get("holding_days"), errors="coerce").median()) if not executed.empty else np.nan,
                    "median_d1_open_gap_pct": float(pd.to_numeric(executed.get("d1_open_gap_vs_signal_close_pct"), errors="coerce").median()) if em == "D1_OPEN_CAUSAL" and not executed.empty else np.nan,
                    "gap_through_stop_rate_pct": float(_num(executed, "gap_through_stop", 0).mean()*100.0) if not executed.empty else np.nan,
                    "same_day_stop_plus5_collision_rate_pct": float(_num(executed, "same_day_stop_plus5_collision", 0).mean()*100.0) if not executed.empty else np.nan,
                    "roundtrip_cost_bps_assumption": cost_bps, "research_only": True,
                })
    return df, pd.DataFrame(sums)

def _stable_frame_hash(df: pd.DataFrame, cols: Optional[List[str]] = None) -> str:
    if df is None or df.empty:
        return hashlib.sha256(b"EMPTY").hexdigest()[:20]
    q = df.copy()
    use = [c for c in (cols or list(q.columns)) if c in q.columns]
    if not use:
        return hashlib.sha256(b"NO_COLUMNS").hexdigest()[:20]
    q = q[use].copy()
    for c in q.columns:
        if "date" in str(c).lower():
            z = pd.to_datetime(q[c], errors="coerce")
            q[c] = np.where(z.notna(), z.dt.strftime("%Y-%m-%d"), q[c].fillna("").astype(str))
        else:
            q[c] = q[c].fillna("").astype(str)
    q = q.sort_values(use, kind="stable").reset_index(drop=True)
    h = pd.util.hash_pandas_object(q, index=False).values.tobytes()
    return hashlib.sha256(h).hexdigest()[:20]


def _context_parity_audit(
    recon: pd.DataFrame, weekly: pd.DataFrame, state_df: pd.DataFrame,
    transition_df: pd.DataFrame, seeds: pd.DataFrame, out: Path,
) -> pd.DataFrame:
    """Explain sparse-weekly vs continuous-daily state context without inventing a daily RESTART."""
    if recon is None or recon.empty:
        return pd.DataFrame()
    unresolved = recon[recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("WEEKLY_ONLY_UNRECONCILED")]
    if unresolved.empty:
        return pd.DataFrame()
    sd = state_df.copy() if isinstance(state_df, pd.DataFrame) else pd.DataFrame()
    td = transition_df.copy() if isinstance(transition_df, pd.DataFrame) else pd.DataFrame()
    if not sd.empty:
        sd["date"] = pd.to_datetime(sd.get("date"), errors="coerce").dt.normalize(); sd["code"] = sd.get("code", pd.Series("", index=sd.index)).map(_norm_code)
    if not td.empty:
        td["date"] = pd.to_datetime(td.get("date"), errors="coerce").dt.normalize(); td["code"] = td.get("code", pd.Series("", index=td.index)).map(_norm_code)
    seed_codes = set(seeds.get("code", pd.Series(dtype=str)).astype(str)) if isinstance(seeds, pd.DataFrame) and not seeds.empty else set()
    rows: List[Dict[str, Any]] = []
    for _, rr in unresolved.iterrows():
        code = _norm_code(rr.get("code", "")); wd = pd.to_datetime(rr.get("weekly_restart_date"), errors="coerce")
        if pd.isna(wd):
            rows.append({"version": VERSION, "code": code, "resolution_class": "UNEXPLAINED_BAD_WEEKLY_DATE", "explained": 0, "research_only": True}); continue
        wd = pd.Timestamp(wd).normalize()
        same = sd[(sd["code"] == code) & (sd["date"] == wd)] if not sd.empty else pd.DataFrame()
        prior = td[(td["code"] == code) & (td["date"] <= wd)].sort_values("date", kind="stable") if not td.empty else pd.DataFrame()
        after = td[(td["code"] == code) & (td["date"] > wd)].sort_values("date", kind="stable") if not td.empty else pd.DataFrame()
        ctx = prior.iloc[-1].to_dict() if not prior.empty else {}
        anchors = [("weekly_l0_date","l0_date"),("weekly_h1_date","h1_date"),("weekly_pullback_date","pullback_date")]
        am = sum(int(bool(_fmt_date(rr.get(a))) and _fmt_date(rr.get(a)) == _fmt_date(ctx.get(b))) for a,b in anchors)
        same_state = "|".join(sorted(set(same.get("core224_state", pd.Series(dtype=str)).astype(str)))) if not same.empty else ""
        last_to = str(ctx.get("to_state", "")); last_date = _fmt_date(ctx.get("date")); next_to = str(after.iloc[0].get("to_state", "")) if not after.empty else ""; next_date = _fmt_date(after.iloc[0].get("date")) if not after.empty else ""
        px, _ = thesis._read_price_cache_for_code(out, code); pxn = thesis._normalize_lifecycle_price(px)
        cache_has = int(not pxn.empty and (pxn.index.normalize() == wd).any())
        if code not in seed_codes:
            cls = "UNEXPLAINED_WEEKLY_CODE_NOT_SEEDED"; explained = 0
        elif not cache_has:
            cls = "UNEXPLAINED_WEEKLY_DATE_PRICE_MISSING"; explained = 0
        elif am >= 2 and same_state and "CORE224_RESTART" not in same_state:
            cls = "EXPLAINED_WEEKLY_SPARSE_STATE_ARTIFACT"; explained = 1
        elif am >= 2 and last_to and last_to != "CORE224_RESTART":
            cls = "EXPLAINED_CONTINUOUS_DAILY_CONTEXT_ADVANCED_DIFFERENTLY"; explained = 1
        else:
            cls = "UNEXPLAINED_CONTEXT_DIVERGENCE"; explained = 0
        rows.append({
            "version": VERSION, "code": code, "name": rr.get("name", ""), "weekly_restart_date": _fmt_date(wd),
            "weekly_cycle_id": rr.get("weekly_cycle_id", ""), "seed_present": int(code in seed_codes), "price_cache_weekly_date_present": cache_has,
            "daily_state_at_weekly_date": same_state, "daily_last_transition_date": last_date, "daily_last_transition_to_state": last_to,
            "daily_next_transition_date": next_date, "daily_next_transition_to_state": next_to, "weekly_anchor_match_to_daily_context": am,
            "weekly_l0_date": rr.get("weekly_l0_date", ""), "weekly_h1_date": rr.get("weekly_h1_date", ""), "weekly_pullback_date": rr.get("weekly_pullback_date", ""),
            "daily_context_l0_date": _fmt_date(ctx.get("l0_date")), "daily_context_h1_date": _fmt_date(ctx.get("h1_date")), "daily_context_pullback_date": _fmt_date(ctx.get("pullback_date")),
            "resolution_class": cls, "explained": explained,
            "resolution_note": "EXPLAINED does not create a daily RESTART; it only explains why sparse-weekly and continuous-daily state machines disagree.",
            "research_only": True,
        })
    return pd.DataFrame(rows)



def _weekly_daily_full_state_trace(
    recon: pd.DataFrame, weekly_state: pd.DataFrame, seeds: pd.DataFrame, out: Path,
    global_amount: pd.DataFrame, st: pd.Timestamp, en: pd.Timestamp,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Row-level trace for unresolved weekly↔daily CORE224 state-machine divergences.

    This deliberately replays only the unresolved code(s), from the same cached OHLC/verified
    Amount inputs already used by daily episode replay.  It does not alter a signal.  The goal is
    to expose the *first shared weekly observation date* where state/anchors diverge, so a later
    human/code audit can fix the root cause instead of papering it over.
    """
    if recon is None or recon.empty:
        return pd.DataFrame(), pd.DataFrame()
    unresolved = recon[recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("WEEKLY_ONLY_UNRECONCILED")]
    if unresolved.empty:
        return pd.DataFrame(), pd.DataFrame()

    ws = weekly_state.copy() if isinstance(weekly_state, pd.DataFrame) else pd.DataFrame()
    if not ws.empty:
        ws["signal_date"] = pd.to_datetime(ws.get("signal_date"), errors="coerce").dt.normalize()
        ws["code"] = ws.get("code", pd.Series("", index=ws.index)).map(_norm_code)
    seed_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(seeds, pd.DataFrame) and not seeds.empty:
        for _, r in seeds.iterrows():
            seed_map[_norm_code(r.get("code", ""))] = r.to_dict()

    trace_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    anchor_cols = ["l0_date", "accum_date", "h1_date", "pullback_date", "healthy_date", "restart_date"]

    for _, rr in unresolved.iterrows():
        code = _norm_code(rr.get("code", "")); name = str(rr.get("name", "") or "")
        wd = pd.to_datetime(rr.get("weekly_restart_date"), errors="coerce")
        if not code or pd.isna(wd):
            summary_rows.append({
                "version": VERSION, "code": code, "name": name,
                "weekly_restart_date": _fmt_date(wd), "trace_status": "INVALID_UNRESOLVED_KEY",
                "first_divergence_date": "", "research_only": True,
            })
            continue
        wd = pd.Timestamp(wd).normalize()
        px_raw, _meta = thesis._read_price_cache_for_code(out, code)
        px_follow = thesis._normalize_lifecycle_price(px_raw)
        if px_raw is None or px_raw.empty or px_follow.empty:
            summary_rows.append({
                "version": VERSION, "code": code, "name": name,
                "weekly_restart_date": _fmt_date(wd), "trace_status": "PRICE_CACHE_MISSING",
                "first_divergence_date": "", "research_only": True,
            })
            continue
        amount_auth = _merge_amount_authority(out, code, global_amount)
        q = thesis._overlay_actual_amount(px_raw, code, amount_auth)
        q.index = pd.to_datetime(q.index, errors="coerce")
        q = q[q.index.notna()].sort_index(); q = q[q.index.normalize() <= en].copy()
        try:
            daily_full, daily_ev, _inv = thesis.evaluate_core224(q)
        except Exception as exc:
            summary_rows.append({
                "version": VERSION, "code": code, "name": name,
                "weekly_restart_date": _fmt_date(wd), "trace_status": f"EVAL_ERROR:{type(exc).__name__}",
                "first_divergence_date": "", "research_only": True,
            })
            continue
        if daily_full is None: daily_full = pd.DataFrame()
        if daily_ev is None: daily_ev = pd.DataFrame()
        if not daily_full.empty:
            daily_full = daily_full.copy(); daily_full["date"] = pd.to_datetime(daily_full.get("date"), errors="coerce").dt.normalize()
        if not daily_ev.empty:
            daily_ev = daily_ev.copy(); daily_ev["date"] = pd.to_datetime(daily_ev.get("date"), errors="coerce").dt.normalize()

        wcode = ws[ws["code"].eq(code)].copy() if not ws.empty else pd.DataFrame()
        # Trace begins before the earliest known weekly anchor so the exact first divergence can be seen.
        anchor_dates = []
        for c in ["weekly_l0_date", "weekly_h1_date", "weekly_pullback_date", "weekly_restart_date"]:
            d = pd.to_datetime(rr.get(c), errors="coerce")
            if pd.notna(d): anchor_dates.append(pd.Timestamp(d).normalize())
        trace_start = min(anchor_dates) - pd.Timedelta(days=35) if anchor_dates else wd - pd.Timedelta(days=140)
        trace_start = max(pd.Timestamp(st).normalize(), trace_start)
        trace_end = min(pd.Timestamp(en).normalize(), wd + pd.Timedelta(days=35))
        dsub = daily_full[daily_full["date"].between(trace_start, trace_end, inclusive="both")].copy() if not daily_full.empty else pd.DataFrame()
        esub = daily_ev[daily_ev["date"].between(trace_start, trace_end, inclusive="both")].copy() if not daily_ev.empty else pd.DataFrame()
        wsub = wcode[wcode["signal_date"].between(trace_start, trace_end, inclusive="both")].copy() if not wcode.empty else pd.DataFrame()

        dates: set[pd.Timestamp] = set()
        if not dsub.empty: dates.update(pd.Timestamp(x).normalize() for x in dsub["date"].dropna())
        if not wsub.empty: dates.update(pd.Timestamp(x).normalize() for x in wsub["signal_date"].dropna())
        if not dates: dates.add(wd)

        first_div: Optional[Dict[str, Any]] = None
        shared_obs_count = 0
        for dt in sorted(dates):
            drs = dsub[dsub["date"].eq(dt)] if not dsub.empty else pd.DataFrame()
            wrs = wsub[wsub["signal_date"].eq(dt)] if not wsub.empty else pd.DataFrame()
            ers = esub[esub["date"].eq(dt)] if not esub.empty else pd.DataFrame()
            dr = drs.iloc[-1].to_dict() if not drs.empty else {}
            wr = wrs.iloc[-1].to_dict() if not wrs.empty else {}
            is_weekly = int(not wrs.empty)
            if is_weekly: shared_obs_count += 1
            wstate = str(wr.get("core224_state", ""))
            dstate = str(dr.get("core224_state", ""))
            state_match = int(bool(is_weekly) and wstate == dstate)
            anchor_match = 0; anchor_present = 0
            for c in anchor_cols:
                wa, da = _fmt_date(wr.get(c)), _fmt_date(dr.get(c))
                if wa or da:
                    anchor_present += 1
                    anchor_match += int(wa == da and bool(wa))
            anchor_all_match = int(anchor_present == 0 or anchor_match == anchor_present)
            diverges = int(bool(is_weekly) and (not state_match or not anchor_all_match))
            trans_to = "|".join(sorted(set(ers.get("to_state", pd.Series(dtype=str)).astype(str))) if not ers.empty else [] )
            trans_from = "|".join(sorted(set(ers.get("from_state", pd.Series(dtype=str)).astype(str))) if not ers.empty else [] )
            rec = {
                "version": VERSION, "code": code, "name": name,
                "weekly_restart_date": _fmt_date(wd), "trace_date": _fmt_date(dt),
                "is_weekly_observation": is_weekly, "weekly_state": wstate, "daily_state": dstate,
                "weekly_daily_state_match": state_match if is_weekly else np.nan,
                "weekly_daily_anchor_match_count": anchor_match if is_weekly else np.nan,
                "weekly_daily_anchor_present_count": anchor_present if is_weekly else np.nan,
                "weekly_daily_anchor_all_match": anchor_all_match if is_weekly else np.nan,
                "diverges_on_shared_observation": diverges,
                "daily_transition_from": trans_from, "daily_transition_to": trans_to,
                "daily_actual_amount_history_ready20": dr.get("actual_amount_history_ready20", np.nan),
                "research_only": True,
            }
            for c in anchor_cols:
                rec[f"weekly_{c}"] = _fmt_date(wr.get(c)); rec[f"daily_{c}"] = _fmt_date(dr.get(c))
            trace_rows.append(rec)
            if diverges and first_div is None:
                first_div = rec.copy()

        if first_div is None:
            trace_status = "NO_SHARED_OBSERVATION_DIVERGENCE_FOUND"
            fd = {}
        else:
            trace_status = "FIRST_DIVERGENCE_EXPOSED"
            fd = first_div
        summary_rows.append({
            "version": VERSION, "code": code, "name": name, "weekly_restart_date": _fmt_date(wd),
            "trace_status": trace_status, "trace_start": _fmt_date(trace_start), "trace_end": _fmt_date(trace_end),
            "trace_rows": int(sum(1 for x in trace_rows if x.get("code") == code and x.get("weekly_restart_date") == _fmt_date(wd))),
            "shared_weekly_observations": shared_obs_count,
            "first_divergence_date": fd.get("trace_date", ""),
            "first_divergence_weekly_state": fd.get("weekly_state", ""),
            "first_divergence_daily_state": fd.get("daily_state", ""),
            "first_divergence_anchor_match_count": fd.get("weekly_daily_anchor_match_count", np.nan),
            "first_divergence_anchor_present_count": fd.get("weekly_daily_anchor_present_count", np.nan),
            "resolution_note": "TRACE_ONLY_DOES_NOT_CREATE_OR_DELETE_RESTART",
            "research_only": True,
        })
    return pd.DataFrame(trace_rows), pd.DataFrame(summary_rows)


def _weekly_900bar_contract_parity(
    recon: pd.DataFrame, weekly_state: pd.DataFrame, out: Path,
    global_amount: pd.DataFrame,
) -> pd.DataFrame:
    """Replay unresolved weekly rows using the *exact shard-side 900-bar input contract*.

    V25 shard sidecars call price_reader(days=900), truncate at the historical as-of date,
    then ``tail(900)`` before evaluate_core224().  The daily episode lane intentionally runs a
    continuous cached history, so a path-dependent state machine can disagree even though both
    are individually trailing-only.  This audit tests that precise contract for unresolved rows.
    A match explains the discrepancy; it never manufactures/deletes a daily RESTART.
    """
    if recon is None or recon.empty or weekly_state is None or weekly_state.empty:
        return pd.DataFrame()
    uq = recon[recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("WEEKLY_ONLY_UNRECONCILED")]
    if uq.empty:
        return pd.DataFrame()
    ws = weekly_state.copy()
    ws["signal_date"] = pd.to_datetime(ws.get("signal_date"), errors="coerce").dt.normalize()
    ws["code"] = ws.get("code", pd.Series("", index=ws.index)).map(_norm_code)
    anchors = ["l0_date","accum_date","h1_date","pullback_date","healthy_date","restart_date"]
    rows: List[Dict[str, Any]] = []
    for _, rr in uq.iterrows():
        code=_norm_code(rr.get("code","")); wd=pd.to_datetime(rr.get("weekly_restart_date"), errors="coerce")
        if not code or pd.isna(wd):
            continue
        wd=pd.Timestamp(wd).normalize()
        wrs=ws[ws["code"].eq(code) & ws["signal_date"].eq(wd)]
        wr=wrs.iloc[-1].to_dict() if not wrs.empty else {}
        px_raw, meta = thesis._read_price_cache_for_code(out, code)
        if px_raw is None or px_raw.empty:
            rows.append({"version":VERSION,"code":code,"weekly_restart_date":_fmt_date(wd),"contract_status":"PRICE_CACHE_MISSING","contract_explained":0,"research_only":True})
            continue
        # Match shard-side authority scope: build_date_sidecar loads at most 90 historical
        # all-market Amount snapshots as of the weekly date, then may reuse the ticker cache.
        weekly_amount_panel = thesis.load_cached_amount_panel(out, wd, [code], max_files=90)
        amount_auth=_merge_amount_authority(out, code, weekly_amount_panel)
        q=thesis._overlay_actual_amount(px_raw, code, amount_auth)
        q.index=pd.to_datetime(q.index, errors="coerce")
        q=q[q.index.notna() & (q.index.normalize() <= wd)].sort_index().tail(900).copy()
        if q.empty:
            rows.append({"version":VERSION,"code":code,"weekly_restart_date":_fmt_date(wd),"contract_status":"NO_PRICE_ASOF","contract_explained":0,"research_only":True})
            continue
        try:
            d,e,inv=thesis.evaluate_core224(q)
        except Exception as exc:
            rows.append({"version":VERSION,"code":code,"weekly_restart_date":_fmt_date(wd),"contract_status":f"EVAL_ERROR:{type(exc).__name__}","contract_explained":0,"research_only":True})
            continue
        dr=d.iloc[-1].to_dict() if isinstance(d,pd.DataFrame) and not d.empty else {}
        wstate=str(wr.get("core224_state", rr.get("weekly_state","")) or "")
        dstate=str(dr.get("core224_state","") or "")
        am=0; ap=0
        rec={
            "version":VERSION,"code":code,"name":rr.get("name",""),"weekly_restart_date":_fmt_date(wd),
            "weekly_state":wstate,"contract_900bar_state":dstate,"state_match":int(wstate==dstate and bool(wstate)),
            "contract_history_rows":len(q),"contract_history_start":_fmt_date(q.index.min()),"contract_history_end":_fmt_date(q.index.max()),
            "contract_invariant_fail_rows":len(inv) if isinstance(inv,pd.DataFrame) else 0,
            "cache_source":meta.get("source", meta.get("price_cache_source","")) if isinstance(meta,dict) else "",
            "research_only":True,
        }
        for c in anchors:
            wa=_fmt_date(wr.get(c)); da=_fmt_date(dr.get(c))
            rec[f"weekly_{c}"]=wa; rec[f"contract_{c}"]=da
            if wa or da:
                ap += 1; am += int(bool(wa) and wa==da)
        exact=int(rec["state_match"]==1 and (ap==0 or am==ap))
        rec["anchor_match_count"]=am; rec["anchor_present_count"]=ap; rec["anchor_all_match"]=int(ap==0 or am==ap)
        rec["contract_explained"]=exact
        rec["contract_status"]="EXPLAINED_WEEKLY_900BAR_WINDOW_CONTRACT" if exact else "UNEXPLAINED_AFTER_WEEKLY_900BAR_WINDOW_CONTRACT"
        rec["resolution_note"]="DIAGNOSTIC_ONLY_DAILY_EVENT_SET_FROZEN"
        rows.append(rec)
    return pd.DataFrame(rows)


def _extract_shard_restart_input_records(payloads: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]]=[]
    for z in (payloads or []):
        if not isinstance(z,dict): continue
        side=z.get("runtime_sidecars",{}) if isinstance(z.get("runtime_sidecars",{}),dict) else {}
        vals=side.get("V25_CORE224_RESTART_INPUTS",[]) or []
        for r in vals:
            if isinstance(r,dict): rows.append(dict(r))
    return rows


def _exact_input_payload_hash(rows: Any) -> str:
    try:
        raw=json.dumps(list(rows or []),ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False).encode("utf-8")
    except Exception:
        return ""
    return hashlib.sha256(raw).hexdigest()


def _exact_shard_restart_input_proof(
    weekly: pd.DataFrame, payloads: Optional[List[Dict[str, Any]]], continuous_inputs: Dict[str,pd.DataFrame],
) -> pd.DataFrame:
    """Replay the exact shard-local normalized input saved at each weekly RESTART.

    This is the only parity proof allowed to satisfy Policy Lock.  Reconstructing a weekly row
    from a parent-merged cache is diagnostic only because that cache may carry a different start
    window or Amount authority set.
    """
    if weekly is None or weekly.empty:
        return pd.DataFrame()
    side_rows=_extract_shard_restart_input_records(payloads)
    by_key={}
    for r in side_rows:
        code=_norm_code(r.get("code","")); ds=_fmt_date(r.get("signal_date") or r.get("expected_restart_date"))
        if code and ds: by_key[(code,ds)]=r
    out=[]
    anchors=["l0_date","accum_date","h1_date","pullback_date","healthy_date","restart_date"]
    for _,wr0 in weekly.iterrows():
        wr=wr0.to_dict(); code=_norm_code(wr.get("code","")); obs=_fmt_date(wr.get("weekly_observation_date") or wr.get("weekly_restart_date")); rst=_fmt_date(wr.get("weekly_restart_date"))
        src=by_key.get((code,obs)) or by_key.get((code,rst))
        base={"version":VERSION,"code":code,"name":wr.get("name",""),"weekly_observation_date":obs,"weekly_restart_date":rst,"research_only":True}
        if not src:
            out.append({**base,"proof_status":"MISSING_EXACT_SHARD_RESTART_INPUT","exact_shard_replay_pass":0,"same_input_nondeterminism":0,"cross_lane_explained":0})
            continue
        payload=src.get("input_payload",[]) or []; stored_sha=str(src.get("input_sha256","") or ""); calc_sha=_exact_input_payload_hash(payload); hash_ok=int(bool(stored_sha) and stored_sha==calc_sha)
        try:
            q=pd.DataFrame(payload)
            if not q.empty:
                q["date"]=pd.to_datetime(q.get("date"),errors="coerce"); q=q[q["date"].notna()].sort_values("date",kind="stable").reset_index(drop=True)
            d,e,inv=thesis.evaluate_core224(q) if not q.empty else (pd.DataFrame(),pd.DataFrame(),pd.DataFrame())
            dr=d.iloc[-1].to_dict() if isinstance(d,pd.DataFrame) and not d.empty else {}
            er={}
            if isinstance(e,pd.DataFrame) and not e.empty:
                _ed=pd.to_datetime(e.get("date"),errors="coerce").dt.normalize()
                _target=pd.to_datetime(obs or rst,errors="coerce")
                _mask=e.get("to_state",pd.Series("",index=e.index)).astype(str).eq("CORE224_RESTART")
                if pd.notna(_target): _mask=_mask & _ed.eq(pd.Timestamp(_target).normalize())
                if _mask.any(): er=e[_mask].iloc[-1].to_dict()
            replay_ctx={**dr,**er}
        except Exception as exc:
            out.append({**base,"proof_status":f"EXACT_SHARD_REPLAY_ERROR:{type(exc).__name__}","stored_input_sha256":stored_sha,"recomputed_input_sha256":calc_sha,"input_hash_match":hash_ok,"exact_shard_replay_pass":0,"same_input_nondeterminism":0,"cross_lane_explained":0})
            continue
        exp_state=str(src.get("expected_state","CORE224_RESTART") or "CORE224_RESTART"); replay_state=str(dr.get("core224_state","") or "")
        am=0; ap=0; rec={**base,"input_schema":src.get("schema",""),"stored_input_sha256":stored_sha,"recomputed_input_sha256":calc_sha,"input_hash_match":hash_ok,"shard_input_rows":int(src.get("input_rows",len(payload)) or len(payload)),"shard_input_start":src.get("input_start",""),"shard_input_end":src.get("input_end",""),"expected_state":exp_state,"replay_state":replay_state,"state_match":int(exp_state==replay_state),"replay_invariant_fail_rows":len(inv) if isinstance(inv,pd.DataFrame) else 0}
        for a in anchors:
            ev=_fmt_date(src.get("expected_"+a)); rv=_fmt_date(replay_ctx.get(a)); rec["expected_"+a]=ev; rec["replay_"+a]=rv
            if ev or rv: ap+=1; am+=int(bool(ev) and ev==rv)
        rec["anchor_match_count"]=am; rec["anchor_present_count"]=ap; rec["anchor_all_match"]=int(ap==0 or am==ap)
        proof=int(hash_ok==1 and rec["state_match"]==1 and rec["anchor_all_match"]==1 and rec["replay_invariant_fail_rows"]==0)
        # Compare to the continuous Daily-lane prefix using the same normalizer/hash contract.
        cq=continuous_inputs.get(code,pd.DataFrame()) if isinstance(continuous_inputs,dict) else pd.DataFrame(); cont_state=""; cont_sha=""; cont_rows=0; cont_start=""; cont_end=""
        if isinstance(cq,pd.DataFrame) and not cq.empty:
            try:
                wd=pd.to_datetime(obs,errors="coerce"); pref=cq.copy(); pref.index=pd.to_datetime(pref.index,errors="coerce"); pref=pref[pref.index.notna() & (pref.index.normalize()<=pd.Timestamp(wd).normalize())].sort_index()
                cpayload,cont_sha=thesis._core224_exact_replay_input(pref); cont_rows=len(cpayload); cont_start=cpayload[0]["date"] if cpayload else ""; cont_end=cpayload[-1]["date"] if cpayload else ""
                cd,_,_=thesis.evaluate_core224(pref); cont_state=str(cd.iloc[-1].get("core224_state","") or "") if isinstance(cd,pd.DataFrame) and not cd.empty else ""
            except Exception:
                pass
        same_input=int(bool(stored_sha) and stored_sha==cont_sha)
        same_input_nondet=int(proof==1 and same_input==1 and cont_state and cont_state!=replay_state)
        cross_explained=int(proof==1 and same_input==0)
        if not proof: status="EXACT_SHARD_INPUT_REPLAY_MISMATCH"
        elif same_input_nondet: status="INVALID_SAME_INPUT_NONDETERMINISM"
        elif cross_explained: status="EXACT_SHARD_INPUT_REPLAY_PASS_CONTINUOUS_INPUT_DIFFERS"
        else: status="EXACT_SHARD_INPUT_REPLAY_PASS_SAME_CONTEXT"
        rec.update({"exact_shard_replay_pass":proof,"continuous_prefix_state":cont_state,"continuous_prefix_sha256":cont_sha,"continuous_prefix_rows":cont_rows,"continuous_prefix_start":cont_start,"continuous_prefix_end":cont_end,"same_as_continuous_input":same_input,"same_input_nondeterminism":same_input_nondet,"cross_lane_explained":cross_explained,"proof_status":status,"resolution_note":"EXACT_SHARD_INPUT_PROOF_ONLY; DAILY_EVENT_SET_NOT_MUTATED"})
        out.append(rec)
    return pd.DataFrame(out)


def _semantic_hash(df: pd.DataFrame, cols: Optional[List[str]] = None) -> str:
    """Version-insensitive deterministic hash for input/regression identity."""
    if df is None or df.empty:
        return hashlib.sha256(b"EMPTY").hexdigest()[:20]
    q=df.copy()
    volatile={"version","research_only","elapsed_sec","live_logic_changed","real_order_changed"}
    if cols is None:
        cols=[c for c in q.columns if c not in volatile]
    else:
        cols=[c for c in cols if c in q.columns and c not in volatile]
    return _stable_frame_hash(q, cols)


def _input_fingerprint_regression(
    out: Path, state: pd.DataFrame, seeds: pd.DataFrame, weekly_seed_authority: pd.DataFrame, state_df: pd.DataFrame,
    transition_df: pd.DataFrame, restart_df: pd.DataFrame, price_parts: List[str], targeted_dates: pd.DataFrame, exact_shard_proof: pd.DataFrame,
) -> pd.DataFrame:
    """Canonical semantic fingerprints.

    V25.4.5 hashed several derived frames with their ``version`` column, so a code-version bump
    could look like changed market data.  V25.4.6 separates source-input identity from derived
    outputs and ignores volatile metadata.  A schema migration is not counted as an input change.
    """
    cache_root = hist_asof._set_cache_root(out)
    p = Path(cache_root) / INPUT_FINGERPRINT_CACHE_FILE
    weekly_cols=["signal_date","code","core224_state","l0_date","accum_date","h1_date","pullback_date","healthy_date","restart_date","actual_amount_history_ready20"]
    seed_cols=["code","seed_reason","first_seed_date","last_seed_date","weekly_seed_rows","weekly_non_none_rows","weekly_states_seen"]
    weekly_seed_cols=["signal_date","code","core224_state","weekly_seed_reason","watch_start_date","watch_end_exclusive"]
    shard_proof_cols=["code","weekly_observation_date","weekly_restart_date","stored_input_sha256","recomputed_input_sha256","exact_shard_replay_pass","same_input_nondeterminism","cross_lane_explained"]
    daily_state_cols=["code","date","core224_state","core224_transition","l0_date","accum_date","h1_date","pullback_date","healthy_date","restart_date","amount_is_actual","actual_amount_history_ready20"]
    transition_cols=["code","date","from_state","to_state","l0_date","accum_date","h1_date","pullback_date","healthy_date","restart_date"]
    restart_cols=["event_id","cycle_id","code","restart_date","l0_date","accum_date","h1_date","pullback_date"]
    authority_cols=["signal_date","asof_date","status","complete","fallback_used","targeted_date_complete","full_name_complete","valid_market_days","required_market_days"]
    current = {
        "fingerprint_schema_version": FINGERPRINT_SCHEMA_VERSION,
        "weekly_core_input": _semantic_hash(state, weekly_cols),
        "seed_ledger": _semantic_hash(seeds, seed_cols),
        "weekly_seed_authority": _semantic_hash(weekly_seed_authority, weekly_seed_cols),
        "exact_shard_restart_input_proof": _semantic_hash(exact_shard_proof, shard_proof_cols),
        "price_amount_input": hashlib.sha256("|".join(sorted(price_parts)).encode()).hexdigest()[:20] if price_parts else hashlib.sha256(b"EMPTY").hexdigest()[:20],
        "daily_state": _semantic_hash(state_df, daily_state_cols),
        "daily_transition": _semantic_hash(transition_df, transition_cols),
        "daily_restart": _semantic_hash(restart_df, restart_cols),
        "targeted_authority": _semantic_hash(targeted_dates, authority_cols),
    }
    current["composite"] = hashlib.sha256("|".join(current[k] for k in ["weekly_core_input","seed_ledger","weekly_seed_authority","exact_shard_restart_input_proof","price_amount_input","daily_state","daily_transition","daily_restart","targeted_authority"]).encode()).hexdigest()[:20]
    prev: Dict[str, Any] = {}
    try:
        if p.exists(): prev = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        prev = {}
    prev_schema=str(prev.get("fingerprint_schema_version", ""))
    compatible = prev_schema == FINGERPRINT_SCHEMA_VERSION
    # weekly_core_input and price_amount_input used the same canonical algorithms in V25.4.5,
    # so they remain comparable across the one-time schema migration. Other components get a
    # fresh baseline instead of a false CHANGED flag caused by the old version column.
    cross_schema_comparable={"weekly_core_input","price_amount_input"}
    source_components={"weekly_core_input","seed_ledger","weekly_seed_authority","exact_shard_restart_input_proof","price_amount_input","targeted_authority"}
    rows=[]
    for key in ["weekly_core_input","seed_ledger","weekly_seed_authority","exact_shard_restart_input_proof","price_amount_input","daily_state","daily_transition","daily_restart","targeted_authority","composite"]:
        old=str(prev.get(key,"")); new=str(current.get(key,"")); can_compare=bool(old) and (compatible or key in cross_schema_comparable)
        changed=int(can_compare and old!=new)
        if not old:
            cls="FIRST_OBSERVATION"
        elif not compatible and key not in cross_schema_comparable:
            cls="SCHEMA_MIGRATION_BASELINE"
        else:
            cls="CHANGED" if old!=new else "IDENTICAL"
        rows.append({
            "version":VERSION,"fingerprint_schema_version":FINGERPRINT_SCHEMA_VERSION,"previous_schema_version":prev_schema,
            "component":key,"component_type":"SOURCE_INPUT" if key in source_components else "DERIVED_OUTPUT",
            "previous_fingerprint":old,"current_fingerprint":new,"previous_available":int(bool(old)),
            "comparable_to_previous":int(can_compare),"changed_vs_previous":changed,
            "source_input_changed":int(key in source_components and changed),"classification":cls,"research_only":True,
        })
    try:
        tmp=p.with_suffix(p.suffix+".tmp"); tmp.write_text(json.dumps(current,ensure_ascii=False,sort_keys=True,indent=2),encoding="utf-8"); os.replace(tmp,p)
    except Exception:
        pass
    return pd.DataFrame(rows)



def _policy_specs() -> List[Dict[str, Any]]:
    """Frozen policy family chosen *before* forward OOS scoring.

    PRIMARY is Fib61.8 for structural/risk-balance reasons, not because it maximized the same
    sample. Challengers remain fixed and can never replace PRIMARY automatically.
    """
    return [
        {"policy_id":"PRIMARY_FIB618_PLUS5_D1","role":"PRIMARY","entry_mode":"D1_OPEN_CAUSAL","entry_style":"SINGLE","stop_lens":"FIB_61_8","exit_policy":"PLUS5_FULL_EXIT","roundtrip_cost_bps":20.0},
        {"policy_id":"CHALLENGER_PBLOW_PLUS5_D1","role":"CHALLENGER_A","entry_mode":"D1_OPEN_CAUSAL","entry_style":"SINGLE","stop_lens":"PB_LOW","exit_policy":"PLUS5_FULL_EXIT","roundtrip_cost_bps":20.0},
        {"policy_id":"CHALLENGER_FIB786_PLUS5_D1","role":"CHALLENGER_B","entry_mode":"D1_OPEN_CAUSAL","entry_style":"SINGLE","stop_lens":"FIB_78_6","exit_policy":"PLUS5_FULL_EXIT","roundtrip_cost_bps":20.0},
        {"policy_id":"SHADOW_L0_PLUS5_D1","role":"SHADOW","entry_mode":"D1_OPEN_CAUSAL","entry_style":"SINGLE","stop_lens":"L0_STRUCTURE","exit_policy":"PLUS5_FULL_EXIT","roundtrip_cost_bps":20.0},
    ]


def _policy_lock_cutoff() -> str:
    raw=str(os.getenv("V25_POLICY_LOCK_CUTOFF_DATE", POLICY_LOCK_CUTOFF_DEFAULT)).strip()
    d=pd.to_datetime(raw, errors="coerce")
    return pd.Timestamp(d).strftime("%Y-%m-%d") if pd.notna(d) else POLICY_LOCK_CUTOFF_DEFAULT


def _policy_lock_manifest(
    out: Path, parity_unexplained: int, source_input_changed: int,
    training_restart_events: int, causal_proven_events: int, policy_training_eligible_events: int,
    authority_dates: int, authority_complete_dates: int, invariant_fail_rows: int,
    weekly_restart_events: int, exact_shard_replay_pass_events: int, same_input_nondeterminism_events: int,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cutoff=_policy_lock_cutoff(); specs=_policy_specs()
    payload={
        "schema":POLICY_LOCK_SCHEMA_VERSION,"training_cutoff_date":cutoff,
        "declared_lock_date":POLICY_LOCK_CREATED_DEFAULT,"selection_rule":"STRUCTURAL_RISK_BALANCE_NOT_SAME_SAMPLE_BEST_RETURN",
        "primary_policy_id":"PRIMARY_FIB618_PLUS5_D1","max_follow_days":60,"same_day_collision":"STOP_FIRST",
        "gap_through_stop":"EXIT_AT_OPEN_IF_OPEN_BELOW_STOP","d1_entry_gap_below_stop":"CANCEL_ENTRY",
        "weekly_seed_gate":"LATEST_WEEKLY_SNAPSHOT_AT_OR_BEFORE_RESTART_MUST_SEED_CODE",
        "canonical_signal_lane":"CONTINUOUS_DAILY_CORE224_RESTART",
        "weekly_lane_role":"CAUSAL_WATCHLIST_SEED_ONLY",
        "exact_weekly_parity":"SHARD_LOCAL_RESTART_INPUT_REPLAY_REQUIRED",
        "policies":specs,"research_only":True,"live_logic_changed":False,"real_order_changed":False,
    }
    digest=hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()
    root=Path(hist_asof._set_cache_root(out))/"v25_forward_oos"; root.mkdir(parents=True,exist_ok=True); lockp=root/POLICY_LOCK_CACHE_FILE
    old={}
    try:
        if lockp.exists(): old=json.loads(lockp.read_text(encoding="utf-8"))
    except Exception: old={}
    old_digest=str(old.get("policy_lock_digest",""))
    prerequisites_ok=(
        int(parity_unexplained)==0 and int(source_input_changed)==0 and int(invariant_fail_rows)==0
        and int(training_restart_events)>=30 and int(causal_proven_events)>=30 and int(policy_training_eligible_events)>=30
        and int(authority_dates)>0 and int(authority_complete_dates)==int(authority_dates)
        and int(weekly_restart_events)>0 and int(exact_shard_replay_pass_events)==int(weekly_restart_events)
        and int(same_input_nondeterminism_events)==0
    )
    if old_digest:
        status="LOCKED" if old_digest==digest else "INVALID_POLICY_LOCK_DRIFT"
    elif not prerequisites_ok:
        status="PENDING_POLICY_LOCK_PREREQUISITES"
    else:
        status="LOCKED"
        towrite={**payload,"policy_lock_digest":digest}
        try:
            tmp=lockp.with_suffix(lockp.suffix+".tmp"); tmp.write_text(json.dumps(towrite,ensure_ascii=False,sort_keys=True,indent=2),encoding="utf-8"); os.replace(tmp,lockp)
        except Exception:
            status="INVALID_POLICY_LOCK_PERSIST_FAILED"
    rows=[]
    for x in specs:
        rows.append({
            "version":VERSION,"policy_lock_schema":POLICY_LOCK_SCHEMA_VERSION,"policy_lock_status":status,
            "policy_lock_digest":digest,"persisted_digest":old_digest or (digest if status=="LOCKED" else ""),
            "training_cutoff_date":cutoff,"declared_lock_date":POLICY_LOCK_CREATED_DEFAULT,
            "selection_rule":payload["selection_rule"],"primary_policy_id":payload["primary_policy_id"],
            "max_follow_days":60,"same_day_collision":"STOP_FIRST","gap_through_stop":"EXIT_AT_OPEN_IF_OPEN_BELOW_STOP",
            "d1_entry_gap_below_stop":"CANCEL_ENTRY","auto_policy_switch":0,"policy_tuning_after_cutoff":0,
            "lock_prereq_training_restart_events":int(training_restart_events),"lock_prereq_causal_proven_events":int(causal_proven_events),
            "lock_prereq_policy_training_eligible_events":int(policy_training_eligible_events),
            "lock_prereq_authority_dates":int(authority_dates),"lock_prereq_authority_complete_dates":int(authority_complete_dates),"lock_prereq_invariant_fail_rows":int(invariant_fail_rows),
            "lock_prereq_weekly_restart_events":int(weekly_restart_events),"lock_prereq_exact_shard_replay_pass_events":int(exact_shard_replay_pass_events),
            "lock_prereq_same_input_nondeterminism_events":int(same_input_nondeterminism_events),
            **x,"research_only":True,"live_logic_changed":False,"real_order_changed":False,
        })
    meta={
        "status":status,"digest":digest,"cutoff":cutoff,"primary_policy_id":payload["primary_policy_id"],"specs":specs,"prerequisites_ok":int(prerequisites_ok),
        "parity_unexplained":int(parity_unexplained),"source_input_changed":int(source_input_changed),
        "training_restart_events":int(training_restart_events),"causal_proven_events":int(causal_proven_events),"policy_training_eligible_events":int(policy_training_eligible_events),
        "authority_dates":int(authority_dates),"authority_complete_dates":int(authority_complete_dates),"invariant_fail_rows":int(invariant_fail_rows),
        "weekly_restart_events":int(weekly_restart_events),"exact_shard_replay_pass_events":int(exact_shard_replay_pass_events),
        "same_input_nondeterminism_events":int(same_input_nondeterminism_events),
    }
    return pd.DataFrame(rows), meta


def _forward_cache_dir(out: Path) -> Path:
    # Reuse the already-persisted V20 Historical-AsOf cache root so GitHub Actions can
    # resume the policy lock and forward ledger without any workflow/cache-path change.
    p=Path(hist_asof._set_cache_root(out))/"v25_forward_oos"; p.mkdir(parents=True,exist_ok=True); return p


def _read_csv_safe(p: Path) -> pd.DataFrame:
    if not p.exists(): return pd.DataFrame()
    try: return pd.read_csv(p, dtype={"code":str})
    except Exception: return pd.DataFrame()


def _write_csv_atomic(p: Path, df: pd.DataFrame) -> None:
    p.parent.mkdir(parents=True,exist_ok=True); tmp=p.with_name(p.name+f".{os.getpid()}.tmp")
    df.to_csv(tmp,index=False,encoding="utf-8-sig"); os.replace(tmp,p)


def _is_final_forward_execution(status: Any) -> int:
    return int(str(status or "") in {"STRUCTURE_STOP","PLUS5_FULL_EXIT","OBSERVATION_END","ENTRY_CANCEL_GAP_AT_OR_BELOW_STOP"})


def _max_consecutive_losses(vals: pd.Series) -> int:
    mx=cur=0
    for v in pd.to_numeric(vals,errors="coerce").dropna():
        if float(v)<0: cur+=1; mx=max(mx,cur)
        else: cur=0
    return int(mx)


def _forward_oos_locked_ledgers(
    out: Path, restart_df: pd.DataFrame, px_by_code: Dict[str,pd.DataFrame], cfg: thesis.Core224LifecycleConfig,
    lock_df: pd.DataFrame, lock_meta: Dict[str,Any],
) -> Tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,pd.DataFrame,Dict[str,Any]]:
    """Persistent forward OOS/PAPER ledger after the frozen 2026-08-10 training cutoff.

    Finalized historical outcomes are immutable.  Right-censored/open rows may update as new bars
    arrive.  Authority can upgrade from unresolved to proven without rewriting the frozen price
    outcome.  No policy is automatically selected/switchable based on this ledger.
    """
    status=str(lock_meta.get("status","")); cutoff=pd.to_datetime(lock_meta.get("cutoff"),errors="coerce")
    if status!="LOCKED" or pd.isna(cutoff):
        return pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),{"status":"POLICY_LOCK_NOT_ACTIVE","events":0,"eligible":0,"finalized":0,"immutability_conflicts":0}
    cutoff=pd.Timestamp(cutoff).normalize(); root=_forward_cache_dir(out)
    event_cache=root/"forward_oos_event_ledger.csv"; policy_cache=root/"forward_oos_policy_ledger.csv"
    r=restart_df.copy() if isinstance(restart_df,pd.DataFrame) else pd.DataFrame()
    if not r.empty:
        r["_signal_date"]=pd.to_datetime(r.get("restart_date",r.get("date")),errors="coerce").dt.normalize()
        r=r[r["_signal_date"].gt(cutoff)].copy()
    event_rows=[]; policy_rows=[]
    specs=lock_meta.get("specs",[]) or []
    for _,rr in r.iterrows():
        rec=rr.to_dict(); code=_norm_code(rec.get("code","")); sig=rec.get("_signal_date")
        eid=str(rec.get("event_id",rec.get("cycle_id","")) or f"{code}:{_fmt_date(sig)}")
        proven=int(float(rec.get("daily_universe_membership_proven",0) or 0)==1)
        seed_eligible=int(float(rec.get("weekly_seed_causal_eligible",0) or 0)==1)
        policy_eligible=int(proven==1 and seed_eligible==1)
        event_rows.append({
            "version":VERSION,"policy_lock_digest":lock_meta.get("digest",""),"event_id":eid,"cycle_id":rec.get("cycle_id",""),
            "code":code,"name":rec.get("name",""),"signal_date":_fmt_date(sig),"training_cutoff_date":_fmt_date(cutoff),
            "forward_oos":1,"daily_universe_membership_proven":proven,"daily_universe_authority":rec.get("daily_universe_authority",""),
            "weekly_seed_causal_eligible":seed_eligible,"weekly_seed_reason":rec.get("weekly_seed_reason",""),
            "targeted_authority_class":rec.get("targeted_authority_class",""),"score_eligible":policy_eligible,
            "authority_status":"PROVEN_CAUSAL_SEED_AND_UNIVERSE" if policy_eligible else ("UNIVERSE_PROVEN_SEED_NOT_CAUSAL" if proven else "AUTHORITY_PENDING_OR_UNRESOLVED"),"first_seen_version":VERSION,
            "research_only":True,"paper_only":True,"real_order_changed":False,
        })
        px=px_by_code.get(code,pd.DataFrame())
        if px is None or px.empty: continue
        pb=thesis._low_between(px,rec.get("pullback_date"),rec.get("restart_date")); l0=pd.to_numeric(pd.Series([rec.get("l0_low")]),errors="coerce").iloc[0]; h1=pd.to_numeric(pd.Series([rec.get("h1_high")]),errors="coerce").iloc[0]
        if pb is None or pd.isna(l0) or pd.isna(h1): continue
        lenses=thesis._stop_lenses(float(l0),float(h1),float(pb),cfg)
        for spec in specs:
            lens=str(spec.get("stop_lens","")); stop=lenses.get(lens)
            if stop is None: continue
            sim=_simulate_execution_causality_one(rec,px,lens,float(stop),cfg,"D1_OPEN_CAUSAL","PLUS5_FULL_EXIT",float(spec.get("roundtrip_cost_bps",20.0)))
            sim.update({
                "policy_lock_digest":lock_meta.get("digest",""),"policy_id":spec.get("policy_id",""),"policy_role":spec.get("role",""),
                "training_cutoff_date":_fmt_date(cutoff),"forward_oos":1,"score_eligible":policy_eligible,"weekly_seed_causal_eligible":seed_eligible,"paper_only":True,
                "finalized":_is_final_forward_execution(sim.get("execution_status")),"auto_policy_switch":0,
            })
            policy_rows.append(sim)
    cur_events=pd.DataFrame(event_rows); cur_policy=pd.DataFrame(policy_rows)
    old_events=_read_csv_safe(event_cache); old_policy=_read_csv_safe(policy_cache); imm=[]

    # Merge event authority monotonically; once proven, never downgrade.
    if old_events.empty: merged_events=cur_events.copy()
    elif cur_events.empty: merged_events=old_events.copy()
    else:
        om={str(x.get("event_id","")):x.to_dict() for _,x in old_events.iterrows()};
        for _,x in cur_events.iterrows():
            d=x.to_dict(); k=str(d.get("event_id","")); prev=om.get(k,{})
            if prev:
                d["first_seen_version"]=prev.get("first_seen_version",d.get("first_seen_version"))
                if int(float(prev.get("daily_universe_membership_proven",0) or 0))==1:
                    d["daily_universe_membership_proven"]=1
                if int(float(prev.get("weekly_seed_causal_eligible",0) or 0))==1:
                    d["weekly_seed_causal_eligible"]=1
                d["score_eligible"]=int(int(float(d.get("daily_universe_membership_proven",0) or 0))==1 and int(float(d.get("weekly_seed_causal_eligible",0) or 0))==1)
                d["authority_status"]="PROVEN_CAUSAL_SEED_AND_UNIVERSE" if d["score_eligible"] else ("UNIVERSE_PROVEN_SEED_NOT_CAUSAL" if int(float(d.get("daily_universe_membership_proven",0) or 0))==1 else "AUTHORITY_PENDING_OR_UNRESOLVED")
            om[k]={**prev,**d}
        merged_events=pd.DataFrame(list(om.values()))

    # Merge policy outcomes. Finalized price outcomes are immutable; authority eligibility may upgrade.
    keycols=["policy_id","event_id"]
    if old_policy.empty: merged_policy=cur_policy.copy()
    elif cur_policy.empty: merged_policy=old_policy.copy()
    else:
        om={(str(x.get("policy_id","")),str(x.get("event_id",""))):x.to_dict() for _,x in old_policy.iterrows()}
        critical=["entry_date","entry_price","stop_price","execution_status","exit_reason","exit_date","exit_price","net_r_multiple"]
        for _,x in cur_policy.iterrows():
            d=x.to_dict(); k=(str(d.get("policy_id","")),str(d.get("event_id",""))); prev=om.get(k)
            if prev and int(float(prev.get("finalized",0) or 0))==1:
                diffs=[]
                numeric_critical={"entry_price","stop_price","exit_price","net_r_multiple"}
                for c in critical:
                    if c in numeric_critical:
                        av=pd.to_numeric(pd.Series([prev.get(c)]),errors="coerce").iloc[0]; bv=pd.to_numeric(pd.Series([d.get(c)]),errors="coerce").iloc[0]
                        if pd.isna(av) and pd.isna(bv):
                            continue
                        if pd.isna(av) != pd.isna(bv) or (pd.notna(av) and pd.notna(bv) and not np.isclose(float(av),float(bv),rtol=1e-10,atol=1e-12)):
                            diffs.append(c)
                    else:
                        a=str(prev.get(c,"")); b=str(d.get(c,""))
                        if a!=b: diffs.append(c)
                if diffs:
                    imm.append({"version":VERSION,"policy_id":k[0],"event_id":k[1],"conflict_columns":"|".join(diffs),"status":"FINALIZED_OUTCOME_IMMUTABILITY_CONFLICT","research_only":True})
                # Preserve frozen result, but permit authority eligibility to upgrade.
                kept=dict(prev)
                kept["daily_universe_membership_proven"]=max(int(float(prev.get("daily_universe_membership_proven",0) or 0)),int(float(d.get("daily_universe_membership_proven",0) or 0)))
                kept["weekly_seed_causal_eligible"]=max(int(float(prev.get("weekly_seed_causal_eligible",0) or 0)),int(float(d.get("weekly_seed_causal_eligible",0) or 0)))
                kept["score_eligible"]=int(kept["daily_universe_membership_proven"]==1 and kept["weekly_seed_causal_eligible"]==1)
                om[k]=kept
            else:
                om[k]={**(prev or {}),**d}
        merged_policy=pd.DataFrame(list(om.values()))
    if not merged_events.empty: merged_events=merged_events.sort_values([c for c in ["signal_date","code","event_id"] if c in merged_events.columns],kind="stable")
    if not merged_policy.empty: merged_policy=merged_policy.sort_values([c for c in ["signal_date","policy_role","policy_id","code"] if c in merged_policy.columns],kind="stable")
    try:
        _write_csv_atomic(event_cache,merged_events); _write_csv_atomic(policy_cache,merged_policy)
    except Exception:
        pass
    imm_df=pd.DataFrame(imm)

    sums=[]
    if not merged_policy.empty:
        for (pid,role),g in merged_policy.groupby(["policy_id","policy_role"],dropna=False):
            elig=g[_num(g,"score_eligible",0).eq(1)].copy(); exe=elig[_num(elig,"trade_executed",0).eq(1)].copy(); fin=exe[_num(exe,"finalized",0).eq(1)].copy(); openq=exe[_num(exe,"finalized",0).ne(1)].copy()
            nr=pd.to_numeric(fin.get("net_r_multiple"),errors="coerce") if not fin.empty else pd.Series(dtype=float)
            order=fin.copy()
            if not order.empty:
                order["_sd"]=pd.to_datetime(order.get("signal_date"),errors="coerce"); order=order.sort_values(["_sd","event_id"],kind="stable"); seq=pd.to_numeric(order.get("net_r_multiple"),errors="coerce").fillna(0); cum=seq.cumsum(); dd=(cum.cummax()-cum); cumulative=float(cum.iloc[-1]) if len(cum) else 0.0; maxdd=float(dd.max()) if len(dd) else 0.0; maxloss=_max_consecutive_losses(seq)
            else: cumulative=0.0; maxdd=0.0; maxloss=0
            sums.append({
                "version":VERSION,"policy_lock_digest":lock_meta.get("digest",""),"policy_id":pid,"policy_role":role,
                "training_cutoff_date":_fmt_date(cutoff),"forward_events":len(g),"causal_eligible_events":len(elig),"executed_trades":len(exe),"finalized_trades":len(fin),"open_or_censored_trades":len(openq),
                "plus5_exits":int(fin.get("execution_status",pd.Series(dtype=str)).astype(str).eq("PLUS5_FULL_EXIT").sum()) if not fin.empty else 0,
                "structure_stops":int(fin.get("execution_status",pd.Series(dtype=str)).astype(str).eq("STRUCTURE_STOP").sum()) if not fin.empty else 0,
                "positive_net_r_rate_pct":float((nr.dropna()>0).mean()*100.0) if nr.notna().any() else np.nan,
                "mean_net_r":float(nr.mean()) if nr.notna().any() else np.nan,"median_net_r":float(nr.median()) if nr.notna().any() else np.nan,"trim10_net_r":_trimmed_mean(nr),
                "cumulative_trade_sequence_r":cumulative,"max_drawdown_trade_sequence_r":maxdd,"max_consecutive_loss_trades":maxloss,
                "median_mae_pct":float(pd.to_numeric(fin.get("mae_pct"),errors="coerce").median()) if not fin.empty else np.nan,"median_mfe_pct":float(pd.to_numeric(fin.get("mfe_pct"),errors="coerce").median()) if not fin.empty else np.nan,
                "median_holding_days":float(pd.to_numeric(fin.get("holding_days"),errors="coerce").median()) if not fin.empty else np.nan,
                "drawdown_definition":"TRADE_SEQUENCE_R_NOT_PORTFOLIO_DRAWDOWN","auto_policy_switch":0,"research_only":True,"paper_only":True,
            })
    summary=pd.DataFrame(sums)
    evn=len(merged_events); eligible=int(_num(merged_events,"score_eligible",0).eq(1).sum()) if not merged_events.empty else 0; finalized=int(_num(merged_policy,"finalized",0).eq(1).sum()) if not merged_policy.empty else 0
    primary=merged_policy[merged_policy.get("policy_role",pd.Series(dtype=str)).astype(str).eq("PRIMARY")].copy() if not merged_policy.empty else pd.DataFrame()
    primary_finalized=int((_num(primary,"score_eligible",0).eq(1) & _num(primary,"trade_executed",0).eq(1) & _num(primary,"finalized",0).eq(1)).sum()) if not primary.empty else 0
    if len(imm_df)>0: fstatus="INVALID_FORWARD_LEDGER_IMMUTABILITY_CONFLICT"
    elif evn==0: fstatus="LOCKED_AWAITING_FORWARD_EVENTS"
    elif eligible==0: fstatus="FORWARD_PAPER_AUTHORITY_PENDING"
    elif primary_finalized<30: fstatus="FORWARD_OOS_WARMUP"
    else: fstatus="FORWARD_OOS_ACTIVE_LOCKED"
    meta={"status":fstatus,"events":evn,"eligible":eligible,"finalized":finalized,"primary_finalized":primary_finalized,"immutability_conflicts":len(imm_df)}
    return merged_events,merged_policy,summary,imm_df,meta


def _stop_lens_risk_tradeoff(risk_df: pd.DataFrame, path_summary: pd.DataFrame) -> pd.DataFrame:
    if risk_df is None or risk_df.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]]=[]
    scopes=[("ALL_RESEARCH",risk_df),("EXACT_CAUSAL_ASOF",risk_df[_num(risk_df,"daily_universe_membership_proven",0).eq(1)])]
    for scope,base in scopes:
        if base.empty: continue
        for lens,g in base.groupby("stop_lens",dropna=False):
            sr=pd.to_numeric(g.get("single_final_r_multiple"),errors="coerce"); sc=pd.to_numeric(g.get("scale_final_r_multiple"),errors="coerce")
            rec={
                "version":VERSION,"scope":scope,"stop_lens":lens,"events":len(g),
                "mean_single_r":float(sr.mean()) if sr.notna().any() else np.nan,"median_single_r":float(sr.median()) if sr.notna().any() else np.nan,"trim10_single_r":_trimmed_mean(sr),
                "mean_scale_r":float(sc.mean()) if sc.notna().any() else np.nan,"median_scale_r":float(sc.median()) if sc.notna().any() else np.nan,"trim10_scale_r":_trimmed_mean(sc),
                "median_single_risk_pct":float(pd.to_numeric(g.get("single_planned_risk_pct"),errors="coerce").median()),
                "median_scale_risk_pct":float(pd.to_numeric(g.get("scale_planned_risk_pct"),errors="coerce").median()),
                "median_single_mae_pct":float(pd.to_numeric(g.get("single_mae_pct"),errors="coerce").median()),
                "median_scale_mae_pct":float(pd.to_numeric(g.get("scale_mae_pct"),errors="coerce").median()),
                "median_single_holding_days":float(pd.to_numeric(g.get("single_holding_days"),errors="coerce").median()),
                "median_scale_holding_days":float(pd.to_numeric(g.get("scale_holding_days"),errors="coerce").median()),
                "research_only":True,
            }
            if path_summary is not None and not path_summary.empty:
                z=path_summary[(path_summary.get("scope",pd.Series(dtype=str)).astype(str)==scope)&(path_summary.get("stop_lens",pd.Series(dtype=str)).astype(str)==str(lens))]
                if not z.empty:
                    for cls,col in [("EARLY_STOP_RECOVERY","early_stop_recovery_pct"),("TRUE_FAILURE","true_failure_pct"),("CLEAN_WIN","clean_win_pct"),("PROFIT_THEN_BREAK","profit_then_break_pct")]:
                        x=z[z.get("path_class",pd.Series(dtype=str)).astype(str).eq(cls)]
                        rec[col]=float(x.iloc[0].get("pct",np.nan)) if not x.empty else 0.0
            rows.append(rec)
    return pd.DataFrame(rows)

def _stratified_manual(restart_df: pd.DataFrame, order_df: pd.DataFrame, policy_df: pd.DataFrame, recon: pd.DataFrame, limit: int = 60) -> pd.DataFrame:
    if restart_df is None or restart_df.empty: return pd.DataFrame()
    chosen: List[Dict[str, Any]] = []; seen: set[str] = set()
    def take(ids: Iterable[str], bucket: str, n: Optional[int] = None) -> None:
        count = 0
        for eid in ids:
            eid = str(eid)
            if not eid or eid in seen: continue
            rr = restart_df[restart_df["event_id"].astype(str).eq(eid)]
            if rr.empty: continue
            rec = rr.iloc[0].to_dict(); rec["review_bucket"] = bucket; chosen.append(rec); seen.add(eid); count += 1
            if len(chosen) >= limit or (n is not None and count >= n): break
    take(restart_df[_num(restart_df, "daily_universe_membership_proven", 0).eq(1)]["event_id"].astype(str), "EXACT_CAUSAL_ALL", None)
    if recon is not None and not recon.empty:
        bad = recon[recon["reconciliation_status"].astype(str).eq("WEEKLY_ONLY_UNRECONCILED")]
        for _, b in bad.iterrows():
            cand = restart_df[restart_df["code"].astype(str).eq(str(b.get("code", "")))]
            take(cand["event_id"].astype(str), "WEEKLY_RECON_UNRESOLVED", None)
    pb = order_df[order_df.get("stop_lens", pd.Series(dtype=str)).astype(str).eq("PB_LOW")] if order_df is not None and not order_df.empty else pd.DataFrame()
    if not pb.empty:
        take(pb[_num(pb, "stop_before_avg_recovery", 0).eq(1)]["event_id"].astype(str), "PB_STOP_BEFORE_RECOVERY", 10)
        take(pb[_num(pb, "profit5_high_before_stop", 0).eq(1)]["event_id"].astype(str), "PB_PLUS5_BEFORE_STOP", 10)
        take(pb[_num(pb, "any_same_day_collision", 0).eq(1)]["event_id"].astype(str), "PB_SAME_DAY_COLLISION", 10)
    pp = policy_df[policy_df.get("stop_lens", pd.Series(dtype=str)).astype(str).eq("PB_LOW")] if policy_df is not None and not policy_df.empty else pd.DataFrame()
    if not pp.empty:
        take(pp[pd.to_numeric(pp.get("entry_count"), errors="coerce").eq(2)]["event_id"].astype(str), "ENTRY2_ONLY", 10)
        take(pp[pd.to_numeric(pp.get("entry_count"), errors="coerce").ge(3)]["event_id"].astype(str), "ENTRY3_FILLED", 10)
    if len(chosen) < limit: take(restart_df["event_id"].astype(str), "FILL_TO_LIMIT", limit - len(chosen))
    out = pd.DataFrame(chosen)
    cols = [c for c in ["review_bucket","event_id","cycle_id","restart_date","code","name","restart_discovery","daily_universe_authority","l0_date","accum_date","h1_date","pullback_date","healthy_date","restart_evidence_count","restart_bullish","restart_prev_high_reclaim","restart_flow_uptick"] if c in out.columns]
    return out[cols].copy() if cols else out




def _env_bool(name: str, default: bool = False) -> bool:
    return str(os.getenv(name, "1" if default else "0")).strip().lower() in {"1", "true", "yes", "y", "on"}


class _CountingStockProxy:
    """Bounded retry wrapper around pykrx.stock used only by targeted authority reconstruction."""
    def __init__(self, stock: Any, max_calls: int = 650, retries: int = 1):
        self._stock = stock
        self.max_calls = max(0, int(max_calls))
        self.retries = max(0, int(retries))
        self.calls = 0
        self.errors = 0

    def __getattr__(self, name: str) -> Any:
        obj = getattr(self._stock, name)
        if not callable(obj):
            return obj
        def wrapped(*args, **kwargs):
            last = None
            for attempt in range(self.retries + 1):
                if self.calls >= self.max_calls:
                    raise RuntimeError("V25_TARGETED_CAUSAL_PROVIDER_CALL_LIMIT")
                self.calls += 1
                try:
                    return obj(*args, **kwargs)
                except Exception as exc:
                    self.errors += 1
                    last = exc
                    if attempt < self.retries:
                        time.sleep(min(1.5, 0.25 * (attempt + 1)))
            raise last  # type: ignore[misc]
        return wrapped


def _load_cached_name_map(out: Path) -> Dict[str, str]:
    names: Dict[str, str] = {}
    roots = [Path(os.getenv("V20_ASOF_CACHE_DIR", str(out / ".cache/v20_asof_snapshots"))), out / ".cache/v20_asof_snapshots"]
    for root in roots:
        p = root / "ticker_name_map.json"
        try:
            if p.exists():
                import json
                q = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(q, dict):
                    names.update({_norm_code(k): str(v or "") for k, v in q.items() if _norm_code(k) and str(v or "")})
        except Exception:
            continue
    # HF3 persists the listing seed across shards. It is used here for labels only, never for
    # historical membership: D-1 market snapshot codes still define the causal membership set.
    lp = out / "v73_listing_cache.csv"
    try:
        if lp.exists():
            z = pd.read_csv(lp, dtype=str)
            cc = next((c for c in ["Code","code","Symbol","종목코드"] if c in z.columns), None)
            nc = next((c for c in ["Name","name","종목명"] if c in z.columns), None)
            if cc and nc:
                for c, n in zip(z[cc], z[nc]):
                    code=_norm_code(c); name=str(n or "")
                    if code and name: names[code]=name
    except Exception:
        pass
    return names


def _known_common_security_name(name: Any) -> bool:
    n = str(name or "").strip()
    if not n:
        return False
    bad = r"ETF|ETN|스팩|제[0-9]+호|우$|우A$|우B$|우C$|우선주"
    return re.search(bad, n, flags=re.IGNORECASE) is None


def _cached_actual_market(day: pd.Timestamp) -> Tuple[pd.DataFrame, str]:
    ymd = pd.Timestamp(day).strftime("%Y%m%d")
    try:
        z = hist_asof._read_cache("market", ymd)
    except Exception:
        z = pd.DataFrame()
    if isinstance(z, pd.DataFrame) and not z.empty and hist_asof._actual_snapshot_rows(z) > 0:
        return z, "V25_DISK_CACHE:REPORTED_TRADING_VALUE"
    return pd.DataFrame(), "MISSING"


def _targeted_trading_calendar(out: Path, restart_df: pd.DataFrame, px_by_code: Optional[Dict[str, pd.DataFrame]] = None) -> List[pd.Timestamp]:
    """Build an exchange-day calendar from already cached daily prices; no provider calls."""
    days: set[pd.Timestamp] = set()
    codes = sorted(set(restart_df.get("code", pd.Series(dtype=str)).map(_norm_code))) if isinstance(restart_df, pd.DataFrame) else []
    for code in codes:
        if not code:
            continue
        px = (px_by_code or {}).get(code, pd.DataFrame()) if px_by_code is not None else pd.DataFrame()
        if px is None or px.empty:
            try:
                raw, _ = thesis._read_price_cache_for_code(out, code)
                px = thesis._normalize_lifecycle_price(raw)
            except Exception:
                px = pd.DataFrame()
        if isinstance(px, pd.DataFrame) and not px.empty:
            idx = pd.to_datetime(px.index, errors="coerce")
            days.update(pd.Timestamp(x).normalize() for x in idx if pd.notna(x))
    return sorted(days)


def _targeted_authority_for_restarts(
    out: Path,
    restart_df: pd.DataFrame,
    px_by_code: Optional[Dict[str, pd.DataFrame]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """Window-completion-first targeted D-1 Historical-AsOf authority.

    The first-RESTART event set is frozen before this function is called.  This lane never
    changes CORE224 membership.  It restores authoritative all-market trading-value snapshots
    from the persistent V20 cache, then spends a small bounded provider budget on the *fewest
    missing complete 20-session windows first*.  Successful snapshots are written through
    historical_asof_universe's existing market cache, so GitHub's V21 cache save/restore makes
    the lane resumable across runs.

    Full historical security names are not required to prove a positive TOP500 membership when
    the target security itself has a known ordinary-share name: ranking against the unfiltered
    name-unknown superset is conservative, because removing ETFs/SPAC/preferred rows can only
    improve the target's rank.  A negative NOT_IN verdict still requires full-name authority.
    """
    if restart_df is None or restart_df.empty:
        return pd.DataFrame(), pd.DataFrame(), {
            "provider_calls": 0, "provider_errors": 0, "dates": 0, "complete_dates": 0,
            "full_name_complete_dates": 0, "window_complete_dates": 0,
        }
    q = restart_df.copy()
    q["restart_date"] = pd.to_datetime(q.get("restart_date"), errors="coerce").dt.normalize()
    q["code"] = q.get("code", pd.Series("", index=q.index)).map(_norm_code)
    q = q[q["restart_date"].notna() & q["code"].ne("")].copy()
    if q.empty:
        return pd.DataFrame(), pd.DataFrame(), {
            "provider_calls": 0, "provider_errors": 0, "dates": 0, "complete_dates": 0,
            "full_name_complete_dates": 0, "window_complete_dates": 0,
        }

    cache_root = hist_asof._set_cache_root(out)
    progress_path = Path(cache_root) / TARGET_PROGRESS_CACHE_FILE
    name_map = _load_cached_name_map(out)
    # The frozen event ledger has the historically displayed name for the target itself.  This
    # is used only to establish that the target is an ordinary stock, never to rank other names.
    for _, rr in q.iterrows():
        code = _norm_code(rr.get("code", "")); name = str(rr.get("name", "") or "").strip()
        if code and name and code not in name_map:
            name_map[code] = name

    fetch_enabled = (
        _env_bool("V25_TARGETED_CAUSAL_FETCH_ENABLE", True)
        and bool(os.getenv("KRX_ID", "").strip())
        and bool(os.getenv("KRX_PW", "").strip())
    )
    stock_proxy: Any = None
    import_error = ""
    max_calls = int(float(os.getenv("V25_TARGETED_CAUSAL_MAX_PROVIDER_CALLS", "60")))
    retries = int(float(os.getenv("V25_TARGETED_CAUSAL_RETRIES", "1")))
    fetch_delay = max(0.0, float(os.getenv("V25_TARGETED_CAUSAL_FETCH_DELAY_SEC", "0.30")))
    if fetch_enabled:
        try:
            from pykrx import stock as _stock  # type: ignore
            stock_proxy = _CountingStockProxy(_stock, max_calls=max_calls, retries=retries)
        except Exception as exc:
            import_error = f"{type(exc).__name__}:{exc}"
            stock_proxy = None

    unique_dates = sorted(pd.Timestamp(x).normalize() for x in q["restart_date"].dropna().unique())
    trading_days = _targeted_trading_calendar(out, q, px_by_code=px_by_code)
    requirements: Dict[str, List[pd.Timestamp]] = {}
    for sig in unique_dates:
        prior = [d for d in trading_days if d < sig]
        requirements[sig.strftime("%Y-%m-%d")] = prior[-20:] if len(prior) >= 20 else []

    required_market_days = sorted({d for xs in requirements.values() for d in xs})
    local_market_cache: Dict[str, pd.DataFrame] = {}
    def cached_market(day: pd.Timestamp) -> pd.DataFrame:
        ymd = pd.Timestamp(day).strftime("%Y%m%d")
        if ymd not in local_market_cache:
            z, _ = _cached_actual_market(day); local_market_cache[ymd] = z
        return local_market_cache[ymd]
    cache_before_valid: set[str] = set()
    for day in required_market_days:
        z = cached_market(day)
        if not z.empty:
            cache_before_valid.add(_fmt_date(day))

    # Completion-first greedy scheduler.  After every completed window we recompute deficits,
    # because one fetched market snapshot may satisfy many nearby RESTART dates.
    fetched_success_days: set[str] = set()
    fetched_empty_days: set[str] = set()
    fetch_fail_days: Dict[str, str] = {}
    blocked_dates: set[str] = set()

    def valid_cached(day: pd.Timestamp) -> bool:
        return not cached_market(day).empty

    def missing_for(ds: str) -> List[pd.Timestamp]:
        req = requirements.get(ds, [])
        if len(req) < 20:
            return list(req)
        return [d for d in req if not valid_cached(d)]

    while stock_proxy is not None and int(getattr(stock_proxy, "calls", 0)) < max_calls:
        candidates: List[Tuple[int, str, List[pd.Timestamp]]] = []
        for ds, req in requirements.items():
            if len(req) < 20 or ds in blocked_dates:
                continue
            miss = missing_for(ds)
            if miss:
                candidates.append((len(miss), ds, miss))
        if not candidates:
            break
        candidates.sort(key=lambda x: (x[0], x[1]))
        _, ds, miss = candidates[0]
        made_progress = False
        # Newest-first is causally natural and tends to overlap adjacent RESTART windows most.
        for day in sorted(miss, reverse=True):
            if int(getattr(stock_proxy, "calls", 0)) >= max_calls:
                break
            ymd = day.strftime("%Y%m%d")
            if valid_cached(day):
                continue
            try:
                z, src = hist_asof._get_market_snapshot(stock_proxy, ymd)
                if isinstance(z, pd.DataFrame) and not z.empty and hist_asof._actual_snapshot_rows(z) > 0:
                    local_market_cache[ymd] = z
                    fetched_success_days.add(_fmt_date(day)); made_progress = True
                else:
                    fetched_empty_days.add(_fmt_date(day))
            except RuntimeError as exc:
                if "V25_TARGETED_CAUSAL_PROVIDER_CALL_LIMIT" in str(exc):
                    break
                fetch_fail_days[_fmt_date(day)] = f"{type(exc).__name__}:{exc}"
            except Exception as exc:
                fetch_fail_days[_fmt_date(day)] = f"{type(exc).__name__}:{exc}"
            if fetch_delay > 0:
                time.sleep(fetch_delay)
            if len(missing_for(ds)) == 0:
                break
        if len(missing_for(ds)) == 0:
            made_progress = True
        if not made_progress:
            blocked_dates.add(ds)
        if int(getattr(stock_proxy, "calls", 0)) >= max_calls:
            break

    membership_by_date: Dict[str, pd.DataFrame] = {}
    window_complete_by_date: Dict[str, int] = {}
    full_name_complete_by_date: Dict[str, int] = {}
    unknown_name_by_date: Dict[str, int] = {}
    date_rows: List[Dict[str, Any]] = []

    for sig in unique_dates:
        ds = sig.strftime("%Y-%m-%d")
        req = requirements.get(ds, [])
        snapshots: Dict[pd.Timestamp, pd.DataFrame] = {}
        sources: List[str] = []
        for day in req:
            z = cached_market(day)
            if not z.empty:
                snapshots[pd.Timestamp(day).normalize()] = z
                sources.append("V25_DISK_CACHE:REPORTED_TRADING_VALUE")
        actual20 = len(snapshots)
        if len(req) < 20 or actual20 < 20:
            date_rows.append({
                "version": VERSION, "signal_date": ds, "status": "AUTHORITY_MISSING", "complete": 0,
                "window_complete": 0, "full_name_complete": 0,
                "actual_snapshot_days": actual20, "required_snapshot_days": 20,
                "required_calendar_days": len(req), "listing_rows": 0, "unknown_name_rows": 0,
                "cap_rows": 0, "final_universe_rows": 0,
                "source": "|".join(sorted(set(sources))) if sources else "MISSING",
                "reason": "INSUFFICIENT_ACTUAL_D_MINUS_1_HISTORY",
                "missing_required_days": len([d for d in req if not valid_cached(d)]),
                "research_only": True,
            })
            window_complete_by_date[ds] = 0; full_name_complete_by_date[ds] = 0
            continue

        snaps_sorted = dict(sorted(snapshots.items())[-20:])
        d1_day = max(snaps_sorted); d1 = snaps_sorted[d1_day].copy()
        d1["code"] = d1.get("code", pd.Series("", index=d1.index)).map(_norm_code)
        d1 = d1[d1["code"].ne("")].drop_duplicates("code", keep="last")
        listing = pd.DataFrame({"Code": d1["code"], "Name": d1["code"].map(name_map).fillna("")})
        if "market" in d1.columns:
            mk = d1["market"].fillna("").astype(str).str.upper()
            if mk.isin(["KOSPI", "KOSDAQ", "코스피", "코스닥", "유가"]).any():
                listing["Market"] = d1["market"].astype(str)
        unknown_names = int(listing["Name"].fillna("").astype(str).str.len().eq(0).sum())
        unknown_name_by_date[ds] = unknown_names
        cap = pd.DataFrame()
        if "marcap" in d1.columns:
            cap = d1[["code", "marcap"]].copy(); cap["marcap"] = pd.to_numeric(cap["marcap"], errors="coerce")
            cap = cap[cap["marcap"].notna() & cap["code"].ne("")]
        final, _stats = hist_asof.build_asof_universe_from_snapshots(
            sig, listing, snaps_sorted, cap_snapshot=cap,
            core_n=int(float(os.getenv("V1081_DIRECT_TOP_N", "500"))),
            event_max=int(float(os.getenv("V1081_EVENT_EXPANSION_MAX", "100"))),
            min_price=float(os.getenv("V1081_ASOF_MIN_PRICE", "3000")),
            min_marcap=float(os.getenv("V1081_ASOF_MIN_MARCAP", "30000000000")),
            event_amount_ratio=float(os.getenv("V1081_EVENT_AMOUNT_RATIO", "3.0")),
            event_volume_ratio=float(os.getenv("V1081_EVENT_VOLUME_RATIO", "3.0")),
            event_prev_ret_pct=float(os.getenv("V1081_EVENT_PREV_RET_PCT", "5.0")),
            event_min_amount=float(os.getenv("V1081_EVENT_MIN_AMOUNT", "10000000000")),
            official_geo_codes=hist_asof._official_geo_codes(out, hist_asof._asof_1503(sig)),
        )
        window_complete = int(actual20 >= 20 and not cap.empty and not final.empty)
        full_name_complete = int(window_complete == 1 and unknown_names == 0)
        window_complete_by_date[ds] = window_complete
        full_name_complete_by_date[ds] = full_name_complete
        if window_complete:
            membership_by_date[ds] = final.copy()
        status = (
            "VALID_TARGETED_CAUSAL_ASOF" if full_name_complete
            else "VALID_WINDOW_SECURITY_NAME_PARTIAL_CONSERVATIVE_SUPERSET" if window_complete
            else "AUTHORITY_MISSING"
        )
        reason = (
            "D_MINUS_1_20D_ACTUAL_AMOUNT_MARCAP_FULL_NAME_AUTHORITY" if full_name_complete
            else f"D_MINUS_1_20D_ACTUAL_AMOUNT_MARCAP_NAME_PARTIAL;unknown_names={unknown_names}" if window_complete
            else f"actual20={actual20};cap_rows={len(cap)};final_rows={len(final)}"
        )
        date_rows.append({
            "version": VERSION, "signal_date": ds, "status": status, "complete": window_complete,
            "window_complete": window_complete, "full_name_complete": full_name_complete,
            "actual_snapshot_days": actual20, "required_snapshot_days": 20,
            "required_calendar_days": len(req), "liquidity_asof_date": _fmt_date(d1_day),
            "listing_rows": len(listing), "unknown_name_rows": unknown_names,
            "cap_rows": len(cap), "final_universe_rows": len(final),
            "source": "|".join(sorted(set(sources))), "reason": reason,
            "missing_required_days": 0, "research_only": True,
        })

    event_rows: List[Dict[str, Any]] = []
    for _, r in q.iterrows():
        code = _norm_code(r.get("code", "")); ds = pd.Timestamp(r["restart_date"]).strftime("%Y-%m-%d")
        prior_proven = int(float(r.get("daily_universe_membership_proven", 0) or 0)) == 1
        final = membership_by_date.get(ds, pd.DataFrame())
        cls = "AUTHORITY_MISSING"; rank = np.nan; source = ""; proven = 0
        target_name = str(r.get("name", "") or name_map.get(code, ""))
        target_good = _known_common_security_name(target_name)
        if prior_proven:
            cls = "EXACT_CAUSAL_PRIOR_WEEKLY_PROOF"; proven = 1
        elif window_complete_by_date.get(ds, 0) == 1:
            hit = final[final.get("code", pd.Series(dtype=str)).map(_norm_code).eq(code)] if not final.empty else pd.DataFrame()
            if not hit.empty and target_good:
                h = hit.iloc[0]
                rank = pd.to_numeric(pd.Series([h.get("universe_rank")]), errors="coerce").iloc[0]
                source = str(h.get("universe_source", ""))
                partial = full_name_complete_by_date.get(ds, 0) == 0
                if bool(h.get("is_event_expansion", False)):
                    cls = "EXACT_CAUSAL_EVENT_EXPANSION_CONSERVATIVE" if partial else "EXACT_CAUSAL_EVENT_EXPANSION"
                else:
                    cls = "EXACT_CAUSAL_TOP500_CONSERVATIVE" if partial else "EXACT_CAUSAL_TOP500"
                proven = 1
            elif not target_good:
                cls = "AUTHORITY_TARGET_SECURITY_IDENTITY_PARTIAL"
            elif full_name_complete_by_date.get(ds, 0) == 1:
                cls = "NOT_IN_CAUSAL_UNIVERSE"
            else:
                # With unknown competing names, absence from the superset cannot prove a negative
                # historical membership verdict without a complete security-name filter.
                cls = "AUTHORITY_SECURITY_NAME_PARTIAL"
        elif unknown_name_by_date.get(ds, 0) > 0:
            cls = "AUTHORITY_SECURITY_NAME_PARTIAL"
        event_rows.append({
            "version": VERSION, "event_id": r.get("event_id", r.get("cycle_id", "")),
            "cycle_id": r.get("cycle_id", ""), "code": code, "name": r.get("name", ""),
            "restart_date": ds, "targeted_authority_class": cls,
            "targeted_membership_proven": proven, "targeted_universe_rank": rank,
            "targeted_universe_source": source,
            "targeted_date_complete": int(window_complete_by_date.get(ds, 0)),
            "targeted_date_full_name_complete": int(full_name_complete_by_date.get(ds, 0)),
            "target_security_name_proven_good": int(target_good), "research_only": True,
        })

    provider_calls = int(getattr(stock_proxy, "calls", 0) if stock_proxy is not None else 0)
    provider_errors = int(getattr(stock_proxy, "errors", 0) if stock_proxy is not None else 0)
    cache_after_valid = {_fmt_date(d) for d in required_market_days if valid_cached(d)}
    complete_dates = int(sum(window_complete_by_date.values()))
    full_name_complete_dates = int(sum(full_name_complete_by_date.values()))
    budget_exhausted = int(stock_proxy is not None and provider_calls >= max_calls)
    progress = {
        "version": VERSION, "target_signal_dates": len(unique_dates),
        "required_unique_market_dates": len(required_market_days),
        "cache_valid_before": len(cache_before_valid), "cache_valid_after": len(cache_after_valid),
        "new_valid_market_dates": len(cache_after_valid - cache_before_valid),
        "fetched_success_days": len(fetched_success_days), "fetched_empty_days": len(fetched_empty_days),
        "fetch_fail_days": len(fetch_fail_days), "provider_calls": provider_calls,
        "provider_errors": provider_errors, "provider_call_budget": max_calls,
        "budget_exhausted": budget_exhausted, "window_complete_dates": complete_dates,
        "full_name_complete_dates": full_name_complete_dates,
        "remaining_incomplete_dates": max(0, len(unique_dates) - complete_dates),
        "persistent_market_cache_root": str(cache_root), "research_only": True,
    }
    try:
        tmp = progress_path.with_suffix(progress_path.suffix + ".tmp")
        tmp.write_text(json.dumps(progress, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(tmp, progress_path)
    except Exception:
        pass
    _write_csv(out / TARGET_PROGRESS_AUDIT_FILE, pd.DataFrame([progress]))
    meta = {
        "provider_calls": provider_calls, "provider_errors": provider_errors,
        "fetch_enabled": int(fetch_enabled), "provider_import_error": import_error,
        "dates": len(unique_dates), "complete_dates": complete_dates,
        "window_complete_dates": complete_dates, "full_name_complete_dates": full_name_complete_dates,
        "market_dates_loaded": len(cache_after_valid), "cache_valid_before": len(cache_before_valid),
        "cache_valid_after": len(cache_after_valid), "new_valid_market_dates": len(cache_after_valid-cache_before_valid),
        "provider_call_budget": max_calls, "budget_exhausted": budget_exhausted,
        "persistent_progress_path": str(progress_path),
    }
    return pd.DataFrame(event_rows), pd.DataFrame(date_rows), meta

def _episode_overlap_audit(restart_df: pd.DataFrame, transition_df: pd.DataFrame, px_by_code: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Conservative, return-blind near-duplicate audit of adjacent RESTART events."""
    if restart_df is None or restart_df.empty:
        return pd.DataFrame(), pd.DataFrame(), restart_df.copy() if isinstance(restart_df, pd.DataFrame) else pd.DataFrame()
    q = restart_df.copy(); q["restart_date"] = pd.to_datetime(q.get("restart_date"), errors="coerce").dt.normalize(); q["code"] = q.get("code", pd.Series("", index=q.index)).map(_norm_code)
    q = q.sort_values(["code", "restart_date"], kind="stable").reset_index(drop=True)
    parent = list(range(len(q)))
    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb: parent[rb] = ra
    tr = transition_df.copy() if isinstance(transition_df, pd.DataFrame) else pd.DataFrame()
    if not tr.empty:
        tr["date"] = pd.to_datetime(tr.get("date"), errors="coerce").dt.normalize(); tr["code"] = tr.get("code", pd.Series("", index=tr.index)).map(_norm_code)
    pairs: List[Dict[str, Any]] = []
    anchors = ["l0_date", "accum_date", "h1_date", "pullback_date"]
    for code, g in q.groupby("code", sort=False):
        idxs = list(g.index)
        for a, b in zip(idxs, idxs[1:]):
            ra, rb = q.loc[a].to_dict(), q.loc[b].to_dict(); da, db = pd.Timestamp(ra["restart_date"]), pd.Timestamp(rb["restart_date"])
            px = px_by_code.get(code, pd.DataFrame()); gap_sessions = np.nan
            if isinstance(px, pd.DataFrame) and not px.empty:
                gap_sessions = int(((px.index.normalize() > da) & (px.index.normalize() <= db)).sum())
            matches = {k: int(bool(_fmt_date(ra.get(k))) and _fmt_date(ra.get(k)) == _fmt_date(rb.get(k))) for k in anchors}
            anchor_match_count = sum(matches.values())
            fresh_wave = False
            if not tr.empty:
                z = tr[(tr["code"] == code) & (tr["date"] > da) & (tr["date"] <= db) & tr.get("to_state", pd.Series("", index=tr.index)).astype(str).eq("CORE224_WAVE1")]
                fresh_wave = not z.empty
            gs = int(gap_sessions) if pd.notna(gap_sessions) else int((db-da).days)
            if not fresh_wave and gs <= 20 and anchor_match_count >= 3:
                cls = "DEFINITE_SAME_EPISODE_REPEAT"; union(a, b)
            elif not fresh_wave and gs <= 20 and anchor_match_count >= 1:
                cls = "OVERLAP_REVIEW_NO_AUTO_SUPPRESS"
            elif fresh_wave:
                cls = "NEW_EPISODE_FRESH_WAVE1"
            else:
                cls = "NEW_EPISODE_SEPARATED"
            pairs.append({
                "version": VERSION, "code": code, "name": ra.get("name", ""),
                "prior_event_id": ra.get("event_id", ""), "next_event_id": rb.get("event_id", ""),
                "prior_restart_date": _fmt_date(da), "next_restart_date": _fmt_date(db), "gap_trading_sessions": gap_sessions,
                "anchor_match_count": anchor_match_count, **{f"same_{k}": v for k, v in matches.items()},
                "fresh_wave1_between": int(fresh_wave), "episode_relation": cls,
                "auto_suppressed": int(cls == "DEFINITE_SAME_EPISODE_REPEAT"), "research_only": True,
            })
    family_ids: Dict[int, str] = {}
    for i in range(len(q)):
        r = find(i)
        if r not in family_ids:
            seed = str(q.loc[r].get("event_id", q.loc[r].get("cycle_id", r)))
            family_ids[r] = hashlib.sha256((str(q.loc[r].get("code", "")) + "|" + seed).encode()).hexdigest()[:20]
        q.loc[i, "episode_family_id"] = family_ids[r]
    q["event_is_independent_first"] = 0
    keep_idx: List[int] = []
    fam_rows: List[Dict[str, Any]] = []
    for fid, g in q.groupby("episode_family_id", sort=False):
        gs = g.sort_values("restart_date", kind="stable"); keep_idx.append(int(gs.index[0])); q.loc[gs.index[0], "event_is_independent_first"] = 1
        fam_rows.append({"version": VERSION, "episode_family_id": fid, "code": str(gs.iloc[0].get("code", "")), "events_in_family": len(gs), "first_restart_date": _fmt_date(gs.iloc[0].get("restart_date")), "last_restart_date": _fmt_date(gs.iloc[-1].get("restart_date")), "suppressed_events": max(0, len(gs)-1), "research_only": True})
    independent = q.loc[keep_idx].copy().sort_values(["restart_date", "code"], kind="stable").reset_index(drop=True)
    independent["event_id"] = independent["episode_family_id"]
    return pd.DataFrame(pairs), pd.DataFrame(fam_rows), independent


def _weekly_unresolved_rootcause(recon: pd.DataFrame, seeds: pd.DataFrame, state_df: pd.DataFrame, transition_df: pd.DataFrame, out: Path) -> pd.DataFrame:
    if recon is None or recon.empty: return pd.DataFrame()
    unresolved = recon[recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("WEEKLY_ONLY_UNRECONCILED")]
    rows: List[Dict[str, Any]] = []
    seed_codes = set(seeds.get("code", pd.Series(dtype=str)).astype(str)) if isinstance(seeds, pd.DataFrame) and not seeds.empty else set()
    sd = state_df.copy() if isinstance(state_df, pd.DataFrame) else pd.DataFrame(); td = transition_df.copy() if isinstance(transition_df, pd.DataFrame) else pd.DataFrame()
    if not sd.empty: sd["date"] = pd.to_datetime(sd.get("date"), errors="coerce").dt.normalize(); sd["code"] = sd.get("code", pd.Series("", index=sd.index)).map(_norm_code)
    if not td.empty: td["date"] = pd.to_datetime(td.get("date"), errors="coerce").dt.normalize(); td["code"] = td.get("code", pd.Series("", index=td.index)).map(_norm_code)
    for _, rr in unresolved.iterrows():
        code = _norm_code(rr.get("code", "")); wd = pd.to_datetime(rr.get("weekly_restart_date"), errors="coerce")
        px, _ = thesis._read_price_cache_for_code(out, code); pxn = thesis._normalize_lifecycle_price(px)
        cache_has = int(pd.notna(wd) and not pxn.empty and (pxn.index.normalize() == pd.Timestamp(wd).normalize()).any())
        same_state = sd[(sd["code"] == code) & (sd["date"] == pd.Timestamp(wd).normalize())] if pd.notna(wd) and not sd.empty else pd.DataFrame()
        same_trans = td[(td["code"] == code) & (td["date"] == pd.Timestamp(wd).normalize())] if pd.notna(wd) and not td.empty else pd.DataFrame()
        near = td[(td["code"] == code) & td.get("to_state", pd.Series("", index=td.index)).astype(str).eq("CORE224_RESTART")] if not td.empty else pd.DataFrame()
        if pd.notna(wd) and not near.empty:
            near = near.assign(_dist=(near["date"]-pd.Timestamp(wd).normalize()).abs()).sort_values("_dist", kind="stable")
        nrow = near.iloc[0].to_dict() if not near.empty else {}
        anchor_match = 0
        for a, b in [("weekly_l0_date","l0_date"),("weekly_h1_date","h1_date"),("weekly_pullback_date","pullback_date")]:
            anchor_match += int(bool(_fmt_date(rr.get(a))) and _fmt_date(rr.get(a)) == _fmt_date(nrow.get(b)))
        if code not in seed_codes: root = "WEEKLY_CODE_NOT_IN_DAILY_SEED"
        elif not cache_has: root = "PRICE_CACHE_WEEKLY_DATE_MISSING"
        elif not same_state.empty and same_state.get("core224_state", pd.Series("", index=same_state.index)).astype(str).eq("CORE224_RESTART").any(): root = "DAILY_STATE_RESTART_BUT_TRANSITION_NOT_EMITTED"
        elif not same_trans.empty: root = "DAILY_TRANSITION_DIFFERENT_AT_WEEKLY_DATE"
        elif not near.empty and anchor_match > 0: root = "RESTART_DATE_SHIFT_WITH_ANCHOR_DRIFT"
        elif not near.empty: root = "ANCHOR_IDENTITY_DIVERGENCE"
        else: root = "STATE_MACHINE_CONTEXT_DIVERGENCE"
        rows.append({
            "version": VERSION, "code": code, "name": rr.get("name", ""), "weekly_restart_date": _fmt_date(wd),
            "weekly_cycle_id": rr.get("weekly_cycle_id", ""), "seed_present": int(code in seed_codes), "price_cache_weekly_date_present": cache_has,
            "daily_state_at_weekly_date": "|".join(sorted(set(same_state.get("core224_state", pd.Series(dtype=str)).astype(str)))) if not same_state.empty else "",
            "daily_transition_at_weekly_date": "|".join(sorted(set(same_trans.get("to_state", pd.Series(dtype=str)).astype(str)))) if not same_trans.empty else "",
            "nearest_daily_restart_date": _fmt_date(nrow.get("date")), "nearest_anchor_match_count": anchor_match,
            "rootcause_class": root, "research_only": True,
        })
    return pd.DataFrame(rows)


def _path_classification(policy_df: pd.DataFrame, order_df: pd.DataFrame, out: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if policy_df is None or policy_df.empty or order_df is None or order_df.empty: return pd.DataFrame(), pd.DataFrame()
    od = order_df.set_index(["event_id", "stop_lens"], drop=False)
    px_cache: Dict[str, pd.DataFrame] = {}; rows: List[Dict[str, Any]] = []
    for _, r in policy_df.iterrows():
        rec = r.to_dict(); key=(str(rec.get("event_id", rec.get("cycle_id", ""))), str(rec.get("stop_lens", "")))
        if key not in od.index: continue
        o = od.loc[key]
        if isinstance(o, pd.DataFrame): o=o.iloc[0]
        status=str(rec.get("lifecycle_status", "")); pre_success=bool(int(o.get("profit5_high_before_stop",0) or 0) or int(o.get("h1_close_rebreak_before_stop",0) or 0))
        post_recovery=post_h1=post_p5=0
        if status=="STRUCTURE_STOP" and not pre_success:
            code=_norm_code(rec.get("code", "")); sig=pd.to_datetime(rec.get("signal_date"),errors="coerce"); stopd=pd.to_datetime(rec.get("end_date"),errors="coerce")
            if code not in px_cache:
                pr,_=thesis._read_price_cache_for_code(out,code); px_cache[code]=thesis._normalize_lifecycle_price(pr)
            px=px_cache.get(code,pd.DataFrame()); avg=pd.to_numeric(pd.Series([rec.get("avg_cost_final")]),errors="coerce").iloc[0]; h1=pd.to_numeric(pd.Series([rec.get("h1_high")]),errors="coerce").iloc[0]
            if pd.notna(sig) and pd.notna(stopd) and not px.empty:
                z=px[(px.index.normalize()>pd.Timestamp(stopd).normalize()) & (px.index.normalize()<=pd.Timestamp(sig).normalize()+pd.Timedelta(days=100))].head(60)
                if not z.empty and pd.notna(avg) and float(avg)>0:
                    post_recovery=int((pd.to_numeric(z["close"],errors="coerce")>=float(avg)).any()); post_p5=int((pd.to_numeric(z["high"],errors="coerce")>=float(avg)*1.05).any())
                if not z.empty and pd.notna(h1): post_h1=int((pd.to_numeric(z["close"],errors="coerce")>=float(h1)).any())
        if status=="STRUCTURE_STOP":
            if pre_success: cls="PROFIT_THEN_BREAK"
            elif post_recovery or post_h1 or post_p5: cls="EARLY_STOP_RECOVERY"
            else: cls="TRUE_FAILURE"
        elif pre_success: cls="CLEAN_WIN"
        elif status=="SURVIVED_60D_OBSERVATION_END": cls="CAPITAL_LOCK"
        else: cls="RIGHT_CENSORED"
        rows.append({
            "version":VERSION,"event_id":key[0],"cycle_id":rec.get("cycle_id",""),"code":rec.get("code",""),"name":rec.get("name",""),"signal_date":rec.get("signal_date",""),"stop_lens":key[1],
            "path_class":cls,"pre_stop_success":int(pre_success),"post_stop_avg_recovery":post_recovery,"post_stop_h1_close_rebreak":post_h1,"post_stop_profit5_high":post_p5,
            "daily_universe_membership_proven":int(float(rec.get("daily_universe_membership_proven",0) or 0)),"research_only":True,
        })
    df=pd.DataFrame(rows); sums:List[Dict[str,Any]]=[]
    if not df.empty:
        scopes=[("ALL_RESEARCH",df),("EXACT_CAUSAL_ASOF",df[_num(df,"daily_universe_membership_proven",0).eq(1)])]
        for scope,base in scopes:
            if base.empty: continue
            for lens,g in base.groupby("stop_lens",dropna=False):
                total=len(g)
                for cls,cg in g.groupby("path_class",dropna=False):
                    sums.append({"version":VERSION,"scope":scope,"stop_lens":lens,"path_class":cls,"events":len(cg),"pct":len(cg)/max(1,total)*100.0,"total_events":total,"research_only":True})
    return df,pd.DataFrame(sums)


def run_daily_episode_replay(output_dir: str | Path, state: pd.DataFrame, payloads: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    t0 = time.monotonic(); out = Path(output_dir or "reports"); out.mkdir(parents=True, exist_ok=True)
    cohort, st, en = _cohort_bounds()
    enabled = str(os.getenv("V25_DAILY_EPISODE_REPLAY_ENABLE", "1")).strip().lower() not in {"0", "false", "off", "no"}
    if not enabled:
        ready = pd.DataFrame([{"version": VERSION, "status": "DISABLED", "research_only": True}]); _write_csv(out / READINESS_FILE, ready)
        return {"status": "DISABLED", "readiness": ready, "report": ""}

    seeds = _build_seed_ledger(state, st, en)
    weekly_seed_authority = _weekly_seed_authority(state, st, en)
    weekly_ledger = _weekly_restart_ledger(state, st, en)
    weekly_restart_exact = {(str(r.get("code", "")), str(r.get("weekly_restart_date", ""))) for _, r in weekly_ledger.iterrows()}
    exact_authority = _exact_authority_map(out)
    state_exact_codes: set[Tuple[str, str]] = set()
    if state is not None and not state.empty:
        sq = state.copy(); sq["signal_date"] = pd.to_datetime(sq.get("signal_date"), errors="coerce").dt.normalize(); sq["code"] = sq.get("code", pd.Series("", index=sq.index)).map(_norm_code)
        state_exact_codes = {(pd.Timestamp(d).strftime("%Y-%m-%d"), c) for d, c in zip(sq["signal_date"], sq["code"]) if pd.notna(d) and c}

    codes = seeds["code"].astype(str).tolist() if not seeds.empty else []
    global_amount = thesis.load_cached_amount_panel(out, en, codes, max_files=max(800, int(os.getenv("V25_DAILY_AMOUNT_CACHE_MAX_FILES", "800")))) if codes else pd.DataFrame()
    compact_states: List[pd.DataFrame] = []; transitions: List[pd.DataFrame] = []; invariants: List[pd.DataFrame] = []
    raw_restart_rows: List[Dict[str, Any]] = []; seed_runtime_rows: List[Dict[str, Any]] = []
    px_follow_by_code: Dict[str, pd.DataFrame] = {}
    continuous_input_by_code: Dict[str, pd.DataFrame] = {}
    cfg_life = thesis.Core224LifecycleConfig(max_follow_days=max(20, int(float(os.getenv("V25_LIFECYCLE_MAX_DAYS", "60")))))
    evaluated = 0; price_missing = 0; amount_ready_codes = 0
    price_fingerprint_parts: List[str] = []

    for _, sr in seeds.iterrows():
        seed = sr.to_dict(); code = str(seed.get("code", "")); px_raw, cache_meta = thesis._read_price_cache_for_code(out, code); px_follow = thesis._normalize_lifecycle_price(px_raw)
        if px_raw is None or px_raw.empty or px_follow.empty:
            price_missing += 1; seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": "PRICE_CACHE_MISSING", "actual_amount_days": 0}); continue
        px_follow_by_code[code] = px_follow
        amount_auth = _merge_amount_authority(out, code, global_amount); q = thesis._overlay_actual_amount(px_raw, code, amount_auth); q.index = pd.to_datetime(q.index, errors="coerce"); q = q[q.index.notna()].sort_index(); q_detect = q[q.index.normalize() <= en].copy()
        if q_detect.empty:
            seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": "NO_PRICE_BEFORE_COHORT_END", "actual_amount_days": 0}); continue
        continuous_input_by_code[code] = q_detect.copy()
        actual_days = int(pd.to_numeric(q_detect.get("amount_is_actual", pd.Series(0, index=q_detect.index)), errors="coerce").fillna(0).eq(1).sum())
        fp_cols=[c for c in ["open","high","low","close","volume","Amount","actual_amount","amount_is_actual"] if c in q_detect.columns]
        price_fingerprint_parts.append(code+":"+_stable_frame_hash(q_detect.reset_index().rename(columns={q_detect.index.name or "index":"date"}), ["date"]+fp_cols))
        if actual_days >= thesis.Core224Config().actual_amount_min_history_days: amount_ready_codes += 1
        try: daily, ev, inv = thesis.evaluate_core224(q_detect)
        except Exception as exc:
            seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": f"EVAL_ERROR:{type(exc).__name__}", "actual_amount_days": actual_days}); continue
        evaluated += 1; seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": "PASS", "actual_amount_days": actual_days})
        if not daily.empty:
            daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize(); dz = daily[daily["date"].between(st, en, inclusive="both")].copy(); keep = dz.get("core224_state", pd.Series("", index=dz.index)).astype(str).ne("NONE") | _num(dz, "core224_transition", 0).eq(1); dz = dz[keep]
            if not dz.empty:
                dz.insert(0, "version", VERSION); dz.insert(1, "code", code); dz.insert(2, "name", seed.get("name", "")); dz["seed_reason"] = seed.get("seed_reason", ""); dz["research_only"] = True; compact_states.append(dz)
        if not ev.empty:
            ev["date"] = pd.to_datetime(ev["date"], errors="coerce").dt.normalize(); ez = ev[ev["date"].between(st, en, inclusive="both")].copy()
            if not ez.empty:
                ez = _decorate_event(ez, seed); transitions.append(ez); rz = ez[ez.get("to_state", pd.Series("", index=ez.index)).astype(str).eq("CORE224_RESTART")]
                for _, rr in rz.iterrows():
                    rec = rr.to_dict(); ds = _fmt_date(rec.get("date")); key = (code, ds); auth = exact_authority.get(ds, {}); exact_code = int((ds, code) in state_exact_codes); exact_causal = int(exact_code == 1 and int(auth.get("complete", 0)) == 1)
                    authority_label = "EXACT_CAUSAL_ASOF_PROVEN" if exact_causal else ("EXACT_SIGNAL_DATE_FALLBACK" if exact_code and int(auth.get("fallback_used", 0)) == 1 else "DAILY_UNIVERSE_NOT_PROVEN")
                    rec.update({"restart_date": ds, "weekly_restart_observed": int(key in weekly_restart_exact), "restart_discovery": "WEEKLY_ALREADY_OBSERVED" if key in weekly_restart_exact else "RECOVERED_BETWEEN_WEEKLY_SNAPSHOTS", "exact_materialized_code_present": exact_code, "historical_asof_complete_exact_date": int(auth.get("complete", 0)), "historical_asof_status_exact_date": str(auth.get("status", "NO_EXACT_WEEKLY_AUTHORITY")), "daily_universe_authority": authority_label, "daily_universe_membership_proven": exact_causal, "research_only": True})
                    raw_restart_rows.append(rec)
        if not inv.empty:
            inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.normalize(); iz = inv[inv["date"].between(st, en, inclusive="both")].copy()
            if not iz.empty: invariants.append(_decorate_event(iz, seed))

    seed_runtime = pd.DataFrame(seed_runtime_rows); state_df = pd.concat(compact_states, ignore_index=True, sort=False) if compact_states else pd.DataFrame(); transition_df = pd.concat(transitions, ignore_index=True, sort=False) if transitions else pd.DataFrame(); invariant_df = pd.concat(invariants, ignore_index=True, sort=False) if invariants else pd.DataFrame()
    raw_restart_df = pd.DataFrame(raw_restart_rows)
    if not raw_restart_df.empty:
        raw_restart_df["restart_date"] = pd.to_datetime(raw_restart_df["restart_date"], errors="coerce").dt.strftime("%Y-%m-%d"); raw_restart_df = raw_restart_df.sort_values(["restart_date", "code"], kind="stable")
    cycle_restart_df, dedup_audit = _dedupe_restart_events(raw_restart_df)
    overlap_audit, family_summary, restart_df = _episode_overlap_audit(cycle_restart_df, transition_df, px_follow_by_code)
    seed_causality = _daily_seed_causality_audit(restart_df, state, st, en)
    if not restart_df.empty and not seed_causality.empty:
        sc = seed_causality[[c for c in ["event_id","latest_weekly_snapshot","next_weekly_snapshot","weekly_seed_causal_eligible","weekly_seed_reason","weekly_state_at_authorization","seed_causality_status"] if c in seed_causality.columns]].copy()
        restart_df = restart_df.merge(sc, on="event_id", how="left")
        restart_df["weekly_seed_causal_eligible"] = pd.to_numeric(restart_df.get("weekly_seed_causal_eligible"), errors="coerce").fillna(0).astype(int)
    recon = _reconcile_weekly_daily(weekly_ledger, restart_df)
    unresolved_rootcause = _weekly_unresolved_rootcause(recon, seed_runtime, state_df, transition_df, out)
    context_parity = _context_parity_audit(recon, weekly_ledger, state_df, transition_df, seed_runtime, out)
    context_state_trace, context_state_trace_summary = _weekly_daily_full_state_trace(recon, state, seed_runtime, out, global_amount, st, en)
    weekly_900bar_parity = _weekly_900bar_contract_parity(recon, state, out, global_amount)
    exact_shard_proof = _exact_shard_restart_input_proof(weekly_ledger, payloads, continuous_input_by_code)

    # V25.4.7 Targeted Causal Authority: the RESTART set is frozen before any authority lookup.
    # This lane can only classify an already-found event; it cannot create/delete CORE224 signals.
    targeted_events, targeted_dates, targeted_meta = _targeted_authority_for_restarts(out, restart_df, px_by_code=px_follow_by_code)
    if not restart_df.empty and not targeted_events.empty:
        te = targeted_events[[c for c in ["event_id","targeted_authority_class","targeted_membership_proven","targeted_universe_rank","targeted_universe_source","targeted_date_complete","targeted_date_full_name_complete","target_security_name_proven_good"] if c in targeted_events.columns]].copy()
        restart_df = restart_df.merge(te, on="event_id", how="left")
        prior_proven = pd.to_numeric(restart_df.get("daily_universe_membership_proven"), errors="coerce").fillna(0).astype(int)
        targeted_proven = pd.to_numeric(restart_df.get("targeted_membership_proven"), errors="coerce").fillna(0).astype(int)
        restart_df["daily_universe_membership_proven"] = np.maximum(prior_proven, targeted_proven)
        cls = restart_df.get("targeted_authority_class", pd.Series("", index=restart_df.index)).fillna("").astype(str)
        old_auth = restart_df.get("daily_universe_authority", pd.Series("", index=restart_df.index)).fillna("").astype(str)
        restart_df["daily_universe_authority"] = np.where(cls.ne("") & ~cls.eq("AUTHORITY_MISSING"), cls, old_auth)
    if not restart_df.empty:
        restart_df["weekly_seed_causal_eligible"] = pd.to_numeric(restart_df.get("weekly_seed_causal_eligible"), errors="coerce").fillna(0).astype(int)
        restart_df["policy_training_eligible"] = (restart_df["weekly_seed_causal_eligible"].eq(1) & pd.to_numeric(restart_df.get("daily_universe_membership_proven"), errors="coerce").fillna(0).eq(1)).astype(int)
    targeted_authority_class_summary = _target_authority_class_summary(targeted_events)

    lifecycle_signals: List[Dict[str, Any]] = []; lifecycle_policy: List[Dict[str, Any]] = []; lifecycle_fills: List[Dict[str, Any]] = []; lifecycle_horizons: List[Dict[str, Any]] = []; single_rows: List[Dict[str, Any]] = []
    for _, rr in restart_df.iterrows():
        rec = rr.to_dict(); code = _norm_code(rec.get("code", "")); px = px_follow_by_code.get(code)
        if px is None or px.empty:
            pr, _ = thesis._read_price_cache_for_code(out, code); px = thesis._normalize_lifecycle_price(pr); px_follow_by_code[code] = px
        sig, pol, ff, hh = _daily_lifecycle_for_restart(out, rec, px, cohort, cfg_life)
        if not sig: continue
        common = {"cycle_id": rec.get("cycle_id", ""), "event_id": rec.get("event_id", rec.get("cycle_id", "")), "daily_universe_authority": rec.get("daily_universe_authority", ""), "daily_universe_membership_proven": rec.get("daily_universe_membership_proven", 0),
                  "weekly_seed_causal_eligible": rec.get("weekly_seed_causal_eligible", 0), "weekly_seed_reason": rec.get("weekly_seed_reason", ""),
                  "policy_training_eligible": rec.get("policy_training_eligible", 0), "restart_discovery": rec.get("restart_discovery", "")}
        sig.update(common); lifecycle_signals.append(sig)
        for x in pol: x.update(common)
        for x in ff: x.update(common)
        for x in hh: x.update(common)
        lifecycle_policy.extend(pol); lifecycle_fills.extend(ff); lifecycle_horizons.extend(hh)
        lenses = thesis._stop_lenses(float(rec.get("l0_low", np.nan)), float(rec.get("h1_high", np.nan)), float(sig.get("pullback_low", np.nan)), cfg_life)
        for lens, stop in lenses.items(): single_rows.append(_simulate_single_entry_one(rec, px, lens, stop, cfg_life))

    life_signal_df = pd.DataFrame(lifecycle_signals); life_policy_df = pd.DataFrame(lifecycle_policy); life_fill_df = pd.DataFrame(lifecycle_fills); life_horizon_df = pd.DataFrame(lifecycle_horizons); single_df = pd.DataFrame(single_rows)
    life_stop = thesis._policy_stop_summary(life_policy_df, str(cohort.get("cohort_id", "COHORT_ALL"))) if not life_policy_df.empty else pd.DataFrame()
    order_df = _build_event_order(life_policy_df, out); order_summary = _order_summary(order_df); risk_df, risk_summary = _risk_parity(life_policy_df, life_fill_df, single_df)
    risk_fill_group_summary = _risk_parity_fill_group_summary(risk_df)
    path_df, path_summary = _path_classification(life_policy_df, order_df, out)
    stop_lens_compare = _stop_lens_path_compare(path_summary)
    stop_lens_risk_tradeoff = _stop_lens_risk_tradeoff(risk_df, path_summary)
    exit_shadow_df, exit_shadow_summary = _exit_policy_shadow(restart_df, px_follow_by_code, cfg_life)
    stop_exit_policy_matrix = _stop_exit_policy_matrix(exit_shadow_summary)
    execution_causality_df, execution_causality_summary = _execution_causality_shadow(restart_df, px_follow_by_code, cfg_life)
    input_fingerprint = _input_fingerprint_regression(
        out, state, seed_runtime, weekly_seed_authority, state_df, transition_df, restart_df,
        price_fingerprint_parts, targeted_dates, exact_shard_proof,
    )
    # V25.4.7 lock authority: parent-merged-cache parity remains diagnostic only.  A weekly
    # RESTART is policy-reconciled only when its exact shard-local evaluator input replays cleanly.
    weekly_unresolved_pre = int(recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("WEEKLY_ONLY_UNRECONCILED").sum()) if not recon.empty else 0
    unresolved_keys = set()
    if not recon.empty:
        _uq = recon[recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("WEEKLY_ONLY_UNRECONCILED")]
        unresolved_keys = {(_norm_code(r.get("code","")), _fmt_date(r.get("weekly_restart_date"))) for _, r in _uq.iterrows()}
    exact_shard_replay_pass = int(_num(exact_shard_proof, "exact_shard_replay_pass", 0).eq(1).sum()) if not exact_shard_proof.empty else 0
    exact_shard_same_input_nondeterminism = int(_num(exact_shard_proof, "same_input_nondeterminism", 0).eq(1).sum()) if not exact_shard_proof.empty else 0
    exact_shard_cross_lane_explained = int(_num(exact_shard_proof, "cross_lane_explained", 0).eq(1).sum()) if not exact_shard_proof.empty else 0
    exact_shard_unresolved_lane_explained = 0
    if unresolved_keys and not exact_shard_proof.empty:
        for _, _pr in exact_shard_proof.iterrows():
            _k = (_norm_code(_pr.get("code","")), _fmt_date(_pr.get("weekly_restart_date")))
            if _k in unresolved_keys and int(float(_pr.get("exact_shard_replay_pass",0) or 0)) == 1 and int(float(_pr.get("cross_lane_explained",0) or 0)) == 1 and int(float(_pr.get("same_input_nondeterminism",0) or 0)) == 0:
                exact_shard_unresolved_lane_explained += 1
    policy_unexplained_pre = max(0, weekly_unresolved_pre - exact_shard_unresolved_lane_explained)
    source_input_changed_pre = int(_num(input_fingerprint, "source_input_changed", 0).sum()) if not input_fingerprint.empty else 0

    cutoff_ts = pd.to_datetime(_policy_lock_cutoff(), errors="coerce")
    _train = restart_df.copy() if isinstance(restart_df, pd.DataFrame) else pd.DataFrame()
    if not _train.empty and pd.notna(cutoff_ts):
        _train["_lock_signal_date"] = pd.to_datetime(_train.get("restart_date", _train.get("date")), errors="coerce").dt.normalize()
        _train = _train[_train["_lock_signal_date"].le(pd.Timestamp(cutoff_ts).normalize())].copy()
    training_restart_events = len(_train)
    training_causal_proven = int(_num(_train, "daily_universe_membership_proven", 0).eq(1).sum()) if not _train.empty else 0
    training_policy_eligible = int(_num(_train, "policy_training_eligible", 0).eq(1).sum()) if not _train.empty else 0
    training_authority_dates = 0; training_authority_complete_dates = 0
    if not _train.empty:
        _train_dates = set(pd.to_datetime(_train.get("restart_date"), errors="coerce").dropna().dt.normalize())
        training_authority_dates = len(_train_dates)
        if not targeted_dates.empty:
            _td = targeted_dates.copy()
            _td["_signal_date"] = pd.to_datetime(_td.get("signal_date", _td.get("restart_date", _td.get("date"))), errors="coerce").dt.normalize()
            _td = _td[_td["_signal_date"].isin(_train_dates)]
            _complete_col = "targeted_date_complete" if "targeted_date_complete" in _td.columns else "complete"
            training_authority_complete_dates = int(_num(_td, _complete_col, 0).eq(1).sum())
    policy_lock_manifest, policy_lock_meta = _policy_lock_manifest(
        out, policy_unexplained_pre, source_input_changed_pre, training_restart_events,
        training_causal_proven, training_policy_eligible,
        training_authority_dates, training_authority_complete_dates, len(invariant_df),
        len(weekly_ledger), exact_shard_replay_pass, exact_shard_same_input_nondeterminism,
    )
    forward_oos_events, forward_oos_policy, forward_oos_summary, forward_oos_immutability, forward_oos_meta = _forward_oos_locked_ledgers(out, restart_df, px_follow_by_code, cfg_life, policy_lock_manifest, policy_lock_meta)
    manual = _stratified_manual(restart_df, order_df, life_policy_df, recon, 60)

    raw_n = len(raw_restart_df); cycle_unique_n = len(cycle_restart_df); unique_n = len(restart_df)
    suppressed_n = int(_num(dedup_audit, "suppressed_repeat_rows", 0).sum()) if not dedup_audit.empty else 0
    overlap_suppressed_n = max(0, cycle_unique_n - unique_n)
    overlap_review_pairs = int(overlap_audit.get("episode_relation", pd.Series(dtype=str)).astype(str).eq("OVERLAP_REVIEW_NO_AUTO_SUPPRESS").sum()) if not overlap_audit.empty else 0
    recovered_n = int(restart_df.get("restart_discovery", pd.Series(dtype=str)).astype(str).eq("RECOVERED_BETWEEN_WEEKLY_SNAPSHOTS").sum()) if not restart_df.empty else 0
    exact_causal_n = int(_num(restart_df, "daily_universe_membership_proven", 0).eq(1).sum()) if not restart_df.empty else 0; eligible_n = int(_num(life_signal_df, "lifecycle_eligible", 0).eq(1).sum()) if not life_signal_df.empty else 0; inv_fail = len(invariant_df)
    targeted_complete_dates = int(targeted_meta.get("complete_dates", 0) or 0); targeted_full_name_complete_dates = int(targeted_meta.get("full_name_complete_dates", 0) or 0); targeted_provider_calls = int(targeted_meta.get("provider_calls", 0) or 0); targeted_provider_errors = int(targeted_meta.get("provider_errors", 0) or 0)
    targeted_cache_before = int(targeted_meta.get("cache_valid_before", 0) or 0); targeted_cache_after = int(targeted_meta.get("cache_valid_after", 0) or 0); targeted_new_cache = int(targeted_meta.get("new_valid_market_dates", 0) or 0); targeted_budget = int(targeted_meta.get("provider_call_budget", 0) or 0); targeted_budget_exhausted = int(targeted_meta.get("budget_exhausted", 0) or 0)
    weekly_exact = int(recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("EXACT_DATE_MATCH").sum()) if not recon.empty else 0; weekly_shift = int(recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("SAME_CYCLE_DATE_SHIFT").sum()) if not recon.empty else 0; weekly_unresolved = int(recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("WEEKLY_ONLY_UNRECONCILED").sum()) if not recon.empty else 0
    weekly_explained = int(_num(context_parity, "explained", 0).eq(1).sum()) if not context_parity.empty else 0
    weekly_contract_explained = int(_num(weekly_900bar_parity, "contract_explained", 0).eq(1).sum()) if not weekly_900bar_parity.empty else 0
    # Legacy context/900bar audits remain diagnostic. Exact shard-local replay is lock authority.
    weekly_exact_shard_lane_explained = int(exact_shard_unresolved_lane_explained)
    weekly_unexplained = max(0, weekly_unresolved - weekly_exact_shard_lane_explained)
    weekly_reconciled = weekly_exact + weekly_shift + weekly_exact_shard_lane_explained
    authority_rate = exact_causal_n / max(1, unique_n); price_cache_coverage = evaluated / max(1, len(seeds))
    if len(seeds) == 0: status = "WARMUP_NO_WEEKLY_EPISODE_SEEDS"
    elif inv_fail: status = "INVALID_DAILY_SEQUENCE_INVARIANT"
    elif evaluated == 0: status = "INVALID_NO_PRICE_CACHE_EVALUATION"
    elif unique_n == 0: status = "WARMUP_NO_DAILY_RESTART"
    elif weekly_unexplained > 0: status = "RESEARCH_SAMPLE_READY_RECONCILIATION_REVIEW"
    elif eligible_n < 30: status = "RESEARCH_SAMPLE_WARMUP"
    elif authority_rate < 0.70: status = "RESEARCH_SAMPLE_READY_UNIVERSE_AUTHORITY_WARMUP"
    else: status = "DATA_READY_RESEARCH_ONLY"

    readiness = pd.DataFrame([{
        "version": VERSION, "status": status, "cohort_id": cohort.get("cohort_id", ""), "cohort_start": st.strftime("%Y-%m-%d"), "cohort_end": en.strftime("%Y-%m-%d"),
        "seed_codes": len(seeds), "evaluated_codes": evaluated, "price_cache_missing_codes": price_missing, "price_cache_coverage_pct": price_cache_coverage*100.0, "actual_amount_ready20_codes": amount_ready_codes,
        "daily_state_rows": len(state_df), "daily_transition_rows": len(transition_df), "daily_invariant_fail_rows": inv_fail,
        "weekly_restart_cycles": len(weekly_ledger), "weekly_restart_exact_date_matches": weekly_exact, "weekly_restart_same_cycle_date_shifts": weekly_shift, "weekly_restart_explained_context_divergences": weekly_explained, "weekly_restart_reconciled": weekly_reconciled, "weekly_restart_unreconciled_raw": weekly_unresolved, "weekly_restart_unexplained": weekly_unexplained,
        "weekly_unresolved_rootcause_rows": len(unresolved_rootcause), "context_parity_rows": len(context_parity),
        "weekly_restart_900bar_contract_explained": weekly_contract_explained, "weekly_900bar_parity_rows": len(weekly_900bar_parity),
        "weekly_seed_authority_rows": len(weekly_seed_authority),
        "daily_seed_causal_eligible_events": int(_num(restart_df, "weekly_seed_causal_eligible", 0).eq(1).sum()) if not restart_df.empty else 0,
        "daily_seed_causal_ineligible_events": int(_num(restart_df, "weekly_seed_causal_eligible", 0).ne(1).sum()) if not restart_df.empty else 0,
        "policy_training_eligible_events": int(_num(restart_df, "policy_training_eligible", 0).eq(1).sum()) if not restart_df.empty else 0,
        "training_restart_events_before_cutoff": int(training_restart_events),
        "training_universe_proven_before_cutoff": int(training_causal_proven),
        "training_policy_eligible_before_cutoff": int(training_policy_eligible),
        "exact_shard_restart_input_expected": len(weekly_ledger),
        "exact_shard_restart_input_proof_rows": len(exact_shard_proof),
        "exact_shard_restart_input_replay_pass": int(exact_shard_replay_pass),
        "exact_shard_cross_lane_explained": int(exact_shard_cross_lane_explained),
        "exact_shard_unresolved_lane_explained": int(weekly_exact_shard_lane_explained),
        "exact_shard_same_input_nondeterminism": int(exact_shard_same_input_nondeterminism),
        "context_state_trace_rows": len(context_state_trace), "context_state_trace_summary_rows": len(context_state_trace_summary),
        "raw_daily_restart_rows": raw_n, "cycle_first_restart_events": cycle_unique_n, "daily_restart_events": unique_n,
        "suppressed_repeat_restart_rows": suppressed_n, "episode_overlap_suppressed_events": overlap_suppressed_n, "episode_overlap_review_pairs": overlap_review_pairs,
        "recovered_restart_events": recovered_n, "exact_causal_asof_restart_events": exact_causal_n, "daily_universe_authority_rate_pct": authority_rate*100.0,
        "targeted_authority_dates": int(targeted_meta.get("dates",0) or 0), "targeted_authority_complete_dates": targeted_complete_dates,
        "targeted_authority_full_name_complete_dates": targeted_full_name_complete_dates,
        "targeted_authority_cache_valid_before": targeted_cache_before, "targeted_authority_cache_valid_after": targeted_cache_after,
        "targeted_authority_new_valid_market_dates": targeted_new_cache, "targeted_authority_provider_call_budget": targeted_budget,
        "targeted_authority_budget_exhausted": targeted_budget_exhausted,
        "targeted_authority_provider_calls": targeted_provider_calls, "targeted_authority_provider_errors": targeted_provider_errors,
        "daily_lifecycle_signals": len(life_signal_df), "daily_lifecycle_eligible": eligible_n, "event_order_rows": len(order_df), "path_class_rows": len(path_df), "risk_parity_rows": len(risk_df),
        "risk_parity_fill_group_rows": len(risk_fill_group_summary), "stop_lens_compare_rows": len(stop_lens_compare), "stop_lens_risk_tradeoff_rows": len(stop_lens_risk_tradeoff), "exit_shadow_rows": len(exit_shadow_df), "exit_shadow_summary_rows": len(exit_shadow_summary),
        "stop_exit_policy_matrix_rows": len(stop_exit_policy_matrix), "execution_causality_rows": len(execution_causality_df), "execution_causality_summary_rows": len(execution_causality_summary),
        "targeted_authority_class_summary_rows": len(targeted_authority_class_summary), "execution_roundtrip_cost_bps_assumption": _execution_cost_bps(),
        "input_fingerprint_changed_components": int(_num(input_fingerprint, "changed_vs_previous", 0).sum()) if not input_fingerprint.empty else 0,
        "input_fingerprint_source_changed_components": int(_num(input_fingerprint, "source_input_changed", 0).sum()) if not input_fingerprint.empty else 0,
        "policy_lock_status": str(policy_lock_meta.get("status","UNKNOWN")), "policy_lock_digest": str(policy_lock_meta.get("digest","")), "policy_lock_cutoff_date": str(policy_lock_meta.get("cutoff","")),
        "forward_oos_status": str(forward_oos_meta.get("status","UNKNOWN")), "forward_oos_events": int(forward_oos_meta.get("events",0) or 0), "forward_oos_causal_eligible": int(forward_oos_meta.get("eligible",0) or 0), "forward_oos_finalized_policy_rows": int(forward_oos_meta.get("finalized",0) or 0), "forward_oos_primary_finalized_trades": int(forward_oos_meta.get("primary_finalized",0) or 0), "forward_oos_immutability_conflicts": int(forward_oos_meta.get("immutability_conflicts",0) or 0),
        "provider_calls": targeted_provider_calls, "core_daily_replay_provider_calls": 0, "close_times_volume_substitution": 0,
        "live_logic_changed": False, "real_order_changed": False, "research_only": True, "elapsed_sec": round(time.monotonic()-t0,3)
    }])

    for fn, df in [
        (SEED_FILE, seed_runtime),(STATE_FILE,state_df),(TRANSITION_FILE,transition_df),(RAW_RESTART_FILE,raw_restart_df),(RESTART_FILE,restart_df),
        (DEDUP_AUDIT_FILE,dedup_audit),(EPISODE_OVERLAP_FILE,overlap_audit),(EPISODE_FAMILY_FILE,family_summary),(RECON_FILE,recon),(UNRESOLVED_ROOTCAUSE_FILE,unresolved_rootcause),(CONTEXT_PARITY_FILE,context_parity),
        (CONTEXT_STATE_TRACE_FILE,context_state_trace),(CONTEXT_STATE_TRACE_SUMMARY_FILE,context_state_trace_summary),(WEEKLY_900BAR_PARITY_FILE,weekly_900bar_parity),
        (WEEKLY_SEED_AUTHORITY_FILE,weekly_seed_authority),(DAILY_SEED_CAUSALITY_FILE,seed_causality),(SHARD_RESTART_INPUT_PROOF_FILE,exact_shard_proof),
        (TARGET_AUTHORITY_FILE,targeted_events),(TARGET_AUTHORITY_DATE_FILE,targeted_dates),(TARGET_AUTHORITY_CLASS_SUMMARY_FILE,targeted_authority_class_summary),(INPUT_FINGERPRINT_FILE,input_fingerprint),(INVARIANT_FILE,invariant_df),(MANUAL_FILE,manual),
        (LIFECYCLE_SIGNAL_FILE,life_signal_df),(LIFECYCLE_POLICY_FILE,life_policy_df),(LIFECYCLE_FILL_FILE,life_fill_df),(LIFECYCLE_HORIZON_FILE,life_horizon_df),(LIFECYCLE_STOP_FILE,life_stop),
        (ORDER_FILE,order_df),(ORDER_SUMMARY_FILE,order_summary),(PATH_CLASS_FILE,path_df),(PATH_CLASS_SUMMARY_FILE,path_summary),(STOP_LENS_PATH_COMPARE_FILE,stop_lens_compare),(STOP_LENS_RISK_TRADEOFF_FILE,stop_lens_risk_tradeoff),
        (SINGLE_POLICY_FILE,single_df),(RISK_PARITY_FILE,risk_df),(RISK_PARITY_SUMMARY_FILE,risk_summary),(RISK_PARITY_FILL_GROUP_FILE,risk_fill_group_summary),
        (EXIT_SHADOW_FILE,exit_shadow_df),(EXIT_SHADOW_SUMMARY_FILE,exit_shadow_summary),(STOP_EXIT_POLICY_MATRIX_FILE,stop_exit_policy_matrix),
        (EXECUTION_CAUSALITY_FILE,execution_causality_df),(EXECUTION_CAUSALITY_SUMMARY_FILE,execution_causality_summary),
        (POLICY_LOCK_MANIFEST_FILE,policy_lock_manifest),(FORWARD_OOS_EVENT_FILE,forward_oos_events),(FORWARD_OOS_POLICY_FILE,forward_oos_policy),(FORWARD_OOS_SUMMARY_FILE,forward_oos_summary),(FORWARD_OOS_IMMUTABILITY_FILE,forward_oos_immutability),(READINESS_FILE,readiness)
    ]: _write_csv(out / fn, df)

    pb = life_stop[life_stop.get("stop_lens", pd.Series(dtype=str)).astype(str).eq("PB_LOW")].iloc[0] if not life_stop.empty and (life_stop.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW").any() else pd.Series(dtype=object)
    pbo = order_summary[(order_summary.get("scope", pd.Series(dtype=str)).astype(str)=="ALL_RESEARCH") & (order_summary.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW")].iloc[0] if not order_summary.empty and ((order_summary.get("scope", pd.Series(dtype=str)).astype(str)=="ALL_RESEARCH") & (order_summary.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW")).any() else pd.Series(dtype=object)
    pbr = risk_summary[(risk_summary.get("scope", pd.Series(dtype=str)).astype(str)=="ALL_RESEARCH") & (risk_summary.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW")].iloc[0] if not risk_summary.empty and ((risk_summary.get("scope", pd.Series(dtype=str)).astype(str)=="ALL_RESEARCH") & (risk_summary.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW")).any() else pd.Series(dtype=object)
    rootcause_text = ",".join(f"{k}:{v}" for k,v in unresolved_rootcause.get("rootcause_class", pd.Series(dtype=str)).astype(str).value_counts().to_dict().items()) if not unresolved_rootcause.empty else "NONE"
    lines=[
        HEADER,
        f"📌 {VERSION} · status={status} · CORE224 daily detection은 cache-only/provider 0 · frozen RESTART 날짜의 targeted causal authority만 cache-first bounded fetch",
        f"🧭 seed {len(seeds)}종목 → daily 평가 {evaluated} · 가격cache 누락 {price_missing} · actual Amount 20일-ready 종목 {amount_ready_codes}",
        f"🔁 RESTART raw {raw_n} → cycle-first {cycle_unique_n} → episode-independent {unique_n} · exact-cycle 반복억제 {suppressed_n} · overlap 자동억제 {overlap_suppressed_n} · overlap REVIEW {overlap_review_pairs}",
        f"🔗 weekly↔daily: exact {weekly_exact}/{len(weekly_ledger)} · same-cycle shift {weekly_shift} · exact-shard lane-explained {weekly_exact_shard_lane_explained} · unexplained {weekly_unexplained} · legacy-context {weekly_explained} · rootcause {rootcause_text}",
        f"🛂 weekly-seed causal gate: active {int(_num(restart_df,'weekly_seed_causal_eligible',0).eq(1).sum()) if not restart_df.empty else 0}/{unique_n} · retrospective-only excluded {int(_num(restart_df,'weekly_seed_causal_eligible',0).ne(1).sum()) if not restart_df.empty else 0} · universe proven {exact_causal_n} · strict policy-training {int(_num(restart_df,'policy_training_eligible',0).eq(1).sum()) if not restart_df.empty else 0} · cutoff-train strict {training_policy_eligible}/{training_restart_events}",
        f"🧷 exact shard RESTART input proof: pass {exact_shard_replay_pass}/{len(weekly_ledger)} · cross-lane explained {exact_shard_cross_lane_explained} (unresolved {weekly_exact_shard_lane_explained}) · same-input nondeterminism {exact_shard_same_input_nondeterminism}",
        f"🧵 unresolved full-state trace: {len(context_state_trace_summary)}건 · first-divergence exposed {int(context_state_trace_summary.get('trace_status',pd.Series(dtype=str)).astype(str).eq('FIRST_DIVERGENCE_EXPOSED').sum()) if not context_state_trace_summary.empty else 0} · 신호 강제 reconcile 0",
        f"📦 독립 RESTART {unique_n} 중 주간사이 복원 {recovered_n} · causal policy-proof {exact_causal_n}/{unique_n} · targeted window-complete {targeted_complete_dates}/{int(targeted_meta.get('dates',0) or 0)} · full-name-complete {targeted_full_name_complete_dates}/{int(targeted_meta.get('dates',0) or 0)}",
        f"♻️ targeted authority resume: cache {targeted_cache_before}→{targeted_cache_after} (+{targeted_new_cache}) · provider {targeted_provider_calls}/{targeted_budget} calls · errors {targeted_provider_errors} · budget_exhausted {targeted_budget_exhausted}",
        f"🧬 입력 fingerprint: changed {int(_num(input_fingerprint, 'changed_vs_previous', 0).sum()) if not input_fingerprint.empty else 0} · source-changed {int(_num(input_fingerprint, 'source_input_changed', 0).sum()) if not input_fingerprint.empty else 0} · schema {FINGERPRINT_SCHEMA_VERSION}",
        f"🔒 policy lock: {policy_lock_meta.get('status')} · cutoff {policy_lock_meta.get('cutoff')} · PRIMARY {policy_lock_meta.get('primary_policy_id')} · auto-switch 0",
        f"🧪 forward OOS/PAPER: {forward_oos_meta.get('status')} · events {forward_oos_meta.get('events',0)} · causal-eligible {forward_oos_meta.get('eligible',0)} · PRIMARY-finalized {forward_oos_meta.get('primary_finalized',0)} · finalized-policy-rows {forward_oos_meta.get('finalized',0)} · immutability-conflict {forward_oos_meta.get('immutability_conflicts',0)}",
        f"🧪 daily lifecycle {len(life_signal_df)} · eligible {eligible_n} · 구조손절/30-30-40/20·40·60일 규칙 동일 · 조건 튜닝 0",
        "⚠️ targeted authority는 이미 고정된 RESTART만 분류하며 CORE224 신호를 만들거나 삭제하지 않습니다. NOT_IN_CAUSAL_UNIVERSE와 AUTHORITY_MISSING을 분리합니다.",
        "⚠️ 동일 일봉에서 stop과 목표가가 모두 닿을 수 있는 경우 전략결과는 기존대로 STOP_FIRST 보수처리하고 collision 원장에 별도 표시합니다.",
        "⚠️ 한계: 주간 seed/base lens에 한 번도 걸리지 않은 초단기 전체 사이클은 발견하지 않습니다. episode-resolution 감사이며 전체시장 일별 universe 백테스트가 아닙니다."
    ]
    if not targeted_authority_class_summary.empty:
        tier_counts = targeted_authority_class_summary.groupby("authority_proof_tier", dropna=False)["events"].sum().to_dict()
        lines.append("🔐 causal authority tiers: " + " · ".join(f"{k} {int(v)}" for k,v in tier_counts.items()))
    if not context_state_trace_summary.empty:
        for _, tr in context_state_trace_summary.iterrows():
            lines.append(f"🧵 divergence {tr.get('code')} {tr.get('weekly_restart_date')}: first {tr.get('first_divergence_date') or '-'} · weekly {tr.get('first_divergence_weekly_state') or '-'} ↔ daily {tr.get('first_divergence_daily_state') or '-'} · anchors {tr.get('first_divergence_anchor_match_count')}/{tr.get('first_divergence_anchor_present_count')}")
    if not weekly_900bar_parity.empty:
        for _, tr in weekly_900bar_parity.iterrows():
            lines.append(f"🧪 legacy parent-cache 900bar diagnostic {tr.get('code')} {tr.get('weekly_restart_date')}: {tr.get('contract_status')} · weekly {tr.get('weekly_state','-')} ↔ parent-cache {tr.get('contract_900bar_state','-')} · anchors {tr.get('anchor_match_count',0)}/{tr.get('anchor_present_count',0)} · bars {tr.get('contract_history_rows',0)}")
    if not input_fingerprint.empty:
        ch=input_fingerprint[_num(input_fingerprint,'changed_vs_previous',0).eq(1)]
        mig=input_fingerprint[input_fingerprint.get('classification',pd.Series(dtype=str)).astype(str).eq('SCHEMA_MIGRATION_BASELINE')]
        lines.append("🧬 fingerprint components changed: " + (", ".join(ch.get('component',pd.Series(dtype=str)).astype(str).tolist()) if not ch.empty else "NONE") + (f" · schema-migration-baseline {','.join(mig.get('component',pd.Series(dtype=str)).astype(str).tolist())}" if not mig.empty else ""))
    if not pb.empty: lines.append(f"🎯 Daily PB_LOW: n{int(pb.get('signals',0) or 0)} · 2차체결 {float(pb.get('entry2_fill_rate_pct',np.nan)):.1f}% · 3차체결 {float(pb.get('entry3_fill_rate_pct',np.nan)):.1f}% · 구조손절 {float(pb.get('structure_stop_rate_pct',np.nan)):.1f}%")
    if not pbo.empty: lines.append(f"⏱️ PB_LOW 선후: 평단회복-before-stop {float(pbo.get('avg_recovery_before_stop_pct',np.nan)):.1f}% · H1종가-before-stop {float(pbo.get('h1_close_before_stop_pct',np.nan)):.1f}% · +5고가-before-stop {float(pbo.get('profit5_high_before_stop_pct',np.nan)):.1f}% · same-day collision {float(pbo.get('same_day_collision_pct',np.nan)):.1f}%")
    if not pbr.empty: lines.append(f"⚖️ PB_LOW 동일 1R 진단: SINGLE 중앙R {float(pbr.get('median_single_r',np.nan)):+.2f} ↔ 30/30/40 중앙R {float(pbr.get('median_scale_r',np.nan)):+.2f} · scale-single {float(pbr.get('median_scale_minus_single_r',np.nan)):+.2f}R · 자동 정책선택 금지")
    if not stop_lens_compare.empty:
        z=stop_lens_compare[stop_lens_compare.get('scope',pd.Series(dtype=str)).astype(str).eq('ALL_RESEARCH')]
        for _,r in z.iterrows():
            lines.append(f"🧭 {r.get('stop_lens')} 경로: CLEAN {float(r.get('clean_win_pct',0)):.1f}% · PROFIT→BREAK {float(r.get('profit_then_break_pct',0)):.1f}% · EARLY_STOP→RECOVERY {float(r.get('early_stop_recovery_pct',0)):.1f}% · TRUE_FAIL {float(r.get('true_failure_pct',0)):.1f}% · LOCK {float(r.get('capital_lock_pct',0)):.1f}% · CENSORED {float(r.get('right_censored_pct',0)):.1f}%")
    if not stop_lens_risk_tradeoff.empty:
        for scope in ["ALL_RESEARCH", "EXACT_CAUSAL_ASOF"]:
            z=stop_lens_risk_tradeoff[stop_lens_risk_tradeoff.get("scope",pd.Series(dtype=str)).astype(str).eq(scope)]
            for _,r in z.iterrows():
                lines.append(f"⚖️ {scope} {r.get('stop_lens')} risk: n{int(r.get('events',0) or 0)} · SINGLE medR {float(r.get('median_single_r',np.nan)):+.2f}/trim {float(r.get('trim10_single_r',np.nan)):+.2f} · SCALE medR {float(r.get('median_scale_r',np.nan)):+.2f} · risk {float(r.get('median_single_risk_pct',np.nan)):.1f}% · MAE {float(r.get('median_single_mae_pct',np.nan)):.1f}% · hold {float(r.get('median_single_holding_days',np.nan)):.1f}D")
    if not risk_fill_group_summary.empty:
        z=risk_fill_group_summary[(risk_fill_group_summary.get('scope',pd.Series(dtype=str)).astype(str)=='ALL_RESEARCH') & (risk_fill_group_summary.get('stop_lens',pd.Series(dtype=str)).astype(str)=='PB_LOW')]
        for fg in ['ENTRY2_FILLED','ENTRY3_FILLED']:
            x=z[z.get('fill_group',pd.Series(dtype=str)).astype(str).eq(fg)]
            if not x.empty:
                r=x.iloc[0]; lines.append(f"⚖️ PB_LOW {fg}: n{int(r.get('events',0) or 0)} · SINGLE 중앙R {float(r.get('median_single_r',np.nan)):+.2f} ↔ SCALE {float(r.get('median_scale_r',np.nan)):+.2f} · edge {float(r.get('median_scale_minus_single_r',np.nan)):+.2f}R · 평단개선 {float(r.get('median_avg_cost_improvement_pct',np.nan)):.2f}%")
    if not exit_shadow_summary.empty:
        z=exit_shadow_summary[(exit_shadow_summary.get('scope',pd.Series(dtype=str)).astype(str)=='ALL_RESEARCH') & (exit_shadow_summary.get('stop_lens',pd.Series(dtype=str)).astype(str)=='PB_LOW')]
        for policy in ['STRUCTURE_HOLD','PLUS5_FULL_EXIT','H1_CLOSE_PB_TRAIL']:
            x=z[z.get('exit_policy',pd.Series(dtype=str)).astype(str).eq(policy)]
            if not x.empty:
                r=x.iloc[0]; lines.append(f"🚦 PB_LOW exit SHADOW {policy}: n{int(r.get('events',0) or 0)} · 중앙R {float(r.get('median_r',np.nan)):+.2f} · 절사R {float(r.get('trim10_r',np.nan)):+.2f} · 양수 {float(r.get('positive_r_rate_pct',np.nan)):.1f}% · 보유중앙 {float(r.get('median_holding_days',np.nan)):.1f}D")
        for lens in ['FIB_61_8','FIB_78_6','L0_STRUCTURE']:
            x=exit_shadow_summary[(exit_shadow_summary.get('scope',pd.Series(dtype=str)).astype(str)=='ALL_RESEARCH') & (exit_shadow_summary.get('stop_lens',pd.Series(dtype=str)).astype(str)==lens) & (exit_shadow_summary.get('exit_policy',pd.Series(dtype=str)).astype(str)=='WIDE_H1_PB_TRAIL')]
            if not x.empty:
                r=x.iloc[0]; lines.append(f"🚦 {lens}→H1후 PB trail: n{int(r.get('events',0) or 0)} · 중앙R {float(r.get('median_r',np.nan)):+.2f} · 절사R {float(r.get('trim10_r',np.nan)):+.2f} · 양수 {float(r.get('positive_r_rate_pct',np.nan)):.1f}% · 보유중앙 {float(r.get('median_holding_days',np.nan)):.1f}D")
        zc=exit_shadow_summary[exit_shadow_summary.get('scope',pd.Series(dtype=str)).astype(str).eq('EXACT_CAUSAL_ASOF')]
        for lens,policy in [('PB_LOW','PLUS5_FULL_EXIT'),('FIB_61_8','WIDE_H1_PB_TRAIL'),('FIB_78_6','WIDE_H1_PB_TRAIL'),('L0_STRUCTURE','WIDE_H1_PB_TRAIL')]:
            x=zc[(zc.get('stop_lens',pd.Series(dtype=str)).astype(str)==lens)&(zc.get('exit_policy',pd.Series(dtype=str)).astype(str)==policy)]
            if not x.empty:
                r=x.iloc[0]; lines.append(f"🔒 CAUSAL {lens}/{policy}: n{int(r.get('events',0) or 0)} · 중앙R {float(r.get('median_r',np.nan)):+.2f} · 절사R {float(r.get('trim10_r',np.nan)):+.2f} · 양수 {float(r.get('positive_r_rate_pct',np.nan)):.1f}%")
    if not stop_exit_policy_matrix.empty:
        zc = stop_exit_policy_matrix[(stop_exit_policy_matrix.get("scope",pd.Series(dtype=str)).astype(str)=="EXACT_CAUSAL_ASOF") & (stop_exit_policy_matrix.get("normalized_exit_policy",pd.Series(dtype=str)).astype(str)=="PLUS5_FULL_EXIT")]
        for _,r in zc.iterrows():
            lines.append(f"🧮 CAUSAL stop×+5 {r.get('stop_lens')}: n{int(r.get('events',0) or 0)} · medR {float(r.get('median_r',np.nan)):+.2f} · trim {float(r.get('trim10_r',np.nan)):+.2f} · 양수 {float(r.get('positive_r_rate_pct',np.nan)):.1f}% · hold {float(r.get('median_holding_days',np.nan)):.1f}D")
    if not execution_causality_summary.empty:
        ex = execution_causality_summary[(execution_causality_summary.get("scope",pd.Series(dtype=str)).astype(str)=="EXACT_CAUSAL_ASOF") & (execution_causality_summary.get("entry_mode",pd.Series(dtype=str)).astype(str)=="D1_OPEN_CAUSAL") & (execution_causality_summary.get("exit_policy",pd.Series(dtype=str)).astype(str)=="PLUS5_FULL_EXIT")]
        for lens in ["PB_LOW","FIB_61_8","FIB_78_6","L0_STRUCTURE"]:
            x=ex[ex.get("stop_lens",pd.Series(dtype=str)).astype(str).eq(lens)]
            if not x.empty:
                r=x.iloc[0]; lines.append(f"🕘 CAUSAL D+1 OPEN {lens}/+5: exec {int(r.get('executed_trades',0) or 0)}/{int(r.get('signals',0) or 0)} · net medR {float(r.get('median_net_r',np.nan)):+.2f} · trim {float(r.get('trim10_net_r',np.nan)):+.2f} · 양수 {float(r.get('positive_net_r_rate_pct',np.nan)):.1f}% · gap-cancel {float(r.get('entry_cancel_gap_below_stop_rate_pct',np.nan)):.1f}% · cost {float(r.get('roundtrip_cost_bps_assumption',np.nan)):.0f}bp")
        exs = execution_causality_summary[(execution_causality_summary.get("scope",pd.Series(dtype=str)).astype(str)=="POLICY_TRAINING_CAUSAL") & (execution_causality_summary.get("entry_mode",pd.Series(dtype=str)).astype(str)=="D1_OPEN_CAUSAL") & (execution_causality_summary.get("exit_policy",pd.Series(dtype=str)).astype(str)=="PLUS5_FULL_EXIT")]
        for lens in ["PB_LOW","FIB_61_8","FIB_78_6","L0_STRUCTURE"]:
            x=exs[exs.get("stop_lens",pd.Series(dtype=str)).astype(str).eq(lens)]
            if not x.empty:
                r=x.iloc[0]; lines.append(f"🕘 STRICT POLICY-TRAIN D+1 OPEN {lens}/+5: exec {int(r.get('executed_trades',0) or 0)}/{int(r.get('signals',0) or 0)} · net medR {float(r.get('median_net_r',np.nan)):+.2f} · trim {float(r.get('trim10_net_r',np.nan)):+.2f} · 양수 {float(r.get('positive_net_r_rate_pct',np.nan)):.1f}% · gap-cancel {float(r.get('entry_cancel_gap_below_stop_rate_pct',np.nan)):.1f}%")
    if not forward_oos_summary.empty:
        for _,r in forward_oos_summary.iterrows():
            lines.append(f"📈 FORWARD {r.get('policy_role')}/{r.get('policy_id')}: events {int(r.get('forward_events',0) or 0)} · eligible {int(r.get('causal_eligible_events',0) or 0)} · finalized {int(r.get('finalized_trades',0) or 0)} · medR {float(r.get('median_net_r',np.nan)):+.2f} · trim {float(r.get('trim10_net_r',np.nan)):+.2f} · cumR {float(r.get('cumulative_trade_sequence_r',0)):+.2f} · DD {float(r.get('max_drawdown_trade_sequence_r',0)):.2f}R")
    lines.append(f"⏱️ daily episode replay elapsed {time.monotonic()-t0:.1f}s"); lines.append(f"- CSV: {RESTART_FILE} · {WEEKLY_SEED_AUTHORITY_FILE} · {DAILY_SEED_CAUSALITY_FILE} · {SHARD_RESTART_INPUT_PROOF_FILE} · {INPUT_FINGERPRINT_FILE} · {POLICY_LOCK_MANIFEST_FILE} · {FORWARD_OOS_SUMMARY_FILE} · {READINESS_FILE}")
    report="\n".join(lines); (out/REPORT_FILE).write_text(report+"\n",encoding="utf-8")
    return {"status":status,"seed":seed_runtime,"weekly_seed_authority":weekly_seed_authority,"seed_causality":seed_causality,"exact_shard_proof":exact_shard_proof,"state":state_df,"transitions":transition_df,"raw_restarts":raw_restart_df,"cycle_restarts":cycle_restart_df,"restarts":restart_df,"dedup_audit":dedup_audit,"episode_overlap":overlap_audit,"episode_family":family_summary,"reconciliation":recon,"unresolved_rootcause":unresolved_rootcause,"context_parity":context_parity,"context_state_trace":context_state_trace,"context_state_trace_summary":context_state_trace_summary,"weekly_900bar_parity":weekly_900bar_parity,"input_fingerprint":input_fingerprint,"policy_lock_manifest":policy_lock_manifest,"policy_lock_meta":policy_lock_meta,"forward_oos_events":forward_oos_events,"forward_oos_policy":forward_oos_policy,"forward_oos_summary":forward_oos_summary,"forward_oos_immutability":forward_oos_immutability,"forward_oos_meta":forward_oos_meta,"targeted_authority":targeted_events,"targeted_authority_dates":targeted_dates,"targeted_authority_class_summary":targeted_authority_class_summary,"invariants":invariant_df,"manual":manual,"lifecycle_signal":life_signal_df,"lifecycle_policy":life_policy_df,"lifecycle_fill":life_fill_df,"lifecycle_horizon":life_horizon_df,"lifecycle_stop":life_stop,"event_order":order_df,"event_order_summary":order_summary,"path_class":path_df,"path_class_summary":path_summary,"stop_lens_path_compare":stop_lens_compare,"stop_lens_risk_tradeoff":stop_lens_risk_tradeoff,"single_policy":single_df,"risk_parity":risk_df,"risk_parity_summary":risk_summary,"risk_parity_fill_group_summary":risk_fill_group_summary,"exit_shadow":exit_shadow_df,"exit_shadow_summary":exit_shadow_summary,"stop_exit_policy_matrix":stop_exit_policy_matrix,"execution_causality":execution_causality_df,"execution_causality_summary":execution_causality_summary,"readiness":readiness,"report":report}

def force_report(output_dir: str | Path = "reports") -> str:
    p = Path(output_dir or "reports") / REPORT_FILE
    return p.read_text(encoding="utf-8") if p.exists() else ""


__all__ = ["VERSION", "RESEARCH_ONLY", "run_daily_episode_replay", "force_report"]
