from __future__ import annotations

"""V25.4 CORE224 daily episode replay (research-only, cache-only).

Purpose
-------
The 2-year ALL workflow samples signal dates weekly for runtime reasons.  That is ideal for
finding candidate CORE224 episodes, but a short-lived HEALTHY_PULLBACK -> RESTART transition
can happen between two weekly snapshots.  This module does **not** widen the universe, tune a
threshold, or call a provider.  It takes only names already seeded by the weekly CORE224
sidecar/base lens, replays their existing cached daily price + verified trading-value history,
and records any chronologically valid daily RESTART transitions plus the same structural
scale-in lifecycle used by V25.3.1.

Daily replay is therefore an episode-resolution audit, not proof that the stock belonged to the
causal TOP500 on every recovered day.  Exact daily universe authority is reported separately and
never imputed from a neighboring weekly snapshot.
"""

import os
import time
import hashlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import original_thesis_reconstruction as thesis

VERSION = "V73.3.6.6.25.4.1"
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
REPORT_FILE = "v73_v25_core224_daily_episode_report.txt"

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
    base = {"version": VERSION, "event_id": event_id, "cycle_id": restart.get("cycle_id", ""), "code": code, "name": restart.get("name", ""), "stop_lens": stop_name, "research_only": True}
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
            "single_status": sr.get("single_status", ""), "scale_status": r.get("lifecycle_status", ""),
            "single_planned_risk_pct": sr.get("planned_risk_pct", np.nan), "scale_planned_risk_pct": scale_risk * 100.0 if np.isfinite(scale_risk) else np.nan,
            "single_final_r_multiple": single_r, "scale_final_r_multiple": scale_r,
            "scale_minus_single_r": scale_r - single_r if np.isfinite(scale_r) and np.isfinite(single_r) else np.nan,
            "entry_count": r.get("entry_count", np.nan), "deployed_weight": r.get("deployed_weight", np.nan),
            "avg_cost_improvement_vs_entry1_pct": (1.0 - avg / entry1) * 100.0 if np.isfinite(avg) and np.isfinite(entry1) and entry1 > 0 else np.nan,
            "single_capital_per_1R": (100.0 / float(pd.to_numeric(pd.Series([sr.get("planned_risk_pct")]), errors="coerce").iloc[0])) if pd.notna(pd.to_numeric(pd.Series([sr.get("planned_risk_pct")]), errors="coerce").iloc[0]) and float(pd.to_numeric(pd.Series([sr.get("planned_risk_pct")]), errors="coerce").iloc[0]) > 0 else np.nan,
            "scale_capital_per_1R": 1.0 / scale_risk if np.isfinite(scale_risk) and scale_risk > 0 else np.nan,
            "risk_parity_note": "R_MULTIPLE_ONLY_NOT_POSITION_SIZING_RECOMMENDATION", "research_only": True,
        })
    df = pd.DataFrame(rows)
    sums: List[Dict[str, Any]] = []
    if not df.empty:
        scopes = [("ALL_RESEARCH", df), ("EXACT_CAUSAL_ASOF", df[_num(df, "daily_universe_membership_proven", 0).eq(1)])]
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


def run_daily_episode_replay(output_dir: str | Path, state: pd.DataFrame) -> Dict[str, Any]:
    t0 = time.monotonic(); out = Path(output_dir or "reports"); out.mkdir(parents=True, exist_ok=True)
    cohort, st, en = _cohort_bounds()
    enabled = str(os.getenv("V25_DAILY_EPISODE_REPLAY_ENABLE", "1")).strip().lower() not in {"0", "false", "off", "no"}
    if not enabled:
        ready = pd.DataFrame([{"version": VERSION, "status": "DISABLED", "research_only": True}]); _write_csv(out / READINESS_FILE, ready)
        return {"status": "DISABLED", "readiness": ready, "report": ""}

    seeds = _build_seed_ledger(state, st, en)
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
    cfg_life = thesis.Core224LifecycleConfig(max_follow_days=max(20, int(float(os.getenv("V25_LIFECYCLE_MAX_DAYS", "60")))))
    evaluated = 0; price_missing = 0; amount_ready_codes = 0

    for _, sr in seeds.iterrows():
        seed = sr.to_dict(); code = str(seed.get("code", "")); px_raw, cache_meta = thesis._read_price_cache_for_code(out, code); px_follow = thesis._normalize_lifecycle_price(px_raw)
        if px_raw is None or px_raw.empty or px_follow.empty:
            price_missing += 1; seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": "PRICE_CACHE_MISSING", "actual_amount_days": 0}); continue
        px_follow_by_code[code] = px_follow
        amount_auth = _merge_amount_authority(out, code, global_amount); q = thesis._overlay_actual_amount(px_raw, code, amount_auth); q.index = pd.to_datetime(q.index, errors="coerce"); q = q[q.index.notna()].sort_index(); q_detect = q[q.index.normalize() <= en].copy()
        if q_detect.empty:
            seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": "NO_PRICE_BEFORE_COHORT_END", "actual_amount_days": 0}); continue
        actual_days = int(pd.to_numeric(q_detect.get("amount_is_actual", pd.Series(0, index=q_detect.index)), errors="coerce").fillna(0).eq(1).sum())
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
    restart_df, dedup_audit = _dedupe_restart_events(raw_restart_df)
    recon = _reconcile_weekly_daily(weekly_ledger, restart_df)

    lifecycle_signals: List[Dict[str, Any]] = []; lifecycle_policy: List[Dict[str, Any]] = []; lifecycle_fills: List[Dict[str, Any]] = []; lifecycle_horizons: List[Dict[str, Any]] = []; single_rows: List[Dict[str, Any]] = []
    for _, rr in restart_df.iterrows():
        rec = rr.to_dict(); code = _norm_code(rec.get("code", "")); px = px_follow_by_code.get(code)
        if px is None or px.empty:
            pr, _ = thesis._read_price_cache_for_code(out, code); px = thesis._normalize_lifecycle_price(pr); px_follow_by_code[code] = px
        sig, pol, ff, hh = _daily_lifecycle_for_restart(out, rec, px, cohort, cfg_life)
        if not sig: continue
        common = {"cycle_id": rec.get("cycle_id", ""), "event_id": rec.get("event_id", rec.get("cycle_id", "")), "daily_universe_authority": rec.get("daily_universe_authority", ""), "daily_universe_membership_proven": rec.get("daily_universe_membership_proven", 0), "restart_discovery": rec.get("restart_discovery", "")}
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
    manual = _stratified_manual(restart_df, order_df, life_policy_df, recon, 60)

    raw_n = len(raw_restart_df); unique_n = len(restart_df); suppressed_n = int(_num(dedup_audit, "suppressed_repeat_rows", 0).sum()) if not dedup_audit.empty else 0
    recovered_n = int(restart_df.get("restart_discovery", pd.Series(dtype=str)).astype(str).eq("RECOVERED_BETWEEN_WEEKLY_SNAPSHOTS").sum()) if not restart_df.empty else 0
    exact_causal_n = int(_num(restart_df, "daily_universe_membership_proven", 0).eq(1).sum()) if not restart_df.empty else 0; eligible_n = int(_num(life_signal_df, "lifecycle_eligible", 0).eq(1).sum()) if not life_signal_df.empty else 0; inv_fail = len(invariant_df)
    weekly_exact = int(recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("EXACT_DATE_MATCH").sum()) if not recon.empty else 0; weekly_shift = int(recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("SAME_CYCLE_DATE_SHIFT").sum()) if not recon.empty else 0; weekly_unresolved = int(recon.get("reconciliation_status", pd.Series(dtype=str)).astype(str).eq("WEEKLY_ONLY_UNRECONCILED").sum()) if not recon.empty else 0
    weekly_reconciled = weekly_exact + weekly_shift; authority_rate = exact_causal_n / max(1, unique_n); price_cache_coverage = evaluated / max(1, len(seeds))
    if len(seeds) == 0: status = "WARMUP_NO_WEEKLY_EPISODE_SEEDS"
    elif inv_fail: status = "INVALID_DAILY_SEQUENCE_INVARIANT"
    elif evaluated == 0: status = "INVALID_NO_PRICE_CACHE_EVALUATION"
    elif unique_n == 0: status = "WARMUP_NO_DAILY_RESTART"
    elif weekly_unresolved > 0: status = "RESEARCH_SAMPLE_READY_RECONCILIATION_REVIEW"
    elif eligible_n < 30: status = "RESEARCH_SAMPLE_WARMUP"
    elif authority_rate < 0.70: status = "RESEARCH_SAMPLE_READY_UNIVERSE_AUTHORITY_WARMUP"
    else: status = "DATA_READY_RESEARCH_ONLY"

    readiness = pd.DataFrame([{"version": VERSION, "status": status, "cohort_id": cohort.get("cohort_id", ""), "cohort_start": st.strftime("%Y-%m-%d"), "cohort_end": en.strftime("%Y-%m-%d"), "seed_codes": len(seeds), "evaluated_codes": evaluated, "price_cache_missing_codes": price_missing, "price_cache_coverage_pct": price_cache_coverage*100.0, "actual_amount_ready20_codes": amount_ready_codes, "daily_state_rows": len(state_df), "daily_transition_rows": len(transition_df), "daily_invariant_fail_rows": inv_fail, "weekly_restart_cycles": len(weekly_ledger), "weekly_restart_exact_date_matches": weekly_exact, "weekly_restart_same_cycle_date_shifts": weekly_shift, "weekly_restart_reconciled": weekly_reconciled, "weekly_restart_unreconciled": weekly_unresolved, "raw_daily_restart_rows": raw_n, "daily_restart_events": unique_n, "suppressed_repeat_restart_rows": suppressed_n, "recovered_restart_events": recovered_n, "exact_causal_asof_restart_events": exact_causal_n, "daily_universe_authority_rate_pct": authority_rate*100.0, "daily_lifecycle_signals": len(life_signal_df), "daily_lifecycle_eligible": eligible_n, "event_order_rows": len(order_df), "risk_parity_rows": len(risk_df), "provider_calls": 0, "close_times_volume_substitution": 0, "live_logic_changed": False, "real_order_changed": False, "research_only": True, "elapsed_sec": round(time.monotonic()-t0,3)}])

    for fn, df in [(SEED_FILE, seed_runtime),(STATE_FILE,state_df),(TRANSITION_FILE,transition_df),(RAW_RESTART_FILE,raw_restart_df),(RESTART_FILE,restart_df),(DEDUP_AUDIT_FILE,dedup_audit),(RECON_FILE,recon),(INVARIANT_FILE,invariant_df),(MANUAL_FILE,manual),(LIFECYCLE_SIGNAL_FILE,life_signal_df),(LIFECYCLE_POLICY_FILE,life_policy_df),(LIFECYCLE_FILL_FILE,life_fill_df),(LIFECYCLE_HORIZON_FILE,life_horizon_df),(LIFECYCLE_STOP_FILE,life_stop),(ORDER_FILE,order_df),(ORDER_SUMMARY_FILE,order_summary),(SINGLE_POLICY_FILE,single_df),(RISK_PARITY_FILE,risk_df),(RISK_PARITY_SUMMARY_FILE,risk_summary),(READINESS_FILE,readiness)]: _write_csv(out / fn, df)

    pb = life_stop[life_stop.get("stop_lens", pd.Series(dtype=str)).astype(str).eq("PB_LOW")].iloc[0] if not life_stop.empty and (life_stop.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW").any() else pd.Series(dtype=object)
    pbo = order_summary[(order_summary.get("scope", pd.Series(dtype=str)).astype(str)=="ALL_RESEARCH") & (order_summary.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW")].iloc[0] if not order_summary.empty and ((order_summary.get("scope", pd.Series(dtype=str)).astype(str)=="ALL_RESEARCH") & (order_summary.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW")).any() else pd.Series(dtype=object)
    pbr = risk_summary[(risk_summary.get("scope", pd.Series(dtype=str)).astype(str)=="ALL_RESEARCH") & (risk_summary.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW")].iloc[0] if not risk_summary.empty and ((risk_summary.get("scope", pd.Series(dtype=str)).astype(str)=="ALL_RESEARCH") & (risk_summary.get("stop_lens", pd.Series(dtype=str)).astype(str)=="PB_LOW")).any() else pd.Series(dtype=object)
    lines=[HEADER, f"📌 {VERSION} · status={status} · weekly 전체 TOP500 재계산 없이 기존 cache만 사용 · provider call 0", f"🧭 seed {len(seeds)}종목 → daily 평가 {evaluated} · 가격cache 누락 {price_missing} · actual Amount 20일-ready 종목 {amount_ready_codes}", f"🔁 RESTART raw {raw_n} → 독립 first-cycle {unique_n} · 반복억제 {suppressed_n} · weekly exact {weekly_exact}/{len(weekly_ledger)} · same-cycle shift {weekly_shift} · 미해결 {weekly_unresolved}", f"📦 독립 RESTART {unique_n} 중 주간사이 복원 {recovered_n} · exact causal-asof {exact_causal_n}/{unique_n} · ALL은 구조연구, EXACT_CAUSAL만 정책검증 분모", f"🧪 daily lifecycle {len(life_signal_df)} · eligible {eligible_n} · 구조손절/30-30-40/20·40·60일 규칙 동일 · 조건 튜닝 0", "⚠️ 동일 일봉에서 stop과 목표가가 모두 닿을 수 있는 경우 전략결과는 기존대로 STOP_FIRST 보수처리하고 collision 원장에 별도 표시합니다.", "⚠️ 한계: 주간 seed/base lens에 한 번도 걸리지 않은 초단기 전체 사이클은 발견하지 않습니다. episode-resolution 감사이며 전체시장 일별 universe 백테스트가 아닙니다."]
    if not pb.empty: lines.append(f"🎯 Daily PB_LOW: n{int(pb.get('signals',0) or 0)} · 2차체결 {float(pb.get('entry2_fill_rate_pct',np.nan)):.1f}% · 3차체결 {float(pb.get('entry3_fill_rate_pct',np.nan)):.1f}% · 구조손절 {float(pb.get('structure_stop_rate_pct',np.nan)):.1f}%")
    if not pbo.empty: lines.append(f"⏱️ PB_LOW 선후: 평단회복-before-stop {float(pbo.get('avg_recovery_before_stop_pct',np.nan)):.1f}% · H1종가-before-stop {float(pbo.get('h1_close_before_stop_pct',np.nan)):.1f}% · +5고가-before-stop {float(pbo.get('profit5_high_before_stop_pct',np.nan)):.1f}% · same-day collision {float(pbo.get('same_day_collision_pct',np.nan)):.1f}%")
    if not pbr.empty: lines.append(f"⚖️ PB_LOW 동일 1R 진단: SINGLE 중앙R {float(pbr.get('median_single_r',np.nan)):+.2f} ↔ 30/30/40 중앙R {float(pbr.get('median_scale_r',np.nan)):+.2f} · scale-single {float(pbr.get('median_scale_minus_single_r',np.nan)):+.2f}R · 자동 정책선택 금지")
    lines.append(f"⏱️ daily episode replay elapsed {time.monotonic()-t0:.1f}s"); lines.append(f"- CSV: {RESTART_FILE} · {RECON_FILE} · {DEDUP_AUDIT_FILE} · {ORDER_FILE} · {RISK_PARITY_FILE} · {READINESS_FILE}")
    report="\n".join(lines); (out/REPORT_FILE).write_text(report+"\n",encoding="utf-8")
    return {"status":status,"seed":seed_runtime,"state":state_df,"transitions":transition_df,"raw_restarts":raw_restart_df,"restarts":restart_df,"dedup_audit":dedup_audit,"reconciliation":recon,"invariants":invariant_df,"manual":manual,"lifecycle_signal":life_signal_df,"lifecycle_policy":life_policy_df,"lifecycle_fill":life_fill_df,"lifecycle_horizon":life_horizon_df,"lifecycle_stop":life_stop,"event_order":order_df,"event_order_summary":order_summary,"single_policy":single_df,"risk_parity":risk_df,"risk_parity_summary":risk_summary,"readiness":readiness,"report":report}

def force_report(output_dir: str | Path = "reports") -> str:
    p = Path(output_dir or "reports") / REPORT_FILE
    return p.read_text(encoding="utf-8") if p.exists() else ""


__all__ = ["VERSION", "RESEARCH_ONLY", "run_daily_episode_replay", "force_report"]
