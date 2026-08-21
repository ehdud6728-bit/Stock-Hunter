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
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import original_thesis_reconstruction as thesis

VERSION = "V73.3.6.6.25.4"
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


def run_daily_episode_replay(output_dir: str | Path, state: pd.DataFrame) -> Dict[str, Any]:
    t0 = time.monotonic(); out = Path(output_dir or "reports"); out.mkdir(parents=True, exist_ok=True)
    cohort, st, en = _cohort_bounds()
    enabled = str(os.getenv("V25_DAILY_EPISODE_REPLAY_ENABLE", "1")).strip().lower() not in {"0", "false", "off", "no"}
    if not enabled:
        ready = pd.DataFrame([{"version": VERSION, "status": "DISABLED", "research_only": True}])
        _write_csv(out / READINESS_FILE, ready)
        return {"status": "DISABLED", "readiness": ready, "report": ""}

    seeds = _build_seed_ledger(state, st, en)
    weekly_restart = _weekly_restart_dates(state, st, en)
    exact_authority = _exact_authority_map(out)
    state_exact_codes: set[Tuple[str, str]] = set()
    if state is not None and not state.empty:
        sq = state.copy(); sq["signal_date"] = pd.to_datetime(sq.get("signal_date"), errors="coerce").dt.normalize(); sq["code"] = sq.get("code", pd.Series("", index=sq.index)).map(_norm_code)
        state_exact_codes = {(pd.Timestamp(d).strftime("%Y-%m-%d"), c) for d, c in zip(sq["signal_date"], sq["code"]) if pd.notna(d) and c}

    codes = seeds["code"].astype(str).tolist() if not seeds.empty else []
    # Read cached market turnover once for all seeded names. No provider calls are allowed here.
    global_amount = thesis.load_cached_amount_panel(out, en, codes, max_files=max(800, int(os.getenv("V25_DAILY_AMOUNT_CACHE_MAX_FILES", "800")))) if codes else pd.DataFrame()

    compact_states: List[pd.DataFrame] = []; transitions: List[pd.DataFrame] = []; invariants: List[pd.DataFrame] = []
    restart_rows: List[Dict[str, Any]] = []; lifecycle_signals: List[Dict[str, Any]] = []
    lifecycle_policy: List[Dict[str, Any]] = []; lifecycle_fills: List[Dict[str, Any]] = []; lifecycle_horizons: List[Dict[str, Any]] = []
    seed_runtime_rows: List[Dict[str, Any]] = []
    cfg_life = thesis.Core224LifecycleConfig(max_follow_days=max(20, int(float(os.getenv("V25_LIFECYCLE_MAX_DAYS", "60")))))
    evaluated = 0; price_missing = 0; amount_ready_codes = 0

    for _, sr in seeds.iterrows():
        seed = sr.to_dict(); code = str(seed.get("code", ""));
        px_raw, cache_meta = thesis._read_price_cache_for_code(out, code)
        px_follow = thesis._normalize_lifecycle_price(px_raw)
        if px_raw is None or px_raw.empty or px_follow.empty:
            price_missing += 1
            seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": "PRICE_CACHE_MISSING", "actual_amount_days": 0})
            continue
        amount_auth = _merge_amount_authority(out, code, global_amount)
        q = thesis._overlay_actual_amount(px_raw, code, amount_auth)
        q.index = pd.to_datetime(q.index, errors="coerce"); q = q[q.index.notna()].sort_index()
        # Signal detection is strictly cut at cohort end; future bars remain available only to lifecycle follow-up.
        q_detect = q[q.index.normalize() <= en].copy()
        if q_detect.empty:
            seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": "NO_PRICE_BEFORE_COHORT_END", "actual_amount_days": 0})
            continue
        actual_days = int(pd.to_numeric(q_detect.get("amount_is_actual", pd.Series(0, index=q_detect.index)), errors="coerce").fillna(0).eq(1).sum())
        if actual_days >= thesis.Core224Config().actual_amount_min_history_days: amount_ready_codes += 1
        try:
            daily, ev, inv = thesis.evaluate_core224(q_detect)
        except Exception as exc:
            seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": f"EVAL_ERROR:{type(exc).__name__}", "actual_amount_days": actual_days})
            continue
        evaluated += 1
        seed_runtime_rows.append({**seed, **cache_meta, "daily_eval_status": "PASS", "actual_amount_days": actual_days})
        if not daily.empty:
            daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize()
            dz = daily[daily["date"].between(st, en, inclusive="both")].copy()
            keep = dz.get("core224_state", pd.Series("", index=dz.index)).astype(str).ne("NONE") | _num(dz, "core224_transition", 0).eq(1)
            dz = dz[keep]
            if not dz.empty:
                dz.insert(0, "version", VERSION); dz.insert(1, "code", code); dz.insert(2, "name", seed.get("name", "")); dz["seed_reason"] = seed.get("seed_reason", ""); dz["research_only"] = True
                compact_states.append(dz)
        if not ev.empty:
            ev["date"] = pd.to_datetime(ev["date"], errors="coerce").dt.normalize()
            ez = ev[ev["date"].between(st, en, inclusive="both")].copy()
            if not ez.empty:
                ez = _decorate_event(ez, seed); transitions.append(ez)
                rz = ez[ez.get("to_state", pd.Series("", index=ez.index)).astype(str).eq("CORE224_RESTART")]
                for _, rr in rz.iterrows():
                    rec = rr.to_dict(); ds = _fmt_date(rec.get("date")); key = (code, ds)
                    auth = exact_authority.get(ds, {})
                    exact_code = int((ds, code) in state_exact_codes)
                    exact_causal = int(exact_code == 1 and int(auth.get("complete", 0)) == 1)
                    if exact_causal:
                        authority_label = "EXACT_CAUSAL_ASOF_PROVEN"
                    elif exact_code and int(auth.get("fallback_used", 0)) == 1:
                        authority_label = "EXACT_SIGNAL_DATE_FALLBACK"
                    else:
                        authority_label = "DAILY_UNIVERSE_NOT_PROVEN"
                    rec.update({
                        "restart_date": ds,
                        "weekly_restart_observed": int(key in weekly_restart),
                        "restart_discovery": "WEEKLY_ALREADY_OBSERVED" if key in weekly_restart else "RECOVERED_BETWEEN_WEEKLY_SNAPSHOTS",
                        "exact_materialized_code_present": exact_code,
                        "historical_asof_complete_exact_date": int(auth.get("complete", 0)),
                        "historical_asof_status_exact_date": str(auth.get("status", "NO_EXACT_WEEKLY_AUTHORITY")),
                        "daily_universe_authority": authority_label,
                        "daily_universe_membership_proven": exact_causal,
                        "research_only": True,
                    })
                    restart_rows.append(rec)
                    sig, pol, ff, hh = _daily_lifecycle_for_restart(out, rec, px_follow, cohort, cfg_life)
                    if sig:
                        sig.update({k: rec.get(k) for k in ["weekly_restart_observed", "restart_discovery", "daily_universe_authority", "daily_universe_membership_proven"]})
                        lifecycle_signals.append(sig); lifecycle_policy.extend(pol); lifecycle_fills.extend(ff); lifecycle_horizons.extend(hh)
        if not inv.empty:
            inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.normalize()
            iz = inv[inv["date"].between(st, en, inclusive="both")].copy()
            if not iz.empty: invariants.append(_decorate_event(iz, seed))

    seed_runtime = pd.DataFrame(seed_runtime_rows)
    state_df = pd.concat(compact_states, ignore_index=True, sort=False) if compact_states else pd.DataFrame()
    transition_df = pd.concat(transitions, ignore_index=True, sort=False) if transitions else pd.DataFrame()
    invariant_df = pd.concat(invariants, ignore_index=True, sort=False) if invariants else pd.DataFrame()
    restart_df = pd.DataFrame(restart_rows)
    if not restart_df.empty:
        restart_df["restart_date"] = pd.to_datetime(restart_df["restart_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        restart_df = restart_df.sort_values(["restart_date", "code"], kind="stable").drop_duplicates(["restart_date", "code"], keep="last")
    life_signal_df = pd.DataFrame(lifecycle_signals)
    life_policy_df = pd.DataFrame(lifecycle_policy)
    life_fill_df = pd.DataFrame(lifecycle_fills)
    life_horizon_df = pd.DataFrame(lifecycle_horizons)
    life_stop = thesis._policy_stop_summary(life_policy_df, str(cohort.get("cohort_id", "COHORT_ALL"))) if not life_policy_df.empty else pd.DataFrame()

    recovered_n = int(restart_df.get("restart_discovery", pd.Series(dtype=str)).astype(str).eq("RECOVERED_BETWEEN_WEEKLY_SNAPSHOTS").sum()) if not restart_df.empty else 0
    exact_causal_n = int(_num(restart_df, "daily_universe_membership_proven", 0).eq(1).sum()) if not restart_df.empty else 0
    eligible_n = int(_num(life_signal_df, "lifecycle_eligible", 0).eq(1).sum()) if not life_signal_df.empty else 0
    inv_fail = len(invariant_df)
    price_cache_coverage = evaluated / max(1, len(seeds))
    authority_rate = exact_causal_n / max(1, len(restart_df))
    if len(seeds) == 0:
        status = "WARMUP_NO_WEEKLY_EPISODE_SEEDS"
    elif inv_fail:
        status = "INVALID_DAILY_SEQUENCE_INVARIANT"
    elif evaluated == 0:
        status = "INVALID_NO_PRICE_CACHE_EVALUATION"
    elif len(restart_df) == 0:
        status = "WARMUP_NO_DAILY_RESTART"
    elif eligible_n < 30:
        status = "RESEARCH_SAMPLE_WARMUP"
    elif authority_rate < 0.70:
        status = "RESEARCH_SAMPLE_READY_UNIVERSE_AUTHORITY_WARMUP"
    else:
        status = "DATA_READY_RESEARCH_ONLY"

    readiness = pd.DataFrame([{
        "version": VERSION, "status": status, "cohort_id": cohort.get("cohort_id", ""),
        "cohort_start": st.strftime("%Y-%m-%d"), "cohort_end": en.strftime("%Y-%m-%d"),
        "seed_codes": len(seeds), "evaluated_codes": evaluated, "price_cache_missing_codes": price_missing,
        "price_cache_coverage_pct": price_cache_coverage * 100.0, "actual_amount_ready20_codes": amount_ready_codes,
        "daily_state_rows": len(state_df), "daily_transition_rows": len(transition_df), "daily_invariant_fail_rows": inv_fail,
        "weekly_restart_keys": len(weekly_restart), "daily_restart_events": len(restart_df), "recovered_restart_events": recovered_n,
        "exact_causal_asof_restart_events": exact_causal_n, "daily_universe_authority_rate_pct": authority_rate * 100.0,
        "daily_lifecycle_signals": len(life_signal_df), "daily_lifecycle_eligible": eligible_n,
        "provider_calls": 0, "close_times_volume_substitution": 0,
        "live_logic_changed": False, "real_order_changed": False, "research_only": True,
        "elapsed_sec": round(time.monotonic() - t0, 3),
    }])

    manual_cols = [c for c in [
        "restart_date", "code", "name", "restart_discovery", "daily_universe_authority",
        "l0_date", "l0_low", "accum_date", "h1_date", "h1_high", "pullback_date", "healthy_date",
        "restart_evidence_count", "restart_bullish", "restart_prev_high_reclaim", "restart_flow_uptick",
    ] if c in restart_df.columns]
    manual = restart_df[manual_cols].head(45).copy() if not restart_df.empty else pd.DataFrame(columns=manual_cols)

    _write_csv(out / SEED_FILE, seed_runtime)
    _write_csv(out / STATE_FILE, state_df)
    _write_csv(out / TRANSITION_FILE, transition_df)
    _write_csv(out / RESTART_FILE, restart_df)
    _write_csv(out / INVARIANT_FILE, invariant_df)
    _write_csv(out / MANUAL_FILE, manual)
    _write_csv(out / LIFECYCLE_SIGNAL_FILE, life_signal_df)
    _write_csv(out / LIFECYCLE_POLICY_FILE, life_policy_df)
    _write_csv(out / LIFECYCLE_FILL_FILE, life_fill_df)
    _write_csv(out / LIFECYCLE_HORIZON_FILE, life_horizon_df)
    _write_csv(out / LIFECYCLE_STOP_FILE, life_stop)
    _write_csv(out / READINESS_FILE, readiness)

    pb = life_stop[life_stop.get("stop_lens", pd.Series(dtype=str)).astype(str).eq("PB_LOW")].iloc[0] if not life_stop.empty and (life_stop.get("stop_lens", pd.Series(dtype=str)).astype(str) == "PB_LOW").any() else pd.Series(dtype=object)
    lines = [
        HEADER,
        f"📌 {VERSION} · status={status} · weekly 전체 TOP500 재계산 없이 기존 cache만 사용 · provider call 0",
        f"🧭 seed {len(seeds)}종목 → daily 평가 {evaluated} · 가격cache 누락 {price_missing} · actual Amount 20일-ready 종목 {amount_ready_codes}",
        f"🔁 daily transitions {len(transition_df)} · invariant fail {inv_fail} · RESTART {len(restart_df)} (기존 weekly 관측 {len(restart_df)-recovered_n} / 주간사이 복원 {recovered_n})",
        f"📦 RESTART exact causal-asof 증명 {exact_causal_n}/{len(restart_df)} · 나머지는 DAILY_UNIVERSE_NOT_PROVEN/FALLBACK로 격리 · 인접 주간 membership을 일별 권한으로 위장하지 않음",
        f"🧪 daily lifecycle {len(life_signal_df)} · eligible {eligible_n} · 구조손절/30-30-40 분할/20·40·60일 규칙은 V25.3.1과 동일 · 조건 튜닝 0",
        "⚠️ 한계: 주간 seed/base lens에 한 번도 걸리지 않은 초단기 전체 사이클은 이 확대 Replay로도 발견하지 않습니다. 이는 전체시장 일별 재계산이 아니라 episode-resolution 감사입니다.",
    ]
    if not pb.empty:
        lines.append(
            f"🎯 Daily PB_LOW: n{int(pb.get('signals',0) or 0)} · 2차체결 {float(pb.get('entry2_fill_rate_pct',np.nan)):.1f}% · 3차체결 {float(pb.get('entry3_fill_rate_pct',np.nan)):.1f}% · "
            f"구조손절 {float(pb.get('structure_stop_rate_pct',np.nan)):.1f}% · H1종가재돌파 {float(pb.get('h1_close_rebreak_rate_pct',np.nan)):.1f}% · +5고가 {float(pb.get('profit5_high_rate_pct',np.nan)):.1f}%"
        )
    lines.append(f"⏱️ daily episode replay elapsed {time.monotonic()-t0:.1f}s")
    lines.append(f"- CSV: {SEED_FILE} · {STATE_FILE} · {TRANSITION_FILE} · {RESTART_FILE} · {LIFECYCLE_POLICY_FILE} · {READINESS_FILE}")
    report = "\n".join(lines)
    (out / REPORT_FILE).write_text(report + "\n", encoding="utf-8")
    return {
        "status": status, "seed": seed_runtime, "state": state_df, "transitions": transition_df,
        "restarts": restart_df, "invariants": invariant_df, "manual": manual,
        "lifecycle_signal": life_signal_df, "lifecycle_policy": life_policy_df,
        "lifecycle_fill": life_fill_df, "lifecycle_horizon": life_horizon_df,
        "lifecycle_stop": life_stop, "readiness": readiness, "report": report,
    }


def force_report(output_dir: str | Path = "reports") -> str:
    p = Path(output_dir or "reports") / REPORT_FILE
    return p.read_text(encoding="utf-8") if p.exists() else ""


__all__ = ["VERSION", "RESEARCH_ONLY", "run_daily_episode_replay", "force_report"]
