from __future__ import annotations

"""Build the current CORE224 LIVE weekly seed from a dedicated no-hold materialized snapshot.

Research/PAPER only. This module deliberately separates the historical backtest calendar
(which is truncated by V1080_BACKTEST_HOLD_DAYS so outcomes are observable) from the current
LIVE watch-list snapshot (which must use the latest completed as-of date with hold=0).
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd

import direct_replay_materialized_v23 as mat
import v25_core224_daily_episode_replay as daily

VERSION = "V73.3.6.6.25.4.11"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🛰️ [CORE224 CURRENT LIVE SEED · NO-HOLD MATERIALIZED SNAPSHOT · RESEARCH/PAPER]"

LIVE_CACHE_DIR = Path(".cache/v25_core224_live")
SEED_SNAPSHOT_FILE = "weekly_seed_snapshot.csv"
SEED_UNIVERSE_FILE = "seed_universe.csv"
SEED_META_FILE = "seed_meta.json"
AUDIT_FILE = "v73_v25_core224_current_live_seed_audit.csv"
REPORT_FILE = "v73_v25_core224_current_live_seed_report.txt"


def _norm_code(v: Any) -> str:
    return daily._norm_code(v)


def _atomic_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    os.replace(tmp, path)


def _atomic_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _pick_col(df: pd.DataFrame, names: list[str]) -> str | None:
    if df is None or df.empty:
        return None
    lower = {str(c).lower(): c for c in df.columns}
    for n in names:
        if n in df.columns:
            return n
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def _policy_lock(output_dir: Path) -> Dict[str, Any]:
    p = output_dir / daily.POLICY_LOCK_MANIFEST_FILE
    if not p.exists():
        return {"status": "MISSING_POLICY_LOCK_MANIFEST", "primary_policy_id": "PRIMARY_FIB618_PLUS5_D1", "training_cutoff_date": ""}
    try:
        q = pd.read_csv(p, dtype=str).fillna("")
        if q.empty:
            raise ValueError("EMPTY_POLICY_LOCK_MANIFEST")
        r = q.iloc[-1].to_dict()
        return {
            "status": str(r.get("policy_lock_status", r.get("status", "")) or ""),
            "primary_policy_id": str(r.get("primary_policy_id", "PRIMARY_FIB618_PLUS5_D1") or "PRIMARY_FIB618_PLUS5_D1"),
            "training_cutoff_date": str(r.get("training_cutoff_date", r.get("cutoff_date", "")) or ""),
            "policy_lock_schema": str(r.get("policy_lock_schema", r.get("schema", "")) or ""),
        }
    except Exception as exc:
        return {"status": f"INVALID_POLICY_LOCK_MANIFEST:{type(exc).__name__}", "primary_policy_id": "PRIMARY_FIB618_PLUS5_D1", "training_cutoff_date": ""}


def _load_one_snapshot(source_dir: Path) -> Tuple[pd.Timestamp, Dict[str, Any], Dict[str, Any]]:
    mp = source_dir / mat.SHARD_MANIFEST_JSON
    if not mp.exists():
        raise RuntimeError("LIVE_SEED_SHARD_MANIFEST_MISSING")
    manifest = json.loads(mp.read_text(encoding="utf-8"))
    if str(manifest.get("status", "")) != "COMPLETE":
        raise RuntimeError(f"LIVE_SEED_SHARD_NOT_COMPLETE:{manifest.get('status','MISSING')}")
    selected = [pd.Timestamp(x).normalize() for x in (manifest.get("selected_dates") or [])]
    selected = sorted(set(selected))
    if len(selected) != 1:
        raise RuntimeError(f"LIVE_SEED_EXPECTED_ONE_DATE:{len(selected)}")
    asof = selected[0]
    payload = mat.load_materialized_date(source_dir, asof, require_current_identity=False)
    if not isinstance(payload, dict):
        raise RuntimeError("LIVE_SEED_MATERIALIZED_PAYLOAD_MISSING_OR_INVALID")
    return asof, payload, manifest


def _seed_frames(asof: pd.Timestamp, payload: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    side = payload.get("runtime_sidecars") if isinstance(payload.get("runtime_sidecars"), dict) else {}
    rows = side.get("V25_CORE224_ROWS") or []
    state = pd.DataFrame(rows)
    if not state.empty:
        if "signal_date" not in state.columns:
            state["signal_date"] = asof.strftime("%Y-%m-%d")
        state["signal_date"] = pd.to_datetime(state["signal_date"], errors="coerce").dt.normalize()
        state["code"] = state.get("code", pd.Series("", index=state.index)).map(_norm_code)
        state = state[state["signal_date"].eq(asof) & state["code"].ne("")].copy()
        qual, reason = daily._weekly_seed_qual_mask(state)
        state["weekly_seed_qualified"] = qual.astype(int)
        state["weekly_seed_reason"] = reason
        wa = state[state["weekly_seed_qualified"].eq(1)].copy()
    else:
        wa = pd.DataFrame()

    keep = [c for c in ["signal_date","code","name","market","sector","core224_state","weekly_seed_reason"] if c in wa.columns]
    wa = wa[keep].copy() if not wa.empty else pd.DataFrame(columns=["signal_date","code","name","market","sector","core224_state","weekly_seed_reason"])
    wa.insert(0, "version", VERSION)
    wa["watch_start_date"] = asof.strftime("%Y-%m-%d")
    wa["watch_end_exclusive"] = ""
    wa["live_latest_known_snapshot"] = 1
    wa["live_runtime_policy_role"] = "CAUSAL_WATCHLIST_SEED_ONLY"
    wa["research_only"] = True
    if "signal_date" in wa.columns:
        wa["signal_date"] = pd.to_datetime(wa["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    wa = wa.sort_values("code", kind="stable").drop_duplicates("code", keep="last") if not wa.empty else wa

    mem = payload.get("universe_membership") if isinstance(payload.get("universe_membership"), pd.DataFrame) else pd.DataFrame()
    su = pd.DataFrame()
    if not mem.empty:
        q = mem.copy()
        cc = _pick_col(q, ["code","Code","Symbol","종목코드"])
        nc = _pick_col(q, ["name","Name","종목명"])
        mc = _pick_col(q, ["market","Market","시장"])
        if cc:
            su = pd.DataFrame({"code": q[cc].map(_norm_code)})
            su["name"] = q[nc].astype(str) if nc else ""
            su["market"] = q[mc].astype(str) if mc else ""
            active = set(wa.get("code", pd.Series(dtype=str)).astype(str))
            su = su[su["code"].isin(active)].drop_duplicates("code", keep="last")
    if su.empty:
        su = wa[[c for c in ["code","name","market"] if c in wa.columns]].copy()
        for c in ["code","name","market"]:
            if c not in su.columns:
                su[c] = ""
    su.insert(0, "version", VERSION)
    su["signal_date"] = asof.strftime("%Y-%m-%d")
    su["research_only"] = True

    inv = side.get("V25_CORE224_INVARIANTS") or []
    diag = {
        "state_rows": int(len(state)),
        "seed_rows": int(len(wa)),
        "universe_rows": int(len(mem)),
        "invariant_fail_rows": int(len(inv)),
    }
    return wa, su, diag


def build(source_dir: str | Path, output_dir: str | Path, expected_end: str = "") -> Dict[str, Any]:
    src = Path(source_dir); out = Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    asof, payload, manifest = _load_one_snapshot(src)
    exp = pd.to_datetime(expected_end, errors="coerce")
    if pd.notna(exp):
        exp = pd.Timestamp(exp).normalize()
        if asof > exp:
            raise RuntimeError(f"LIVE_SEED_ASOF_AFTER_EXPECTED_END:{asof.date()}>{exp.date()}")
        if (exp - asof).days > 7:
            raise RuntimeError(f"LIVE_SEED_ASOF_TOO_OLD_FOR_EXPECTED_END:{asof.date()} vs {exp.date()}")

    wa, su, diag = _seed_frames(asof, payload)
    if diag["invariant_fail_rows"]:
        raise RuntimeError(f"LIVE_SEED_CORE224_INVARIANT_FAIL:{diag['invariant_fail_rows']}")
    lock = _policy_lock(out)

    cache = out / LIVE_CACHE_DIR
    cache.mkdir(parents=True, exist_ok=True)
    _atomic_csv(cache / SEED_SNAPSHOT_FILE, wa)
    _atomic_csv(cache / SEED_UNIVERSE_FILE, su)
    codes = sorted(set(wa.get("code", pd.Series(dtype=str)).astype(str))) if not wa.empty else []
    meta = {
        "version": VERSION,
        "schema": "V25.4.11_CURRENT_NO_HOLD_LIVE_SEED_1",
        "weekly_snapshot_date": asof.strftime("%Y-%m-%d"),
        "seed_count": len(codes),
        "seed_code_sha256": hashlib.sha256("|".join(codes).encode("utf-8")).hexdigest(),
        "snapshot_calendar_source": "DEDICATED_CURRENT_NO_HOLD_MATERIALIZED_SNAPSHOT",
        "snapshot_calendar_complete": 1,
        "snapshot_calendar_count": 1,
        "snapshot_calendar_empty_count": int(len(wa) == 0),
        "source": "CURRENT_NO_HOLD_TOP500_MATERIALIZED_V23",
        "backtest_hold_days_ignored_for_live_seed": True,
        "source_materialized_signal_date": str(payload.get("signal_date", "")),
        "source_shard_status": str(manifest.get("status", "")),
        "source_universe_rows": diag["universe_rows"],
        "source_state_rows": diag["state_rows"],
        "source_invariant_fail_rows": diag["invariant_fail_rows"],
        "policy_lock_status": lock.get("status", ""),
        "primary_policy_id": lock.get("primary_policy_id", "PRIMARY_FIB618_PLUS5_D1"),
        "training_cutoff_date": lock.get("training_cutoff_date", ""),
        "policy_lock_schema": lock.get("policy_lock_schema", ""),
        "causal_rule": "CURRENT_LIVE_SEED_IS_SEPARATE_FROM_BACKTEST_OUTCOME_HOLD_HORIZON;ENTRY_REQUIRES_POLICY_LOCKED",
        "research_only": True,
        "paper_only": True,
        "live_score_rank_changed": False,
        "real_order_changed": False,
    }
    _atomic_json(cache / SEED_META_FILE, meta)

    audit = pd.DataFrame([{
        "version": VERSION,
        "status": "PASS" if int(meta["snapshot_calendar_complete"]) == 1 else "INVALID",
        "snapshot_date": meta["weekly_snapshot_date"],
        "seed_count": meta["seed_count"],
        "state_rows": diag["state_rows"],
        "universe_rows": diag["universe_rows"],
        "invariant_fail_rows": diag["invariant_fail_rows"],
        "policy_lock_status": meta["policy_lock_status"],
        "hold_horizon_decoupled": 1,
        "research_only": True,
    }])
    _atomic_csv(out / AUDIT_FILE, audit)
    report = "\n".join([
        HEADER,
        f"📌 {VERSION} · status=PASS · snapshot {meta['weekly_snapshot_date']} · hold=0 for LIVE seed only",
        f"📦 current materialized: universe {diag['universe_rows']} · CORE224 rows {diag['state_rows']} · seed {meta['seed_count']} · invariant {diag['invariant_fail_rows']}",
        f"🔒 policy lock {meta['policy_lock_status']} · PRIMARY {meta['primary_policy_id']} · entry {'ENABLED_PAPER_REVIEW' if meta['policy_lock_status']=='LOCKED' else 'BLOCKED_PAPER_WATCH_ONLY'}",
        "🧭 historical backtest hold horizon is not reused as LIVE seed freshness authority.",
        "🔒 LIVE 점수·랭크·실주문 변경 0 · RESEARCH/PAPER only",
    ])
    (out / REPORT_FILE).write_text(report + "\n", encoding="utf-8")
    print(report, flush=True)
    return {"status": "PASS", "meta": meta, "audit": audit, "report": report}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-dir", default="reports/v25_live_seed_build")
    ap.add_argument("--output-dir", default="reports")
    ap.add_argument("--expected-end", default=os.getenv("V25_TWO_YEAR_END_DATE", ""))
    args = ap.parse_args()
    try:
        build(args.source_dir, args.output_dir, args.expected_end)
        return 0
    except Exception as exc:
        print(f"V25_CURRENT_LIVE_SEED_FATAL {type(exc).__name__}:{exc}", flush=True)
        return 211


if __name__ == "__main__":
    raise SystemExit(main())
