#!/usr/bin/env python3
"""V25.4.11 HF5-compatible lightweight ALL-cohort parent.

Consumes the already materialized A/B/C/D V23 payloads and runs only the V23
parent integrity report plus V25 CORE224 thesis/lifecycle finalization. It
intentionally skips the legacy V72/V24 full research chain for ALL because
that chain scales with the 2-year denominator and can exceed the parent step
runtime. Research-only; no LIVE or order logic is imported or changed.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd

import direct_replay_materialized_v23 as mat
import original_thesis_reconstruction as thesis
import v25_core224_daily_episode_replay as daily_replay

VERSION = "V73.3.6.6.25.4.11-HF1-HF5COMPAT"
AUDIT_FILE = "v73_v25_core224_only_parent_audit.csv"
REPORT_FILE = "v73_v25_core224_only_parent_report.txt"
HEADER = "⚡ [V25 ALL CORE224 LIGHTWEIGHT PARENT · RESEARCH_ONLY]"


def _on(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _write_audit(out: Path, row: dict) -> None:
    p = out / AUDIT_FILE
    with p.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row.keys()))
        w.writeheader(); w.writerow(row)


def _split_text(text: str, limit: int = 3400) -> list[str]:
    chunks: list[str] = []
    cur = ""
    for line in str(text or "").splitlines(True):
        if len(line) > limit:
            if cur:
                chunks.append(cur); cur = ""
            for i in range(0, len(line), limit):
                chunks.append(line[i:i + limit])
            continue
        if len(cur) + len(line) > limit and cur:
            chunks.append(cur); cur = ""
        cur += line
    if cur:
        chunks.append(cur)
    return chunks or [str(text or "")[:limit]]


def _send_test_telegram(text: str) -> str:
    if not _on("STOCKHUNTER_FORCE_BACKTEST_TELEGRAM", "0"):
        return "DISABLED"
    # Research-only parent must never fall back to the LIVE Telegram token/route.
    token = os.getenv("TELEGRAM_BACKTEST_TOKEN", "")
    chat = os.getenv("TEST_CHAT_ID_OVERRIDE", "") or os.getenv("TELEGRAM_CHAT_ID_EFFECTIVE", "")
    if not token or not chat:
        return "MISSING_TOKEN_OR_CHAT"
    try:
        for chunk in _split_text(text):
            data = urllib.parse.urlencode({"chat_id": chat, "text": chunk, "disable_web_page_preview": "true"}).encode()
            req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data, method="POST")
            with urllib.request.urlopen(req, timeout=15) as resp:
                if int(getattr(resp, "status", 200)) >= 300:
                    return f"HTTP_{getattr(resp, 'status', 'ERR')}"
        return "SENT"
    except Exception as exc:
        return f"ERROR:{type(exc).__name__}"


def run(output_dir: str) -> int:
    t0 = time.monotonic()
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    os.environ["V25_CORE224_ONLY_PARENT"] = "1"

    mp = out / mat.MERGE_AUDIT_JSON
    if not mp.exists():
        raise RuntimeError("V23_MERGE_AUDIT_MISSING")
    merge = json.loads(mp.read_text(encoding="utf-8"))
    selected = [str(x) for x in (merge.get("selected_dates") or [])]
    if str(merge.get("status")) != "COMPLETE_HANDOFF" or not selected:
        raise RuntimeError("INVALID_INCOMPLETE_SHARD_HANDOFF:" + json.dumps({
            "status": merge.get("status"), "dates": len(selected),
            "shards": len(merge.get("complete_shards") or []),
        }, ensure_ascii=False, sort_keys=True))
    if str(merge.get("cohort_mode", "")).upper() != "ALL":
        raise RuntimeError(f"CORE224_ONLY_PARENT_REQUIRES_ALL:{merge.get('cohort_mode')}")

    pre = mat.verify_parent_materialized(out, selected, raise_on_error=True)
    base_report, _ = mat.finalize_parent(out, "")
    base_report = base_report.replace(
        "🧠 [후속연구] parent는 materialized candidate/full-universe sidecar와 합쳐진 price cache를 사용해 기존 Eval/Formula/Context/Scale-In/Geo/Stability/보고서 체인을 그대로 수행합니다.",
        "🧠 [HF5 ALL 전용] parent는 materialized CORE224 sidecar와 합쳐진 price cache만 사용해 V25 구조손절·분할매수·20/40/60일 lifecycle을 finalization합니다. 기존 V72/V24 후속연구는 재계산하지 않습니다.",
    )

    # This parent intentionally skips the legacy Sequence/Stability branch.  Make that skip
    # explicit in V25's transfer audit without changing the V25 source module itself (and thus
    # without perturbing the shard/materialized source fingerprint).
    def _skip_pattern_transfer(_out):
        return pd.DataFrame([{
            "sequence_pattern_only_rows": 0, "stability_pattern_only_rows_before": 0,
            "stability_pattern_only_rows": 0, "missing_before_refresh": 0,
            "extra_before_refresh": 0, "refresh_applied": 0,
            "missing_in_stability": 0, "extra_in_stability": 0,
            "transfer_match": 1, "status": "SKIPPED_CORE224_ONLY_PARENT",
        }])
    thesis._pattern_only_transfer = _skip_pattern_transfer

    payloads = []
    for ds in selected:
        z = mat.load_materialized_date(out, ds, require_current_identity=True)
        if z is None:
            raise RuntimeError(f"MATERIALIZED_LOAD_FAILED:{ds}")
        payloads.append(z)

    report, tables = thesis.finalize(
        out,
        payloads=payloads,
        source_file=Path("main7_bugfix_2.py"),
        registry_path=Path("search_formula_contract_registry.json"),
        base_report=base_report,
    )
    # V25.4: resolve only weekly-seeded CORE224 episodes at daily resolution from already
    # materialized caches. No provider calls and no full-universe daily recomputation.
    daily_result = daily_replay.run_daily_episode_replay(
        out,
        state=tables.get("state", pd.DataFrame()) if isinstance(tables, dict) else pd.DataFrame(),
        payloads=payloads,
    )
    if str(daily_result.get("report", "")).strip():
        report = report.rstrip() + "\n\n" + str(daily_result.get("report", "")).strip()
    activation = tables.get("activation", pd.DataFrame()) if isinstance(tables, dict) else pd.DataFrame()
    life = tables.get("lifecycle", {}) if isinstance(tables, dict) else {}
    readiness = life.get("readiness", pd.DataFrame()) if isinstance(life, dict) else pd.DataFrame()
    ar = activation.iloc[-1] if isinstance(activation, pd.DataFrame) and not activation.empty else pd.Series(dtype=object)
    lr = readiness.iloc[-1] if isinstance(readiness, pd.DataFrame) and not readiness.empty else pd.Series(dtype=object)
    dr = daily_result.get("readiness", pd.DataFrame()) if isinstance(daily_result, dict) else pd.DataFrame()
    drr = dr.iloc[-1] if isinstance(dr, pd.DataFrame) and not dr.empty else pd.Series(dtype=object)

    elapsed = time.monotonic() - t0
    audit = {
        "version": VERSION,
        "status": "PENDING",
        "mode": "ALL_CORE224_ONLY_PARENT",
        "expected_dates": int(pre.get("expected_date_count", 0) or 0),
        "valid_dates": int(pre.get("valid_date_count", 0) or 0),
        "materialized_dates": int(float(ar.get("materialized_dates", 0) or 0)),
        "sidecar_dates": int(float(ar.get("sidecar_dates", 0) or 0)),
        "core224_rows": int(float(ar.get("core224_rows", 0) or 0)),
        "transition_rows": int(float(ar.get("transition_rows", 0) or 0)),
        "invariant_fail_rows": int(float(ar.get("invariant_fail_rows", 0) or 0)),
        "restart_rows": int(float(ar.get("restart_rows", 0) or 0)),
        "lifecycle_status": str(lr.get("status", "UNKNOWN")),
        "lifecycle_restart_signals": int(float(lr.get("restart_signal_rows", 0) or 0)),
        "lifecycle_eligible_signals": int(float(lr.get("eligible_restart_signals", 0) or 0)),
        "boundary_forced_exit": int(float(lr.get("boundary_forced_exit", 0) or 0)),
        "auto_chain": int(float(lr.get("auto_chain", 0) or 0)),
        "daily_episode_status": str(drr.get("status", "UNKNOWN")),
        "daily_seed_codes": int(float(drr.get("seed_codes", 0) or 0)),
        "daily_evaluated_codes": int(float(drr.get("evaluated_codes", 0) or 0)),
        "daily_raw_restart_rows": int(float(drr.get("raw_daily_restart_rows", drr.get("daily_restart_events", 0)) or 0)),
        "daily_cycle_first_restart_events": int(float(drr.get("cycle_first_restart_events", drr.get("daily_restart_events", 0)) or 0)),
        "daily_restart_events": int(float(drr.get("daily_restart_events", 0) or 0)),
        "daily_suppressed_repeat_restart_rows": int(float(drr.get("suppressed_repeat_restart_rows", 0) or 0)),
        "daily_episode_overlap_suppressed_events": int(float(drr.get("episode_overlap_suppressed_events", 0) or 0)),
        "daily_episode_overlap_review_pairs": int(float(drr.get("episode_overlap_review_pairs", 0) or 0)),
        "daily_recovered_restart_events": int(float(drr.get("recovered_restart_events", 0) or 0)),
        "daily_weekly_restart_cycles": int(float(drr.get("weekly_restart_cycles", 0) or 0)),
        "daily_weekly_exact_date_matches": int(float(drr.get("weekly_restart_exact_date_matches", 0) or 0)),
        "daily_weekly_same_cycle_date_shifts": int(float(drr.get("weekly_restart_same_cycle_date_shifts", 0) or 0)),
        "daily_weekly_explained_context_divergences": int(float(drr.get("weekly_restart_explained_context_divergences", 0) or 0)),
        "daily_weekly_900bar_contract_explained": int(float(drr.get("weekly_restart_900bar_contract_explained", 0) or 0)),
        "daily_weekly_exact_shard_lane_explained": int(float(drr.get("exact_shard_unresolved_lane_explained", 0) or 0)),
        "daily_weekly_reconciled": int(float(drr.get("weekly_restart_reconciled", 0) or 0)),
        "daily_weekly_unreconciled_raw": int(float(drr.get("weekly_restart_unreconciled_raw", drr.get("weekly_restart_unreconciled", 0)) or 0)),
        "daily_weekly_unexplained": int(float(drr.get("weekly_restart_unexplained", drr.get("weekly_restart_unreconciled", 0)) or 0)),
        "daily_weekly_seed_authority_rows": int(float(drr.get("weekly_seed_authority_rows", 0) or 0)),
        "daily_weekly_snapshot_calendar_dates": int(float(drr.get("weekly_snapshot_calendar_dates", 0) or 0)),
        "daily_weekly_snapshot_calendar_complete": int(float(drr.get("weekly_snapshot_calendar_complete", 0) or 0)),
        "daily_weekly_snapshot_empty_dates": int(float(drr.get("weekly_snapshot_empty_dates", 0) or 0)),
        "daily_weekly_snapshot_calendar_latest": str(drr.get("weekly_snapshot_calendar_latest", "")),
        "daily_seed_causal_eligible_events": int(float(drr.get("daily_seed_causal_eligible_events", 0) or 0)),
        "daily_seed_causal_ineligible_events": int(float(drr.get("daily_seed_causal_ineligible_events", 0) or 0)),
        "daily_policy_training_eligible_events": int(float(drr.get("policy_training_eligible_events", 0) or 0)),
        "daily_training_restart_events_before_cutoff": int(float(drr.get("training_restart_events_before_cutoff", 0) or 0)),
        "daily_training_universe_proven_before_cutoff": int(float(drr.get("training_universe_proven_before_cutoff", 0) or 0)),
        "daily_training_policy_eligible_before_cutoff": int(float(drr.get("training_policy_eligible_before_cutoff", 0) or 0)),
        "daily_exact_shard_restart_input_expected": int(float(drr.get("exact_shard_restart_input_expected", 0) or 0)),
        "daily_exact_shard_restart_input_proof_rows": int(float(drr.get("exact_shard_restart_input_proof_rows", 0) or 0)),
        "daily_exact_shard_restart_input_replay_pass": int(float(drr.get("exact_shard_restart_input_replay_pass", 0) or 0)),
        "daily_exact_shard_cross_lane_explained": int(float(drr.get("exact_shard_cross_lane_explained", 0) or 0)),
        "daily_exact_shard_same_input_nondeterminism": int(float(drr.get("exact_shard_same_input_nondeterminism", 0) or 0)),
        "daily_context_state_trace_rows": int(float(drr.get("context_state_trace_rows", 0) or 0)),
        "daily_context_state_trace_summary_rows": int(float(drr.get("context_state_trace_summary_rows", 0) or 0)),
        "daily_event_order_rows": int(float(drr.get("event_order_rows", 0) or 0)),
        "daily_path_class_rows": int(float(drr.get("path_class_rows", 0) or 0)),
        "daily_risk_parity_rows": int(float(drr.get("risk_parity_rows", 0) or 0)),
        "daily_invariant_fail_rows": int(float(drr.get("daily_invariant_fail_rows", 0) or 0)),
        "daily_lifecycle_eligible": int(float(drr.get("daily_lifecycle_eligible", 0) or 0)),
        "daily_exact_causal_asof_restart_events": int(float(drr.get("exact_causal_asof_restart_events", 0) or 0)),
        "daily_targeted_authority_dates": int(float(drr.get("targeted_authority_dates", 0) or 0)),
        "daily_targeted_authority_complete_dates": int(float(drr.get("targeted_authority_complete_dates", 0) or 0)),
        "daily_targeted_authority_full_name_complete_dates": int(float(drr.get("targeted_authority_full_name_complete_dates", 0) or 0)),
        "daily_targeted_authority_cache_valid_before": int(float(drr.get("targeted_authority_cache_valid_before", 0) or 0)),
        "daily_targeted_authority_cache_valid_after": int(float(drr.get("targeted_authority_cache_valid_after", 0) or 0)),
        "daily_targeted_authority_new_valid_market_dates": int(float(drr.get("targeted_authority_new_valid_market_dates", 0) or 0)),
        "daily_targeted_authority_provider_call_budget": int(float(drr.get("targeted_authority_provider_call_budget", 0) or 0)),
        "daily_targeted_authority_budget_exhausted": int(float(drr.get("targeted_authority_budget_exhausted", 0) or 0)),
        "daily_targeted_authority_provider_calls": int(float(drr.get("targeted_authority_provider_calls", 0) or 0)),
        "daily_targeted_authority_provider_errors": int(float(drr.get("targeted_authority_provider_errors", 0) or 0)),
        "daily_risk_parity_fill_group_rows": int(float(drr.get("risk_parity_fill_group_rows", 0) or 0)),
        "daily_stop_lens_compare_rows": int(float(drr.get("stop_lens_compare_rows", 0) or 0)),
        "daily_stop_lens_risk_tradeoff_rows": int(float(drr.get("stop_lens_risk_tradeoff_rows", 0) or 0)),
        "daily_input_fingerprint_changed_components": int(float(drr.get("input_fingerprint_changed_components", 0) or 0)),
        "daily_input_fingerprint_source_changed_components": int(float(drr.get("input_fingerprint_source_changed_components", 0) or 0)),
        "daily_policy_lock_status": str(drr.get("policy_lock_status", "UNKNOWN")),
        "daily_policy_lock_digest": str(drr.get("policy_lock_digest", "")),
        "daily_policy_lock_cutoff_date": str(drr.get("policy_lock_cutoff_date", "")),
        "daily_policy_lock_migration_status": str(drr.get("policy_lock_migration_status", "NONE")),
        "daily_policy_lock_forward_epoch_reset": int(float(drr.get("policy_lock_forward_epoch_reset", 0) or 0)),
        "daily_forward_oos_status": str(drr.get("forward_oos_status", "UNKNOWN")),
        "daily_forward_oos_events": int(float(drr.get("forward_oos_events", 0) or 0)),
        "daily_forward_oos_causal_eligible": int(float(drr.get("forward_oos_causal_eligible", 0) or 0)),
        "daily_forward_oos_finalized_policy_rows": int(float(drr.get("forward_oos_finalized_policy_rows", 0) or 0)),
        "daily_forward_oos_primary_finalized_trades": int(float(drr.get("forward_oos_primary_finalized_trades", 0) or 0)),
        "daily_forward_oos_immutability_conflicts": int(float(drr.get("forward_oos_immutability_conflicts", 0) or 0)),
        "daily_live_board_rows": int(float(drr.get("live_board_rows", 0) or 0)),
        "daily_live_board_entry_review": int(float(drr.get("live_board_entry_review", 0) or 0)),
        "daily_live_board_restart_wait": int(float(drr.get("live_board_restart_wait", 0) or 0)),
        "daily_live_board_initial_watch": int(float(drr.get("live_board_initial_watch", 0) or 0)),
        "daily_live_board_base_watch": int(float(drr.get("live_board_base_watch", 0) or 0)),
        "daily_live_board_excluded_restart": int(float(drr.get("live_board_excluded_restart", 0) or 0)),
        "daily_d1_execution_board_rows": int(float(drr.get("d1_execution_board_rows", 0) or 0)),
        "daily_d1_execution_paper_entries": int(float(drr.get("d1_execution_paper_entries", 0) or 0)),
        "daily_d1_execution_entry_cancels": int(float(drr.get("d1_execution_entry_cancels", 0) or 0)),
        "daily_exit_shadow_rows": int(float(drr.get("exit_shadow_rows", 0) or 0)),
        "daily_exit_shadow_summary_rows": int(float(drr.get("exit_shadow_summary_rows", 0) or 0)),
        "daily_stop_exit_policy_matrix_rows": int(float(drr.get("stop_exit_policy_matrix_rows", 0) or 0)),
        "daily_execution_causality_rows": int(float(drr.get("execution_causality_rows", 0) or 0)),
        "daily_execution_causality_summary_rows": int(float(drr.get("execution_causality_summary_rows", 0) or 0)),
        "daily_targeted_authority_class_summary_rows": int(float(drr.get("targeted_authority_class_summary_rows", 0) or 0)),
        "daily_execution_roundtrip_cost_bps_assumption": float(drr.get("execution_roundtrip_cost_bps_assumption", 0) or 0),
        "daily_core_replay_provider_calls": int(float(drr.get("core_daily_replay_provider_calls", 0) or 0)),
        "daily_provider_calls": int(float(drr.get("provider_calls", 0) or 0)),
        "legacy_v72_v24_parent_research": "SKIPPED_INTENTIONALLY",
        "live_logic_changed": False,
        "real_order_changed": False,
        "elapsed_sec": round(elapsed, 3),
    }
    structural_guard_ok = (
        str(ar.get("pipeline_status", "")) == "VALID_SHADOW"
        and audit["expected_dates"] > 0
        and audit["valid_dates"] == audit["expected_dates"]
        and audit["materialized_dates"] == audit["expected_dates"]
        and audit["sidecar_dates"] == audit["expected_dates"]
        and audit["core224_rows"] > 0
        and audit["invariant_fail_rows"] == 0
        and audit["boundary_forced_exit"] == 0
        and audit["auto_chain"] == 1
        and audit["lifecycle_status"] not in {"", "INVALID", "DISABLED"}
        and not audit["daily_episode_status"].startswith("INVALID")
        and audit["daily_invariant_fail_rows"] == 0
        and audit["daily_core_replay_provider_calls"] == 0
        and audit["daily_weekly_unexplained"] == 0
        and audit["daily_weekly_snapshot_calendar_complete"] == 1
        and audit["daily_weekly_snapshot_calendar_dates"] == audit["expected_dates"]
        and audit["daily_exact_shard_restart_input_expected"] == audit["daily_weekly_restart_cycles"]
        and audit["daily_exact_shard_restart_input_replay_pass"] == audit["daily_weekly_restart_cycles"]
        and audit["daily_exact_shard_same_input_nondeterminism"] == 0
        and audit["daily_input_fingerprint_source_changed_components"] == 0
        and audit["daily_forward_oos_immutability_conflicts"] == 0
    )
    policy_status = audit["daily_policy_lock_status"]
    policy_state_valid = policy_status in {"LOCKED", "PENDING_POLICY_LOCK_PREREQUISITES"}
    guard_ok = structural_guard_ok and policy_state_valid
    audit["research_pipeline_valid"] = int(structural_guard_ok)
    audit["policy_state_valid"] = int(policy_state_valid)
    audit["execution_authority"] = (
        "ENABLED_POLICY_LOCKED" if policy_status == "LOCKED"
        else "BLOCKED_POLICY_LOCK_PENDING" if policy_status == "PENDING_POLICY_LOCK_PREREQUISITES"
        else "BLOCKED_INVALID_POLICY_STATE"
    )
    audit["status"] = "PASS" if guard_ok else "INVALID"
    _write_audit(out, audit)
    parent_block = "\n".join([
        HEADER,
        f"📌 {VERSION} · status={audit['status']} · execution={audit['execution_authority']} · ALL A/B/C/D materialized payload만 사용",
        f"📦 V23 parent {audit['valid_dates']}/{audit['expected_dates']}일 · CORE224 rows {audit['core224_rows']} · transitions {audit['transition_rows']} · invariant fail {audit['invariant_fail_rows']}",
        f"🧭 weekly lifecycle {audit['lifecycle_status']} · RESTART {audit['lifecycle_restart_signals']} · eligible {audit['lifecycle_eligible_signals']} · boundary forced exit {audit['boundary_forced_exit']}",
        f"🔬 daily episode {audit['daily_episode_status']} · seed {audit['daily_seed_codes']} → 평가 {audit['daily_evaluated_codes']} · RESTART raw {audit['daily_raw_restart_rows']} → cycle-first {audit['daily_cycle_first_restart_events']} → episode-independent {audit['daily_restart_events']} (cycle반복억제 {audit['daily_suppressed_repeat_restart_rows']} / overlap억제 {audit['daily_episode_overlap_suppressed_events']} / REVIEW {audit['daily_episode_overlap_review_pairs']} / 주간사이복원 {audit['daily_recovered_restart_events']}) · invariant fail {audit['daily_invariant_fail_rows']}",
        f"🔗 weekly↔daily RESTART: weekly {audit['daily_weekly_restart_cycles']} · exact {audit['daily_weekly_exact_date_matches']} · same-cycle shift {audit['daily_weekly_same_cycle_date_shifts']} · exact-shard lane-explained {audit['daily_weekly_exact_shard_lane_explained']} · reconciled {audit['daily_weekly_reconciled']} · unexplained {audit['daily_weekly_unexplained']} · legacy-900bar {audit['daily_weekly_900bar_contract_explained']}",
        f"🧷 exact shard input proof: pass {audit['daily_exact_shard_restart_input_replay_pass']}/{audit['daily_exact_shard_restart_input_expected']} · proof rows {audit['daily_exact_shard_restart_input_proof_rows']} · cross-lane explained {audit['daily_exact_shard_cross_lane_explained']} · same-input nondeterminism {audit['daily_exact_shard_same_input_nondeterminism']}",
        f"🗓️ backtest-evaluable weekly calendar: {audit['daily_weekly_snapshot_calendar_dates']}/{audit['expected_dates']} · latest {audit['daily_weekly_snapshot_calendar_latest']} · hold {int(float(os.getenv('V1080_BACKTEST_HOLD_DAYS','10')))}D · empty {audit['daily_weekly_snapshot_empty_dates']} · authority {'PASS' if audit['daily_weekly_snapshot_calendar_complete']==1 else 'UNPROVEN'}",
        f"🛂 weekly-seed causal gate: active {audit['daily_seed_causal_eligible_events']}/{audit['daily_restart_events']} · excluded {audit['daily_seed_causal_ineligible_events']} · strict policy-training {audit['daily_policy_training_eligible_events']} · cutoff-train {audit['daily_training_policy_eligible_before_cutoff']}/{audit['daily_training_restart_events_before_cutoff']} · universe-proven train {audit['daily_training_universe_proven_before_cutoff']}",
        f"📦 targeted causal authority: proven events {audit['daily_exact_causal_asof_restart_events']}/{audit['daily_restart_events']} · window-complete {audit['daily_targeted_authority_complete_dates']}/{audit['daily_targeted_authority_dates']} · full-name {audit['daily_targeted_authority_full_name_complete_dates']}/{audit['daily_targeted_authority_dates']} · cache {audit['daily_targeted_authority_cache_valid_before']}→{audit['daily_targeted_authority_cache_valid_after']} (+{audit['daily_targeted_authority_new_valid_market_dates']}) · provider {audit['daily_targeted_authority_provider_calls']}/{audit['daily_targeted_authority_provider_call_budget']} · errors {audit['daily_targeted_authority_provider_errors']}",
        f"⏱️ lifecycle audit: event-order {audit['daily_event_order_rows']}행 · path-class {audit['daily_path_class_rows']}행 · stop-lens compare {audit['daily_stop_lens_compare_rows']}행 · risk-tradeoff {audit['daily_stop_lens_risk_tradeoff_rows']}행 · SINGLE↔30/30/40 {audit['daily_risk_parity_rows']}행 · fill-group {audit['daily_risk_parity_fill_group_rows']}행 · exit-shadow {audit['daily_exit_shadow_rows']}행 · stop×exit matrix {audit['daily_stop_exit_policy_matrix_rows']}행 · D+1 execution {audit['daily_execution_causality_rows']}행 · cost {audit['daily_execution_roundtrip_cost_bps_assumption']:.0f}bp · input-fp changed {audit['daily_input_fingerprint_changed_components']}/source {audit['daily_input_fingerprint_source_changed_components']} · 자동정책선택 0",
        f"🔒 policy lock {audit['daily_policy_lock_status']} · execution {audit['execution_authority']} · cutoff {audit['daily_policy_lock_cutoff_date']} · migration {audit['daily_policy_lock_migration_status']} · forward-reset {audit['daily_policy_lock_forward_epoch_reset']} · forward {audit['daily_forward_oos_status']} · events {audit['daily_forward_oos_events']} · causal {audit['daily_forward_oos_causal_eligible']} · PRIMARY-finalized {audit['daily_forward_oos_primary_finalized_trades']} · finalized-policy {audit['daily_forward_oos_finalized_policy_rows']} · immutability-conflict {audit['daily_forward_oos_immutability_conflicts']}",
        f"🚦 CORE224 LIVE BOARD: 내일진입 {audit['daily_live_board_entry_review']} · RESTART대기 {audit['daily_live_board_restart_wait']} · 초기관찰 {audit['daily_live_board_initial_watch']} · BASE {audit['daily_live_board_base_watch']} · 제외RESTART {audit['daily_live_board_excluded_restart']} · D+1실행행 {audit['daily_d1_execution_board_rows']} (paper-entry {audit['daily_d1_execution_paper_entries']} / cancel {audit['daily_d1_execution_entry_cancels']})",
        "🚫 2년 ALL 실행에서는 기존 V72/V24 전체분모·HAM·FAMILIAR·시장/섹터/검색식 후속연구를 parent에서 재계산하지 않습니다.",
        "✅ 목적: 이미 shard에서 계산한 CORE224 증거를 merge하여 구조손절·분할매수·20/40/60일 생명주기만 빠르게 검증합니다.",
        "🔒 LIVE 점수·랭크·진입·청산·주문 변경 0 · 기존 24주/단일 코호트 FULL 연구경로는 그대로 유지",
        f"⏱️ lightweight parent elapsed {elapsed:.1f}s",
        f"- CSV: {AUDIT_FILE} · V25 lifecycle 원장/요약 파일들",
    ])
    (out / REPORT_FILE).write_text(parent_block + "\n", encoding="utf-8")
    final_report = (report.rstrip() + "\n\n" + parent_block).strip()
    print(final_report, flush=True)

    tg = _send_test_telegram(final_report)
    print(f"V25_CORE224_ONLY_PARENT_TELEGRAM {tg}", flush=True)
    print("V25_CORE224_ONLY_PARENT_GUARD", json.dumps(audit, ensure_ascii=False, sort_keys=True), flush=True)

    return 0 if guard_ok else 204


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="reports")
    args = ap.parse_args()
    try:
        return run(args.output_dir)
    except Exception as exc:
        print(f"V25_CORE224_ONLY_PARENT_FATAL {type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        return 206


if __name__ == "__main__":
    raise SystemExit(main())
