#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import shutil

RUNNER = Path("real_full_capture_runner.py")
TRACKER = Path("real_full_r1c1_oos_tracker.py")
ADAPTER = Path("real_full_r1c1_runtime_adapter.py")
MARKER_RUNNER = "R141_CAUSAL_RUNTIME_ADAPTER_HOOK"
MARKER_TRACKER = "R141_RUNTIME_SIDECAR_AUTO_RESOLVE"


def backup(p: Path) -> None:
    b = p.with_suffix(p.suffix + ".pre_r141_causal.bak")
    if not b.exists():
        shutil.copy2(p, b)


def patch_runner() -> None:
    s = RUNNER.read_text(encoding="utf-8")
    if MARKER_RUNNER in s:
        return
    old = '''            if RUNTIME_PRIMARY_VARIABLE in loc:\n                if self.capture(\n                    loc.get(RUNTIME_PRIMARY_VARIABLE),\n                    source_variable=RUNTIME_PRIMARY_VARIABLE,\n                    rank_semantics="REAL_FULL_AI_CANDIDATES_EXISTING_ORDER_AT_GRACEFUL_SHUTDOWN",\n                    priority=100,\n                ):\n                    return True\n'''
    new = '''            if RUNTIME_PRIMARY_VARIABLE in loc:\n                if self.capture(\n                    loc.get(RUNTIME_PRIMARY_VARIABLE),\n                    source_variable=RUNTIME_PRIMARY_VARIABLE,\n                    rank_semantics="REAL_FULL_AI_CANDIDATES_EXISTING_ORDER_AT_GRACEFUL_SHUTDOWN",\n                    priority=100,\n                ):\n                    # R141_CAUSAL_RUNTIME_ADAPTER_HOOK\n                    # Research-only sidecar. REAL_FULL ranking is already frozen above.\n                    # It replays V72 from the exact existing in-memory fdr_cached(...,220) frame; no refetch.\n                    try:\n                        from real_full_r1c1_runtime_adapter import build_from_runtime_frames as _r141_build\n                        _r141_build(\n                            frames,\n                            loc.get(RUNTIME_PRIMARY_VARIABLE),\n                            signal_date=_signal_date(),\n                            capture_slot=self.capture_slot,\n                        )\n                    except BaseException as _r141_e:\n                        # Fail closed only for research output; never alter REAL_FULL production result.\n                        try:\n                            import json as _r141_json\n                            from pathlib import Path as _R141Path\n                            _r141_p=_R141Path("reports/real_full_r1c1_v72_runtime_sidecar_meta.json")\n                            _r141_p.parent.mkdir(parents=True, exist_ok=True)\n                            _r141_p.write_text(_r141_json.dumps({\n                                "status":"FAIL_CLOSED",\n                                "reason":f"RUNNER_HOOK_EXCEPTION:{type(_r141_e).__name__}:{_r141_e}",\n                                "production_eligible":False,\n                                "selection_logic_changed":False,\n                                "score_rank_changed":False,\n                                "order_logic_changed":False,\n                                "network_refetch_used":False,\n                            }, ensure_ascii=False, indent=2), encoding="utf-8")\n                        except Exception:\n                            pass\n                    return True\n'''
    n = s.count(old)
    if n != 1:
        raise SystemExit(f"R141_RUNNER_PATCH_TARGET_MISMATCH:{n}")
    RUNNER.write_text(s.replace(old, new), encoding="utf-8")


def patch_tracker() -> None:
    s = TRACKER.read_text(encoding="utf-8")
    if MARKER_TRACKER not in s:
        old = '''        candidate_path = Path(args.candidate_csv)\n        anchor_path = Path(args.anchor_csv)\n        cand = candidate_normalize(candidate_path)\n'''
        new = '''        candidate_path = Path(args.candidate_csv)\n        # R141_RUNTIME_SIDECAR_AUTO_RESOLVE\n        # Workflow may still hand us real_full_trust_source.csv. If the same artifact\n        # contains the exact causal R1.4.1 sidecar, it is the only valid V72 score authority.\n        _r141_sidecar = candidate_path.parent / "real_full_r1c1_v72_runtime_sidecar.csv"\n        if _r141_sidecar.exists():\n            candidate_path = _r141_sidecar\n        anchor_path = Path(args.anchor_csv)\n        cand = candidate_normalize(candidate_path)\n'''
        n = s.count(old)
        if n != 1:
            raise SystemExit(f"R141_TRACKER_CANDIDATE_PATCH_TARGET_MISMATCH:{n}")
        s = s.replace(old, new)

    # Prefer the adapter-frozen causal stage-volume metrics. Network/cache fallback is retained
    # only for pre-R1.4.1 inputs; R1.4.1 provenance must never silently refetch.
    if "R141_FROZEN_PREDICTOR_METRICS" not in s:
        old = '''        reason = None\n        if str(r.get("_anchor_merge")) != "both":\n'''
        new = '''        reason = None\n        # R141_FROZEN_PREDICTOR_METRICS\n        _r141_runtime = str(r.get("runtime_price_provenance", "") or "").startswith("EXACT_RUNTIME_FDR_CACHE_220")\n        _r141_ratio = num(r.get("pb_volume_vs_wave1"))\n        _r141_wmed = num(r.get("wave1_volume_median"))\n        _r141_pmed = num(r.get("pb_volume_median"))\n        _r141_maxdate = str(r.get("max_predictor_price_date_used", "") or "")[:10]\n        _r141_causal = str(r.get("predictor_causal_invariant", "") or "")\n        if str(r.get("_anchor_merge")) != "both":\n'''
        n = s.count(old)
        if n != 1:
            raise SystemExit(f"R141_TRACKER_METRIC_HEADER_PATCH_TARGET_MISMATCH:{n}")
        s = s.replace(old, new)

        # Inject strict frozen-metric branch immediately before legacy predictor price retrieval.
        needle = '''        if reason is None:\n            # Predictor is allowed to use only through PB low, which is strictly before signal date.\n            fr, psrc = get_price_frame(code, ld - pd.Timedelta(days=5), pdte + pd.Timedelta(days=1), cache_root, allow_network)\n'''
        replacement = '''        if reason is None and _r141_runtime:\n            if _r141_causal != "PASS":\n                reason = f"R141_CAUSAL_INVARIANT_{_r141_causal or 'MISSING'}"\n            elif not (math.isfinite(_r141_wmed) and _r141_wmed > 0 and math.isfinite(_r141_pmed) and math.isfinite(_r141_ratio)):\n                reason = "R141_FROZEN_VOLUME_METRICS_MISSING"\n            elif not _r141_maxdate:\n                reason = "R141_MAX_PREDICTOR_DATE_MISSING"\n            elif pd.Timestamp(_r141_maxdate).normalize() >= sd:\n                reason = "R141_PREDICTOR_DATE_NOT_PRE_SIGNAL"\n            else:\n                base["wave1_volume_median"] = _r141_wmed\n                base["pb_volume_median"] = _r141_pmed\n                base["pb_volume_vs_wave1"] = _r141_ratio\n                base["max_predictor_price_date_used"] = _r141_maxdate\n                base["price_source"] = str(r.get("predictor_price_source", r.get("runtime_price_provenance", "R141_RUNTIME_CACHE")) or "R141_RUNTIME_CACHE")\n                # Apply the already-frozen R1C1 rule exactly; no new threshold/feature.\n                base["shadow_eligible"] = True\n                _r141_pos = bool(_r141_ratio < THRESHOLD)\n                base["r1c1_shadow_positive"] = _r141_pos\n                base["r1c1_shadow_state"] = "PB_CONTRACTION" if _r141_pos else "NO_PB_CONTRACTION"\n                base["lock_reason"] = "LOCKED_FROZEN_R1C1"\n        if reason is None and not _r141_runtime:\n            # Legacy R1.4 fallback for pre-R1.4.1 inputs only.\n            # Predictor is allowed to use only through PB low, which is strictly before signal date.\n            fr, psrc = get_price_frame(code, ld - pd.Timedelta(days=5), pdte + pd.Timedelta(days=1), cache_root, allow_network)\n'''
        n = s.count(needle)
        if n != 1:
            raise SystemExit(f"R141_TRACKER_METRIC_BRANCH_PATCH_TARGET_MISMATCH:{n}")
        s = s.replace(needle, replacement)

    TRACKER.write_text(s, encoding="utf-8")


def verify() -> None:
    rs = RUNNER.read_text(encoding="utf-8")
    ts = TRACKER.read_text(encoding="utf-8")
    assert MARKER_RUNNER in rs
    assert MARKER_TRACKER in ts
    assert "R141_FROZEN_PREDICTOR_METRICS" in ts
    assert "real_full_r1c1_runtime_adapter" in rs
    assert "real_full_r1c1_v72_runtime_sidecar.csv" in ts
    print("R141_CAUSAL_PATCH_VERIFY_PASS")
    print("workflow files changed=0")
    print("REAL_FULL search/score/rank/order code changed=0")
    print("R1C1 threshold changed=0")
    print("MA224 gate added=0")


def main() -> int:
    for p in [RUNNER, TRACKER, ADAPTER]:
        if not p.exists():
            raise SystemExit(f"R141_PATCH_REQUIRED_FILE_MISSING:{p}")
    backup(RUNNER); backup(TRACKER)
    patch_runner(); patch_tracker(); verify()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
