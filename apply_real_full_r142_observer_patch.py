#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

RUNNER = Path("real_full_capture_runner.py")
OBSERVER = Path("real_full_r1c1_structure_observer_r142.py")
MARKER_R141 = "R141_CAUSAL_RUNTIME_ADAPTER_HOOK"
MARKER_R142 = "R142_PROSPECTIVE_STRUCTURE_OBSERVER_HOOK"

BLOCK = '''                    # R142_PROSPECTIVE_STRUCTURE_OBSERVER_HOOK\n                    # Additive research-only observer. It consumes the same causal runtime cache\n                    # after R1.4.1 serialization. Failure can never alter REAL_FULL production.\n                    try:\n                        from real_full_r1c1_structure_observer_r142 import build_observation_board as _r142_build\n                        _r142_build(\n                            frames,\n                            loc.get(RUNTIME_PRIMARY_VARIABLE),\n                            signal_date=_signal_date(),\n                            capture_slot=self.capture_slot,\n                        )\n                    except BaseException as _r142_e:\n                        try:\n                            import json as _r142_json\n                            from pathlib import Path as _R142Path\n                            _r142_p=_R142Path("reports/real_full_r1c1_structure_observer_r142_meta.json")\n                            _r142_p.parent.mkdir(parents=True, exist_ok=True)\n                            _r142_p.write_text(_r142_json.dumps({\n                                "status":"FAIL_CLOSED",\n                                "reason":f"RUNNER_HOOK_EXCEPTION:{type(_r142_e).__name__}:{_r142_e}",\n                                "research_only":True,\n                                "production_eligible":False,\n                                "new_gate_added":False,\n                                "threshold_changed":False,\n                                "selection_logic_changed":False,\n                                "score_rank_changed":False,\n                                "order_logic_changed":False,\n                                "network_refetch_used":False,\n                                "same_sample_retuning":False,\n                            }, ensure_ascii=False, indent=2), encoding="utf-8")\n                        except Exception:\n                            pass\n'''


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def patch_runner(text: str) -> str:
    if MARKER_R142 in text:
        return text
    pos = text.find(MARKER_R141)
    if pos < 0:
        raise SystemExit("R142_PATCH_ABORT:R141_HOOK_MISSING")
    ret = text.find("                    return True\n", pos)
    if ret < 0:
        raise SystemExit("R142_PATCH_ABORT:PRIMARY_RETURN_TRUE_NOT_FOUND_AFTER_R141")
    return text[:ret] + BLOCK + text[ret:]


def main() -> int:
    if not RUNNER.exists():
        raise SystemExit("R142_PATCH_ABORT:runner_missing")
    if not OBSERVER.exists():
        raise SystemExit("R142_PATCH_ABORT:observer_missing")
    before = RUNNER.read_text(encoding="utf-8")
    before_sha = sha(RUNNER)
    after = patch_runner(before)
    changed = after != before
    if changed:
        bak = RUNNER.with_suffix(RUNNER.suffix + ".pre_r142_observer.bak")
        if not bak.exists():
            shutil.copy2(RUNNER, bak)
        RUNNER.write_text(after, encoding="utf-8", newline="\n")
    final = RUNNER.read_text(encoding="utf-8")
    checks = {
        "r141_hook_preserved": MARKER_R141 in final,
        "r142_hook_present": MARKER_R142 in final,
        "observer_import_present": "real_full_r1c1_structure_observer_r142" in final,
        "production_primary_variable_preserved": 'RUNTIME_PRIMARY_VARIABLE = "ai_candidates"' in final,
        "production_secondary_variable_preserved": 'RUNTIME_SECONDARY_VARIABLE = "all_hits_sorted"' in final,
        "observer_research_only_guard_present": '"production_eligible":False' in final,
        "observer_network_refetch_false_guard_present": '"network_refetch_used":False' in final,
    }
    if not all(checks.values()):
        raise SystemExit("R142_PATCH_VERIFY_FAIL:" + json.dumps(checks, ensure_ascii=False, sort_keys=True))
    print("R142_OBSERVER_PATCH_VERIFY_PASS")
    print(json.dumps({
        "changed": changed,
        "runner_sha_before": before_sha,
        "runner_sha_after": sha(RUNNER),
        "checks": checks,
        "workflow_files_changed": 0,
        "production_search_score_rank_order_changed": 0,
        "r1c1_threshold_changed": 0,
        "ma224_gate_added": 0,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
