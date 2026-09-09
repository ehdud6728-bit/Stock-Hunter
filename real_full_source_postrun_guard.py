#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

GUARD_ID = "REAL_FULL_SOURCE_POSTRUN_GUARD_R1"
GUARD_REVISION = "REAL_FULL_SOURCE_POSTRUN_GUARD_R1_1"
VALID_CAPTURE = {"CAPTURED_NONEMPTY", "CAPTURED_ZERO"}

EMPTY_UNIVERSE_MARKERS = (
    "종목 리스트 로드 완전 실패",
    "V73 SAFE STOP용 빈 DataFrame 반환",
)
FORCED_STUB_MARKERS = (
    "STOCKHUNTER_PYKRX_FORCE_STUB=1",
    "pykrx import/auth 실패 → KRX stub으로 우회합니다: RuntimeError: STOCKHUNTER_PYKRX_FORCE_STUB=1",
)


def _now_kst():
    return datetime.now(ZoneInfo("Asia/Seoul"))


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _base_meta(slot: str, runner_rc: int):
    now = _now_kst()
    return {
        "bridge_id": "REAL_FULL_CURRENT_SOURCE_BRIDGE_R1",
        "bridge_revision": "REAL_FULL_SOURCE_BRIDGE_R1_1_GOOGLE_SHEET_HANDOFF_COPY",
        "postrun_guard_id": GUARD_ID,
        "postrun_guard_revision": GUARD_REVISION,
        "signal_date": now.date().isoformat(),
        "captured_at_kst": now.isoformat(timespec="seconds"),
        "capture_slot": slot,
        "selected_handoff_rows": 0,
        "canonical_top_rows": 0,
        "main_exit_code": int(runner_rc),
        "zero_event_interpretable": 0,
        "selection_logic_changed": 0,
        "score_rank_changed": 0,
        "google_sheet_call_changed": 0,
        "order_logic_changed": 0,
    }


def run(args) -> int:
    log_path = Path(args.run_log)
    meta_path = Path(args.meta)
    guard_path = Path(args.guard_output)
    source_path = Path(args.source)
    full_source_path = Path(args.full_source)

    log_text = ""
    if log_path.exists():
        log_text = log_path.read_text(encoding="utf-8", errors="ignore")

    empty_universe = any(m in log_text for m in EMPTY_UNIVERSE_MARKERS)
    forced_stub = any(m in log_text for m in FORCED_STUB_MARKERS)

    meta = _read_json(meta_path) if meta_path.exists() else {}
    prior_status = str(meta.get("capture_status") or "")
    base = _base_meta(args.capture_slot, args.runner_rc)
    for k, v in base.items():
        meta.setdefault(k, v)

    meta["postrun_guard_id"] = GUARD_ID
    meta["postrun_guard_revision"] = GUARD_REVISION
    meta["postrun_checked_at_kst"] = _now_kst().isoformat(timespec="seconds")
    meta["runner_rc"] = int(args.runner_rc)
    meta["empty_universe_detected"] = int(empty_universe)
    meta["forced_stub_detected_in_main_log"] = int(forced_stub)
    meta["prior_capture_status"] = prior_status

    if empty_universe:
        status = "INVALID_EMPTY_UNIVERSE"
        reason = "REAL_FULL listing/universe load failed; zero-event evidence forbidden"
        rc = 73
    elif int(args.runner_rc) != 0:
        status = "RUNNER_NONZERO"
        reason = f"REAL_FULL bridge runner returned rc={int(args.runner_rc)}"
        rc = 74
    elif not meta_path.exists() or not prior_status:
        status = "RUNNER_EXIT_WITHOUT_META"
        reason = "REAL_FULL runner exited without authoritative source metadata"
        rc = 75
    elif prior_status not in VALID_CAPTURE:
        status = prior_status or "CAPTURE_INVALID"
        reason = f"bridge capture is not authoritative: {status}"
        rc = 76
    elif prior_status == "CAPTURED_NONEMPTY" and not source_path.exists():
        status = "INVALID_NONEMPTY_SOURCE_MISSING"
        reason = "nonempty bridge meta exists but canonical source CSV is missing"
        rc = 77
    elif prior_status == "CAPTURED_ZERO":
        handoff_rows = int(meta.get("selected_handoff_rows") or 0)
        zero_ok = int(meta.get("zero_event_interpretable") or 0)
        if handoff_rows != 0 or zero_ok != 1:
            status = "INVALID_ZERO_SEMANTICS"
            reason = "CAPTURED_ZERO lacks authoritative zero semantics"
            rc = 78
        else:
            status = "CAPTURED_ZERO"
            reason = "valid same-day zero captured from loaded REAL_FULL handoff"
            rc = 0
    else:
        status = "CAPTURED_NONEMPTY"
        reason = "valid same-day nonempty REAL_FULL handoff captured"
        rc = 0

    meta["capture_status"] = status
    meta["postrun_guard_reason"] = reason
    meta["postrun_guard_pass"] = int(rc == 0)

    # Any invalid result must never leave a stale canonical source that could
    # later be mistaken for current authority.
    if rc != 0:
        meta["zero_event_interpretable"] = 0
        for p in (source_path, full_source_path):
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    _write_json(meta_path, meta)

    guard_payload = {
        "guard_id": GUARD_ID,
        "guard_revision": GUARD_REVISION,
        "status": status,
        "reason": reason,
        "pass": int(rc == 0),
        "runner_rc": int(args.runner_rc),
        "empty_universe_detected": int(empty_universe),
        "forced_stub_detected_in_main_log": int(forced_stub),
        "capture_slot": args.capture_slot,
        "signal_date": meta.get("signal_date"),
    }
    _write_json(guard_path, guard_payload)

    print(
        "REAL_FULL_SOURCE_POSTRUN_GUARD",
        f"status={status}",
        f"pass={int(rc == 0)}",
        f"runner_rc={int(args.runner_rc)}",
        f"empty_universe={int(empty_universe)}",
        f"forced_stub_log={int(forced_stub)}",
        f"slot={args.capture_slot}",
    )
    print("reason=", reason)
    return rc


def self_test() -> int:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        # 1) Empty universe can NEVER be accepted as zero.
        log = root / "run.log"
        meta = root / "meta.json"
        source = root / "source.csv"
        full = root / "full.csv"
        gout = root / "guard.json"
        log.write_text("🚨 종목 리스트 로드 완전 실패 — V73 SAFE STOP용 빈 DataFrame 반환\n", encoding="utf-8")
        meta.write_text(json.dumps({
            "capture_status":"CAPTURED_ZERO",
            "signal_date":"2099-01-01",
            "selected_handoff_rows":0,
            "zero_event_interpretable":1,
        }), encoding="utf-8")
        source.write_text("code\n005930\n", encoding="utf-8")
        class A: pass
        a=A(); a.run_log=str(log); a.meta=str(meta); a.guard_output=str(gout)
        a.source=str(source); a.full_source=str(full); a.capture_slot="TEST"; a.runner_rc=0
        assert run(a) == 73
        d=json.loads(meta.read_text(encoding="utf-8"))
        assert d["capture_status"]=="INVALID_EMPTY_UNIVERSE"
        assert d["zero_event_interpretable"]==0
        assert not source.exists()

        # 2) No meta after runner => explicit failure meta.
        log.write_text("normal log\n", encoding="utf-8")
        meta.unlink()
        assert run(a) == 75
        d=json.loads(meta.read_text(encoding="utf-8"))
        assert d["capture_status"]=="RUNNER_EXIT_WITHOUT_META"
        assert d["zero_event_interpretable"]==0

        # 3) Clean authoritative zero passes.
        log.write_text("universe loaded 1716\n", encoding="utf-8")
        meta.write_text(json.dumps({
            "capture_status":"CAPTURED_ZERO",
            "signal_date":"2099-01-01",
            "selected_handoff_rows":0,
            "zero_event_interpretable":1,
        }), encoding="utf-8")
        assert run(a) == 0

        # 4) Nonempty capture requires source CSV.
        meta.write_text(json.dumps({
            "capture_status":"CAPTURED_NONEMPTY",
            "signal_date":"2099-01-01",
            "selected_handoff_rows":15,
            "zero_event_interpretable":0,
        }), encoding="utf-8")
        try: source.unlink()
        except FileNotFoundError: pass
        assert run(a) == 77
        source.write_text("code\n005930\n", encoding="utf-8")
        meta.write_text(json.dumps({
            "capture_status":"CAPTURED_NONEMPTY",
            "signal_date":"2099-01-01",
            "selected_handoff_rows":15,
            "zero_event_interpretable":0,
        }), encoding="utf-8")
        assert run(a) == 0

    print("REAL_FULL_SOURCE_POSTRUN_GUARD_SELF_TEST PASS")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-log", default="reports/real_full_main_run.log")
    ap.add_argument("--meta", default="reports/real_full_trust_source_meta.json")
    ap.add_argument("--guard-output", default="reports/real_full_source_postrun_guard.json")
    ap.add_argument("--source", default="reports/real_full_trust_source.csv")
    ap.add_argument("--full-source", default="reports/real_full_current_universe.csv")
    ap.add_argument("--capture-slot", default="UNKNOWN")
    ap.add_argument("--runner-rc", type=int, default=0)
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    return self_test() if args.self_test else run(args)


if __name__ == "__main__":
    raise SystemExit(main())
