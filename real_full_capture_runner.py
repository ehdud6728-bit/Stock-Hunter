#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import builtins
import importlib
import inspect
import json
import math
import os
import re
import runpy
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

BRIDGE_ID = "REAL_FULL_CURRENT_SOURCE_BRIDGE_R1"
BRIDGE_REVISION = "REAL_FULL_SOURCE_BRIDGE_R1_2_OS_EXIT_RUNTIME_AI_CANDIDATES"
TOP_N = 15

REPORT_DIR = Path("reports")
FULL_SOURCE = REPORT_DIR / "real_full_current_universe.csv"
CANONICAL_SOURCE = REPORT_DIR / "real_full_trust_source.csv"
META_PATH = REPORT_DIR / "real_full_trust_source_meta.json"
REPORT_PATH = REPORT_DIR / "real_full_source_bridge_report.txt"

# This is the existing REAL_FULL final ranked candidate frame.
RUNTIME_PRIMARY_VARIABLE = "ai_candidates"
RUNTIME_SECONDARY_VARIABLE = "all_hits_sorted"

CODE_COLUMNS = ["종목코드","code","Code","ticker","Ticker","stock_code"]
NAME_COLUMNS = ["종목명","name","Name","stock_name"]
PRICE_COLUMNS = ["현재가","종가","Close","close","price","Price","entry_price"]
SCORE_COLUMNS = [
    "안전점수","safe_score","N점수","n_score",
    "최종점수","총점","점수","score","Score","total_score",
    "S점수","추천점수","td_prelim_score","td_structure_score"
]
AI_COLUMNS = ["ai_pick_label","AI Pick","AI_PICK","ai_pick_tier","AI등급","AI판정"]
PATTERN_EXACT_COLUMNS = [
    "검색패턴","search_pattern_primary","pattern","Pattern","패턴명",
    "search_pattern_matches","search_pattern_tags",
    "N구분","N조합","N패턴","대표신호","신호","유형","추천단계",
    "수박최종상태","수박상태","watermelon_state",
    "구조판정","저항구름상태","저항구름","정제","정제수박",
    "단계","stage","단계상태","PASS","td_label","td_exec_bucket",
    "final_decision","v1097_gate_tier","v1097_gate_group",
]


class BridgeForcedExit(BaseException):
    def __init__(self, code: int = 0):
        super().__init__(code)
        self.code = int(code or 0)


def _kst_now() -> datetime:
    return datetime.now(ZoneInfo("Asia/Seoul"))


def _signal_date() -> str:
    # Freeze the KST date at workflow/runner start. This prevents a long manual
    # run that crosses midnight from being stamped as the following day.
    v = str(os.environ.get("REAL_FULL_TRUST_SIGNAL_DATE") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return v
    return _kst_now().date().isoformat()


def _clean_code(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-6:].zfill(6) if s else ""


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    return "" if s.lower() in {"","nan","none","nat"} else s


def _jsonable(v: Any) -> Any:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (str,int,bool)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    return str(v)


def _to_dataframe(candidate: Any) -> Optional[pd.DataFrame]:
    if isinstance(candidate, pd.DataFrame):
        return candidate.copy()
    if isinstance(candidate, list):
        if len(candidate) == 0:
            return pd.DataFrame()
        if all(isinstance(x, dict) for x in candidate):
            return pd.DataFrame(candidate)
    if isinstance(candidate, tuple):
        if len(candidate) == 0:
            return pd.DataFrame()
        if all(isinstance(x, dict) for x in candidate):
            return pd.DataFrame(list(candidate))
    return None


def _pattern_columns(df: pd.DataFrame) -> List[str]:
    cols = []
    for c in PATTERN_EXACT_COLUMNS:
        if c in df.columns and c not in cols:
            cols.append(c)
    rx = re.compile(
        r"(pattern|패턴|수박|돌반지|삼각|bb40|blue|구조|저항|정제|stage|단계|td_label|signal|신호|gate)",
        re.I,
    )
    for c in df.columns:
        if c not in cols and rx.search(str(c)):
            cols.append(c)
    return cols[:32]


def _make_canonical(
    df: pd.DataFrame,
    capture_slot: str,
    source_variable: str,
    rank_semantics: str,
) -> pd.DataFrame:
    columns = [
        "signal_date","rank","code","name","snapshot_price",
        "pattern_combo","overlap","score","score_bucket",
        "ai_pick_label","evidence",
        "bridge_capture_slot","bridge_source_variable",
        "bridge_rank_semantics","bridge_score_source_col",
        "bridge_pattern_source_cols","bridge_source_rows",
    ]
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=columns)

    src = df.copy().reset_index(drop=True)
    code_col = _pick_col(src, CODE_COLUMNS)
    name_col = _pick_col(src, NAME_COLUMNS)
    price_col = _pick_col(src, PRICE_COLUMNS)
    score_col = _pick_col(src, SCORE_COLUMNS)
    ai_col = _pick_col(src, AI_COLUMNS)
    pcols = _pattern_columns(src)

    if not code_col:
        return pd.DataFrame(columns=columns)

    rows = []
    seen = set()
    signal_date = _signal_date()

    for _, row in src.iterrows():
        code = _clean_code(row.get(code_col))
        if not code or code in seen:
            continue
        seen.add(code)

        name = _text(row.get(name_col)) if name_col else ""
        price = pd.to_numeric(
            pd.Series([row.get(price_col) if price_col else np.nan]), errors="coerce"
        ).iloc[0]
        score = pd.to_numeric(
            pd.Series([row.get(score_col) if score_col else np.nan]), errors="coerce"
        ).iloc[0]
        ai = _text(row.get(ai_col)) if ai_col else ""

        parts = []
        ev: Dict[str,Any] = {}
        for c in pcols:
            t = _text(row.get(c))
            if not t:
                continue
            ev[c] = _jsonable(row.get(c))
            if t not in parts:
                parts.append(t)

        # Preserve important existing ranking fields even if they are not pattern fields.
        for c in ["단계랭크","안전점수","N점수","safe_score","n_score","td_structure_score","td_prelim_score"]:
            if c in src.columns:
                ev[c] = _jsonable(row.get(c))

        rows.append({
            "signal_date": signal_date,
            "rank": len(rows) + 1,  # exact existing frame order; NO rerank
            "code": code,
            "name": name,
            "snapshot_price": float(price) if pd.notna(price) and float(price) > 0 else np.nan,
            "pattern_combo": " | ".join(parts[:10]) if parts else "UNCLASSIFIED",
            "overlap": len(parts),
            "score": float(score) if pd.notna(score) else np.nan,
            "score_bucket": "SOURCE_NATIVE",
            "ai_pick_label": ai or "NOT_AVAILABLE",
            "evidence": json.dumps(ev, ensure_ascii=False, sort_keys=True, default=str),
            "bridge_capture_slot": capture_slot,
            "bridge_source_variable": source_variable,
            "bridge_rank_semantics": rank_semantics,
            "bridge_score_source_col": score_col or "",
            "bridge_pattern_source_cols": "|".join(pcols),
            "bridge_source_rows": int(len(src)),
        })
        if len(rows) >= TOP_N:
            break

    return pd.DataFrame(rows, columns=columns)


class BridgeState:
    def __init__(self, capture_slot: str):
        self.capture_slot = capture_slot
        self.capture_calls = 0
        self.best_priority = -1
        self.best_valid = False
        self.best_rows = -1
        self.source_variable = ""
        self.rank_semantics = ""
        self.last_error = ""
        self.original_google_calls = 0
        self.os_exit_intercepts = 0
        self.runtime_scan_frames = 0

    def reset_files(self) -> None:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        for p in (FULL_SOURCE, CANONICAL_SOURCE, META_PATH, REPORT_PATH):
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    def capture(
        self,
        candidate: Any,
        *,
        source_variable: str,
        rank_semantics: str,
        priority: int,
    ) -> bool:
        self.capture_calls += 1
        df = _to_dataframe(candidate)
        if df is None:
            self.last_error = f"{source_variable}:unsupported_type:{type(candidate).__name__}"
            return False

        # Higher-priority runtime authority always beats fallback hooks.
        # Within equal priority, keep the larger table.
        row_count = int(len(df))
        if priority < self.best_priority:
            return False
        if priority == self.best_priority and row_count < self.best_rows:
            return False

        canonical = _make_canonical(
            df, self.capture_slot, source_variable, rank_semantics
        )
        if row_count > 0 and canonical.empty:
            self.last_error = f"{source_variable}:nonempty_missing_ticker_identity"
            return False

        now = _kst_now()
        self.best_priority = int(priority)
        self.best_rows = row_count
        self.best_valid = True
        self.source_variable = source_variable
        self.rank_semantics = rank_semantics

        full = df.copy().reset_index(drop=True)
        full.insert(0, "_bridge_source_order", np.arange(1, len(full) + 1))
        full.insert(0, "_bridge_source_variable", source_variable)
        full.insert(0, "_bridge_capture_slot", self.capture_slot)
        full.insert(0, "_bridge_signal_date", _signal_date())
        full.to_csv(FULL_SOURCE, index=False, encoding="utf-8-sig")
        canonical.to_csv(CANONICAL_SOURCE, index=False, encoding="utf-8-sig")

        status = "CAPTURED_ZERO" if row_count == 0 else "CAPTURED_NONEMPTY"
        meta = {
            "bridge_id": BRIDGE_ID,
            "bridge_revision": BRIDGE_REVISION,
            "capture_status": status,
            "signal_date": _signal_date(),
            "captured_at_kst": now.isoformat(timespec="seconds"),
            "capture_slot": self.capture_slot,
            "capture_source_variable": source_variable,
            "capture_priority": int(priority),
            "capture_calls": self.capture_calls,
            "os_exit_intercepts": self.os_exit_intercepts,
            "runtime_scan_frames": self.runtime_scan_frames,
            "selected_handoff_rows": row_count,
            "canonical_top_rows": int(len(canonical)),
            "rank_semantics": rank_semantics,
            "zero_event_interpretable": int(status == "CAPTURED_ZERO"),
            "selection_logic_changed": 0,
            "score_rank_changed": 0,
            "google_sheet_call_changed": 0,
            "order_logic_changed": 0,
        }
        META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return True

    def capture_runtime_frames(self, caller_frame) -> bool:
        """Walk the legacy call stack and capture existing REAL_FULL runtime state."""
        frames = []
        f = caller_frame
        while f is not None:
            frames.append(f)
            f = f.f_back
        self.runtime_scan_frames = max(self.runtime_scan_frames, len(frames))

        # Primary: exact existing final ranked DataFrame.
        for frame in frames:
            loc = frame.f_locals
            if RUNTIME_PRIMARY_VARIABLE in loc:
                if self.capture(
                    loc.get(RUNTIME_PRIMARY_VARIABLE),
                    source_variable=RUNTIME_PRIMARY_VARIABLE,
                    rank_semantics="REAL_FULL_AI_CANDIDATES_EXISTING_ORDER_AT_GRACEFUL_SHUTDOWN",
                    priority=100,
                ):
                    return True

        # Secondary audit fallback: pre-candidate all_hits_sorted.
        # Never preferred over ai_candidates.
        for frame in frames:
            loc = frame.f_locals
            if RUNTIME_SECONDARY_VARIABLE in loc:
                if self.capture(
                    loc.get(RUNTIME_SECONDARY_VARIABLE),
                    source_variable=RUNTIME_SECONDARY_VARIABLE,
                    rank_semantics="REAL_FULL_ALL_HITS_SORTED_EXISTING_ORDER_AT_GRACEFUL_SHUTDOWN",
                    priority=80,
                ):
                    return True
        return False

    def finalize(self, main_exit_code: int) -> None:
        now = _kst_now()
        if not self.best_valid:
            for p in (FULL_SOURCE, CANONICAL_SOURCE):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass
            meta = {
                "bridge_id": BRIDGE_ID,
                "bridge_revision": BRIDGE_REVISION,
                "capture_status": "RUNTIME_SOURCE_NOT_FOUND",
                "signal_date": _signal_date(),
                "captured_at_kst": now.isoformat(timespec="seconds"),
                "capture_slot": self.capture_slot,
                "capture_calls": self.capture_calls,
                "os_exit_intercepts": self.os_exit_intercepts,
                "runtime_scan_frames": self.runtime_scan_frames,
                "selected_handoff_rows": 0,
                "canonical_top_rows": 0,
                "last_error": self.last_error,
                "main_exit_code": int(main_exit_code),
                "zero_event_interpretable": 0,
                "selection_logic_changed": 0,
                "score_rank_changed": 0,
                "google_sheet_call_changed": 0,
                "order_logic_changed": 0,
            }
            META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            try:
                meta = json.loads(META_PATH.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
            meta["main_exit_code"] = int(main_exit_code)
            meta["capture_calls"] = self.capture_calls
            meta["original_google_calls"] = self.original_google_calls
            meta["os_exit_intercepts"] = self.os_exit_intercepts
            meta["runtime_scan_frames"] = self.runtime_scan_frames
            META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        try:
            meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
        report = "\n".join([
            "🧪 [REAL_FULL CURRENT SOURCE BRIDGE]",
            f"status={meta.get('capture_status','UNKNOWN')}",
            f"date={meta.get('signal_date','-')} · slot={meta.get('capture_slot','-')}",
            f"source={meta.get('capture_source_variable','-')} · semantics={meta.get('rank_semantics','-')}",
            f"rows={meta.get('selected_handoff_rows',0)} · trust top rows={meta.get('canonical_top_rows',0)}",
            f"os._exit intercepts={meta.get('os_exit_intercepts',0)} · google calls={meta.get('original_google_calls',0)}",
            f"main exit={meta.get('main_exit_code',main_exit_code)}",
            "※ legacy os._exit 직전 기존 ai_candidates 순서를 연구용으로 복제 · 검색/점수/랭킹/주문 변경 0",
        ])
        REPORT_PATH.write_text(report, encoding="utf-8")
        print(report)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", required=True)
    args, passthrough = ap.parse_known_args()

    script = Path(args.script)
    if not script.exists():
        raise SystemExit(f"REAL_FULL_SOURCE_BRIDGE_SCRIPT_MISSING {script}")

    capture_slot = os.environ.get("REAL_FULL_TRUST_CAPTURE_SLOT", "UNKNOWN").strip() or "UNKNOWN"
    # Freeze before legacy work begins; a long run can cross midnight.
    os.environ.setdefault("REAL_FULL_TRUST_SIGNAL_DATE", _kst_now().date().isoformat())

    state = BridgeState(capture_slot)
    state.reset_files()

    # Fallback hook. Normal/no-candidate paths may never call it, so it is not
    # the primary authority anymore.
    gsm = importlib.import_module("google_sheet_manager")
    original_google = getattr(gsm, "update_google_sheet", None)

    if callable(original_google):
        def wrapped_update_google_sheet(*a, **kw):
            candidate = a[0] if a else kw.get("df", kw.get("data", kw.get("all_hits_sorted")))
            try:
                state.capture(
                    candidate,
                    source_variable="google_sheet_update_argument",
                    rank_semantics="GOOGLE_SHEET_UPDATE_ARGUMENT_EXISTING_ORDER",
                    priority=40,
                )
            except Exception as e:
                state.last_error = f"google_capture_exception:{type(e).__name__}:{e}"
                traceback.print_exc()
            state.original_google_calls += 1
            return original_google(*a, **kw)
        gsm.update_google_sheet = wrapped_update_google_sheet

    # CRITICAL: legacy graceful_shutdown() calls os._exit(), not sys.exit().
    # Intercept only inside this REAL_FULL wrapper. Capture the caller stack,
    # then raise a private BaseException so wrapper finally/fail-closed logic
    # can run. The original os._exit is restored before this runner returns.
    original_os_exit = os._exit

    def wrapped_os_exit(code=0):
        state.os_exit_intercepts += 1
        try:
            caller = inspect.currentframe().f_back
            state.capture_runtime_frames(caller)
        except Exception as e:
            state.last_error = f"os_exit_capture_exception:{type(e).__name__}:{e}"
            traceback.print_exc()
        raise BridgeForcedExit(int(code or 0))

    os._exit = wrapped_os_exit

    exit_code = 0
    old_argv = sys.argv[:]
    try:
        sys.argv = [str(script)] + passthrough
        runpy.run_path(str(script), run_name="__main__")
    except BridgeForcedExit as e:
        exit_code = int(e.code)
    except SystemExit as e:
        try:
            exit_code = int(e.code or 0)
        except Exception:
            exit_code = 1
    except BaseException:
        exit_code = 1
        traceback.print_exc()
    finally:
        sys.argv = old_argv
        os._exit = original_os_exit
        if callable(original_google):
            gsm.update_google_sheet = original_google
        state.finalize(exit_code)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
