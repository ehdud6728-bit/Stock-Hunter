#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import re
import runpy
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

BRIDGE_ID = "REAL_FULL_CURRENT_SOURCE_BRIDGE_R1"
BRIDGE_REVISION = "REAL_FULL_SOURCE_BRIDGE_R1_1_GOOGLE_SHEET_HANDOFF_COPY"
TOP_N = 15

REPORT_DIR = Path("reports")
FULL_SOURCE = REPORT_DIR / "real_full_current_universe.csv"
CANONICAL_SOURCE = REPORT_DIR / "real_full_trust_source.csv"
META_PATH = REPORT_DIR / "real_full_trust_source_meta.json"
REPORT_PATH = REPORT_DIR / "real_full_source_bridge_report.txt"

CODE_COLUMNS = ["종목코드","code","Code","ticker","Ticker","stock_code"]
NAME_COLUMNS = ["종목명","name","Name","stock_name"]
PRICE_COLUMNS = ["현재가","종가","Close","close","price","Price"]
SCORE_COLUMNS = [
    "최종점수","총점","점수","score","Score","total_score",
    "S점수","추천점수","td_prelim_score","td_structure_score"
]
AI_COLUMNS = ["ai_pick_label","AI Pick","AI_PICK","ai_pick_tier","AI등급","AI판정"]
PATTERN_EXACT_COLUMNS = [
    "검색패턴","search_pattern_primary","pattern","Pattern","패턴명",
    "N구분","N조합","N패턴","대표신호","신호","유형","추천단계",
    "수박최종상태","수박상태","watermelon_state",
    "구조판정","저항구름상태","저항구름","정제","정제수박",
    "단계","stage","PASS","td_label","td_exec_bucket",
]


def _kst_now() -> datetime:
    return datetime.now(ZoneInfo("Asia/Seoul"))


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


def _pattern_columns(df: pd.DataFrame) -> List[str]:
    cols = []
    for c in PATTERN_EXACT_COLUMNS:
        if c in df.columns and c not in cols:
            cols.append(c)
    # Broad capture only for existing descriptive pattern/tag fields.
    rx = re.compile(r"(pattern|패턴|수박|돌반지|삼각|bb40|blue|구조|저항|정제|stage|단계|td_label|signal|신호)", re.I)
    for c in df.columns:
        if c not in cols and rx.search(str(c)):
            cols.append(c)
    return cols[:24]


def _make_canonical(df: pd.DataFrame, capture_slot: str, call_index: int) -> pd.DataFrame:
    columns = [
        "signal_date","rank","code","name","snapshot_price",
        "pattern_combo","overlap","score","score_bucket",
        "ai_pick_label","evidence",
        "bridge_capture_slot","bridge_call_index",
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
        # A non-empty table without ticker identity is not a valid candidate authority.
        return pd.DataFrame(columns=columns)

    now = _kst_now()
    rows = []
    seen = set()

    for idx, row in src.iterrows():
        code = _clean_code(row.get(code_col))
        if not code or code in seen:
            continue
        seen.add(code)

        name = _text(row.get(name_col)) if name_col else ""
        price = pd.to_numeric(pd.Series([row.get(price_col) if price_col else np.nan]), errors="coerce").iloc[0]
        score = pd.to_numeric(pd.Series([row.get(score_col) if score_col else np.nan]), errors="coerce").iloc[0]
        ai = _text(row.get(ai_col)) if ai_col else ""

        parts = []
        ev = {}
        for c in pcols:
            t = _text(row.get(c))
            if not t:
                continue
            ev[c] = _jsonable(row.get(c))
            if t not in parts:
                parts.append(t)

        pattern_combo = " | ".join(parts[:8]) if parts else "UNCLASSIFIED"
        overlap = len(parts)

        rows.append({
            "signal_date": now.date().isoformat(),
            "rank": len(rows) + 1,  # exact source order handed to sheet
            "code": code,
            "name": name,
            "snapshot_price": float(price) if pd.notna(price) and float(price) > 0 else np.nan,
            "pattern_combo": pattern_combo,
            "overlap": overlap,
            "score": float(score) if pd.notna(score) else np.nan,
            "score_bucket": "SOURCE_NATIVE",
            "ai_pick_label": ai or "NOT_AVAILABLE",
            "evidence": json.dumps(ev, ensure_ascii=False, sort_keys=True, default=str),
            "bridge_capture_slot": capture_slot,
            "bridge_call_index": call_index,
            "bridge_rank_semantics": "REAL_FULL_ALL_HITS_SORTED_ORDER_AT_GOOGLE_SHEET_HANDOFF",
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
        self.hook_calls = 0
        self.best_rows = -1
        self.best_valid = False
        self.last_error = ""
        self.original_google_calls = 0

    def reset_files(self) -> None:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        for p in (FULL_SOURCE, CANONICAL_SOURCE, META_PATH, REPORT_PATH):
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    def capture(self, candidate: Any) -> None:
        self.hook_calls += 1
        call_index = self.hook_calls
        if not isinstance(candidate, pd.DataFrame):
            self.last_error = f"hook_arg_not_dataframe:{type(candidate).__name__}"
            return

        df = candidate.copy()
        row_count = int(len(df))
        # Keep the largest handoff table; do not let a later small helper table replace it.
        if row_count < self.best_rows:
            return

        canonical = _make_canonical(df, self.capture_slot, call_index)
        if row_count > 0 and canonical.empty:
            self.last_error = "nonempty_handoff_missing_ticker_identity"
            return

        now = _kst_now()
        self.best_rows = row_count
        self.best_valid = True

        full = df.copy().reset_index(drop=True)
        full.insert(0, "_bridge_source_order", np.arange(1, len(full) + 1))
        full.insert(0, "_bridge_capture_slot", self.capture_slot)
        full.insert(0, "_bridge_capture_date_kst", now.date().isoformat())
        full.to_csv(FULL_SOURCE, index=False, encoding="utf-8-sig")
        canonical.to_csv(CANONICAL_SOURCE, index=False, encoding="utf-8-sig")

        status = "CAPTURED_ZERO" if row_count == 0 else "CAPTURED_NONEMPTY"
        meta = {
            "bridge_id": BRIDGE_ID,
            "bridge_revision": BRIDGE_REVISION,
            "capture_status": status,
            "signal_date": now.date().isoformat(),
            "captured_at_kst": now.isoformat(timespec="seconds"),
            "capture_slot": self.capture_slot,
            "hook_calls": self.hook_calls,
            "selected_handoff_rows": row_count,
            "canonical_top_rows": int(len(canonical)),
            "rank_semantics": "REAL_FULL_ALL_HITS_SORTED_ORDER_AT_GOOGLE_SHEET_HANDOFF",
            "zero_event_interpretable": int(status == "CAPTURED_ZERO"),
            "selection_logic_changed": 0,
            "score_rank_changed": 0,
            "google_sheet_call_changed": 0,
            "order_logic_changed": 0,
        }
        META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    def finalize(self, main_exit_code: int) -> None:
        now = _kst_now()
        if not self.best_valid:
            # Explicitly create a current failure meta; never leave stale prior source files.
            for p in (FULL_SOURCE, CANONICAL_SOURCE):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass
            meta = {
                "bridge_id": BRIDGE_ID,
                "bridge_revision": BRIDGE_REVISION,
                "capture_status": "HOOK_NOT_CALLED" if self.hook_calls == 0 else "CAPTURE_INVALID",
                "signal_date": now.date().isoformat(),
                "captured_at_kst": now.isoformat(timespec="seconds"),
                "capture_slot": self.capture_slot,
                "hook_calls": self.hook_calls,
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
            meta["hook_calls"] = self.hook_calls
            meta["original_google_calls"] = self.original_google_calls
            META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        try:
            meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
        report = "\n".join([
            "🧪 [REAL_FULL CURRENT SOURCE BRIDGE]",
            f"status={meta.get('capture_status','UNKNOWN')}",
            f"date={meta.get('signal_date','-')} · slot={meta.get('capture_slot','-')}",
            f"handoff rows={meta.get('selected_handoff_rows',0)} · trust top rows={meta.get('canonical_top_rows',0)}",
            f"hook calls={meta.get('hook_calls',0)} · original Google calls={meta.get('original_google_calls',0)}",
            f"main exit={meta.get('main_exit_code',main_exit_code)}",
            "※ Google Sheet handoff 입력의 연구용 사본만 저장 · 검색/점수/랭킹/주문 변경 0",
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
    state = BridgeState(capture_slot)
    state.reset_files()

    gsm = importlib.import_module("google_sheet_manager")
    original = getattr(gsm, "update_google_sheet", None)
    if not callable(original):
        state.last_error = "google_sheet_manager.update_google_sheet_not_callable"
        state.finalize(72)
        raise SystemExit(72)

    def wrapped_update_google_sheet(*a, **kw):
        candidate = a[0] if a else kw.get("df", kw.get("data", kw.get("all_hits_sorted")))
        try:
            state.capture(candidate)
        except Exception as e:
            state.last_error = f"capture_exception:{type(e).__name__}:{e}"
            traceback.print_exc()
        state.original_google_calls += 1
        return original(*a, **kw)

    gsm.update_google_sheet = wrapped_update_google_sheet

    exit_code = 0
    old_argv = sys.argv[:]
    try:
        sys.argv = [str(script)] + passthrough
        runpy.run_path(str(script), run_name="__main__")
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
        state.finalize(exit_code)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
