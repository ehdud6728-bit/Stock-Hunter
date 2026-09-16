#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

REVISION = "R1_4_1_CAUSAL_RUNTIME_FDR_CACHE_220_REPLAY_20260916"
SHADOW_ID = "REAL_FULL_R1C1"
SHADOW_REVISION = "R1C1_PB_VOLUME_CONTRACTION_V1"
ANCHOR_METHOD = "CAUSAL_LOW_HIGH_PULLBACK_V1"
ANCHOR_PROVENANCE = "EXACT_ASOF_HISTORY_SHADOW_SERIALIZER"
RUNTIME_PRICE_PROVENANCE = "EXACT_RUNTIME_FDR_CACHE_220_CAPTURED_DURING_REAL_FULL"
TOP_N = 15

REPORT_DIR = Path("reports")
SIDE_CSV = REPORT_DIR / "real_full_r1c1_v72_runtime_sidecar.csv"
ANCHOR_CSV = REPORT_DIR / "v72_formula_selector_anchor_serialization.csv"
META_JSON = REPORT_DIR / "real_full_r1c1_v72_runtime_sidecar_meta.json"
REPORT_TXT = REPORT_DIR / "real_full_r1c1_v72_runtime_sidecar_report.txt"


def _norm_code(v: Any) -> str:
    s = str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    for suffix in (".KS", ".KQ", ".KRX"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break
    s = "".join(ch for ch in s if ch.isalnum())
    if len(s) == 7 and s.startswith("A"):
        s = s[1:]
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    return s[-6:] if len(s) >= 6 else s


def _num(v: Any, default: float = float("nan")) -> float:
    try:
        x = float(str(v).replace(",", "").replace("%", "").strip())
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _first(row: pd.Series, names: Iterable[str], default: Any = "") -> Any:
    for c in names:
        if c not in row.index:
            continue
        v = row.get(c)
        try:
            if pd.isna(v):
                continue
        except Exception:
            pass
        if str(v).strip() != "":
            return v
    return default


def _top15(ai_candidates: Any, signal_date: str) -> pd.DataFrame:
    if isinstance(ai_candidates, pd.DataFrame):
        src = ai_candidates.copy().reset_index(drop=True)
    elif isinstance(ai_candidates, list) and all(isinstance(x, dict) for x in ai_candidates):
        src = pd.DataFrame(ai_candidates)
    else:
        return pd.DataFrame()
    if src.empty:
        return pd.DataFrame()

    rows = []
    seen = set()
    for _, r in src.iterrows():
        code = _norm_code(_first(r, ["종목코드", "code", "Code", "ticker", "Ticker", "stock_code"]))
        if not code or code in seen:
            continue
        seen.add(code)
        rows.append({
            "signal_date": signal_date,
            "rank": len(rows) + 1,
            "code": code,
            "name": str(_first(r, ["종목명", "name", "Name", "stock_name"], "")),
            "snapshot_price": _num(_first(r, ["현재가", "종가", "Close", "close", "price", "Price", "entry_price"])),
        })
        if len(rows) >= TOP_N:
            break
    return pd.DataFrame(rows)


def _frame_globals(frames: list[Any]) -> dict[str, Any]:
    """Find the live legacy-main globals without importing/re-running the scanner."""
    best: dict[str, Any] = {}
    for f in frames:
        g = getattr(f, "f_globals", {}) or {}
        detector = g.get("_v1107_4_5_72_detect_pullback_restart")
        cache = g.get("_fdr_cache")
        if callable(detector) and isinstance(cache, dict):
            best = {
                "detector": detector,
                "cache": cache,
                "get_indicators": g.get("get_indicators"),
                "enrich": g.get("_v1107_4_5_72_enrich_ohlcv"),
                "module_name": str(g.get("__name__", "")),
                "module_file": str(g.get("__file__", "")),
            }
            break
    return best


def _normalize_ohlcv(fr: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(fr, pd.DataFrame) or fr.empty:
        return pd.DataFrame()
    q = fr.copy(deep=True)
    ren = {
        "시가": "Open", "고가": "High", "저가": "Low", "종가": "Close", "거래량": "Volume",
        "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume",
    }
    q = q.rename(columns={c: ren.get(str(c), str(c)) for c in q.columns})
    if not isinstance(q.index, pd.DatetimeIndex):
        if "Date" in q.columns:
            q.index = pd.to_datetime(q["Date"], errors="coerce")
        elif "날짜" in q.columns:
            q.index = pd.to_datetime(q["날짜"], errors="coerce")
        else:
            q.index = pd.to_datetime(q.index, errors="coerce")
    q.index = pd.DatetimeIndex(q.index).normalize()
    q = q[q.index.notna()].sort_index()
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in q.columns:
            q[c] = pd.to_numeric(q[c], errors="coerce")
    return q


def _find_exact_cache220(cache: dict, code: str, signal_date: str) -> tuple[pd.DataFrame, str]:
    ymd = signal_date.replace("-", "")
    suffix = f"_220_{ymd}"
    hits: list[tuple[str, pd.DataFrame]] = []
    for k, v in cache.items():
        key = str(k)
        if not key.endswith(suffix):
            continue
        prefix = key[:-len(suffix)]
        if _norm_code(prefix) != code:
            continue
        if isinstance(v, pd.DataFrame):
            hits.append((key, v))
    if len(hits) != 1:
        return pd.DataFrame(), f"CACHE220_MATCH_COUNT_{len(hits)}"
    return _normalize_ohlcv(hits[0][1]), hits[0][0]


def _causal_guard(fr: pd.DataFrame, signal_date: str) -> tuple[bool, str, dict[str, Any]]:
    sd = pd.Timestamp(signal_date).normalize()
    info: dict[str, Any] = {
        "history_rows": int(len(fr)) if isinstance(fr, pd.DataFrame) else 0,
        "history_min_date": "",
        "history_max_date": "",
        "signal_day_present": False,
    }
    if fr is None or fr.empty:
        return False, "EMPTY_CACHE220_FRAME", info
    required = {"High", "Low", "Close", "Volume"}
    if not required.issubset(fr.columns):
        return False, f"OHLCV_COLUMNS_MISSING:{sorted(required-set(fr.columns))}", info
    if fr.index.has_duplicates:
        return False, "DUPLICATE_DAILY_INDEX", info
    info["history_min_date"] = fr.index.min().date().isoformat()
    info["history_max_date"] = fr.index.max().date().isoformat()
    info["signal_day_present"] = bool((fr.index == sd).any())
    if (fr.index > sd).any():
        return False, "FUTURE_PRICE_ROW_PRESENT", info
    if fr.index.max() != sd:
        return False, f"MAX_DATE_NOT_SIGNAL_DATE:{fr.index.max().date().isoformat()}", info
    if not info["signal_day_present"]:
        return False, "SIGNAL_DAY_SNAPSHOT_MISSING", info
    if pd.to_numeric(fr["Volume"], errors="coerce").dropna().lt(0).any():
        return False, "NEGATIVE_VOLUME", info
    return True, "PASS", info


def _same_enrichment(api: dict[str, Any], raw: pd.DataFrame) -> pd.DataFrame:
    getter = api.get("get_indicators")
    enrich = api.get("enrich")
    try:
        if callable(getter):
            out = getter(raw.copy(deep=True))
        elif callable(enrich):
            out = enrich(raw.copy(deep=True))
        else:
            raise RuntimeError("NO_ENRICHMENT_API")
    except Exception:
        if callable(enrich):
            out = enrich(raw.copy(deep=True))
        else:
            raise
    if not isinstance(out, pd.DataFrame) or out.empty:
        raise RuntimeError("ENRICHED_FRAME_EMPTY")
    return out


def _frozen_volume_metrics(raw: pd.DataFrame, anchor: dict[str, Any], signal_date: str) -> dict[str, Any]:
    try:
        ld = pd.Timestamp(anchor.get("wave1_low_date")).normalize()
        hd = pd.Timestamp(anchor.get("wave1_high_date")).normalize()
        pdte = pd.Timestamp(anchor.get("pullback_low_date")).normalize()
        sd = pd.Timestamp(signal_date).normalize()
    except Exception:
        return {"metric_status": "ANCHOR_DATES_INVALID"}
    if not (ld < hd < pdte <= sd):
        return {"metric_status": "ANCHOR_TEMPORAL_INVALID"}
    wave = pd.to_numeric(raw.loc[(raw.index >= ld) & (raw.index <= hd), "Volume"], errors="coerce").dropna()
    pb = pd.to_numeric(raw.loc[(raw.index > hd) & (raw.index <= pdte), "Volume"], errors="coerce").dropna()
    if wave.empty or pb.empty:
        return {"metric_status": "VOLUME_STAGE_EMPTY"}
    wmed = float(wave.median())
    pmed = float(pb.median())
    ratio = pmed / wmed if math.isfinite(wmed) and wmed > 0 and math.isfinite(pmed) else float("nan")
    if not math.isfinite(ratio):
        return {"metric_status": "PB_VOLUME_RATIO_UNAVAILABLE"}
    return {
        "metric_status": "PASS",
        "wave1_volume_median": wmed,
        "pb_volume_median": pmed,
        "pb_volume_vs_wave1": ratio,
        "max_predictor_price_date_used": pdte.date().isoformat(),
        "predictor_price_source": RUNTIME_PRICE_PROVENANCE,
        "predictor_causal_invariant": "PASS" if pdte < sd else "FAIL_PULLBACK_NOT_PRE_SIGNAL",
    }


def _clean_outputs() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    for p in [SIDE_CSV, ANCHOR_CSV, META_JSON, REPORT_TXT]:
        try:
            p.unlink()
        except FileNotFoundError:
            pass


def _write_fail(signal_date: str, reason: str, extra: dict[str, Any] | None = None) -> bool:
    meta = {
        "revision": REVISION,
        "status": "FAIL_CLOSED",
        "reason": reason,
        "signal_date": signal_date,
        "shadow_id": SHADOW_ID,
        "shadow_revision": SHADOW_REVISION,
        "research_only": True,
        "production_eligible": False,
        "selection_logic_changed": False,
        "score_rank_changed": False,
        "order_logic_changed": False,
        "network_refetch_used": False,
    }
    if extra:
        meta.update(extra)
    META_JSON.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    REPORT_TXT.write_text(
        "🧪 [REAL_FULL R1C1 R1.4.1 CAUSAL RUNTIME ADAPTER]\n"
        f"status=FAIL_CLOSED\nsignal_date={signal_date}\nreason={reason}\n"
        "network_refetch=0\nproduction/search/score/rank/order changes=0\n",
        encoding="utf-8",
    )
    print(REPORT_TXT.read_text(encoding="utf-8"))
    return False


def build_from_runtime_frames(frames: list[Any], ai_candidates: Any, signal_date: str, capture_slot: str = "") -> bool:
    """Build the R1.4.1 sidecar strictly after REAL_FULL has finished ranking.

    Causality authority is the exact in-memory `_fdr_cache` entry created by the
    existing scanner for `fdr_cached(code, days=220)`. There is no network refetch.
    The legacy detector is replayed against that exact captured frame only.
    """
    _clean_outputs()
    signal_date = str(signal_date)[:10]
    top = _top15(ai_candidates, signal_date)
    if len(top) != TOP_N:
        return _write_fail(signal_date, f"TOP15_AUTHORITY_ROW_COUNT_{len(top)}")

    api = _frame_globals(frames)
    if not api:
        return _write_fail(signal_date, "RUNTIME_LEGACY_GLOBALS_MISSING")

    try:
        import search_formula_complete_pipeline as sfcp
        anchor_fn = getattr(sfcp, "causal_anchor_v1", None)
    except Exception as e:
        return _write_fail(signal_date, f"ANCHOR_IMPORT_FAIL:{type(e).__name__}:{e}")
    if not callable(anchor_fn):
        return _write_fail(signal_date, "CAUSAL_ANCHOR_V1_MISSING")

    side_rows: list[dict[str, Any]] = []
    anchor_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for _, r in top.iterrows():
        code = str(r["code"])
        raw, cache_key = _find_exact_cache220(api["cache"], code, signal_date)
        ok, reason, cinfo = _causal_guard(raw, signal_date)
        if not ok:
            failures.append({"code": code, "reason": reason, "cache_key": cache_key, **cinfo})
            continue
        try:
            enriched = _same_enrichment(api, raw)
            out = api["detector"](enriched.copy(deep=True))
            if not isinstance(out, dict):
                raise RuntimeError("V72_DETECTOR_NON_DICT")
            score = _num(out.get("score"))
            if not math.isfinite(score):
                raise RuntimeError("V72_SCORE_NONFINITE")

            anchor = anchor_fn(raw.copy(deep=True), signal_date)
            if not isinstance(anchor, dict):
                raise RuntimeError("ANCHOR_NON_DICT")
            anchor = dict(anchor)
            # Anchor/metric ineligibility is not a serialization failure.
            # R1C1 frozen eligibility is enforced downstream (e.g. PB low must be pre-signal).
            # We still serialize every Top15 exact V72 score so an unknown score can never
            # be silently treated as a non-score100 row.
            metrics = _frozen_volume_metrics(raw, anchor, signal_date)

            side_rows.append({
                "revision": REVISION,
                "signal_date": signal_date,
                "rank": int(r["rank"]),
                "code": code,
                "name": r["name"],
                "snapshot_price": r["snapshot_price"],
                "source_entry_price": r["snapshot_price"],
                "v72_pullback_restart_score": score,
                "v72_pullback_restart_score_raw": _num(out.get("raw_score", out.get("score_raw"))),
                "v72_pullback_restart_grade": str(out.get("grade", "") or ""),
                "v72_pullback_restart_ok": bool(out.get("ok", False)),
                "v72_pullback_restart_reason": str(out.get("reason", "") or ""),
                "v72_pullback_restart_debug": str(out.get("debug", "") or ""),
                "v72_impulse_pct": _num(out.get("impulse_pct")),
                "v72_pullback_days": _num(out.get("pullback_days")),
                "v72_support_count": _num(out.get("support_count")),
                "v72_volume_ratio20": _num(out.get("volume_ratio20")),
                "v72_headroom_pct": _num(out.get("headroom_pct")),
                "v72_entry_line": _num(out.get("entry_line")),
                "v72_stop_line": _num(out.get("stop_line")),
                "runtime_cache_key": cache_key,
                "runtime_price_provenance": RUNTIME_PRICE_PROVENANCE,
                "history_rows": cinfo["history_rows"],
                "history_min_date": cinfo["history_min_date"],
                "history_max_date": cinfo["history_max_date"],
                "signal_day_snapshot_present": cinfo["signal_day_present"],
                "causal_invariant": "PASS",
                "network_refetch_used": False,
                "runtime_v72_module": api.get("module_name", ""),
                "selection_logic_changed": False,
                "score_rank_changed": False,
                "order_logic_changed": False,
            })

            anchor.update({
                "version": REVISION,
                "code": code,
                "name": r["name"],
                "anchor_method": anchor.get("anchor_method", ANCHOR_METHOD),
                "anchor_provenance": ANCHOR_PROVENANCE,
                "selector_logic_changed": False,
                "runtime_price_provenance": RUNTIME_PRICE_PROVENANCE,
                "runtime_cache_key": cache_key,
                "causal_invariant": "PASS",
                "network_refetch_used": False,
                **metrics,
            })
            anchor_rows.append(anchor)
        except Exception as e:
            failures.append({
                "code": code,
                "reason": f"{type(e).__name__}:{e}",
                "cache_key": cache_key,
                **cinfo,
            })

    if failures or len(side_rows) != TOP_N or len(anchor_rows) != TOP_N:
        return _write_fail(signal_date, "PARTIAL_OR_NONCAUSAL_REPLAY", {
            "top15_rows": int(len(top)),
            "sidecar_rows": int(len(side_rows)),
            "anchor_rows": int(len(anchor_rows)),
            "failures": failures[:30],
            "runtime_module": api.get("module_name", ""),
        })

    side = pd.DataFrame(side_rows).sort_values(["rank", "code"], kind="stable")
    anchors = pd.DataFrame(anchor_rows).sort_values(["signal_date", "code"], kind="stable")

    # Hard prospective causal invariants.
    if not side["causal_invariant"].astype(str).eq("PASS").all():
        return _write_fail(signal_date, "SIDECAR_CAUSAL_INVARIANT_FAIL")
    if not anchors["causal_invariant"].astype(str).eq("PASS").all():
        return _write_fail(signal_date, "ANCHOR_CAUSAL_INVARIANT_FAIL")
    if not anchors["network_refetch_used"].eq(False).all():
        return _write_fail(signal_date, "NETWORK_REFETCH_DETECTED")

    # Do NOT fail the whole day merely because a serialized anchor is R1C1-ineligible.
    # Historical R1.3 itself had 96 score100 rows but only 95 causal-eligible rows;
    # same-day PB or other eligibility failures must be append-only INELIGIBLE observations,
    # not dropped/retuned events. The tracker enforces PB low < signal date fail-closed.
    SIDE_CSV.parent.mkdir(parents=True, exist_ok=True)
    side.to_csv(SIDE_CSV, index=False, encoding="utf-8-sig")
    anchors.to_csv(ANCHOR_CSV, index=False, encoding="utf-8-sig")

    score100 = int(pd.to_numeric(side["v72_pullback_restart_score"], errors="coerce").eq(100).sum())
    _ratio = pd.to_numeric(anchors.get("pb_volume_vs_wave1", pd.Series(index=anchors.index, dtype=float)), errors="coerce")
    contraction = int((_ratio < 1.0).sum())
    metric_pass = int(anchors.get("metric_status", pd.Series(index=anchors.index, dtype=str)).astype(str).eq("PASS").sum())
    pb_pre_signal = int(pd.to_datetime(anchors.get("pullback_low_date"), errors="coerce").lt(pd.Timestamp(signal_date)).sum())
    meta = {
        "revision": REVISION,
        "status": "PASS",
        "signal_date": signal_date,
        "capture_slot": capture_slot,
        "top15_rows": TOP_N,
        "sidecar_rows": int(len(side)),
        "anchor_rows": int(len(anchors)),
        "score100_rows": score100,
        "pb_contraction_rows_all_top15": contraction,
        "frozen_metric_pass_rows": metric_pass,
        "pullback_pre_signal_rows": pb_pre_signal,
        "anchor_available_rows": int(anchors["anchor_status"].astype(str).eq("AVAILABLE").sum()),
        "temporal_pass_rows": int(anchors["temporal_invariant"].astype(str).eq("PASS").sum()),
        "runtime_price_provenance": RUNTIME_PRICE_PROVENANCE,
        "anchor_method": ANCHOR_METHOD,
        "anchor_provenance": ANCHOR_PROVENANCE,
        "network_refetch_used": False,
        "research_only": True,
        "production_eligible": False,
        "selection_logic_changed": False,
        "score_rank_changed": False,
        "order_logic_changed": False,
        "same_sample_retuning": False,
        "r1c1_threshold": "<1.0 FROZEN",
        "ma224_gate_used": False,
    }
    META_JSON.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    REPORT_TXT.write_text(
        "🧪 [REAL_FULL R1C1 R1.4.1 CAUSAL RUNTIME ADAPTER]\n"
        f"status=PASS\ndate={signal_date} top15={TOP_N} score100={score100}\n"
        f"anchors={meta['anchor_available_rows']}/{TOP_N} temporal={meta['temporal_pass_rows']}/{TOP_N}\n"
        f"source={RUNTIME_PRICE_PROVENANCE}\nnetwork_refetch=0\n"
        "R1C1 rule=PB median Volume / Wave1 median Volume < 1.0 (FROZEN)\n"
        "MA224=context only; not a gate\nproduction/search/score/rank/order changes=0\n",
        encoding="utf-8",
    )
    print(REPORT_TXT.read_text(encoding="utf-8"))
    return True


def self_test() -> int:
    # Minimal deterministic cache/causal test; no network access.
    sd = "2026-09-15"
    idx = pd.to_datetime(["2026-09-12", "2026-09-15"])
    fr = pd.DataFrame({"Open": [10, 11], "High": [11, 12], "Low": [9, 10], "Close": [10, 11], "Volume": [100, 80]}, index=idx)
    cache = {"000001_220_20260915": fr}
    got, key = _find_exact_cache220(cache, "000001", sd)
    assert key == "000001_220_20260915"
    ok, reason, info = _causal_guard(got, sd)
    assert ok and reason == "PASS" and info["signal_day_present"]
    bad = fr.copy(); bad.loc[pd.Timestamp("2026-09-16")] = [11, 12, 10, 11, 90]
    ok2, reason2, _ = _causal_guard(_normalize_ohlcv(bad), sd)
    assert not ok2 and reason2 == "FUTURE_PRICE_ROW_PRESENT"
    print("REAL_FULL_R141_CAUSAL_RUNTIME_ADAPTER_SELF_TEST_PASS")
    return 0


if __name__ == "__main__":
    import sys
    if "--self-test" in sys.argv:
        raise SystemExit(self_test())
    raise SystemExit("This adapter is invoked by real_full_capture_runner.py after REAL_FULL ranking is frozen.")
