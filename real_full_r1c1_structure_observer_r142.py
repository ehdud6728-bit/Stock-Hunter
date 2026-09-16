#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import real_full_r1c1_runtime_adapter as r141

REVISION = "R1_4_2_PROSPECTIVE_STRUCTURE_OBSERVER_20260916"
SHADOW_ID = "REAL_FULL_R1C1"
SHADOW_REVISION = "R1C1_PB_VOLUME_CONTRACTION_V1"
TOP_N = 15
REPORT_DIR = Path("reports")
OUT_CSV = REPORT_DIR / "real_full_r1c1_structure_observer_r142.csv"
META_JSON = REPORT_DIR / "real_full_r1c1_structure_observer_r142_meta.json"
REPORT_TXT = REPORT_DIR / "real_full_r1c1_structure_observer_r142_report.txt"
R141_SIDE = REPORT_DIR / "real_full_r1c1_v72_runtime_sidecar.csv"
R141_ANCHOR = REPORT_DIR / "v72_formula_selector_anchor_serialization.csv"
R141_META = REPORT_DIR / "real_full_r1c1_v72_runtime_sidecar_meta.json"


def _num(v: Any) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else float("nan")
    except Exception:
        return float("nan")


def _safe_ratio(a: Any, b: Any) -> float:
    aa, bb = _num(a), _num(b)
    if not (math.isfinite(aa) and math.isfinite(bb) and bb != 0):
        return float("nan")
    return aa / bb


def _clv(fr: pd.DataFrame) -> pd.Series:
    h = pd.to_numeric(fr.get("High"), errors="coerce")
    l = pd.to_numeric(fr.get("Low"), errors="coerce")
    c = pd.to_numeric(fr.get("Close"), errors="coerce")
    rng = h - l
    out = (c - l) / rng.replace(0, np.nan)
    return out.clip(lower=0, upper=1)


def _amount_proxy(fr: pd.DataFrame) -> pd.Series:
    return pd.to_numeric(fr.get("Close"), errors="coerce") * pd.to_numeric(fr.get("Volume"), errors="coerce")


def _find_long_cache(cache: dict, code: str, signal_date: str) -> tuple[pd.DataFrame, str, int]:
    """Use only a causal same-signal-date cache already populated by the live scanner.

    This function never fetches from the network. It looks for <code>_<days>_<YYYYMMDD>
    entries with days >= 224 and chooses the largest available window.
    """
    ymd = signal_date.replace("-", "")
    rx = re.compile(rf"^(.*)_(\d+)_{re.escape(ymd)}$")
    hits: list[tuple[int, str, pd.DataFrame]] = []
    for k, v in cache.items():
        if not isinstance(v, pd.DataFrame):
            continue
        m = rx.match(str(k))
        if not m:
            continue
        if r141._norm_code(m.group(1)) != code:
            continue
        days = int(m.group(2))
        if days < 224:
            continue
        q = r141._normalize_ohlcv(v)
        ok, _, _ = r141._causal_guard(q, signal_date)
        if ok:
            hits.append((days, str(k), q))
    if not hits:
        return pd.DataFrame(), "", 0
    days, key, fr = sorted(hits, key=lambda x: (x[0], x[1]), reverse=True)[0]
    return fr, key, days


def _ma224_metrics(long_raw: pd.DataFrame, signal_date: str, anchor: pd.Series) -> dict[str, Any]:
    out = {
        "ma224_status": "UNAVAILABLE_NO_RUNTIME_CACHE_GE224",
        "ma224_runtime_cache_key": "",
        "ma224_runtime_cache_days": 0,
        "signal_ma224": np.nan,
        "signal_close_vs_ma224_pct": np.nan,
        "ma224_slope_20d_pct": np.nan,
        "wave1_low_close_vs_ma224_pct": np.nan,
        "wave1_high_close_vs_ma224_pct": np.nan,
        "pullback_low_close_vs_ma224_pct": np.nan,
    }
    if long_raw is None or long_raw.empty or len(long_raw) < 224:
        return out
    q = long_raw.copy()
    q["MA224"] = pd.to_numeric(q["Close"], errors="coerce").rolling(224, min_periods=224).mean()
    sd = pd.Timestamp(signal_date).normalize()
    if sd not in q.index or not math.isfinite(_num(q.at[sd, "MA224"])):
        out["ma224_status"] = "UNAVAILABLE_INSUFFICIENT_224_OBSERVATIONS"
        return out
    ma = _num(q.at[sd, "MA224"])
    close = _num(q.at[sd, "Close"])
    out["signal_ma224"] = ma
    out["signal_close_vs_ma224_pct"] = (_safe_ratio(close, ma) - 1.0) * 100.0
    pos = q.index.get_loc(sd)
    if isinstance(pos, (int, np.integer)) and pos >= 20:
        old = _num(q.iloc[pos - 20]["MA224"])
        if math.isfinite(old) and old > 0:
            out["ma224_slope_20d_pct"] = (ma / old - 1.0) * 100.0
    for src, dst in [
        ("wave1_low_date", "wave1_low_close_vs_ma224_pct"),
        ("wave1_high_date", "wave1_high_close_vs_ma224_pct"),
        ("pullback_low_date", "pullback_low_close_vs_ma224_pct"),
    ]:
        dt = pd.to_datetime(anchor.get(src), errors="coerce")
        if pd.isna(dt):
            continue
        dt = pd.Timestamp(dt).normalize()
        if dt in q.index:
            m = _num(q.at[dt, "MA224"])
            c = _num(q.at[dt, "Close"])
            if math.isfinite(m) and m > 0 and math.isfinite(c):
                out[dst] = (c / m - 1.0) * 100.0
    out["ma224_status"] = "PASS_EXACT_RUNTIME_CACHE"
    return out


def _structure_metrics(raw: pd.DataFrame, signal_date: str, anchor: pd.Series) -> dict[str, Any]:
    sd = pd.Timestamp(signal_date).normalize()
    ld = pd.to_datetime(anchor.get("wave1_low_date"), errors="coerce")
    hd = pd.to_datetime(anchor.get("wave1_high_date"), errors="coerce")
    pdte = pd.to_datetime(anchor.get("pullback_low_date"), errors="coerce")
    if pd.isna(ld) or pd.isna(hd) or pd.isna(pdte):
        return {"observer_metric_status": "ANCHOR_DATE_INVALID"}
    ld, hd, pdte = pd.Timestamp(ld).normalize(), pd.Timestamp(hd).normalize(), pd.Timestamp(pdte).normalize()
    if not (ld < hd < pdte <= sd):
        return {"observer_metric_status": "ANCHOR_TEMPORAL_INVALID"}
    if sd not in raw.index:
        return {"observer_metric_status": "SIGNAL_DAY_MISSING"}

    wave = raw.loc[(raw.index >= ld) & (raw.index <= hd)].copy()
    pb = raw.loc[(raw.index > hd) & (raw.index <= pdte)].copy()
    if wave.empty or pb.empty:
        return {"observer_metric_status": "STAGE_EMPTY"}

    wave_vol = pd.to_numeric(wave["Volume"], errors="coerce")
    pb_vol = pd.to_numeric(pb["Volume"], errors="coerce")
    wave_amt = _amount_proxy(wave)
    pb_amt = _amount_proxy(pb)
    sig_vol = _num(raw.at[sd, "Volume"])
    sig_amt = _num(raw.at[sd, "Close"]) * sig_vol

    prior20 = raw.loc[raw.index <= sd].tail(20)
    med20_vol = _num(pd.to_numeric(prior20["Volume"], errors="coerce").median())
    med20_amt = _num(_amount_proxy(prior20).median())

    # Down-day proxy uses close-to-previous-close sign. It is NOT aggressor/order-flow data.
    closes = pd.to_numeric(raw["Close"], errors="coerce")
    prev = closes.shift(1)
    pb_prev = prev.reindex(pb.index)
    pb_down = pd.to_numeric(pb["Close"], errors="coerce") < pb_prev
    pb_total_vol = _num(pb_vol.sum())
    pb_total_amt = _num(pb_amt.sum())
    down_vol = _num(pb_vol[pb_down].sum())
    down_amt = _num(pb_amt[pb_down].sum())

    sign = np.sign(closes.diff()).reindex(pb.index).fillna(0.0)
    signed_vol = _num((sign * pb_vol).sum())

    down_series = pb_vol[pb_down].dropna()
    mid = max(1, len(pb) // 2)
    first_idx = pb.index[:mid]
    second_idx = pb.index[mid:]
    first_down_med = _num(pb_vol.reindex(first_idx)[pb_down.reindex(first_idx).fillna(False)].median())
    second_down_med = _num(pb_vol.reindex(second_idx)[pb_down.reindex(second_idx).fillna(False)].median())

    pb_clv = _clv(pb)
    signal_clv = _num(_clv(raw.loc[[sd]]).iloc[0])

    age_after_pb = raw.loc[(raw.index > pdte) & (raw.index <= sd)]
    age_after_high = raw.loc[(raw.index > hd) & (raw.index <= sd)]

    return {
        "observer_metric_status": "PASS",
        "wave1_amount_proxy_median": _num(wave_amt.median()),
        "pb_amount_proxy_median": _num(pb_amt.median()),
        "pb_amount_vs_wave1": _safe_ratio(pb_amt.median(), wave_amt.median()),
        "signal_volume": sig_vol,
        "signal_amount_proxy": sig_amt,
        "signal_volume_vs_pb_median": _safe_ratio(sig_vol, pb_vol.median()),
        "signal_volume_vs_20d_median": _safe_ratio(sig_vol, med20_vol),
        "signal_amount_vs_pb_median": _safe_ratio(sig_amt, pb_amt.median()),
        "signal_amount_vs_20d_median": _safe_ratio(sig_amt, med20_amt),
        "pb_days": int(len(pb)),
        "pb_down_day_count": int(pb_down.fillna(False).sum()),
        "pb_down_day_ratio": _safe_ratio(pb_down.fillna(False).sum(), len(pb)),
        "pb_down_volume_share": _safe_ratio(down_vol, pb_total_vol),
        "pb_down_amount_share": _safe_ratio(down_amt, pb_total_amt),
        "pb_signed_volume_flow_ratio_proxy": _safe_ratio(signed_vol, pb_total_vol),
        "pb_first_half_down_volume_median": first_down_med,
        "pb_second_half_down_volume_median": second_down_med,
        "pb_second_vs_first_down_volume": _safe_ratio(second_down_med, first_down_med),
        "pb_clv_median": _num(pb_clv.median()),
        "pb_last3_clv_median": _num(pb_clv.tail(3).median()),
        "signal_clv": signal_clv,
        "pullback_low_age_calendar_days": int((sd - pdte).days),
        "pullback_low_age_trading_days": int(len(age_after_pb)),
        "wave1_high_age_calendar_days": int((sd - hd).days),
        "wave1_high_age_trading_days": int(len(age_after_high)),
        "sell_pressure_metrics_are_proxy_only": True,
    }


def _write_fail(signal_date: str, reason: str, extra: dict[str, Any] | None = None) -> bool:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    meta = {
        "revision": REVISION,
        "status": "FAIL_CLOSED",
        "reason": reason,
        "signal_date": signal_date,
        "research_only": True,
        "production_eligible": False,
        "new_gate_added": False,
        "threshold_changed": False,
        "selection_logic_changed": False,
        "score_rank_changed": False,
        "order_logic_changed": False,
        "network_refetch_used": False,
        "same_sample_retuning": False,
    }
    if extra:
        meta.update(extra)
    META_JSON.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    REPORT_TXT.write_text(
        "🧪 [REAL_FULL R1C1 R1.4.2 PROSPECTIVE STRUCTURE OBSERVER]\n"
        f"status=FAIL_CLOSED\nsignal_date={signal_date}\nreason={reason}\n"
        "research_only=1 network_refetch=0 new_gate=0 threshold_change=0 production_changes=0\n",
        encoding="utf-8",
    )
    return False


def build_observation_board(frames: list[Any], ai_candidates: Any, signal_date: str, capture_slot: str = "") -> bool:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    for p in (OUT_CSV, META_JSON, REPORT_TXT):
        try:
            p.unlink()
        except FileNotFoundError:
            pass

    signal_date = str(signal_date)[:10]
    if not (R141_META.exists() and R141_SIDE.exists() and R141_ANCHOR.exists()):
        return _write_fail(signal_date, "R141_AUTHORITY_FILES_MISSING")
    try:
        r141_meta = json.loads(R141_META.read_text(encoding="utf-8-sig"))
    except Exception as e:
        return _write_fail(signal_date, f"R141_META_READ_FAIL:{type(e).__name__}:{e}")
    if r141_meta.get("status") != "PASS" or int(r141_meta.get("sidecar_rows", 0)) != TOP_N or int(r141_meta.get("anchor_rows", 0)) != TOP_N:
        return _write_fail(signal_date, "R141_NOT_PASS_15_15", {"r141_status": r141_meta.get("status")})
    if bool(r141_meta.get("network_refetch_used")):
        return _write_fail(signal_date, "R141_NETWORK_REFETCH_FLAG_TRUE")

    top = r141._top15(ai_candidates, signal_date)
    if len(top) != TOP_N:
        return _write_fail(signal_date, f"TOP15_AUTHORITY_ROW_COUNT_{len(top)}")
    api = r141._frame_globals(frames)
    if not api:
        return _write_fail(signal_date, "RUNTIME_LEGACY_GLOBALS_MISSING")

    side = pd.read_csv(R141_SIDE, dtype={"code": str})
    anchors = pd.read_csv(R141_ANCHOR, dtype={"code": str})
    side["code"] = side["code"].map(r141._norm_code)
    anchors["code"] = anchors["code"].map(r141._norm_code)
    if side["code"].duplicated().any() or anchors["code"].duplicated().any():
        return _write_fail(signal_date, "R141_DUPLICATE_CODE_IDENTITY")

    rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for _, t in top.iterrows():
        code = str(t["code"])
        sm = side[side["code"].eq(code)]
        am = anchors[anchors["code"].eq(code)]
        if len(sm) != 1 or len(am) != 1:
            failures.append({"code": code, "reason": "R141_IDENTITY_MATCH_NOT_1_TO_1"})
            continue
        s = sm.iloc[0]
        a = am.iloc[0]
        raw, cache_key = r141._find_exact_cache220(api["cache"], code, signal_date)
        ok, reason, cinfo = r141._causal_guard(raw, signal_date)
        if not ok:
            failures.append({"code": code, "reason": reason})
            continue

        metric = _structure_metrics(raw, signal_date, a)
        long_raw, long_key, long_days = _find_long_cache(api["cache"], code, signal_date)
        ma = _ma224_metrics(long_raw, signal_date, a)
        ma["ma224_runtime_cache_key"] = long_key
        ma["ma224_runtime_cache_days"] = long_days

        pb_ratio = _num(a.get("pb_volume_vs_wave1"))
        pb_date = str(a.get("pullback_low_date", ""))[:10]
        score = _num(s.get("v72_pullback_restart_score"))
        eligible = bool(
            math.isfinite(score) and score == 100 and
            str(a.get("metric_status", "")) == "PASS" and
            str(a.get("predictor_causal_invariant", "")) == "PASS" and
            pb_date < signal_date and math.isfinite(pb_ratio)
        )
        state = "INELIGIBLE"
        if eligible:
            state = "PB_CONTRACTION" if pb_ratio < 1.0 else "NO_PB_CONTRACTION"

        row = {
            "revision": REVISION,
            "signal_date": signal_date,
            "rank": int(t["rank"]),
            "code": code,
            "name": t["name"],
            "v72_pullback_restart_score": score,
            "r1c1_shadow_eligible": eligible,
            "r1c1_shadow_state": state,
            "wave1_low_date": a.get("wave1_low_date", ""),
            "wave1_high_date": a.get("wave1_high_date", ""),
            "pullback_low_date": a.get("pullback_low_date", ""),
            "wave1_volume_median": _num(a.get("wave1_volume_median")),
            "pb_volume_median": _num(a.get("pb_volume_median")),
            "pb_volume_vs_wave1": pb_ratio,
            "v72_volume_ratio20": _num(s.get("v72_volume_ratio20")),
            "runtime_cache220_key": cache_key,
            "runtime_history_rows": cinfo.get("history_rows"),
            "runtime_history_max_date": cinfo.get("history_max_date"),
            "network_refetch_used": False,
            "new_gate_added": False,
            "threshold_changed": False,
            "production_eligible": False,
            **metric,
            **ma,
        }
        rows.append(row)

    if failures or len(rows) != TOP_N:
        return _write_fail(signal_date, "PARTIAL_OBSERVER_SERIALIZATION", {"rows": len(rows), "failures": failures[:30]})

    out = pd.DataFrame(rows).sort_values(["rank", "code"], kind="stable")
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    eligible_n = int(out["r1c1_shadow_eligible"].astype(bool).sum())
    pos_n = int(out["r1c1_shadow_state"].eq("PB_CONTRACTION").sum())
    neg_n = int(out["r1c1_shadow_state"].eq("NO_PB_CONTRACTION").sum())
    ma_n = int(out["ma224_status"].eq("PASS_EXACT_RUNTIME_CACHE").sum())
    metric_n = int(out["observer_metric_status"].eq("PASS").sum())
    meta = {
        "revision": REVISION,
        "status": "PASS",
        "signal_date": signal_date,
        "capture_slot": capture_slot,
        "rows": int(len(out)),
        "observer_metric_pass_rows": metric_n,
        "r1c1_eligible_rows": eligible_n,
        "r1c1_pb_contraction_rows": pos_n,
        "r1c1_no_pb_contraction_rows": neg_n,
        "ma224_exact_runtime_rows": ma_n,
        "ma224_unavailable_rows": int(TOP_N - ma_n),
        "research_only": True,
        "production_eligible": False,
        "new_gate_added": False,
        "threshold_changed": False,
        "selection_logic_changed": False,
        "score_rank_changed": False,
        "order_logic_changed": False,
        "network_refetch_used": False,
        "same_sample_retuning": False,
        "frozen_r1c1_threshold": "pb_volume_vs_wave1 < 1.0",
        "ma224_role": "CONTEXT_ONLY_NOT_GATE",
        "sell_pressure_note": "close-sign/volume/amount/CLV proxies; not aggressor order-flow",
    }
    META_JSON.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "🧪 [REAL_FULL R1C1 R1.4.2 PROSPECTIVE STRUCTURE OBSERVER]",
        f"status=PASS date={signal_date} rows={len(out)}/{TOP_N}",
        f"R1C1 eligible={eligible_n} · PB_CONTRACTION={pos_n} · NO_PB_CONTRACTION={neg_n}",
        f"structure metrics={metric_n}/{TOP_N} · exact runtime MA224={ma_n}/{TOP_N}",
        "observes: PB volume/amount contraction → signal-day volume/amount reacceleration → sell-pressure proxies → structure age → MA224 context",
        "sell-pressure fields are descriptive proxies only; no order-flow claim",
        "R1C1 threshold unchanged: PB median Volume / Wave1 median Volume < 1.0",
        "MA224=context only; no gate · network refetch=0 · same-sample retuning=0",
        "production/search/score/rank/order changes=0",
    ]
    REPORT_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(REPORT_TXT.read_text(encoding="utf-8"))
    return True


def self_test() -> int:
    idx = pd.bdate_range("2025-09-01", periods=250)
    close = pd.Series(np.linspace(100, 150, len(idx)), index=idx)
    raw = pd.DataFrame({
        "Open": close.values - 0.5,
        "High": close.values + 1.0,
        "Low": close.values - 1.0,
        "Close": close.values,
        "Volume": np.linspace(1000, 1500, len(idx)),
    }, index=idx)
    sd = idx[-1]
    anchor = pd.Series({
        "wave1_low_date": idx[-30].date().isoformat(),
        "wave1_high_date": idx[-20].date().isoformat(),
        "pullback_low_date": idx[-5].date().isoformat(),
    })
    m = _structure_metrics(raw, sd.date().isoformat(), anchor)
    assert m["observer_metric_status"] == "PASS"
    ma = _ma224_metrics(raw, sd.date().isoformat(), anchor)
    assert ma["ma224_status"] == "PASS_EXACT_RUNTIME_CACHE"
    assert math.isfinite(ma["signal_close_vs_ma224_pct"])
    cache = {f"000001_250_{sd.strftime('%Y%m%d')}": raw}
    fr, key, days = _find_long_cache(cache, "000001", sd.date().isoformat())
    assert len(fr) == len(raw) and days == 250 and key
    print("REAL_FULL_R142_STRUCTURE_OBSERVER_SELF_TEST_PASS")
    return 0


if __name__ == "__main__":
    import sys
    if "--self-test" in sys.argv:
        raise SystemExit(self_test())
    raise SystemExit("R1.4.2 observer is invoked by real_full_capture_runner.py after R1.4.1 serialization.")
