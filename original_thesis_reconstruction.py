from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
import hashlib
import json
import math
import os
import re

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6.25.2.1"
HOTFIX = "HF1_ASOF_EMPTY_FRAME_SCALAR_SAFE"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🧭 [V25 ORIGINAL THESIS RECONSTRUCTION · CORE224 SHADOW · RESEARCH_ONLY]"

STATE_LEDGER_FILE = "v73_v25_core224_state_ledger.csv"
TRANSITION_FILE = "v73_v25_core224_transition_ledger.csv"
INVARIANT_FILE = "v73_v25_core224_invariant_audit.csv"
MANUAL_AUDIT_FILE = "v73_v25_manual_chart_audit_ledger.csv"
MANUAL_SAMPLE_FILE = "v73_v25_manual_chart_review_sample.csv"
AMOUNT_AUTHORITY_FILE = "v73_v25_amount_authority_coverage.csv"
FORMULA_AUDIT_FILE = "v73_v25_formula_audit_01_to_07.csv"
SOURCE_AUDIT_FILE = "v73_v25_current_code_audit.csv"
ACTIVATION_FILE = "v73_v25_activation_status.csv"
UNIVERSE_RECON_FILE = "v73_v25_historical_asof_reconciliation.csv"
PATTERN_TRANSFER_FILE = "v73_v25_pattern_only_transfer_audit.csv"
REPORT_FILE = "v73_v25_original_thesis_report.txt"


CORE_STATES = (
    "CORE224_BASE",
    "CORE224_ACCUMULATION",
    "CORE224_WAVE1",
    "CORE224_FIRST_PULLBACK",
    "CORE224_HEALTHY_PULLBACK",
    "CORE224_RESTART",
)


@dataclass(frozen=True)
class Core224Config:
    """Research defaults encode the reconstructed thesis, not return tuning.

    V25-R2 deliberately records several lenses instead of claiming that one
    numeric boundary is already proven. CORE224_BASE itself remains faithful to
    the user's original statement: long structural bottom AND strict below-MA224
    context. NEAR224/STRUCTURAL lenses are emitted for manual audit comparison.
    """

    ma_period: int = 224
    base_below_lookback: int = 60
    base_below_min_ratio: float = 0.70
    base_low_lookback: int = 120
    base_near_low_max_pct: float = 0.25

    # 6~8 months on a Korean trading calendar is roughly 126~168 sessions.
    structural_bottom_lookback: int = 160
    structural_bottom_max_range_pct: float = 0.45
    structural_bottom_max_location: float = 0.55
    near224_max_above_pct: float = 0.05

    accumulation_short: int = 5
    accumulation_long: int = 20
    accumulation_min_ratio: float = 1.03
    accumulation_max_ratio: float = 2.50
    accumulation_breadth_lookback: int = 10
    accumulation_breadth_min_days: int = 3
    accumulation_explosion_cap: float = 3.00

    # V25.2 data-authority recovery. These values govern evidence availability, not returns.
    actual_amount_min_history_days: int = 20
    actual_amount_fetch_lookback_sessions: int = 320

    wave1_min_gain_pct: float = 0.12
    wave1_min_bars: int = 3
    wave1_long_bullish_5pct: float = 0.05
    wave1_long_bullish_10pct: float = 0.10
    wave1_big_turnover_krw: float = 200_000_000_000.0  # 2,000억

    pullback_trigger_pct: float = 0.05
    pullback_trigger_retrace: float = 0.15
    healthy_retrace_min: float = 0.15
    healthy_retrace_max: float = 0.618
    support_tolerance: float = 0.015
    dry_ratio_max: float = 0.85

    restart_flow_uptick: float = 1.05
    restart_prev_high_tolerance: float = 0.995
    restart_max_above_h1_pct: float = 0.05
    restart_hold_days: int = 0
    restart_cooldown_days: int = 10

    structural_break_retrace: float = 0.70

    # Source-derived top-warning research lens. It is SHADOW ONLY and does not
    # alter the CORE224 state transitions or LIVE behavior.
    top_risk_lookback: int = 160
    top_risk_runup_2x: float = 2.0
    top_risk_runup_3x: float = 3.0
    top_risk_doji_body_ratio_max: float = 0.20
    top_risk_upper_wick_body_mult: float = 1.5
    top_risk_upper_wick_range_min: float = 0.40
    top_risk_gap_min_pct: float = 0.03
    top_risk_bear_body_min_pct: float = 0.05


@dataclass
class AnchorBook:
    l0_idx: Optional[int] = None
    l0_date: str = ""
    l0_low: float = 0.0
    accum_idx: Optional[int] = None
    accum_date: str = ""
    h1_idx: Optional[int] = None
    h1_date: str = ""
    h1_high: float = 0.0
    pullback_idx: Optional[int] = None
    pullback_date: str = ""
    healthy_idx: Optional[int] = None
    healthy_date: str = ""
    restart_idx: Optional[int] = None
    restart_date: str = ""

    def reset_after_base(self) -> None:
        self.h1_idx = None
        self.h1_date = ""
        self.h1_high = 0.0
        self.pullback_idx = None
        self.pullback_date = ""
        self.healthy_idx = None
        self.healthy_date = ""
        self.restart_idx = None
        self.restart_date = ""


ALIASES = {
    "date": ["Date", "date", "일자", "날짜", "asof_date", "signal_date"],
    "open": ["Open", "open", "시가"],
    "high": ["High", "high", "고가"],
    "low": ["Low", "low", "저가"],
    "close": ["Close", "close", "종가", "price", "현재가"],
    "volume": ["Volume", "volume", "거래량"],
    "amount": ["Amount", "amount", "거래대금", "Value", "Turnover"],
    "code": ["Code", "code", "종목코드", "ticker"],
    "name": ["Name", "name", "종목명"],
}


def _find_col(df: pd.DataFrame, key: str) -> Optional[str]:
    for c in ALIASES[key]:
        if c in df.columns:
            return c
    return None


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cols = {k: _find_col(df, k) for k in ALIASES}
    missing = [k for k in ("open", "high", "low", "close", "volume") if not cols[k]]
    if missing:
        raise ValueError(f"missing OHLCV columns: {missing}")
    out = pd.DataFrame(index=df.index.copy())
    for k in ("open", "high", "low", "close", "volume"):
        out[k] = pd.to_numeric(df[cols[k]], errors="coerce")
    if cols["amount"]:
        _raw_amount = pd.to_numeric(df[cols["amount"]], errors="coerce")
        # V25.1: a numeric Amount column alone is NOT enough to call it actual.
        # Require explicit provenance from the PYKRX cross-section/cache overlay.
        if "amount_is_actual" in df.columns:
            _actual_mask = pd.to_numeric(df["amount_is_actual"], errors="coerce").fillna(0).eq(1)
        elif "amount_source" in df.columns:
            _src = df["amount_source"].fillna("").astype(str).str.upper()
            _actual_mask = _src.isin(["ACTUAL", "PYKRX_ACTUAL", "PYKRX_DAILY_CROSS_SECTION", "V25_ACTUAL_OVERLAY"])
        else:
            _actual_mask = pd.Series(False, index=df.index)
        out["amount"] = _raw_amount.where(_actual_mask, np.nan)
        out["amount_source"] = np.where(_actual_mask & _raw_amount.notna(), "ACTUAL_VERIFIED", "UNVERIFIED_OR_MISSING")
    else:
        # Trading value is primary evidence. Close×Volume is never an admission substitute.
        out["amount"] = np.nan
        out["amount_source"] = "MISSING"
    if cols["date"]:
        out["date"] = pd.to_datetime(df[cols["date"]], errors="coerce")
    else:
        out["date"] = pd.to_datetime(df.index, errors="coerce")
    out = out.dropna(subset=["date", "open", "high", "low", "close", "volume"])
    out = out.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    out["amount_valid"] = (out["amount"].notna() & (out["amount"] > 0)).astype(int)
    return out


def _safe_ratio(a: float, b: float, default: float = 0.0) -> float:
    if b is None or not np.isfinite(b) or abs(float(b)) < 1e-12:
        return default
    return float(a) / float(b)


def _fmt_date(v: Any) -> str:
    try:
        return pd.Timestamp(v).strftime("%Y-%m-%d")
    except Exception:
        return ""


def add_trailing_features(df: pd.DataFrame, cfg: Core224Config) -> pd.DataFrame:
    """All features are trailing-only. No centered rolling and no negative shift."""
    h = normalize_ohlcv(df)
    if h.empty:
        return h

    h["ma224"] = h["close"].rolling(cfg.ma_period, min_periods=cfg.ma_period).mean()
    h["ma224_valid"] = h["ma224"].notna()
    h["below_ma224"] = (h["ma224_valid"] & (h["close"] < h["ma224"])).astype(int)
    h["below_ma224_ratio60"] = h["below_ma224"].rolling(
        cfg.base_below_lookback, min_periods=cfg.base_below_lookback
    ).mean()
    h["low120"] = h["low"].rolling(cfg.base_low_lookback, min_periods=cfg.base_low_lookback).min()

    # 6~8 month structural-bottom lens. These are audit fields, not optimized.
    lb = cfg.structural_bottom_lookback
    h["structural_low"] = h["low"].rolling(lb, min_periods=lb).min()
    h["structural_high"] = h["high"].rolling(lb, min_periods=lb).max()
    h["structural_range_pct"] = h["structural_high"] / h["structural_low"].replace(0, np.nan) - 1.0
    denom = (h["structural_high"] - h["structural_low"]).replace(0, np.nan)
    h["structural_location"] = (h["close"] - h["structural_low"]) / denom
    h["structural_bottom_valid"] = h["structural_low"].notna() & h["structural_high"].notna()
    h["structural_bottom_ok"] = (
        h["structural_bottom_valid"]
        & (h["structural_range_pct"] <= cfg.structural_bottom_max_range_pct)
        & (h["structural_location"] <= cfg.structural_bottom_max_location)
    ).astype(int)

    h["base_lens_strict224"] = (h["ma224_valid"] & (h["close"] <= h["ma224"])).astype(int)
    h["base_lens_near224"] = (
        h["ma224_valid"] & (h["close"] <= h["ma224"] * (1.0 + cfg.near224_max_above_pct))
    ).astype(int)
    h["base_lens_structural"] = h["structural_bottom_ok"].astype(int)

    for field in ("volume", "amount"):
        h[f"{field}_ma5"] = h[field].rolling(cfg.accumulation_short, min_periods=cfg.accumulation_short).mean()
        h[f"{field}_ma20"] = h[field].rolling(cfg.accumulation_long, min_periods=cfg.accumulation_long).mean()
        h[f"{field}_ratio5_20"] = h[f"{field}_ma5"] / h[f"{field}_ma20"].replace(0, np.nan)
        h[f"{field}_ratio1_20"] = h[field] / h[f"{field}_ma20"].replace(0, np.nan)
        prev5 = h[field].shift(1).rolling(5, min_periods=3).mean()
        h[f"{field}_soft_inflow"] = (h[field] > prev5 * 1.05).fillna(False)

    # Keep volume breadth for comparison, but amount breadth is the primary lane.
    h["volume_inflow_breadth10"] = h["volume_soft_inflow"].astype(int).rolling(
        cfg.accumulation_breadth_lookback,
        min_periods=cfg.accumulation_breadth_lookback,
    ).sum()
    h["amount_inflow_breadth10"] = h["amount_soft_inflow"].astype(int).rolling(
        cfg.accumulation_breadth_lookback,
        min_periods=cfg.accumulation_breadth_lookback,
    ).sum()
    h["inflow_breadth10"] = h["amount_inflow_breadth10"]  # compatibility alias

    # Top-risk shadow fields.
    t = cfg.top_risk_lookback
    h["toprisk_low"] = h["low"].rolling(t, min_periods=t).min()
    h["runup_from_long_low_x"] = h["high"] / h["toprisk_low"].replace(0, np.nan)
    candle_range = (h["high"] - h["low"]).replace(0, np.nan)
    body = (h["close"] - h["open"]).abs()
    upper_wick = h["high"] - h[["open", "close"]].max(axis=1)
    h["candle_body_ratio"] = body / candle_range
    h["upper_wick_ratio"] = upper_wick / candle_range
    h["toprisk_doji"] = (h["candle_body_ratio"] <= cfg.top_risk_doji_body_ratio_max).fillna(False).astype(int)
    h["toprisk_long_upper_wick"] = (
        (upper_wick >= body * cfg.top_risk_upper_wick_body_mult)
        & (h["upper_wick_ratio"] >= cfg.top_risk_upper_wick_range_min)
    ).fillna(False).astype(int)
    prev_close = h["close"].shift(1)
    gap_pct = h["open"] / prev_close.replace(0, np.nan) - 1.0
    bear_body_pct = (h["open"] - h["close"]) / prev_close.replace(0, np.nan)
    h["toprisk_gap_bear"] = (
        (gap_pct >= cfg.top_risk_gap_min_pct)
        & (h["close"] < h["open"])
        & (bear_body_pct >= cfg.top_risk_bear_body_min_pct)
    ).fillna(False).astype(int)
    h["toprisk_extended_2x"] = (h["runup_from_long_low_x"] >= cfg.top_risk_runup_2x).fillna(False).astype(int)
    h["toprisk_extended_3x"] = (h["runup_from_long_low_x"] >= cfg.top_risk_runup_3x).fillna(False).astype(int)
    h["toprisk_warning"] = (
        (h["toprisk_extended_2x"] == 1)
        & ((h["toprisk_long_upper_wick"] == 1) | (h["toprisk_doji"] == 1) | (h["toprisk_gap_bear"] == 1))
    ).astype(int)

    return h


def _is_base(row: pd.Series, cfg: Core224Config) -> bool:
    if not bool(row.get("ma224_valid", False)):
        return False
    ma224 = float(row["ma224"])
    low120 = float(row.get("low120", np.nan))
    below_ratio = float(row.get("below_ma224_ratio60", np.nan))
    structural = bool(int(row.get("structural_bottom_ok", 0)))
    if not np.isfinite(low120) or not np.isfinite(below_ratio) or ma224 <= 0 or low120 <= 0:
        return False
    strict224 = float(row["close"]) <= ma224
    long_below = below_ratio >= cfg.base_below_min_ratio
    near_long_low = float(row["close"]) <= low120 * (1.0 + cfg.base_near_low_max_pct)
    return bool(strict224 and structural and long_below and near_long_low)


def _is_accum(row: pd.Series, cfg: Core224Config) -> Tuple[bool, Dict[str, bool]]:
    vr = float(row.get("volume_ratio5_20", np.nan))
    ar = float(row.get("amount_ratio5_20", np.nan))
    v1 = float(row.get("volume_ratio1_20", np.nan))
    a1 = float(row.get("amount_ratio1_20", np.nan))
    amount_breadth = float(row.get("amount_inflow_breadth10", np.nan))
    amount_valid = bool(int(row.get("amount_valid", 0)))

    vol_grad = np.isfinite(vr) and cfg.accumulation_min_ratio <= vr <= cfg.accumulation_max_ratio
    amt_grad = np.isfinite(ar) and cfg.accumulation_min_ratio <= ar <= cfg.accumulation_max_ratio
    amount_breadth_ok = np.isfinite(amount_breadth) and amount_breadth >= cfg.accumulation_breadth_min_days
    no_amount_explosion = not np.isfinite(a1) or a1 < cfg.accumulation_explosion_cap
    no_volume_explosion = not np.isfinite(v1) or v1 < cfg.accumulation_explosion_cap

    # V25-R2 source hierarchy: trading value is PRIMARY. Volume is supporting
    # evidence only. Missing actual Amount cannot admit ACCUMULATION.
    ok = bool(amount_valid and amt_grad and amount_breadth_ok and no_amount_explosion)
    return ok, {
        "accum_amount_primary_valid": amount_valid,
        "accum_amount_gradual": bool(amt_grad),
        "accum_amount_breadth_ok": bool(amount_breadth_ok),
        "accum_amount_no_explosion": bool(no_amount_explosion),
        "accum_volume_support_gradual": bool(vol_grad),
        "accum_volume_no_explosion": bool(no_volume_explosion),
    }


def _wave1_observation(h: pd.DataFrame, i: int, a: AnchorBook, cfg: Core224Config) -> Dict[str, Any]:
    r = h.iloc[i]
    prev_close = float(h.iloc[i - 1]["close"]) if i > 0 else float(r["open"])
    day_ret = _safe_ratio(float(r["close"]), prev_close, 1.0) - 1.0
    body_ret = _safe_ratio(float(r["close"]), float(r["open"]), 1.0) - 1.0
    amount = float(r["amount"]) if np.isfinite(float(r.get("amount", np.nan))) else np.nan
    amount_ratio = float(r.get("amount_ratio1_20", np.nan))
    volume_ratio = float(r.get("volume_ratio1_20", np.nan))
    long5 = bool(float(r["close"]) > float(r["open"]) and max(day_ret, body_ret) >= cfg.wave1_long_bullish_5pct)
    long10 = bool(float(r["close"]) > float(r["open"]) and max(day_ret, body_ret) >= cfg.wave1_long_bullish_10pct)
    big200 = bool(np.isfinite(amount) and amount >= cfg.wave1_big_turnover_krw)
    return {
        "wave1_day_return_pct": day_ret * 100.0,
        "wave1_body_return_pct": body_ret * 100.0,
        "wave1_amount_krw": amount,
        "wave1_amount_ratio1_20": amount_ratio,
        "wave1_volume_ratio1_20": volume_ratio,
        "wave1_long_bullish_5pct": int(long5),
        "wave1_long_bullish_10pct": int(long10),
        "wave1_turnover_200bn": int(big200),
        "wave1_big_money_confirmation": int(long5 and big200),
    }


def _sequence_ok(a: AnchorBook, state: str) -> bool:
    # Original thesis authority: BASE → ACCUMULATION → WAVE1 is not optional.
    # L0 may be refreshed while accumulation is forming, but ACCUM must exist before H1.
    if state == "CORE224_BASE":
        return a.l0_idx is not None
    if state == "CORE224_ACCUMULATION":
        return a.l0_idx is not None and a.accum_idx is not None
    if state == "CORE224_WAVE1":
        return (a.l0_idx is not None and a.accum_idx is not None and a.h1_idx is not None
                and a.l0_idx < a.h1_idx and a.accum_idx < a.h1_idx)
    if state == "CORE224_FIRST_PULLBACK":
        return (
            a.l0_idx is not None and a.accum_idx is not None and a.h1_idx is not None and a.pullback_idx is not None
            and a.l0_idx < a.h1_idx < a.pullback_idx and a.accum_idx < a.h1_idx
        )
    if state == "CORE224_HEALTHY_PULLBACK":
        return (
            a.l0_idx is not None and a.accum_idx is not None and a.h1_idx is not None
            and a.pullback_idx is not None and a.healthy_idx is not None
            and a.l0_idx < a.h1_idx < a.pullback_idx <= a.healthy_idx and a.accum_idx < a.h1_idx
        )
    if state == "CORE224_RESTART":
        return (
            a.l0_idx is not None and a.accum_idx is not None and a.h1_idx is not None and a.pullback_idx is not None
            and a.healthy_idx is not None and a.restart_idx is not None
            and a.l0_idx < a.h1_idx < a.pullback_idx <= a.healthy_idx <= a.restart_idx
            and a.accum_idx < a.h1_idx
        )
    return False


def _pullback_metrics(h: pd.DataFrame, i: int, a: AnchorBook, cfg: Core224Config) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "retrace": np.nan,
        "support_floor": np.nan,
        "support_ok": False,
        "volume_dry_ratio": np.nan,
        "amount_dry_ratio": np.nan,
        "volume_dry_ok": False,
        "amount_dry_ok": False,
        "first_pullback_low": np.nan,
    }
    if a.l0_idx is None or a.h1_idx is None or a.pullback_idx is None or a.h1_high <= a.l0_low:
        return out
    wave_range = a.h1_high - a.l0_low
    pb = h.iloc[a.pullback_idx : i + 1]
    w1 = h.iloc[a.l0_idx : a.h1_idx + 1]
    if pb.empty or w1.empty:
        return out
    pb_low = float(pb["low"].min())
    retrace = (a.h1_high - pb_low) / wave_range
    support_floor = a.h1_high - wave_range * cfg.healthy_retrace_max
    support_ok = pb_low >= support_floor * (1.0 - cfg.support_tolerance) and pb_low > a.l0_low
    wv = float(w1["volume"].mean())
    wa = float(w1["amount"].mean())
    pv = float(pb["volume"].mean())
    pa = float(pb["amount"].mean())
    vdry = _safe_ratio(pv, wv, np.nan)
    adry = _safe_ratio(pa, wa, np.nan)
    out.update({
        "retrace": retrace,
        "support_floor": support_floor,
        "support_ok": bool(support_ok),
        "volume_dry_ratio": vdry,
        "amount_dry_ratio": adry,
        "volume_dry_ok": bool(np.isfinite(vdry) and vdry <= cfg.dry_ratio_max),
        "amount_dry_ok": bool(np.isfinite(adry) and adry <= cfg.dry_ratio_max),
        "first_pullback_low": pb_low,
    })
    return out


def _restart_evidence(h: pd.DataFrame, i: int, a: AnchorBook, cfg: Core224Config) -> Dict[str, Any]:
    r = h.iloc[i]
    prev = h.iloc[i - 1] if i > 0 else r
    pb_start = a.pullback_idx if a.pullback_idx is not None else max(0, i - 3)
    prior_pb = h.iloc[pb_start:i]
    if prior_pb.empty:
        prior_pb = h.iloc[max(0, i - 3):i]
    base_v = float(prior_pb["volume"].tail(3).mean()) if not prior_pb.empty else 0.0
    base_a = float(prior_pb["amount"].tail(3).mean()) if not prior_pb.empty else 0.0
    bullish = float(r["close"]) > float(r["open"]) and float(r["close"]) > float(prev["close"])
    prior_high_reclaim = float(r["close"]) >= float(prev["high"]) * cfg.restart_prev_high_tolerance
    # Amount is preferred when valid; volume is allowed as generic restart evidence,
    # but HEALTHY_PULLBACK already required actual Amount dry-up.
    amount_uptick = base_a > 0 and np.isfinite(float(r.get("amount", np.nan))) and float(r["amount"]) >= base_a * cfg.restart_flow_uptick
    volume_uptick = base_v > 0 and float(r["volume"]) >= base_v * cfg.restart_flow_uptick
    flow_uptick = amount_uptick or volume_uptick
    not_too_late = a.h1_high <= 0 or float(r["close"]) <= a.h1_high * (1.0 + cfg.restart_max_above_h1_pct)
    score = int(bullish) + int(prior_high_reclaim) + int(flow_uptick)
    return {
        "restart_bullish": bool(bullish),
        "restart_prev_high_reclaim": bool(prior_high_reclaim),
        "restart_flow_uptick": bool(flow_uptick),
        "restart_amount_uptick": bool(amount_uptick),
        "restart_volume_uptick": bool(volume_uptick),
        "restart_not_too_late": bool(not_too_late),
        "restart_evidence_count": score,
        "restart_ok": bool(score >= 2 and not_too_late),
    }


def evaluate_core224(df: pd.DataFrame, cfg: Optional[Core224Config] = None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (daily_state_rows, transition_events, invariant_rows).

    Guarantees:
    - trailing-only calculations
    - explicit L0/H1/PB/HEALTHY/RESTART sequence
    - CORE state independent from legacy AUX, market context, sector context, and top-risk tags
    - trading value primary for ACCUMULATION and mandatory for HEALTHY_PULLBACK
    """
    cfg = cfg or Core224Config()
    h = add_trailing_features(df, cfg)
    if h.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    state = "NONE"
    a = AnchorBook()
    cooldown_until_idx = -1
    rows: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    invariants: List[Dict[str, Any]] = []
    latest_wave_obs: Dict[str, Any] = {}

    def transition(i: int, new_state: str, reason: str, extra: Optional[Dict[str, Any]] = None) -> None:
        nonlocal state
        old = state
        state = new_state
        rec = {
            "date": _fmt_date(h.iloc[i]["date"]),
            "idx": i,
            "from_state": old,
            "to_state": new_state,
            "reason": reason,
            **asdict(a),
        }
        if extra:
            rec.update(extra)
        events.append(rec)

    for i in range(len(h)):
        r = h.iloc[i]
        if state == "CORE224_RESTART" and a.restart_idx is not None and i > a.restart_idx + cfg.restart_hold_days:
            cooldown_until_idx = max(cooldown_until_idx, i + cfg.restart_cooldown_days - 1)
            state = "NONE"
            a = AnchorBook()
            latest_wave_obs = {}

        base_ok = _is_base(r, cfg)
        accum_ok, accum_flags = _is_accum(r, cfg)
        pbm: Dict[str, Any] = {}
        rst: Dict[str, Any] = {}
        invalid_reason = ""

        if state == "NONE":
            if i <= cooldown_until_idx:
                pass
            elif base_ok:
                a = AnchorBook(l0_idx=i, l0_date=_fmt_date(r["date"]), l0_low=float(r["low"]))
                transition(i, "CORE224_BASE", "structural_bottom_and_strict_below_ma224")

        elif state in ("CORE224_BASE", "CORE224_ACCUMULATION"):
            if a.l0_idx is None or float(r["low"]) < a.l0_low:
                a.l0_idx = i
                a.l0_date = _fmt_date(r["date"])
                a.l0_low = float(r["low"])
                a.reset_after_base()
                latest_wave_obs = {}

            if not base_ok and a.l0_idx is not None and i - a.l0_idx > cfg.base_below_lookback:
                invalid_reason = "base_context_expired_before_wave1"
                state = "NONE"
                a = AnchorBook()
                latest_wave_obs = {}
            elif state == "CORE224_BASE" and accum_ok:
                a.accum_idx = i
                a.accum_date = _fmt_date(r["date"])
                transition(i, "CORE224_ACCUMULATION", "gradual_trading_value_inflow")

            # V25.1 thesis invariant: WAVE1 can never bypass ACCUMULATION and cannot
            # be born on the exact bar that first established accumulation.
            if state == "CORE224_ACCUMULATION" and a.l0_idx is not None and a.accum_idx is not None and i > a.accum_idx:
                bars = i - a.l0_idx
                gain = _safe_ratio(float(r["high"]), a.l0_low, 1.0) - 1.0 if a.l0_low > 0 else 0.0
                if bars >= cfg.wave1_min_bars and gain >= cfg.wave1_min_gain_pct:
                    a.h1_idx = i
                    a.h1_date = _fmt_date(r["date"])
                    a.h1_high = float(r["high"])
                    latest_wave_obs = _wave1_observation(h, i, a, cfg)
                    transition(i, "CORE224_WAVE1", f"post_accum_wave1_gain={gain:.4f}", latest_wave_obs)

        elif state == "CORE224_WAVE1":
            if a.h1_idx is None or float(r["high"]) >= a.h1_high:
                a.h1_idx = i
                a.h1_date = _fmt_date(r["date"])
                a.h1_high = float(r["high"])
                latest_wave_obs = _wave1_observation(h, i, a, cfg)
            if a.l0_idx is not None and a.h1_idx is not None and i > a.h1_idx:
                wave_range = a.h1_high - a.l0_low
                low_retrace = (a.h1_high - float(r["low"])) / wave_range if wave_range > 0 else 0.0
                close_drop = 1.0 - _safe_ratio(float(r["close"]), a.h1_high, 1.0)
                if close_drop >= cfg.pullback_trigger_pct or low_retrace >= cfg.pullback_trigger_retrace:
                    a.pullback_idx = i
                    a.pullback_date = _fmt_date(r["date"])
                    transition(i, "CORE224_FIRST_PULLBACK", "first_pullback_after_frozen_h1", latest_wave_obs)

        elif state == "CORE224_FIRST_PULLBACK":
            if float(r["high"]) > a.h1_high * 1.002:
                a.h1_idx = i
                a.h1_date = _fmt_date(r["date"])
                a.h1_high = float(r["high"])
                a.pullback_idx = None
                a.pullback_date = ""
                latest_wave_obs = _wave1_observation(h, i, a, cfg)
                transition(i, "CORE224_WAVE1", "new_h1_before_healthy_pullback", latest_wave_obs)
            else:
                pbm = _pullback_metrics(h, i, a, cfg)
                retrace = float(pbm.get("retrace", np.nan))
                if np.isfinite(retrace) and (
                    retrace >= cfg.structural_break_retrace
                    or (not bool(pbm.get("support_ok", False)) and retrace > cfg.healthy_retrace_max + 0.05)
                ):
                    invalid_reason = "first_pullback_broke_wave1_structure"
                    state = "NONE"
                    a = AnchorBook()
                    latest_wave_obs = {}
                elif (
                    np.isfinite(retrace)
                    and cfg.healthy_retrace_min <= retrace <= cfg.healthy_retrace_max
                    and bool(pbm.get("support_ok", False))
                    and bool(pbm.get("volume_dry_ok", False))
                    and bool(pbm.get("amount_dry_ok", False))
                ):
                    a.healthy_idx = i
                    a.healthy_date = _fmt_date(r["date"])
                    transition(i, "CORE224_HEALTHY_PULLBACK", "first_pullback_dry_and_support_preserved", latest_wave_obs)

        elif state == "CORE224_HEALTHY_PULLBACK":
            pbm = _pullback_metrics(h, i, a, cfg)
            retrace = float(pbm.get("retrace", np.nan))
            if np.isfinite(retrace) and (retrace >= cfg.structural_break_retrace or not bool(pbm.get("support_ok", False))):
                invalid_reason = "healthy_pullback_lost_support"
                state = "NONE"
                a = AnchorBook()
                latest_wave_obs = {}
            else:
                rst = _restart_evidence(h, i, a, cfg)
                if bool(rst.get("restart_ok", False)):
                    a.restart_idx = i
                    a.restart_date = _fmt_date(r["date"])
                    transition(i, "CORE224_RESTART", "price_action_plus_flow_restart", {**latest_wave_obs, **rst})

        elif state == "CORE224_RESTART":
            pbm = _pullback_metrics(h, i, a, cfg) if a.pullback_idx is not None else {}
            rst = _restart_evidence(h, i, a, cfg) if i > 0 else {}

        seq_ok = _sequence_ok(a, state) if state != "NONE" else True
        if state != "NONE" and not seq_ok:
            invariants.append({
                "date": _fmt_date(r["date"]),
                "idx": i,
                "state": state,
                "invariant": "BASE→ACCUM; ACCUM<H1; L0<H1<PULLBACK<=HEALTHY<=RESTART",
                "ok": 0,
                **asdict(a),
            })

        latest_event = events[-1] if events and events[-1]["idx"] == i else None
        state_start_idx = None
        if state == "CORE224_BASE":
            state_start_idx = a.l0_idx
        elif state == "CORE224_ACCUMULATION":
            ev = next((x for x in reversed(events) if x["to_state"] == state), None)
            state_start_idx = ev["idx"] if ev else i
        elif state == "CORE224_WAVE1":
            state_start_idx = a.h1_idx
        elif state == "CORE224_FIRST_PULLBACK":
            state_start_idx = a.pullback_idx
        elif state == "CORE224_HEALTHY_PULLBACK":
            state_start_idx = a.healthy_idx
        elif state == "CORE224_RESTART":
            state_start_idx = a.restart_idx
        state_age = i - state_start_idx if state_start_idx is not None else -1

        wave_obs_now = latest_wave_obs if latest_wave_obs else {
            "wave1_day_return_pct": np.nan,
            "wave1_body_return_pct": np.nan,
            "wave1_amount_krw": np.nan,
            "wave1_amount_ratio1_20": np.nan,
            "wave1_volume_ratio1_20": np.nan,
            "wave1_long_bullish_5pct": 0,
            "wave1_long_bullish_10pct": 0,
            "wave1_turnover_200bn": 0,
            "wave1_big_money_confirmation": 0,
        }

        rows.append({
            "date": _fmt_date(r["date"]),
            "idx": i,
            "core224_state": state,
            "core224_transition": int(latest_event is not None),
            "core224_state_age": state_age,
            "core224_sequence_ok": int(seq_ok),
            "ma224": float(r["ma224"]) if bool(r["ma224_valid"]) else np.nan,
            "amount_valid": int(r.get("amount_valid", 0)),
            "amount_source": str(r.get("amount_source", "MISSING")),
            "ma224_valid": int(bool(r["ma224_valid"])),
            "close_vs_ma224_pct": (
                (_safe_ratio(float(r["close"]), float(r["ma224"]), np.nan) - 1.0) * 100.0
                if bool(r["ma224_valid"]) else np.nan
            ),
            "below_ma224_ratio60": float(r.get("below_ma224_ratio60", np.nan)),
            "base_lens_strict224": int(r.get("base_lens_strict224", 0)),
            "base_lens_near224": int(r.get("base_lens_near224", 0)),
            "base_lens_structural": int(r.get("base_lens_structural", 0)),
            "structural_bottom_valid": int(bool(r.get("structural_bottom_valid", False))),
            "structural_bottom_ok": int(r.get("structural_bottom_ok", 0)),
            "structural_range_pct": float(r.get("structural_range_pct", np.nan)),
            "structural_location": float(r.get("structural_location", np.nan)),
            "base_ok": int(base_ok),
            "accum_ok": int(accum_ok),
            **{k: int(v) for k, v in accum_flags.items()},
            "volume_ratio5_20": float(r.get("volume_ratio5_20", np.nan)),
            "amount_ratio5_20": float(r.get("amount_ratio5_20", np.nan)),
            "volume_inflow_breadth10": float(r.get("volume_inflow_breadth10", np.nan)),
            "amount_inflow_breadth10": float(r.get("amount_inflow_breadth10", np.nan)),
            "l0_date": a.l0_date,
            "l0_low": a.l0_low,
            "h1_date": a.h1_date,
            "h1_high": a.h1_high,
            "pullback_date": a.pullback_date,
            "healthy_date": a.healthy_date,
            "restart_date": a.restart_date,
            **wave_obs_now,
            "pullback_retrace": pbm.get("retrace", np.nan),
            "pullback_support_floor": pbm.get("support_floor", np.nan),
            "pullback_support_ok": int(bool(pbm.get("support_ok", False))),
            "pullback_volume_dry_ratio": pbm.get("volume_dry_ratio", np.nan),
            "pullback_amount_dry_ratio": pbm.get("amount_dry_ratio", np.nan),
            "pullback_volume_dry_ok": int(bool(pbm.get("volume_dry_ok", False))),
            "pullback_amount_dry_ok": int(bool(pbm.get("amount_dry_ok", False))),
            "restart_evidence_count": rst.get("restart_evidence_count", 0),
            "restart_bullish": int(bool(rst.get("restart_bullish", False))),
            "restart_prev_high_reclaim": int(bool(rst.get("restart_prev_high_reclaim", False))),
            "restart_flow_uptick": int(bool(rst.get("restart_flow_uptick", False))),
            "restart_amount_uptick": int(bool(rst.get("restart_amount_uptick", False))),
            "restart_volume_uptick": int(bool(rst.get("restart_volume_uptick", False))),
            "runup_from_long_low_x": float(r.get("runup_from_long_low_x", np.nan)),
            "toprisk_extended_2x": int(r.get("toprisk_extended_2x", 0)),
            "toprisk_extended_3x": int(r.get("toprisk_extended_3x", 0)),
            "toprisk_doji": int(r.get("toprisk_doji", 0)),
            "toprisk_long_upper_wick": int(r.get("toprisk_long_upper_wick", 0)),
            "toprisk_gap_bear": int(r.get("toprisk_gap_bear", 0)),
            "toprisk_warning": int(r.get("toprisk_warning", 0)),
            "invalid_reason": invalid_reason,
        })

    inv = pd.DataFrame(invariants)
    if inv.empty:
        inv = pd.DataFrame(columns=["date", "idx", "state", "invariant", "ok"])
    return pd.DataFrame(rows), pd.DataFrame(events), inv


def latest_core224(df: pd.DataFrame, cfg: Optional[Core224Config] = None) -> Dict[str, Any]:
    rows, events, inv = evaluate_core224(df, cfg)
    if rows.empty:
        return {"core224_state": "NONE", "ma224_valid": 0, "core224_sequence_ok": 0}
    out = rows.iloc[-1].to_dict()
    out["event_count"] = int(len(events))
    out["invariant_fail_count"] = int(len(inv))
    return out

# ============================================================
# V25 integration helpers: materialized shard evidence only.
# ============================================================

_MARKET_INDEX_CACHE: Dict[str, pd.DataFrame] = {}


def _out_dir(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _norm_code(v: Any) -> str:
    """Canonical KRX ticker identity; preserves 6-char alphanumeric tickers (e.g. 0126Z0)."""
    raw = str(v or "").strip().upper()
    if raw.endswith(".0") and raw[:-2].isdigit():
        raw = raw[:-2]
    for suffix in (".KS", ".KQ", ".KRX"):
        if raw.endswith(suffix):
            raw = raw[:-len(suffix)]
            break
    s = "".join(ch for ch in raw if ch in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    if len(s) == 7 and s.startswith("A"):
        s = s[1:]
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    if len(s) >= 6:
        return s[-6:]
    return s

def _pick_col(df: pd.DataFrame, names: Iterable[str]) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None


def _col_series(df: pd.DataFrame, name: str, default: Any = np.nan) -> pd.Series:
    """Always return an index-aligned Series; never leak scalar DataFrame.get defaults.

    V25.2.1 HF1: pandas DataFrame.get(name, 0) returns the scalar integer 0 when the
    column is absent. Chaining .fillna/.eq/.any on that scalar crashed a whole shard.
    Research-side missing columns must remain row-local evidence gaps, not shard-fatal.
    """
    if isinstance(df, pd.DataFrame):
        if name in df.columns:
            obj = df[name]
            if isinstance(obj, pd.Series):
                return obj
            # Duplicate column names can return a DataFrame; fail closed to a default Series.
            return pd.Series(default, index=df.index)
        return pd.Series(default, index=df.index)
    return pd.Series(dtype=float)


def _num_col(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(_col_series(df, name, default), errors="coerce").fillna(default)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_csv(path, index=False, encoding="utf-8-sig")


def _amount_cache_files(output_dir: str | Path, asof_date: Any, max_files: int = 90) -> List[Path]:
    root = Path(output_dir or "reports") / ".cache" / "v20_asof_snapshots" / "market"
    if not root.exists():
        return []
    asof = pd.Timestamp(asof_date).normalize()
    found: List[Tuple[pd.Timestamp, Path]] = []
    for p in root.glob("*.csv.gz"):
        try:
            d = pd.Timestamp(p.name.split(".")[0]).normalize()
        except Exception:
            continue
        if d <= asof:
            found.append((d, p))
    found.sort(key=lambda x: x[0])
    return [p for _, p in found[-max(20, int(max_files)):]]


def load_cached_amount_panel(output_dir: str | Path, asof_date: Any, codes: Iterable[Any], max_files: int = 90) -> pd.DataFrame:
    """Read only already-materialized PYKRX daily cross-sections.

    This function never downloads. It gives CORE224 actual trading-value evidence where the
    Historical-AsOf lane has already cached it. Missing dates remain UNKNOWN rather than being
    replaced by Close×Volume.
    """
    wanted = {_norm_code(c) for c in codes if _norm_code(c)}
    frames: List[pd.DataFrame] = []
    for p in _amount_cache_files(output_dir, asof_date, max_files=max_files):
        try:
            q = pd.read_csv(p, dtype={"code": str, "Code": str})
        except Exception:
            continue
        cc = _pick_col(q, ["code", "Code", "티커", "종목코드"])
        ac = _pick_col(q, ["amount", "Amount", "거래대금", "거래대금(원)"])
        vc = _pick_col(q, ["volume", "Volume", "거래량"])
        if not cc or not ac:
            continue
        q = q.copy()
        q["code"] = q[cc].map(_norm_code)
        q = q[q["code"].isin(wanted)]
        # Only explicit actual trading value may advance CORE224. Legacy Close×Volume cache
        # rows are intentionally UNKNOWN for V25 admission.
        if "amount_is_actual" in q.columns:
            q = q[pd.to_numeric(q["amount_is_actual"], errors="coerce").fillna(0).eq(1)]
        else:
            q = q.iloc[0:0]
        if q.empty:
            continue
        try:
            ds = pd.Timestamp(p.name.split(".")[0]).normalize()
        except Exception:
            continue
        z = pd.DataFrame({
            "date": ds,
            "code": q["code"],
            "actual_amount": pd.to_numeric(q[ac], errors="coerce"),
            "actual_volume_snapshot": pd.to_numeric(q[vc], errors="coerce") if vc else np.nan,
        })
        frames.append(z)
    if not frames:
        return pd.DataFrame(columns=["date", "code", "actual_amount", "actual_volume_snapshot"])
    x = pd.concat(frames, ignore_index=True)
    x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.normalize()
    x = x.dropna(subset=["date"]).drop_duplicates(["date", "code"], keep="last")
    return x.sort_values(["code", "date"], kind="stable")


def _ticker_amount_cache_path(output_dir: str | Path, code: str) -> Path:
    root = Path(output_dir or "reports") / ".cache" / "v25_actual_amount_history"
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{_norm_code(code)}.csv.gz"


def _normalize_ticker_amount_history(raw: pd.DataFrame, code: str, source: str = "PYKRX_TICKER_HISTORY_REPORTED") -> pd.DataFrame:
    cols = ["date","code","actual_amount","actual_volume_snapshot","amount_source","amount_is_actual"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=cols)
    q=raw.copy()
    if q.index.name is not None or not isinstance(q.index,pd.RangeIndex):
        q=q.reset_index()
    dc=_pick_col(q,["날짜","Date","date","일자","index"])
    ac=_pick_col(q,["거래대금","Amount","amount","거래대금(원)"])
    vc=_pick_col(q,["거래량","Volume","volume"])
    if not dc or not ac:
        return pd.DataFrame(columns=cols)
    out=pd.DataFrame({
        "date":pd.to_datetime(q[dc],errors="coerce").dt.normalize(),
        "code":_norm_code(code),
        "actual_amount":pd.to_numeric(q[ac],errors="coerce"),
        "actual_volume_snapshot":pd.to_numeric(q[vc],errors="coerce") if vc else np.nan,
    })
    out=out[out["date"].notna() & out["actual_amount"].notna() & out["actual_amount"].ge(0)].copy()
    out["amount_source"]=str(source or "PYKRX_TICKER_HISTORY_REPORTED")
    out["amount_is_actual"]=1
    return out.drop_duplicates(["date","code"],keep="last").sort_values("date",kind="stable")


def _read_ticker_amount_cache(output_dir: str | Path, code: str) -> pd.DataFrame:
    p=_ticker_amount_cache_path(output_dir,code)
    if not p.exists():
        return pd.DataFrame(columns=["date","code","actual_amount","actual_volume_snapshot","amount_source","amount_is_actual"])
    try:
        q=pd.read_csv(p,dtype={"code":str})
        q["date"]=pd.to_datetime(q.get("date"),errors="coerce").dt.normalize()
        q["code"]=q.get("code",pd.Series(_norm_code(code),index=q.index)).map(_norm_code)
        q["actual_amount"]=pd.to_numeric(q.get("actual_amount"),errors="coerce")
        q["amount_is_actual"]=_num_col(q,"amount_is_actual",1).astype(int)
        return q[q["date"].notna() & q["amount_is_actual"].eq(1)].drop_duplicates(["date","code"],keep="last")
    except Exception:
        return pd.DataFrame(columns=["date","code","actual_amount","actual_volume_snapshot","amount_source","amount_is_actual"])


def _write_ticker_amount_cache(output_dir: str | Path, code: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    p=_ticker_amount_cache_path(output_dir,code); p.parent.mkdir(parents=True,exist_ok=True)
    old=_read_ticker_amount_cache(output_dir,code)
    q=df.copy() if old is None or old.empty else pd.concat([old,df],ignore_index=True,sort=False)
    q["date"]=pd.to_datetime(q["date"],errors="coerce").dt.normalize()
    q=q[q["date"].notna()].drop_duplicates(["date","code"],keep="last").sort_values("date",kind="stable")
    tmp=p.with_name(p.name+f".{os.getpid()}.tmp")
    q.to_csv(tmp,index=False,compression="gzip")
    os.replace(tmp,p)


def recover_ticker_actual_amount_history(
    output_dir: str | Path, code: str, start_date: Any, end_date: Any,
    reader: Optional[Callable[..., pd.DataFrame]], min_days: int = 20,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Recover actual reported turnover for one potential CORE224 name only.

    This is a secondary authority lane used when all-market daily snapshots are missing. It can
    unlock CORE224 accumulation evidence but never upgrades Historical-AsOf TOP500 membership,
    because a per-ticker history cannot prove the all-market denominator.
    """
    code=_norm_code(code); st=pd.Timestamp(start_date).normalize(); en=pd.Timestamp(end_date).normalize()
    cached=_read_ticker_amount_cache(output_dir,code)
    use=cached[(cached["date"].ge(st)) & (cached["date"].le(en))].copy() if not cached.empty else cached
    if len(use)>=int(min_days):
        return use,{"fetch_status":"CACHE_HIT","fetch_rows":0,"authority_rows":len(use),"source":"V25_TICKER_AMOUNT_CACHE"}
    if not callable(reader):
        return use,{"fetch_status":"NO_READER","fetch_rows":0,"authority_rows":len(use),"source":"MISSING"}
    try:
        raw=reader(code,st,en)
        got=_normalize_ticker_amount_history(raw,code)
    except Exception as exc:
        return use,{"fetch_status":f"FETCH_ERROR:{type(exc).__name__}","fetch_rows":0,"authority_rows":len(use),"source":"MISSING"}
    if not got.empty:
        _write_ticker_amount_cache(output_dir,code,got)
        cached=_read_ticker_amount_cache(output_dir,code)
        use=cached[(cached["date"].ge(st)) & (cached["date"].le(en))].copy()
        return use,{"fetch_status":"FETCHED","fetch_rows":len(got),"authority_rows":len(use),"source":"PYKRX_TICKER_HISTORY_REPORTED"}
    return use,{"fetch_status":"FETCH_EMPTY","fetch_rows":0,"authority_rows":len(use),"source":"MISSING"}


def _overlay_actual_amount(price_df: pd.DataFrame, code: str, amount_panel: pd.DataFrame) -> pd.DataFrame:
    """Overlay only provenance-verified actual trading value for CORE224.

    A price reader may expose an Amount-like column whose provenance is unknown. V25.1 does
    not let that value enter accumulation/pullback admission unless an explicit actual flag/source
    is present. PYKRX cached cross-sections are the normal authority.
    """
    if price_df is None or price_df.empty:
        return pd.DataFrame()
    q = price_df.copy()
    q.index = pd.to_datetime(q.index, errors="coerce")
    q = q[q.index.notna()].sort_index()
    existing = _pick_col(q, ["Amount", "amount", "거래대금", "Value", "Turnover"])
    raw_amount = pd.to_numeric(q[existing], errors="coerce") if existing else pd.Series(np.nan, index=q.index, dtype=float)
    if "amount_is_actual" in q.columns:
        actual_mask = pd.to_numeric(q["amount_is_actual"], errors="coerce").fillna(0).eq(1)
    elif "amount_source" in q.columns:
        src = q["amount_source"].fillna("").astype(str).str.upper()
        actual_mask = src.isin(["ACTUAL", "PYKRX_ACTUAL", "PYKRX_DAILY_CROSS_SECTION", "V25_ACTUAL_OVERLAY"])
    else:
        actual_mask = pd.Series(False, index=q.index)
    q["Amount"] = raw_amount.where(actual_mask, np.nan)
    q["amount_is_actual"] = actual_mask.astype(int)
    q["amount_source"] = np.where(actual_mask & raw_amount.notna(), "ACTUAL_VERIFIED_INPUT", "MISSING_OR_UNVERIFIED")
    if amount_panel is None or amount_panel.empty:
        return q
    a = amount_panel[amount_panel["code"].eq(_norm_code(code))].copy()
    if a.empty:
        return q
    amap = a.set_index("date")["actual_amount"]
    normalized = pd.DatetimeIndex(q.index).normalize()
    overlay = pd.Series(normalized.map(amap), index=q.index, dtype=float)
    omask = overlay.notna() & overlay.gt(0)
    q.loc[omask, "Amount"] = overlay.loc[omask]
    q.loc[omask, "amount_is_actual"] = 1
    q.loc[omask, "amount_source"] = "V25_ACTUAL_OVERLAY"
    return q


def _market_context_for_date(asof_date: Any, market: str, market_index_reader: Optional[Callable[..., pd.DataFrame]]) -> Dict[str, Any]:
    m = str(market or "").upper()
    if "KOSDAQ" in m or "코스닥" in m:
        key, symbol = "KOSDAQ", "KQ11"
    elif "KOSPI" in m or "코스피" in m or "유가" in m:
        key, symbol = "KOSPI", "KS11"
    else:
        return {"market_context": "MARKET_UNKNOWN", "market_context_known": 0, "market_position252": np.nan}
    if not callable(market_index_reader):
        return {"market_context": "MARKET_UNKNOWN", "market_context_known": 0, "market_position252": np.nan}
    if key not in _MARKET_INDEX_CACHE:
        try:
            asof = pd.Timestamp(asof_date).normalize()
            start = (asof - pd.Timedelta(days=700)).strftime("%Y-%m-%d")
            try:
                z = market_index_reader(symbol, start=start)
            except TypeError:
                z = market_index_reader(symbol, start)
            if isinstance(z, pd.DataFrame) and not z.empty:
                zz = z.copy(); zz.index = pd.to_datetime(zz.index, errors="coerce")
                _MARKET_INDEX_CACHE[key] = zz[zz.index.notna()].sort_index()
            else:
                _MARKET_INDEX_CACHE[key] = pd.DataFrame()
        except Exception:
            _MARKET_INDEX_CACHE[key] = pd.DataFrame()
    z = _MARKET_INDEX_CACHE.get(key, pd.DataFrame())
    if z.empty:
        return {"market_context": "MARKET_UNKNOWN", "market_context_known": 0, "market_position252": np.nan}
    asof = pd.Timestamp(asof_date).normalize()
    g = z[z.index <= asof].tail(252)
    ccol = _pick_col(g, ["Close", "close", "종가"])
    if not ccol or len(g) < 120:
        return {"market_context": "MARKET_UNKNOWN", "market_context_known": 0, "market_position252": np.nan}
    s = pd.to_numeric(g[ccol], errors="coerce").dropna()
    if len(s) < 120:
        return {"market_context": "MARKET_UNKNOWN", "market_context_known": 0, "market_position252": np.nan}
    lo, hi, cur = float(s.min()), float(s.max()), float(s.iloc[-1])
    pos = (cur-lo)/(hi-lo) if hi > lo else np.nan
    ctx = "MARKET_UNKNOWN"
    if np.isfinite(pos):
        ctx = "MARKET_LOW" if pos <= 0.30 else ("MARKET_HIGH" if pos >= 0.70 else "MARKET_NEUTRAL")
    return {"market_context": ctx, "market_context_known": int(np.isfinite(pos)), "market_position252": pos}


def build_date_sidecar(
    asof_date: Any,
    membership: pd.DataFrame,
    price_reader: Callable[..., pd.DataFrame],
    output_dir: str | Path = "reports",
    sector_map: Optional[Dict[str, str]] = None,
    market_index_reader: Optional[Callable[..., pd.DataFrame]] = None,
    actual_amount_history_reader: Optional[Callable[..., pd.DataFrame]] = None,
    log_fn: Optional[Callable[[str], Any]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Evaluate the reconstructed thesis for the authoritative materialized universe.

    It is called only in a V23 shard worker before materialization. It never mutates candidate
    rows, scores, ranks, entries, exits, or orders. Parent later consumes the sidecar only.
    """
    if membership is None or membership.empty or not callable(price_reader):
        return {"V25_CORE224_ROWS": [], "V25_CORE224_EVENTS": [], "V25_CORE224_INVARIANTS": []}
    m = membership.copy()
    cc = _pick_col(m, ["code", "Code", "종목코드"])
    nc = _pick_col(m, ["name", "Name", "종목명"])
    mk = _pick_col(m, ["market", "Market", "시장"])
    if not cc:
        return {"V25_CORE224_ROWS": [], "V25_CORE224_EVENTS": [], "V25_CORE224_INVARIANTS": []}
    m["_code"] = m[cc].map(_norm_code)
    m = m[m["_code"].ne("")].drop_duplicates("_code", keep="last")
    codes = m["_code"].tolist()
    amount_panel = load_cached_amount_panel(output_dir, asof_date, codes, max_files=90)
    asof = pd.Timestamp(asof_date).normalize()
    state_rows: List[Dict[str, Any]] = []
    event_rows: List[Dict[str, Any]] = []
    inv_rows: List[Dict[str, Any]] = []
    total = len(m)
    for pos, (_, meta) in enumerate(m.iterrows(), start=1):
        code = str(meta["_code"])
        name = str(meta[nc] if nc else "")
        market = str(meta[mk] if mk else "")
        sector = str((sector_map or {}).get(code, "") or "")
        try:
            raw = price_reader(code, days=900)
        except TypeError:
            try: raw = price_reader(code, 900)
            except Exception: raw = pd.DataFrame()
        except Exception:
            raw = pd.DataFrame()
        if not isinstance(raw, pd.DataFrame) or raw.empty:
            state_rows.append({
                "version": VERSION, "signal_date": asof.strftime("%Y-%m-%d"), "code": code, "name": name,
                "market": market, "sector": sector, "core224_state": "NONE", "evidence_status": "NO_PRICE_HISTORY",
                "research_only": True, "live_logic_changed": False, "real_order_changed": False,
            })
            continue
        q = raw.copy(); q.index = pd.to_datetime(q.index, errors="coerce")
        q = q[q.index.notna() & (q.index <= asof)].sort_index().tail(900)
        # V25.2.1 HF1: a provider can return a non-empty frame whose entire history is
        # newer than this historical as-of date (e.g. later listing). That is a valid
        # row-level evidence gap, not a shard failure. Never pass an empty frame into
        # scalar-default DataFrame.get chains.
        if q.empty:
            state_rows.append({
                "version": VERSION, "signal_date": asof.strftime("%Y-%m-%d"), "code": code, "name": name,
                "market": market, "sector": sector, "core224_state": "NONE",
                "evidence_status": "NO_PRICE_HISTORY_ASOF", "amount_is_actual": 0,
                "actual_amount_observation_days": 0, "actual_amount_history_ready20": 0,
                "amount_authority_fetch_status": "NOT_APPLICABLE_NO_PRICE_ASOF",
                "amount_authority_fetch_rows": 0, "amount_authority_source": "MISSING",
                "research_only": True, "live_logic_changed": False, "real_order_changed": False,
            })
            continue
        q = _overlay_actual_amount(q, code, amount_panel)
        amount_fetch_meta={"fetch_status":"NOT_NEEDED","fetch_rows":0,"authority_rows":int(_num_col(q,"amount_is_actual",0).eq(1).sum()),"source":"CROSS_SECTION_OR_INPUT"}
        try:
            daily, events, inv = evaluate_core224(q)
            # V25.2 two-pass authority recovery: only names that ever satisfy the price/MA224 base
            # lens are allowed to trigger a per-ticker turnover request. This avoids thousands of
            # unnecessary calls while still recovering names that could progress once actual flow
            # evidence exists.
            actual_days=int(_num_col(q,"amount_is_actual",0).eq(1).sum())
            potential_base=bool(
                (not daily.empty) and (
                    daily.get("core224_state",pd.Series(dtype=str)).astype(str).eq("CORE224_BASE").any()
                    or _num_col(daily,"base_lens_strict224",0).eq(1).any()
                    or _num_col(daily,"base_lens_structural",0).eq(1).any()
                )
            )
            if actual_days < Core224Config().actual_amount_min_history_days and potential_base and callable(actual_amount_history_reader):
                lookback=max(40,int(Core224Config().actual_amount_fetch_lookback_sessions))
                _dates=pd.to_datetime(_col_series(daily,"date",pd.NaT),errors="coerce").dropna()
                _start=_dates.iloc[max(0,len(_dates)-lookback)] if len(_dates) else max(q.index.min(),asof-pd.Timedelta(days=500))
                recovered,amount_fetch_meta=recover_ticker_actual_amount_history(
                    output_dir,code,_start,asof,actual_amount_history_reader,
                    min_days=Core224Config().actual_amount_min_history_days)
                if recovered is not None and not recovered.empty:
                    q=_overlay_actual_amount(q,code,recovered)
                    daily,events,inv=evaluate_core224(q)
        except Exception as exc:
            state_rows.append({
                "version": VERSION, "signal_date": asof.strftime("%Y-%m-%d"), "code": code, "name": name,
                "market": market, "sector": sector, "core224_state": "NONE", "evidence_status": f"EVAL_ERROR:{type(exc).__name__}",
                "research_only": True, "live_logic_changed": False, "real_order_changed": False,
            })
            continue
        if daily.empty:
            continue
        r = daily.iloc[-1].to_dict()
        latest_price_date = pd.to_datetime(daily.iloc[-1].get("date"), errors="coerce")
        r.update({
            "version": VERSION, "signal_date": asof.strftime("%Y-%m-%d"), "code": code, "name": name,
            "market": market, "sector": sector,
            "universe_rank": meta.get("universe_rank", np.nan), "universe_source": meta.get("universe_source", ""),
            "avg_amount20_asof_d1": meta.get("avg_amount20", np.nan), "prev_amount_asof_d1": meta.get("prev_amount", np.nan),
            "amount_ratio_prev_vs20": meta.get("amount_ratio_prev_vs20", np.nan),
            "latest_price_date": latest_price_date.strftime("%Y-%m-%d") if pd.notna(latest_price_date) else "",
            "signal_date_price_present": int(pd.notna(latest_price_date) and latest_price_date.normalize() == asof),
            "actual_amount_observation_days": int(_num_col(q,"amount_is_actual",0).eq(1).sum()),
            "actual_amount_history_ready20": int(_num_col(q,"amount_is_actual",0).eq(1).sum() >= Core224Config().actual_amount_min_history_days),
            "amount_authority_fetch_status": amount_fetch_meta.get("fetch_status","UNKNOWN"),
            "amount_authority_fetch_rows": amount_fetch_meta.get("fetch_rows",0),
            "amount_authority_source": amount_fetch_meta.get("source","MISSING"),
            "evidence_status": "CORE224_SHADOW_VALID" if int(r.get("ma224_valid",0) or 0) else "MA224_HISTORY_INCOMPLETE",
            "research_only": True, "live_logic_changed": False, "real_order_changed": False,
        })
        try:
            if len(q) >= 2:
                close = pd.to_numeric(q["Close"] if "Close" in q.columns else q["close"], errors="coerce")
                r["day_return_pct"] = (float(close.iloc[-1]) / float(close.iloc[-2]) - 1.0) * 100.0 if float(close.iloc[-2]) else np.nan
            else: r["day_return_pct"] = np.nan
        except Exception: r["day_return_pct"] = np.nan
        r.update(_market_context_for_date(asof, market, market_index_reader))
        state_rows.append(r)
        if not events.empty:
            ee = events.copy()
            ee["event_date"] = pd.to_datetime(ee["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            ee = ee[pd.to_datetime(ee["date"], errors="coerce").le(asof)]
            for z in ee.to_dict("records"):
                z.update({"version": VERSION, "signal_date": asof.strftime("%Y-%m-%d"), "code": code, "name": name, "market": market, "sector": sector, "research_only": True})
                event_rows.append(z)
        if not inv.empty:
            for z in inv.to_dict("records"):
                z.update({"version": VERSION, "signal_date": asof.strftime("%Y-%m-%d"), "code": code, "name": name, "market": market, "sector": sector, "research_only": True})
                inv_rows.append(z)
        if callable(log_fn) and (pos % 100 == 0 or pos == total):
            try: log_fn(f"🧭 [V25 CORE224] {asof.strftime('%Y-%m-%d')} {pos}/{total} rows={len(state_rows)} events={len(event_rows)} inv_fail={len(inv_rows)}")
            except Exception: pass

    # Same-sector co-rise is an observation lens, not a CORE gate. Threshold 60% is explicitly
    # labelled HEURISTIC so it cannot be confused with reconstructed source truth.
    if state_rows:
        s = pd.DataFrame(state_rows)
        if "sector" in s.columns:
            ret = _num_col(s,"day_return_pct",np.nan)
            s["_up"] = ret.gt(0)
            stats = s[s["sector"].fillna("").astype(str).str.len().gt(0)].groupby("sector", dropna=False).agg(
                sector_peer_count=("code","nunique"), sector_peer_up_count=("_up","sum")
            ).reset_index()
            stats["sector_peer_up_ratio"] = stats["sector_peer_up_count"] / stats["sector_peer_count"].replace(0,np.nan)
            s = s.merge(stats, on="sector", how="left")
            s["sector_context_known"] = (_num_col(s,"sector_peer_count",0) >= 3).astype(int)
            s["sector_confirmation_heuristic60"] = np.where(
                s["sector_context_known"].eq(1),
                (_num_col(s,"sector_peer_up_ratio",np.nan) >= 0.60).astype(int),
                np.nan,
            )
            s["sector_confirmation_role"] = "AUX_OBSERVATION_NON_GATING"
            state_rows = s.drop(columns=["_up"], errors="ignore").to_dict("records")
    return {"V25_CORE224_ROWS": state_rows, "V25_CORE224_EVENTS": event_rows, "V25_CORE224_INVARIANTS": inv_rows}


_AUDIT_TARGETS = [
    ("MA224", [r"MA224", r"rolling\(224", r"Below_MA224"]),
    ("DOLBANJI", [r"Dolbanzi", r"돌반지"]),
    ("BLUE_LINE", [r"파란점선", r"blue_line", r"Blue-1", r"Blue-2"]),
    ("FORCE_PULLBACK", [r"check_force_pullback", r"세력\s*눌림목"]),
    ("WAVE1_ANCHOR", [r"pivot_low", r"pivot_high", r"base_low", r"first_wave", r"1파"]),
    ("PATTERN_ONLY", [r"PATTERN_ONLY", r"pattern_only"]),
    ("STALE_V22", [r"V22 병렬진단", r"TOP500 4-Shard"]),
]


def audit_source(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=["target","file","line","code","risk"])
    lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    rows: List[Dict[str, Any]] = []
    for target, pats in _AUDIT_TARGETS:
        for n, line in enumerate(lines, 1):
            if any(re.search(pat, line, re.I) for pat in pats):
                rows.append({"target": target, "file": p.name, "line": n, "code": line.strip()[:600], "risk": ""})
    for n, line in enumerate(lines, 1):
        risks = []
        compact = line.replace(" ", "")
        if "center=True" in compact:
            risks.append("CENTERED_ROLLING_LOOKAHEAD")
        if re.search(r"\.shift\(\s*-\d+", line):
            risks.append("NEGATIVE_SHIFT_LOOKAHEAD")
        if risks:
            rows.append({"target":"LOOKAHEAD_RISK","file":p.name,"line":n,"code":line.strip()[:600],"risk":"|".join(risks)})
    return pd.DataFrame(rows)


def _known_intent(name: str) -> Tuple[str, str, str]:
    n = str(name or "")
    if "세력눌림" in n:
        return ("강한 자금유입 뒤 첫 건강한 눌림을 포착", "AUX_PULLBACK_QUALITY_AFTER_CORE224", "FIRST_PULLBACK_SEQUENCE_NOT_PROVEN_IN_LEGACY")
    if "돌반지" in n:
        return ("장기바닥/224 맥락의 이중바닥·자금확대 확인", "AUX_AFTER_CORE224", "LEGACY_ROLE_DIFFERS_FROM_CORE224")
    if "파란" in n or "Blue" in n:
        return ("눌림 지지 또는 재시동 타이밍 확인", "AUX_RESTART_SIGNAL", "SOURCE_SEMANTICS_REQUIRE_AUDIT")
    if "BB40" in n:
        return ("장기 응축·회복/저항 문맥 확인", "AUX_AFTER_CORE224", "SEQUENCE_NOT_INHERENT")
    if "수박" in n:
        return ("초입→눌림→재점화 보조 상태 확인", "AUX_AFTER_CORE224", "SEQUENCE_ENGINE_MUST_BE_CROSS_AUDITED")
    if "삼각" in n:
        return ("응축 후 재시동 가능성 확인", "AUX_AFTER_CORE224", "SEQUENCE_NOT_INHERENT")
    if "OBV" in n:
        return ("매집/지지 보존 보조", "AUX_AFTER_CORE224", "MA224_SEQUENCE_NOT_INHERENT")
    if "5일" in n or "재안착" in n:
        return ("눌림 후 단기 재시동 타이밍", "AUX_RESTART_SIGNAL", "NOT_TOP_LEVEL_THESIS")
    if "거래량폭발초동돌파" in n:
        return ("대형 자금유입·1파 초동 사건 관측", "WAVE1_OBSERVATION_SEPARATE_ENTRY_LAB_DEFERRED", "DO_NOT_TUNE_BEFORE_CORE224_VALIDATION")
    return ("PENDING_ORIGINAL_INTENT_RECONSTRUCTION", "PENDING_ROLE_AUDIT", "NOT_ADMITTED")


def build_formula_audit(registry_path: str | Path) -> pd.DataFrame:
    p = Path(registry_path)
    try:
        reg = json.loads(p.read_text(encoding="utf-8"))
        combos = reg.get("combos", []) if isinstance(reg, dict) else []
    except Exception:
        combos = []
    rows = []
    for item in combos:
        name = str(item.get("combination", ""))
        intent, role, note = _known_intent(name)
        keys = item.get("referenced_keys", []) or []
        condition = str(item.get("condition_source", ""))
        ma_rel = "EXPLICIT_KEY_OR_CODE_AUDIT_REQUIRED" if any("224" in str(k).lower() or "dolban" in str(k).lower() for k in keys) else "NOT_EXPLICIT_IN_COMBO_CONDITION"
        rows.append({
            "formula_index": item.get("index"), "formula": name,
            "01_original_intent": intent,
            "02_current_actual_code": condition,
            "02_source_line": item.get("source_line"),
            "03_raw_inputs_and_asof_timing": "keys=" + "|".join(map(str, keys)) + "; PRODUCER_TIMING_MUST_BE_AUDITED",
            "04_l0_h1_pullback_restart_sequence": "NOT_PROVEN" if role != "WAVE1_OBSERVATION_SEPARATE_ENTRY_LAB_DEFERRED" else "WAVE1_EVENT_ONLY_NOT_PULLBACK_ENTRY",
            "05_ma224_relation": ma_rel,
            "06_chart_true_false_boundary": "PENDING_MANUAL_CHART_AUDIT",
            "07_backtest_oos": "BLOCKED_UNTIL_01_TO_06_PASS",
            "v25_role": role,
            "admission_status": "NOT_ADMITTED",
            "audit_note": note,
        })
    return pd.DataFrame(rows)


def _manual_ledger(state: pd.DataFrame, false_per_date: int = 5) -> pd.DataFrame:
    """Create deterministic TRUE/FALSE/BOUNDARY chart-audit targets.

    Active CORE rows are TRUE_CANDIDATE, structural/near224 non-active rows are BOUNDARY,
    and a small deterministic sample of ordinary NONE rows becomes FALSE_CONTROL. This keeps
    manual review feasible while satisfying the explicit TRUE/FALSE/boundary audit contract.
    """
    if state.empty:
        return pd.DataFrame()
    q = state.copy()
    active = q.get("core224_state", pd.Series("NONE", index=q.index)).astype(str).ne("NONE")
    boundary = (~active) & (
        _num_col(q,"base_lens_structural",0).eq(1)
        | _num_col(q,"base_lens_near224",0).eq(1)
    )
    q["audit_bucket"] = np.where(active, "TRUE_CANDIDATE", np.where(boundary, "BOUNDARY", "FALSE_POOL"))
    keep_mask = active | boundary
    false_idx: List[Any] = []
    pool = q[q["audit_bucket"].eq("FALSE_POOL")].copy()
    if not pool.empty:
        pool["_audit_hash"] = pool.apply(lambda r: hashlib.sha256(f"{r.get('signal_date','')}|{r.get('code','')}".encode()).hexdigest(), axis=1)
        for _, g in pool.sort_values("_audit_hash").groupby("signal_date", dropna=False):
            false_idx.extend(g.head(max(1,int(false_per_date))).index.tolist())
    keep_mask = keep_mask | q.index.isin(false_idx)
    q = q[keep_mask].copy()
    q.loc[q["audit_bucket"].eq("FALSE_POOL"), "audit_bucket"] = "FALSE_CONTROL"
    keep = [c for c in [
        "signal_date","code","name","market","sector","universe_rank","audit_bucket","core224_state","core224_transition",
        "ma224","close_vs_ma224_pct","base_lens_strict224","base_lens_near224","base_lens_structural",
        "structural_range_pct","structural_location","amount_source","actual_amount_observation_days","actual_amount_history_ready20","amount_authority_fetch_status","amount_authority_source",
        "amount_ratio5_20","amount_inflow_breadth10","l0_date","l0_low","accum_date","h1_date","h1_high","pullback_date",
        "healthy_date","restart_date","wave1_day_return_pct","wave1_turnover_200bn","wave1_big_money_confirmation",
        "pullback_retrace","pullback_volume_dry_ratio","pullback_amount_dry_ratio","pullback_support_ok",
        "market_context","market_position252","sector_peer_count","sector_peer_up_ratio","sector_confirmation_heuristic60",
        "toprisk_warning","runup_from_long_low_x","evidence_status"
    ] if c in q.columns]
    q = q[keep].copy()
    q["manual_truth"] = ""          # TRUE / FALSE / BOUNDARY
    q["manual_sequence_ok"] = ""
    q["manual_ma224_context_ok"] = ""
    q["manual_amount_context_ok"] = ""
    q["manual_support_preserved"] = ""
    q["chart_reviewed"] = 0
    q["reviewer_notes"] = ""
    q["formula_admission_after_01_06"] = "BLOCKED"
    return q.sort_values([c for c in ["signal_date","audit_bucket","code"] if c in q.columns], kind="stable")


def _manual_review_sample(manual: pd.DataFrame, per_bucket: int = 15) -> pd.DataFrame:
    """Small human-first sample; the full ledger remains untouched for provenance."""
    if manual is None or manual.empty:
        return pd.DataFrame()
    q=manual.copy()
    q["_h"]=q.apply(lambda r: hashlib.sha256(f"{r.get('audit_bucket','')}|{r.get('signal_date','')}|{r.get('code','')}".encode()).hexdigest(),axis=1)
    rows=[]
    for bucket in ["TRUE_CANDIDATE","BOUNDARY","FALSE_CONTROL"]:
        g=q[q.get("audit_bucket",pd.Series(dtype=str)).astype(str).eq(bucket)].sort_values("_h",kind="stable")
        if not g.empty:
            rows.append(g.head(max(1,int(per_bucket))))
    if not rows:
        return pd.DataFrame(columns=[c for c in q.columns if c!="_h"])
    return pd.concat(rows,ignore_index=True).drop(columns=["_h"],errors="ignore")


def _amount_authority_coverage(state: pd.DataFrame) -> pd.DataFrame:
    if state is None or state.empty:
        return pd.DataFrame(columns=["signal_date","rows","actual_today_rows","history20_ready_rows","history20_ready_codes","fetched_rows","cache_hit_rows","missing_rows"])
    q=state.copy()
    rows=[]
    for d,g in q.groupby("signal_date",dropna=False):
        actual=_num_col(g,"amount_valid",0).eq(1)
        ready=_num_col(g,"actual_amount_history_ready20",0).eq(1)
        st=g.get("amount_authority_fetch_status",pd.Series("",index=g.index)).fillna("").astype(str)
        rows.append({
            "signal_date":d,"rows":len(g),"actual_today_rows":int(actual.sum()),
            "history20_ready_rows":int(ready.sum()),"history20_ready_codes":int(g.loc[ready,"code"].nunique()) if "code" in g.columns else int(ready.sum()),
            "fetched_rows":int(st.eq("FETCHED").sum()),"cache_hit_rows":int(st.eq("CACHE_HIT").sum()),
            "missing_rows":int((~ready).sum()),
        })
    return pd.DataFrame(rows).sort_values("signal_date",kind="stable")


def _reconcile_universe(payloads: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for z in payloads:
        ds = pd.to_datetime(z.get("signal_date"), errors="coerce")
        a = z.get("universe_availability") if isinstance(z.get("universe_availability"), pd.DataFrame) else pd.DataFrame()
        if a.empty:
            rows.append({"signal_date": ds, "status":"MISSING_SIDECAR","complete":0,"fallback_used":1,"availability_rows":0})
            continue
        q=a.copy()
        if "signal_date" in q.columns:
            q["signal_date"]=pd.to_datetime(q["signal_date"],errors="coerce").dt.normalize()
            q=q[q["signal_date"].eq(pd.Timestamp(ds).normalize())] if pd.notna(ds) else q
        r=q.iloc[-1] if not q.empty else pd.Series(dtype=object)
        st=str(r.get("status","MISSING_SIDECAR"))
        complete=int(st=="VALID_CAUSAL_ASOF")
        rows.append({"signal_date": pd.Timestamp(ds).normalize() if pd.notna(ds) else pd.NaT,"status":st,"complete":complete,"fallback_used":int(not complete),"availability_rows":len(q),"listing_source":r.get("listing_source",""),"liquidity_snapshot_days":r.get("liquidity_snapshot_days",np.nan),"market_cap_source":r.get("market_cap_source","")})
    q=pd.DataFrame(rows)
    if not q.empty:
        q=q.sort_values("signal_date").drop_duplicates("signal_date",keep="last")
    return q


def _pattern_only_transfer(out: Path) -> pd.DataFrame:
    """Reconcile Sequence PATTERN_ONLY into Stability research evidence.

    V25 treats the Sequence ledger as authority for bucket membership. If the older Stability
    artifact missed rows because of run-order/stale-output behavior, refresh that research-only
    artifact from the already materialized Sequence event ledger. No trading logic is touched.
    """
    seq_path = out / "v73_sequence_context_catalyst_event_eval.csv"
    stab_path = out / "v73_pattern_only_sequence_event_audit.csv"
    common_path = out / "v73_pattern_only_sequence_commonality.csv"
    readiness_path = out / "v73_bear_winner_stability_readiness.csv"
    try: seq = pd.read_csv(seq_path,dtype={"code":str}) if seq_path.exists() else pd.DataFrame()
    except Exception: seq=pd.DataFrame()
    try: stab = pd.read_csv(stab_path,dtype={"code":str}) if stab_path.exists() else pd.DataFrame()
    except Exception: stab=pd.DataFrame()
    def prep(q: pd.DataFrame, filter_pattern: bool) -> pd.DataFrame:
        if q.empty: return pd.DataFrame(columns=["signal_date","code"])
        d=_pick_col(q,["signal_date","date"]); c=_pick_col(q,["code","Code"])
        if not d or not c: return pd.DataFrame(columns=["signal_date","code"])
        z=q.copy(); z["signal_date"]=pd.to_datetime(z[d],errors="coerce").dt.normalize(); z["code"]=z[c].map(_norm_code)
        if filter_pattern:
            a=_pick_col(z,["research_bucket","alignment_level","context_alignment","alignment"])
            if not a: return pd.DataFrame(columns=["signal_date","code"])
            z=z[z[a].fillna("").astype(str).str.upper().eq("PATTERN_ONLY")]
        return z[z["signal_date"].notna() & z["code"].ne("")].sort_values(["signal_date","code"]).drop_duplicates(["signal_date","code"],keep="last")
    a=prep(seq,True); b=prep(stab,False)
    before=len(b)
    ak=set(zip(a["signal_date"].astype(str),a["code"])) if not a.empty else set()
    bk=set(zip(b["signal_date"].astype(str),b["code"])) if not b.empty else set()
    missing_before=ak-bk; extra_before=bk-ak
    refreshed=0
    if len(a)>0 and (missing_before or extra_before):
        q=a.copy()
        rcol=_pick_col(q,["next3_close_ret","ret3","day3_ret"])
        q["ret3"]=pd.to_numeric(q[rcol],errors="coerce") if rcol else np.nan
        q["outcome_group"]=np.where(q["ret3"].ge(3),"SUCCESS",np.where(q["ret3"].le(-3),"FAIL","NEUTRAL"))
        q["v25_transfer_source"]="SEQUENCE_EVENT_EVAL_AUTHORITY"
        q["research_only"]=True; q["live_logic_changed"]=False; q["real_order_changed"]=False
        _write_csv(stab_path,q)
        feats=[]
        for f in ["sequence_stage_count","volume_ratio","pullback_volume_ratio","distance_low60","upper_space","relative_strength_5d","close_location"]:
            if f not in q.columns: continue
            sa=pd.to_numeric(q.loc[q["outcome_group"].eq("SUCCESS"),f],errors="coerce").dropna()
            fa=pd.to_numeric(q.loc[q["outcome_group"].eq("FAIL"),f],errors="coerce").dropna()
            feats.append({"feature":f,"success_n":len(sa),"fail_n":len(fa),"success_median":sa.median() if len(sa) else np.nan,"fail_median":fa.median() if len(fa) else np.nan,"median_diff":sa.median()-fa.median() if len(sa) and len(fa) else np.nan,"v25_transfer_source":"SEQUENCE_EVENT_EVAL_AUTHORITY"})
        _write_csv(common_path,pd.DataFrame(feats))
        try:
            rd=pd.read_csv(readiness_path) if readiness_path.exists() else pd.DataFrame()
            if not rd.empty:
                rd.loc[rd.index[-1],"pattern_only_event_rows"]=len(q)
                rd.loc[rd.index[-1],"pattern_only_source"]="V25_SEQUENCE_EVENT_EVAL_REFRESH"
                _write_csv(readiness_path,rd)
        except Exception:
            pass
        b=prep(q,False); refreshed=1
    bk=set(zip(b["signal_date"].astype(str),b["code"])) if not b.empty else set()
    missing=ak-bk; extra=bk-ak
    return pd.DataFrame([{
        "sequence_pattern_only_rows":len(a), "stability_pattern_only_rows_before":before,
        "stability_pattern_only_rows":len(b), "missing_before_refresh":len(missing_before),
        "extra_before_refresh":len(extra_before), "refresh_applied":refreshed,
        "missing_in_stability":len(missing), "extra_in_stability":len(extra),
        "transfer_match":int(len(missing)==0 and len(extra)==0),
        "status":"PASS_REFRESHED" if refreshed and len(missing)==0 and len(extra)==0 else ("PASS" if len(missing)==0 and len(extra)==0 else "MISMATCH_REQUIRES_REVIEW"),
    }])

def strip_stale_blocks(text: str) -> str:
    """Remove visible V22 block/marker while preserving V22 CSV audit artifacts."""
    s=str(text or "")
    headers=[
        "⚡ [TOP500 4-Shard 병렬 × Newest-First Cache Prime × Fast-Gate Audit · RESEARCH_ONLY]",
        "⚡ [V22 병렬진단] SUPERSEDED_BY_V23_V24",
    ]
    next_headers=[
        "🚄 [TOP500 6-Shard Materialized Result × Merge-Only Parent × Zero-Recompute · RESEARCH_ONLY]",
        "🧪 [V24 인과 Universe × 전체분모 Formula Shadow × PATTERN_ONLY OOS × 청산기간 연구 · RESEARCH_ONLY]",
        HEADER,
    ]
    for h in headers:
        while h in s:
            st=s.find(h); candidates=[s.find(n,st+len(h)) for n in next_headers]; candidates=[x for x in candidates if x>=0]
            if candidates:
                en=min(candidates); s=(s[:st].rstrip()+"\n\n"+s[en:].lstrip())
            else:
                # Marker is a single line; raw V22 block without next authority is removed to end.
                nl=s.find("\n",st)
                s=(s[:st].rstrip()+("\n"+s[nl+1:] if nl>=0 and h.startswith("⚡ [V22 병렬진단]") else ""))
                break
    return s.strip()


def build_report(out: Path, state: pd.DataFrame, events: pd.DataFrame, inv: pd.DataFrame, activation: pd.DataFrame, universe: pd.DataFrame, formula: pd.DataFrame, transfer: pd.DataFrame) -> str:
    a=activation.iloc[-1] if not activation.empty else pd.Series(dtype=object)
    complete=int(_num_col(universe,"complete",0).sum()) if not universe.empty else 0
    fallback=int(_num_col(universe,"fallback_used",0).sum()) if not universe.empty else 0
    tr=transfer.iloc[-1] if not transfer.empty else pd.Series(dtype=object)
    counts=state.get("core224_state",pd.Series(dtype=str)).astype(str).value_counts().to_dict() if not state.empty else {}
    pipeline_status=str(a.get("pipeline_status","UNKNOWN"))
    lines=[
        HEADER,
        f"📌 {VERSION} · status={pipeline_status} · 수익률 튜닝 금지 · LIVE/점수/랭크/진입/청산/주문 변경 0",
        f"✅ activation={int(a.get('activation_executed',0) or 0)} · materialized {int(a.get('materialized_dates',0) or 0)}일 · sidecar {int(a.get('sidecar_dates',0) or 0)}일 · CORE224 rows {int(a.get('core224_rows',0) or 0)} · transitions {int(a.get('transition_rows',0) or 0)} · invariant fail {int(a.get('invariant_fail_rows',0) or 0)}",
        f"🧭 [상태] BASE {counts.get('CORE224_BASE',0)} · ACCUM {counts.get('CORE224_ACCUMULATION',0)} · WAVE1 {counts.get('CORE224_WAVE1',0)} · FIRST_PB {counts.get('CORE224_FIRST_PULLBACK',0)} · HEALTHY {counts.get('CORE224_HEALTHY_PULLBACK',0)} · RESTART {counts.get('CORE224_RESTART',0)}",
        f"💰 actual Amount 현재증거 {int(a.get('actual_amount_known_rows',0) or 0)}/{int(a.get('core224_rows',0) or 0)} · 20일 history-ready {int(a.get('actual_amount_history20_ready_rows',0) or 0)}행 · ticker-history fetch {int(a.get('actual_amount_fetch_rows',0) or 0)}행 · Close×Volume 대체 금지",
        f"📦 Historical-AsOf authority {len(universe)}일 · complete {complete} · fallback {fallback} · fallback 음수 금지",
        f"🧬 PATTERN_ONLY Sequence→Stability {int(tr.get('sequence_pattern_only_rows',0) or 0)}→{int(tr.get('stability_pattern_only_rows',0) or 0)} · missing {int(tr.get('missing_in_stability',0) or 0)} · {tr.get('status','UNKNOWN')}",
        f"🧾 기존 검색식 전수감사 {len(formula)}식 · ①~⑥ 미통과 식은 ⑦ 백테스트/OOS BLOCKED",
        f"🖼️ 수동차트 감사원장 {int(a.get('manual_audit_rows',0) or 0)}행 · 사람이 먼저 볼 축소표본 {int(a.get('manual_sample_rows',0) or 0)}행 · TRUE/FALSE/BOUNDARY",
        "- CORE224: 장기바닥/224 위치 → 거래대금 매집(필수 선행) → L0<H1 1파 → H1 이후 첫 눌림 → 거래량·거래대금 감소 → 지지보존 → 재시동",
        "- 5~10% 장대양봉·2,000억 거래대금, 시장위치, 섹터동반, TOP_RISK는 SHADOW 관측값이며 하드게이트가 아닙니다.",
        "- 돌반지·파란점선·BB40·수박·삼각·OBV·5일 재안착·세력눌림목은 CORE224 이후 AUX 역할부터 검증합니다.",
        "- 🚀거래량폭발초동돌파의 +3%/D+1 청산 연구는 CORE224 수동감사 완료 전 보류합니다.",
        f"- CSV: {STATE_LEDGER_FILE} · {TRANSITION_FILE} · {MANUAL_AUDIT_FILE} · {MANUAL_SAMPLE_FILE} · {AMOUNT_AUTHORITY_FILE} · {FORMULA_AUDIT_FILE} · {SOURCE_AUDIT_FILE} · {ACTIVATION_FILE}",
    ]
    return "\n".join(lines)


def finalize(
    output_dir: str | Path = "reports",
    payloads: Optional[List[Dict[str, Any]]] = None,
    source_file: str | Path = "main7_bugfix_2.py",
    registry_path: str | Path = "search_formula_contract_registry.json",
    base_report: str = "",
) -> Tuple[str, Dict[str, pd.DataFrame]]:
    out=_out_dir(output_dir)
    payloads=payloads or []
    states=[]; events=[]; invs=[]
    for z in payloads:
        side=z.get("runtime_sidecars",{}) if isinstance(z.get("runtime_sidecars",{}),dict) else {}
        states.extend([dict(x) for x in (side.get("V25_CORE224_ROWS") or []) if isinstance(x,dict)])
        events.extend([dict(x) for x in (side.get("V25_CORE224_EVENTS") or []) if isinstance(x,dict)])
        invs.extend([dict(x) for x in (side.get("V25_CORE224_INVARIANTS") or []) if isinstance(x,dict)])
    state=pd.DataFrame(states); event=pd.DataFrame(events); inv=pd.DataFrame(invs)
    for q in [state,event,inv]:
        if not q.empty and "signal_date" in q.columns:
            q["signal_date"]=pd.to_datetime(q["signal_date"],errors="coerce").dt.strftime("%Y-%m-%d")
    manual=_manual_ledger(state)
    manual_sample=_manual_review_sample(manual, per_bucket=int(os.getenv("V25_MANUAL_SAMPLE_PER_BUCKET","15") or 15))
    amount_authority=_amount_authority_coverage(state)
    formula=build_formula_audit(registry_path)
    source=audit_source(source_file)
    universe=_reconcile_universe(payloads)
    transfer=_pattern_only_transfer(out)
    actual_known=int(_num_col(state,"amount_valid",0).eq(1).sum()) if not state.empty else 0
    market_known=int(_num_col(state,"market_context_known",0).eq(1).sum()) if not state.empty else 0
    sector_known=int(_num_col(state,"sector_context_known",0).eq(1).sum()) if not state.empty else 0
    _sidecar_dates=sum(1 for z in payloads if isinstance(z.get("runtime_sidecars",{}),dict) and "V25_CORE224_ROWS" in z.get("runtime_sidecars",{}))
    _pipeline_ok = bool(len(payloads) > 0 and _sidecar_dates == len(payloads) and len(state) > 0 and len(formula) == 66 and len(universe) == len(payloads))
    activation=pd.DataFrame([{
        "version":VERSION,"activation_executed":1,"pipeline_status":"VALID_SHADOW" if _pipeline_ok else "INVALID_INCOMPLETE_V25_HANDOFF",
        "materialized_dates":len(payloads),"sidecar_dates":_sidecar_dates,
        "core224_rows":len(state),"transition_rows":len(event),"invariant_fail_rows":len(inv),"restart_rows":int(state.get("core224_state",pd.Series(dtype=str)).astype(str).eq("CORE224_RESTART").sum()) if not state.empty else 0,
        "manual_audit_rows":len(manual),"manual_sample_rows":len(manual_sample),
        "actual_amount_known_rows":actual_known,
        "actual_amount_history20_ready_rows":int(_num_col(state,"actual_amount_history_ready20",0).eq(1).sum()) if not state.empty else 0,
        "actual_amount_fetch_rows":int(state.get("amount_authority_fetch_status",pd.Series(dtype=str)).astype(str).eq("FETCHED").sum()) if not state.empty else 0,
        "market_context_known_rows":market_known,"sector_context_known_rows":sector_known,
        "formula_audit_rows":len(formula),"formula_expected":66,"formula_count_ok":int(len(formula)==66),
        "historical_asof_days":len(universe),"historical_complete_days":int(_num_col(universe,"complete",0).sum()) if not universe.empty else 0,
        "historical_fallback_days":int(_num_col(universe,"fallback_used",0).sum()) if not universe.empty else 0,
        "pattern_only_transfer_match":int(transfer.iloc[-1].get("transfer_match",0)) if not transfer.empty else 0,
        "live_logic_changed":False,"real_order_changed":False,"parent_top500_recompute_allowed":False,
        "generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }])
    _write_csv(out/STATE_LEDGER_FILE,state); _write_csv(out/TRANSITION_FILE,event); _write_csv(out/INVARIANT_FILE,inv)
    _write_csv(out/MANUAL_AUDIT_FILE,manual); _write_csv(out/MANUAL_SAMPLE_FILE,manual_sample); _write_csv(out/AMOUNT_AUTHORITY_FILE,amount_authority); _write_csv(out/FORMULA_AUDIT_FILE,formula); _write_csv(out/SOURCE_AUDIT_FILE,source)
    _write_csv(out/UNIVERSE_RECON_FILE,universe); _write_csv(out/PATTERN_TRANSFER_FILE,transfer); _write_csv(out/ACTIVATION_FILE,activation)
    block=build_report(out,state,event,inv,activation,universe,formula,transfer)
    (out/REPORT_FILE).write_text(block,encoding="utf-8")
    raw=strip_stale_blocks(str(base_report or ""))
    if HEADER in raw:
        raw=raw.split(HEADER)[0].rstrip()
    fixed=(raw.rstrip()+"\n\n"+block).strip() if raw.strip() else block
    return fixed,{"state":state,"events":event,"invariants":inv,"manual":manual,"manual_sample":manual_sample,"amount_authority":amount_authority,"formula_audit":formula,"source_audit":source,"universe":universe,"pattern_transfer":transfer,"activation":activation}


def force_report(text: str, output_dir: str | Path = "reports") -> str:
    out=_out_dir(output_dir); p=out/REPORT_FILE
    raw=strip_stale_blocks(str(text or ""))
    if not p.exists(): return raw
    try: block=p.read_text(encoding="utf-8")
    except Exception: return raw
    if HEADER in raw: raw=raw.split(HEADER)[0].rstrip()
    return (raw.rstrip()+"\n\n"+block).strip() if raw.strip() else block
