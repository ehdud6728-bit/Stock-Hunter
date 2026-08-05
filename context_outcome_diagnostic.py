from __future__ import annotations

import hashlib
import json
import math
import os
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6.15"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🧭 [시장 × 단체섹터 × 종목시퀀스 × 수익경로 진단 · RESEARCH_ONLY]"
REPORT_FILE = "v73_context_outcome_diagnostic_report.txt"

EVENT_MASTER_FILE = "v73_backtest_event_master.csv"
MARKET_CONTEXT_FILE = "v73_market_context_diagnostic.csv"
SECTOR_CONTEXT_FILE = "v73_sector_context_diagnostic.csv"
RETURN_PATH_FILE = "v73_return_path_cluster.csv"
COMMONALITY_FILE = "v73_winner_loser_commonality.csv"
FAILURE_FILE = "v73_failure_reason.csv"
FEATURE_LIFT_FILE = "v73_context_feature_lift.csv"
ABLATION_FILE = "v73_context_ablation.csv"
REGIME_PERF_FILE = "v73_context_regime_performance.csv"
SCORECARD_FILE = "v73_search_formula_scorecard.csv"
MISSED_FEATURE_FILE = "v73_missed_feature_audit.csv"
READINESS_FILE = "v73_context_diagnostic_readiness.csv"

MIN_POLICY_ROWS = 30
MIN_POLICY_DATES = 10


def _out(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _norm_code(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    d = re.sub(r"\D", "", s)
    return d.zfill(6)[-6:] if d else ""


def _read(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, low_memory=False, encoding=enc)
        except Exception:
            pass
    return pd.DataFrame()


def _write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_csv(path, index=False, encoding="utf-8-sig")


def _series(df: pd.DataFrame, names: Iterable[str], default: Any = np.nan) -> pd.Series:
    for c in names:
        if c in df.columns:
            return df[c]
    return pd.Series(default, index=df.index)


def _num(df: pd.DataFrame, names: Iterable[str], default: Any = np.nan) -> pd.Series:
    return pd.to_numeric(_series(df, names, default), errors="coerce")


def _text(df: pd.DataFrame, names: Iterable[str], default: str = "") -> pd.Series:
    return _series(df, names, default).fillna(default).astype(str)


def _bool(df: pd.DataFrame, names: Iterable[str], default: bool = False) -> pd.Series:
    s = _series(df, names, default)
    if s.dtype == bool:
        return s.fillna(default)
    return s.fillna(default).astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y", "on", "t"})


def _finite(v: Any) -> bool:
    try:
        return math.isfinite(float(v))
    except Exception:
        return False


def _sha_frame(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "EMPTY"
    cols = sorted(df.columns.astype(str).tolist())
    text = df[cols].astype(str).sort_values(cols[: min(3, len(cols))], kind="stable").to_csv(index=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:20]


def _canonical_base(output_dir: Path, fallback_df: pd.DataFrame | None = None) -> tuple[pd.DataFrame, str]:
    candidates = [
        ("FORMULA_EXPLODED", output_dir / "v72_search_formula_universe_exploded_eval.csv"),
        ("MARKET_EXCESS", output_dir / "v72_market_excess_signal_audit.csv"),
        ("SEQUENCE_JOIN", output_dir / "v73_sequence_context_catalyst_join.csv"),
    ]
    source = "CALLER_DF"
    base = pd.DataFrame()
    for name, fp in candidates:
        q = _read(fp)
        if not q.empty:
            base, source = q, name
            break
    if base.empty and isinstance(fallback_df, pd.DataFrame):
        base = fallback_df.copy()
    if base.empty:
        return base, source

    x = base.copy()
    x["signal_date"] = pd.to_datetime(_text(x, ["signal_date", "date", "신호일", "Date"]), errors="coerce").dt.normalize()
    x["code"] = _text(x, ["code", "Code", "종목코드", "ticker", "Symbol"]).map(_norm_code)
    x = x[x["signal_date"].notna() & x["code"].ne("")].copy()
    if x.empty:
        return x, source

    x["name"] = _text(x, ["name", "Name", "종목명"])
    x["formula"] = _text(x, ["formula", "pattern_combo", "combination", "검색식"], "UNCLASSIFIED").replace({"": "UNCLASSIFIED", "nan": "UNCLASSIFIED"})
    x["market"] = _text(x, ["market", "Market", "시장"], "UNKNOWN").str.upper().replace({"코스피": "KOSPI", "유가": "KOSPI", "코스닥": "KOSDAQ", "": "UNKNOWN"})
    x["sector"] = _text(x, ["sector", "sector_label", "Sector", "theme", "Theme", "섹터", "테마"], "UNKNOWN").replace({"": "UNKNOWN", "nan": "UNKNOWN"})

    x["ret1"] = _num(x, ["ret1", "next1_close_ret", "day1_ret", "return_d1"])
    x["ret3"] = _num(x, ["ret3", "next3_close_ret", "day3_ret", "return_d3"])
    x["ret5"] = _num(x, ["ret5", "next5_close_ret", "day5_ret", "return_d5"])
    x["ret10"] = _num(x, ["ret10", "next10_close_ret", "day10_ret", "return_d10"])
    x["mfe"] = _num(x, ["mfe", "MFE", "max_up_5d", "MFE_5D", "max_favorable_excursion"])
    x["mae"] = _num(x, ["mae", "MAE", "max_down_5d", "MAE_5D", "max_adverse_excursion"])
    x["plus3_first"] = _bool(x, ["plus3_first", "plus3", "hit_plus3_first", "plus3_first_10d"])
    x["stop_first"] = _bool(x, ["stop_first", "hit_stop_first", "stop_first_10d", "minus3_first"])
    x["market_excess3"] = _num(x, ["market_excess3", "market_excess_3d"])
    x["sector_excess3"] = _num(x, ["sector_excess3", "sector_excess_3d"])
    x["market_fwd_ret3"] = _num(x, ["market_fwd_ret3", "benchmark_ret3"])
    x["sector_reference_ret3"] = _num(x, ["sector_reference_ret3", "sector_fwd_ret3", "sector_peer_ret3"])
    x["entry_price"] = _num(x, ["entry_price", "signal_close", "Close", "close"])
    x["score_axis"] = _num(x, ["score_axis", "safe_score", "n_score", "score"])
    x["rsi"] = _num(x, ["rsi", "RSI", "rsi14"])
    x["ma_gap"] = _num(x, ["ma_gap", "disparity", "이격", "ma5_disparity"])
    x["turnover"] = _num(x, ["turnover", "trade_amount", "trading_value", "거래대금"])
    x["distance_low60"] = _num(x, ["distance_low60", "distance_from_60d_low_pct", "low60_distance_pct"])
    x["upper_space"] = _num(x, ["upper_space", "upper_resistance_distance_pct", "upper_space_pct"])
    x["late_wave"] = _bool(x, ["late_wave", "is_late_wave"])
    return x, source


def _dedupe_event_rows(x: pd.DataFrame) -> pd.DataFrame:
    if x.empty:
        return x
    keys = ["signal_date", "code", "formula"]
    order = [c for c in ["combo_invocation", "attempt_rank", "rank"] if c in x.columns]
    if order:
        x = x.sort_values(keys + order, kind="stable")
    return x.drop_duplicates(keys, keep="first").reset_index(drop=True)


def _load_context(output_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    market = _read(output_dir / "v72_market_excess_signal_audit.csv")
    sector = _read(output_dir / "v73_sequence_context_catalyst_join.csv")
    if market.empty:
        market = _read(output_dir / "v73_market_sector_context_ledger.csv")
    if sector.empty:
        sector = _read(output_dir / "v73_market_sector_context_ledger.csv")
    return market, sector


def _prepare_join_frame(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if df.empty:
        return df
    q = df.copy()
    q["signal_date"] = pd.to_datetime(_text(q, ["signal_date", "date", "신호일"]), errors="coerce").dt.normalize()
    q["code"] = _text(q, ["code", "Code", "종목코드"]).map(_norm_code)
    q = q[q["signal_date"].notna() & q["code"].ne("")].copy()
    # Keep one causal snapshot per event. Prefer causal eligible and latest cutoff not later than signal.
    q["_causal"] = _bool(q, ["causal_eligible"], True)
    q["_cutoff"] = pd.to_datetime(_text(q, ["signal_cutoff_at", "snapshot_at"]), errors="coerce", utc=True)
    q["_signal_at"] = pd.to_datetime(_text(q, ["signal_at"]), errors="coerce", utc=True)
    fallback_signal = pd.to_datetime(q["signal_date"].astype(str) + " 15:30:00+09:00", errors="coerce", utc=True)
    q["_signal_at"] = q["_signal_at"].where(q["_signal_at"].notna(), fallback_signal)
    q["_causal_time_ok"] = q["_cutoff"].isna() | q["_signal_at"].isna() | q["_cutoff"].le(q["_signal_at"])
    q = q[q["_causal"] & q["_causal_time_ok"]].copy()
    q = q.sort_values(["signal_date", "code", "_cutoff"], kind="stable").drop_duplicates(["signal_date", "code"], keep="last")
    keep = [c for c in q.columns if c not in {"name", "formula"}]
    q = q[keep]
    rename = {c: f"{c}{suffix}" for c in q.columns if c not in {"signal_date", "code"}}
    return q.rename(columns=rename)


def _merge_context(x: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    market, sector = _load_context(output_dir)
    m = _prepare_join_frame(market, "_m")
    s = _prepare_join_frame(sector, "_s")
    out = x.copy()
    if not m.empty:
        out = out.merge(m, on=["signal_date", "code"], how="left")
    if not s.empty:
        out = out.merge(s, on=["signal_date", "code"], how="left")
    return out


def _coalesce_num(x: pd.DataFrame, names: list[str]) -> pd.Series:
    out = pd.Series(np.nan, index=x.index, dtype=float)
    for c in names:
        if c in x.columns:
            out = out.where(out.notna(), pd.to_numeric(x[c], errors="coerce"))
    return out


def _coalesce_text(x: pd.DataFrame, names: list[str], default: str = "") -> pd.Series:
    out = pd.Series("", index=x.index, dtype=object)
    for c in names:
        if c in x.columns:
            q = x[c].fillna("").astype(str).replace({"nan": "", "None": ""})
            out = out.where(out.astype(str).ne(""), q)
    return out.replace({"": default, "nan": default, "None": default})


def _coalesce_bool(x: pd.DataFrame, names: list[str], default: bool = False) -> pd.Series:
    out = pd.Series(default, index=x.index, dtype=bool)
    seen = pd.Series(False, index=x.index)
    for c in names:
        if c not in x.columns:
            continue
        raw = x[c]
        valid = raw.notna() & raw.astype(str).str.strip().ne("")
        vals = raw.fillna(default).astype(str).str.lower().isin({"1", "true", "yes", "y", "on", "t"})
        out = out.where(seen, vals)
        seen |= valid
    return out


def _availability(x: pd.DataFrame, names: list[str]) -> pd.Series:
    seen = pd.Series(False, index=x.index, dtype=bool)
    for c in names:
        if c in x.columns:
            raw = x[c]
            seen |= raw.notna() & raw.astype(str).str.strip().ne("") & ~raw.astype(str).str.lower().isin({"nan", "none", "unknown"})
    return seen


def _derive_features(x: pd.DataFrame) -> pd.DataFrame:
    q = x.copy()
    _supply_known_raw = _availability(q, ["supply_drying_s", "supply_drying_m", "supply_drying"])
    _compression_known_raw = _availability(q, ["price_compression_s", "price_compression_m", "price_compression"])
    _restart_known_raw = _availability(q, ["restart_trigger_s", "restart_trigger_m", "restart_trigger", "restart_volume_ratio_s", "restart_volume_ratio_m", "restart_volume_ratio"])
    _sequence_known_raw = _availability(q, ["stage_count_s", "stage_count_m", "stage_count", "accepted_breakout_s", "accepted_breakout_m", "accepted_breakout", "first_pullback_s", "first_pullback_m", "first_pullback"])
    _catalyst_known_raw = _availability(q, ["catalyst_state_s", "catalyst_state_m", "catalyst_state", "catalyst_usable_s", "catalyst_usable_m", "catalyst_usable"])
    q["market_regime_raw"] = _coalesce_text(q, ["market_regime_causal_m", "market_regime_m", "market_regime_s", "market_regime", "regime_m"], "UNKNOWN").str.upper()
    q["market_return_5d"] = _coalesce_num(q, ["market_return_5d_s", "market_return_5d_m", "market_past_ret5_m"])
    q["market_turnover_ratio"] = _coalesce_num(q, ["market_turnover_ratio_s", "market_turnover_ratio_m", "market_turnover_change_m"])
    q["market_breadth"] = _coalesce_num(q, ["market_breadth_m", "market_breadth_s", "market_up_ratio_pct_m", "market_up_ratio_pct_s"])
    q["market_above_ma20_ratio"] = _coalesce_num(q, ["market_above_ma20_ratio_m", "market_above_ma20_ratio_s"])
    q["market_investor_flow"] = _coalesce_num(q, ["market_investor_flow_s", "market_investor_flow_m", "market_foreign_inst_flow_m"])

    q["sector_return_5d"] = _coalesce_num(q, ["sector_return_5d_s", "sector_return_5d_m", "sector_past_ret5_s"])
    q["sector_turnover_ratio"] = _coalesce_num(q, ["sector_turnover_ratio_s", "sector_turnover_ratio_m", "sector_turnover_change_s"])
    q["sector_breadth"] = _coalesce_num(q, ["sector_up_ratio_pct_s", "sector_breadth_s", "sector_breadth_m"])
    q["sector_rank"] = _coalesce_num(q, ["sector_rank_s", "sector_rank_m"])
    q["sector_rank_change"] = _coalesce_num(q, ["sector_rank_change_s", "sector_rank_change_m"])
    q["sector_top3_concentration"] = _coalesce_num(q, ["sector_top3_concentration_s", "sector_top3_concentration_m"])
    q["sector_leader_retention"] = _coalesce_num(q, ["sector_leader_retention_s", "sector_leader_retention_m"])
    q["sector_positive"] = _coalesce_bool(q, ["sector_positive_s", "sector_positive_m"], False)
    q["true_sector_index"] = _coalesce_bool(q, ["true_sector_index_s", "true_sector_index_m"], False)

    q["sequence_stage"] = _coalesce_text(q, ["sequence_stage_s", "sequence_stage_m", "sequence_stage"], "UNKNOWN")
    q["stage_count"] = _coalesce_num(q, ["stage_count_s", "stage_count_m", "stage_count"])
    q["supply_drying"] = _coalesce_bool(q, ["supply_drying_s", "supply_drying_m", "supply_drying"], False)
    q["price_compression"] = _coalesce_bool(q, ["price_compression_s", "price_compression_m", "price_compression"], False)
    q["support_hold"] = _coalesce_bool(q, ["support_hold_s", "support_hold_m", "support_hold"], False)
    q["restart_trigger"] = _coalesce_bool(q, ["restart_trigger_s", "restart_trigger_m", "restart_trigger"], False)
    q["accepted_breakout"] = _coalesce_bool(q, ["accepted_breakout_s", "accepted_breakout_m", "accepted_breakout"], False)
    q["first_pullback"] = _coalesce_bool(q, ["first_pullback_s", "first_pullback_m", "first_pullback"], False)
    q["pullback_volume_ratio"] = _coalesce_num(q, ["pullback_volume_ratio_s", "pullback_volume_ratio_m", "pullback_volume_ratio"])
    q["range_contraction_ratio"] = _coalesce_num(q, ["range_contraction_ratio_s", "range_contraction_ratio_m", "range_contraction_ratio"])
    q["restart_volume_ratio"] = _coalesce_num(q, ["restart_volume_ratio_s", "restart_volume_ratio_m", "restart_volume_ratio"])
    q["resistance_room_pct"] = _coalesce_num(q, ["resistance_room_pct_s", "resistance_room_pct_m", "resistance_room_pct", "upper_space"])
    q["catalyst_state"] = _coalesce_text(q, ["catalyst_state_s", "catalyst_state_m", "catalyst_state"], "NO_CAUSAL_CATALYST")
    q["catalyst_usable"] = _coalesce_bool(q, ["catalyst_usable_s", "catalyst_usable_m", "catalyst_usable"], False)
    q["full_alignment_existing"] = _coalesce_bool(q, ["full_alignment_s", "full_alignment_m", "full_alignment"], False)
    q["foreign_inst_flow"] = _coalesce_num(q, ["foreign_inst_flow_s", "foreign_inst_flow_m", "foreign_inst_flow", "net_foreign_institution"])

    q["supply_drying_known"] = _supply_known_raw
    q["price_compression_known"] = _compression_known_raw
    q["restart_trigger_known"] = _restart_known_raw
    q["sequence_known"] = _sequence_known_raw
    q["catalyst_known"] = _catalyst_known_raw
    q["market_context_known"] = q["market_regime_raw"].ne("UNKNOWN") | q["market_return_5d"].notna() | q["market_breadth"].notna()
    q["sector_context_known"] = q["sector_return_5d"].notna() | q["sector_breadth"].notna() | q["sector_rank"].notna() | q["true_sector_index"]

    # Market state: breadth takes precedence when present, otherwise use locked causal regime.
    b = q["market_breadth"]
    tr = q["market_turnover_ratio"]
    mr = q["market_return_5d"]
    reg = q["market_regime_raw"]
    q["market_state"] = "NEUTRAL"
    q.loc[reg.isin(["BEAR", "RISK_OFF"]) | ((mr < 0) & (b < 45)), "market_state"] = "RISK_OFF"
    q.loc[reg.eq("PANIC") & (mr > 0), "market_state"] = "PANIC_REBOUND"
    q.loc[reg.eq("PANIC") & ~(mr > 0), "market_state"] = "RISK_OFF"
    q.loc[reg.isin(["BULL", "RECOVERY", "RISK_ON"]) & ((b >= 55) | b.isna()) & ((tr >= 0.95) | tr.isna()), "market_state"] = "RISK_ON_BROAD"
    q.loc[reg.isin(["BULL", "RECOVERY", "RISK_ON"]) & (b < 55), "market_state"] = "RISK_ON_NARROW"

    sr = q["sector_return_5d"]
    sb = q["sector_breadth"]
    st = q["sector_turnover_ratio"]
    rel = q["sector_excess3"]
    conc = q["sector_top3_concentration"]
    rankchg = q["sector_rank_change"]
    q["sector_state"] = "UNKNOWN"
    q.loc[(sr < 0) & ((sb < 45) | sb.isna()), "sector_state"] = "SELL_OFF"
    q.loc[(sr >= 0) & (rel < 0), "sector_state"] = "LAGGING"
    q.loc[(sr > 0) & (sb >= 60) & ((st >= 1.05) | st.isna()) & ((rel >= 0) | rel.isna()), "sector_state"] = "BROAD_LEADER"
    q.loc[(sr > 0) & ((sb < 50) | (conc >= 65)), "sector_state"] = "NARROW_LEADER"
    q.loc[(sr > 0) & (rankchg < 0) & (st >= 1.15) & ~q["sector_state"].eq("NARROW_LEADER"), "sector_state"] = "ROTATION_START"
    q.loc[(sr >= 5) & ((sb >= 55) | sb.isna()) & ~q["sector_state"].isin(["NARROW_LEADER", "EXHAUSTION"]), "sector_state"] = "MATURE_TREND"
    q.loc[(sr > 0) & (sb < 50) & (st < 1.0), "sector_state"] = "EXHAUSTION"
    q.loc[q["sector_state"].eq("UNKNOWN") & q["sector_positive"], "sector_state"] = "BROAD_LEADER_PROXY"

    q["market_ok"] = q["market_state"].isin(["RISK_ON_BROAD", "RISK_ON_NARROW", "PANIC_REBOUND", "NEUTRAL"])
    q["sector_ok"] = q["sector_state"].isin(["BROAD_LEADER", "BROAD_LEADER_PROXY", "ROTATION_START", "MATURE_TREND"])
    q["sector_conflict"] = q["sector_state"].isin(["LAGGING", "SELL_OFF", "EXHAUSTION"])
    q["sequence_ok"] = q["stage_count"].fillna(0).ge(5) | (q["accepted_breakout"] & q["first_pullback"])
    q["catalyst_ok"] = q["catalyst_usable"] | q["catalyst_state"].isin(["ACTIVE_CATALYST", "LATENT_CATALYST_REACTIVATED", "OFFICIAL_CAUSAL"])

    q["context_alignment"] = "STOCK_ONLY"
    q.loc[~q["market_context_known"] & ~q["sector_context_known"], "context_alignment"] = "CONTEXT_UNKNOWN"
    q.loc[q["market_ok"] & q["sector_ok"], "context_alignment"] = "FULL_ALIGNMENT"
    q.loc[~q["market_ok"] & q["sector_ok"], "context_alignment"] = "SECTOR_ONLY"
    q.loc[q["market_ok"] & ~q["sector_ok"] & ~q["sector_conflict"], "context_alignment"] = "MARKET_ONLY"
    q.loc[~q["market_ok"] & q["sector_conflict"], "context_alignment"] = "FULL_CONFLICT"
    q.loc[q["market_ok"] & q["sector_conflict"], "context_alignment"] = "SECTOR_CONFLICT"
    q.loc[~q["market_context_known"] & ~q["sector_context_known"], "context_alignment"] = "CONTEXT_UNKNOWN"
    q["full_alignment"] = q["context_alignment"].eq("FULL_ALIGNMENT") & q["sequence_ok"] & q["supply_drying"] & q["restart_trigger"]

    # Causal availability is an explicit audit state, never silently imputed as PASS.
    has_context = q["market_regime_raw"].ne("UNKNOWN") | q["sector_state"].ne("UNKNOWN")
    q["context_causal_status"] = np.where(has_context, "CAUSAL_OR_ARCHIVE_CONTEXT_AVAILABLE", "CONTEXT_UNAVAILABLE")
    return q


def _classify_path(q: pd.DataFrame, cost_bp: float = 20.0) -> pd.DataFrame:
    x = q.copy()
    end = x["ret3"].where(x["ret3"].notna(), x["ret5"]).where(lambda s: s.notna(), x["ret10"])
    x["evaluation_ret"] = end
    x["net_ret20"] = end - cost_bp / 100.0
    x["net_ret50"] = end - 0.50
    x["giveback"] = x["mfe"] - end
    causal = _coalesce_text(x, ["first_event_causal", "first_event_causal_event"], "").str.lower()
    conflict = _coalesce_bool(x, ["path_conflict_3", "path_conflict_5"], False) | causal.eq("path_conflict")
    stop_first = causal.eq("stop") | (causal.eq("") & x["stop_first"])
    target_first = causal.isin(["plus3", "plus5"]) | (causal.eq("") & x["plus3_first"] & ~x["stop_first"])
    x["path_conflict"] = conflict
    x["return_path"] = "NO_EDGE"
    x.loc[conflict, "return_path"] = "PATH_CONFLICT"
    x.loc[~conflict & stop_first, "return_path"] = "FAST_LOSS"
    x.loc[~conflict & ~stop_first & ((x["mfe"] >= 10) | (end >= 8)), "return_path"] = "BIG_WIN"
    x.loc[~conflict & ~stop_first & ~x["return_path"].eq("BIG_WIN") & target_first, "return_path"] = "FAST_WIN"
    x.loc[~conflict & ~stop_first & x["return_path"].eq("NO_EDGE") & (x["mfe"] >= 3) & (x["giveback"] >= 3) & (end <= 1), "return_path"] = "GIVEBACK"
    x.loc[~conflict & ~stop_first & x["return_path"].eq("NO_EDGE") & (x["net_ret20"] > 0.5), "return_path"] = "NORMAL_WIN"
    x.loc[~conflict & ~stop_first & x["return_path"].eq("NO_EDGE") & (end < -0.2) & ((x["mfe"] < 1) | x["mfe"].isna()), "return_path"] = "GRIND_LOSS"
    x["winner_group"] = x["return_path"].isin(["BIG_WIN", "FAST_WIN", "NORMAL_WIN"])
    x["loser_group"] = x["return_path"].isin(["FAST_LOSS", "GRIND_LOSS"])
    return x


def _failure_reasons(x: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    q = x.copy()
    reason_map: dict[str, pd.Series] = {
        "MARKET_CONFLICT": ~q["market_ok"],
        "SECTOR_CONFLICT": q["sector_conflict"],
        "STOCK_ONLY": q["context_alignment"].eq("STOCK_ONLY") & q["market_context_known"] & q["sector_context_known"],
        "LATE_ENTRY": q["late_wave"] | (q["ma_gap"] >= 115) | (q["rsi"] >= 70) | ((q["distance_low60"] > 30) & (q["resistance_room_pct"] < 12)),
        "NO_SUPPLY_DRYING": q["supply_drying_known"] & ~q["supply_drying"],
        "NO_PRICE_COMPRESSION": q["price_compression_known"] & ~q["price_compression"],
        "FAKE_RESTART": (q["restart_trigger_known"] & ~q["restart_trigger"]) | (q["restart_volume_ratio"].notna() & (q["restart_volume_ratio"] < 1.05)),
        "OVERHEAD_RESISTANCE": (q["resistance_room_pct"].notna() & (q["resistance_room_pct"] < 8)),
        "INSTITUTIONAL_DISTRIBUTION": q["foreign_inst_flow"].notna() & (q["foreign_inst_flow"] < 0),
        "CATALYST_WEAK": q["catalyst_known"] & ~q["catalyst_ok"],
        "LIQUIDITY_WEAK": q["turnover"].notna() & (q["turnover"] < 5_000_000_000),
        "HOLD_TOO_LONG": (q["ret3"] > 0) & (q["ret5"] < q["ret3"] - 2),
    }
    q["failure_reasons"] = ""
    rows = []
    for reason, mask in reason_map.items():
        mask = mask.fillna(False)
        q.loc[mask, "failure_reasons"] = q.loc[mask, "failure_reasons"].where(q.loc[mask, "failure_reasons"].eq(""), q.loc[mask, "failure_reasons"] + "|") + reason
        g = q[mask]
        losers = q[q["loser_group"]]
        rows.append({
            "reason": reason,
            "flagged_n": int(mask.sum()),
            "loser_flagged_n": int((mask & q["loser_group"]).sum()),
            "loser_capture_rate": float((mask & q["loser_group"]).sum() / max(1, q["loser_group"].sum()) * 100),
            "flagged_loss_rate": float(g["loser_group"].mean() * 100) if len(g) else np.nan,
            "d3_mean": pd.to_numeric(g["ret3"], errors="coerce").mean() if len(g) else np.nan,
            "d3_median": pd.to_numeric(g["ret3"], errors="coerce").median() if len(g) else np.nan,
            "mfe_mean": pd.to_numeric(g["mfe"], errors="coerce").mean() if len(g) else np.nan,
            "mae_mean": pd.to_numeric(g["mae"], errors="coerce").mean() if len(g) else np.nan,
            "incremental_loss_rate_vs_all": (float(g["loser_group"].mean() * 100) - float(q["loser_group"].mean() * 100)) if len(g) else np.nan,
        })
    return q, pd.DataFrame(rows).sort_values(["incremental_loss_rate_vs_all", "loser_capture_rate"], ascending=False)


def _perf(g: pd.DataFrame, label: str, dimension: str, cost_bp: float = 20.0) -> dict[str, Any]:
    r = pd.to_numeric(g.get("evaluation_ret", pd.Series(dtype=float)), errors="coerce")
    r3 = pd.to_numeric(g.get("ret3", pd.Series(dtype=float)), errors="coerce")
    wins = r[r > 0]
    losses = r[r < 0]
    gross_profit = float(wins.sum()) if len(wins) else 0.0
    gross_loss = abs(float(losses.sum())) if len(losses) else 0.0
    top5 = r.sort_values(ascending=False).iloc[5:] if len(r.dropna()) > 5 else pd.Series(dtype=float)
    date_means = g.assign(_r=r).groupby("signal_date")["_r"].mean() if "signal_date" in g.columns else pd.Series(dtype=float)
    return {
        "dimension": dimension,
        "label": label,
        "n": int(len(g)),
        "stocks": int(g["code"].nunique()) if "code" in g.columns else 0,
        "signal_days": int(g["signal_date"].nunique()) if "signal_date" in g.columns else 0,
        "d1_mean": pd.to_numeric(g.get("ret1"), errors="coerce").mean(),
        "d3_mean": r3.mean(),
        "d3_median": r3.median(),
        "d5_mean": pd.to_numeric(g.get("ret5"), errors="coerce").mean(),
        "d10_mean": pd.to_numeric(g.get("ret10"), errors="coerce").mean(),
        "evaluation_mean": r.mean(),
        "evaluation_median": r.median(),
        "positive_rate": float((r > 0).mean() * 100) if r.notna().any() else np.nan,
        "plus3_first_rate": float(g.get("plus3_first", pd.Series(dtype=bool)).mean() * 100) if len(g) else np.nan,
        "stop_first_rate": float(g.get("stop_first", pd.Series(dtype=bool)).mean() * 100) if len(g) else np.nan,
        "mfe_mean": pd.to_numeric(g.get("mfe"), errors="coerce").mean(),
        "mae_mean": pd.to_numeric(g.get("mae"), errors="coerce").mean(),
        "avg_win": wins.mean() if len(wins) else np.nan,
        "avg_loss": losses.mean() if len(losses) else np.nan,
        "payoff_ratio": (wins.mean() / abs(losses.mean())) if len(wins) and len(losses) and losses.mean() != 0 else np.nan,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else np.nan,
        "expectancy_cost20": r.mean() - 0.20 if r.notna().any() else np.nan,
        "expectancy_cost50": r.mean() - 0.50 if r.notna().any() else np.nan,
        "top5_removed_mean": top5.mean() if len(top5) else np.nan,
        "positive_signal_day_rate": float((date_means > 0).mean() * 100) if len(date_means) else np.nan,
    }


def _return_path_table(x: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([_perf(g, str(k), "RETURN_PATH") for k, g in x.groupby("return_path", dropna=False)])


def _market_context_table(x: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for k, g in x.groupby("market_state", dropna=False):
        z = _perf(g, str(k), "MARKET_STATE")
        z.update({
            "market_breadth_median": g["market_breadth"].median(),
            "market_turnover_ratio_median": g["market_turnover_ratio"].median(),
            "market_return_5d_median": g["market_return_5d"].median(),
        })
        rows.append(z)
    return pd.DataFrame(rows)


def _sector_context_table(x: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for k, g in x.groupby("sector_state", dropna=False):
        z = _perf(g, str(k), "SECTOR_STATE")
        z.update({
            "sector_breadth_median": g["sector_breadth"].median(),
            "sector_turnover_ratio_median": g["sector_turnover_ratio"].median(),
            "sector_return_5d_median": g["sector_return_5d"].median(),
            "true_sector_index_rate": float(g["true_sector_index"].mean() * 100) if len(g) else np.nan,
        })
        rows.append(z)
    return pd.DataFrame(rows)


def _commonality(x: pd.DataFrame) -> pd.DataFrame:
    numeric = [
        "ret1", "ret3", "ret5", "ret10", "mfe", "mae", "market_return_5d", "market_breadth",
        "market_turnover_ratio", "sector_return_5d", "sector_breadth", "sector_turnover_ratio",
        "sector_excess3", "market_excess3", "stage_count", "pullback_volume_ratio",
        "range_contraction_ratio", "restart_volume_ratio", "resistance_room_pct", "rsi", "ma_gap",
        "distance_low60", "upper_space", "foreign_inst_flow",
    ]
    binary = [
        "market_ok", "sector_ok", "sector_conflict", "sequence_ok", "supply_drying", "price_compression",
        "support_hold", "restart_trigger", "catalyst_ok", "full_alignment", "late_wave",
    ]
    rows = []
    groups = {"WINNER": x[x["winner_group"]], "LOSER": x[x["loser_group"]], "ALL": x}
    for feature in numeric:
        w = pd.to_numeric(groups["WINNER"].get(feature), errors="coerce")
        l = pd.to_numeric(groups["LOSER"].get(feature), errors="coerce")
        rows.append({
            "feature": feature, "feature_type": "NUMERIC",
            "winner_n": int(w.notna().sum()), "loser_n": int(l.notna().sum()),
            "winner_mean": w.mean(), "loser_mean": l.mean(),
            "winner_median": w.median(), "loser_median": l.median(),
            "median_gap_winner_minus_loser": w.median() - l.median(),
        })
    for feature in binary:
        w = groups["WINNER"].get(feature, pd.Series(dtype=bool)).astype(bool)
        l = groups["LOSER"].get(feature, pd.Series(dtype=bool)).astype(bool)
        rows.append({
            "feature": feature, "feature_type": "BINARY",
            "winner_n": int(len(w)), "loser_n": int(len(l)),
            "winner_rate": float(w.mean() * 100) if len(w) else np.nan,
            "loser_rate": float(l.mean() * 100) if len(l) else np.nan,
            "rate_gap_winner_minus_loser": (float(w.mean() * 100) - float(l.mean() * 100)) if len(w) and len(l) else np.nan,
        })
    return pd.DataFrame(rows)


def _feature_lift(x: pd.DataFrame) -> pd.DataFrame:
    base_plus = float(x["plus3_first"].mean() * 100) if len(x) else np.nan
    base_ret = x["evaluation_ret"].mean()
    tests: dict[str, pd.Series] = {
        "MARKET_OK": x["market_ok"],
        "RISK_ON_BROAD": x["market_state"].eq("RISK_ON_BROAD"),
        "SECTOR_OK": x["sector_ok"],
        "SECTOR_BROAD_LEADER": x["sector_state"].isin(["BROAD_LEADER", "BROAD_LEADER_PROXY"]),
        "FULL_CONTEXT_ALIGNMENT": x["context_alignment"].eq("FULL_ALIGNMENT"),
        "SEQUENCE_OK": x["sequence_ok"],
        "SUPPLY_DRYING": x["supply_drying"],
        "PRICE_COMPRESSION": x["price_compression"],
        "RESTART_TRIGGER": x["restart_trigger"],
        "CATALYST_OK": x["catalyst_ok"],
        "FULL_ALIGNMENT_ALL": x["full_alignment"],
        "NOT_LATE_ENTRY": ~x["late_wave"],
        "RESISTANCE_ROOM_GE12": x["resistance_room_pct"].isna() | x["resistance_room_pct"].ge(12),
    }
    rows = []
    for label, mask in tests.items():
        g = x[mask.fillna(False)]
        p = _perf(g, label, "FEATURE_LIFT")
        p["baseline_plus3_first_rate"] = base_plus
        p["plus3_lift_pct_point"] = p["plus3_first_rate"] - base_plus if _finite(p["plus3_first_rate"]) else np.nan
        p["baseline_evaluation_mean"] = base_ret
        p["return_lift_pct_point"] = p["evaluation_mean"] - base_ret if _finite(p["evaluation_mean"]) else np.nan
        p["coverage_rate"] = len(g) / max(1, len(x)) * 100
        rows.append(p)
    return pd.DataFrame(rows).sort_values(["expectancy_cost20", "plus3_lift_pct_point"], ascending=False, na_position="last")


def _ablation(x: pd.DataFrame) -> pd.DataFrame:
    rules = {
        "FORMULA_ONLY": pd.Series(True, index=x.index),
        "MARKET_ONLY_FILTER": x["market_ok"],
        "SECTOR_ONLY_FILTER": x["sector_ok"],
        "SEQUENCE_ONLY_FILTER": x["sequence_ok"] & x["supply_drying"] & x["restart_trigger"],
        "ALL_CONTEXT": x["market_ok"] & x["sector_ok"] & x["sequence_ok"] & x["supply_drying"] & x["restart_trigger"] & x["catalyst_ok"],
        "ALL_MINUS_MARKET": x["sector_ok"] & x["sequence_ok"] & x["supply_drying"] & x["restart_trigger"] & x["catalyst_ok"],
        "ALL_MINUS_SECTOR": x["market_ok"] & x["sequence_ok"] & x["supply_drying"] & x["restart_trigger"] & x["catalyst_ok"],
        "ALL_MINUS_SEQUENCE": x["market_ok"] & x["sector_ok"] & x["supply_drying"] & x["restart_trigger"] & x["catalyst_ok"],
        "ALL_MINUS_SUPPLY_DRYING": x["market_ok"] & x["sector_ok"] & x["sequence_ok"] & x["restart_trigger"] & x["catalyst_ok"],
        "ALL_MINUS_RESTART": x["market_ok"] & x["sector_ok"] & x["sequence_ok"] & x["supply_drying"] & x["catalyst_ok"],
        "ALL_MINUS_CATALYST": x["market_ok"] & x["sector_ok"] & x["sequence_ok"] & x["supply_drying"] & x["restart_trigger"],
    }
    rows = []
    all_perf = None
    for label, mask in rules.items():
        p = _perf(x[mask.fillna(False)], label, "ABLATION")
        if label == "ALL_CONTEXT":
            all_perf = p
        rows.append(p)
    out = pd.DataFrame(rows)
    if all_perf:
        for col in ["expectancy_cost20", "plus3_first_rate", "stop_first_rate", "mae_mean"]:
            out[f"delta_vs_all_context_{col}"] = pd.to_numeric(out[col], errors="coerce") - float(all_perf.get(col, np.nan))
    return out


def _regime_perf(x: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for keys, g in x.groupby(["market_state", "sector_state", "context_alignment"], dropna=False):
        m, s, a = keys
        z = _perf(g, f"{m}|{s}|{a}", "MARKET_X_SECTOR_X_ALIGNMENT")
        z.update({"market_state": m, "sector_state": s, "context_alignment": a})
        rows.append(z)
    return pd.DataFrame(rows).sort_values(["n", "expectancy_cost20"], ascending=[False, False], na_position="last")


def _formula_scorecard(x: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for formula, g in x.groupby("formula", dropna=False):
        p = _perf(g, str(formula), "SEARCH_FORMULA")
        p["full_alignment_rate"] = float(g["full_alignment"].mean() * 100) if len(g) else np.nan
        p["market_conflict_rate"] = float((~g["market_ok"]).mean() * 100) if len(g) else np.nan
        p["sector_conflict_rate"] = float(g["sector_conflict"].mean() * 100) if len(g) else np.nan
        p["failure_rate"] = float(g["loser_group"].mean() * 100) if len(g) else np.nan
        p["giveback_rate"] = float(g["return_path"].eq("GIVEBACK").mean() * 100) if len(g) else np.nan
        # Score is diagnostic only. It cannot mutate LIVE ranking.
        sample = min(1.0, len(g) / 50.0)
        expectancy = p["expectancy_cost20"] if _finite(p["expectancy_cost20"]) else -5.0
        pf = min(3.0, p["profit_factor"]) if _finite(p["profit_factor"]) else 0.0
        robust = p["top5_removed_mean"] if _finite(p["top5_removed_mean"]) else -5.0
        p["diagnostic_score"] = round(sample * 20 + max(-20, min(30, expectancy * 10)) + pf * 10 + max(-15, min(20, robust * 5)), 2)
        p["policy_status"] = "POLICY_CANDIDATE" if p["n"] >= MIN_POLICY_ROWS and p["signal_days"] >= MIN_POLICY_DATES and expectancy > 0 and (p["top5_removed_mean"] if _finite(p["top5_removed_mean"]) else -1) > 0 else "RESEARCH_ONLY"
        rows.append(p)
    return pd.DataFrame(rows).sort_values(["policy_status", "diagnostic_score", "n"], ascending=[True, False, False], na_position="last")


def _missed_feature_audit(x: pd.DataFrame) -> pd.DataFrame:
    fields = {
        "market_breadth": "시장 단체상승/좁은 상승 구분",
        "market_turnover_ratio": "시장 거래대금 확산",
        "market_investor_flow": "시장 외인·기관 수급",
        "sector_breadth": "섹터 단체상승 여부",
        "sector_turnover_ratio": "섹터 거래대금 확산",
        "sector_rank": "섹터 순위",
        "sector_rank_change": "순환매 초입",
        "sector_top3_concentration": "한 종목 독주 여부",
        "sector_leader_retention": "대장주 고가 유지력",
        "true_sector_index": "진짜 섹터지수 근거",
        "supply_drying": "눌림 매도물량 감소",
        "price_compression": "가격·거래량 압축",
        "restart_trigger": "재출발 확인",
        "resistance_room_pct": "상단 저항 여유",
        "catalyst_ok": "신호시점 사용가능 재료",
        "foreign_inst_flow": "외인·기관 동시 수급",
        "mfe": "최대 유리 경로",
        "mae": "최대 불리 경로",
        "ret10": "장기 반납/Runner 판별",
    }
    rows = []
    for field, purpose in fields.items():
        availability_field = {
            "supply_drying": "supply_drying_known",
            "price_compression": "price_compression_known",
            "restart_trigger": "restart_trigger_known",
            "catalyst_ok": "catalyst_known",
        }.get(field)
        if availability_field and availability_field in x.columns:
            available = int(x[availability_field].fillna(False).astype(bool).sum())
        else:
            s = x[field] if field in x.columns else pd.Series(dtype=float)
            available = int(s.notna().sum())
        coverage = available / max(1, len(x)) * 100
        rows.append({
            "field": field, "purpose": purpose, "available_rows": available, "total_rows": len(x),
            "coverage_pct": coverage,
            "status": "OK" if coverage >= 90 else ("PARTIAL" if coverage > 0 else "MISSING"),
            "promotion_blocked": coverage < 70,
        })
    return pd.DataFrame(rows)


def _insert(text: str, block: str) -> str:
    s = str(text or "")
    if HEADER in s:
        st = s.find(HEADER)
        nxt = [s.find(h, st + len(HEADER)) for h in ["\n🧬 [패턴 시퀀스", "\n🌦️ [시장국면", "\n🤝 [절친", "\n🏆 ["]]
        nxt = [i for i in nxt if i >= 0]
        en = min(nxt) if nxt else len(s)
        s = (s[:st].rstrip() + "\n\n" + s[en:].lstrip()).strip()
    return s.rstrip() + "\n\n" + block


def _report(x: pd.DataFrame, scorecard: pd.DataFrame, failure: pd.DataFrame, ablation: pd.DataFrame, missed: pd.DataFrame, source: str, status: str) -> str:
    lines = [
        HEADER,
        f"📌 {VERSION} · CONTEXT_OUTCOME_DIAGNOSTIC · RESEARCH_ONLY=True",
        "- 검색식 승률만 보지 않고 수익경로·MFE·MAE·비용후 기대값·시장국면·섹터 breadth·거래대금 확산·시퀀스 실패원인을 함께 분석합니다.",
        f"🧾 입력: {source} | 사건 {len(x)}행 | 종목 {x['code'].nunique() if len(x) else 0} | 독립일 {x['signal_date'].nunique() if len(x) else 0} | 상태 {status}",
        f"📈 경로: BIG_WIN {int(x['return_path'].eq('BIG_WIN').sum()) if len(x) else 0} | FAST_WIN {int(x['return_path'].eq('FAST_WIN').sum()) if len(x) else 0} | NORMAL_WIN {int(x['return_path'].eq('NORMAL_WIN').sum()) if len(x) else 0} | GIVEBACK {int(x['return_path'].eq('GIVEBACK').sum()) if len(x) else 0} | FAST_LOSS {int(x['return_path'].eq('FAST_LOSS').sum()) if len(x) else 0} | GRIND_LOSS {int(x['return_path'].eq('GRIND_LOSS').sum()) if len(x) else 0} | NO_EDGE {int(x['return_path'].eq('NO_EDGE').sum()) if len(x) else 0} | PATH_CONFLICT {int(x['return_path'].eq('PATH_CONFLICT').sum()) if len(x) else 0}",
        f"🌍 정렬: FULL_ALIGNMENT {int(x['context_alignment'].eq('FULL_ALIGNMENT').sum()) if len(x) else 0} | SECTOR_CONFLICT {int(x['context_alignment'].eq('SECTOR_CONFLICT').sum()) if len(x) else 0} | FULL_CONFLICT {int(x['context_alignment'].eq('FULL_CONFLICT').sum()) if len(x) else 0}",
        "🏆 [검색식 기대값 상위 · 표본/독립일/상위종목제거 견고성 동시 확인]",
    ]
    good = scorecard[pd.to_numeric(scorecard.get("n"), errors="coerce").ge(5)].head(6) if not scorecard.empty else pd.DataFrame()
    if good.empty:
        lines.append("- 아직 표본 5건 이상 검색식이 없거나 성과 입력이 부족합니다.")
    else:
        for _, r in good.iterrows():
            lines.append(f"- {r.get('label')}: n{int(r.get('n',0))}/일{int(r.get('signal_days',0))} | 비용20bp 기대 {r.get('expectancy_cost20',np.nan):+.2f}% | +3선도 {r.get('plus3_first_rate',np.nan):.1f}% | PF {r.get('profit_factor',np.nan):.2f} | 상위5제거 {r.get('top5_removed_mean',np.nan):+.2f}% | {r.get('policy_status')}")
    lines.append("⚠️ [손실 포착력이 높은 실패원인]")
    for _, r in failure.head(6).iterrows():
        lines.append(f"- {r.get('reason')}: 손실포착 {r.get('loser_capture_rate',np.nan):.1f}% | 표시군 손실률 {r.get('flagged_loss_rate',np.nan):.1f}% | 전체대비 +{r.get('incremental_loss_rate_vs_all',np.nan):.1f}%p")
    lines.append("🧪 [Ablation 핵심]")
    for _, r in ablation[ablation["label"].isin(["FORMULA_ONLY", "ALL_CONTEXT", "ALL_MINUS_MARKET", "ALL_MINUS_SECTOR", "ALL_MINUS_SUPPLY_DRYING", "ALL_MINUS_RESTART"])].iterrows():
        lines.append(f"- {r.get('label')}: n{int(r.get('n',0))} | 비용20bp 기대 {r.get('expectancy_cost20',np.nan):+.2f}% | +3 {r.get('plus3_first_rate',np.nan):.1f}% | SL {r.get('stop_first_rate',np.nan):.1f}% | MAE {r.get('mae_mean',np.nan):+.2f}%")
    missing = missed[missed["status"].ne("OK")]
    lines.append(f"🧩 데이터 누락: {len(missing)}개 변수 PARTIAL/MISSING · 70% 미만은 승격 근거 사용 금지")
    lines += [
        "🔒 정책: 결과는 SHADOW 진단만 수행하며 LIVE 점수·순위·후보·AI Pick·진입·청산·주문 변경 0.",
        f"- 승격 최소조건: 검색식별 {MIN_POLICY_ROWS}행·{MIN_POLICY_DATES}독립일 + 비용20bp 기대값 양수 + 상위5종목 제거 후 양수 + walk-forward 유지.",
        f"- Actions CSV: {EVENT_MASTER_FILE} · {MARKET_CONTEXT_FILE} · {SECTOR_CONTEXT_FILE} · {RETURN_PATH_FILE} · {COMMONALITY_FILE} · {FAILURE_FILE} · {FEATURE_LIFT_FILE} · {ABLATION_FILE} · {REGIME_PERF_FILE} · {SCORECARD_FILE} · {MISSED_FEATURE_FILE} · {READINESS_FILE}",
    ]
    return "\n".join(lines)


def run_backtest(eval_df: pd.DataFrame | None = None, output_dir: str | Path = "reports", base_report: str = "") -> tuple[str, dict[str, Any]]:
    out = _out(output_dir)
    base, source = _canonical_base(out, eval_df)
    formula_base = _dedupe_event_rows(base)
    if formula_base.empty:
        empty = pd.DataFrame()
        for f in [EVENT_MASTER_FILE, MARKET_CONTEXT_FILE, SECTOR_CONTEXT_FILE, RETURN_PATH_FILE, COMMONALITY_FILE, FAILURE_FILE, FEATURE_LIFT_FILE, ABLATION_FILE, REGIME_PERF_FILE, SCORECARD_FILE, MISSED_FEATURE_FILE]:
            _write(out / f, empty)
        ready = pd.DataFrame([{"version": VERSION, "status": "NO_INPUT", "event_rows": 0, "policy_ready": False, "research_only": True, "live_logic_changed": False, "real_order_changed": False}])
        _write(out / READINESS_FILE, ready)
        block = _report(pd.DataFrame(columns=["code","signal_date","return_path","context_alignment"]), empty, empty, empty, empty, source, "NO_INPUT")
        (out / REPORT_FILE).write_text(block, encoding="utf-8")
        return _insert(base_report, block), {"readiness": ready}

    # Context/outcome diagnostics must be one row per date/code. A stock matching
    # several formulas must not receive several votes in winner/loser or regime analysis.
    formula_map = (formula_base.groupby(["signal_date", "code"])["formula"]
                   .agg(lambda s: "|".join(sorted(set(map(str, s)))))
                   .rename("formula_list").reset_index())
    stock_base = (formula_base.sort_values(["signal_date", "code", "formula"], kind="stable")
                  .drop_duplicates(["signal_date", "code"], keep="first")
                  .rename(columns={"formula": "primary_formula"}))
    stock_base = stock_base.merge(formula_map, on=["signal_date", "code"], how="left")
    stock_base["formula_count"] = stock_base["formula_list"].fillna("").map(lambda v: len([z for z in str(v).split("|") if z]))

    x = _merge_context(stock_base, out)
    x = _derive_features(x)
    x = _classify_path(x)
    x, failure = _failure_reasons(x)

    market = _market_context_table(x)
    sector = _sector_context_table(x)
    paths = _return_path_table(x)
    common = _commonality(x)
    lift = _feature_lift(x)
    ablation = _ablation(x)
    regime = _regime_perf(x)
    # Formula scorecard uses formula-expanded rows, while all other diagnostics use
    # one stock event per date/code to avoid multi-match weighting bias.
    formula_x = formula_base[["signal_date", "code", "formula"]].merge(
        x.drop(columns=[c for c in ["primary_formula", "formula_list", "formula_count"] if c in x.columns]),
        on=["signal_date", "code"], how="inner", suffixes=("", "_event")
    )
    scorecard = _formula_scorecard(formula_x)
    missed = _missed_feature_audit(x)

    # Stable event id for append-only Google Sheet dedupe.
    x["event_id"] = x.apply(lambda r: hashlib.sha256(f"{r.get('signal_date')}|{r.get('code')}".encode()).hexdigest()[:24], axis=1)
    x["version"] = VERSION
    x["research_only"] = True
    x["live_logic_changed"] = False
    x["real_order_changed"] = False

    snapshot_id = _sha_frame(x[[c for c in ["signal_date","code","formula","evaluation_ret","market_state","sector_state","return_path"] if c in x.columns]])
    generated_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    for table in [x, market, sector, paths, common, failure, lift, ablation, regime, scorecard, missed]:
        table["version"] = VERSION
        table["snapshot_id"] = snapshot_id
        table["generated_at"] = generated_at
        table["research_only"] = True
        table["live_logic_changed"] = False
        table["real_order_changed"] = False

    _write(out / EVENT_MASTER_FILE, x)
    _write(out / MARKET_CONTEXT_FILE, market)
    _write(out / SECTOR_CONTEXT_FILE, sector)
    _write(out / RETURN_PATH_FILE, paths)
    _write(out / COMMONALITY_FILE, common)
    _write(out / FAILURE_FILE, failure)
    _write(out / FEATURE_LIFT_FILE, lift)
    _write(out / ABLATION_FILE, ablation)
    _write(out / REGIME_PERF_FILE, regime)
    _write(out / SCORECARD_FILE, scorecard)
    _write(out / MISSED_FEATURE_FILE, missed)

    eval_rows = int(x["evaluation_ret"].notna().sum())
    days = int(x.loc[x["evaluation_ret"].notna(), "signal_date"].nunique())
    candidate_count = int(scorecard["policy_status"].eq("POLICY_CANDIDATE").sum()) if not scorecard.empty else 0
    context_cov = float((x["market_state"].ne("NEUTRAL") | x["market_regime_raw"].ne("UNKNOWN")).mean() * 100)
    sector_cov = float(x["sector_state"].ne("UNKNOWN").mean() * 100)
    status = "VALID_SHADOW" if eval_rows > 0 else "VALID_SHADOW_DATA_WARMUP"
    policy_ready = bool(eval_rows >= MIN_POLICY_ROWS and days >= MIN_POLICY_DATES and candidate_count > 0 and context_cov >= 70 and sector_cov >= 70)
    ready = pd.DataFrame([{
        "version": VERSION, "status": status, "source": source, "event_rows": len(x), "evaluated_rows": eval_rows,
        "signal_days": days, "formula_count": int(formula_x["formula"].nunique()), "formula_event_rows": len(formula_x), "policy_candidate_count": candidate_count,
        "market_context_coverage_pct": context_cov, "sector_context_coverage_pct": sector_cov,
        "full_alignment_rows": int(x["full_alignment"].sum()), "winner_rows": int(x["winner_group"].sum()),
        "loser_rows": int(x["loser_group"].sum()), "policy_ready": policy_ready,
        "snapshot_id": snapshot_id, "generated_at": generated_at,
        "research_only": True, "live_logic_changed": False, "real_order_changed": False,
    }])
    _write(out / READINESS_FILE, ready)
    block = _report(x, scorecard, failure, ablation, missed, source, status)
    (out / REPORT_FILE).write_text(block, encoding="utf-8")
    return _insert(base_report, block), {
        "event_master": x, "market_context": market, "sector_context": sector, "return_path": paths,
        "commonality": common, "failure": failure, "feature_lift": lift, "ablation": ablation,
        "regime_performance": regime, "scorecard": scorecard, "missed_features": missed, "readiness": ready,
    }


def force_report(text: str, output_dir: str | Path = "reports") -> str:
    p = _out(output_dir) / REPORT_FILE
    if not p.exists():
        return str(text or "")
    try:
        return _insert(str(text or ""), p.read_text(encoding="utf-8"))
    except Exception:
        return str(text or "")


if __name__ == "__main__":
    report, tables = run_backtest(output_dir=os.environ.get("V1080_BACKTEST_OUTPUT_DIR", "reports"))
    print(report)
