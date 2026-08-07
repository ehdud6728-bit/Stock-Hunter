from __future__ import annotations

import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6.19"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🧭🐻 [하락장 고유승자 × 0패턴 × 안정성 원천정합·재현일 잠금 진단 · RESEARCH_ONLY]"
REPORT_FILE = "v73_bear_winner_stability_report.txt"
STABILITY_INPUT_AUDIT_FILE = "v73_formula_stability_input_audit.csv"
STABILITY_RECON_FILE = "v73_formula_stability_reconciliation.csv"
REPLAY_LOCK_FILE = "v73_replay_date_lock.csv"
DUPLICATE_CALL_AUDIT_FILE = "v73_duplicate_combo_call_audit.csv"
LOCKED_POLICY_FAILURE_FILE = "v73_locked_policy_failure_audit.csv"
PATTERN_ONLY_EVENT_FILE = "v73_pattern_only_sequence_event_audit.csv"
PATTERN_ONLY_COMMONALITY_FILE = "v73_pattern_only_sequence_commonality.csv"

UNIQUE_MASTER_FILE = "v73_bear_winner_event_master_unique.csv"
FORMULA_MEMBERSHIP_FILE = "v73_bear_winner_formula_membership.csv"
UNIQUE_MATCHED_FILE = "v73_bear_winner_matched_control_unique.csv"
UNIQUE_COMMONALITY_FILE = "v73_bear_winner_commonality_unique.csv"
ZERO_PATTERN_FILE = "v73_zero_pattern_winner_audit.csv"
ZERO_PATTERN_FEATURE_FILE = "v73_zero_pattern_feature_commonality.csv"
STABILITY_MATRIX_FILE = "v73_formula_stability_matrix.csv"
STABILITY_POLICY_FILE = "v73_formula_stability_policy.csv"
OFFICIAL_ARCHIVE_FILE = "v73_geo_official_archive_ledger.csv"
SECTOR_BREADTH_FILE = "v73_sector_breadth_history.csv"
SECTOR_BREADTH_JOIN_FILE = "v73_sector_breadth_join_audit.csv"
MINUTE_SCALE_READINESS_FILE = "v73_minute_scale_in_readiness.csv"
READINESS_FILE = "v73_bear_winner_stability_readiness.csv"

MIN_ROWS = 30
MIN_SIGNAL_DAYS = 10
BEAR_REGIMES = {"PANIC", "BEAR", "RISK_OFF", "RISK_OFF_BROAD", "RISK_OFF_NARROW"}
WINNER_CLASSES = {"BEAR_TRUE_WINNER", "BEAR_FAST_WINNER", "BEAR_BIG_WINNER"}
CAUSAL_MODES = {"FORWARD_CAUSAL", "OFFICIAL_ARCHIVE_CAUSAL"}


def _out(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _read(path: Path, dtype: dict[str, Any] | None = None) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=dtype, low_memory=False)
        except Exception:
            continue
    return pd.DataFrame()


def _write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_csv(path, index=False, encoding="utf-8-sig")


def _norm_code(v: Any) -> str:
    digits = re.sub(r"\D", "", str(v or ""))
    return digits.zfill(6)[-6:] if digits else ""


def _num(v: Any, default: float = np.nan) -> float:
    try:
        x = float(str(v).replace(",", "").replace("%", "").strip())
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def _pick(df: pd.DataFrame, names: Iterable[str]) -> str | None:
    return next((c for c in names if c in df.columns), None)


def _series_str(df: pd.DataFrame, names: Iterable[str], default: str = "") -> pd.Series:
    c = _pick(df, names)
    return df[c].fillna(default).astype(str) if c else pd.Series(default, index=df.index, dtype=str)


def _series_num(df: pd.DataFrame, names: Iterable[str], default: float = np.nan) -> pd.Series:
    c = _pick(df, names)
    return pd.to_numeric(df[c], errors="coerce") if c else pd.Series(default, index=df.index, dtype=float)


def _trim_mean(s: pd.Series, p: float = 0.10) -> float:
    z = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    if z.empty:
        return np.nan
    k = int(len(z) * p)
    if k and len(z) > 2 * k:
        z = z.iloc[k:-k]
    return float(z.mean())


def _top_removed(s: pd.Series, n: int = 5) -> float:
    z = pd.to_numeric(s, errors="coerce").dropna().sort_values(ascending=False)
    return float(z.iloc[n:].mean()) if len(z) > n else np.nan


def _profit_factor(s: pd.Series) -> float:
    z = pd.to_numeric(s, errors="coerce").dropna()
    gains = z[z > 0].sum()
    losses = -z[z < 0].sum()
    if losses <= 0:
        return np.inf if gains > 0 else np.nan
    return float(gains / losses)


def _sha_frame(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "EMPTY"
    use = [c for c in cols if c in df.columns]
    if not use:
        return hashlib.sha256(str(len(df)).encode()).hexdigest()[:20]
    raw = df[use].astype(str).sort_values(use, kind="stable").to_csv(index=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _insert(text: str, block: str) -> str:
    s = str(text or "")
    if HEADER in s:
        st = s.find(HEADER)
        candidates = [s.find(h, st + len(HEADER)) for h in ["\n🌍🐻 [", "\n🏆 [V48", "\n🧭 [시장 ×"]]
        candidates = [x for x in candidates if x >= 0]
        en = min(candidates) if candidates else len(s)
        s = (s[:st].rstrip() + "\n\n" + s[en:].lstrip()).strip()
    return s.rstrip() + "\n\n" + block


def _ensure_templates(out: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    official_cols = [
        "source_key", "source_type", "source_name", "source_domain", "source_url", "title", "summary",
        "event_type", "event_occurred_at", "official_at", "published_at", "first_seen_at", "causal_mode",
        "same_day_causal_eligible", "official_source", "independent_source", "code", "name", "sector", "theme",
        "direct_benefit", "materiality", "raw_payload_sha256",
    ]
    sector_cols = [
        "signal_date", "sector", "market", "sector_median_ret", "sector_breadth_pct", "sector_turnover",
        "sector_turnover_change_pct", "leader_count", "member_count", "market_excess_pct", "source_name",
        "captured_at", "causal_mode",
    ]
    op = out / OFFICIAL_ARCHIVE_FILE
    sp = out / SECTOR_BREADTH_FILE
    if not op.exists():
        _write(op, pd.DataFrame(columns=official_cols))
    if not sp.exists():
        _write(sp, pd.DataFrame(columns=sector_cols))
    official = _read(op, dtype={"code": str})
    sector = _read(sp)
    for c in official_cols:
        if c not in official.columns:
            official[c] = ""
    for c in sector_cols:
        if c not in sector.columns:
            sector[c] = ""
    return official[official_cols], sector[sector_cols]


def _normalize_formula_frame(q: pd.DataFrame, source_label: str, source_file: str, *, authoritative: bool) -> tuple[pd.DataFrame, str]:
    if q.empty:
        return pd.DataFrame(), "NO_INPUT"
    dc = _pick(q, ["signal_date", "date", "신호일"])
    cc = _pick(q, ["code", "Code", "종목코드"])
    if not dc or not cc:
        return pd.DataFrame(), "INVALID_SCHEMA"
    q = q.copy()
    q["signal_date"] = pd.to_datetime(q[dc], errors="coerce").dt.normalize()
    q["code"] = q[cc].map(_norm_code)
    q = q[q["signal_date"].notna() & q["code"].ne("")].copy()
    q["formula"] = _series_str(q, ["formula", "검색식", "primary_formula", "search_pattern_primary", "pattern_combo"], "UNCLASSIFIED").replace({"": "UNCLASSIFIED", "nan": "UNCLASSIFIED"})
    q["name"] = _series_str(q, ["name", "Name", "종목명"], "")
    q["sector"] = _series_str(q, ["sector", "sector_label", "Sector", "섹터", "업종"], "UNKNOWN").replace({"": "UNKNOWN", "nan": "UNKNOWN"})
    q["market"] = _series_str(q, ["market", "Market", "exchange"], "UNKNOWN")
    q["market_regime"] = _series_str(q, ["market_regime_causal", "market_regime", "market_state"], "UNKNOWN").str.upper()
    q["winner_class"] = _series_str(q, ["winner_class"], "UNCLASSIFIED")
    q["geo_linked"] = _series_str(q, ["geo_linked"], "False").map(_bool)
    q["directness"] = _series_str(q, ["directness"], "UNKNOWN")
    q["event_family"] = _series_str(q, ["event_family"], "NO_CAUSAL_GEO_EVENT")
    q["event_stage"] = _series_str(q, ["event_stage"], "UNKNOWN")
    q["geo_event_id"] = _series_str(q, ["geo_event_id"], "")
    q["bear_geo_bucket"] = _series_str(q, ["bear_geo_bucket"], "")

    ret_aliases = {
        "ret1": (["next1_close_ret"] if authoritative else []) + ["ret1", "day1_ret", "next1_close_ret"],
        "ret3": (["next3_close_ret"] if authoritative else []) + ["ret3", "day3_ret", "next3_close_ret"],
        "ret5": (["next5_close_ret"] if authoritative else []) + ["ret5", "day5_ret", "next5_close_ret"],
    }
    source_cols: dict[str, str] = {}
    for dest, names in ret_aliases.items():
        c = _pick(q, names)
        source_cols[dest] = c or ""
        q[dest] = pd.to_numeric(q[c], errors="coerce") if c else pd.Series(np.nan, index=q.index, dtype=float)
    if authoritative and source_cols["ret3"] != "next3_close_ret":
        return pd.DataFrame(), "INVALID_AUTHORITATIVE_RET3_SOURCE"

    for dest, names in {
        "market_excess3": ["market_excess3", "excess3", "d3_market_excess"],
        "market_fwd_ret3": ["market_fwd_ret3", "market_ret3", "benchmark_ret3"],
        "mfe": ["mfe", "mfe_5d", "max_up_5d"],
        "mae": ["mae", "mae_5d", "max_down_5d"],
        "turnover": ["turnover", "trading_value", "거래대금"],
        "volume_ratio": ["volume_ratio", "vol_ratio", "거래량비율"],
        "distance_low60": ["distance_low60", "distance_from_60d_low_pct", "low60_distance_pct"],
        "upper_space": ["upper_space", "upper_resistance_distance_pct", "upper_space_pct"],
        "relative_strength_5d": ["relative_strength_5d", "rs5", "stock_minus_market_5d"],
        "close_location": ["close_location", "close_position", "종가위치"],
        "pullback_volume_ratio": ["pullback_volume_ratio", "pullback_vol_ratio"],
    }.items():
        q[dest] = _series_num(q, names)
    q["plus3_first"] = _series_str(q, ["plus3_first", "hit_plus3_first", "plus3_first_10d"], "False").map(_bool)
    q["stop_first"] = _series_str(q, ["stop_first", "hit_stop_first", "stop_first_10d"], "False").map(_bool)
    q["formula_source_label"] = source_label
    q["formula_source_file"] = source_file
    q["ret1_source_column"] = source_cols["ret1"]
    q["ret3_source_column"] = source_cols["ret3"]
    q["ret5_source_column"] = source_cols["ret5"]
    q["authoritative_formula_source"] = bool(authoritative)

    missing_class = q["winner_class"].eq("UNCLASSIFIED")
    bear = q["market_regime"].isin(BEAR_REGIMES)
    r3 = pd.to_numeric(q["ret3"], errors="coerce")
    ex3 = pd.to_numeric(q["market_excess3"], errors="coerce")
    q.loc[missing_class & bear & r3.gt(0) & ex3.gt(0), "winner_class"] = "BEAR_TRUE_WINNER"
    q.loc[missing_class & bear & r3.le(0) & ex3.gt(0), "winner_class"] = "BEAR_RELATIVE_SURVIVOR"
    q.loc[missing_class & bear & r3.gt(0) & ex3.le(0), "winner_class"] = "BEAR_FALSE_WINNER"
    q.loc[missing_class & bear & r3.le(0) & ex3.le(0), "winner_class"] = "BEAR_FAILURE"
    q.loc[missing_class & ~bear, "winner_class"] = "NON_BEAR"
    q = q.sort_values(["signal_date", "code", "formula"], kind="stable").drop_duplicates(["signal_date", "code", "formula"], keep="last")
    return q.reset_index(drop=True), "OK"


def _prepare_formula_rows(out: Path, eval_df: pd.DataFrame | None = None) -> tuple[pd.DataFrame, str]:
    # Unique bear-winner diagnostics may use the enriched geopolitical event ledger.
    candidates = [
        ("GEO_FORMULA_EXPANDED", "v73_bear_geo_winner_event_master.csv", False),
        ("FORMULA_EXPLODED", "v72_search_formula_universe_exploded_eval.csv", True),
    ]
    for label, name, authoritative in candidates:
        path = out / name
        z = _read(path, dtype={"code": str})
        if not z.empty:
            q, status = _normalize_formula_frame(z, label, name, authoritative=authoritative)
            return q, label if status == "OK" else status
    if isinstance(eval_df, pd.DataFrame) and not eval_df.empty:
        q, status = _normalize_formula_frame(eval_df.copy(), "CALLER_DF", "CALLER_DF", authoritative=False)
        return q, "CALLER_DF" if status == "OK" else status
    return pd.DataFrame(), "NO_INPUT"


def _prepare_stability_rows(out: Path, eval_df: pd.DataFrame | None = None) -> tuple[pd.DataFrame, str, pd.DataFrame]:
    # Formula stability MUST use the authoritative full-universe PRE formula ledger.
    candidates = [
        ("AUTHORITATIVE_FORMULA_EXPLODED", "v72_search_formula_universe_exploded_eval.csv", True),
        ("CALLER_DF", "CALLER_DF", False),
        ("GEO_FORMULA_EXPANDED_FALLBACK", "v73_bear_geo_winner_event_master.csv", False),
    ]
    selected = pd.DataFrame(); selected_label = "NO_INPUT"; selected_file = ""; norm_status = "NO_INPUT"
    for label, name, authoritative in candidates:
        if name == "CALLER_DF":
            z = eval_df.copy() if isinstance(eval_df, pd.DataFrame) else pd.DataFrame()
        else:
            z = _read(out / name, dtype={"code": str})
        if z.empty:
            continue
        q, status = _normalize_formula_frame(z, label, name, authoritative=authoritative)
        if q.empty:
            norm_status = status
            continue
        selected, selected_label, selected_file, norm_status = q, label, name, status
        break
    audit = pd.DataFrame([{
        "selected_source_label": selected_label,
        "selected_source_file": selected_file,
        "normalization_status": norm_status,
        "authoritative_source_selected": selected_label == "AUTHORITATIVE_FORMULA_EXPLODED",
        "rows": len(selected),
        "formulas": selected["formula"].nunique() if not selected.empty else 0,
        "signal_days": selected["signal_date"].nunique() if not selected.empty else 0,
        "ret3_source_column": _best_text(selected.get("ret3_source_column", pd.Series(dtype=str)), "") if not selected.empty else "",
        "required_ret3_source_column": "next3_close_ret",
        "status": "VALID" if selected_label == "AUTHORITATIVE_FORMULA_EXPLODED" and norm_status == "OK" else ("FALLBACK_NOT_POLICY_EVALUABLE" if not selected.empty else norm_status),
    }])
    return selected, selected_label, audit

def _best_text(values: pd.Series, default: str = "") -> str:
    x = [str(v) for v in values.fillna("").tolist() if str(v).strip() and str(v).strip().lower() != "nan"]
    return x[-1] if x else default


def _directness_rank(v: str) -> int:
    return {"GEO_DIRECT": 6, "GEO_CHAIN": 5, "GEO_SECTOR": 4, "GEO_NARRATIVE": 3, "GEO_NEGATIVE": 2, "UNKNOWN": 1}.get(str(v), 0)


def _unique_master(formula_rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if formula_rows.empty:
        return pd.DataFrame(), pd.DataFrame()
    member_cols = [
        "signal_date", "code", "name", "formula", "market", "market_regime", "sector", "winner_class",
        "ret1", "ret3", "ret5", "market_excess3", "market_fwd_ret3", "mfe", "mae", "plus3_first", "stop_first",
        "geo_linked", "geo_event_id", "event_family", "event_stage", "directness", "bear_geo_bucket",
    ]
    membership = formula_rows[[c for c in member_cols if c in formula_rows.columns]].copy()
    membership["event_id"] = membership.apply(lambda r: "BEAR-" + hashlib.sha256(f"{r['signal_date']}|{r['code']}".encode()).hexdigest()[:20], axis=1)

    rows: list[dict[str, Any]] = []
    for (d, code), g in formula_rows.groupby(["signal_date", "code"], sort=True):
        formulas = sorted(set(g["formula"].astype(str)))
        direct_idx = g["directness"].astype(str).map(_directness_rank).idxmax() if len(g) else g.index[0]
        base = g.loc[direct_idx].to_dict()
        base["signal_date"] = d
        base["code"] = code
        base["event_id"] = "BEAR-" + hashlib.sha256(f"{d}|{code}".encode()).hexdigest()[:20]
        base["formula_count"] = len(formulas)
        base["formula_list"] = " | ".join(formulas)
        base["primary_formula"] = formulas[0] if formulas else "UNCLASSIFIED"
        base["formula"] = base["primary_formula"]
        base["formula_row_count"] = len(g)
        base["return_consistency_fail"] = bool(pd.to_numeric(g["ret3"], errors="coerce").dropna().nunique() > 1)
        base["winner_class_consistency_fail"] = bool(g["winner_class"].dropna().astype(str).nunique() > 1)
        base["market_regime_consistency_fail"] = bool(g["market_regime"].dropna().astype(str).nunique() > 1)
        base["geo_linked"] = bool(g["geo_linked"].map(_bool).any())
        if base["geo_linked"]:
            linked = g[g["geo_linked"].map(_bool)].copy()
            if not linked.empty:
                linked["_rank"] = linked["directness"].astype(str).map(_directness_rank)
                lr = linked.sort_values(["_rank"], ascending=False).iloc[0]
                for c in ["geo_event_id", "event_family", "event_stage", "directness", "bear_geo_bucket"]:
                    base[c] = lr.get(c, base.get(c, ""))
        rows.append(base)
    master = pd.DataFrame(rows).sort_values(["signal_date", "code"], kind="stable").reset_index(drop=True)
    membership = membership.merge(master[["signal_date", "code", "event_id", "formula_count"]], on=["signal_date", "code"], how="left", suffixes=("", "_master"))
    return master, membership


def _matched_controls(master: pd.DataFrame) -> pd.DataFrame:
    if master.empty:
        return pd.DataFrame()
    bear = master[master["market_regime"].isin(BEAR_REGIMES)].copy()
    winners = bear[bear["winner_class"].isin(WINNER_CLASSES)].copy()
    failures = bear[bear["winner_class"].eq("BEAR_FAILURE")].copy()
    rows: list[dict[str, Any]] = []
    used: set[str] = set()
    for _, w in winners.iterrows():
        cand = failures[(failures["signal_date"].eq(w["signal_date"])) & (~failures["code"].eq(w["code"]))].copy()
        if cand.empty:
            continue
        cand["same_market"] = cand["market"].astype(str).eq(str(w.get("market")))
        cand["same_sector"] = cand["sector"].astype(str).eq(str(w.get("sector"))) & cand["sector"].ne("UNKNOWN")
        wf = set(str(w.get("formula_list", "")).split(" | "))
        cand["shared_formula_count"] = cand["formula_list"].astype(str).map(lambda x: len(wf & set(x.split(" | "))))
        cand["match_level"] = cand["same_market"].astype(int) + cand["same_sector"].astype(int) * 4 + cand["shared_formula_count"].clip(upper=2) * 2
        wt = _num(w.get("turnover"))
        if math.isfinite(wt) and wt > 0:
            cand["turnover_distance"] = (np.log1p(pd.to_numeric(cand["turnover"], errors="coerce").fillna(0)) - math.log1p(wt)).abs()
        else:
            cand["turnover_distance"] = 0.0
        cand = cand[~cand["event_id"].isin(used)].sort_values(["match_level", "turnover_distance", "code"], ascending=[False, True, True])
        if cand.empty:
            continue
        c = cand.iloc[0]
        used.add(str(c["event_id"]))
        rows.append({
            "match_id": "UMATCH-" + hashlib.sha256(f"{w['event_id']}|{c['event_id']}".encode()).hexdigest()[:20],
            "signal_date": w["signal_date"], "winner_event_id": w["event_id"], "control_event_id": c["event_id"],
            "winner_code": w["code"], "winner_name": w.get("name", ""), "control_code": c["code"], "control_name": c.get("name", ""),
            "winner_formula_list": w.get("formula_list", ""), "control_formula_list": c.get("formula_list", ""),
            "shared_formula_count": int(c["shared_formula_count"]), "winner_sector": w.get("sector", ""), "control_sector": c.get("sector", ""),
            "same_market": bool(c["same_market"]), "same_sector": bool(c["same_sector"]), "match_level": int(c["match_level"]),
            "winner_ret3": w.get("ret3"), "control_ret3": c.get("ret3"), "winner_excess3": w.get("market_excess3"), "control_excess3": c.get("market_excess3"),
            "winner_directness": w.get("directness", "UNKNOWN"), "winner_event_family": w.get("event_family", ""),
        })
    return pd.DataFrame(rows)


def _commonality(master: pd.DataFrame) -> pd.DataFrame:
    if master.empty:
        return pd.DataFrame()
    bear = master[master["market_regime"].isin(BEAR_REGIMES)]
    win = bear[bear["winner_class"].isin(WINNER_CLASSES)]
    fail = bear[bear["winner_class"].eq("BEAR_FAILURE")]
    features = ["turnover", "volume_ratio", "distance_low60", "upper_space", "relative_strength_5d", "close_location", "pullback_volume_ratio", "mfe", "mae", "formula_count"]
    rows = []
    for f in features:
        if f not in master.columns:
            continue
        a = pd.to_numeric(win[f], errors="coerce").dropna()
        b = pd.to_numeric(fail[f], errors="coerce").dropna()
        if a.empty and b.empty:
            continue
        rows.append({
            "feature": f, "comparison": "UNIQUE_BEAR_WINNER_VS_FAILURE", "winner_n": len(a), "failure_n": len(b),
            "winner_median": a.median() if len(a) else np.nan, "failure_median": b.median() if len(b) else np.nan,
            "median_diff": (a.median() - b.median()) if len(a) and len(b) else np.nan,
            "winner_mean": a.mean() if len(a) else np.nan, "failure_mean": b.mean() if len(b) else np.nan,
        })
    for dim in ["geo_linked", "directness", "event_family", "event_stage", "formula_count"]:
        if dim not in bear.columns:
            continue
        for label, g in bear.groupby(dim, dropna=False):
            rows.append({
                "feature": dim, "label": str(label), "comparison": "UNIQUE_EVENT_DIMENSION", "winner_n": int(g["winner_class"].isin(WINNER_CLASSES).sum()),
                "failure_n": int(g["winner_class"].eq("BEAR_FAILURE").sum()), "winner_median": pd.to_numeric(g.loc[g["winner_class"].isin(WINNER_CLASSES), "ret3"], errors="coerce").median(),
                "failure_median": pd.to_numeric(g.loc[g["winner_class"].eq("BEAR_FAILURE"), "ret3"], errors="coerce").median(),
            })
    return pd.DataFrame(rows)


def _load_pattern_membership(out: Path) -> pd.DataFrame:
    q = _read(out / "v72_pattern_ai_cross_signal_audit.csv", dtype={"code": str})
    if q.empty:
        return pd.DataFrame(columns=["signal_date", "code", "pattern_overlap_count", "overlap_degree", "pattern_exact_combo"])
    dc = _pick(q, ["signal_date", "date"])
    cc = _pick(q, ["code", "Code"])
    if not dc or not cc:
        return pd.DataFrame(columns=["signal_date", "code", "pattern_overlap_count", "overlap_degree", "pattern_exact_combo"])
    q["signal_date"] = pd.to_datetime(q[dc], errors="coerce").dt.normalize()
    q["code"] = q[cc].map(_norm_code)
    q["pattern_overlap_count"] = _series_num(q, ["pattern_overlap_count"], 0).fillna(0).astype(int)
    q["overlap_degree"] = _series_str(q, ["overlap_degree"], "UNKNOWN")
    q["pattern_exact_combo"] = _series_str(q, ["pattern_exact_combo"], "UNCLASSIFIED")
    return q[["signal_date", "code", "pattern_overlap_count", "overlap_degree", "pattern_exact_combo"]].drop_duplicates(["signal_date", "code"], keep="last")


def _group_perf(g: pd.DataFrame, label: str, dimension: str) -> dict[str, Any]:
    r = pd.to_numeric(g.get("ret3"), errors="coerce")
    ex = pd.to_numeric(g.get("market_excess3"), errors="coerce")
    date_mean = g.assign(_r=r).groupby("signal_date")["_r"].mean() if len(g) else pd.Series(dtype=float)
    return {
        "dimension": dimension, "label": label, "n": len(g), "stocks": g["code"].nunique() if len(g) else 0, "signal_days": g["signal_date"].nunique() if len(g) else 0,
        "d3_mean": r.mean(), "d3_median": r.median(), "d3_trim10": _trim_mean(r), "d3_top5_removed": _top_removed(r),
        "net20_mean": (r - 0.20).mean(), "net50_mean": (r - 0.50).mean(), "excess3_median": ex.median(), "excess3_trim10": _trim_mean(ex), "excess3_top5_removed": _top_removed(ex),
        "winner_rate": float(g["winner_class"].isin(WINNER_CLASSES).mean() * 100) if len(g) else np.nan, "failure_rate": float(g["winner_class"].eq("BEAR_FAILURE").mean() * 100) if len(g) else np.nan,
        "positive_signal_day_rate": float((date_mean > 0).mean() * 100) if len(date_mean) else np.nan,
    }


def _zero_pattern(master: pd.DataFrame, pattern: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if master.empty:
        return master, pd.DataFrame(), pd.DataFrame()
    q = master.merge(pattern, on=["signal_date", "code"], how="left")
    q["pattern_data_available"] = q["pattern_overlap_count"].notna()
    q["pattern_overlap_count"] = pd.to_numeric(q["pattern_overlap_count"], errors="coerce")
    q["overlap_degree"] = q["overlap_degree"].fillna("UNKNOWN")
    q["pattern_exact_combo"] = q["pattern_exact_combo"].fillna("UNAVAILABLE")
    rows = []
    available = q[q["pattern_data_available"]].copy()
    for label, g in available.groupby("overlap_degree"):
        rows.append(_group_perf(g, str(label), "OVERLAP_DEGREE_UNIQUE_EVENT"))
    bear = available[available["market_regime"].isin(BEAR_REGIMES)]
    for label, g in bear.groupby("overlap_degree"):
        rows.append(_group_perf(g, str(label), "BEAR_OVERLAP_DEGREE_UNIQUE_EVENT"))
    zero = bear[bear["pattern_overlap_count"].eq(0)]
    for wc, g in zero.groupby("winner_class"):
        rows.append(_group_perf(g, str(wc), "ZERO_PATTERN_WINNER_CLASS"))
    audit = pd.DataFrame(rows)

    features = []
    zw = zero[zero["winner_class"].isin(WINNER_CLASSES)]
    zf = zero[zero["winner_class"].eq("BEAR_FAILURE")]
    for f in ["turnover", "volume_ratio", "distance_low60", "upper_space", "relative_strength_5d", "close_location", "pullback_volume_ratio", "mfe", "mae"]:
        if f not in q.columns:
            continue
        a = pd.to_numeric(zw[f], errors="coerce").dropna()
        b = pd.to_numeric(zf[f], errors="coerce").dropna()
        if a.empty and b.empty:
            continue
        features.append({"feature": f, "winner_n": len(a), "failure_n": len(b), "winner_median": a.median() if len(a) else np.nan, "failure_median": b.median() if len(b) else np.nan, "median_diff": a.median() - b.median() if len(a) and len(b) else np.nan})
    return q, audit, pd.DataFrame(features)


def _load_replay_dates(out: Path, membership: pd.DataFrame) -> tuple[list[pd.Timestamp], pd.DataFrame]:
    candidates = [
        "v72_search_formula_universe_attempt_audit.csv",
        "v72_search_formula_universe_truth_raw.csv",
        "v72_market_excess_benchmark_daily.csv",
        "v72_search_formula_universe_coverage_summary.csv",
    ]
    selected_dates: list[pd.Timestamp] = []
    selected_source = "MEMBERSHIP_FALLBACK"
    selected_column = "signal_date"
    for name in candidates:
        q = _read(out / name)
        if q.empty:
            continue
        dc = _pick(q, ["signal_date", "date", "replay_date", "trade_date", "기준일"])
        if not dc:
            continue
        vals = sorted({pd.Timestamp(v).normalize() for v in pd.to_datetime(q[dc], errors="coerce").dropna()})
        if vals:
            selected_dates = vals
            selected_source = name
            selected_column = dc
            break
    if not selected_dates and not membership.empty:
        selected_dates = sorted({pd.Timestamp(v).normalize() for v in pd.to_datetime(membership["signal_date"], errors="coerce").dropna()})
    rows = []
    for i, d in enumerate(selected_dates, 1):
        hit_rows = int(membership["signal_date"].eq(d).sum()) if not membership.empty else 0
        rows.append({
            "replay_index": i, "replay_date": d, "source_file": selected_source, "source_column": selected_column,
            "formula_hit_rows": hit_rows, "formula_hit_count": int(membership.loc[membership["signal_date"].eq(d), "formula"].nunique()) if hit_rows else 0,
            "zero_formula_hit_date": hit_rows == 0, "lock_status": "LOCKED",
        })
    return selected_dates, pd.DataFrame(rows)


def _window_list() -> list[int]:
    raw = os.environ.get("V7336618_STABILITY_WINDOWS", os.environ.get("V7336617_STABILITY_WINDOWS", "24,12,8,4"))
    vals = []
    for x in raw.split(","):
        try:
            v = int(x.strip())
            if v > 0 and v not in vals:
                vals.append(v)
        except Exception:
            pass
    return sorted(vals or [24, 12, 8, 4], reverse=True)


def _window_perf(g: pd.DataFrame, formula: str, requested: int, all_dates: list[pd.Timestamp]) -> dict[str, Any]:
    actual_dates = all_dates[-requested:] if len(all_dates) >= requested else list(all_dates)
    complete = len(actual_dates) >= requested
    z = g[g["signal_date"].isin(actual_dates)].copy() if actual_dates else g.iloc[0:0].copy()
    r = pd.to_numeric(z.get("ret3"), errors="coerce")
    ex = pd.to_numeric(z.get("market_excess3"), errors="coerce")
    date_mean = z.assign(_r=r).groupby("signal_date")["_r"].mean() if len(z) else pd.Series(dtype=float)
    if not actual_dates:
        status, reason = "DATE_LOCK_EMPTY", "No replay dates were resolved"
    elif not complete:
        status, reason = "WINDOW_INCOMPLETE", f"Only {len(actual_dates)} replay dates are available for requested {requested}"
    elif z.empty:
        status, reason = "NO_HITS_IN_WINDOW", "Formula has zero authoritative PRE hits in the locked replay window"
    elif r.notna().sum() == 0:
        status, reason = "RETURN_MISSING", "Authoritative next3_close_ret is missing for all formula hits"
    else:
        status, reason = "OK", ""
    return {
        "formula": formula, "window_requested": requested, "window_actual_replay_dates": len(actual_dates), "window_complete": complete,
        "window_start": min(actual_dates) if actual_dates else pd.NaT, "window_end": max(actual_dates) if actual_dates else pd.NaT,
        "window_status": status, "window_reason": reason,
        "n": len(z), "return_observed_n": int(r.notna().sum()), "stocks": z["code"].nunique() if len(z) else 0, "signal_days": z["signal_date"].nunique() if len(z) else 0,
        "d3_mean": r.mean(), "d3_median": r.median(), "d3_trim10": _trim_mean(r), "d3_top2_removed": _top_removed(r, 2), "d3_top5_removed": _top_removed(r, 5),
        "net20_mean": (r - 0.20).mean(), "net50_mean": (r - 0.50).mean(), "profit_factor": _profit_factor(r),
        "excess3_coverage_pct": float(ex.notna().mean() * 100) if len(z) else np.nan, "excess3_mean": ex.mean(), "excess3_median": ex.median(), "excess3_trim10": _trim_mean(ex), "excess3_top5_removed": _top_removed(ex, 5),
        "plus3_first_rate": float(z["plus3_first"].map(_bool).mean() * 100) if len(z) else np.nan, "stop_first_rate": float(z["stop_first"].map(_bool).mean() * 100) if len(z) else np.nan,
        "positive_signal_day_rate": float((date_mean > 0).mean() * 100) if len(date_mean) else np.nan,
        "source_file": _best_text(z.get("formula_source_file", pd.Series(dtype=str)), _best_text(g.get("formula_source_file", pd.Series(dtype=str)), "")),
        "ret3_source_column": _best_text(z.get("ret3_source_column", pd.Series(dtype=str)), _best_text(g.get("ret3_source_column", pd.Series(dtype=str)), "")),
    }


def _stability(membership: pd.DataFrame, replay_dates: list[pd.Timestamp] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if membership.empty:
        return pd.DataFrame(), pd.DataFrame()
    dates = list(replay_dates or [])
    if not dates:
        dates = sorted({pd.Timestamp(x).normalize() for x in pd.to_datetime(membership["signal_date"], errors="coerce").dropna()})
    windows = _window_list()
    matrix_rows = []
    for formula, g in membership.groupby("formula"):
        for w in windows:
            matrix_rows.append(_window_perf(g, str(formula), w, dates))
    matrix = pd.DataFrame(matrix_rows)
    if matrix.empty:
        return matrix, pd.DataFrame()

    maxw = max(windows)
    recent12 = 12 if 12 in windows else sorted(windows, reverse=True)[min(1, len(windows)-1)]
    recent8 = 8 if 8 in windows else min(windows)
    recent4 = 4 if 4 in windows else min(windows)
    policy_rows = []
    for formula, g in membership.groupby("formula"):
        rows = matrix[matrix["formula"].eq(formula)].set_index("window_requested")
        full = rows.loc[maxw] if maxw in rows.index else pd.Series(dtype=object)
        w12 = rows.loc[recent12] if recent12 in rows.index else pd.Series(dtype=object)
        w8 = rows.loc[recent8] if recent8 in rows.index else pd.Series(dtype=object)
        w4 = rows.loc[recent4] if recent4 in rows.index else pd.Series(dtype=object)
        cut = max(1, int(len(dates) * 2 / 3))
        train_dates, oos_dates = dates[:cut], dates[cut:]
        tr = g[g["signal_date"].isin(train_dates)]
        oo = g[g["signal_date"].isin(oos_dates)]
        tr_r = pd.to_numeric(tr["ret3"], errors="coerce")
        oo_r = pd.to_numeric(oo["ret3"], errors="coerce")

        full_ok = str(full.get("window_status", "")) == "OK"
        sample_ok = full_ok and _num(full.get("n"), 0) >= MIN_ROWS and _num(full.get("signal_days"), 0) >= MIN_SIGNAL_DAYS
        robust_full = sample_ok and all(_num(full.get(c), -999) > 0 for c in ["d3_median", "d3_trim10", "d3_top5_removed", "net50_mean"])
        excess_required = _num(full.get("excess3_coverage_pct"), 0) >= 70
        excess_ok = (not excess_required) or all(_num(full.get(c), -999) > 0 for c in ["excess3_median", "excess3_trim10", "excess3_top5_removed"])
        recent12_eval = str(w12.get("window_status", "")) == "OK" and _num(w12.get("n"), 0) >= 5 and _num(w12.get("signal_days"), 0) >= 3
        recent8_eval = str(w8.get("window_status", "")) == "OK" and _num(w8.get("n"), 0) >= 5 and _num(w8.get("signal_days"), 0) >= 3
        recent12_ok = recent12_eval and all(_num(w12.get(c), -999) > 0 for c in ["d3_median", "d3_trim10", "net50_mean"])
        recent8_ok = recent8_eval and all(_num(w8.get(c), -999) > 0 for c in ["d3_median", "d3_trim10", "net50_mean"])
        wf_eval = len(oo) >= 5 and oo["signal_date"].nunique() >= 3
        wf_ok = wf_eval and _num(oo_r.median(), -999) > 0 and _num((oo_r - 0.5).mean(), -999) > 0
        broad_edge = sample_ok and _num(full.get("net20_mean"), -999) > 0 and _num(full.get("d3_top5_removed"), -999) > 0
        if not full_ok:
            status = "WINDOW_NOT_EVALUABLE"
        elif not sample_ok:
            status = "INSUFFICIENT_SAMPLE"
        elif not recent8_eval or not wf_eval:
            status = "WINDOW_NOT_EVALUABLE"
        elif robust_full and excess_ok and recent12_ok and recent8_ok and wf_ok:
            status = "STABLE_POLICY_CANDIDATE"
        elif robust_full and (not recent8_ok or not wf_ok):
            status = "REGIME_SENSITIVE"
        elif broad_edge and (_num(full.get("d3_median"), 0) <= 0 or _num(full.get("d3_trim10"), 0) <= 0):
            status = "BROAD_SCANNER_ONLY"
        else:
            status = "RESEARCH_ONLY"
        policy_rows.append({
            "formula": formula, "policy_status": status, "base_window": maxw, "available_replay_dates": len(dates),
            "full_window_status": full.get("window_status", ""), "full_n": full.get("n", np.nan), "full_signal_days": full.get("signal_days", np.nan), "full_d3_mean": full.get("d3_mean", np.nan), "full_d3_median": full.get("d3_median", np.nan), "full_d3_trim10": full.get("d3_trim10", np.nan),
            "full_d3_top2_removed": full.get("d3_top2_removed", np.nan), "full_d3_top5_removed": full.get("d3_top5_removed", np.nan), "full_net50_mean": full.get("net50_mean", np.nan), "full_excess3_median": full.get("excess3_median", np.nan),
            "recent12_status": w12.get("window_status", ""), "recent12_n": w12.get("n", np.nan), "recent12_days": w12.get("signal_days", np.nan), "recent12_median": w12.get("d3_median", np.nan), "recent12_trim10": w12.get("d3_trim10", np.nan), "recent12_net50": w12.get("net50_mean", np.nan),
            "recent8_status": w8.get("window_status", ""), "recent8_n": w8.get("n", np.nan), "recent8_days": w8.get("signal_days", np.nan), "recent8_median": w8.get("d3_median", np.nan), "recent8_trim10": w8.get("d3_trim10", np.nan), "recent8_net50": w8.get("net50_mean", np.nan),
            "recent4_status": w4.get("window_status", ""), "recent4_n": w4.get("n", np.nan), "recent4_days": w4.get("signal_days", np.nan), "recent4_median": w4.get("d3_median", np.nan),
            "wf_train_n": len(tr), "wf_train_days": tr["signal_date"].nunique(), "wf_train_median": tr_r.median(), "wf_train_net50": (tr_r - 0.5).mean(),
            "wf_oos_n": len(oo), "wf_oos_days": oo["signal_date"].nunique(), "wf_oos_median": oo_r.median(), "wf_oos_net50": (oo_r - 0.5).mean(),
            "sample_ok": sample_ok, "robust_full": robust_full, "excess_required": excess_required, "excess_ok": excess_ok, "recent12_evaluable": recent12_eval, "recent8_evaluable": recent8_eval, "recent12_ok": recent12_ok, "recent8_ok": recent8_ok, "walk_forward_evaluable": wf_eval, "walk_forward_ok": wf_ok,
            "source_file": full.get("source_file", ""), "ret3_source_column": full.get("ret3_source_column", ""),
        })
    policy = pd.DataFrame(policy_rows)
    order = {"STABLE_POLICY_CANDIDATE": 0, "REGIME_SENSITIVE": 1, "BROAD_SCANNER_ONLY": 2, "RESEARCH_ONLY": 3, "WINDOW_NOT_EVALUABLE": 4, "INSUFFICIENT_SAMPLE": 5}
    if not policy.empty:
        policy["_order"] = policy["policy_status"].map(order).fillna(9)
        policy = policy.sort_values(["_order", "full_d3_top5_removed", "full_n"], ascending=[True, False, False]).drop(columns="_order")
    return matrix, policy


def _stability_reconciliation(out: Path, matrix: pd.DataFrame) -> pd.DataFrame:
    ref = _read(out / "v72_search_formula_universe_formula_performance.csv")
    if matrix.empty:
        return pd.DataFrame([{"status": "NOT_EVALUABLE_MATRIX_EMPTY", "metric": "ALL"}])
    maxw = int(pd.to_numeric(matrix["window_requested"], errors="coerce").max())
    full = matrix[matrix["window_requested"].eq(maxw)].copy()
    if ref.empty:
        return pd.DataFrame([{"status": "NOT_EVALUABLE_REFERENCE_MISSING", "metric": "ALL", "matrix_formulas": full["formula"].nunique()}])
    if "regime" in ref.columns:
        ref = ref[ref["regime"].fillna("ALL").astype(str).str.upper().eq("ALL")].copy()
    fc = _pick(ref, ["formula", "label"])
    if not fc:
        return pd.DataFrame([{"status": "NOT_EVALUABLE_REFERENCE_SCHEMA", "metric": "ALL"}])
    ref["formula"] = ref[fc].astype(str)
    metric_map = {
        "n": "n", "signal_days": "signal_days", "d3_mean": "d3_mean", "d3_median": "d3_median", "d3_trim10": "d3_trim10", "d3_top2_removed": "d3_ex_top2",
    }
    rows = []
    joined = full.merge(ref, on="formula", how="left", suffixes=("_matrix", "_reference"))
    for _, r in joined.iterrows():
        for mcol, rcol in metric_map.items():
            a = _num(r.get(f"{mcol}_matrix" if f"{mcol}_matrix" in joined.columns else mcol))
            b = _num(r.get(f"{rcol}_reference" if f"{rcol}_reference" in joined.columns else rcol))
            evaluable = math.isfinite(a) and math.isfinite(b)
            tol = 0.0 if mcol in {"n", "signal_days"} else 1e-8
            match = evaluable and abs(a - b) <= tol
            rows.append({"formula": r["formula"], "metric": mcol, "matrix_value": a, "reference_value": b, "abs_diff": abs(a-b) if evaluable else np.nan, "tolerance": tol, "evaluable": evaluable, "match": match, "status": "MATCH" if match else ("REFERENCE_MISSING" if not math.isfinite(b) else "MISMATCH")})
    return pd.DataFrame(rows)


def _duplicate_call_audit(out: Path) -> pd.DataFrame:
    raw = _read(out / "v72_search_formula_universe_truth_raw.csv", dtype={"code": str})
    if raw.empty:
        return pd.DataFrame([{"status": "NOT_EVALUABLE_SOURCE_MISSING", "duplicate_key": ""}])
    dc = _pick(raw, ["signal_date", "date"]); cc = _pick(raw, ["code", "Code"])
    if not dc or not cc:
        return pd.DataFrame([{"status": "NOT_EVALUABLE_SCHEMA", "duplicate_key": ""}])
    q = raw.copy(); q["signal_date"] = pd.to_datetime(q[dc], errors="coerce").dt.normalize(); q["code"] = q[cc].map(_norm_code)
    compare = [c for c in ["formula_truth_bitmap", "formula_post_truth_bitmap", "formula_truth_registry_sha256", "formula_post_truth_registry_sha256", "analyze_returned"] if c in q.columns]
    rows=[]
    for (d, code), g in q.groupby(["signal_date", "code"], dropna=False):
        if len(g) <= 1: continue
        conflicts = [c for c in compare if g[c].fillna("").astype(str).nunique() > 1]
        rows.append({"signal_date": d, "code": code, "duplicate_key": f"{d}|{code}", "invocations": len(g), "compared_columns": "|".join(compare), "conflict_columns": "|".join(conflicts), "status": "INVALID_DUPLICATE_CALL" if conflicts else "DEDUPED_VALID"})
    if not rows:
        rows.append({"duplicate_key": "", "invocations": 0, "compared_columns": "|".join(compare), "conflict_columns": "", "status": "NO_DUPLICATE_CALL"})
    return pd.DataFrame(rows)


def _locked_policy_failure_audit(out: Path) -> pd.DataFrame:
    specs = [
        ("P1_D1_EXIT_HOLD", "v72_d1_exit_hold_policy_robustness.csv"),
        ("PATTERN_AI_LOCK", "v72_pattern_ai_cross_locked_oos_summary.csv"),
    ]
    rows=[]
    for label, name in specs:
        q=_read(out/name)
        if q.empty:
            rows.append({"policy_family":label,"source_file":name,"status":"NOT_EVALUABLE_SOURCE_MISSING"}); continue
        split_col=_pick(q,["split","phase","dataset","sample_role"])
        o=q[q[split_col].astype(str).str.upper().str.contains("OOS")] if split_col else q
        if o.empty:
            rows.append({"policy_family":label,"source_file":name,"status":"NOT_EVALUABLE_OOS_MISSING"}); continue
        ncol=_pick(o,["n","count","rows"]); medcol=_pick(o,["d3_median","median","policy_median"]); trimcol=_pick(o,["d3_trim10","trim10","policy_trim10"]); topcol=_pick(o,["d3_ex_top2","d3_top2_removed","top2_removed","policy_ex_top2"])
        n=float(pd.to_numeric(o[ncol],errors="coerce").sum()) if ncol else len(o)
        med=float(pd.to_numeric(o[medcol],errors="coerce").median()) if medcol else np.nan
        trim=float(pd.to_numeric(o[trimcol],errors="coerce").median()) if trimcol else np.nan
        top=float(pd.to_numeric(o[topcol],errors="coerce").median()) if topcol else np.nan
        observed=[x for x in [med,trim,top] if math.isfinite(x)]
        failed=bool(n>0 and observed and all(x<0 for x in observed))
        rows.append({"policy_family":label,"source_file":name,"oos_n":n,"oos_median":med,"oos_trim10":trim,"oos_top_removed":top,"status":"LOCKED_FAILED_OOS" if failed else "RESEARCH_ONLY_NOT_FAILED"})
    return pd.DataFrame(rows)


def _pattern_only_sequence_audit(out: Path) -> tuple[pd.DataFrame,pd.DataFrame]:
    q=_read(out/"v73_sequence_context_catalyst_join.csv",dtype={"code":str})
    if q.empty: return pd.DataFrame(),pd.DataFrame()
    dc=_pick(q,["signal_date","date"]); cc=_pick(q,["code","Code"]); ac=_pick(q,["alignment_level","context_alignment","alignment"])
    if not dc or not cc or not ac: return pd.DataFrame(),pd.DataFrame()
    q=q.copy(); q["signal_date"]=pd.to_datetime(q[dc],errors="coerce").dt.normalize(); q["code"]=q[cc].map(_norm_code)
    q=q[q[ac].fillna("").astype(str).str.upper().eq("PATTERN_ONLY")].copy()
    if q.empty: return q,pd.DataFrame()
    rcol=_pick(q,["next3_close_ret","ret3","day3_ret"]); q["ret3"]=pd.to_numeric(q[rcol],errors="coerce") if rcol else np.nan
    q=q.sort_values(["signal_date","code"],kind="stable").drop_duplicates(["signal_date","code"],keep="last")
    q["outcome_group"]=np.where(q["ret3"].ge(3),"SUCCESS",np.where(q["ret3"].le(-3),"FAIL","NEUTRAL"))
    features=[]
    for f in ["sequence_stage_count","volume_ratio","pullback_volume_ratio","distance_low60","upper_space","relative_strength_5d","close_location"]:
        if f not in q.columns: continue
        a=pd.to_numeric(q.loc[q["outcome_group"].eq("SUCCESS"),f],errors="coerce").dropna(); b=pd.to_numeric(q.loc[q["outcome_group"].eq("FAIL"),f],errors="coerce").dropna()
        features.append({"feature":f,"success_n":len(a),"fail_n":len(b),"success_median":a.median() if len(a) else np.nan,"fail_median":b.median() if len(b) else np.nan,"median_diff":a.median()-b.median() if len(a) and len(b) else np.nan})
    return q,pd.DataFrame(features)

def _sector_breadth_join(master: pd.DataFrame, sector_history: pd.DataFrame) -> pd.DataFrame:
    if master.empty:
        return pd.DataFrame()
    q = sector_history.copy()
    if q.empty:
        return pd.DataFrame([{"status": "MISSING", "event_rows": len(master), "joined_rows": 0, "coverage_pct": 0.0, "reason": "v73_sector_breadth_history.csv has no causal rows"}])
    q["signal_date"] = pd.to_datetime(q["signal_date"], errors="coerce").dt.normalize()
    q["sector"] = q["sector"].fillna("UNKNOWN").astype(str)
    q["causal_mode"] = q["causal_mode"].fillna("").astype(str).str.upper()
    q = q[q["causal_mode"].isin(CAUSAL_MODES)].copy()
    q = q.drop_duplicates(["signal_date", "sector"], keep="last")
    joined = master.merge(q, on=["signal_date", "sector"], how="left", suffixes=("", "_sector"))
    joined["sector_breadth_available"] = pd.to_numeric(joined.get("sector_breadth_pct"), errors="coerce").notna()
    joined["sector_alignment"] = np.where(
        joined["sector_breadth_available"] & (pd.to_numeric(joined.get("sector_breadth_pct"), errors="coerce") >= 60) & (pd.to_numeric(joined.get("sector_turnover_change_pct"), errors="coerce") > 0),
        "BROAD_SECTOR_SUPPORT",
        np.where(joined["sector_breadth_available"], "NO_BROAD_SECTOR_SUPPORT", "UNKNOWN"),
    )
    return joined


def _minute_readiness(out: Path) -> pd.DataFrame:
    specs = [
        ("HAM", "v72_ham_intraday_feature_ledger.csv", ["code", "signal_date"]),
        ("OPEN0930", "v73_open0930_response_snapshot.csv", ["code", "signal_date"]),
        ("MINUTE_SCALE", "v73_minute_scale_in_ledger.csv", ["code", "signal_date"]),
    ]
    rows = []
    for label, name, keys in specs:
        q = _read(out / name, dtype={"code": str})
        available = len(q)
        code_col = _pick(q, ["code", "Code", "종목코드"]) if not q.empty else None
        date_col = _pick(q, ["signal_date", "date", "captured_at", "snapshot_at"]) if not q.empty else None
        unique_events = 0
        if code_col and date_col:
            z = q.copy()
            z["_code"] = z[code_col].map(_norm_code)
            z["_date"] = pd.to_datetime(z[date_col], errors="coerce").dt.normalize()
            unique_events = len(z[z["_code"].ne("") & z["_date"].notna()].drop_duplicates(["_date", "_code"]))
        rows.append({"source": label, "source_file": name, "rows": available, "unique_stock_days": unique_events, "status": "READY" if unique_events >= 30 else ("WARMUP" if unique_events else "MISSING"), "note": "09:30·14:40·15:03 확인형 분할매수는 분봉/시점 원장으로만 검증"})
    return pd.DataFrame(rows)


def _fmt(v: Any, digits: int = 2, signed: bool = True) -> str:
    x = _num(v)
    if not math.isfinite(x):
        return "N/A"
    return f"{x:+.{digits}f}%" if signed else f"{x:.{digits}f}%"


def _report(master: pd.DataFrame, membership: pd.DataFrame, matched: pd.DataFrame, zero: pd.DataFrame, policy: pd.DataFrame,
            sector_join: pd.DataFrame, minute: pd.DataFrame, source: str, stability_source: str, input_audit: pd.DataFrame,
            reconciliation: pd.DataFrame, replay_lock: pd.DataFrame, duplicate_audit: pd.DataFrame,
            locked_failure: pd.DataFrame, pattern_only: pd.DataFrame, status: str) -> str:
    bear = master[master["market_regime"].isin(BEAR_REGIMES)] if not master.empty else pd.DataFrame()
    counts = bear["winner_class"].value_counts().to_dict() if not bear.empty else {}
    formula_rows = len(membership); unique_rows = len(master); duplicate_votes_removed = max(0, formula_rows - unique_rows)
    selected_file = input_audit.iloc[0].get("selected_source_file", "") if not input_audit.empty else ""
    ret3_col = input_audit.iloc[0].get("ret3_source_column", "") if not input_audit.empty else ""
    recon_eval = reconciliation[reconciliation.get("evaluable", pd.Series(False, index=reconciliation.index)).fillna(False)] if not reconciliation.empty else pd.DataFrame()
    recon_mismatch = int((~recon_eval["match"].fillna(False)).sum()) if not recon_eval.empty and "match" in recon_eval.columns else 0
    duplicate_invalid = int(duplicate_audit["status"].eq("INVALID_DUPLICATE_CALL").sum()) if not duplicate_audit.empty and "status" in duplicate_audit.columns else 0
    lines = [
        HEADER,
        f"📌 {VERSION} · STABILITY_RECONCILIATION_REPLAY_LOCK_GUARD · RESEARCH_ONLY=True",
        "- 하락장 승자 회계는 신호일+종목 1표, 검색식 안정성은 전체 유니버스 PRE 원장만 사용합니다.",
        f"🧾 고유승자 입력: {source} | 검색식행 {formula_rows} · 고유사건 {unique_rows} · 중복투표 제거 {duplicate_votes_removed} · 하락장 고유사건 {len(bear)}",
        f"🔐 안정성 입력: {stability_source} | 파일 {selected_file or '-'} | D+3 원천 {ret3_col or '-'} | 재현일 {len(replay_lock)}개 | 상태 {status}",
        f"🧪 기존 formula performance 정합: 평가 {len(recon_eval)}개 metric · 불일치 {recon_mismatch} | COMBO 중복충돌 {duplicate_invalid}",
        "🐻 [하락장 고유사건 분류]",
        f"- 진짜승자 {counts.get('BEAR_TRUE_WINNER',0)} · 빠른승자 {counts.get('BEAR_FAST_WINNER',0)} · 큰승자 {counts.get('BEAR_BIG_WINNER',0)} · 상대방어 {counts.get('BEAR_RELATIVE_SURVIVOR',0)} · 시장베타 {counts.get('BEAR_FALSE_WINNER',0)} · 실패 {counts.get('BEAR_FAILURE',0)}",
        f"- 같은 날 고유 패자 매칭 {len(matched)}쌍",
        "🧩 [0_PATTERN 고유사건]",
    ]
    zshow = zero[zero["dimension"].isin(["OVERLAP_DEGREE_UNIQUE_EVENT", "BEAR_OVERLAP_DEGREE_UNIQUE_EVENT"])].copy() if not zero.empty else pd.DataFrame()
    if zshow.empty:
        lines.append("- 패턴 교차 원장이 없어 UNKNOWN으로 유지합니다.")
    else:
        for _, r in zshow.sort_values(["dimension", "label"]).head(8).iterrows():
            lines.append(f"- {r['dimension']} · {r['label']}: n{int(r['n'])}/일{int(r['signal_days'])} | D3 중앙 {_fmt(r['d3_median'])}·절사 {_fmt(r['d3_trim10'])}·상5제거 {_fmt(r['d3_top5_removed'])} | 승자 {_fmt(r['winner_rate'],1,False)}")
    lines.append("📊 [24·12·8·4주 검색식 안정성 · authoritative PRE]")
    pshow = policy.head(10) if not policy.empty else pd.DataFrame()
    if pshow.empty:
        lines.append("- 안정성 표본 없음")
    else:
        for _, r in pshow.iterrows():
            lines.append(f"- {r['formula']}: 24W n{int(_num(r.get('full_n'),0))}/일{int(_num(r.get('full_signal_days'),0))} 중앙 {_fmt(r.get('full_d3_median'))}·상5 {_fmt(r.get('full_d3_top5_removed'))} | 12W n{int(_num(r.get('recent12_n'),0))} {_fmt(r.get('recent12_median'))} | 8W n{int(_num(r.get('recent8_n'),0))} {_fmt(r.get('recent8_median'))} ({r.get('recent8_status','')}) | OOS {_fmt(r.get('wf_oos_median'))} | {r['policy_status']}")
    lines.append("🧬 [PATTERN_ONLY 완성 시퀀스]")
    if pattern_only.empty:
        lines.append("- 원장 없음 또는 표본 0")
    else:
        rr=pd.to_numeric(pattern_only.get("ret3"),errors="coerce")
        lines.append(f"- 고유사건 {len(pattern_only)}·날짜 {pattern_only['signal_date'].nunique()} | D3 평균 {_fmt(rr.mean())}·중앙 {_fmt(rr.median())}·절사 {_fmt(_trim_mean(rr))}·상2제거 {_fmt(_top_removed(rr,2))}")
    lines.append("🔒 [잠금정책 OOS 상태]")
    for _, r in locked_failure.iterrows():
        lines.append(f"- {r.get('policy_family')}: {r.get('status')} | OOS n{int(_num(r.get('oos_n'),0))} · 중앙 {_fmt(r.get('oos_median'))}")
    lines.append("🌐 [섹터 breadth·거래대금 역사 원장]")
    if sector_join.empty or "sector_breadth_available" not in sector_join.columns:
        lines.append("- 인과 섹터 역사 원장이 없어 DATA_WARMUP입니다.")
    else:
        cov = float(sector_join["sector_breadth_available"].mean() * 100) if len(sector_join) else 0.0
        lines.append(f"- 고유사건 결합률 {cov:.1f}% | 70% 미만은 정책 승격 근거로 사용 금지")
    lines.append("🪜 [분봉 확인형 분할매수 준비]")
    for _, r in minute.iterrows():
        lines.append(f"- {r['source']}: {int(r['unique_stock_days'])} 종목일 · {r['status']}")
    lines += [
        "🔒 [검증 운용]",
        "- 24개 replay 날짜를 파일로 잠그고 같은 날짜 집합에서 24·12·8·4주와 앞 2/3→뒤 1/3 Walk-forward를 계산합니다.",
        "- 안정성 24W는 v72_search_formula_universe_formula_performance.csv와 n·신호일·평균·중앙·절사·상2제거가 일치해야 합니다.",
        "- 8W에 점등이 없으면 NaN만 표시하지 않고 NO_HITS_IN_WINDOW와 해당 n=0을 함께 기록합니다.",
        "- 중복 COMBO 호출은 결과가 같으면 DEDUPED_VALID, 하나라도 다르면 INVALID_DUPLICATE_CALL입니다.",
        "- 실제 섹터 breadth·지정학 인과원장·분봉 원장이 없으면 UNKNOWN/WARMUP으로 유지합니다.",
        "- Actions CSV: " + " · ".join([UNIQUE_MASTER_FILE, FORMULA_MEMBERSHIP_FILE, UNIQUE_MATCHED_FILE, UNIQUE_COMMONALITY_FILE, ZERO_PATTERN_FILE, ZERO_PATTERN_FEATURE_FILE, STABILITY_MATRIX_FILE, STABILITY_POLICY_FILE, STABILITY_INPUT_AUDIT_FILE, STABILITY_RECON_FILE, REPLAY_LOCK_FILE, DUPLICATE_CALL_AUDIT_FILE, LOCKED_POLICY_FAILURE_FILE, PATTERN_ONLY_EVENT_FILE, PATTERN_ONLY_COMMONALITY_FILE, OFFICIAL_ARCHIVE_FILE, SECTOR_BREADTH_FILE, SECTOR_BREADTH_JOIN_FILE, MINUTE_SCALE_READINESS_FILE, READINESS_FILE]),
    ]
    return "\n".join(lines)


def run_backtest(eval_df: pd.DataFrame | None = None, output_dir: str | Path = "reports", base_report: str = "") -> tuple[str, dict[str, pd.DataFrame]]:
    out = _out(output_dir)
    generated_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    official, sector_history = _ensure_templates(out)

    # A. Enriched geo rows are used only for unique winner/event diagnostics.
    formula_rows, source = _prepare_formula_rows(out, eval_df)
    master, membership = _unique_master(formula_rows)
    matched = _matched_controls(master)
    commonality = _commonality(master)
    pattern = _load_pattern_membership(out)
    master_with_pattern, zero_audit, zero_features = _zero_pattern(master, pattern)

    # B. Formula stability is isolated to the authoritative PRE full-universe ledger.
    stability_rows, stability_source, input_audit = _prepare_stability_rows(out, eval_df)
    replay_dates, replay_lock = _load_replay_dates(out, stability_rows)
    matrix, policy = _stability(stability_rows, replay_dates)
    reconciliation = _stability_reconciliation(out, matrix)
    duplicate_audit = _duplicate_call_audit(out)
    locked_failure = _locked_policy_failure_audit(out)
    pattern_only, pattern_only_common = _pattern_only_sequence_audit(out)

    # A formula cannot keep a candidate label when its 24W reference reconciliation failed or was unavailable.
    if not policy.empty:
        rec_ok = {}
        if not reconciliation.empty and "formula" in reconciliation.columns:
            for f, g in reconciliation.groupby("formula"):
                ev = g[g.get("evaluable", pd.Series(False,index=g.index)).fillna(False)]
                rec_ok[str(f)] = bool(len(ev) > 0 and ev.get("match", pd.Series(False,index=ev.index)).fillna(False).all())
        policy["reconciliation_ok"] = policy["formula"].astype(str).map(rec_ok).fillna(False)
        policy["policy_status_before_reconciliation"] = policy["policy_status"]
        policy.loc[~policy["reconciliation_ok"], "policy_status"] = "RECONCILIATION_BLOCKED"

    sector_join = _sector_breadth_join(master_with_pattern, sector_history)
    minute = _minute_readiness(out)
    snapshot = _sha_frame(master_with_pattern, ["signal_date", "code", "winner_class", "formula_list", "geo_event_id"])
    tables = [master_with_pattern, membership, matched, commonality, zero_audit, zero_features, matrix, policy, input_audit, reconciliation, replay_lock, duplicate_audit, locked_failure, pattern_only, pattern_only_common, official, sector_history, sector_join, minute]
    for q in tables:
        if isinstance(q, pd.DataFrame):
            q["version"] = VERSION; q["snapshot_id"] = snapshot; q["generated_at"] = generated_at
            q["research_only"] = True; q["live_logic_changed"] = False; q["real_order_changed"] = False

    unique_days = master_with_pattern["signal_date"].nunique() if not master_with_pattern.empty else 0
    replay_day_count = len(replay_dates)
    bear_unique = int(master_with_pattern["market_regime"].isin(BEAR_REGIMES).sum()) if not master_with_pattern.empty else 0
    consistency_fail = int(master_with_pattern[["return_consistency_fail", "winner_class_consistency_fail", "market_regime_consistency_fail"]].any(axis=1).sum()) if not master_with_pattern.empty else 0
    stable_count = int(policy["policy_status"].eq("STABLE_POLICY_CANDIDATE").sum()) if not policy.empty else 0
    sector_cov = float(sector_join["sector_breadth_available"].mean() * 100) if not sector_join.empty and "sector_breadth_available" in sector_join.columns else 0.0
    minute_ready = int(minute["status"].eq("READY").sum()) if not minute.empty else 0
    source_valid = not input_audit.empty and bool(input_audit.iloc[0].get("authoritative_source_selected", False)) and str(input_audit.iloc[0].get("ret3_source_column", "")) == "next3_close_ret"
    recon_eval = reconciliation[reconciliation.get("evaluable", pd.Series(False,index=reconciliation.index)).fillna(False)] if not reconciliation.empty else pd.DataFrame()
    recon_missing = reconciliation.empty or ("status" in reconciliation.columns and reconciliation["status"].astype(str).str.startswith("NOT_EVALUABLE").all())
    recon_mismatch = int((~recon_eval["match"].fillna(False)).sum()) if not recon_eval.empty and "match" in recon_eval.columns else 0
    duplicate_invalid = int(duplicate_audit["status"].eq("INVALID_DUPLICATE_CALL").sum()) if not duplicate_audit.empty and "status" in duplicate_audit.columns else 0
    if formula_rows.empty:
        status = "NO_SIGNAL_INPUT"
    elif not source_valid:
        status = "INVALID_STABILITY_INPUT_SOURCE"
    elif consistency_fail:
        status = "INVALID_EVENT_CONSISTENCY"
    elif duplicate_invalid:
        status = "INVALID_DUPLICATE_CALL"
    elif recon_mismatch:
        status = "INVALID_STABILITY_RECONCILIATION"
    elif replay_day_count < 24:
        status = "VALID_SHADOW_WINDOW_WARMUP"
    elif recon_missing:
        status = "VALID_SHADOW_RECONCILIATION_WARMUP"
    elif official.empty or sector_cov < 70 or minute_ready == 0:
        status = "VALID_SHADOW_DATA_WARMUP"
    else:
        status = "VALID_SHADOW"
    policy_ready = bool(status == "VALID_SHADOW" and stable_count > 0)
    readiness = pd.DataFrame([{
        "version": VERSION, "status": status, "unique_event_source": source, "stability_source": stability_source,
        "geo_formula_rows": len(formula_rows), "stability_formula_rows": len(stability_rows), "unique_event_rows": len(master_with_pattern),
        "duplicate_formula_votes_removed": max(0, len(formula_rows) - len(master_with_pattern)), "replay_date_count": replay_day_count, "unique_signal_days": unique_days, "bear_unique_event_rows": bear_unique,
        "matched_control_rows": len(matched), "event_consistency_fail_rows": consistency_fail, "authoritative_stability_source": source_valid,
        "stability_reconciliation_evaluable_metrics": len(recon_eval), "stability_reconciliation_mismatch_metrics": recon_mismatch,
        "duplicate_combo_conflict_rows": duplicate_invalid, "official_archive_rows": len(official), "sector_breadth_coverage_pct": sector_cov,
        "minute_ready_source_count": minute_ready, "stable_policy_candidate_count": stable_count, "locked_failed_oos_policy_count": int(locked_failure["status"].eq("LOCKED_FAILED_OOS").sum()) if not locked_failure.empty else 0,
        "pattern_only_event_rows": len(pattern_only), "policy_ready": policy_ready, "recommended_backtest_weeks": 24, "stability_windows": ",".join(map(str, _window_list())),
        "snapshot_id": snapshot, "generated_at": generated_at, "research_only": True, "live_logic_changed": False, "real_order_changed": False,
    }])

    file_map = {
        UNIQUE_MASTER_FILE: master_with_pattern, FORMULA_MEMBERSHIP_FILE: membership, UNIQUE_MATCHED_FILE: matched, UNIQUE_COMMONALITY_FILE: commonality,
        ZERO_PATTERN_FILE: zero_audit, ZERO_PATTERN_FEATURE_FILE: zero_features, STABILITY_MATRIX_FILE: matrix, STABILITY_POLICY_FILE: policy,
        STABILITY_INPUT_AUDIT_FILE: input_audit, STABILITY_RECON_FILE: reconciliation, REPLAY_LOCK_FILE: replay_lock,
        DUPLICATE_CALL_AUDIT_FILE: duplicate_audit, LOCKED_POLICY_FAILURE_FILE: locked_failure, PATTERN_ONLY_EVENT_FILE: pattern_only, PATTERN_ONLY_COMMONALITY_FILE: pattern_only_common,
        OFFICIAL_ARCHIVE_FILE: official, SECTOR_BREADTH_FILE: sector_history, SECTOR_BREADTH_JOIN_FILE: sector_join, MINUTE_SCALE_READINESS_FILE: minute, READINESS_FILE: readiness,
    }
    for name, q in file_map.items(): _write(out / name, q)
    block = _report(master_with_pattern, membership, matched, zero_audit, policy, sector_join, minute, source, stability_source, input_audit, reconciliation, replay_lock, duplicate_audit, locked_failure, pattern_only, status)
    (out / REPORT_FILE).write_text(block, encoding="utf-8")
    return _insert(base_report, block), {
        "unique_master": master_with_pattern, "formula_membership": membership, "matched_control": matched, "commonality": commonality,
        "zero_pattern": zero_audit, "zero_pattern_features": zero_features, "stability_matrix": matrix, "stability_policy": policy,
        "stability_input_audit": input_audit, "stability_reconciliation": reconciliation, "replay_date_lock": replay_lock,
        "duplicate_call_audit": duplicate_audit, "locked_policy_failure": locked_failure, "pattern_only_events": pattern_only, "pattern_only_commonality": pattern_only_common,
        "official_archive": official, "sector_breadth": sector_history, "sector_join": sector_join, "minute_readiness": minute, "readiness": readiness,
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
    report, _ = run_backtest(output_dir=os.environ.get("V1080_BACKTEST_OUTPUT_DIR", "reports"))
    print(report)
