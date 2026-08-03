from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6"
FACTOR_NAME = "FAMILIAR_NAME_PATTERN_RESEARCH"
RESEARCH_ONLY = True
REPORT_HEADER = "🤝 [절친 종목·절친 패턴 순위 연구 · RESEARCH_ONLY]"
SHADOW_HEADER = "🤝 [15:03 절친 종목·패턴 SHADOW 브리핑 · 순위영향 없음]"

POLICY_FILE = "v72_familiar_policy_lock.json"
POLICY_AUDIT = "v72_familiar_policy_lock_audit.csv"
ROW_AUDIT = "v72_familiar_signal_causal_audit.csv"
NAME_PROFILE = "v72_familiar_name_profile.csv"
PATTERN_PROFILE = "v72_familiar_pattern_profile.csv"
MODEL_SUMMARY = "v72_familiar_model_abc_summary.csv"
OOS_SUMMARY = "v72_familiar_locked_oos_summary.csv"
MATCHED_PAIRS = "v72_familiar_matched_control_pairs.csv"
MATCHED_SUMMARY = "v72_familiar_matched_control_summary.csv"
BOOTSTRAP_SUMMARY = "v72_familiar_cluster_bootstrap.csv"
CONCENTRATION_SUMMARY = "v72_familiar_concentration_summary.csv"
EXECUTION_SUMMARY = "v72_familiar_execution_pnl_summary.csv"
REPEAT_LEDGER = "v72_familiar_repeat_appearance_ledger.csv"
SHADOW_LEDGER = "v72_familiar_1503_shadow_brief.csv"
DATA_AUDIT = "v72_familiar_data_availability_audit.csv"
PATTERN_MARKET_SUMMARY = "v72_familiar_pattern_market_summary.csv"
PATTERN_SECTOR_SUMMARY = "v72_familiar_pattern_sector_summary.csv"
PATTERN_TRAIN_OOS_SUMMARY = "v72_familiar_pattern_train_oos_summary.csv"
REPORT_BLOCK_FILE = "v72_familiar_report_block.txt"

PATTERNS = [
    "FIRST_LONG_BULL_PULLBACK",
    "VOLUME_COMPRESSION_RECLAIM",
    "BREAKOUT_PULLBACK",
    "INTRADAY_HIGH_BREAK_CLOSE_RECOVERY",
    "THEME_LEADER_FIRST_RESTART",
    "FOREIGN_INSTITUTION_PULLBACK",
    "TURNOVER_EXPLOSION_DRY_PULLBACK",
    "MARKET_CRASH_RELATIVE_STRENGTH",
    "PRIME_RECOVERY",
    "LCZ_RESTART_THEME",
]

PATTERN_KO = {
    "FIRST_LONG_BULL_PULLBACK": "첫 장대양봉 후 첫 눌림",
    "VOLUME_COMPRESSION_RECLAIM": "거래량 응축 후 기준선 재지지",
    "BREAKOUT_PULLBACK": "전고점 돌파 후 눌림",
    "INTRADAY_HIGH_BREAK_CLOSE_RECOVERY": "장중 고가 돌파 후 종가 회복",
    "THEME_LEADER_FIRST_RESTART": "테마 대장주의 첫 재양봉",
    "FOREIGN_INSTITUTION_PULLBACK": "외국인·기관 동시 매수 후 눌림",
    "TURNOVER_EXPLOSION_DRY_PULLBACK": "거래대금 폭발 후 정상 눌림",
    "MARKET_CRASH_RELATIVE_STRENGTH": "시장 급락 후 상대강도 유지",
    "PRIME_RECOVERY": "PRIME-RECOVERY",
    "LCZ_RESTART_THEME": "LCZ-RESTART-THEME",
}


def _outdir(output_dir: str | Path = "reports") -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _code(v) -> str:
    d = "".join(ch for ch in str(v or "") if ch.isdigit())
    return d[-6:].zfill(6) if d else ""


def _num(v, default=np.nan) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _series(df: pd.DataFrame, names: Sequence[str], default=np.nan) -> pd.Series:
    for c in names:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce")
    return pd.Series(default, index=df.index, dtype=float)


def _str_series(df: pd.DataFrame, names: Sequence[str], default="") -> pd.Series:
    for c in names:
        if c in df.columns:
            return df[c].fillna(default).astype(str)
    return pd.Series(default, index=df.index, dtype=str)


def _first_existing(row: pd.Series | dict, names: Sequence[str], default=""):
    for c in names:
        try:
            v = row.get(c, default)
            if pd.notna(v) and str(v).strip() not in ("", "nan", "None"):
                return v
        except Exception:
            pass
    return default


def _trim_mean(s: pd.Series, p=0.1) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    if x.empty:
        return np.nan
    k = int(len(x) * p)
    if k and len(x) > 2 * k:
        x = x.iloc[k:-k]
    return float(x.mean())


def _clip(v, lo, hi):
    try:
        return max(lo, min(hi, float(v)))
    except Exception:
        return lo


def _int(v, default=0) -> int:
    try:
        x=float(v)
        return int(x) if math.isfinite(x) else int(default)
    except Exception:
        return int(default)


def _fmt(v, digits=2, sign=True, suffix="") -> str:
    x=_num(v)
    if not math.isfinite(x):
        return "N/A"
    spec=("+" if sign else "")+f".{digits}f"
    return format(x,spec)+suffix


def _safe_rate(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return float(x.mean() * 100.0) if not x.empty else np.nan


def _normalise_eval(eval_df: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    if eval_df is None or eval_df.empty:
        return pd.DataFrame()
    x = eval_df.copy()
    x["signal_date"] = pd.to_datetime(_str_series(x, ["signal_date", "신호일", "date"]), errors="coerce").dt.normalize()
    x["code"] = _str_series(x, ["code", "Code", "종목코드"]).map(_code)
    x["name"] = _str_series(x, ["name", "Name", "종목명"])
    x["eval_status"] = _str_series(x, ["eval_status"], "OK")
    x = x[x["signal_date"].notna() & x["code"].ne("")].copy()
    if "eval_status" in x.columns:
        x = x[x["eval_status"].astype(str).eq("OK")].copy()
    if x.empty:
        return x
    x["ret1"] = _series(x, ["next1_close_ret", "NEXT_DAY_RET", "day1_ret", "ret1"])
    x["ret3"] = _series(x, ["next3_close_ret", "D3_RET", "day3_ret", "ret3"])
    x["ret5"] = _series(x, ["next5_close_ret", "D5_RET", "day5_ret", "ret5"])
    x["plus3"] = _series(x, ["plus3_first", "plus3_hit", "+3먼저"], 0).fillna(0).clip(0, 1)
    x["stop_first"] = _series(x, ["stop_first", "minus3_first", "손절먼저"], 0).fillna(0).clip(0, 1)
    x["plus5"] = _series(x, ["plus5_first", "plus5_hit", "+5먼저"], 0).fillna(0).clip(0, 1)
    x["mfe"] = _series(x, ["max_up_5d", "MFE_5D", "mfe"])
    x["mae"] = _series(x, ["max_down_5d", "MAE_5D", "mae"])
    x["rank"] = _series(x, ["rank", "순위"], 999).fillna(999)
    x["base_raw"] = _series(x, ["n_score", "N점수", "safe_score", "안전점수"], np.nan)
    for c in ["source", "strategy", "pattern", "search_pattern_primary", "search_pattern_matches", "search_pattern_tags", "structure_pattern", "final_decision", "phase", "volume_state"]:
        if c not in x.columns:
            x[c] = ""
        x[c] = x[c].fillna("").astype(str)
    x["all_text"] = x[["source", "strategy", "pattern", "search_pattern_primary", "search_pattern_matches", "search_pattern_tags", "structure_pattern", "final_decision", "phase", "volume_state"]].agg(" | ".join, axis=1)
    x["theme"] = _str_series(x, ["theme", "Theme", "테마", "sector", "Sector", "섹터", "sector_name"], "")
    x["market_regime"] = _str_series(x, ["market_regime", "global_regime", "market_state", "시장국면"], "UNKNOWN")
    x["market5"] = _series(x, ["market_5d_ret", "market5", "시장5일"], np.nan)
    x["distance_low60"] = _series(x, ["distance_from_60d_low_pct", "low60_distance_pct", "60일저점이격"], np.nan)
    x["upper_space"] = _series(x, ["upper_resistance_distance_pct", "upper_space_pct", "상단저항거리", "upper_recovery_gap_pct"], np.nan)
    x["pullback_value_ratio"] = _series(x, ["PULLBACK_VALUE_RATIO_TO_HAM", "pullback_value_ratio_to_ham", "pullback_value_ratio"], np.nan)
    x["close1503_from_high"] = _series(x, ["CLOSE1503_FROM_DAY_HIGH", "close1503_from_day_high"], np.nan)
    x["day_return"] = _series(x, ["day_return", "ChangeRate", "당일등락률", "today_ret"], np.nan)
    x["day_amount"] = _series(x, ["day_amount", "Amount", "거래대금", "amount"], np.nan)
    x["market_cap"] = _series(x, ["market_cap", "Marcap", "시가총액", "marcap"], np.nan)
    x["late_wave"] = _str_series(x, ["failure_reasons", "reason_tags", "attribution_tags"], "").str.contains("LATE_WAVE", case=False, na=False).astype(int)
    x["base_score"] = 0.0
    for d, idx in x.groupby("signal_date").groups.items():
        z = x.loc[idx, "base_raw"]
        if z.notna().sum() >= 2 and float(z.max() - z.min()) > 1e-9:
            x.loc[idx, "base_score"] = ((z - z.min()) / (z.max() - z.min()) * 100.0).fillna(0).values
        else:
            r = x.loc[idx, "rank"].replace(0, np.nan).fillna(999)
            n = max(1, len(r))
            x.loc[idx, "base_score"] = (100.0 - (r - 1) / max(1, n - 1) * 100.0).clip(0, 100).values
    x = _join_context(x, output_dir)
    x = _tag_patterns(x)
    x = _add_theme_day_features(x)
    return x.sort_values(["signal_date", "rank", "code"]).reset_index(drop=True)


def _join_context(x: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    # Sector/theme registry. Missing values stay explicit; never infer a theme from name alone.
    candidates = [
        output_dir / "v73_shared_sector_registry.csv",
        output_dir / "v73_latest_discovery_snapshot.csv",
        output_dir / "v72_24_open0930_signal_ledger.csv",
        output_dir / "v72_24_open0930_universe_snapshot.csv",
    ]
    maps = []
    for fp in candidates:
        try:
            if not fp.exists() or fp.stat().st_size == 0:
                continue
            d = pd.read_csv(fp, dtype=str, low_memory=False)
            cc = next((c for c in ["code", "Code", "종목코드"] if c in d.columns), None)
            sc = next((c for c in ["theme", "Theme", "테마", "sector", "Sector", "Sector0930", "섹터"] if c in d.columns), None)
            if cc and sc:
                z = pd.DataFrame({"code": d[cc].map(_code), "theme_ctx": d[sc].fillna("").astype(str)})
                z = z[z["code"].ne("") & z["theme_ctx"].ne("")].drop_duplicates("code", keep="last")
                maps.append(z)
        except Exception:
            continue
    if maps:
        m = pd.concat(maps, ignore_index=True).drop_duplicates("code", keep="last")
        x = x.merge(m, on="code", how="left")
        x["theme"] = np.where(x["theme"].astype(str).str.strip().ne(""), x["theme"], x["theme_ctx"].fillna(""))
        x = x.drop(columns=["theme_ctx"], errors="ignore")
    x["theme"] = x["theme"].fillna("").astype(str).replace({"nan": "", "None": ""})
    x.loc[x["theme"].eq(""), "theme"] = "UNKNOWN"

    # Join real HAM intraday evidence if available.
    hfp = output_dir / "v72_ham_intraday_feature_ledger.csv"
    if hfp.exists() and hfp.stat().st_size > 0:
        try:
            h = pd.read_csv(hfp, dtype={"code": str}, low_memory=False)
            h["code"] = h["code"].map(_code)
            h["signal_date"] = pd.to_datetime(h.get("trade_date"), errors="coerce").dt.normalize()
            keep = [c for c in ["signal_date", "code", "HAM_MAX_RATIO", "HAM_MAX_VALUE", "PULLBACK_VALUE_RATIO_TO_HAM", "CLOSE1503_FROM_DAY_HIGH", "CLOSE1503_RESTART_ALIVE", "HAM_RESTART_CLOSE_CANDIDATE"] if c in h.columns]
            if {"signal_date", "code"}.issubset(keep):
                h = h[keep].drop_duplicates(["signal_date", "code"], keep="last")
                x = x.merge(h, on=["signal_date", "code"], how="left", suffixes=("", "_ham"))
                if "PULLBACK_VALUE_RATIO_TO_HAM" in x:
                    x["pullback_value_ratio"] = x["pullback_value_ratio"].fillna(pd.to_numeric(x["PULLBACK_VALUE_RATIO_TO_HAM"], errors="coerce"))
                if "CLOSE1503_FROM_DAY_HIGH" in x:
                    x["close1503_from_high"] = x["close1503_from_high"].fillna(pd.to_numeric(x["CLOSE1503_FROM_DAY_HIGH"], errors="coerce"))
        except Exception:
            pass
    return x


def _tag_patterns(x: pd.DataFrame) -> pd.DataFrame:
    def tags(r) -> List[str]:
        t = str(r.get("all_text", "")).lower()
        out: List[str] = []
        if any(k in t for k in ["pullback_restart_close", "첫눌림", "첫 눌림", "재양봉", "triangle_pullback"]):
            out.append("FIRST_LONG_BULL_PULLBACK")
        if ("응축" in t or "compression" in t) and any(k in t for k in ["재지지", "reclaim", "기준선", "baseline"]):
            out.append("VOLUME_COMPRESSION_RECLAIM")
        if any(k in t for k in ["저항돌파", "전고점", "breakout"]) and any(k in t for k in ["눌림", "pullback", "test"]):
            out.append("BREAKOUT_PULLBACK")
        if any(k in t for k in ["장중급등후종가밀림", "고가돌파", "high break", "종가회복"]):
            out.append("INTRADAY_HIGH_BREAK_CLOSE_RECOVERY")
        if any(k in t for k in ["대장", "leader"]) and any(k in t for k in ["restart", "재양봉", "재가속"]):
            out.append("THEME_LEADER_FIRST_RESTART")
        if any(k in t for k in ["외인+기관", "외국인·기관", "외국인+기관", "foreign institution"]):
            out.append("FOREIGN_INSTITUTION_PULLBACK")
        pvr = _num(r.get("pullback_value_ratio"))
        ham_close = _num(r.get("HAM_RESTART_CLOSE_CANDIDATE"), 0)
        if (math.isfinite(pvr) and pvr <= 0.70) or ham_close >= 1 or any(k in t for k in ["거래대금 폭발", "turnover explosion"]):
            out.append("TURNOVER_EXPLOSION_DRY_PULLBACK")
        m5 = _num(r.get("market5"))
        if (math.isfinite(m5) and m5 <= -2.0 and _num(r.get("ret1"), 0) > m5 + 1.0) or "relative_strength" in t:
            out.append("MARKET_CRASH_RELATIVE_STRENGTH")
        if "prime-recovery" in t or "prime_recovery" in t or "prime recovery" in t:
            out.append("PRIME_RECOVERY")
        if "lcz-restart-theme" in t or "lcz_restart_theme" in t or "lcz restart theme" in t:
            out.append("LCZ_RESTART_THEME")
        return list(dict.fromkeys(out))
    x["pattern_tokens"] = x.apply(tags, axis=1)
    x["pattern_combo"] = x["pattern_tokens"].map(lambda z: "|".join(z) if z else "UNCLASSIFIED")
    x["primary_pattern"] = x["pattern_tokens"].map(lambda z: z[0] if z else "UNCLASSIFIED")
    return x


def _add_theme_day_features(x: pd.DataFrame) -> pd.DataFrame:
    x["theme_day_mean_ret1"] = x.groupby(["signal_date", "theme"])["ret1"].transform("mean")
    x["theme_sync_positive"] = (x["theme_day_mean_ret1"] > 0).astype(int)
    x["theme_rank_proxy"] = x.groupby(["signal_date", "theme"])["base_score"].rank(method="first", ascending=False)
    x["leader_proxy"] = ((x["theme"].ne("UNKNOWN")) & (x["theme_rank_proxy"] <= 2)).astype(int)
    return x


def _paper_execution_map(output_dir: Path) -> pd.DataFrame:
    fps = [
        output_dir / "v73_execution_bridge.csv",
        output_dir / "v73_paper_forward_ledger.csv",
        output_dir / "v73_signal_lifecycle_ledger.csv",
        output_dir / "paper_forward_ledger.csv",
    ]
    rows = []
    for fp in fps:
        try:
            if not fp.exists() or fp.stat().st_size == 0:
                continue
            d = pd.read_csv(fp, dtype=str, low_memory=False)
            cc = next((c for c in ["code", "Code", "종목코드"] if c in d.columns), None)
            dc = next((c for c in ["signal_date", "trade_date", "date", "entry_date"] if c in d.columns), None)
            pc = next((c for c in ["execution_pnl_pct", "actual_pnl_pct", "paper_pnl_pct", "pnl_pct", "ret_pct"] if c in d.columns), None)
            if cc and pc:
                z = pd.DataFrame({
                    "code": d[cc].map(_code),
                    "paper_date": pd.to_datetime(d[dc], errors="coerce").dt.normalize() if dc else pd.NaT,
                    "paper_pnl": pd.to_numeric(d[pc], errors="coerce"),
                    "paper_source": fp.name,
                }).dropna(subset=["paper_pnl"])
                rows.append(z)
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["code", "paper_date", "paper_pnl", "paper_source"])
    return pd.concat(rows, ignore_index=True, sort=False)


def _posterior_rate(hits: float, n: int, prior: float, strength: float = 5.0) -> float:
    return (float(hits) + prior * strength) / (max(0, int(n)) + strength)


def _shrunk_mean(s: pd.Series, prior: float, strength: float = 5.0) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return (float(x.sum()) + prior * strength) / (len(x) + strength) if len(x) else prior


def _consecutive_losses(hist: pd.DataFrame) -> int:
    if hist.empty:
        return 0
    n = 0
    for v in pd.to_numeric(hist.sort_values("signal_date", ascending=False)["ret3"], errors="coerce"):
        if pd.isna(v):
            continue
        if v <= 0:
            n += 1
        else:
            break
    return n


def _overheat_penalty(hist: pd.DataFrame, current: pd.Series) -> Tuple[float, str]:
    if hist.empty:
        return 0.0, "NONE"
    d = pd.Timestamp(current["signal_date"])
    recent5 = hist[hist["signal_date"] >= d - pd.offsets.BDay(7)]
    reasons = []
    pen = 0.0
    if len(recent5) >= 3:
        pen += min(6.0, (len(recent5) - 2) * 2.0); reasons.append("REPEAT_5D")
    lowdist = _num(current.get("distance_low60"))
    if math.isfinite(lowdist) and lowdist >= 25:
        pen += min(6.0, (lowdist - 20) / 5.0 * 2.0); reasons.append("HIGH_LOW60_DISTANCE")
    if int(_num(current.get("late_wave"), 0)):
        pen += 4.0; reasons.append("LATE_WAVE")
    rr = pd.to_numeric(hist.sort_values("signal_date").tail(3)["ret3"], errors="coerce").dropna()
    allr = pd.to_numeric(hist["ret3"], errors="coerce").dropna()
    if len(rr) >= 2 and len(allr) >= 4 and rr.mean() < allr.mean() - 2.0:
        pen += 5.0; reasons.append("RECENT_DETERIORATION")
    return min(15.0, pen), "|".join(reasons) if reasons else "NONE"


def _score_name(hist: pd.DataFrame, global_hist: pd.DataFrame, paper_hist: pd.DataFrame, current: pd.Series) -> dict:
    n = len(hist)
    gp3 = float(global_hist["plus3"].mean()) if len(global_hist) else 0.45
    gsl = float(global_hist["stop_first"].mean()) if len(global_hist) else 0.45
    gd1 = float(global_hist["ret1"].mean()) if len(global_hist) else 0.0
    gd3 = float(global_hist["ret3"].mean()) if len(global_hist) else 0.0
    gd5 = float(global_hist["ret5"].mean()) if len(global_hist) else 0.0
    p3 = _posterior_rate(hist["plus3"].sum() if n else 0, n, gp3)
    sl = _posterior_rate(hist["stop_first"].sum() if n else 0, n, gsl)
    d1 = _shrunk_mean(hist["ret1"] if n else pd.Series(dtype=float), gd1)
    d3 = _shrunk_mean(hist["ret3"] if n else pd.Series(dtype=float), gd3)
    d5 = _shrunk_mean(hist["ret5"] if n else pd.Series(dtype=float), gd5)
    restart = hist[hist["pattern_tokens"].map(lambda z: "FIRST_LONG_BULL_PULLBACK" in z if isinstance(z, list) else False)] if n else pd.DataFrame()
    restart_rate = float(((restart["plus3"] > 0) | (restart["ret3"] > 0)).mean()) if len(restart) else np.nan
    dry = pd.to_numeric(hist.get("pullback_value_ratio", pd.Series(dtype=float)), errors="coerce").dropna()
    dry_rate = float((dry <= 0.70).mean()) if len(dry) else np.nan
    theme_sync = float(hist["theme_sync_positive"].mean()) if n else np.nan
    leader = float(hist["leader_proxy"].mean()) if n else np.nan
    paper = pd.to_numeric(paper_hist.get("paper_pnl", pd.Series(dtype=float)), errors="coerce").dropna()
    paper_mean = float(paper.mean()) if len(paper) else np.nan
    streak = _consecutive_losses(hist)
    penalty, penalty_reason = _overheat_penalty(hist, current)
    raw = 50.0
    raw += 35.0 * (p3 - gp3)
    raw -= 30.0 * (sl - gsl)
    raw += 1.8 * _clip(d3 - gd3, -5, 5)
    raw += 0.7 * _clip(d1 - gd1, -4, 4)
    raw += 0.4 * _clip(d5 - gd5, -5, 5)
    if math.isfinite(restart_rate): raw += 7.0 * (restart_rate - 0.5)
    if math.isfinite(dry_rate): raw += 5.0 * (dry_rate - 0.5)
    if math.isfinite(theme_sync): raw += 5.0 * (theme_sync - 0.5)
    if math.isfinite(leader): raw += 4.0 * (leader - 0.25)
    if math.isfinite(paper_mean): raw += 0.8 * _clip(paper_mean, -5, 5)
    raw -= min(9.0, streak * 2.5)
    raw -= penalty
    reliability = min(1.0, n / 10.0)
    score = _clip(50.0 + (raw - 50.0) * reliability, 0, 100)
    return {
        "familiar_name_score": score,
        "name_prior_n": n,
        "name_entry_n": int(hist["ret1"].notna().sum()) if n else 0,
        "name_d1_mean": float(pd.to_numeric(hist["ret1"], errors="coerce").mean()) if n else np.nan,
        "name_d3_mean": float(pd.to_numeric(hist["ret3"], errors="coerce").mean()) if n else np.nan,
        "name_d3_median": float(pd.to_numeric(hist["ret3"], errors="coerce").median()) if n else np.nan,
        "name_d5_mean": float(pd.to_numeric(hist["ret5"], errors="coerce").mean()) if n else np.nan,
        "name_plus3_rate": float(hist["plus3"].mean() * 100) if n else np.nan,
        "name_stop_rate": float(hist["stop_first"].mean() * 100) if n else np.nan,
        "name_restart_success_rate": restart_rate * 100 if math.isfinite(restart_rate) else np.nan,
        "name_pullback_dry_rate": dry_rate * 100 if math.isfinite(dry_rate) else np.nan,
        "name_theme_sync_rate": theme_sync * 100 if math.isfinite(theme_sync) else np.nan,
        "name_leader_rate": leader * 100 if math.isfinite(leader) else np.nan,
        "name_consecutive_losses": streak,
        "name_paper_n": len(paper),
        "name_paper_mean": paper_mean,
        "name_overheat_penalty": penalty,
        "name_overheat_reason": penalty_reason,
        "name_score_reliability": reliability,
    }


def _pattern_history(exploded: pd.DataFrame, token: str, date: pd.Timestamp) -> pd.DataFrame:
    if exploded.empty:
        return exploded
    return exploded[(exploded["pattern_token"] == token) & (exploded["signal_date"] < date)]


def _score_pattern(ph: pd.DataFrame, global_hist: pd.DataFrame) -> dict:
    n = len(ph)
    gp3 = float(global_hist["plus3"].mean()) if len(global_hist) else 0.45
    gsl = float(global_hist["stop_first"].mean()) if len(global_hist) else 0.45
    gd3 = float(global_hist["ret3"].mean()) if len(global_hist) else 0.0
    p3 = _posterior_rate(ph["plus3"].sum() if n else 0, n, gp3)
    sl = _posterior_rate(ph["stop_first"].sum() if n else 0, n, gsl)
    d3 = _shrunk_mean(ph["ret3"] if n else pd.Series(dtype=float), gd3)
    med = float(pd.to_numeric(ph["ret3"], errors="coerce").median()) if n else gd3
    pos_days = 0.5
    if n:
        dm = ph.groupby("signal_date")["ret3"].mean()
        pos_days = float((dm > 0).mean()) if len(dm) else 0.5
    theme_div = int(ph["theme"].nunique()) if n else 0
    stock_div = int(ph["code"].nunique()) if n else 0
    raw = 50 + 35 * (p3 - gp3) - 30 * (sl - gsl) + 1.5 * _clip(d3 - gd3, -5, 5) + 0.8 * _clip(med - gd3, -5, 5)
    raw += 5 * (pos_days - 0.5)
    if n >= 5 and stock_div <= 2:
        raw -= 6
    if n >= 5 and theme_div <= 1:
        raw -= 4
    reliability = min(1.0, n / 12.0)
    score = _clip(50 + (raw - 50) * reliability, 0, 100)
    return {
        "score": score,
        "n": n,
        "unique_stocks": stock_div,
        "signal_days": int(ph["signal_date"].nunique()) if n else 0,
        "ret1_mean": float(pd.to_numeric(ph["ret1"], errors="coerce").mean()) if n else np.nan,
        "ret3_mean": float(pd.to_numeric(ph["ret3"], errors="coerce").mean()) if n else np.nan,
        "ret3_median": med if n else np.nan,
        "ret5_mean": float(pd.to_numeric(ph["ret5"], errors="coerce").mean()) if n else np.nan,
        "plus3_rate": float(ph["plus3"].mean() * 100) if n else np.nan,
        "stop_rate": float(ph["stop_first"].mean() * 100) if n else np.nan,
        "mfe": float(pd.to_numeric(ph["mfe"], errors="coerce").mean()) if n else np.nan,
        "mae": float(pd.to_numeric(ph["mae"], errors="coerce").mean()) if n else np.nan,
        "positive_days": pos_days * 100 if n else np.nan,
        "reliability": reliability,
    }


def _build_causal_scores(x: pd.DataFrame, output_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    paper = _paper_execution_map(output_dir)
    exact_exec = {}
    if not paper.empty:
        for _,pr in paper.dropna(subset=["paper_date"]).sort_values("paper_date").iterrows():
            exact_exec[(pr["code"],pd.Timestamp(pr["paper_date"]).normalize())]=(pr["paper_pnl"],pr["paper_source"])
    ex_rows = []
    for _, r in x.iterrows():
        toks = r["pattern_tokens"] if isinstance(r["pattern_tokens"], list) else []
        if not toks:
            toks = ["UNCLASSIFIED"]
        for tok in toks:
            z = r.to_dict(); z["pattern_token"] = tok; ex_rows.append(z)
    exploded = pd.DataFrame(ex_rows)
    rows = []
    for i, r in x.iterrows():
        d = pd.Timestamp(r["signal_date"])
        start = d - pd.offsets.BDay(60)
        global_hist = x[x["signal_date"] < d]
        hist = global_hist[(global_hist["code"] == r["code"]) & (global_hist["signal_date"] >= start)]
        phist_paper = paper[(paper["code"] == r["code"]) & ((paper["paper_date"].isna()) | (paper["paper_date"] < d))] if not paper.empty else paper
        ns = _score_name(hist, global_hist, phist_paper, r)
        pstats = []
        for tok in (r["pattern_tokens"] if isinstance(r["pattern_tokens"], list) and r["pattern_tokens"] else ["UNCLASSIFIED"]):
            ph = _pattern_history(exploded, tok, d)
            ps = _score_pattern(ph, global_hist)
            ps["token"] = tok
            pstats.append(ps)
        weights = [max(1.0, p["n"]) for p in pstats]
        pscore = float(np.average([p["score"] for p in pstats], weights=weights)) if pstats else 50.0
        best = max(pstats, key=lambda p: (p["score"], p["n"])) if pstats else {"token": "UNCLASSIFIED", "n": 0, "score": 50}
        z = r.to_dict(); z.update(ns)
        _ex=exact_exec.get((r["code"],d),(np.nan,""))
        z["current_execution_pnl"]=_ex[0]; z["current_execution_source"]=_ex[1]
        z.update({
            "familiar_pattern_score": pscore,
            "familiar_pattern_primary": best["token"],
            "pattern_prior_n": int(best["n"]),
            "pattern_primary_score": float(best["score"]),
            "pattern_score_reliability": float(best.get("reliability", 0)),
            "window_start": pd.Timestamp(start).strftime("%Y-%m-%d"),
            "window_method": "PRIOR_60_BUSINESS_DAYS_CAUSAL",
        })
        rows.append(z)
    aud = pd.DataFrame(rows)
    return aud, exploded


def _profile_tables(aud: pd.DataFrame, exploded: pd.DataFrame, output_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    paper = _paper_execution_map(output_dir)
    name_rows = []
    for code, g in aud.groupby("code"):
        q = g.sort_values("signal_date")
        last = q.iloc[-1]
        dnext = pd.Timestamp(q["signal_date"].max()) + pd.offsets.BDay(1)
        dummy = last.copy(); dummy["signal_date"] = dnext
        ph = paper[paper["code"] == code] if not paper.empty else paper
        full_score = _score_name(q, aud, ph, dummy)
        recent20 = q[q["signal_date"] >= q["signal_date"].max() - pd.offsets.BDay(20)]
        recent5 = q[q["signal_date"] >= q["signal_date"].max() - pd.offsets.BDay(5)]
        name_rows.append({
            "code": code, "name": last.get("name", ""), "theme": last.get("theme", "UNKNOWN"),
            "candidate_count_60d": len(q[q["signal_date"] >= dnext-pd.offsets.BDay(60)]),
            "candidate_count_20d_observed": len(recent20), "candidate_count_5d_observed": len(recent5),
            "entry_count_60d": int(q["ret1"].notna().sum()), "d1_mean": q["ret1"].mean(), "d3_mean": q["ret3"].mean(),
            "d3_median": q["ret3"].median(), "d5_mean": q["ret5"].mean(), "plus3_rate": q["plus3"].mean()*100,
            "stop_rate": q["stop_first"].mean()*100, "restart_success_rate": full_score.get("name_restart_success_rate"),
            "pullback_dry_rate": full_score.get("name_pullback_dry_rate"), "theme_sync_rate": q["theme_sync_positive"].mean()*100,
            "leader_rate": q["leader_proxy"].mean()*100, "consecutive_losses": _consecutive_losses(q),
            "paper_n": len(ph), "paper_mean": pd.to_numeric(ph.get("paper_pnl",pd.Series(dtype=float)),errors="coerce").mean() if len(ph) else np.nan,
            "familiar_name_score": full_score.get("familiar_name_score"), "reliability": full_score.get("name_score_reliability"),
            "overheat_penalty": full_score.get("name_overheat_penalty"), "overheat_reason": full_score.get("name_overheat_reason"),
            "last_signal_date": pd.Timestamp(last["signal_date"]).strftime("%Y-%m-%d"),
        })
    name_df = pd.DataFrame(name_rows)

    pattern_rows = []
    for token in PATTERNS + ["UNCLASSIFIED"]:
        q = exploded[exploded["pattern_token"] == token]
        if q.empty:
            pattern_rows.append({"pattern_token": token, "pattern_name": PATTERN_KO.get(token, token), "n": 0, "familiar_pattern_score":50.0})
            continue
        ps=_score_pattern(q,aud)
        pattern_rows.append({
            "pattern_token": token, "pattern_name": PATTERN_KO.get(token, token), "n": len(q), "unique_stocks": q["code"].nunique(),
            "signal_days": q["signal_date"].nunique(), "d1_mean": q["ret1"].mean(), "d3_mean": q["ret3"].mean(), "d3_median": q["ret3"].median(),
            "d5_mean": q["ret5"].mean(), "plus3_rate": q["plus3"].mean() * 100, "stop_rate": q["stop_first"].mean() * 100,
            "mfe": q["mfe"].mean(), "mae": q["mae"].mean(), "themes": q["theme"].nunique(),
            "cost20_d1": q["ret1"].mean() - 0.20, "cost20_d3": q["ret3"].mean() - 0.20, "cost50_d3": q["ret3"].mean() - 0.50,
            "familiar_pattern_score":ps.get("score"),"score_reliability":ps.get("reliability"),
        })
    pattern_df = pd.DataFrame(pattern_rows)
    name_df.to_csv(output_dir / NAME_PROFILE, index=False, encoding="utf-8-sig")
    pattern_df.to_csv(output_dir / PATTERN_PROFILE, index=False, encoding="utf-8-sig")
    # Requested pattern x market-regime and pattern x sector views. Unknown stays explicit.
    pm=[]; psx=[]
    for (tok,reg),g in exploded.groupby(["pattern_token","market_regime"],dropna=False):
        z=_perf(g,"PATTERN_MARKET",str(reg));z.update({"pattern_token":tok,"market_regime":str(reg)});pm.append(z)
    for (tok,sec),g in exploded.groupby(["pattern_token","theme"],dropna=False):
        z=_perf(g,"PATTERN_SECTOR",str(sec));z.update({"pattern_token":tok,"theme":str(sec)});psx.append(z)
    pd.DataFrame(pm).to_csv(output_dir/PATTERN_MARKET_SUMMARY,index=False,encoding="utf-8-sig")
    pd.DataFrame(psx).to_csv(output_dir/PATTERN_SECTOR_SUMMARY,index=False,encoding="utf-8-sig")
    return name_df, pattern_df

def _top_k_per_date(df: pd.DataFrame, score_col: str, k: int = 5, filter_mask: Optional[pd.Series] = None) -> pd.DataFrame:
    z = df.copy()
    if filter_mask is not None:
        z = z[filter_mask.reindex(z.index).fillna(False)].copy()
    if z.empty:
        return z
    return z.sort_values(["signal_date", score_col, "base_score", "rank"], ascending=[True, False, False, True]).groupby("signal_date", group_keys=False).head(k)


def _portfolio_mdd(df: pd.DataFrame) -> float:
    if df is None or df.empty:return np.nan
    daily=pd.to_numeric(df.groupby("signal_date")["ret3"].mean(),errors="coerce").dropna()/100.0
    if daily.empty:return np.nan
    eq=(1+daily).cumprod();peak=eq.cummax();dd=(eq/peak-1)*100
    return float(dd.min())


def _perf(df: pd.DataFrame, model="", bucket="") -> dict:
    if df is None or df.empty:
        return {"model": model, "bucket": bucket, "n": 0}
    r3 = pd.to_numeric(df["ret3"], errors="coerce")
    profit = r3.clip(lower=0)
    total_profit = float(profit.sum())
    top2 = float(profit.nlargest(2).sum()) if len(profit) else 0.0
    code_dedup = df.sort_values("signal_date").drop_duplicates("code", keep="first")
    date_means = df.groupby("signal_date")["ret3"].mean()
    return {
        "model": model, "bucket": bucket, "n": len(df), "unique_stocks": df["code"].nunique(), "signal_days": df["signal_date"].nunique(),
        "themes": df["theme"].nunique(), "ret1_mean": df["ret1"].mean(), "ret3_mean": r3.mean(), "ret3_median": r3.median(),
        "ret3_trim10": _trim_mean(r3), "ret5_mean": df["ret5"].mean(), "plus3_rate": df["plus3"].mean() * 100,
        "stop_rate": df["stop_first"].mean() * 100, "mfe": df["mfe"].mean(), "mae": df["mae"].mean(),
        "cost20_ret3": r3.mean() - 0.20, "cost50_ret3": r3.mean() - 0.50,
        "positive_signal_days": int((date_means > 0).sum()), "top2_profit_concentration": top2 / total_profit * 100 if total_profit > 0 else np.nan,
        "code_dedup_ret3": code_dedup["ret3"].mean(), "code_dedup_median": code_dedup["ret3"].median(),
        "max_drawdown_proxy": df["mae"].min(), "portfolio_mdd_pct":_portfolio_mdd(df),
        "top_code_share_pct":df["code"].value_counts(normalize=True).max()*100 if len(df) else np.nan,
        "top_theme_share_pct":df["theme"].value_counts(normalize=True).max()*100 if len(df) else np.nan,
    }


def _objective(df: pd.DataFrame) -> float:
    if df is None or df.empty:
        return -1e9
    p = _perf(df)
    conc = p.get("top2_profit_concentration")
    conc_pen = max(0, (_num(conc, 100) - 50) / 10) if math.isfinite(_num(conc)) else 5
    date_pen = max(0, 4 - int(p.get("signal_days", 0))) * 1.5
    n_pen = max(0, 15 - int(p.get("n", 0))) * 0.15
    return (
        _num(p.get("ret3_median"), -20) + 0.6 * _num(p.get("ret3_trim10"), -20) + 0.3 * _num(p.get("cost50_ret3"), -20)
        + 0.025 * (_num(p.get("plus3_rate"), 0) - _num(p.get("stop_rate"), 100)) - conc_pen - date_pen - n_pen
    )


@dataclass
class Policy:
    policy_id: str
    train_start: str
    train_end: str
    oos_start: str
    top_k: int
    b_name_cap: float
    b_pattern_cap: float
    c_name_min_n: int
    c_pattern_min_n: int
    c_name_min_score: float
    c_pattern_min_score: float
    c_max_overheat_penalty: float
    created_at: str
    policy_hash: str = ""

    def finish(self):
        d = asdict(self).copy(); d.pop("policy_hash", None)
        self.policy_hash = hashlib.sha256(json.dumps(d, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]
        return self


def _apply_policy(aud: pd.DataFrame, policy: Policy) -> Dict[str, pd.DataFrame]:
    z = aud.copy()
    z["model_a_score"] = z["base_score"]
    z["model_b_score"] = z["base_score"] + policy.b_name_cap * (z["familiar_name_score"] - 50) / 50 + policy.b_pattern_cap * (z["familiar_pattern_score"] - 50) / 50
    cmask = (
        (z["name_prior_n"] >= policy.c_name_min_n) & (z["pattern_prior_n"] >= policy.c_pattern_min_n)
        & (z["familiar_name_score"] >= policy.c_name_min_score) & (z["familiar_pattern_score"] >= policy.c_pattern_min_score)
        & (z["name_overheat_penalty"] <= policy.c_max_overheat_penalty)
    )
    return {
        "A_BASE": _top_k_per_date(z, "model_a_score", policy.top_k),
        "B_RANK_AUX": _top_k_per_date(z, "model_b_score", policy.top_k),
        "C_FILTER": _top_k_per_date(z, "model_b_score", policy.top_k, cmask),
        "ALL": z,
    }


def _train_policy(aud: pd.DataFrame, output_dir: Path) -> Tuple[Optional[Policy], str, pd.DataFrame]:
    dates = sorted(pd.to_datetime(aud["signal_date"].dropna().unique()))
    min_days = int(os.environ.get("FAMILIAR_MIN_LOCK_SIGNAL_DAYS", "10"))
    force = str(os.environ.get("FAMILIAR_FORCE_RELOCK", "0")).lower() in ("1", "true", "yes")
    fp = output_dir / POLICY_FILE
    if fp.exists() and not force:
        try:
            p = Policy(**json.loads(fp.read_text(encoding="utf-8")))
            return p, "LOCKED_EXISTING", pd.DataFrame()
        except Exception:
            pass
    if len(dates) < min_days:
        return None, f"WARMUP_SIGNAL_DAYS_{len(dates)}_LT_{min_days}", pd.DataFrame()
    cut = max(6, int(len(dates) * 0.70))
    cut = min(cut, len(dates) - 2)
    train_dates = dates[:cut]
    train = aud[aud["signal_date"].isin(train_dates)].copy()
    if train.empty:
        return None, "WARMUP_NO_TRAIN", pd.DataFrame()
    rows = []
    best = None
    top_k = int(os.environ.get("FAMILIAR_RESEARCH_TOP_K", "5"))
    b_grid = [(2, 2), (3, 2), (3, 3), (4, 3), (4, 4)]
    c_grid = [
        (3, 5, 55, 55, 8), (5, 5, 55, 60, 8), (5, 8, 60, 60, 6),
        (8, 8, 60, 65, 5), (8, 10, 65, 65, 4),
    ]
    for bn, bp in b_grid:
        for cn, cp, csn, csp, oh in c_grid:
            p = Policy(
                policy_id="FAMILIAR_POLICY_LOCK_" + pd.Timestamp(train_dates[-1]).strftime("%Y%m%d"),
                train_start=pd.Timestamp(train_dates[0]).strftime("%Y-%m-%d"), train_end=pd.Timestamp(train_dates[-1]).strftime("%Y-%m-%d"),
                oos_start=pd.Timestamp(dates[cut]).strftime("%Y-%m-%d"), top_k=top_k,
                b_name_cap=bn, b_pattern_cap=bp, c_name_min_n=cn, c_pattern_min_n=cp,
                c_name_min_score=csn, c_pattern_min_score=csp, c_max_overheat_penalty=oh,
                created_at=datetime.now().isoformat(),
            ).finish()
            models = _apply_policy(train, p)
            aobj = _objective(models["A_BASE"]); bobj = _objective(models["B_RANK_AUX"]); cobj = _objective(models["C_FILTER"])
            row = asdict(p); row.update({"a_objective": aobj, "b_objective": bobj, "c_objective": cobj, "selection_objective": max(bobj, cobj), "b_edge": bobj-aobj, "c_edge": cobj-aobj})
            rows.append(row)
            # Prefer B on ties because user asked ranking auxiliary before hard filter.
            candidate = (max(bobj, cobj), bobj, -abs(len(models["C_FILTER"]) - len(models["A_BASE"])), p)
            if best is None or candidate[:3] > best[:3]:
                best = candidate
    sweep = pd.DataFrame(rows).sort_values(["selection_objective", "b_objective"], ascending=False)
    policy = best[3]
    fp.write_text(json.dumps(asdict(policy), ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame([asdict(policy) | {"lock_status": "NEW_LOCK"}]).to_csv(output_dir / POLICY_AUDIT, index=False, encoding="utf-8-sig")
    return policy, "NEW_LOCK", sweep


def _matched_pairs(models: Dict[str, pd.DataFrame], all_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    pairs = []
    for m in ["B_RANK_AUX", "C_FILTER"]:
        treated = models[m]
        base = models["A_BASE"]
        for d in sorted(set(treated["signal_date"]) | set(base["signal_date"])):
            t = treated[treated["signal_date"] == d]
            a = base[base["signal_date"] == d]
            add = t[~t["code"].isin(a["code"])]
            controls = a[~a["code"].isin(t["code"])].copy()
            used = set()
            for _, r in add.iterrows():
                cand = controls[~controls["code"].isin(used)].copy()
                if cand.empty:
                    continue
                cand["sector_pen"] = (cand["theme"] != r["theme"]).astype(int)
                cand["dist"] = (cand["base_score"] - r["base_score"]).abs() + cand["sector_pen"] * 10
                for _c,_scale in [("day_return",2.0),("close1503_from_high",2.0)]:
                    _rv=_num(r.get(_c))
                    if math.isfinite(_rv) and _c in cand:
                        cand["dist"] += (pd.to_numeric(cand[_c],errors="coerce")-_rv).abs().fillna(5)*_scale
                for _c,_scale in [("day_amount",0.000000001),("market_cap",0.0000000001)]:
                    _rv=_num(r.get(_c))
                    if math.isfinite(_rv) and _rv>0 and _c in cand:
                        cand["dist"] += (np.log1p(pd.to_numeric(cand[_c],errors="coerce").clip(lower=0))-math.log1p(_rv)).abs().fillna(3)*_scale*1e9
                c = cand.sort_values(["dist", "rank"]).iloc[0]
                used.add(c["code"])
                pairs.append({
                    "model": m, "signal_date": d, "treated_code": r["code"], "control_code": c["code"], "theme": r["theme"],
                    "treated_base": r["base_score"], "control_base": c["base_score"], "treated_ret3": r["ret3"], "control_ret3": c["ret3"],
                    "edge_ret3": r["ret3"] - c["ret3"], "treated_plus3": r["plus3"], "control_plus3": c["plus3"],
                    "treated_stop": r["stop_first"], "control_stop": c["stop_first"],
                })
    pdf = pd.DataFrame(pairs)
    sums = []
    for m, g in pdf.groupby("model") if not pdf.empty else []:
        sums.append({"model": m, "n_pairs": len(g), "edge_ret3_mean": g["edge_ret3"].mean(), "edge_ret3_median": g["edge_ret3"].median(), "edge_positive_rate": (g["edge_ret3"] > 0).mean() * 100, "plus3_edge_p": (g["treated_plus3"] - g["control_plus3"]).mean() * 100, "stop_edge_p": (g["treated_stop"] - g["control_stop"]).mean() * 100})
    return pdf, pd.DataFrame(sums)


def _cluster_bootstrap(a: pd.DataFrame, b: pd.DataFrame, cluster: str, n_iter=500, seed=73366) -> dict:
    keys = sorted(set(a[cluster].dropna().astype(str)) | set(b[cluster].dropna().astype(str)))
    if len(keys) < 2:
        return {"cluster": cluster, "clusters": len(keys), "iterations": 0, "edge_mean": np.nan, "ci05": np.nan, "ci95": np.nan, "positive_prob": np.nan}
    rng = np.random.default_rng(seed + len(keys))
    vals = []
    aa = a.copy(); bb = b.copy(); aa[cluster] = aa[cluster].astype(str); bb[cluster] = bb[cluster].astype(str)
    for _ in range(n_iter):
        draw = rng.choice(keys, size=len(keys), replace=True)
        ar = []; br = []
        for k in draw:
            ar.extend(pd.to_numeric(aa.loc[aa[cluster] == k, "ret3"], errors="coerce").dropna().tolist())
            br.extend(pd.to_numeric(bb.loc[bb[cluster] == k, "ret3"], errors="coerce").dropna().tolist())
        if ar and br:
            vals.append(float(np.mean(br) - np.mean(ar)))
    if not vals:
        return {"cluster": cluster, "clusters": len(keys), "iterations": 0, "edge_mean": np.nan, "ci05": np.nan, "ci95": np.nan, "positive_prob": np.nan}
    v = np.asarray(vals)
    return {"cluster": cluster, "clusters": len(keys), "iterations": len(v), "edge_mean": v.mean(), "ci05": np.quantile(v, .05), "ci95": np.quantile(v, .95), "positive_prob": (v > 0).mean() * 100}


def _execution_summary(models: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for m, df in models.items():
        if m == "ALL" or df.empty:
            continue
        actual=pd.to_numeric(df.get("current_execution_pnl",pd.Series(dtype=float)),errors="coerce").dropna()
        if len(actual):
            rows.append({"model":m,"cost_bp":"LEDGER","n":len(actual),"execution_ret3_mean":actual.mean(),"execution_ret3_median":actual.median(),"win_rate":(actual>0).mean()*100,"source":"PAPER_OR_ACTUAL_EXECUTION_LEDGER"})
        for bp in [20, 50]:
            cost = bp / 100.0
            ret = pd.to_numeric(df["ret3"], errors="coerce") - cost
            rows.append({"model": m, "cost_bp": bp, "n": ret.notna().sum(), "execution_ret3_mean": ret.mean(), "execution_ret3_median": ret.median(), "win_rate": (ret > 0).mean() * 100, "source": "CLOSE_RETURN_MINUS_COST_PROXY"})
    return pd.DataFrame(rows)

def _repeat_ledger(aud: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in aud.iterrows():
        d = pd.Timestamp(r["signal_date"])
        prev = aud[(aud["code"] == r["code"]) & (aud["signal_date"] < d)]
        p20 = prev[prev["signal_date"] >= d - pd.offsets.BDay(20)]
        p5 = prev[prev["signal_date"] >= d - pd.offsets.BDay(5)]
        tprev = aud[(aud["theme"] == r["theme"]) & (aud["signal_date"] < d)] if r["theme"] != "UNKNOWN" else pd.DataFrame()
        rows.append({
            "signal_date": d, "code": r["code"], "name": r["name"], "theme": r["theme"], "appear_20d": len(p20), "appear_5d": len(p5),
            "recent_plus3": int(p20["plus3"].sum()) if len(p20) else 0, "recent_stop": int(p20["stop_first"].sum()) if len(p20) else 0,
            "theme_appear_20d": int((tprev["signal_date"] >= d - pd.offsets.BDay(20)).sum()) if len(tprev) else 0,
            "leader_count_20d": int(p20["leader_proxy"].sum()) if len(p20) else 0,
            "familiar_name_score": r["familiar_name_score"], "familiar_pattern_score": r["familiar_pattern_score"],
            "overheat_penalty": r["name_overheat_penalty"], "overheat_reason": r["name_overheat_reason"],
        })
    return pd.DataFrame(rows)


def _data_availability(aud: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    rows = []
    specs = {
        "sector_theme": aud["theme"].ne("UNKNOWN"),
        "ham_turnover_ratio": pd.to_numeric(aud.get("HAM_MAX_RATIO", np.nan), errors="coerce").notna() if "HAM_MAX_RATIO" in aud else pd.Series(False, index=aud.index),
        "pullback_turnover_ratio": pd.to_numeric(aud["pullback_value_ratio"], errors="coerce").notna(),
        "close1503_distance": pd.to_numeric(aud["close1503_from_high"], errors="coerce").notna(),
        "paper_execution": pd.Series(aud["name_paper_n"].fillna(0).gt(0), index=aud.index),
        "matched_day_return": pd.to_numeric(aud["day_return"],errors="coerce").notna(),
        "matched_day_amount": pd.to_numeric(aud["day_amount"],errors="coerce").notna(),
        "matched_market_cap": pd.to_numeric(aud["market_cap"],errors="coerce").notna(),
        "leader_role_actual": pd.Series(False,index=aud.index),
    }
    for k, m in specs.items():
        rows.append({"field_group": k, "available_rows": int(m.sum()), "total_rows": len(aud), "coverage_pct": float(m.mean() * 100) if len(aud) else 0, "status": "AVAILABLE" if m.any() else "MISSING"})
    for k in ["downside_trade_intensity", "ask_depletion_speed", "closing_buy_execution_share", "vwap_recovery"]:
        rows.append({"field_group": k, "available_rows": 0, "total_rows": len(aud), "coverage_pct": 0, "status": "UNAVAILABLE_NO_TICK_OR_VWAP_FEED"})
    out = pd.DataFrame(rows)
    out.to_csv(output_dir / DATA_AUDIT, index=False, encoding="utf-8-sig")
    return out


def _insert_report(report: str, block: str) -> str:
    s = str(report or "")
    if REPORT_HEADER in s:
        st = s.find(REPORT_HEADER)
        stops = ["\n🌙 [전일 야간환경", "\n🏆 [V48/V61", "\n🛡️ [손절거리", "\n📊 [거래량 결론]"]
        ends = [s.find(z, st + 1) for z in stops if s.find(z, st + 1) >= 0]
        s = s[:st].rstrip() + (("\n\n" + s[min(ends):].lstrip("\n")) if ends else "")
    anchors = ["\n🌙 [전일 야간환경", "\n🏆 [V48/V61", "\n🛡️ [손절거리"]
    pos = [s.find(a) for a in anchors if s.find(a) >= 0]
    if pos:
        k = min(pos)
        return s[:k].rstrip() + "\n\n" + block + "\n\n" + s[k:].lstrip("\n")
    return s.rstrip() + "\n\n" + block


def build_report(aud: pd.DataFrame, policy: Optional[Policy], lock_status: str, summaries: pd.DataFrame, oos: pd.DataFrame, matched: pd.DataFrame, boots: pd.DataFrame, data_audit: pd.DataFrame) -> str:
    lines = [
        REPORT_HEADER,
        f"📌 {VERSION} · {FACTOR_NAME} · RESEARCH_ONLY=True",
        "- 목적: 과거에 잘된 종목을 외우는 것이 아니라, 해당 신호일 이전 60거래일의 반복 성과로 기존 후보의 순위 안정성이 개선되는지 검증합니다.",
        "- LIVE/PRIME/LCZ/M5R/ENVUP/기존 RESTART/익절·손절 정책 영향 0 · +3 우선익절, D+1~D+3 관리, 5일 장기보유 금지 유지.",
        f"📁 causal rows {len(aud)} · 종목 {aud['code'].nunique() if len(aud) else 0} · 신호일 {aud['signal_date'].nunique() if len(aud) else 0} · policy={lock_status}",
    ]
    if policy is None:
        lines.append("- POLICY LOCK 대기: 독립 신호일 최소표본이 쌓이기 전에는 A/B/C 결과를 탐색표로만 저장하며 OOS 승격판정을 하지 않습니다.")
    else:
        lines.append(f"🔒 policy {policy.policy_id} · TRAIN {policy.train_start}~{policy.train_end} · OOS {policy.oos_start}~ · hash {policy.policy_hash}")
        lines.append(f"- B 보조상한: 종목 {policy.b_name_cap:.1f}점 + 패턴 {policy.b_pattern_cap:.1f}점 · C 최소: name n{policy.c_name_min_n}/score{policy.c_name_min_score:.0f}, pattern n{policy.c_pattern_min_n}/score{policy.c_pattern_min_score:.0f}, 과열≤{policy.c_max_overheat_penalty:.0f}")
    lines.append("📊 [A 기존 / B 순위보조 / C 최소성과필터]")
    if summaries.empty:
        lines.append("- 평가 가능한 모델 표본 없음")
    else:
        for _, r in summaries.iterrows():
            lines.append(f"- {r['bucket']} {r['model']}: n{_int(r.get('n',0))}·종목{_int(r.get('unique_stocks',0))}·날짜{_int(r.get('signal_days',0))}·테마{_int(r.get('themes',0))} | D1 {_fmt(r.get('ret1_mean'),2)}% / D3 평균 {_fmt(r.get('ret3_mean'),2)}·중앙 {_fmt(r.get('ret3_median'),2)}·절사 {_fmt(r.get('ret3_trim10'),2)} / D5 {_fmt(r.get('ret5_mean'),2)} | +3 {_fmt(r.get('plus3_rate'),1,False)}%/SL {_fmt(r.get('stop_rate'),1,False)}% | 50bp D3 {_fmt(r.get('cost50_ret3'),2)}% | 종목중복제거 {_fmt(r.get('code_dedup_ret3'),2)}% | 상2집중 {_fmt(r.get('top2_profit_concentration'),1,False)}%")
    if not matched.empty:
        lines.append("⚖️ [동일 날짜·기존점수 근접 matched-control]")
        for _, r in matched.iterrows():
            lines.append(f"- {r['model']}: pairs {int(r['n_pairs'])} | D3 edge 평균 {_fmt(r['edge_ret3_mean'],2)}·중앙 {_fmt(r['edge_ret3_median'],2)} | edge>0 {_fmt(r['edge_positive_rate'],1,False)}% | +3차 {_fmt(r['plus3_edge_p'],1)}p / SL차 {_fmt(r['stop_edge_p'],1)}p")
    if not boots.empty:
        lines.append("🧪 [클러스터 부트스트랩 · B-A OOS D3 edge]")
        for _, r in boots.iterrows():
            lines.append(f"- {r['cluster']}: clusters {int(r['clusters'])} · edge {_fmt(r['edge_mean'],2)}% · 90%CI [{_fmt(r['ci05'],2)},{_fmt(r['ci95'],2)}] · 양수확률 {_fmt(r['positive_prob'],1,False)}%")
    if not data_audit.empty:
        miss = data_audit[data_audit["status"].astype(str).str.startswith("UNAVAILABLE")]
        lines.append(f"🧾 데이터커버리지: theme {_num(data_audit.loc[data_audit['field_group']=='sector_theme','coverage_pct'].iloc[0] if (data_audit['field_group']=='sector_theme').any() else 0):.0f}% · HAM/pullback 실제 원장 가용분만 사용 · 체결강도/호가소진/종가매수비중/VWAP은 미연결 시 점수 0·UNKNOWN 유지")
    lines.extend([
        "- C 필터는 비교용 연구모델일 뿐 LIVE 허용목록이 아닙니다. B도 기존 점수가 비슷한 후보의 shadow 순위만 계산합니다.",
        "- PAPER/실제 체결 원장이 있으면 친숙도에 제한적으로 반영하며, 없으면 일봉 수익으로 대체하지 않고 MISSING 처리합니다.",
        f"- Actions CSV: {ROW_AUDIT} · {NAME_PROFILE} · {PATTERN_PROFILE} · {PATTERN_MARKET_SUMMARY} · {PATTERN_SECTOR_SUMMARY} · {PATTERN_TRAIN_OOS_SUMMARY} · {MODEL_SUMMARY} · {OOS_SUMMARY} · {MATCHED_PAIRS} · {MATCHED_SUMMARY} · {BOOTSTRAP_SUMMARY} · {CONCENTRATION_SUMMARY} · {EXECUTION_SUMMARY} · {REPEAT_LEDGER} · {DATA_AUDIT}",
    ])
    return "\n".join(lines)


def run_backtest(eval_df: pd.DataFrame, output_dir: str | Path = "reports", base_report: str = "") -> Tuple[str, pd.DataFrame]:
    out = _outdir(output_dir)
    x = _normalise_eval(eval_df, out)
    if x.empty:
        block = REPORT_HEADER + f"\n📌 {VERSION} · RESEARCH_ONLY=True\n- 평가 가능한 기존 후보가 없어 정책/점수 갱신을 하지 않았습니다."
        _empty_csvs(out)
        try:
            (out / REPORT_BLOCK_FILE).write_text(block, encoding="utf-8")
        except Exception:
            pass
        return _insert_report(base_report, block), pd.DataFrame()
    aud, exploded = _build_causal_scores(x, out)
    aud.to_csv(out / ROW_AUDIT, index=False, encoding="utf-8-sig")
    _profile_tables(aud, exploded, out)
    repeat = _repeat_ledger(aud); repeat.to_csv(out / REPEAT_LEDGER, index=False, encoding="utf-8-sig")
    data_audit = _data_availability(aud, out)
    policy, lock_status, sweep = _train_policy(aud, out)
    if not sweep.empty:
        sweep.to_csv(out / "v72_familiar_policy_train_sweep.csv", index=False, encoding="utf-8-sig")
    if policy is None:
        # Stable neutral policy for descriptive A/B/C only; never persisted or called locked.
        dates = sorted(aud["signal_date"].unique())
        p = Policy("WARMUP_NO_LOCK", str(pd.Timestamp(dates[0]).date()), str(pd.Timestamp(dates[-1]).date()), "", 5, 3, 3, 5, 5, 60, 60, 6, datetime.now().isoformat()).finish()
        buckets = {"FULL_EXPLORATORY": aud}
    else:
        p = policy
        train_end = pd.Timestamp(p.train_end)
        oos_start = pd.Timestamp(p.oos_start)
        buckets = {"TRAIN": aud[aud["signal_date"] <= train_end], "OOS": aud[aud["signal_date"] >= oos_start]}
    # Pattern-level TRAIN/OOS table uses the same locked chronological boundary.
    pto=[]
    if policy is None:
        _pbuckets={"FULL_EXPLORATORY":exploded}
    else:
        _pbuckets={"TRAIN":exploded[exploded["signal_date"]<=pd.Timestamp(p.train_end)],"OOS":exploded[exploded["signal_date"]>=pd.Timestamp(p.oos_start)]}
    for _b,_q in _pbuckets.items():
        for _tok,_g in _q.groupby("pattern_token") if not _q.empty else []:
            _z=_perf(_g,"PATTERN",_b);_z["pattern_token"]=_tok;_z["pattern_name"]=PATTERN_KO.get(_tok,_tok);pto.append(_z)
    pd.DataFrame(pto).to_csv(out/PATTERN_TRAIN_OOS_SUMMARY,index=False,encoding="utf-8-sig")
    rows = []
    oos_models = None
    for bucket, q in buckets.items():
        models = _apply_policy(q, p)
        if bucket == "OOS": oos_models = models
        for m in ["A_BASE", "B_RANK_AUX", "C_FILTER"]:
            rows.append(_perf(models[m], m, bucket))
    summary = pd.DataFrame(rows)
    summary.to_csv(out / MODEL_SUMMARY, index=False, encoding="utf-8-sig")
    oos = summary[summary["bucket"].eq("OOS")].copy() if "bucket" in summary else pd.DataFrame()
    oos.to_csv(out / OOS_SUMMARY, index=False, encoding="utf-8-sig")
    models_for_match = oos_models if oos_models is not None else _apply_policy(aud, p)
    pairs, matched = _matched_pairs(models_for_match, models_for_match["ALL"])
    pairs.to_csv(out / MATCHED_PAIRS, index=False, encoding="utf-8-sig")
    matched.to_csv(out / MATCHED_SUMMARY, index=False, encoding="utf-8-sig")
    execution = _execution_summary(models_for_match); execution.to_csv(out / EXECUTION_SUMMARY, index=False, encoding="utf-8-sig")
    concentration = summary[[c for c in ["model", "bucket", "n", "unique_stocks", "themes", "top2_profit_concentration", "code_dedup_ret3", "max_drawdown_proxy", "portfolio_mdd_pct", "top_code_share_pct", "top_theme_share_pct"] if c in summary.columns]].copy()
    concentration.to_csv(out / CONCENTRATION_SUMMARY, index=False, encoding="utf-8-sig")
    boots = []
    if oos_models is not None and not oos_models["A_BASE"].empty and not oos_models["B_RANK_AUX"].empty:
        aa = oos_models["A_BASE"].copy(); bb = oos_models["B_RANK_AUX"].copy()
        for cl in ["signal_date", "code", "theme"]:
            boots.append(_cluster_bootstrap(aa, bb, cl))
    boot_df = pd.DataFrame(boots); boot_df.to_csv(out / BOOTSTRAP_SUMMARY, index=False, encoding="utf-8-sig")
    block = build_report(aud, policy, lock_status, summary, oos, matched, boot_df, data_audit)
    try:
        (out / REPORT_BLOCK_FILE).write_text(block, encoding="utf-8")
    except Exception:
        pass
    return _insert_report(base_report, block), aud



def force_report(report: str, output_dir: str | Path = "reports") -> str:
    out = _outdir(output_dir)
    try:
        block = (out / REPORT_BLOCK_FILE).read_text(encoding="utf-8").strip()
    except Exception:
        block = ""
    return _insert_report(report, block) if block else str(report or "")

def _empty_csvs(out: Path):
    schemas = {
        ROW_AUDIT: ["signal_date", "code", "familiar_name_score", "familiar_pattern_score"], NAME_PROFILE: ["code", "familiar_name_score"],
        PATTERN_PROFILE: ["pattern_token", "n"], MODEL_SUMMARY: ["model", "bucket", "n"], OOS_SUMMARY: ["model", "n"],
        MATCHED_PAIRS: ["model", "signal_date", "treated_code", "control_code"], MATCHED_SUMMARY: ["model", "n_pairs"],
        BOOTSTRAP_SUMMARY: ["cluster", "clusters"], REPEAT_LEDGER: ["signal_date", "code", "appear_20d"], SHADOW_LEDGER: ["signal_date", "code", "familiar_name_score"],
        DATA_AUDIT: ["field_group", "coverage_pct", "status"], PATTERN_MARKET_SUMMARY:["pattern_token","market_regime","n"], PATTERN_SECTOR_SUMMARY:["pattern_token","theme","n"], PATTERN_TRAIN_OOS_SUMMARY:["pattern_token","bucket","n"], EXECUTION_SUMMARY: ["model", "cost_bp", "execution_ret3_mean"], CONCENTRATION_SUMMARY: ["model", "top2_profit_concentration"],
    }
    for name, cols in schemas.items():
        fp = out / name
        if not fp.exists(): pd.DataFrame(columns=cols).to_csv(fp, index=False, encoding="utf-8-sig")


def _latest_profiles(output_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    try: n = pd.read_csv(output_dir / NAME_PROFILE, dtype={"code": str}, low_memory=False)
    except Exception: n = pd.DataFrame()
    try: p = pd.read_csv(output_dir / PATTERN_PROFILE, low_memory=False)
    except Exception: p = pd.DataFrame()
    if not n.empty: n["code"] = n["code"].map(_code)
    return n, p


def build_shadow_brief(candidate_df: pd.DataFrame, output_dir: str | Path = "reports", now: Optional[pd.Timestamp] = None, force=False) -> str:
    out = _outdir(output_dir)
    t = pd.Timestamp(now or datetime.now())
    if not force and str(os.environ.get("FAMILIAR_FORCE_SHADOW", "0")).lower() not in ("1", "true", "yes") and t.strftime("%H:%M") < "14:50":
        return ""
    if candidate_df is None or candidate_df.empty:
        return SHADOW_HEADER + "\n- 현재 최종 후보가 없어 shadow 카드 생략 · 기존 LIVE 순위 영향 없음."
    nprof, pprof = _latest_profiles(out)
    c = candidate_df.copy()
    cc = next((z for z in ["code", "Code", "종목코드"] if z in c.columns), None)
    nc = next((z for z in ["name", "Name", "종목명"] if z in c.columns), None)
    if not cc:
        return SHADOW_HEADER + "\n- 후보 코드 필드가 없어 친숙도 결합 불가 · LIVE 영향 없음."
    c["code"] = c[cc].map(_code); c["name"] = c[nc].astype(str) if nc else c["code"]
    if not nprof.empty: c = c.merge(nprof, on="code", how="left", suffixes=("", "_profile"))
    # Current raw pattern tags; score uses latest locked/descriptive pattern profile only.
    for col in ["N조합", "검색패턴", "저항구름태그", "수박정제태그", "추천단계", "유형", "거래량상태"]:
        if col not in c: c[col] = ""
    c["all_text"] = c[["N조합", "검색패턴", "저항구름태그", "수박정제태그", "추천단계", "유형", "거래량상태"]].fillna("").astype(str).agg(" | ".join, axis=1)
    temp = pd.DataFrame({"all_text": c["all_text"], "pullback_value_ratio": np.nan, "market5": np.nan, "ret1": np.nan, "HAM_RESTART_CLOSE_CANDIDATE": 0})
    temp = _tag_patterns(temp)
    c["pattern_tokens_now"] = temp["pattern_tokens"]
    pmap = dict(zip(pprof.get("pattern_token", []), pd.to_numeric(pprof.get("familiar_pattern_score", []), errors="coerce"))) if not pprof.empty else {}
    # Convert descriptive pattern profile to conservative 50-centred score; causal score remains backtest-only.
    c["familiar_pattern_score_shadow"] = c["pattern_tokens_now"].map(lambda xs: float(np.nanmean([pmap.get(x,50) for x in xs])) if xs else 50.0)
    ham = pd.DataFrame()
    try:
        h = pd.read_csv(out / "v72_ham_intraday_feature_ledger.csv", dtype={"code": str}, low_memory=False)
        h["code"] = h["code"].map(_code); h["trade_date"] = pd.to_datetime(h["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        ham = h[h["trade_date"] == t.strftime("%Y-%m-%d")].drop_duplicates("code", keep="last")
    except Exception: pass
    if not ham.empty:
        keep = [z for z in ["code", "HAM_MAX_RATIO", "PULLBACK_VALUE_RATIO_TO_HAM", "CLOSE1503_FROM_DAY_HIGH", "CLOSE1503_RESTART_ALIVE"] if z in ham]
        c = c.merge(ham[keep], on="code", how="left")
    rows = []
    for _, r in c.head(10).iterrows():
        ns = _num(r.get("familiar_name_score"), 50); ps = _num(r.get("familiar_pattern_score_shadow"), 50)
        over = _num(r.get("overheat_penalty"), 0); losses = int(_num(r.get("consecutive_losses"), 0))
        judgment = "우선 후보(연구)" if ns >= 65 and ps >= 60 and over <= 4 else ("제외 관찰(연구)" if over >= 8 or losses >= 3 else "관찰(연구)")
        market_fit=str(_first_existing(r,["시장적합도","market_fit","시장환경"],"UNKNOWN"))
        sync=_num(r.get("theme_sync_rate")); sector_persist="강" if math.isfinite(sync) and sync>=60 else ("보통" if math.isfinite(sync) and sync>=40 else ("약" if math.isfinite(sync) else "UNKNOWN"))
        tr=_num(_first_existing(r,["theme_rank","테마순위","sector_rank"],np.nan)); theme_role="대장" if tr==1 else ("2등" if tr==2 else ("후발" if math.isfinite(tr) else "UNKNOWN"))
        pvr = _num(r.get("PULLBACK_VALUE_RATIO_TO_HAM"))
        dry = f"{max(0, (1-pvr)*100):.0f}% 감소" if math.isfinite(pvr) else "UNKNOWN"
        hratio = _num(r.get("HAM_MAX_RATIO")); htxt = f"{hratio:.1f}배" if math.isfinite(hratio) else "WARMUP/UNKNOWN"
        dh = _num(r.get("CLOSE1503_FROM_DAY_HIGH")); dht = f"{dh:+.1f}%" if math.isfinite(dh) else "UNKNOWN"
        rows.append({
            "signal_date": t.strftime("%Y-%m-%d"), "code": r["code"], "name": r["name"], "theme": r.get("theme", r.get("theme_profile", "UNKNOWN")),
            "appear_20d": int(_num(r.get("candidate_count_20d_observed"), 0)), "appear_5d": int(_num(r.get("candidate_count_5d_observed"), 0)),
            "plus3_rate": _num(r.get("plus3_rate")), "stop_rate": _num(r.get("stop_rate")), "familiar_name_score": ns,
            "familiar_pattern_score": ps, "ham_ratio": hratio, "pullback_dry_text": dry, "close1503_from_high": dh, "research_judgment": judgment,
            "market_fit":market_fit,"sector_persistence":sector_persist,"theme_role":theme_role,
        })
    sdf = pd.DataFrame(rows); sdf.to_csv(out / SHADOW_LEDGER, index=False, encoding="utf-8-sig")
    lines = [SHADOW_HEADER, f"📌 {VERSION} · 표시만 추가 · 기존 후보점수/순위/매수판정 변경 0", "- 체결강도·호가소진·종가직전 매수비중은 틱/호가 원장 미연결 시 UNKNOWN이며 점수에 넣지 않습니다."]
    for _, r in sdf.iterrows():
        lines.append(f"- {r['name']}({r['code']}) | 테마 {r['theme'] or 'UNKNOWN'} | 20일 {int(r['appear_20d'])}회/5일 {int(r['appear_5d'])}회 | +3 {_num(r['plus3_rate']):.1f}%/SL {_num(r['stop_rate']):.1f}% | 거래대금 {r['ham_ratio']:.1f}x" if math.isfinite(_num(r['ham_ratio'])) else f"- {r['name']}({r['code']}) | 테마 {r['theme'] or 'UNKNOWN'} | 20일 {int(r['appear_20d'])}회/5일 {int(r['appear_5d'])}회 | +3 {_num(r['plus3_rate']):.1f}%/SL {_num(r['stop_rate']):.1f}% | 거래대금 WARMUP")
        lines.append(f"  ↳ 눌림대금 {r['pullback_dry_text']} · 고가대비 {r['close1503_from_high']:+.1f}%" if math.isfinite(_num(r['close1503_from_high'])) else f"  ↳ 눌림대금 {r['pullback_dry_text']} · 고가대비 UNKNOWN")
        lines.append(f"  ↳ 시장 {r['market_fit']} · 섹터지속 {r['sector_persistence']} · 테마위치 {r['theme_role']}")
        lines.append(f"  ↳ 절친종목 {r['familiar_name_score']:.0f} · 절친패턴 {r['familiar_pattern_score']:.0f} · {r['research_judgment']}")
    lines.append(f"- Actions CSV: {SHADOW_LEDGER} · {NAME_PROFILE} · {PATTERN_PROFILE}")
    return "\n".join(lines)

