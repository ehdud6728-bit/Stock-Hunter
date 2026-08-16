from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6.2"
FACTOR_NAME = "PATTERN_COMBINATION_SCORE_AI_PICK_CROSS_AUDIT"
RESEARCH_ONLY = True
REPORT_HEADER = "🧬 [검색패턴 조합 × 점수 × AI Pick 교차감사 · RESEARCH_ONLY]"
SHADOW_HEADER = "🧬 [장마감 후보 검색패턴 중복 × 점수 × AI Pick SHADOW · 순위영향 없음]"

ROW_AUDIT = "v72_pattern_ai_cross_signal_audit.csv"
PATTERN_SUMMARY = "v72_pattern_ai_cross_pattern_summary.csv"
COMBO_SUMMARY = "v72_pattern_ai_cross_exact_combo_summary.csv"
OVERLAP_SUMMARY = "v72_pattern_ai_cross_overlap_degree_summary.csv"
SCORE_SUMMARY = "v72_pattern_ai_cross_score_bucket_summary.csv"
AI_SUMMARY = "v72_pattern_ai_cross_ai_pick_summary.csv"
CROSS_SUMMARY = "v72_pattern_ai_cross_full_interaction_summary.csv"
DEDUP_SUMMARY = "v72_pattern_ai_cross_code_dedup_summary.csv"
TRAIN_OOS_SUMMARY = "v72_pattern_ai_cross_train_oos_summary.csv"
POLICY_FILE = "v72_pattern_ai_cross_policy_lock.json"
POLICY_AUDIT = "v72_pattern_ai_cross_policy_lock_audit.csv"
POLICY_SWEEP = "v72_pattern_ai_cross_policy_train_sweep.csv"
LOCKED_OOS = "v72_pattern_ai_cross_locked_oos_summary.csv"
BOOTSTRAP_SUMMARY = "v72_pattern_ai_cross_cluster_bootstrap.csv"
CONCENTRATION_SUMMARY = "v72_pattern_ai_cross_concentration_summary.csv"
EXECUTION_SUMMARY = "v72_pattern_ai_cross_execution_cost_summary.csv"
DATA_AUDIT = "v72_pattern_ai_cross_data_availability_audit.csv"
LATE_WAVE_SUMMARY = "v72_pattern_ai_cross_late_wave_diagnostic.csv"
AI_PROVENANCE_AUDIT = "v72_pattern_ai_cross_ai_provenance_audit.csv"
SHADOW_LEDGER = "v72_pattern_ai_cross_1503_shadow.csv"
REPORT_BLOCK_FILE = "v72_pattern_ai_cross_report_block.txt"

PATTERNS = [
    "VALUE_BB40_RECOVERY",
    "TRIANGLE_WAVE1_FIRST_PULLBACK",
    "PRE_TRIANGLE",
    "BB_COMPRESSION",
    "MA5_PULLBACK_RECLAIM",
    "BLUE_DOTTED_RECLAIM",
]

PATTERN_KO = {
    "VALUE_BB40_RECOVERY": "저평가/BB40 회복",
    "TRIANGLE_WAVE1_FIRST_PULLBACK": "삼각수렴 1파 첫눌림",
    "PRE_TRIANGLE": "예비삼각수렴",
    "BB_COMPRESSION": "BB응축",
    "MA5_PULLBACK_RECLAIM": "5일선 눌림·재안착",
    "BLUE_DOTTED_RECLAIM": "파란점선 재지지",
}

TRUE_AI_LABELS = {
    "🤖 AI Pick": "AI_STRONG",
    "🤖 AI 관찰 Pick": "AI_WATCH",
    "🛡 보수적 AI Pick": "AI_CONSERVATIVE",
}


def _outdir(output_dir: str | Path = "reports") -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _code(v: Any) -> str:
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

def _num(v, default=np.nan) -> float:
    try:
        x = float(str(v).replace(",", "").replace("%", "").strip())
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _int(v, default=0) -> int:
    x = _num(v)
    return int(x) if math.isfinite(x) else int(default)


def _fmt(v, digits=2, sign=True, suffix="") -> str:
    x = _num(v)
    if not math.isfinite(x):
        return "N/A"
    return format(x, ("+" if sign else "") + f".{digits}f") + suffix


def _first(row, names: Sequence[str], default=""):
    for n in names:
        try:
            v = row.get(n, default)
            if pd.notna(v) and str(v).strip() not in ("", "nan", "None", "NaT"):
                return v
        except Exception:
            pass
    return default


def _str_series(df: pd.DataFrame, names: Sequence[str], default="") -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n].fillna(default).astype(str)
    return pd.Series(default, index=df.index, dtype=str)


def _num_series(df: pd.DataFrame, names: Sequence[str], default=np.nan) -> pd.Series:
    for n in names:
        if n in df.columns:
            return pd.to_numeric(df[n], errors="coerce")
    return pd.Series(default, index=df.index, dtype=float)


def _truthy(v) -> bool:
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if isinstance(v, (int, float)) and not pd.isna(v):
        return float(v) != 0
    s = str(v or "").strip().lower()
    return s in {"1", "true", "yes", "y", "on", "통과", "확인", "접촉", "위안착", "재지지", "reclaim", "hold"}


def _row_blob(row) -> str:
    fields = [
        "source", "strategy", "pattern", "search_pattern_primary", "search_pattern_matches",
        "search_pattern_tags", "structure_pattern", "final_decision", "phase", "volume_state",
        "N조합", "검색패턴", "검색식대표", "검색식매칭", "검색식태그", "저항구름태그",
        "수박정제태그", "수박최종상태", "추천단계", "유형", "파란점선상태", "파동타점상태",
        "v7337_candidate_groups", "v7337_aux_combo", "pattern_source_raw",
    ]
    return " | ".join(str(_first(row, [f], "")) for f in fields).lower()


def _explicit_pattern_evidence(row) -> Tuple[List[str], Dict[str, str], Dict[str, str]]:
    """Return tokens, evidence and evidence quality without pretending weak text is native selector state."""
    blob = _row_blob(row)
    groups = set(x.strip() for x in str(_first(row, ["v7337_candidate_groups"], "")).split("|") if x.strip())
    tokens: List[str] = []
    evidence: Dict[str, str] = {}
    quality: Dict[str, str] = {}

    def add(tok: str, ev: str, q: str):
        if tok not in tokens:
            tokens.append(tok)
            evidence[tok] = ev
            quality[tok] = q

    # Native AUX memberships are the strongest historical provenance currently available.
    if "VALUE_WAVE_BB40" in groups:
        add("VALUE_BB40_RECOVERY", "AUX_GROUP:VALUE_WAVE_BB40", "NATIVE_GROUP")
    if "TRIANGLE_PULLBACK" in groups:
        add("TRIANGLE_WAVE1_FIRST_PULLBACK", "AUX_GROUP:TRIANGLE_PULLBACK", "NATIVE_GROUP")
    if "PRE_TRIANGLE" in groups:
        add("PRE_TRIANGLE", "AUX_GROUP:PRE_TRIANGLE", "NATIVE_GROUP")

    # Explicit research capture fields written by capture_signal_provenance.
    explicit = {
        "VALUE_BB40_RECOVERY": ["cross_value_bb40_recovery", "저평가1파BB40", "ymgp_bb40_hold", "value_wave_bb40"],
        "TRIANGLE_WAVE1_FIRST_PULLBACK": ["cross_triangle_wave1_first_pullback", "triangle_wave1_first_pullback", "triangle_pullback"],
        "PRE_TRIANGLE": ["cross_pre_triangle", "pre_triangle", "triangle_pre_squeeze"],
        "BB_COMPRESSION": ["cross_bb_compression", "bb_compression", "bb_squeeze", "볼린저응축"],
        "MA5_PULLBACK_RECLAIM": ["cross_ma5_pullback_reclaim", "ma5_pullback", "ma5_reanchor", "5일선재안착"],
        "BLUE_DOTTED_RECLAIM": ["cross_blue_dotted_reclaim", "blue_line_reclaim", "수박파란점선", "파란점선1단기", "파란점선2스윙"],
    }
    for tok, cols in explicit.items():
        for c in cols:
            try:
                if c in row and _truthy(row.get(c)):
                    add(tok, f"EXPLICIT_FIELD:{c}={row.get(c)}", "EXPLICIT_FIELD")
                    break
            except Exception:
                pass

    # State values are stronger than free text but weaker than direct boolean selectors.
    blue_state = str(_first(row, ["cross_blue_state", "파란점선상태", "blue_line_state"], "")).lower()
    if any(k in blue_state for k in ["접촉", "위안착", "재지지", "reclaim", "hold"]):
        add("BLUE_DOTTED_RECLAIM", f"STATE:blue={blue_state}", "STATE_FIELD")
    ma5_state = str(_first(row, ["cross_ma5_state", "5일선재안착상태", "ma5_reanchor_state"], "")).lower()
    if any(k in ma5_state for k in ["재안착", "눌림", "reclaim", "hold"]):
        add("MA5_PULLBACK_RECLAIM", f"STATE:ma5={ma5_state}", "STATE_FIELD")

    # Controlled text fallback. It is explicitly labelled and never called a native selector anchor.
    if not any(t == "VALUE_BB40_RECOVERY" for t in tokens) and any(k in blob for k in ["value_wave_bb40", "저평가1파", "저평가 1파", "bb40 회복", "bb40_recovery"]):
        add("VALUE_BB40_RECOVERY", "TEXT_FALLBACK", "TEXT_FALLBACK")
    if not any(t == "TRIANGLE_WAVE1_FIRST_PULLBACK" for t in tokens) and (
        "triangle_pullback" in blob or ("삼각" in blob and ("첫눌림" in blob or "첫 눌림" in blob or "1파" in blob))
    ):
        add("TRIANGLE_WAVE1_FIRST_PULLBACK", "TEXT_FALLBACK", "TEXT_FALLBACK")
    if not any(t == "PRE_TRIANGLE" for t in tokens) and any(k in blob for k in ["pre_triangle", "예비삼각", "예비 삼각"]):
        add("PRE_TRIANGLE", "TEXT_FALLBACK", "TEXT_FALLBACK")
    if not any(t == "BB_COMPRESSION" for t in tokens) and any(k in blob for k in ["bb응축", "bb 응축", "볼린저 응축", "bb compression", "bb_compression"]):
        add("BB_COMPRESSION", "TEXT_FALLBACK", "TEXT_FALLBACK")
    if not any(t == "MA5_PULLBACK_RECLAIM" for t in tokens) and any(k in blob for k in ["5일선 눌림", "5일선 재안착", "ma5 pullback", "ma5_reanchor"]):
        add("MA5_PULLBACK_RECLAIM", "TEXT_FALLBACK", "TEXT_FALLBACK")
    if not any(t == "BLUE_DOTTED_RECLAIM" for t in tokens) and any(k in blob for k in ["파란점선 재지지", "파란점선 위안착", "파란점선 접촉", "blue line reclaim", "blue_line_reclaim"]):
        add("BLUE_DOTTED_RECLAIM", "TEXT_FALLBACK", "TEXT_FALLBACK")

    return tokens, evidence, quality


def capture_signal_provenance(row, record: Optional[dict] = None) -> dict:
    """Add research-only source evidence to a signal record. Never changes score, rank or decision."""
    rec = dict(record or {})
    try:
        d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    except Exception:
        d = {}
    merged = dict(d)
    merged.update(rec)
    tokens, evidence, quality = _explicit_pattern_evidence(merged)
    rec.update({
        "pattern_cross_version": VERSION,
        "pattern_source_raw": " | ".join(f"{k}:{evidence.get(k, '')}" for k in tokens),
        "pattern_source_quality": " | ".join(f"{k}:{quality.get(k, '')}" for k in tokens),
        "pattern_exact_combo": " + ".join(tokens) if tokens else "UNCLASSIFIED",
        "pattern_overlap_count": len(tokens),
        "cross_value_bb40_recovery": int("VALUE_BB40_RECOVERY" in tokens),
        "cross_triangle_wave1_first_pullback": int("TRIANGLE_WAVE1_FIRST_PULLBACK" in tokens),
        "cross_pre_triangle": int("PRE_TRIANGLE" in tokens),
        "cross_bb_compression": int("BB_COMPRESSION" in tokens),
        "cross_ma5_pullback_reclaim": int("MA5_PULLBACK_RECLAIM" in tokens),
        "cross_blue_dotted_reclaim": int("BLUE_DOTTED_RECLAIM" in tokens),
        "cross_ma5_state": str(_first(merged, ["5일선재안착상태", "ma5_reanchor_state", "5일선재안착", "ma5_reanchor"], "")),
        "cross_blue_state": str(_first(merged, ["파란점선상태", "blue_line_state", "수박최종상태"], "")),
        "cross_top15_rank": _int(_first(merged, ["rank", "순위", "top15_rank"], 0)),
        "cross_n_score": _num(_first(merged, ["N점수", "n_score"], np.nan)),
        "cross_safe_score": _num(_first(merged, ["안전점수", "safe_score"], np.nan)),
    })
    return rec


def _load_aux_eval(out: Path) -> pd.DataFrame:
    fp = out / "v72_aux_candidate_group_shadow_eval.csv"
    try:
        if fp.exists() and fp.stat().st_size > 0:
            d = pd.read_csv(fp, dtype={"code": str}, low_memory=False)
            if not d.empty:
                return d
    except Exception:
        pass
    return pd.DataFrame()


def _load_true_ai_ledger(out: Path) -> pd.DataFrame:
    fps: List[Path] = []
    env = str(os.environ.get("V1080_SIGNAL_CSV", "")).strip()
    if env:
        fps.append(Path(env))
    fps.extend([out / "v1080_stockhunter_signals.csv", Path("reports/v1080_stockhunter_signals.csv")])
    rows = []
    seen = set()
    for fp in fps:
        try:
            key = str(fp.resolve())
        except Exception:
            key = str(fp)
        if key in seen:
            continue
        seen.add(key)
        try:
            if not fp.exists() or fp.stat().st_size == 0:
                continue
            d = pd.read_csv(fp, dtype={"code": str}, low_memory=False)
            dc = next((c for c in ["signal_date", "date", "신호일"] if c in d.columns), None)
            cc = next((c for c in ["code", "Code", "종목코드"] if c in d.columns), None)
            ac = next((c for c in ["ai_pick_tier", "AI_PICK", "ai_pick"] if c in d.columns), None)
            if not dc or not cc or not ac:
                continue
            z = pd.DataFrame({
                "signal_date": pd.to_datetime(d[dc], errors="coerce").dt.normalize(),
                "code": d[cc].map(_code),
                "ai_pick_tier_recorded": d[ac].fillna("").astype(str),
                "ai_pick_reason_recorded": d.get("ai_pick_reason", pd.Series("", index=d.index)).fillna("").astype(str),
                "ai_recorded_at": d.get("recorded_at", pd.Series("", index=d.index)).fillna("").astype(str),
                "ai_ledger_source": fp.name,
            })
            z = z[z["signal_date"].notna() & z["code"].ne("")]
            # Historical direct replay placeholder is not an AI observation.
            z = z[~z["ai_pick_tier_recorded"].str.contains("과거 AI 미실행|직접검증", na=False)]
            rows.append(z)
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["signal_date", "code", "ai_pick_tier_recorded", "ai_pick_reason_recorded", "ai_ledger_source"])
    z = pd.concat(rows, ignore_index=True, sort=False)
    return z.drop_duplicates(["signal_date", "code"], keep="last")


def _merge_aux_and_main(eval_df: pd.DataFrame, out: Path) -> pd.DataFrame:
    aux = _load_aux_eval(out)
    if aux.empty:
        return eval_df.copy() if eval_df is not None else pd.DataFrame()
    # AUX is the requested candidate-group universe. Main rows enrich actual TOP15 rank/AI when the same key exists.
    x = aux.copy()
    if eval_df is not None and not eval_df.empty:
        m = eval_df.copy()
        for df in (x, m):
            df["signal_date"] = pd.to_datetime(_str_series(df, ["signal_date", "신호일", "date"]), errors="coerce").dt.normalize()
            df["code"] = _str_series(df, ["code", "Code", "종목코드"]).map(_code)
        enrich_cols = [c for c in [
            "signal_date", "code", "rank", "safe_score", "n_score", "ai_pick_tier", "ai_pick_reason", "source",
            "distance_from_60d_low_pct", "low60_distance_pct", "upper_resistance_distance_pct", "upper_space_pct",
            "failure_reasons", "reason_tags", "attribution_tags", "theme", "sector", "market_regime",
        ] if c in m.columns]
        if {"signal_date", "code"}.issubset(enrich_cols):
            mm = m[enrich_cols].drop_duplicates(["signal_date", "code"], keep="last")
            x = x.merge(mm, on=["signal_date", "code"], how="left", suffixes=("", "_main"))
    return x


def _normalise(eval_df: pd.DataFrame, out: Path) -> pd.DataFrame:
    x = _merge_aux_and_main(eval_df, out)
    if x is None or x.empty:
        return pd.DataFrame()
    x = x.copy()
    x["signal_date"] = pd.to_datetime(_str_series(x, ["signal_date", "신호일", "date"]), errors="coerce").dt.normalize()
    x["code"] = _str_series(x, ["code", "Code", "종목코드"]).map(_code)
    x["name"] = _str_series(x, ["name", "Name", "종목명"])
    x["eval_status"] = _str_series(x, ["eval_status"], "OK")
    x = x[x["signal_date"].notna() & x["code"].ne("") & x["eval_status"].eq("OK")].copy()
    if x.empty:
        return x
    # one PnL row per date/code; exact membership remains a list, so no duplicate PnL inflation.
    x = x.sort_values(["signal_date", "code"]).drop_duplicates(["signal_date", "code"], keep="last")
    x["ret1"] = _num_series(x, ["next1_close_ret", "day1_ret", "ret1"])
    x["ret3"] = _num_series(x, ["next3_close_ret", "day3_ret", "ret3"])
    x["ret5"] = _num_series(x, ["next5_close_ret", "day5_ret", "ret5"])
    x["plus3"] = _num_series(x, ["hit_plus3_first", "plus3_first_10d", "plus3_first", "plus3_hit"], 0).fillna(0).clip(0, 1)
    x["stop_first"] = _num_series(x, ["hit_stop_first", "stop_first_10d", "stop_first", "minus3_first"], 0).fillna(0).clip(0, 1)
    x["mfe"] = _num_series(x, ["max_up_5d", "MFE_5D", "mfe"])
    x["mae"] = _num_series(x, ["max_down_5d", "MAE_5D", "mae"])
    x["rank"] = _num_series(x, ["cross_top15_rank", "rank", "순위"], 999).fillna(999)
    x["n_score"] = _num_series(x, ["cross_n_score", "n_score", "N점수"])
    x["safe_score"] = _num_series(x, ["cross_safe_score", "safe_score", "안전점수"])
    # exact raw score; no within-date percentile is described as the original score.
    x["score_axis"] = x["n_score"].where(x["n_score"].notna(), x["safe_score"])
    x["score_source"] = np.where(x["n_score"].notna(), "N_SCORE", np.where(x["safe_score"].notna(), "SAFE_SCORE", "MISSING"))
    x["score_bucket"] = pd.cut(x["score_axis"], [-np.inf, 69.999, 79.999, 89.999, np.inf], labels=["LT70", "70_79", "80_89", "GE90"]).astype(str)
    x.loc[x["score_axis"].isna(), "score_bucket"] = "MISSING"
    x["theme"] = _str_series(x, ["theme", "Theme", "테마", "sector", "Sector", "섹터"], "UNKNOWN").replace({"": "UNKNOWN", "nan": "UNKNOWN"})
    x["market_regime"] = _str_series(x, ["market_regime", "global_regime", "market_state", "시장국면"], "UNKNOWN").replace({"": "UNKNOWN"})
    x["distance_low60"] = _num_series(x, ["distance_from_60d_low_pct", "low60_distance_pct", "60일저점이격"])
    x["upper_space"] = _num_series(x, ["upper_resistance_distance_pct", "upper_space_pct", "상단저항거리"])
    reason = _str_series(x, ["failure_reasons", "reason_tags", "attribution_tags", "v73363_reason_tags"])
    x["late_wave"] = reason.str.contains("LATE_WAVE", case=False, na=False).astype(int)

    pat = x.apply(_explicit_pattern_evidence, axis=1)
    x["pattern_tokens"] = pat.map(lambda z: z[0])
    x["pattern_evidence"] = pat.map(lambda z: " | ".join(f"{k}:{v}" for k, v in z[1].items()))
    x["pattern_evidence_quality"] = pat.map(lambda z: " | ".join(f"{k}:{v}" for k, v in z[2].items()))
    x["pattern_combo"] = x["pattern_tokens"].map(lambda z: " + ".join(z) if z else "UNCLASSIFIED")
    x["pattern_overlap_count"] = x["pattern_tokens"].map(len)
    x["overlap_degree"] = x["pattern_overlap_count"].map(lambda n: "0_PATTERN" if n == 0 else ("1_PATTERN" if n == 1 else ("2_PATTERN" if n == 2 else "3PLUS_PATTERN")))

    # True historical AI observations are merged only from recorded live ledgers.
    ai = _load_true_ai_ledger(out)
    if not ai.empty:
        x = x.merge(ai, on=["signal_date", "code"], how="left")
    else:
        x["ai_pick_tier_recorded"] = ""
        x["ai_pick_reason_recorded"] = ""
        x["ai_ledger_source"] = ""
    # Causal AI comparison is ledger-only. A value merely present in the replay/eval
    # DataFrame is display metadata, not proof that AI actually ran on that historical date.
    local_ai = _str_series(x, ["ai_pick_tier", "ai_pick_tier_main"], "")
    recorded = x["ai_pick_tier_recorded"].fillna("").astype(str)
    ledger_source = x["ai_ledger_source"].fillna("").astype(str)
    chosen = recorded.copy()
    unavailable = (
        chosen.str.contains("과거 AI 미실행|직접검증", na=False)
        | chosen.str.strip().eq("")
        | ledger_source.str.strip().eq("")
    )
    x["ai_pick_label"] = "AI_NONE"
    for raw, lab in TRUE_AI_LABELS.items():
        x.loc[chosen.eq(raw), "ai_pick_label"] = lab
    x.loc[chosen.str.contains("관찰", na=False), "ai_pick_label"] = "AI_WATCH"
    x.loc[chosen.str.contains("보수", na=False), "ai_pick_label"] = "AI_CONSERVATIVE"
    x.loc[chosen.str.contains("AI Pick", na=False) & ~chosen.str.contains("없음|관찰|보수", na=False), "ai_pick_label"] = "AI_STRONG"
    x.loc[chosen.str.contains("없음", na=False), "ai_pick_label"] = "AI_NONE"
    x.loc[unavailable, "ai_pick_label"] = "AI_UNAVAILABLE_HISTORICAL"
    x["ai_observed"] = x["ai_pick_label"].ne("AI_UNAVAILABLE_HISTORICAL").astype(int)
    x["ai_selected"] = x["ai_pick_label"].isin(["AI_STRONG", "AI_WATCH", "AI_CONSERVATIVE"]).astype(int)
    x["ai_pick_tier_raw"] = chosen
    x["ai_pick_tier_local_untrusted"] = local_ai

    # Repeat/high-location diagnostic: use causal familiar audit when available.
    ffp = out / "v72_familiar_signal_causal_audit.csv"
    try:
        if ffp.exists() and ffp.stat().st_size > 0:
            f = pd.read_csv(ffp, dtype={"code": str}, low_memory=False)
            f["signal_date"] = pd.to_datetime(f["signal_date"], errors="coerce").dt.normalize()
            f["code"] = f["code"].map(_code)
            keep = [c for c in ["signal_date", "code", "name_overheat_penalty", "name_overheat_reason", "familiar_name_score"] if c in f.columns]
            f = f[keep].drop_duplicates(["signal_date", "code"], keep="last")
            x = x.merge(f, on=["signal_date", "code"], how="left")
    except Exception:
        pass
    x["overheat_penalty"] = _num_series(x, ["name_overheat_penalty", "overheat_penalty"], 0).fillna(0)

    # Actual execution/PAPER PnL, if present. Never fabricate it from close returns.
    x["execution_pnl"] = np.nan
    for fp in [out / "v73_execution_bridge.csv", out / "v73_paper_forward_ledger.csv", out / "paper_forward_ledger.csv"]:
        try:
            if not fp.exists() or fp.stat().st_size == 0:
                continue
            e = pd.read_csv(fp, dtype=str, low_memory=False)
            cc = next((c for c in ["code", "Code", "종목코드"] if c in e.columns), None)
            dc = next((c for c in ["signal_date", "trade_date", "entry_date", "date"] if c in e.columns), None)
            pc = next((c for c in ["execution_pnl_pct", "actual_pnl_pct", "paper_pnl_pct", "pnl_pct"] if c in e.columns), None)
            if cc and dc and pc:
                z = pd.DataFrame({"signal_date": pd.to_datetime(e[dc], errors="coerce").dt.normalize(), "code": e[cc].map(_code), "execution_pnl_join": pd.to_numeric(e[pc], errors="coerce")}).dropna(subset=["signal_date", "code", "execution_pnl_join"])
                z = z.drop_duplicates(["signal_date", "code"], keep="last")
                x = x.merge(z, on=["signal_date", "code"], how="left")
                x["execution_pnl"] = x["execution_pnl"].fillna(x["execution_pnl_join"])
                x = x.drop(columns=["execution_pnl_join"], errors="ignore")
        except Exception:
            continue
    return x.sort_values(["signal_date", "rank", "code"]).reset_index(drop=True)


def _trim_mean(s: pd.Series, p=0.1) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    if x.empty:
        return np.nan
    k = int(len(x) * p)
    if k and len(x) > 2 * k:
        x = x.iloc[k:-k]
    return float(x.mean())


def _top2_excl(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values(ascending=False)
    return float(x.iloc[2:].mean()) if len(x) > 2 else np.nan


def _perf(g: pd.DataFrame, label: str, dimension: str, bucket="FULL") -> dict:
    if g is None:
        g = pd.DataFrame()
    ret1 = pd.to_numeric(g.get("ret1", pd.Series(dtype=float)), errors="coerce")
    ret3 = pd.to_numeric(g.get("ret3", pd.Series(dtype=float)), errors="coerce")
    ret5 = pd.to_numeric(g.get("ret5", pd.Series(dtype=float)), errors="coerce")
    prof = ret3[ret3 > 0]
    top2 = prof.nlargest(2).sum() if len(prof) else 0
    profit_sum = prof.sum() if len(prof) else 0
    # one result per code: earliest signal in the bucket, deliberately conservative against repeat-name inflation.
    ded = g.sort_values("signal_date").drop_duplicates("code", keep="first") if len(g) and "code" in g else pd.DataFrame()
    return {
        "dimension": dimension,
        "label": label,
        "bucket": bucket,
        "n": len(g),
        "unique_codes": g["code"].nunique() if len(g) and "code" in g else 0,
        "signal_days": g["signal_date"].nunique() if len(g) and "signal_date" in g else 0,
        "themes": g["theme"].nunique() if len(g) and "theme" in g else 0,
        "ret1_mean": ret1.mean(),
        "ret1_median": ret1.median(),
        "ret3_mean": ret3.mean(),
        "ret3_median": ret3.median(),
        "ret3_trim10": _trim_mean(ret3),
        "ret3_top2_excl": _top2_excl(ret3),
        "ret5_mean": ret5.mean(),
        "ret5_median": ret5.median(),
        "plus3_rate": pd.to_numeric(g.get("plus3", pd.Series(dtype=float)), errors="coerce").mean() * 100 if len(g) else np.nan,
        "stop_rate": pd.to_numeric(g.get("stop_first", pd.Series(dtype=float)), errors="coerce").mean() * 100 if len(g) else np.nan,
        "mfe_mean": pd.to_numeric(g.get("mfe", pd.Series(dtype=float)), errors="coerce").mean(),
        "mae_mean": pd.to_numeric(g.get("mae", pd.Series(dtype=float)), errors="coerce").mean(),
        "cost20_ret3": (ret3 - 0.20).mean(),
        "cost50_ret3": (ret3 - 0.50).mean(),
        "code_dedup_ret3": pd.to_numeric(ded.get("ret3", pd.Series(dtype=float)), errors="coerce").mean() if len(ded) else np.nan,
        "top2_profit_concentration": top2 / profit_sum * 100 if profit_sum > 0 else np.nan,
        "top_code_share_pct": g["code"].value_counts(normalize=True).iloc[0] * 100 if len(g) and "code" in g else np.nan,
        "top_theme_share_pct": g["theme"].value_counts(normalize=True).iloc[0] * 100 if len(g) and "theme" in g else np.nan,
        "late_wave_rate": pd.to_numeric(g.get("late_wave", pd.Series(dtype=float)), errors="coerce").mean() * 100 if len(g) else np.nan,
        "low60_distance_median": pd.to_numeric(g.get("distance_low60", pd.Series(dtype=float)), errors="coerce").median(),
        "upper_space_median": pd.to_numeric(g.get("upper_space", pd.Series(dtype=float)), errors="coerce").median(),
        "overheat_penalty_mean": pd.to_numeric(g.get("overheat_penalty", pd.Series(dtype=float)), errors="coerce").mean(),
        "ai_observed_rate": pd.to_numeric(g.get("ai_observed", pd.Series(dtype=float)), errors="coerce").mean() * 100 if len(g) else np.nan,
        "execution_n": pd.to_numeric(g.get("execution_pnl", pd.Series(dtype=float)), errors="coerce").notna().sum(),
        "execution_mean": pd.to_numeric(g.get("execution_pnl", pd.Series(dtype=float)), errors="coerce").mean(),
    }


def _exploded(x: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in x.iterrows():
        for tok in r["pattern_tokens"]:
            d = r.to_dict(); d["pattern_token"] = tok; d["pattern_name"] = PATTERN_KO.get(tok, tok); rows.append(d)
    return pd.DataFrame(rows)


def _summary_tables(x: pd.DataFrame, out: Path, train_end: Optional[pd.Timestamp] = None, oos_start: Optional[pd.Timestamp] = None) -> Dict[str, pd.DataFrame]:
    ex = _exploded(x)
    pattern_rows = [_perf(g, PATTERN_KO.get(tok, tok), "PATTERN") | {"pattern_token": tok} for tok, g in ex.groupby("pattern_token")] if not ex.empty else []
    combo_rows = [_perf(g, str(combo), "EXACT_COMBO") for combo, g in x.groupby("pattern_combo") if str(combo) != "UNCLASSIFIED"]
    overlap_rows = [_perf(g, str(deg), "OVERLAP") for deg, g in x.groupby("overlap_degree")]
    score_rows = [_perf(g, str(b), "SCORE_BUCKET") for b, g in x.groupby("score_bucket")]
    ai_rows = [_perf(g, str(a), "AI_PICK") for a, g in x.groupby("ai_pick_label")]
    cross_rows = []
    for (combo, score, ai), g in x.groupby(["pattern_combo", "score_bucket", "ai_pick_label"]):
        if combo == "UNCLASSIFIED":
            continue
        z = _perf(g, f"{combo} × {score} × {ai}", "FULL_INTERACTION")
        z.update({"pattern_combo": combo, "score_bucket": score, "ai_pick_label": ai})
        cross_rows.append(z)
    ded_rows = []
    for dim, col in [("PATTERN", "pattern_combo"), ("OVERLAP", "overlap_degree"), ("SCORE", "score_bucket"), ("AI", "ai_pick_label")]:
        for lab, g in x.groupby(col):
            q = g.sort_values("signal_date").drop_duplicates("code", keep="first")
            ded_rows.append(_perf(q, str(lab), f"{dim}_CODE_DEDUP"))
    train_rows = []
    if train_end is not None:
        buckets = {"TRAIN": x[x["signal_date"] <= train_end]}
        if oos_start is not None:
            buckets["OOS"] = x[x["signal_date"] >= oos_start]
        for b, q in buckets.items():
            for combo, g in q.groupby("pattern_combo"):
                if combo == "UNCLASSIFIED": continue
                train_rows.append(_perf(g, str(combo), "EXACT_COMBO", b))
            for ai, g in q.groupby("ai_pick_label"):
                train_rows.append(_perf(g, str(ai), "AI_PICK", b))
    tables = {
        PATTERN_SUMMARY: pd.DataFrame(pattern_rows),
        COMBO_SUMMARY: pd.DataFrame(combo_rows),
        OVERLAP_SUMMARY: pd.DataFrame(overlap_rows),
        SCORE_SUMMARY: pd.DataFrame(score_rows),
        AI_SUMMARY: pd.DataFrame(ai_rows),
        CROSS_SUMMARY: pd.DataFrame(cross_rows),
        DEDUP_SUMMARY: pd.DataFrame(ded_rows),
        TRAIN_OOS_SUMMARY: pd.DataFrame(train_rows),
    }
    for name, df in tables.items():
        df.to_csv(out / name, index=False, encoding="utf-8-sig")
    return tables


@dataclass
class Policy:
    policy_id: str
    train_start: str
    train_end: str
    oos_start: str
    min_score_bucket: str
    ai_mode: str
    pattern_rule: str
    min_overlap: int
    max_overlap: int
    created_at: str
    policy_hash: str = ""

    def finish(self):
        payload = asdict(self).copy(); payload.pop("policy_hash", None)
        self.policy_hash = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]
        return self


def _bucket_floor(bucket: str) -> int:
    return {"ANY": -999, "LT70": -999, "70_79": 70, "80_89": 80, "GE90": 90}.get(str(bucket), -999)


def _rule_mask(x: pd.DataFrame, rule: dict) -> pd.Series:
    m = x["pattern_overlap_count"].between(int(rule["min_overlap"]), int(rule["max_overlap"]))
    floor = _bucket_floor(rule["min_score_bucket"])
    if floor > -999:
        m &= x["score_axis"].ge(floor)
    if rule["ai_mode"] == "AI_SELECTED":
        m &= x["ai_selected"].eq(1) & x["ai_observed"].eq(1)
    elif rule["ai_mode"] == "AI_NONE":
        m &= x["ai_pick_label"].eq("AI_NONE") & x["ai_observed"].eq(1)
    elif rule["ai_mode"] == "AI_OBSERVED_ANY":
        m &= x["ai_observed"].eq(1)
    if rule["pattern_rule"] != "ANY":
        # Policy candidates are sourced from exact-combination labels, so selection must
        # remain exact. A 2-pattern rule must not silently absorb 3+ pattern supersets.
        m &= x["pattern_combo"].eq(rule["pattern_rule"])
    return m


def _policy_objective(g: pd.DataFrame) -> float:
    if len(g) < 5 or g["signal_date"].nunique() < 3 or g["code"].nunique() < 4:
        return -1e9
    r3 = pd.to_numeric(g["ret3"], errors="coerce")
    med = r3.median(); trim = _trim_mean(r3); topx = _top2_excl(r3)
    p3 = g["plus3"].mean() * 100; sl = g["stop_first"].mean() * 100
    ded = g.sort_values("signal_date").drop_duplicates("code", keep="first")["ret3"].mean()
    dates = g.groupby("signal_date")["ret3"].mean()
    concentration = g["code"].value_counts(normalize=True).iloc[0] * 100
    vals = [v for v in [med, trim, topx, ded] if math.isfinite(_num(v))]
    robust = np.mean(vals) if vals else -20
    return robust + 0.04 * (p3 - sl) + 0.6 * ((dates > 0).mean() * 100 - 50) / 10 - max(0, concentration - 25) * 0.08


def _train_lock(x: pd.DataFrame, out: Path) -> Tuple[Optional[Policy], str, pd.DataFrame]:
    lock = out / POLICY_FILE
    if lock.exists() and str(os.environ.get("PATTERN_AI_CROSS_FORCE_RELOCK", "0")).lower() not in ("1", "true", "yes"):
        try:
            p = Policy(**json.loads(lock.read_text(encoding="utf-8")))
            return p, "LOCKED_REUSED", pd.DataFrame()
        except Exception:
            pass
    dates = sorted(pd.Timestamp(d) for d in x["signal_date"].dropna().unique())
    min_days = max(8, _int(os.environ.get("PATTERN_AI_CROSS_MIN_LOCK_SIGNAL_DAYS", 10), 10))
    if len(dates) < min_days:
        return None, f"WARMUP_SIGNAL_DAYS_{len(dates)}_LT_{min_days}", pd.DataFrame()
    split = min(max(5, int(len(dates) * 0.70)), len(dates) - 2)
    train_dates = dates[:split]; train = x[x["signal_date"].isin(train_dates)].copy()
    combos = ["ANY"]
    combo_counts = train[train["pattern_combo"].ne("UNCLASSIFIED")]["pattern_combo"].value_counts()
    combos += [c for c, n in combo_counts.items() if n >= 5][:12]
    ai_modes = ["ANY"]
    if train["ai_observed"].sum() >= 8:
        ai_modes += ["AI_OBSERVED_ANY", "AI_SELECTED", "AI_NONE"]
    rules = []
    for score in ["ANY", "70_79", "80_89", "GE90"]:
        for ai_mode in ai_modes:
            for combo in combos:
                for lo, hi in [(1, 1), (2, 2), (1, 2), (1, 99)]:
                    rule = {"min_score_bucket": score, "ai_mode": ai_mode, "pattern_rule": combo, "min_overlap": lo, "max_overlap": hi}
                    g = train[_rule_mask(train, rule)]
                    rules.append(rule | {"n": len(g), "signal_days": g["signal_date"].nunique(), "unique_codes": g["code"].nunique(), "objective": _policy_objective(g), "ret3_median": g["ret3"].median() if len(g) else np.nan, "ret3_trim10": _trim_mean(g["ret3"]) if len(g) else np.nan, "ret3_top2_excl": _top2_excl(g["ret3"]) if len(g) else np.nan})
    sw = pd.DataFrame(rules).sort_values(["objective", "n"], ascending=[False, False])
    sw.to_csv(out / POLICY_SWEEP, index=False, encoding="utf-8-sig")
    best = sw.iloc[0]
    if _num(best["objective"], -1e9) <= -1e8:
        return None, "TRAIN_NO_ELIGIBLE_POLICY", sw
    p = Policy(
        policy_id="PATTERN_AI_CROSS_LOCK_" + pd.Timestamp(train_dates[-1]).strftime("%Y%m%d"),
        train_start=str(train_dates[0].date()), train_end=str(train_dates[-1].date()), oos_start=str(dates[split].date()),
        min_score_bucket=str(best["min_score_bucket"]), ai_mode=str(best["ai_mode"]), pattern_rule=str(best["pattern_rule"]),
        min_overlap=int(best["min_overlap"]), max_overlap=int(best["max_overlap"]), created_at=datetime.now().isoformat(),
    ).finish()
    lock.write_text(json.dumps(asdict(p), ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame([asdict(p)]).to_csv(out / POLICY_AUDIT, index=False, encoding="utf-8-sig")
    return p, "LOCKED_NEW", sw


def _cluster_bootstrap(g: pd.DataFrame, cluster: str, iterations=1000, seed=733661) -> dict:
    if g.empty or cluster not in g:
        return {"cluster": cluster, "clusters": 0, "iterations": 0}
    keys = sorted(g[cluster].dropna().astype(str).unique())
    if len(keys) < 2:
        return {"cluster": cluster, "clusters": len(keys), "iterations": 0}
    z = g.copy(); z[cluster] = z[cluster].astype(str)
    rng = np.random.default_rng(seed + len(keys))
    vals = []
    for _ in range(iterations):
        draw = rng.choice(keys, len(keys), replace=True)
        rr = []
        for k in draw:
            rr.extend(pd.to_numeric(z.loc[z[cluster].eq(k), "ret3"], errors="coerce").dropna().tolist())
        if rr:
            vals.append(float(np.mean(rr)))
    if not vals:
        return {"cluster": cluster, "clusters": len(keys), "iterations": 0}
    a = np.asarray(vals)
    return {"cluster": cluster, "clusters": len(keys), "iterations": len(a), "mean": a.mean(), "ci05": np.quantile(a, .05), "ci95": np.quantile(a, .95), "positive_prob": (a > 0).mean() * 100}


def _policy_outputs(x: pd.DataFrame, p: Optional[Policy], out: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if p is None:
        empty = pd.DataFrame(columns=["bucket", "n", "ret3_mean"])
        empty.to_csv(out / LOCKED_OOS, index=False, encoding="utf-8-sig")
        pd.DataFrame(columns=["cluster", "clusters"]).to_csv(out / BOOTSTRAP_SUMMARY, index=False, encoding="utf-8-sig")
        return empty, pd.DataFrame(), pd.DataFrame()
    rule = {"min_score_bucket": p.min_score_bucket, "ai_mode": p.ai_mode, "pattern_rule": p.pattern_rule, "min_overlap": p.min_overlap, "max_overlap": p.max_overlap}
    train = x[x["signal_date"] <= pd.Timestamp(p.train_end)]
    oos = x[x["signal_date"] >= pd.Timestamp(p.oos_start)]
    selected_train = train[_rule_mask(train, rule)]
    selected_oos = oos[_rule_mask(oos, rule)]
    rows = [_perf(selected_train, p.policy_id, "LOCKED_POLICY", "TRAIN"), _perf(selected_oos, p.policy_id, "LOCKED_POLICY", "OOS")]
    s = pd.DataFrame(rows); s.to_csv(out / LOCKED_OOS, index=False, encoding="utf-8-sig")
    boots = pd.DataFrame([_cluster_bootstrap(selected_oos, c) for c in ["signal_date", "code", "theme"]]) if not selected_oos.empty else pd.DataFrame(columns=["cluster", "clusters"])
    boots.to_csv(out / BOOTSTRAP_SUMMARY, index=False, encoding="utf-8-sig")
    return s, selected_train, selected_oos


def _diagnostics(x: pd.DataFrame, out: Path):
    conc = []
    late = []
    for col, dim in [("pattern_combo", "COMBO"), ("overlap_degree", "OVERLAP"), ("score_bucket", "SCORE"), ("ai_pick_label", "AI")]:
        for lab, g in x.groupby(col):
            z = _perf(g, str(lab), dim)
            conc.append({k: z.get(k) for k in ["dimension", "label", "n", "unique_codes", "themes", "top2_profit_concentration", "top_code_share_pct", "top_theme_share_pct", "code_dedup_ret3"]})
            late.append({k: z.get(k) for k in ["dimension", "label", "n", "ret3_median", "late_wave_rate", "low60_distance_median", "upper_space_median", "overheat_penalty_mean"]})
    pd.DataFrame(conc).to_csv(out / CONCENTRATION_SUMMARY, index=False, encoding="utf-8-sig")
    pd.DataFrame(late).to_csv(out / LATE_WAVE_SUMMARY, index=False, encoding="utf-8-sig")
    exe = []
    for bp in [20, 50]:
        r = pd.to_numeric(x["ret3"], errors="coerce") - bp / 100.0
        exe.append({"scope": "ALL", "cost_bp": bp, "n": r.notna().sum(), "ret3_mean": r.mean(), "ret3_median": r.median(), "source": "CLOSE_RETURN_MINUS_COST_PROXY"})
    act = pd.to_numeric(x["execution_pnl"], errors="coerce").dropna()
    if len(act):
        exe.append({"scope": "ALL", "cost_bp": "LEDGER", "n": len(act), "ret3_mean": act.mean(), "ret3_median": act.median(), "source": "PAPER_OR_ACTUAL_EXECUTION_LEDGER"})
    pd.DataFrame(exe).to_csv(out / EXECUTION_SUMMARY, index=False, encoding="utf-8-sig")

    coverage = [
        {"field": "exact_pattern_any", "available": int(x["pattern_overlap_count"].gt(0).sum()), "total": len(x)},
        {"field": "native_or_explicit_pattern", "available": int(x["pattern_evidence_quality"].str.contains("NATIVE_GROUP|EXPLICIT_FIELD|STATE_FIELD", na=False).sum()), "total": len(x)},
        {"field": "score_axis", "available": int(x["score_axis"].notna().sum()), "total": len(x)},
        {"field": "true_ai_observed", "available": int(x["ai_observed"].sum()), "total": len(x)},
        {"field": "actual_execution_pnl", "available": int(x["execution_pnl"].notna().sum()), "total": len(x)},
        {"field": "late_wave", "available": int(x["late_wave"].notna().sum()), "total": len(x)},
        {"field": "low60_distance", "available": int(x["distance_low60"].notna().sum()), "total": len(x)},
        {"field": "upper_space", "available": int(x["upper_space"].notna().sum()), "total": len(x)},
    ]
    da = pd.DataFrame(coverage)
    da["coverage_pct"] = np.where(da["total"] > 0, da["available"] / da["total"] * 100, 0)
    da["status"] = np.where(da["available"] > 0, "AVAILABLE", "MISSING")
    da.to_csv(out / DATA_AUDIT, index=False, encoding="utf-8-sig")
    ai = x.groupby(["ai_pick_label", "ai_observed"], dropna=False).size().reset_index(name="n")
    ai["note"] = np.where(ai["ai_observed"].eq(1), "RECORDED_TRUE_AI", "DIRECT_REPLAY_AI_NOT_RUN_EXCLUDED_FROM_AI_EFFECT")
    ai.to_csv(out / AI_PROVENANCE_AUDIT, index=False, encoding="utf-8-sig")
    return da, ai


def _insert_report(report: str, block: str) -> str:
    s = str(report or "")
    if REPORT_HEADER in s:
        st = s.find(REPORT_HEADER)
        stops = ["\n🌙 [전일 야간환경", "\n🏆 [V48/V61", "\n🛡️ [손절거리", "\n📊 [거래량 결론]"]
        ends = [s.find(a, st + 1) for a in stops if s.find(a, st + 1) >= 0]
        s = s[:st].rstrip() + (("\n\n" + s[min(ends):].lstrip("\n")) if ends else "")
    anchors = ["\n🌙 [전일 야간환경", "\n🏆 [V48/V61", "\n🛡️ [손절거리"]
    pos = [s.find(a) for a in anchors if s.find(a) >= 0]
    if pos:
        k = min(pos)
        return s[:k].rstrip() + "\n\n" + block + "\n\n" + s[k:].lstrip("\n")
    return s.rstrip() + "\n\n" + block


def _top_rows(df: pd.DataFrame, n=6) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    q = df.copy()
    q = q[(q["n"] >= 2) if "n" in q else pd.Series(False, index=q.index)]
    if q.empty:
        return q
    for c in ["ret3_median", "ret3_trim10", "ret3_top2_excl"]:
        if c not in q: q[c] = np.nan
    q["robust_sort"] = q[["ret3_median", "ret3_trim10", "ret3_top2_excl"]].mean(axis=1, skipna=True)
    return q.sort_values(["robust_sort", "n"], ascending=[False, False]).head(n)


def build_report(x: pd.DataFrame, tables: Dict[str, pd.DataFrame], policy: Optional[Policy], lock_status: str, locked: pd.DataFrame, boots: pd.DataFrame, data_audit: pd.DataFrame) -> str:
    true_ai_n = int(x["ai_observed"].sum()) if len(x) else 0
    lines = [
        REPORT_HEADER,
        f"📌 {VERSION} · {FACTOR_NAME} · RESEARCH_ONLY=True",
        "- 질문: 저평가/BB40 회복·삼각수렴 1파 첫눌림·예비삼각수렴·BB응축·5일선 눌림·파란점선 재지지가 여러 개 겹칠 때, 점수대와 실제 AI Pick이 추가 설명력을 갖는지 분리합니다.",
        "- 같은 종목·신호일 수익률은 1번만 계산하고 membership만 explode합니다. LIVE 점수·순위·AI Pick 호출·진입/익절/손절 변경 0.",
        f"📁 분석 {len(x)}행 · 종목 {x['code'].nunique() if len(x) else 0} · 신호일 {x['signal_date'].nunique() if len(x) else 0} · 패턴태그행 {int(x['pattern_overlap_count'].gt(0).sum()) if len(x) else 0} · 실제 AI 관측 {true_ai_n}행",
    ]
    if true_ai_n == 0:
        lines.append("⚠️ Direct Replay는 과거 AI 배치를 재호출하지 않습니다. 따라서 AI Pick 효과는 '미실행'을 AI 미선정으로 간주하지 않고 UNAVAILABLE로 분리하며, 실제 저장된 AI 원장이 쌓일 때만 비교합니다.")
    else:
        lines.append("✅ AI 비교는 v1080 신호 원장에 실제 저장된 AI Pick만 사용합니다. Direct Replay의 '과거 AI 미실행' 표본은 AI 효과 계산에서 제외합니다.")
    lines.append("🧩 [정확한 패턴 조합 · n≥2, 견고지표 우선]")
    top = _top_rows(tables.get(COMBO_SUMMARY, pd.DataFrame()), 8)
    if top.empty:
        lines.append("- 평가 가능한 정확조합 표본 부족")
    else:
        for _, r in top.iterrows():
            lines.append(f"- {r['label']}: n{_int(r['n'])}·종목{_int(r['unique_codes'])}·날짜{_int(r['signal_days'])} | D3 평균 {_fmt(r['ret3_mean'])}%·중앙 {_fmt(r['ret3_median'])}%·절사 {_fmt(r['ret3_trim10'])}%·상2제외 {_fmt(r['ret3_top2_excl'])}% | D5 {_fmt(r['ret5_mean'])}% | +3 {_fmt(r['plus3_rate'],1,False)}%/SL {_fmt(r['stop_rate'],1,False)}% | 중복제거 {_fmt(r['code_dedup_ret3'])}%")
    lines.append("🔢 [패턴 중복 개수]")
    ov = tables.get(OVERLAP_SUMMARY, pd.DataFrame())
    if ov.empty:
        lines.append("- 표본 없음")
    else:
        for _, r in ov.sort_values("label").iterrows():
            lines.append(f"- {r['label']}: n{_int(r['n'])} | D3 평균 {_fmt(r['ret3_mean'])}%·중앙 {_fmt(r['ret3_median'])}%·상2제외 {_fmt(r['ret3_top2_excl'])}% | D5 {_fmt(r['ret5_mean'])}% | LATE {_fmt(r['late_wave_rate'],1,False)}% · 저점이격중앙 {_fmt(r['low60_distance_median'],1,False)}% · 상단공간 {_fmt(r['upper_space_median'],1,False)}%")
    lines.append("🎚️ [원점수 구간]")
    sc = tables.get(SCORE_SUMMARY, pd.DataFrame())
    for _, r in sc.iterrows() if not sc.empty else []:
        lines.append(f"- {r['label']}: n{_int(r['n'])} | D3 중앙 {_fmt(r['ret3_median'])}%·절사 {_fmt(r['ret3_trim10'])}%·상2제외 {_fmt(r['ret3_top2_excl'])}% | +3 {_fmt(r['plus3_rate'],1,False)}%/SL {_fmt(r['stop_rate'],1,False)}%")
    lines.append("🤖 [실제 AI Pick별]")
    ai = tables.get(AI_SUMMARY, pd.DataFrame())
    if ai.empty:
        lines.append("- 실제 저장 AI 표본 없음")
    else:
        for _, r in ai.iterrows():
            note = " (효과판정 제외)" if r["label"] == "AI_UNAVAILABLE_HISTORICAL" else ""
            lines.append(f"- {r['label']}{note}: n{_int(r['n'])} | D3 평균 {_fmt(r['ret3_mean'])}%·중앙 {_fmt(r['ret3_median'])}%·상2제외 {_fmt(r['ret3_top2_excl'])}% | +3 {_fmt(r['plus3_rate'],1,False)}%/SL {_fmt(r['stop_rate'],1,False)}%")
    lines.append("🧯 [중복이 확신인지 후행인지 진단]")
    lines.append("- 중복군의 LATE_WAVE·60일 저점이격·상단저항거리·최근 반복/과열감점을 함께 저장합니다. 중복 성과가 좋아도 위치지표가 악화되면 '확신 증가'가 아니라 후행조건 동시점등으로 해석합니다.")
    if policy is None:
        lines.append(f"🔒 POLICY LOCK 대기: {lock_status} · TRAIN에서 규칙을 확정하기 전 OOS 성과를 정책 선택에 사용하지 않습니다.")
    else:
        lines.append(f"🔒 {policy.policy_id} · TRAIN {policy.train_start}~{policy.train_end} · OOS {policy.oos_start}~ · score≥{policy.min_score_bucket} · AI={policy.ai_mode} · pattern={policy.pattern_rule} · overlap {policy.min_overlap}~{policy.max_overlap} · hash {policy.policy_hash}")
        for _, r in locked.iterrows() if not locked.empty else []:
            lines.append(f"- LOCKED {r['bucket']}: n{_int(r['n'])}·날짜{_int(r['signal_days'])} | D3 평균 {_fmt(r['ret3_mean'])}%·중앙 {_fmt(r['ret3_median'])}%·절사 {_fmt(r['ret3_trim10'])}%·상2제외 {_fmt(r['ret3_top2_excl'])}% | 50bp {_fmt(r['cost50_ret3'])}% | 중복제거 {_fmt(r['code_dedup_ret3'])}%")
    if boots is not None and not boots.empty:
        lines.append("🧪 [LOCKED OOS 클러스터 부트스트랩]")
        for _, r in boots.iterrows():
            lines.append(f"- {r.get('cluster')}: clusters {_int(r.get('clusters'))} · mean {_fmt(r.get('mean'))}% · 90%CI [{_fmt(r.get('ci05'))},{_fmt(r.get('ci95'))}] · 양수확률 {_fmt(r.get('positive_prob'),1,False)}%")
    lines.extend([
        "- 점수구간은 원본 N점수 우선, 없으면 안전점수를 사용하며 날짜 내 백분위 점수를 원점수로 위장하지 않습니다.",
        "- AI 표본이 없을 때 AI_SELECTED 정책은 탐색하지 않습니다. AI 효과는 저장 원장과 날짜·종목이 일치한 행만 인정하며 replay DataFrame의 AI 문자열은 증거로 사용하지 않습니다.",
        "- TRAIN 정책의 패턴 규칙은 정확 조합 일치로 고정하며 2개 조합이 3개 이상 상위조합을 흡수하지 않습니다.",
        "- 20bp·50bp 비용, 종목 중복 제거, 상위2개 수익집중, 날짜·종목·테마 클러스터 부트스트랩, 실제 PAPER/체결 원장 가용분을 별도 저장합니다.",
        f"- Actions CSV: {ROW_AUDIT} · {PATTERN_SUMMARY} · {COMBO_SUMMARY} · {OVERLAP_SUMMARY} · {SCORE_SUMMARY} · {AI_SUMMARY} · {CROSS_SUMMARY} · {DEDUP_SUMMARY} · {TRAIN_OOS_SUMMARY} · {LOCKED_OOS} · {BOOTSTRAP_SUMMARY} · {CONCENTRATION_SUMMARY} · {EXECUTION_SUMMARY} · {LATE_WAVE_SUMMARY} · {AI_PROVENANCE_AUDIT} · {DATA_AUDIT}",
    ])
    return "\n".join(lines)


def _empty_csvs(out: Path):
    schemas = {
        ROW_AUDIT: ["signal_date", "code", "pattern_combo", "score_bucket", "ai_pick_label"],
        PATTERN_SUMMARY: ["pattern_token", "n"], COMBO_SUMMARY: ["label", "n"], OVERLAP_SUMMARY: ["label", "n"],
        SCORE_SUMMARY: ["label", "n"], AI_SUMMARY: ["label", "n"], CROSS_SUMMARY: ["pattern_combo", "score_bucket", "ai_pick_label", "n"],
        DEDUP_SUMMARY: ["dimension", "label", "n"], TRAIN_OOS_SUMMARY: ["bucket", "dimension", "label", "n"],
        POLICY_AUDIT: ["policy_id", "policy_hash"], LOCKED_OOS: ["bucket", "n"], BOOTSTRAP_SUMMARY: ["cluster", "clusters"],
        CONCENTRATION_SUMMARY: ["dimension", "label", "n"], EXECUTION_SUMMARY: ["scope", "cost_bp", "n"],
        DATA_AUDIT: ["field", "available", "total", "coverage_pct", "status"], LATE_WAVE_SUMMARY: ["dimension", "label", "n"],
        AI_PROVENANCE_AUDIT: ["ai_pick_label", "ai_observed", "n", "note"], SHADOW_LEDGER: ["signal_date", "code", "pattern_combo", "score_bucket", "ai_pick_label"],
    }
    for name, cols in schemas.items():
        fp = out / name
        if not fp.exists():
            pd.DataFrame(columns=cols).to_csv(fp, index=False, encoding="utf-8-sig")


def run_backtest(eval_df: pd.DataFrame, output_dir: str | Path = "reports", base_report: str = "") -> Tuple[str, pd.DataFrame]:
    out = _outdir(output_dir)
    x = _normalise(eval_df, out)
    if x.empty:
        _empty_csvs(out)
        block = REPORT_HEADER + f"\n📌 {VERSION} · RESEARCH_ONLY=True\n- 평가 가능한 AUX/기존 후보가 없어 정책·LIVE 순위를 변경하지 않았습니다."
        (out / REPORT_BLOCK_FILE).write_text(block, encoding="utf-8")
        return _insert_report(base_report, block), x
    policy, lock_status, sweep = _train_lock(x, out)
    if not sweep.empty:
        sweep.to_csv(out / POLICY_SWEEP, index=False, encoding="utf-8-sig")
    train_end = pd.Timestamp(policy.train_end) if policy else None
    oos_start = pd.Timestamp(policy.oos_start) if policy else None
    tables = _summary_tables(x, out, train_end, oos_start)
    locked, _, selected_oos = _policy_outputs(x, policy, out)
    da, _ = _diagnostics(x, out)
    x.to_csv(out / ROW_AUDIT, index=False, encoding="utf-8-sig")
    boots = pd.read_csv(out / BOOTSTRAP_SUMMARY, low_memory=False) if (out / BOOTSTRAP_SUMMARY).exists() else pd.DataFrame()
    block = build_report(x, tables, policy, lock_status, locked, boots, da)
    (out / REPORT_BLOCK_FILE).write_text(block, encoding="utf-8")
    return _insert_report(base_report, block), x


def force_report(report: str, output_dir: str | Path = "reports") -> str:
    out = _outdir(output_dir)
    try:
        block = (out / REPORT_BLOCK_FILE).read_text(encoding="utf-8").strip()
    except Exception:
        block = ""
    return _insert_report(report, block) if block else str(report or "")


def build_shadow_brief(candidate_df: pd.DataFrame, output_dir: str | Path = "reports", now: Optional[pd.Timestamp] = None, force=False) -> str:
    out = _outdir(output_dir)
    t = pd.Timestamp(now or datetime.now())
    if not force and str(os.environ.get("PATTERN_AI_CROSS_FORCE_SHADOW", "0")).lower() not in ("1", "true", "yes") and t.strftime("%H:%M") < "14:50":
        return ""
    if candidate_df is None or getattr(candidate_df, "empty", True):
        return SHADOW_HEADER + "\n- 현재 후보 없음 · 기존 LIVE 순위 영향 없음."
    c = candidate_df.copy()
    cc = next((z for z in ["code", "Code", "종목코드"] if z in c.columns), None)
    nc = next((z for z in ["name", "Name", "종목명"] if z in c.columns), None)
    if not cc:
        return SHADOW_HEADER + "\n- 종목코드 필드 없음 · 기존 LIVE 영향 없음."
    c["code"] = c[cc].map(_code); c["name"] = c[nc].astype(str) if nc else c["code"]
    rows = []
    for rank, (_, r) in enumerate(c.head(15).iterrows(), start=1):
        rec = capture_signal_provenance(r, {"rank": rank, "code": r["code"], "name": r["name"]})
        toks = [p for p in PATTERNS if _int(rec.get({
            "VALUE_BB40_RECOVERY":"cross_value_bb40_recovery", "TRIANGLE_WAVE1_FIRST_PULLBACK":"cross_triangle_wave1_first_pullback",
            "PRE_TRIANGLE":"cross_pre_triangle", "BB_COMPRESSION":"cross_bb_compression", "MA5_PULLBACK_RECLAIM":"cross_ma5_pullback_reclaim",
            "BLUE_DOTTED_RECLAIM":"cross_blue_dotted_reclaim"}[p]))]
        ai_raw = str(_first(r, ["ai_pick_tier", "AI_PICK"], ""))
        if not ai_raw:
            ai_raw = "현재 카드 필드 미전달"
        score = _num(_first(r, ["N점수", "n_score", "안전점수", "safe_score"], np.nan))
        bucket = "MISSING" if not math.isfinite(score) else ("LT70" if score < 70 else ("70_79" if score < 80 else ("80_89" if score < 90 else "GE90")))
        rows.append({"signal_date": t.strftime("%Y-%m-%d"), "rank": rank, "code": r["code"], "name": r["name"], "pattern_combo": " + ".join(toks) if toks else "UNCLASSIFIED", "overlap": len(toks), "score": score, "score_bucket": bucket, "ai_pick_label": ai_raw, "evidence": rec.get("pattern_source_raw", "")})
    s = pd.DataFrame(rows); s.to_csv(out / SHADOW_LEDGER, index=False, encoding="utf-8-sig")
    lines = [SHADOW_HEADER, f"📌 {VERSION} · 현재 후보 표시만 · 기존 점수/순위/AI 호출/매수판정 변경 0"]
    for _, r in s.iterrows():
        score_txt = f"{r['score']:.0f}({r['score_bucket']})" if math.isfinite(_num(r["score"])) else "MISSING"
        lines.append(f"- {int(r['rank'])}) {r['name']}({r['code']}) | {r['pattern_combo']} | 중복 {int(r['overlap'])} | 점수 {score_txt} | AI {r['ai_pick_label']}")
    lines.append(f"- Actions CSV: {SHADOW_LEDGER} · {ROW_AUDIT} · {CROSS_SUMMARY}")
    return "\n".join(lines)
