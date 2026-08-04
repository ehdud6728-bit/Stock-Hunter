from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6.10.1"
RESEARCH_ONLY = True
HEADER = "🧱 [검색식 완성형 계산파이프라인 · PRE-FIX SHADOW · RESEARCH_ONLY]"
REPORT_FILE = "v72_search_formula_complete_pipeline_report_block.txt"
REGISTRY_FILE = "search_formula_contract_registry.json"

ROW_FILE = "v72_formula_pre_old_vs_fixed_truth.csv"
FORMULA_FILE = "v72_formula_pre_fixed_performance.csv"
TIMING_FILE = "v72_formula_timing_provenance.csv"
SCORE_FILE = "v72_formula_score_rank_shadow.csv"
ANCHOR_FILE = "v72_formula_selector_anchor_serialization.csv"
TEMPORAL_FILE = "v72_formula_temporal_invariant_audit.csv"
REGIME_FILE = "v72_formula_pre_fixed_regime_performance.csv"
READINESS_FILE = "v72_formula_complete_readiness.csv"
MANUAL_FILE = "v72_formula_manual_chart_manifest_v2.csv"
DATA_FILE = "v72_formula_complete_data_availability.csv"

MIN_POLICY_ROWS = 30
MIN_POLICY_DATES = 10

# Only values proven to be produced after the active PRE score call are overlaid.
# This is a timing repair shadow, not a new search formula.
LATE_PRODUCER_KEYS = (
    "fib_support_382",
    "fib_support_618",
    "pivot_support",
    "closing_bet",
    "closing_bet_grade",
)

# These values may differ between PRE and POST because the active execution path
# intentionally applies a semantic override (e.g. sequence-confirmed yeok_break).
# POST values must never overwrite PRE values in the timing-repair shadow.
SEMANTIC_OVERRIDE_KEYS = (
    "yeok_break",
    "style",
)


def _root() -> Path:
    return Path(__file__).resolve().parent


def _registry() -> dict:
    return json.loads((_root() / REGISTRY_FILE).read_text(encoding="utf-8"))


def _norm_code(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    d = re.sub(r"\D", "", s)
    return d.zfill(6)[-6:] if d else ""


def _loads(v: Any, default: Any) -> Any:
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(str(v))
    except Exception:
        return default


def _safe(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return None if not math.isfinite(v) else round(v, 8)
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if hasattr(v, "item"):
        try:
            return _safe(v.item())
        except Exception:
            pass
    if isinstance(v, dict):
        return {str(k): _safe(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_safe(x) for x in v]
    return str(v)


def _num(v: Any) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else float("nan")
    except Exception:
        return float("nan")


def _fmt(v: Any, digits: int = 2) -> str:
    x = _num(v)
    return "N/A" if not math.isfinite(x) else f"{x:+.{digits}f}%"


def _rate(v: Any) -> str:
    x = _num(v)
    return "N/A" if not math.isfinite(x) else f"{x:.1f}%"


def _trim_mean(s: pd.Series, frac: float = 0.10) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    if x.empty:
        return float("nan")
    k = int(len(x) * frac)
    q = x.iloc[k: len(x) - k] if len(x) - 2 * k > 0 else x
    return float(q.mean()) if len(q) else float("nan")


def _ex_top2_mean(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values(ascending=False)
    return float(x.iloc[2:].mean()) if len(x) > 2 else float("nan")


def _bool_rate(s: pd.Series) -> float:
    try:
        q = s.dropna()
        return float(q.astype(bool).mean() * 100.0) if len(q) else float("nan")
    except Exception:
        return float("nan")


def _normalize_capture(capture_rows: Iterable[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for r in capture_rows or []:
        z = dict(r or {})
        z["code"] = _norm_code(z.get("code"))
        z["signal_date"] = pd.to_datetime(z.get("signal_date"), errors="coerce")
        z["combo_invocation"] = int(float(z.get("combo_invocation", 0) or 0))
        z["formula_truth_bitmap"] = str(z.get("formula_truth_bitmap", "") or "")
        z["formula_post_truth_bitmap"] = str(z.get("formula_post_truth_bitmap", "") or "")
        rows.append(z)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[df["code"].ne("") & df["signal_date"].notna()].copy()
    return df.sort_values(["signal_date", "code", "combo_invocation"], kind="stable").reset_index(drop=True)


def _overlay_fixed_inputs(pre_inputs: dict, post_inputs: dict) -> tuple[dict, dict]:
    fixed = dict(pre_inputs or {})
    provenance: dict[str, dict] = {}
    for key in LATE_PRODUCER_KEYS:
        old = fixed.get(key, "__MISSING__")
        if key in post_inputs and post_inputs.get(key) != "__MISSING__":
            fixed[key] = post_inputs.get(key)
            provenance[key] = {
                "stage": "LATE_PRODUCER_OVERLAY",
                "pre": _safe(old),
                "post": _safe(post_inputs.get(key)),
                "changed": old != post_inputs.get(key),
            }
        else:
            provenance[key] = {
                "stage": "LATE_PRODUCER_MISSING_POST",
                "pre": _safe(old),
                "post": None,
                "changed": False,
            }
    for key in SEMANTIC_OVERRIDE_KEYS:
        provenance[key] = {
            "stage": "SEMANTIC_OVERRIDE_PRESERVE_PRE",
            "pre": _safe(pre_inputs.get(key, "__MISSING__")),
            "post": _safe(post_inputs.get(key, "__MISSING__")),
            "changed": pre_inputs.get(key, "__MISSING__") != post_inputs.get(key, "__MISSING__"),
        }
        if key in pre_inputs:
            fixed[key] = pre_inputs.get(key)
    return fixed, provenance


def _best_formula(signals: dict, combo_table: list[dict]) -> dict:
    matched: list[dict] = []
    for i, combo in enumerate(combo_table):
        try:
            if not bool(combo["cond"](signals)):
                continue
            score = combo["score_fn"](signals) if "score_fn" in combo else combo.get("score", 0)
            matched.append({
                "index": i,
                "combination": combo.get("combination", ""),
                "grade": combo.get("grade", ""),
                "score_before_style": _num(score),
            })
        except Exception:
            continue
    if not matched:
        bonus = 0
        if bool(signals.get("obv_rising")): bonus += 30
        if bool(signals.get("mfi_strong")): bonus += 20
        if bool(signals.get("volume_surge")): bonus += 10
        return {"index": -1, "combination": "🔍기본", "grade": "D", "score": 100 + bonus}
    best = max(matched, key=lambda x: _num(x.get("score_before_style")))
    score = _num(best.get("score_before_style"))
    style = str(signals.get("style", "NONE") or "NONE")
    name = str(best.get("combination", ""))
    if style == "SWING" and any(k in name for k in ("폭발", "바닥", "매집", "수렴")):
        score += 30
    elif style == "SCALP":
        if any(k in name for k in ("수박", "돌파", "거래량", "골파기")):
            score += 30
        if any(k in name for k in ("바닥", "매집완성")):
            score -= 20
    best = dict(best)
    best["score"] = score
    return best


def _changed_formulas(old_bits: str, fixed_bits: str, formulas: list[dict]) -> tuple[list[str], list[str]]:
    on: list[str] = []
    off: list[str] = []
    for c in formulas:
        i = int(c["index"])
        a = old_bits[i] if i < len(old_bits) else "?"
        b = fixed_bits[i] if i < len(fixed_bits) else "?"
        if a != "T" and b == "T":
            on.append(c["combination"])
        elif a == "T" and b != "T":
            off.append(c["combination"])
    return on, off


def _formula_timing_table(formulas: list[dict]) -> pd.DataFrame:
    rows = []
    late = set(LATE_PRODUCER_KEYS)
    semantic = set(SEMANTIC_OVERRIDE_KEYS)
    for c in formulas:
        keys = set(c.get("referenced_keys", []))
        if keys & late:
            stage = "LATE_PRODUCER_SHADOW_ELIGIBLE"
        elif keys & semantic:
            stage = "SEMANTIC_OVERRIDE_PRE_AUTHORITATIVE"
        else:
            stage = "PRE_NATIVE"
        rows.append({
            "formula_index": int(c["index"]),
            "formula": c["combination"],
            "referenced_keys": "|".join(c.get("referenced_keys", [])),
            "timing_class": stage,
            "late_keys": "|".join(sorted(keys & late)),
            "semantic_keys": "|".join(sorted(keys & semantic)),
            "used_for_live": False,
            "used_for_shadow_performance": stage in {"LATE_PRODUCER_SHADOW_ELIGIBLE", "PRE_NATIVE", "SEMANTIC_OVERRIDE_PRE_AUTHORITATIVE"},
        })
    return pd.DataFrame(rows)


def _evaluate(base: pd.DataFrame, evaluator: Callable[[pd.DataFrame], pd.DataFrame] | None) -> pd.DataFrame:
    if base.empty:
        return pd.DataFrame()
    q = base.copy()
    q["source"] = "FORMULA_PRE_FIXED_SHADOW"
    q["entry_price"] = 0.0
    cols = [c for c in [
        "signal_date", "code", "name", "entry_price", "source", "market", "candidate_selected",
        "formula_truth_registry_sha256", "formula_truth_bitmap", "formula_truth_true",
        "formula_truth_scores_json", "formula_truth_errors_json", "formula_truth_missing_json",
        "formula_truth_inputs_json", "combo_invocation", "pre_old_bitmap", "pre_fixed_bitmap",
        "pre_old_best", "pre_fixed_best", "pre_fixed_changed",
    ] if c in q.columns]
    q = q[cols].copy()
    try:
        out = evaluator(q) if callable(evaluator) else pd.DataFrame()
    except Exception as exc:
        out = q.copy()
        out["eval_status"] = f"EVALUATOR_ERROR:{type(exc).__name__}:{exc}"
    return out if isinstance(out, pd.DataFrame) else pd.DataFrame()


def _explode(eval_df: pd.DataFrame, formulas: list[dict], bitmap_col: str = "pre_fixed_bitmap") -> pd.DataFrame:
    rows = []
    for _, r in eval_df.iterrows():
        bits = str(r.get(bitmap_col, r.get("formula_truth_bitmap", "")) or "")
        for c in formulas:
            i = int(c["index"])
            if i < len(bits) and bits[i] == "T":
                z = r.to_dict()
                z["formula_index"] = i
                z["formula"] = c["combination"]
                z["formula_grade"] = c.get("grade", "")
                z["formula_base_score"] = c.get("base_score")
                rows.append(z)
    x = pd.DataFrame(rows)
    if not x.empty:
        x = x.drop_duplicates(["signal_date", "code", "formula"], keep="first").reset_index(drop=True)
    return x


def _perf(g: pd.DataFrame, formula: str, regime: str = "ALL") -> dict:
    def nums(col: str) -> pd.Series:
        if isinstance(g, pd.DataFrame) and col in g.columns:
            return pd.to_numeric(g[col], errors="coerce")
        return pd.Series(dtype=float)

    d1 = nums("next1_close_ret")
    d3 = nums("next3_close_ret")
    d5 = nums("next5_close_ret")
    selected = g["candidate_selected"] if isinstance(g, pd.DataFrame) and "candidate_selected" in g.columns else pd.Series(dtype=bool)
    mex = nums("market_excess3")
    return {
        "formula": formula,
        "regime": regime,
        "n": int(len(g)),
        "stocks": int(g["code"].nunique()) if "code" in g.columns else 0,
        "signal_days": int(g["signal_date"].nunique()) if "signal_date" in g.columns else 0,
        "selected_rows": int(selected.fillna(False).astype(bool).sum()) if len(selected) else 0,
        "d1_mean": d1.mean(),
        "d3_mean": d3.mean(),
        "d3_median": d3.median(),
        "d3_trim10": _trim_mean(d3),
        "d3_ex_top2": _ex_top2_mean(d3),
        "d5_mean": d5.mean(),
        "d3_cost20bp": d3.mean() - 0.20 if d3.notna().any() else np.nan,
        "d3_cost50bp": d3.mean() - 0.50 if d3.notna().any() else np.nan,
        "plus3_first_rate": _bool_rate(g["plus3_first"] if "plus3_first" in g.columns else pd.Series(dtype=bool)),
        "stop_first_rate": _bool_rate(g["stop_first"] if "stop_first" in g.columns else pd.Series(dtype=bool)),
        "market_excess3_mean": mex.mean(),
        "market_excess3_median": mex.median(),
    }


def _formula_perf(exploded: pd.DataFrame, formulas: list[dict]) -> pd.DataFrame:
    rows = []
    for c in formulas:
        g = exploded[exploded["formula"].eq(c["combination"])] if not exploded.empty else pd.DataFrame()
        z = _perf(g, c["combination"])
        z.update({"formula_index": int(c["index"]), "grade": c.get("grade", ""), "base_score": c.get("base_score")})
        rows.append(z)
    return pd.DataFrame(rows)


def _market_map(listing_df: pd.DataFrame | None) -> dict[str, str]:
    if listing_df is None or not isinstance(listing_df, pd.DataFrame) or listing_df.empty:
        return {}
    c = next((x for x in ("Code", "Symbol", "code", "종목코드") if x in listing_df.columns), None)
    m = next((x for x in ("Market", "market", "시장") if x in listing_df.columns), None)
    if not c or not m:
        return {}
    q = listing_df[[c, m]].copy()
    q[c] = q[c].map(_norm_code)
    q[m] = q[m].astype(str).str.upper()
    q.loc[q[m].str.contains("KOSDAQ|코스닥", case=False, na=False), m] = "KOSDAQ"
    q.loc[q[m].str.contains("KOSPI|코스피|유가", case=False, na=False), m] = "KOSPI"
    return q.drop_duplicates(c).set_index(c)[m].to_dict()


def _history_frame(v: Any, signal_date: pd.Timestamp) -> pd.DataFrame:
    if isinstance(v, pd.DataFrame):
        h = v.copy()
    elif isinstance(v, list):
        h = pd.DataFrame(v)
    else:
        return pd.DataFrame()
    if h.empty:
        return h
    if not isinstance(h.index, pd.DatetimeIndex):
        dcol = next((c for c in ("Date", "date", "날짜") if c in h.columns), None)
        if dcol:
            h.index = pd.to_datetime(h[dcol], errors="coerce")
        else:
            h.index = pd.to_datetime(h.index, errors="coerce")
    h = h[h.index.notna()].sort_index()
    h = h[h.index.normalize() <= pd.Timestamp(signal_date).normalize()].copy()
    return h.tail(120)


def causal_anchor_v1(history: pd.DataFrame, signal_date: Any) -> dict:
    """Deterministic causal anchor serializer.

    It does not claim to be an old selector's hidden native anchor. It serializes a
    chronological low→high→pullback sequence on the exact as-of OHLC history so every
    selected membership can be chart-audited without inventing dates from scores.
    """
    sd = pd.Timestamp(signal_date).normalize()
    h = _history_frame(history, sd)
    required = {"High", "Low", "Close"}
    if h.empty or not required.issubset(h.columns) or len(h) < 8:
        return {
            "anchor_status": "HISTORY_UNAVAILABLE",
            "anchor_method": "CAUSAL_LOW_HIGH_PULLBACK_V1",
            "signal_date": sd.strftime("%Y-%m-%d"),
            "temporal_invariant": "UNKNOWN",
            "anchor_reason": "OHLC_HISTORY_MISSING_OR_TOO_SHORT",
        }
    w = h.tail(60).copy()
    highs = pd.to_numeric(w["High"], errors="coerce")
    lows = pd.to_numeric(w["Low"], errors="coerce")
    closes = pd.to_numeric(w["Close"], errors="coerce")
    best = None
    idx = list(w.index)
    # Leave at least one bar after the peak for a pullback/retest observation.
    for i in range(max(0, len(w) - 60), len(w) - 3):
        low = lows.iloc[i]
        if not math.isfinite(_num(low)) or low <= 0:
            continue
        for j in range(i + 2, len(w) - 1):
            high = highs.iloc[j]
            if not math.isfinite(_num(high)) or high <= low:
                continue
            rise = (high / low - 1.0) * 100.0
            # Prefer the largest chronological impulse; tie-break toward the latest peak.
            key = (rise, j, -i)
            if best is None or key > best[0]:
                best = (key, i, j, float(low), float(high))
    if best is None:
        return {
            "anchor_status": "NO_CHRONOLOGICAL_IMPULSE",
            "anchor_method": "CAUSAL_LOW_HIGH_PULLBACK_V1",
            "signal_date": sd.strftime("%Y-%m-%d"),
            "temporal_invariant": "UNKNOWN",
            "anchor_reason": "NO_LOW_HIGH_PULLBACK_SEQUENCE",
        }
    _, i, j, low_price, high_price = best
    after = w.iloc[j + 1:]
    if after.empty:
        pull_k = j
        pull_price = float(lows.iloc[j])
    else:
        rel = pd.to_numeric(after["Low"], errors="coerce").idxmin()
        pull_k = idx.index(rel)
        pull_price = float(pd.to_numeric(w.loc[rel, "Low"], errors="coerce"))
    base_dt = pd.Timestamp(idx[i]).normalize()
    peak_dt = pd.Timestamp(idx[j]).normalize()
    pull_dt = pd.Timestamp(idx[pull_k]).normalize()
    close_now = _num(closes.iloc[-1])
    rise_pct = (high_price / low_price - 1.0) * 100.0 if low_price > 0 else np.nan
    retrace_pct = ((high_price - pull_price) / (high_price - low_price) * 100.0) if high_price > low_price else np.nan
    low_distance = ((close_now / float(lows.min())) - 1.0) * 100.0 if _num(lows.min()) > 0 else np.nan
    upper_space = ((float(highs.max()) / close_now) - 1.0) * 100.0 if close_now > 0 else np.nan
    valid = bool(base_dt < peak_dt < pull_dt <= sd)
    return {
        "anchor_status": "AVAILABLE" if valid else "TEMPORAL_INVALID",
        "anchor_method": "CAUSAL_LOW_HIGH_PULLBACK_V1",
        "signal_date": sd.strftime("%Y-%m-%d"),
        "wave1_low_date": base_dt.strftime("%Y-%m-%d"),
        "wave1_low_price": low_price,
        "wave1_high_date": peak_dt.strftime("%Y-%m-%d"),
        "wave1_high_price": high_price,
        "pullback_low_date": pull_dt.strftime("%Y-%m-%d"),
        "pullback_low_price": pull_price,
        "wave1_rise_pct": rise_pct,
        "pullback_retrace_pct": retrace_pct,
        "low60_distance_pct": low_distance,
        "upper60_space_pct": upper_space,
        "temporal_invariant": "PASS" if valid else "FAIL",
    }


def _aux_memberships(out: Path) -> dict[tuple[str, str], str]:
    fp = out / "v72_aux_candidate_group_shadow_raw.csv"
    if not fp.exists():
        return {}
    try:
        q = pd.read_csv(fp, low_memory=False)
    except Exception:
        return {}
    if q.empty:
        return {}
    dcol = next((c for c in ("signal_date", "신호일", "date") if c in q.columns), None)
    ccol = next((c for c in ("code", "Code", "종목코드") if c in q.columns), None)
    gcol = next((c for c in ("v7337_candidate_groups", "candidate_groups", "group") if c in q.columns), None)
    if not dcol or not ccol or not gcol:
        return {}
    q["_d"] = pd.to_datetime(q[dcol], errors="coerce").dt.strftime("%Y-%m-%d")
    q["_c"] = q[ccol].map(_norm_code)
    return q.dropna(subset=["_d"]).drop_duplicates(["_d", "_c"], keep="last").set_index(["_d", "_c"])[gcol].astype(str).to_dict()


def _anchor_tables(causal: pd.DataFrame, history_map: dict, out: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    memberships = _aux_memberships(out)
    rows = []
    for _, r in causal.iterrows():
        ds = pd.Timestamp(r["signal_date"]).strftime("%Y-%m-%d")
        code = _norm_code(r["code"])
        h = history_map.get((ds, code), history_map.get(f"{ds}|{code}")) if isinstance(history_map, dict) else None
        a = causal_anchor_v1(h, r["signal_date"])
        a.update({
            "version": VERSION,
            "code": code,
            "name": r.get("name", ""),
            "selector_memberships": memberships.get((ds, code), ""),
            "anchor_provenance": "EXACT_ASOF_HISTORY_SHADOW_SERIALIZER",
            "selector_logic_changed": False,
        })
        rows.append(a)
    ad = pd.DataFrame(rows)
    required_cols = {
        "anchor_status": "HISTORY_UNAVAILABLE",
        "anchor_method": "CAUSAL_LOW_HIGH_PULLBACK_V1",
        "signal_date": "",
        "temporal_invariant": "UNKNOWN",
        "anchor_reason": "",
    }
    for col, default in required_cols.items():
        if col not in ad.columns:
            ad[col] = default
        else:
            ad[col] = ad[col].fillna(default)
    if ad.empty:
        td = pd.DataFrame(columns=["temporal_invariant", "n", "rate_pct"])
    else:
        td = ad.groupby("temporal_invariant", dropna=False).size().reset_index(name="n")
        td["rate_pct"] = td["n"] / len(ad) * 100.0
    return ad, td


def _manual_manifest(row_df: pd.DataFrame, anchors: pd.DataFrame, formulas: list[dict]) -> pd.DataFrame:
    rows = []
    changed = row_df[row_df.get("pre_fixed_changed", False).astype(bool)] if not row_df.empty and "pre_fixed_changed" in row_df.columns else pd.DataFrame()
    src = changed if not changed.empty else row_df.head(40)
    for _, r in src.head(80).iterrows():
        ds = pd.Timestamp(r["signal_date"]).strftime("%Y-%m-%d")
        code = _norm_code(r["code"])
        a = anchors[(anchors["code"].eq(code)) & (anchors["signal_date"].eq(ds))].head(1) if not anchors.empty else pd.DataFrame()
        ar = a.iloc[0].to_dict() if not a.empty else {}
        rows.append({
            "signal_date": ds,
            "code": code,
            "name": r.get("name", ""),
            "sample_class": "TIMING_CHANGED" if bool(r.get("pre_fixed_changed", False)) else "CONTROL",
            "old_best": r.get("pre_old_best", ""),
            "fixed_best": r.get("pre_fixed_best", ""),
            "turned_on_formulas": r.get("turned_on_formulas", ""),
            "wave1_low_date": ar.get("wave1_low_date", ""),
            "wave1_high_date": ar.get("wave1_high_date", ""),
            "pullback_low_date": ar.get("pullback_low_date", ""),
            "temporal_invariant": ar.get("temporal_invariant", ""),
            "human_formula_match": "",
            "human_anchor_match": "",
            "review_note": "",
        })
    return pd.DataFrame(rows)


def _insert_block(text: str, block: str) -> str:
    s = str(text or "")
    if HEADER in s:
        st = s.find(HEADER)
        stops = ["\n🌙 [전일 야간환경", "\n🏆 [V48/V61", "\n🛡️ [손절거리"]
        ends = [s.find(x, st + len(HEADER)) for x in stops if s.find(x, st + len(HEADER)) >= 0]
        s = s[:st].rstrip() + ("\n\n" + s[min(ends):].lstrip("\n") if ends else "")
    anchors = ["\n🌙 [전일 야간환경", "\n🏆 [V48/V61", "\n🛡️ [손절거리"]
    pos = [s.find(a) for a in anchors if s.find(a) >= 0]
    if pos:
        k = min(pos)
        return s[:k].rstrip() + "\n\n" + block + "\n" + s[k:]
    return s.rstrip() + "\n\n" + block


def run_backtest(
    capture_rows: Iterable[dict],
    attempt_rows: Iterable[dict],
    history_map: dict,
    *,
    output_dir: str = "reports",
    base_report: str = "",
    evaluator: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    listing_df: pd.DataFrame | None = None,
    combo_table: list[dict] | None = None,
    capture_truth_fn: Callable[[dict, list[dict]], dict] | None = None,
) -> tuple[str, dict[str, pd.DataFrame]]:
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    reg = _registry()
    formulas = reg.get("combos", [])
    reg_sha = str(reg.get("registry_sha256", ""))
    expected = int(reg.get("combo_count", len(formulas)))
    combo_table = combo_table or []

    raw = _normalize_capture(capture_rows)
    causal = raw.drop_duplicates(["signal_date", "code"], keep="first").copy() if not raw.empty else pd.DataFrame()
    timing = _formula_timing_table(formulas)
    timing.to_csv(out / TIMING_FILE, index=False, encoding="utf-8-sig")

    rows = []
    for _, r in causal.iterrows():
        pre_inputs = _loads(r.get("formula_truth_inputs_json", "{}"), {})
        post_inputs = _loads(r.get("formula_post_truth_inputs_json", "{}"), {})
        fixed, provenance = _overlay_fixed_inputs(pre_inputs, post_inputs)
        if callable(capture_truth_fn) and len(combo_table) == expected:
            truth = capture_truth_fn(fixed, combo_table)
        else:
            truth = {"registry_sha256": reg_sha, "bitmap": "", "true_formulas": [], "true_scores": [], "errors": [{"stage": "contract", "error": "capture_truth_unavailable"}], "missing_keys": {}, "inputs": fixed}
        old_bits = str(r.get("formula_truth_bitmap", "") or "")
        fixed_bits = str(truth.get("bitmap", "") or "")
        on, off = _changed_formulas(old_bits, fixed_bits, formulas)
        old_best = _best_formula(pre_inputs, combo_table) if len(combo_table) == expected else {}
        fixed_best = _best_formula(fixed, combo_table) if len(combo_table) == expected else {}
        z = r.to_dict()
        z.update({
            "pre_old_bitmap": old_bits,
            "pre_fixed_bitmap": fixed_bits,
            "pre_fixed_registry_sha256": truth.get("registry_sha256", ""),
            "pre_fixed_true": " / ".join(truth.get("true_formulas", [])),
            "pre_fixed_scores_json": json.dumps(truth.get("true_scores", []), ensure_ascii=False, separators=(",", ":")),
            "pre_fixed_errors_json": json.dumps(truth.get("errors", []), ensure_ascii=False, separators=(",", ":")),
            "pre_fixed_missing_json": json.dumps(truth.get("missing_keys", {}), ensure_ascii=False, separators=(",", ":")),
            "pre_fixed_inputs_json": json.dumps(truth.get("inputs", fixed), ensure_ascii=False, separators=(",", ":")),
            "timing_overlay_provenance_json": json.dumps(provenance, ensure_ascii=False, separators=(",", ":")),
            "turned_on_formulas": " | ".join(on),
            "turned_off_formulas": " | ".join(off),
            "pre_fixed_changed": bool(on or off),
            "late_overlay_complete": all(v.get("stage") != "LATE_PRODUCER_MISSING_POST" for k, v in provenance.items() if k in LATE_PRODUCER_KEYS),
            "pre_old_best": old_best.get("combination", ""),
            "pre_old_best_score": old_best.get("score"),
            "pre_fixed_best": fixed_best.get("combination", ""),
            "pre_fixed_best_score": fixed_best.get("score"),
            "winner_changed": old_best.get("combination", "") != fixed_best.get("combination", ""),
            "live_score_changed": False,
        })
        # Evaluator compatibility: fixed truth becomes the shadow evaluation bitmap only.
        z["formula_truth_registry_sha256"] = truth.get("registry_sha256", "")
        z["formula_truth_bitmap"] = fixed_bits
        z["formula_truth_true"] = z["pre_fixed_true"]
        z["formula_truth_scores_json"] = z["pre_fixed_scores_json"]
        z["formula_truth_errors_json"] = z["pre_fixed_errors_json"]
        z["formula_truth_missing_json"] = z["pre_fixed_missing_json"]
        z["formula_truth_inputs_json"] = z["pre_fixed_inputs_json"]
        rows.append(z)
    row_df = pd.DataFrame(rows)
    row_df.to_csv(out / ROW_FILE, index=False, encoding="utf-8-sig")

    mmap = _market_map(listing_df)
    if not row_df.empty:
        row_df["market"] = row_df["code"].map(mmap).fillna("UNKNOWN")
        row_df["candidate_selected"] = pd.Series(row_df.get("analyze_returned", False)).fillna(False).astype(bool)
        eval_base = row_df[row_df["pre_fixed_bitmap"].map(lambda s: "T" in str(s))].copy()
    else:
        eval_base = pd.DataFrame()
    eval_df = _evaluate(eval_base, evaluator)
    # Preserve fixed metadata if evaluator drops optional columns.
    if not eval_df.empty and "pre_fixed_bitmap" not in eval_df.columns:
        keep = row_df[["signal_date", "code", "pre_fixed_bitmap", "pre_old_bitmap", "pre_old_best", "pre_fixed_best", "pre_fixed_changed"]].drop_duplicates(["signal_date", "code"])
        eval_df = eval_df.merge(keep, on=["signal_date", "code"], how="left")
    exploded = _explode(eval_df, formulas)

    regime_perf = pd.DataFrame()
    try:
        import search_formula_universe_audit as u9
        if not exploded.empty and callable(getattr(u9, "_attach_regime", None)):
            exploded, regime_perf, _ = u9._attach_regime(exploded, out)  # research module internal reuse
    except Exception:
        regime_perf = pd.DataFrame()
    perf = _formula_perf(exploded, formulas)
    perf.to_csv(out / FORMULA_FILE, index=False, encoding="utf-8-sig")
    regime_perf.to_csv(out / REGIME_FILE, index=False, encoding="utf-8-sig")

    score_cols = [c for c in ["signal_date", "code", "name", "pre_old_best", "pre_old_best_score", "pre_fixed_best", "pre_fixed_best_score", "winner_changed", "turned_on_formulas", "turned_off_formulas", "live_score_changed"] if c in row_df.columns]
    score_df = row_df[score_cols].copy() if score_cols else pd.DataFrame()
    score_df.to_csv(out / SCORE_FILE, index=False, encoding="utf-8-sig")

    anchors, temporal = _anchor_tables(causal, history_map, out)
    anchors.to_csv(out / ANCHOR_FILE, index=False, encoding="utf-8-sig")
    temporal.to_csv(out / TEMPORAL_FILE, index=False, encoding="utf-8-sig")
    manual = _manual_manifest(row_df, anchors, formulas)
    manual.to_csv(out / MANUAL_FILE, index=False, encoding="utf-8-sig")

    combo_rows = len(causal)
    combo_days = int(causal["signal_date"].nunique()) if not causal.empty else 0
    fixed_complete = int(row_df["pre_fixed_bitmap"].str.len().eq(expected).sum()) if not row_df.empty else 0
    fixed_errors = int(row_df["pre_fixed_errors_json"].map(lambda x: len(_loads(x, []))).sum()) if not row_df.empty else 0
    changed_rows = int(row_df["pre_fixed_changed"].sum()) if not row_df.empty else 0
    overlay_complete = int(row_df["late_overlay_complete"].sum()) if not row_df.empty and "late_overlay_complete" in row_df.columns else 0
    winner_changed = int(row_df["winner_changed"].sum()) if not row_df.empty else 0
    anchor_available = int(anchors["anchor_status"].eq("AVAILABLE").sum()) if not anchors.empty and "anchor_status" in anchors.columns else 0
    anchor_unknown = int(anchors["temporal_invariant"].eq("UNKNOWN").sum()) if not anchors.empty and "temporal_invariant" in anchors.columns else 0
    temporal_fail = int(anchors["temporal_invariant"].eq("FAIL").sum()) if not anchors.empty and "temporal_invariant" in anchors.columns else 0
    eval_ok = int(eval_df.get("eval_status", pd.Series(dtype=str)).eq("OK").sum()) if not eval_df.empty else 0
    eval_days = int(exploded["signal_date"].nunique()) if not exploded.empty else 0
    contract_valid = bool(
        combo_rows > 0
        and fixed_complete == combo_rows
        and overlay_complete == combo_rows
        and fixed_errors == 0
        and anchor_available == combo_rows
        and temporal_fail == 0
    )
    policy_ready = bool(eval_ok >= MIN_POLICY_ROWS and eval_days >= MIN_POLICY_DATES)

    readiness = pd.DataFrame([{
        "version": VERSION,
        "combo_rows": combo_rows,
        "combo_days": combo_days,
        "formula_count": expected,
        "pre_fixed_complete_rows": fixed_complete,
        "pre_fixed_condition_errors": fixed_errors,
        "late_overlay_complete_rows": overlay_complete,
        "timing_changed_rows": changed_rows,
        "winner_changed_rows": winner_changed,
        "anchor_available_rows": anchor_available,
        "anchor_coverage_pct": anchor_available / combo_rows * 100.0 if combo_rows else np.nan,
        "temporal_invariant_fail_rows": temporal_fail,
        "temporal_invariant_unknown_rows": anchor_unknown,
        "evaluated_rows": eval_ok,
        "evaluated_signal_days": eval_days,
        "contract_valid": contract_valid,
        "policy_ready": policy_ready,
        "live_logic_changed": False,
        "status": "VALID_SHADOW" if contract_valid else "INVALID",
    }])
    readiness.to_csv(out / READINESS_FILE, index=False, encoding="utf-8-sig")

    pd.DataFrame([
        {"item": "CAPTURE_ROWS", "available": combo_rows, "status": "OK" if combo_rows else "MISSING"},
        {"item": "FIXED_BITMAP_COMPLETE", "available": fixed_complete, "status": "OK" if fixed_complete == combo_rows and combo_rows else "PARTIAL"},
        {"item": "HISTORY_MAP", "available": len(history_map or {}), "status": "OK" if history_map else "MISSING"},
        {"item": "ANCHOR_AVAILABLE", "available": anchor_available, "status": "OK" if anchor_available else "MISSING"},
        {"item": "EVALUATED_ROWS", "available": eval_ok, "status": "OK" if eval_ok else "EMPTY"},
    ]).to_csv(out / DATA_FILE, index=False, encoding="utf-8-sig")

    changed_formula_counts: list[tuple[str, int]] = []
    if not row_df.empty:
        counts: dict[str, int] = {}
        for text in row_df["turned_on_formulas"].astype(str):
            for name in [x.strip() for x in text.split("|") if x.strip()]:
                counts[name] = counts.get(name, 0) + 1
        changed_formula_counts = sorted(counts.items(), key=lambda x: (-x[1], x[0]))

    lines = [
        HEADER,
        f"📌 {VERSION} · COMPLETE_FORMULA_INPUT_ORDER_ANCHOR_REGIME_PIPELINE · RESEARCH_ONLY=True",
        "- 목적: 실제 PRE 입력을 보존한 채, 늦게 생성되던 피보나치·피봇·종가배팅 원천값만 PRE-FIX SHADOW에 옮겨 동일 모집단에서 재계산합니다.",
        "- yeok_break처럼 PRE에서 시퀀스 의미가 확정되는 값은 POST로 덮지 않습니다. 계산순서 수정과 조건정의 변경을 분리합니다.",
        f"🧾 계약: COMBO {combo_rows}행·{combo_days}일 | FIXED bitmap {fixed_complete}/{combo_rows} | late-input {overlay_complete}/{combo_rows} | condition error {fixed_errors} | anchor {anchor_available}/{combo_rows} | temporal fail {temporal_fail}·unknown {anchor_unknown} | {'✅ VALID_SHADOW' if contract_valid else '⛔ INVALID'}",
        f"🔄 영향: timing 변경 {changed_rows}행 | 최고식 변경 {winner_changed}행 | LIVE 점수·순위·후보·주문 변경 0",
    ]
    lines.append("⏱️ [PRE-OLD → PRE-FIX 실제 점등 변화]")
    if not changed_formula_counts:
        lines.append("- 늦은 producer 주입으로 새로 점등된 식이 없습니다.")
    else:
        for name, n in changed_formula_counts[:10]:
            lines.append(f"- {name}: PRE-FIX 신규점등 {n}건")
    if not score_df.empty and winner_changed:
        lines.append("🏆 [최고 조합 SHADOW 변경 예시]")
        for _, r in score_df[score_df["winner_changed"].astype(bool)].head(8).iterrows():
            lines.append(f"- {pd.Timestamp(r['signal_date']).strftime('%Y-%m-%d')} {r['code']} {r.get('name','')} | {r.get('pre_old_best','')} → {r.get('pre_fixed_best','')} | LIVE 미반영")
    lines.append("🧬 [Selector membership 시점 anchor 직렬화]")
    lines.append(f"- exact as-of OHLC 기반 chronological low→high→pullback anchor {anchor_available}/{combo_rows} · 시간순서 위반 {temporal_fail}건 · 미확인 {anchor_unknown}건")
    lines.append("- 기존 selector가 날짜를 저장하지 않았던 문제를 보완하기 위한 SHADOW 증거이며, 점수에서 날짜를 역추정하지 않습니다.")
    lines.append("📊 [PRE-FIX 성과판정 준비]")
    lines.append(f"- 평가 OK {eval_ok}행·독립일 {eval_days}일 | {'READY' if policy_ready else 'NOT_READY'} · 최소 {MIN_POLICY_ROWS}행·{MIN_POLICY_DATES}일 전 삭제/승격 금지")
    hit = perf[pd.to_numeric(perf.get("n"), errors="coerce").fillna(0).gt(0)].copy() if not perf.empty else pd.DataFrame()
    if not hit.empty:
        lines.append("🔍 [PRE-FIX 점등 상위 검색식]")
        for _, r in hit.sort_values(["n", "d3_median"], ascending=[False, False], na_position="last").head(10).iterrows():
            lines.append(
                f"- {r['formula']}: n{int(r['n'])}·날짜{int(r.get('signal_days',0))} | D3 평균 {_fmt(r.get('d3_mean'))}·중앙 {_fmt(r.get('d3_median'))}·절사 {_fmt(r.get('d3_trim10'))}·상2제외 {_fmt(r.get('d3_ex_top2'))} | D5 {_fmt(r.get('d5_mean'))} | +3 {_rate(r.get('plus3_first_rate'))}/SL {_rate(r.get('stop_first_rate'))}"
            )
    lines += [
        "🔒 [승격 규칙]",
        "- VALID_SHADOW는 계산계약이 완성됐다는 뜻이며 수익성이 입증됐다는 뜻이 아닙니다.",
        "- 10독립일 이후 PRE-OLD vs PRE-FIX의 중앙값·절사·상위2개 제외·50bp후 지수초과가 함께 개선될 때만 LIVE 반영 검토합니다.",
        f"- Actions CSV: {ROW_FILE} · {FORMULA_FILE} · {TIMING_FILE} · {SCORE_FILE} · {ANCHOR_FILE} · {TEMPORAL_FILE} · {REGIME_FILE} · {READINESS_FILE} · {MANUAL_FILE} · {DATA_FILE}",
    ]
    block = "\n".join(lines)
    (out / REPORT_FILE).write_text(block, encoding="utf-8")
    return _insert_block(base_report, block), {
        "rows": row_df,
        "performance": perf,
        "timing": timing,
        "score": score_df,
        "anchors": anchors,
        "temporal": temporal,
        "regime": regime_perf,
        "readiness": readiness,
        "manual": manual,
    }


def force_report(text: str, output_dir: str = "reports") -> str:
    p = Path(output_dir or "reports") / REPORT_FILE
    if not p.exists():
        return str(text or "")
    try:
        return _insert_block(str(text or ""), p.read_text(encoding="utf-8"))
    except Exception:
        return str(text or "")
