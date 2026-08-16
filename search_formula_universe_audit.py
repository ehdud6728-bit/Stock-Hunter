from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6.9.1"
RESEARCH_ONLY = True
HEADER = "🌐 [전체 유니버스 검색식 Truth × 성과 × 시장국면 전수감사 · RESEARCH_ONLY]"
REGISTRY_FILE = "search_formula_contract_registry.json"
REPORT_FILE = "v72_search_formula_universe_report_block.txt"

RAW_FILE = "v72_search_formula_universe_truth_raw.csv"
ATTEMPT_FILE = "v72_search_formula_universe_attempt_audit.csv"
COVERAGE_FILE = "v72_search_formula_universe_coverage_summary.csv"
EVAL_FILE = "v72_search_formula_universe_truth_eval.csv"
EXPLODED_FILE = "v72_search_formula_universe_exploded_eval.csv"
PERF_FILE = "v72_search_formula_universe_formula_performance.csv"
REGIME_FILE = "v72_search_formula_universe_formula_regime_performance.csv"
POST_ONLY_FILE = "v72_search_formula_universe_post_only_timing_breach.csv"
ERROR_FILE = "v72_search_formula_universe_condition_error_audit.csv"
DATA_FILE = "v72_search_formula_universe_data_availability_audit.csv"

MIN_POLICY_ROWS = 30
MIN_POLICY_DATES = 10


def _root() -> Path:
    return Path(__file__).resolve().parent


def _registry() -> dict:
    return json.loads((_root() / REGISTRY_FILE).read_text(encoding="utf-8"))


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

def _loads(v: Any, default: Any) -> Any:
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(str(v))
    except Exception:
        return default


def _num(v: Any) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else float("nan")
    except Exception:
        return float("nan")


def _bool_rate(s: pd.Series) -> float:
    try:
        q = s.dropna()
        return float(q.astype(bool).mean() * 100.0) if len(q) else float("nan")
    except Exception:
        return float("nan")


def _trim_mean(s: pd.Series, frac: float = 0.10) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    if x.empty:
        return float("nan")
    k = int(len(x) * frac)
    q = x.iloc[k:len(x) - k] if len(x) - 2 * k > 0 else x
    return float(q.mean()) if len(q) else float("nan")


def _ex_top2_mean(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values(ascending=False)
    return float(x.iloc[2:].mean()) if len(x) > 2 else float("nan")


def _fmt(v: Any, digits: int = 2, suffix: str = "%") -> str:
    x = _num(v)
    return "N/A" if not math.isfinite(x) else f"{x:+.{digits}f}{suffix}"


def _rate(v: Any) -> str:
    x = _num(v)
    return "N/A" if not math.isfinite(x) else f"{x:.1f}%"


def _normalize_capture(capture_rows: Iterable[dict]) -> pd.DataFrame:
    rows = []
    for r in capture_rows or []:
        z = dict(r or {})
        z["code"] = _norm_code(z.get("code"))
        z["signal_date"] = pd.to_datetime(z.get("signal_date"), errors="coerce")
        z["formula_truth_bitmap"] = str(z.get("formula_truth_bitmap", "") or "")
        z["formula_post_truth_bitmap"] = str(z.get("formula_post_truth_bitmap", "") or "")
        z["formula_truth_registry_sha256"] = str(z.get("formula_truth_registry_sha256", "") or "")
        z["formula_post_truth_registry_sha256"] = str(z.get("formula_post_truth_registry_sha256", "") or "")
        z["combo_invocation"] = int(float(z.get("combo_invocation", 0) or 0))
        rows.append(z)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[df["code"].ne("") & df["signal_date"].notna()].copy()
    # The first active score invocation is the causal scoring pass. Repeated calls are retained
    # in RAW, but performance uses one row per date/code to prevent duplicate returns.
    df = df.sort_values(["signal_date", "code", "combo_invocation"], kind="stable").reset_index(drop=True)
    return df


def _normalize_attempts(attempt_rows: Iterable[dict]) -> pd.DataFrame:
    rows = []
    for r in attempt_rows or []:
        z = dict(r or {})
        z["code"] = _norm_code(z.get("code"))
        z["signal_date"] = pd.to_datetime(z.get("signal_date"), errors="coerce")
        rows.append(z)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["code"].ne("") & df["signal_date"].notna()].copy()
        df = df.sort_values(["signal_date", "attempt_rank", "code"], kind="stable").reset_index(drop=True)
    return df


def _listing_market_map(listing_df: pd.DataFrame | None) -> dict[str, str]:
    if listing_df is None or not isinstance(listing_df, pd.DataFrame) or listing_df.empty:
        return {}
    ccol = next((c for c in ["Code", "Symbol", "code", "종목코드"] if c in listing_df.columns), None)
    mcol = next((c for c in ["Market", "market", "시장"] if c in listing_df.columns), None)
    if not ccol or not mcol:
        return {}
    q = listing_df[[ccol, mcol]].copy()
    q[ccol] = q[ccol].map(_norm_code)
    q[mcol] = q[mcol].astype(str).str.upper().replace({"코스피": "KOSPI", "유가": "KOSPI", "코스닥": "KOSDAQ"})
    q.loc[q[mcol].str.contains("KOSDAQ|코스닥", case=False, na=False), mcol] = "KOSDAQ"
    q.loc[q[mcol].str.contains("KOSPI|코스피|유가", case=False, na=False), mcol] = "KOSPI"
    return q.drop_duplicates(ccol).set_index(ccol)[mcol].to_dict()


def _build_eval_base(causal: pd.DataFrame, market_map: dict[str, str]) -> pd.DataFrame:
    if causal.empty:
        return pd.DataFrame()
    q = causal.copy()
    q["source"] = "FULL_UNIVERSE_FORMULA_TRUTH"
    q["entry_price"] = 0.0  # evaluator resolves exact signal-date close
    q["name"] = q.get("name", "").astype(str)
    q["market"] = q["code"].map(market_map).fillna("UNKNOWN")
    q["candidate_selected"] = q.get("analyze_returned", False).fillna(False).astype(bool)
    q["pre_true_count"] = q["formula_truth_bitmap"].map(lambda s: str(s).count("T"))
    q["post_true_count"] = q["formula_post_truth_bitmap"].map(lambda s: str(s).count("T"))
    q["pre_has_any_true"] = q["pre_true_count"].gt(0)
    q["post_has_any_true"] = q["post_true_count"].gt(0)
    return q


def _evaluate_rows(base: pd.DataFrame, evaluator: Callable[[pd.DataFrame], pd.DataFrame] | None) -> pd.DataFrame:
    if base.empty:
        return pd.DataFrame()
    # PnL is needed only when at least one PRE formula was truly active. All-F rows remain
    # in the raw truth ledger and zero-hit denominator, avoiding unnecessary price requests.
    target = base[base["pre_has_any_true"]].copy()
    if target.empty:
        return target
    cols = [c for c in ["signal_date", "code", "name", "entry_price", "source", "market", "candidate_selected",
                        "formula_truth_registry_sha256", "formula_truth_bitmap", "formula_truth_true",
                        "formula_truth_scores_json", "formula_truth_errors_json", "formula_truth_missing_json",
                        "formula_truth_inputs_json", "formula_post_truth_registry_sha256", "formula_post_truth_bitmap",
                        "formula_post_truth_true", "formula_post_truth_scores_json", "formula_post_truth_errors_json",
                        "formula_post_truth_missing_json", "formula_post_truth_inputs_json", "combo_invocation",
                        "pre_true_count", "post_true_count"] if c in target.columns]
    target = target[cols].copy()
    try:
        out = evaluator(target) if callable(evaluator) else pd.DataFrame()
    except Exception as exc:
        out = target.copy()
        out["eval_status"] = f"EVALUATOR_ERROR:{type(exc).__name__}:{exc}"
    return out if isinstance(out, pd.DataFrame) else pd.DataFrame()


def _explode(eval_df: pd.DataFrame, formulas: list[dict]) -> pd.DataFrame:
    if eval_df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in eval_df.iterrows():
        bits = str(r.get("formula_truth_bitmap", "") or "")
        for c in formulas:
            i = int(c["index"])
            if i >= len(bits) or bits[i] != "T":
                continue
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
    d1 = pd.to_numeric(g.get("next1_close_ret"), errors="coerce")
    d3 = pd.to_numeric(g.get("next3_close_ret"), errors="coerce")
    d5 = pd.to_numeric(g.get("next5_close_ret"), errors="coerce")
    date_means = g.assign(_d3=d3).groupby("signal_date")["_d3"].mean() if "signal_date" in g.columns else pd.Series(dtype=float)
    return {
        "formula": formula,
        "regime": regime,
        "n": int(len(g)),
        "stocks": int(g["code"].nunique()) if "code" in g.columns else 0,
        "signal_days": int(g["signal_date"].nunique()) if "signal_date" in g.columns else 0,
        "selected_rows": int(pd.Series(g.get("candidate_selected", False)).fillna(False).astype(bool).sum()),
        "d1_mean": d1.mean(),
        "d3_mean": d3.mean(),
        "d3_median": d3.median(),
        "d3_trim10": _trim_mean(d3),
        "d3_ex_top2": _ex_top2_mean(d3),
        "d5_mean": d5.mean(),
        "d3_cost20bp": d3.mean() - 0.20 if d3.notna().any() else np.nan,
        "d3_cost50bp": d3.mean() - 0.50 if d3.notna().any() else np.nan,
        "positive_signal_days": int((date_means > 0).sum()) if len(date_means) else 0,
        "positive_signal_day_rate": float((date_means > 0).mean() * 100.0) if len(date_means) else np.nan,
        "plus3_first_rate": _bool_rate(g["plus3_first"] if "plus3_first" in g.columns else pd.Series(dtype=bool)),
        "stop_first_rate": _bool_rate(g["stop_first"] if "stop_first" in g.columns else pd.Series(dtype=bool)),
        "market_excess3_mean": pd.to_numeric(g.get("market_excess3"), errors="coerce").mean() if "market_excess3" in g.columns else np.nan,
        "market_excess3_median": pd.to_numeric(g.get("market_excess3"), errors="coerce").median() if "market_excess3" in g.columns else np.nan,
    }


def _formula_summary(exploded: pd.DataFrame, formulas: list[dict], causal: pd.DataFrame) -> pd.DataFrame:
    rows = []
    total_causal = len(causal)
    for c in formulas:
        f = c["combination"]
        g = exploded[exploded["formula"].eq(f)] if not exploded.empty else pd.DataFrame()
        if g.empty:
            rows.append({
                "formula": f, "formula_index": c["index"], "grade": c.get("grade", ""),
                "base_score": c.get("base_score"), "n": 0, "stocks": 0, "signal_days": 0,
                "hit_rate_of_combo_reached": 0.0 if total_causal else np.nan,
                "status": "NO_HIT_EXPLORATORY",
            })
        else:
            z = _perf(g, f)
            z.update({"formula_index": c["index"], "grade": c.get("grade", ""), "base_score": c.get("base_score"),
                      "hit_rate_of_combo_reached": len(g) / total_causal * 100.0 if total_causal else np.nan,
                      "status": "EXPLORATORY_ONLY"})
            rows.append(z)
    out = pd.DataFrame(rows)
    if "d3_median" not in out.columns:
        out["d3_median"] = np.nan
    return out.sort_values(["n", "d3_median"], ascending=[False, False], na_position="last").reset_index(drop=True)


def _post_only_rows(causal: pd.DataFrame, formulas: list[dict]) -> pd.DataFrame:
    rows = []
    for _, r in causal.iterrows():
        pre = str(r.get("formula_truth_bitmap", "") or "")
        post = str(r.get("formula_post_truth_bitmap", "") or "")
        for c in formulas:
            i = int(c["index"])
            a = pre[i] if i < len(pre) else "?"
            b = post[i] if i < len(post) else "?"
            if a == "F" and b == "T":
                rows.append({
                    "signal_date": r.get("signal_date"), "code": r.get("code"), "name": r.get("name", ""),
                    "market": r.get("market", "UNKNOWN"), "formula_index": i, "formula": c["combination"],
                    "pre": a, "post": b, "candidate_selected": bool(r.get("analyze_returned", False)),
                    "combo_invocation": r.get("combo_invocation", 0),
                })
    return pd.DataFrame(rows)


def _error_rows(causal: pd.DataFrame, formulas: list[dict]) -> pd.DataFrame:
    rows = []
    fmap = {int(c["index"]): c["combination"] for c in formulas}
    for _, r in causal.iterrows():
        for phase, col in [("PRE", "formula_truth_errors_json"), ("POST", "formula_post_truth_errors_json")]:
            for e in _loads(r.get(col, "[]"), []):
                z = dict(e or {})
                z.update({"phase": phase, "signal_date": r.get("signal_date"), "code": r.get("code"), "name": r.get("name", "")})
                if not z.get("formula") and z.get("index") is not None:
                    z["formula"] = fmap.get(int(z["index"]), "")
                rows.append(z)
    return pd.DataFrame(rows)


def _attach_regime(exploded: pd.DataFrame, output_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    if exploded.empty:
        return exploded, pd.DataFrame(), []
    audit: list[dict] = []
    try:
        import market_regime_excess_research as mr
        minimal = exploded[["signal_date", "market"]].drop_duplicates().copy()
        bench, src_audit = mr._load_benchmarks(minimal, output_dir)  # type: ignore[attr-defined]
        audit.extend(src_audit)
        feat = mr._benchmark_feature_table(bench)  # type: ignore[attr-defined]
        if feat.empty:
            x = exploded.copy(); x["market_regime_causal"] = "UNKNOWN"; x["market_fwd_ret3"] = np.nan; x["market_excess3"] = np.nan
            return x, pd.DataFrame(), audit
        x = exploded.copy()
        x["signal_date"] = pd.to_datetime(x["signal_date"], errors="coerce").dt.normalize()
        feat["signal_date"] = pd.to_datetime(feat["signal_date"], errors="coerce").dt.normalize()
        x = x.merge(feat[["signal_date", "market", "market_regime_causal", "market_fwd_ret1", "market_fwd_ret3", "market_fwd_ret5"]],
                    on=["signal_date", "market"], how="left")
        x["market_regime_causal"] = x["market_regime_causal"].fillna("UNKNOWN")
        x["market_excess3"] = pd.to_numeric(x.get("next3_close_ret"), errors="coerce") - pd.to_numeric(x.get("market_fwd_ret3"), errors="coerce")
        rows = []
        for (f, regime), g in x.groupby(["formula", "market_regime_causal"], dropna=False):
            rows.append(_perf(g, str(f), str(regime)))
        return x, pd.DataFrame(rows).sort_values(["n", "market_excess3_median"], ascending=[False, False], na_position="last"), audit
    except Exception as exc:
        audit.append({"source_type": "REGIME_JOIN", "source": "market_regime_excess_research", "status": f"ERROR:{type(exc).__name__}:{exc}", "rows": 0})
        x = exploded.copy(); x["market_regime_causal"] = "UNKNOWN"; x["market_excess3"] = np.nan
        return x, pd.DataFrame(), audit


def _insert_block(text: str, block: str) -> str:
    s = str(text or "")
    if HEADER in s:
        start = s.find(HEADER)
        stops = [s.find(h, start + len(HEADER)) for h in ["\n🌙 [전일 야간환경", "\n🏆 [V48/V61", "\n🛡️ [손절거리"] if s.find(h, start + len(HEADER)) >= 0]
        s = s[:start].rstrip() + ("\n\n" + s[min(stops):].lstrip("\n") if stops else "")
    anchors = ["\n🌙 [전일 야간환경", "\n🏆 [V48/V61", "\n🛡️ [손절거리"]
    pos = [s.find(a) for a in anchors if s.find(a) >= 0]
    if pos:
        k = min(pos)
        return s[:k].rstrip() + "\n\n" + block + "\n" + s[k:]
    return s.rstrip() + "\n\n" + block


def run_backtest(
    capture_rows: Iterable[dict],
    attempt_rows: Iterable[dict],
    *,
    output_dir: str = "reports",
    base_report: str = "",
    evaluator: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    listing_df: pd.DataFrame | None = None,
) -> tuple[str, dict[str, pd.DataFrame]]:
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    reg = _registry()
    formulas = reg.get("combos", [])
    expected = int(reg.get("combo_count", len(formulas)))
    reg_sha = str(reg.get("registry_sha256", ""))

    raw = _normalize_capture(capture_rows)
    attempts = _normalize_attempts(attempt_rows)
    raw.to_csv(out / RAW_FILE, index=False, encoding="utf-8-sig")
    attempts.to_csv(out / ATTEMPT_FILE, index=False, encoding="utf-8-sig")

    if raw.empty:
        block = "\n".join([HEADER, f"📌 {VERSION} · RESEARCH_ONLY=True", "- 캡처된 COMBO_TABLE 호출이 없습니다. 전체 유니버스 전수감사 결과를 만들 수 없습니다."])
        (out / REPORT_FILE).write_text(block, encoding="utf-8")
        return _insert_block(base_report, block), {"raw": raw, "attempts": attempts}

    raw["pre_len"] = raw["formula_truth_bitmap"].str.len()
    raw["post_len"] = raw["formula_post_truth_bitmap"].str.len()
    raw["pre_registry_ok"] = raw["formula_truth_registry_sha256"].eq(reg_sha)
    raw["post_registry_ok"] = raw["formula_post_truth_registry_sha256"].eq(reg_sha)
    raw["pre_contract_ok"] = raw["pre_len"].eq(expected) & raw["pre_registry_ok"]
    raw["post_contract_ok"] = raw["post_len"].eq(expected) & raw["post_registry_ok"]

    # One causal active-score invocation per date/code. Repeated invocations are audited separately.
    causal = raw.sort_values(["signal_date", "code", "combo_invocation"], kind="stable").drop_duplicates(["signal_date", "code"], keep="first").copy()
    market_map = _listing_market_map(listing_df)
    causal["market"] = causal["code"].map(market_map).fillna("UNKNOWN")
    base = _build_eval_base(causal, market_map)
    eval_df = _evaluate_rows(base, evaluator)
    eval_df.to_csv(out / EVAL_FILE, index=False, encoding="utf-8-sig")

    exploded = _explode(eval_df, formulas)
    exploded, regime_perf, regime_audit = _attach_regime(exploded, out)
    exploded.to_csv(out / EXPLODED_FILE, index=False, encoding="utf-8-sig")
    formula_perf = _formula_summary(exploded, formulas, causal)
    formula_perf.to_csv(out / PERF_FILE, index=False, encoding="utf-8-sig")
    regime_perf.to_csv(out / REGIME_FILE, index=False, encoding="utf-8-sig")

    post_only = _post_only_rows(causal, formulas)
    post_only.to_csv(out / POST_ONLY_FILE, index=False, encoding="utf-8-sig")
    errors = _error_rows(causal, formulas)
    errors.to_csv(out / ERROR_FILE, index=False, encoding="utf-8-sig")
    pre_errors = errors[errors["phase"].eq("PRE")].copy() if not errors.empty and "phase" in errors.columns else pd.DataFrame()
    post_errors = errors[errors["phase"].eq("POST")].copy() if not errors.empty and "phase" in errors.columns else pd.DataFrame()

    attempted_n = len(attempts)
    attempted_days = attempts["signal_date"].nunique() if not attempts.empty else 0
    combo_rows = len(causal)
    combo_days = causal["signal_date"].nunique()
    combo_codes = causal["code"].nunique()
    reached_attempts = attempts["combo_calls"].fillna(0).astype(float).gt(0).sum() if "combo_calls" in attempts.columns else combo_rows
    no_combo = attempted_n - int(reached_attempts) if attempted_n else 0
    selected_attempts = attempts["analyze_returned"].fillna(False).astype(bool).sum() if "analyze_returned" in attempts.columns else 0
    duplicate_invocations = int(len(raw) - combo_rows)
    duplicate_conflict_keys = 0
    duplicate_compare_cols = [c for c in ["formula_truth_bitmap", "formula_post_truth_bitmap", "formula_truth_registry_sha256", "formula_post_truth_registry_sha256", "analyze_returned"] if c in raw.columns]
    if duplicate_invocations > 0:
        for _, dg in raw.groupby(["signal_date", "code"], dropna=False):
            if len(dg) <= 1:
                continue
            if any(dg[c].fillna("").astype(str).nunique() > 1 for c in duplicate_compare_cols):
                duplicate_conflict_keys += 1
    duplicate_contract_status = "INVALID_DUPLICATE_CALL" if duplicate_conflict_keys else ("DEDUPED_VALID" if duplicate_invocations else "NO_DUPLICATE_CALL")
    pre_complete = int(causal["pre_contract_ok"].sum())
    post_complete = int(causal["post_contract_ok"].sum())
    true_rows = int(causal["formula_truth_bitmap"].map(lambda s: "T" in str(s)).sum())
    evaluated_rows = int((eval_df.get("eval_status", pd.Series(dtype=str)) == "OK").sum()) if not eval_df.empty else 0
    signal_days = int(exploded["signal_date"].nunique()) if not exploded.empty else 0
    policy_ready = evaluated_rows >= MIN_POLICY_ROWS and signal_days >= MIN_POLICY_DATES
    # PRE is the actual score-time contract. POST is a later diagnostic stage and may be
    # unreachable for rows that legitimately exit after scoring; its coverage is reported,
    # but it must not invalidate the causal PRE formula ledger.
    contract_valid = combo_rows > 0 and pre_complete == combo_rows and pre_errors.empty and duplicate_conflict_keys == 0

    coverage = pd.DataFrame([{
        "version": VERSION,
        "registry_sha256": reg_sha,
        "formula_count": expected,
        "attempted_rows": attempted_n,
        "attempted_dates": attempted_days,
        "combo_reached_rows": combo_rows,
        "combo_reached_codes": combo_codes,
        "combo_reached_dates": combo_days,
        "pre_combo_filtered_rows": no_combo,
        "selected_candidate_attempts": int(selected_attempts),
        "duplicate_combo_invocations": duplicate_invocations,
        "duplicate_combo_conflict_keys": duplicate_conflict_keys,
        "duplicate_combo_contract_status": duplicate_contract_status,
        "pre_complete_rows": pre_complete,
        "post_complete_rows": post_complete,
        "rows_with_pre_true": true_rows,
        "evaluated_pre_true_rows": evaluated_rows,
        "exploded_true_formula_rows": len(exploded),
        "post_only_timing_breaches": len(post_only),
        "condition_errors": len(errors),
        "pre_condition_errors": len(pre_errors),
        "post_condition_errors": len(post_errors),
        "post_coverage_pct": (post_complete / combo_rows * 100.0) if combo_rows else np.nan,
        "contract_valid": contract_valid,
        "policy_ready": policy_ready,
    }])
    coverage.to_csv(out / COVERAGE_FILE, index=False, encoding="utf-8-sig")

    data_rows = [
        {"item": "UNIVERSE_ATTEMPTS", "available": attempted_n, "status": "OK" if attempted_n else "MISSING"},
        {"item": "COMBO_REACHED", "available": combo_rows, "status": "OK" if combo_rows else "MISSING"},
        {"item": "PRE_BITMAP_COMPLETE", "available": pre_complete, "status": "OK" if pre_complete == combo_rows else "PARTIAL"},
        {"item": "POST_BITMAP_COMPLETE", "available": post_complete, "status": "OK" if post_complete == combo_rows else "PARTIAL"},
        {"item": "PRE_TRUE_EVALUATED", "available": evaluated_rows, "status": "OK" if evaluated_rows else "EMPTY"},
        {"item": "MARKET_MAPPED", "available": int(causal["market"].ne("UNKNOWN").sum()), "status": "OK" if causal["market"].ne("UNKNOWN").any() else "MISSING"},
    ]
    data_rows.extend(regime_audit)
    pd.DataFrame(data_rows).to_csv(out / DATA_FILE, index=False, encoding="utf-8-sig")

    lines = [
        HEADER,
        f"📌 {VERSION} · FULL_UNIVERSE_FORMULA_TRUTH_PERFORMANCE_REGIME_AUDIT · RESEARCH_ONLY=True",
        "- 목적: 최종 후보만 보지 않고 Direct Replay가 실제로 분석한 전 종목에서 66개 COMBO_TABLE 식의 PRE/POST 진실값과 이후 성과를 분리합니다.",
        "- LIVE 점수·순위·후보·AI 호출·진입·익절·손절 변경 0. POST-only 점등은 성과 신호에서 제외하고 계산순서 위반 후보로만 집계합니다.",
        f"📁 분석시도 {attempted_n}행·{attempted_days}일 | COMBO 도달 {combo_rows}행·{combo_codes}종목·{combo_days}일 | COMBO 이전 종료 {no_combo}행 | 최종후보 반환 {int(selected_attempts)}행",
        f"🧾 계약: PRE {pre_complete}/{combo_rows} · PRE error {len(pre_errors)} | POST 진단도달 {post_complete}/{combo_rows} · POST error {len(post_errors)} · 중복호출 {duplicate_invocations}({duplicate_contract_status}) | {'✅ VALID' if contract_valid else '⛔ INVALID'}",
        f"📈 PRE 실제점등 종목행 {true_rows} · 성과평가 OK {evaluated_rows} · formula explode {len(exploded)}행 | 정책판정 {'✅ READY' if policy_ready else '⏳ NOT_READY'} ({evaluated_rows}행·{signal_days}일 / 최소 {MIN_POLICY_ROWS}행·{MIN_POLICY_DATES}일)",
    ]

    if no_combo > 0:
        lines += [
            "⚠️ [COMBO 이전 종료]",
            f"- {no_combo}행은 analyze_final 내부의 선행 데이터/품질 조건에서 종료되어 COMBO_TABLE 신호맵이 생성되지 않았습니다. 이를 66개 식 FALSE로 위장하지 않습니다.",
        ]

    lines.append("🔍 [PRE 실제점등 상위 검색식 · 후보선정 전 전체 분석행]")
    hit = formula_perf[pd.to_numeric(formula_perf.get("n"), errors="coerce").fillna(0).gt(0)].copy()
    if hit.empty:
        lines.append("- PRE 시점에 점등된 검색식이 없습니다.")
    else:
        for _, r in hit.sort_values(["n", "d3_median"], ascending=[False, False], na_position="last").head(12).iterrows():
            lines.append(
                f"- {r['formula']}: n{int(r['n'])}·종목{int(r.get('stocks',0))}·날짜{int(r.get('signal_days',0))} | "
                f"D1 {_fmt(r.get('d1_mean'))} / D3 평균 {_fmt(r.get('d3_mean'))}·중앙 {_fmt(r.get('d3_median'))}·절사 {_fmt(r.get('d3_trim10'))}·상2제외 {_fmt(r.get('d3_ex_top2'))} | "
                f"D5 {_fmt(r.get('d5_mean'))} | +3 {_rate(r.get('plus3_first_rate'))}/SL {_rate(r.get('stop_first_rate'))} | 후보선정 {int(r.get('selected_rows',0))}"
            )

    lines.append("⏱️ [POST-only 뒤늦은 점등 · 계산순서 감사]")
    if post_only.empty:
        lines.append("- PRE F → POST T 검색식이 없습니다.")
    else:
        pc = post_only.groupby("formula").agg(rows=("formula", "size"), stocks=("code", "nunique"), dates=("signal_date", "nunique")).reset_index()
        for _, r in pc.sort_values(["rows", "dates"], ascending=False).head(10).iterrows():
            lines.append(f"- {r['formula']}: {int(r['rows'])}건·종목{int(r['stocks'])}·날짜{int(r['dates'])} | 실제 점수선정에는 미반영")
        for _, r in post_only.head(5).iterrows():
            lines.append(f"  ↳ {pd.Timestamp(r['signal_date']).strftime('%Y-%m-%d')} {r['code']} {r.get('name','')} · {r['formula']} PRE F→POST T")

    lines.append("🌡️ [검색식 × 시장국면 · n≥3]")
    qreg = regime_perf[pd.to_numeric(regime_perf.get("n"), errors="coerce").fillna(0).ge(3)].copy() if not regime_perf.empty else pd.DataFrame()
    if qreg.empty:
        lines.append("- 시장 매핑 또는 n≥3 표본이 부족합니다.")
    else:
        qreg = qreg.sort_values(["market_excess3_median", "n"], ascending=[False, False], na_position="last").head(10)
        for _, r in qreg.iterrows():
            lines.append(
                f"- {r['formula']} · {r['regime']}: n{int(r['n'])} | D3 중앙 {_fmt(r.get('d3_median'))} | "
                f"지수초과 평균 {_fmt(r.get('market_excess3_mean'))}·중앙 {_fmt(r.get('market_excess3_median'))} | 상2제외 {_fmt(r.get('d3_ex_top2'))}"
            )

    if not policy_ready:
        lines += [
            "⏳ [판정 제한]",
            f"- 계산/전달이 VALID여도 독립 신호일이 {signal_days}일이므로 검색식 삭제·PERFORMANCE_FAIL·LIVE 승격은 금지합니다.",
            "- 이번 표는 계산식이 실제로 언제 점등되는지, 후보선정 전에 어떤 종목을 잡는지, 시장국면별 방향성이 있는지를 확인하는 탐색표입니다.",
        ]

    lines += [
        "🧭 [다음 판정 순서]",
        "- ① POST-only 반복식을 계산순서 결함으로 확정 → ② 해당 원천값을 PRE 평가 전에 이동한 SHADOW 재계산 → ③ 동일 모집단 PRE-OLD vs PRE-FIX 비교 → ④ 10일 이상 OOS 후에만 검색식 조건 수정 검토",
        f"- Actions CSV: {ATTEMPT_FILE} · {RAW_FILE} · {COVERAGE_FILE} · {EVAL_FILE} · {EXPLODED_FILE} · {PERF_FILE} · {REGIME_FILE} · {POST_ONLY_FILE} · {ERROR_FILE} · {DATA_FILE}",
    ]
    block = "\n".join(lines)
    (out / REPORT_FILE).write_text(block, encoding="utf-8")
    return _insert_block(base_report, block), {
        "raw": raw, "attempts": attempts, "coverage": coverage, "eval": eval_df,
        "exploded": exploded, "performance": formula_perf, "regime": regime_perf,
        "post_only": post_only, "errors": errors,
    }


def force_report(text: str, output_dir: str = "reports") -> str:
    p = Path(output_dir or "reports") / REPORT_FILE
    if not p.exists():
        return str(text or "")
    try:
        block = p.read_text(encoding="utf-8")
        return _insert_block(str(text or ""), block)
    except Exception:
        return str(text or "")
