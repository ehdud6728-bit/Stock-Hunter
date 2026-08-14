from __future__ import annotations

import gzip
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6.24"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🧪 [V24 인과 Universe × 전체분모 Formula Shadow × PATTERN_ONLY OOS × 청산기간 연구 · RESEARCH_ONLY]"
REPORT_FILE = "v73_v24_research_readiness_report.txt"

UNIVERSE_RECON_FILE = "v73_v24_universe_materialized_reconciliation.csv"
UNIVERSE_DAILY_FILE = "v73_v24_universe_daily_authority.csv"
FULL_DENOM_LONG_FILE = "v73_v24_full_denominator_formula_truth.csv.gz"
FULL_DENOM_SUMMARY_FILE = "v73_v24_full_denominator_formula_summary.csv"
FULL_DENOM_COVERAGE_FILE = "v73_v24_full_denominator_coverage.csv"
ATTEMPT_OUTCOME_FILE = "v73_v24_full_universe_attempt_outcomes.csv"
PATTERN_STABILITY_FILE = "v73_v24_pattern_only_stability.csv"
PATTERN_WF_FILE = "v73_v24_pattern_only_walkforward.csv"
VOL_EXIT_FILE = "v73_v24_volume_breakout_exit_policy.csv"
VOL_EXIT_SPLIT_FILE = "v73_v24_volume_breakout_exit_train_oos.csv"
MA_CONT_FILE = "v73_v24_ma_gc_continuation_policy.csv"
MA_CONT_SPLIT_FILE = "v73_v24_ma_gc_continuation_train_oos.csv"
PROMOTION_FILE = "v73_v24_paper_promotion_readiness.csv"
DATA_AVAIL_FILE = "v73_v24_data_availability.csv"

FORMULA_VOL_BREAK = "🚀거래량폭발초동돌파"
FORMULA_MA_GC = "💛종베단독(MA골크)"


def _out(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _norm_code(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-6:].zfill(6) if s else ""


def _num(v: Any, default: float = float("nan")) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _trim_mean(s: pd.Series, trim: float = 0.10) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values().reset_index(drop=True)
    if x.empty:
        return np.nan
    k = int(len(x) * trim)
    if k * 2 >= len(x):
        return float(x.mean())
    return float(x.iloc[k:len(x)-k].mean())


def _top_removed(s: pd.Series, n: int) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values(ascending=False)
    if len(x) <= n:
        return np.nan
    return float(x.iloc[n:].mean())


def _robust(g: pd.DataFrame, col: str = "ret3") -> dict[str, Any]:
    x = pd.to_numeric(g.get(col), errors="coerce").dropna() if isinstance(g, pd.DataFrame) and col in g.columns else pd.Series(dtype=float)
    dates = pd.to_datetime(g.get("signal_date"), errors="coerce").dt.normalize() if isinstance(g, pd.DataFrame) and "signal_date" in g.columns else pd.Series(dtype="datetime64[ns]")
    used_dates = dates.reindex(x.index) if len(x) and len(dates) else pd.Series(dtype="datetime64[ns]")
    return {
        "n": int(len(x)),
        "signal_days": int(used_dates.nunique()) if len(used_dates) else 0,
        "mean": float(x.mean()) if len(x) else np.nan,
        "median": float(x.median()) if len(x) else np.nan,
        "trim10": _trim_mean(x),
        "top2_removed": _top_removed(x, 2),
        "top5_removed": _top_removed(x, 5),
        "positive_rate": float((x > 0).mean() * 100.0) if len(x) else np.nan,
    }


def _read_csv(path: Path, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(path, **kwargs) if path.exists() else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _registry(combos: Iterable[dict] | None = None, registry_path: str | Path = "search_formula_contract_registry.json") -> list[dict[str, Any]]:
    if combos:
        rows = []
        for i, c in enumerate(combos):
            if not isinstance(c, dict):
                continue
            rows.append({"index": i, "combination": str(c.get("combination", "")), "grade": str(c.get("grade", "")), "base_score": c.get("score")})
        if rows:
            return rows
    try:
        p = json.loads(Path(registry_path).read_text(encoding="utf-8"))
        return list(p.get("combos") or [])
    except Exception:
        return []


def _bitmap_state(bitmap: Any, i: int) -> str:
    s = str(bitmap or "")
    if i >= len(s):
        return "UNKNOWN_BITMAP_SHORT"
    ch = s[i].upper()
    if ch == "T": return "TRUE"
    if ch == "F": return "FALSE"
    if ch == "E": return "ERROR"
    if ch in {"M", "?", "U"}: return "UNKNOWN"
    return "UNKNOWN"


def collect_materialized_payloads(output_dir: str | Path, materialized_module: Any = None) -> list[dict[str, Any]]:
    out = _out(output_dir)
    root = out / "v23_materialized"
    payloads: list[dict[str, Any]] = []
    if not root.exists():
        return payloads
    for p in sorted(root.glob("date_*.pkl.gz")):
        try:
            if materialized_module is not None and callable(getattr(materialized_module, "_load_pickle", None)):
                z = materialized_module._load_pickle(p)
            else:
                import pickle
                with gzip.open(p, "rb") as fh:
                    z = pickle.load(fh)
            if isinstance(z, dict):
                payloads.append(z)
        except Exception:
            continue
    return payloads


def rebuild_universe_from_materialized(output_dir: str | Path, payloads: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Authoritative parent reconstruction from all materialized dates.

    This fixes per-shard process-local accounting. No current listing is substituted here.
    """
    out = _out(output_dir)
    mems: list[pd.DataFrame] = []
    sums: list[pd.DataFrame] = []
    avs: list[pd.DataFrame] = []
    recon_rows: list[dict[str, Any]] = []
    for z in payloads:
        ds = pd.to_datetime(z.get("signal_date"), errors="coerce").normalize() if z.get("signal_date") else pd.NaT
        m = z.get("universe_membership") if isinstance(z.get("universe_membership"), pd.DataFrame) else pd.DataFrame()
        s = z.get("universe_summary") if isinstance(z.get("universe_summary"), pd.DataFrame) else pd.DataFrame()
        a = z.get("universe_availability") if isinstance(z.get("universe_availability"), pd.DataFrame) else pd.DataFrame()
        for q in (m, s, a):
            if isinstance(q, pd.DataFrame) and not q.empty and "signal_date" in q.columns:
                q["signal_date"] = pd.to_datetime(q["signal_date"], errors="coerce").dt.normalize()
        if not m.empty:
            # A materialized date is authoritative only for its own signal date.
            if pd.notna(ds) and "signal_date" in m.columns:
                m = m[m["signal_date"].eq(ds)].copy()
            mems.append(m)
        if not s.empty:
            if pd.notna(ds) and "signal_date" in s.columns:
                s = s[s["signal_date"].eq(ds)].copy()
            sums.append(s)
        if not a.empty:
            if pd.notna(ds) and "signal_date" in a.columns:
                a = a[a["signal_date"].eq(ds)].copy()
            avs.append(a)
        recon_rows.append({
            "signal_date": ds,
            "materialized_file_date": ds,
            "membership_rows": int(len(m)),
            "summary_rows": int(len(s)),
            "availability_rows": int(len(a)),
            "payload_shard_index": z.get("shard_index"),
            "source_fingerprint": z.get("source_fingerprint", ""),
            "config_fingerprint": z.get("config_fingerprint", ""),
            "status": "OK" if len(m) > 0 and len(a) > 0 else "MISSING_SIDECAR",
        })
    mem = pd.concat(mems, ignore_index=True, sort=False) if mems else pd.DataFrame()
    summ = pd.concat(sums, ignore_index=True, sort=False) if sums else pd.DataFrame()
    avail = pd.concat(avs, ignore_index=True, sort=False) if avs else pd.DataFrame()
    if not mem.empty:
        mem["signal_date"] = pd.to_datetime(mem["signal_date"], errors="coerce").dt.normalize()
        if "code" in mem.columns: mem["code"] = mem["code"].map(_norm_code)
        mem = mem.drop_duplicates([c for c in ["signal_date", "code"] if c in mem.columns], keep="last")
        mem = mem.sort_values([c for c in ["signal_date", "universe_rank", "code"] if c in mem.columns], kind="stable")
    if not summ.empty:
        summ["signal_date"] = pd.to_datetime(summ["signal_date"], errors="coerce").dt.normalize()
        summ = summ.drop_duplicates(["signal_date"], keep="last").sort_values("signal_date")
    if not avail.empty:
        avail["signal_date"] = pd.to_datetime(avail["signal_date"], errors="coerce").dt.normalize()
        avail = avail.drop_duplicates(["signal_date"], keep="last").sort_values("signal_date")
    # Rewrite legacy filenames so downstream diagnostics see the complete 24-date authority.
    mem.to_csv(out / "v73_universe_asof_membership.csv", index=False, encoding="utf-8-sig")
    summ.to_csv(out / "v73_universe_asof_summary.csv", index=False, encoding="utf-8-sig")
    avail.to_csv(out / "v73_universe_data_availability.csv", index=False, encoding="utf-8-sig")
    recon = pd.DataFrame(recon_rows)
    recon.to_csv(out / UNIVERSE_RECON_FILE, index=False, encoding="utf-8-sig")
    daily = pd.DataFrame()
    if not avail.empty:
        daily = avail.copy()
        daily["is_valid_causal_asof"] = daily.get("status", "").astype(str).eq("VALID_CAUSAL_ASOF")
        daily["is_fallback"] = ~daily["is_valid_causal_asof"]
        if not summ.empty:
            cols = [c for c in ["signal_date", "final_universe_rows", "core_rows", "event_expansion_rows", "history_days", "eligible_rows"] if c in summ.columns]
            daily = daily.merge(summ[cols], on="signal_date", how="left")
    daily.to_csv(out / UNIVERSE_DAILY_FILE, index=False, encoding="utf-8-sig")
    return mem, summ, avail, recon


def collect_attempt_capture_shadow(payloads: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    attempts: list[dict[str, Any]] = []
    captures: list[dict[str, Any]] = []
    shadow: list[dict[str, Any]] = []
    for z in payloads:
        attempts.extend([dict(x) for x in (z.get("attempt_rows") or []) if isinstance(x, dict)])
        captures.extend([dict(x) for x in (z.get("capture_rows") or []) if isinstance(x, dict)])
        side = z.get("runtime_sidecars") if isinstance(z.get("runtime_sidecars"), dict) else {}
        shadow.extend([dict(x) for x in (side.get("V24_PRECOMBO_SHADOW_ROWS") or []) if isinstance(x, dict)])
    a = pd.DataFrame(attempts)
    c = pd.DataFrame(captures)
    s = pd.DataFrame(shadow)
    for q in (a, c, s):
        if not q.empty:
            if "signal_date" in q.columns: q["signal_date"] = pd.to_datetime(q["signal_date"], errors="coerce").dt.normalize()
            if "code" in q.columns: q["code"] = q["code"].map(_norm_code)
    if not a.empty:
        a = a.drop_duplicates([c for c in ["signal_date", "code", "attempt_rank"] if c in a.columns], keep="last")
    if not c.empty:
        _sort_cols = [x for x in ["signal_date", "code", "combo_invocation"] if x in c.columns]
        if _sort_cols: c = c.sort_values(_sort_cols, kind="stable")
        _dedup_cols = [x for x in ["signal_date", "code"] if x in c.columns]
        if _dedup_cols: c = c.drop_duplicates(_dedup_cols, keep="first")
    if not s.empty:
        _sort_cols = [x for x in ["signal_date", "code"] if x in s.columns]
        if _sort_cols: s = s.sort_values(_sort_cols, kind="stable")
        _dedup_cols = [x for x in ["signal_date", "code"] if x in s.columns]
        if _dedup_cols: s = s.drop_duplicates(_dedup_cols, keep="last")
    return a, c, s


def build_full_denominator_truth(
    output_dir: str | Path,
    attempts: pd.DataFrame,
    captures: pd.DataFrame,
    shadow: pd.DataFrame,
    formulas: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    out = _out(output_dir)
    if attempts.empty or not formulas:
        empty = pd.DataFrame()
        empty.to_csv(out / FULL_DENOM_SUMMARY_FILE, index=False)
        empty.to_csv(out / FULL_DENOM_COVERAGE_FILE, index=False)
        return empty, empty, empty
    cap_map = {(r.signal_date, r.code): r._asdict() for r in captures.itertuples(index=False)} if not captures.empty else {}
    sh_map = {(r.signal_date, r.code): r._asdict() for r in shadow.itertuples(index=False)} if not shadow.empty else {}
    rows: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}
    for r in attempts.itertuples(index=False):
        rd = r._asdict()
        key = (rd.get("signal_date"), rd.get("code"))
        rec = cap_map.get(key)
        source = "ACTIVE_PRE"
        if rec is None:
            rec = sh_map.get(key)
            if rec is not None and str(rec.get("formula_truth_bitmap", "")):
                source = "SHADOW_BYPASS_PRECOMBO"
            elif rec is not None:
                source = "UNKNOWN_PRECOMBO_SHADOW_UNRESOLVED"
                rec = None
            else:
                source = "UNKNOWN_NOT_EVALUATED"
        source_counts[source] = source_counts.get(source, 0) + 1
        bitmap = rec.get("formula_truth_bitmap", "") if rec else ""
        for f in formulas:
            idx = int(f.get("index", 0))
            state = _bitmap_state(bitmap, idx) if rec is not None else (
                "UNKNOWN_ANALYZE_ERROR" if str(rd.get("status", "")) == "ANALYZE_ERROR" else "UNKNOWN_PRECOMBO_UNRESOLVED"
            )
            rows.append({
                "signal_date": rd.get("signal_date"), "code": rd.get("code"), "name": rd.get("name", ""),
                "attempt_rank": rd.get("attempt_rank"), "attempt_status": rd.get("status", ""),
                "truth_source": source, "formula_index": idx, "formula": f.get("combination", ""),
                "formula_grade": f.get("grade", ""), "truth_state": state,
                "is_true": state == "TRUE", "is_false": state == "FALSE",
                "is_known": state in {"TRUE", "FALSE"},
                "shadow_only": source == "SHADOW_BYPASS_PRECOMBO",
                "research_only": True, "live_logic_changed": False,
            })
    long = pd.DataFrame(rows)
    long.to_csv(out / FULL_DENOM_LONG_FILE, index=False, encoding="utf-8-sig", compression="gzip")
    summary_rows = []
    for f, g in long.groupby("formula", dropna=False):
        known = g[g["is_known"]]
        summary_rows.append({
            "formula": f,
            "formula_index": int(g["formula_index"].iloc[0]),
            "attempt_rows": int(g[["signal_date", "code"]].drop_duplicates().shape[0]),
            "known_rows": int(known.shape[0]),
            "unknown_rows": int((~g["is_known"]).sum()),
            "true_rows": int(g["is_true"].sum()),
            "active_pre_true_rows": int((g["is_true"] & g["truth_source"].eq("ACTIVE_PRE")).sum()),
            "shadow_only_true_rows": int((g["is_true"] & g["truth_source"].eq("SHADOW_BYPASS_PRECOMBO")).sum()),
            "known_coverage_pct": float(g["is_known"].mean() * 100.0),
            "true_rate_known_pct": float(known["is_true"].mean() * 100.0) if len(known) else np.nan,
        })
    summary = pd.DataFrame(summary_rows).sort_values(["true_rows", "active_pre_true_rows"], ascending=False)
    summary.to_csv(out / FULL_DENOM_SUMMARY_FILE, index=False, encoding="utf-8-sig")
    coverage = pd.DataFrame([{
        "attempt_rows": int(attempts[["signal_date", "code"]].drop_duplicates().shape[0]),
        "signal_days": int(attempts["signal_date"].nunique()),
        "formula_count": len(formulas),
        "truth_cells": int(len(long)),
        "active_pre_attempts": int(source_counts.get("ACTIVE_PRE", 0)),
        "shadow_bypass_attempts": int(source_counts.get("SHADOW_BYPASS_PRECOMBO", 0)),
        "unresolved_attempts": int(source_counts.get("UNKNOWN_NOT_EVALUATED", 0) + source_counts.get("UNKNOWN_PRECOMBO_SHADOW_UNRESOLVED", 0)),
        "shadow_unresolved_attempts": int(source_counts.get("UNKNOWN_PRECOMBO_SHADOW_UNRESOLVED", 0)),
        "known_cells": int(long["is_known"].sum()),
        "unknown_cells": int((~long["is_known"]).sum()),
        "known_cell_coverage_pct": float(long["is_known"].mean() * 100.0),
        "contract": "ACTIVE_PRE_OR_RESEARCH_SHADOW; unresolved stays UNKNOWN, never FALSE",
    }])
    coverage.to_csv(out / FULL_DENOM_COVERAGE_FILE, index=False, encoding="utf-8-sig")
    return long, summary, coverage


def _find_price_cols(df: pd.DataFrame) -> tuple[str | None, str | None, str | None, str | None]:
    def pick(names):
        return next((c for c in names if c in df.columns), None)
    return pick(["Open", "open", "시가"]), pick(["High", "high", "고가"]), pick(["Low", "low", "저가"]), pick(["Close", "close", "종가"])


def evaluate_attempt_outcomes(output_dir: str | Path, attempts: pd.DataFrame, price_reader: Callable[..., pd.DataFrame] | None) -> pd.DataFrame:
    out = _out(output_dir)
    if attempts.empty or not callable(price_reader):
        q = pd.DataFrame(); q.to_csv(out / ATTEMPT_OUTCOME_FILE, index=False); return q
    events = attempts[[c for c in ["signal_date", "code", "name", "attempt_status"] if c in attempts.columns]].copy()
    if "attempt_status" not in events.columns and "status" in attempts.columns:
        events["attempt_status"] = attempts["status"]
    events["signal_date"] = pd.to_datetime(events["signal_date"], errors="coerce").dt.normalize()
    events["code"] = events["code"].map(_norm_code)
    events = events.dropna(subset=["signal_date"]).drop_duplicates(["signal_date", "code"], keep="last")
    rows: list[dict[str, Any]] = []
    by_code = {c: g.copy() for c, g in events.groupby("code")}
    for code, eg in by_code.items():
        try:
            df = price_reader(code, days=900)
        except TypeError:
            try: df = price_reader(code, 900)
            except Exception: df = pd.DataFrame()
        except Exception:
            df = pd.DataFrame()
        if not isinstance(df, pd.DataFrame) or df.empty:
            for _, e in eg.iterrows():
                rows.append({**e.to_dict(), "outcome_status": "PRICE_MISSING"})
            continue
        q = df.copy()
        try:
            q.index = pd.to_datetime(q.index, errors="coerce")
            q = q[q.index.notna()].sort_index()
        except Exception:
            pass
        oc, hc, lc, cc = _find_price_cols(q)
        if not cc:
            for _, e in eg.iterrows(): rows.append({**e.to_dict(), "outcome_status": "CLOSE_COLUMN_MISSING"})
            continue
        norm_idx = pd.Series(q.index.normalize(), index=np.arange(len(q)))
        pos_map = {pd.Timestamp(d).normalize(): int(i) for i, d in norm_idx.items()}
        close = pd.to_numeric(q[cc], errors="coerce").to_numpy()
        high = pd.to_numeric(q[hc], errors="coerce").to_numpy() if hc else close.copy()
        low = pd.to_numeric(q[lc], errors="coerce").to_numpy() if lc else close.copy()
        openv = pd.to_numeric(q[oc], errors="coerce").to_numpy() if oc else close.copy()
        for _, e in eg.iterrows():
            ds = pd.Timestamp(e["signal_date"]).normalize()
            i = pos_map.get(ds)
            base = e.to_dict()
            if i is None or not math.isfinite(_num(close[i])) or close[i] <= 0:
                rows.append({**base, "outcome_status": "SIGNAL_BAR_MISSING"}); continue
            entry = float(close[i])
            z: dict[str, Any] = {**base, "entry_close": entry, "outcome_status": "OK"}
            for d in [1, 3, 5, 10]:
                j = i + d
                z[f"ret{d}"] = (float(close[j]) / entry - 1.0) * 100.0 if j < len(close) and math.isfinite(_num(close[j])) else np.nan
            if i + 1 < len(close):
                z["d1_open_ret"] = (float(openv[i+1]) / entry - 1.0) * 100.0 if math.isfinite(_num(openv[i+1])) else np.nan
                z["d1_high_ret"] = (float(high[i+1]) / entry - 1.0) * 100.0 if math.isfinite(_num(high[i+1])) else np.nan
                z["d1_low_ret"] = (float(low[i+1]) / entry - 1.0) * 100.0 if math.isfinite(_num(low[i+1])) else np.nan
            for horizon in [3, 5, 10]:
                end = min(len(q)-1, i + horizon)
                if end > i:
                    hh = pd.to_numeric(pd.Series(high[i+1:end+1]), errors="coerce")
                    ll = pd.to_numeric(pd.Series(low[i+1:end+1]), errors="coerce")
                    z[f"mfe{horizon}"] = (float(hh.max()) / entry - 1.0) * 100.0 if hh.notna().any() else np.nan
                    z[f"mae{horizon}"] = (float(ll.min()) / entry - 1.0) * 100.0 if ll.notna().any() else np.nan
            # Daily-bar first-touch ledger; same-day target+stop is explicitly ambiguous.
            for tp in [3.0, 5.0]:
                target_day = stop_day = None
                conflict_day = None
                for k in range(i+1, min(len(q), i+6)):
                    hi_ret = (float(high[k]) / entry - 1.0) * 100.0 if math.isfinite(_num(high[k])) else np.nan
                    lo_ret = (float(low[k]) / entry - 1.0) * 100.0 if math.isfinite(_num(low[k])) else np.nan
                    hit_t = math.isfinite(hi_ret) and hi_ret >= tp
                    hit_s = math.isfinite(lo_ret) and lo_ret <= -5.0
                    if hit_t and target_day is None: target_day = k-i
                    if hit_s and stop_day is None: stop_day = k-i
                    if hit_t and hit_s and conflict_day is None: conflict_day = k-i
                    if target_day is not None or stop_day is not None:
                        if not (target_day == stop_day): break
                z[f"tp{int(tp)}_first_day"] = target_day
                z[f"stop5_first_day_tp{int(tp)}"] = stop_day
                z[f"tp{int(tp)}_stop_path"] = (
                    "PATH_CONFLICT" if target_day is not None and stop_day is not None and target_day == stop_day else
                    "TARGET_FIRST" if target_day is not None and (stop_day is None or target_day < stop_day) else
                    "STOP_FIRST" if stop_day is not None else "NO_TOUCH"
                )
            rows.append(z)
    qout = pd.DataFrame(rows)
    qout.to_csv(out / ATTEMPT_OUTCOME_FILE, index=False, encoding="utf-8-sig")
    return qout


def attach_formula_performance(output_dir: str | Path, truth_long: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    out = _out(output_dir)
    summary = _read_csv(out / FULL_DENOM_SUMMARY_FILE)
    if truth_long.empty or outcomes.empty:
        return summary
    t = truth_long[truth_long["is_true"]].copy()
    ocols = [c for c in ["signal_date", "code", "ret1", "ret3", "ret5", "ret10", "mfe3", "mfe5", "mae3", "mae5", "d1_high_ret", "d1_low_ret", "tp3_stop_path", "tp5_stop_path"] if c in outcomes.columns]
    x = t.merge(outcomes[ocols], on=["signal_date", "code"], how="left")
    rows = []
    for (f, src), g in x.groupby(["formula", "truth_source"], dropna=False):
        r = _robust(g, "ret3")
        rows.append({"formula": f, "truth_source": src, **{f"d3_{k}": v for k, v in r.items()}, "d1_mean": pd.to_numeric(g.get("ret1"), errors="coerce").mean(), "d5_mean": pd.to_numeric(g.get("ret5"), errors="coerce").mean(), "mfe3_mean": pd.to_numeric(g.get("mfe3"), errors="coerce").mean()})
    perf = pd.DataFrame(rows)
    if not summary.empty and not perf.empty:
        actual = perf[perf["truth_source"].eq("ACTIVE_PRE")].copy().add_prefix("actual_").rename(columns={"actual_formula":"formula"})
        shadow = perf[perf["truth_source"].eq("SHADOW_BYPASS_PRECOMBO")].copy().add_prefix("shadow_").rename(columns={"shadow_formula":"formula"})
        summary = summary.merge(actual, on="formula", how="left").merge(shadow, on="formula", how="left")
        summary.to_csv(out / FULL_DENOM_SUMMARY_FILE, index=False, encoding="utf-8-sig")
    return summary


def _window_stats(events: pd.DataFrame, dates: list[pd.Timestamp], label: str) -> dict[str, Any]:
    g = events[events["signal_date"].isin(dates)].copy() if not events.empty else pd.DataFrame()
    r = _robust(g, "ret3")
    return {"window": label, **r, "cost20_mean": r["mean"] - 0.20 if math.isfinite(_num(r["mean"])) else np.nan, "cost50_mean": r["mean"] - 0.50 if math.isfinite(_num(r["mean"])) else np.nan, "status": "OK" if r["n"] else "NO_HITS_IN_WINDOW"}


def pattern_only_stability(output_dir: str | Path, outcomes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    out = _out(output_dir)
    join = _read_csv(out / "v73_sequence_context_catalyst_join.csv", dtype={"code": str})
    if join.empty or outcomes.empty:
        pd.DataFrame().to_csv(out / PATTERN_STABILITY_FILE, index=False); pd.DataFrame().to_csv(out / PATTERN_WF_FILE, index=False)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    dc = next((c for c in ["signal_date", "date"] if c in join.columns), None)
    ac = next((c for c in ["research_bucket", "alignment_level", "context_alignment", "alignment"] if c in join.columns), None)
    cc = next((c for c in ["code", "Code"] if c in join.columns), None)
    if not dc or not ac or not cc:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    q = join.copy(); q["signal_date"] = pd.to_datetime(q[dc], errors="coerce").dt.normalize(); q["code"] = q[cc].map(_norm_code)
    q = q[q[ac].fillna("").astype(str).str.upper().eq("PATTERN_ONLY")].copy()
    q = q.drop_duplicates(["signal_date", "code"], keep="last")
    o = outcomes[[c for c in ["signal_date", "code", "ret1", "ret3", "ret5", "ret10", "mfe3", "mae3"] if c in outcomes.columns]].copy()
    e = q.merge(o, on=["signal_date", "code"], how="left")
    dates = sorted(pd.to_datetime(outcomes["signal_date"], errors="coerce").dropna().dt.normalize().unique())
    dts = [pd.Timestamp(x) for x in dates]
    rows = []
    for n in [24, 12, 8, 4]:
        use = dts[-min(n, len(dts)):]
        rows.append(_window_stats(e, use, f"{n}W"))
    stab = pd.DataFrame(rows)
    stab.to_csv(out / PATTERN_STABILITY_FILE, index=False, encoding="utf-8-sig")
    cut = max(1, int(len(dts) * 2 / 3)) if dts else 0
    train_dates, oos_dates = dts[:cut], dts[cut:]
    wf_rows = []
    for label, use in [("TRAIN_FIRST_2_3", train_dates), ("OOS_LAST_1_3", oos_dates)]:
        r = _window_stats(e, use, label); wf_rows.append(r)
    wf = pd.DataFrame(wf_rows)
    wf.to_csv(out / PATTERN_WF_FILE, index=False, encoding="utf-8-sig")
    return e, stab, wf


def _policy_stats(name: str, values: pd.Series, signal_dates: pd.Series, cost_bp: float = 0.0) -> dict[str, Any]:
    x = pd.to_numeric(values, errors="coerce") - float(cost_bp) / 100.0
    g = pd.DataFrame({"signal_date": pd.to_datetime(signal_dates, errors="coerce").dt.normalize(), "ret": x})
    r = _robust(g.rename(columns={"ret":"ret3"}), "ret3")
    return {"policy": name, "cost_bp": cost_bp, **r}


def volume_breakout_exit_lab(output_dir: str | Path, truth_long: pd.DataFrame, outcomes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = _out(output_dir)
    if truth_long.empty or outcomes.empty:
        pd.DataFrame().to_csv(out / VOL_EXIT_FILE, index=False); pd.DataFrame().to_csv(out / VOL_EXIT_SPLIT_FILE, index=False); return pd.DataFrame(), pd.DataFrame()
    t = truth_long[(truth_long["formula"].eq(FORMULA_VOL_BREAK)) & truth_long["is_true"] & truth_long["truth_source"].eq("ACTIVE_PRE")][["signal_date", "code"]].drop_duplicates()
    e = t.merge(outcomes, on=["signal_date", "code"], how="left")
    if e.empty:
        return pd.DataFrame(), pd.DataFrame()
    d1 = pd.to_numeric(e.get("ret1"), errors="coerce")
    d3 = pd.to_numeric(e.get("ret3"), errors="coerce")
    d5 = pd.to_numeric(e.get("ret5"), errors="coerce")
    h1 = pd.to_numeric(e.get("d1_high_ret"), errors="coerce")
    policies = {
        "HOLD_D1_CLOSE": d1,
        "HOLD_D3_CLOSE": d3,
        "HOLD_D5_CLOSE": d5,
        "TP3_D1_ELSE_D1_CLOSE": pd.Series(np.where(h1.ge(3.0), 3.0, d1), index=e.index),
        "TP5_D1_ELSE_D1_CLOSE": pd.Series(np.where(h1.ge(5.0), 5.0, d1), index=e.index),
    }
    rows = []
    for pn, vals in policies.items():
        for bp in [0, 20, 50]: rows.append(_policy_stats(pn, vals, e["signal_date"], bp))
    pol = pd.DataFrame(rows)
    pol.to_csv(out / VOL_EXIT_FILE, index=False, encoding="utf-8-sig")
    dates = sorted(e["signal_date"].dropna().unique()); cut=max(1,int(len(dates)*2/3)) if dates else 0
    split_rows=[]
    fixed="TP3_D1_ELSE_D1_CLOSE"
    for label,use in [("TRAIN_FIRST_2_3",dates[:cut]),("OOS_LAST_1_3",dates[cut:])]:
        g=e[e["signal_date"].isin(use)].copy(); vals=policies[fixed].loc[g.index]
        for bp in [20,50]: split_rows.append({"split":label, **_policy_stats(fixed, vals, g["signal_date"], bp)})
    split=pd.DataFrame(split_rows); split.to_csv(out/VOL_EXIT_SPLIT_FILE,index=False,encoding="utf-8-sig")
    return pol, split


def ma_gc_continuation_lab(output_dir: str | Path, truth_long: pd.DataFrame, outcomes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = _out(output_dir)
    if truth_long.empty or outcomes.empty:
        pd.DataFrame().to_csv(out / MA_CONT_FILE, index=False); pd.DataFrame().to_csv(out / MA_CONT_SPLIT_FILE, index=False); return pd.DataFrame(), pd.DataFrame()
    t=truth_long[(truth_long["formula"].eq(FORMULA_MA_GC)) & truth_long["is_true"] & truth_long["truth_source"].eq("ACTIVE_PRE")][["signal_date","code"]].drop_duplicates()
    e=t.merge(outcomes,on=["signal_date","code"],how="left")
    policies={"HOLD_D1_CLOSE":pd.to_numeric(e.get("ret1"),errors="coerce"),"HOLD_D3_CLOSE":pd.to_numeric(e.get("ret3"),errors="coerce"),"HOLD_D5_CLOSE":pd.to_numeric(e.get("ret5"),errors="coerce"),"HOLD_D10_CLOSE":pd.to_numeric(e.get("ret10"),errors="coerce")}
    rows=[]
    for pn,vals in policies.items():
        for bp in [0,20,50]: rows.append(_policy_stats(pn,vals,e["signal_date"],bp))
    # Increment after D3 answers whether D5 performance is true continuation rather than entry outlier.
    r3=pd.to_numeric(e.get("ret3"),errors="coerce")/100.0; r5=pd.to_numeric(e.get("ret5"),errors="coerce")/100.0
    inc=((1+r5)/(1+r3)-1)*100.0
    rows.append(_policy_stats("D3_TO_D5_INCREMENT",inc,e["signal_date"],0))
    pol=pd.DataFrame(rows); pol.to_csv(out/MA_CONT_FILE,index=False,encoding="utf-8-sig")
    dates=sorted(e["signal_date"].dropna().unique()); cut=max(1,int(len(dates)*2/3)) if dates else 0
    split_rows=[]; fixed="HOLD_D5_CLOSE"
    for label,use in [("TRAIN_FIRST_2_3",dates[:cut]),("OOS_LAST_1_3",dates[cut:])]:
        g=e[e["signal_date"].isin(use)].copy(); vals=policies[fixed].loc[g.index]
        for bp in [20,50]: split_rows.append({"split":label, **_policy_stats(fixed,vals,g["signal_date"],bp)})
    split=pd.DataFrame(split_rows); split.to_csv(out/MA_CONT_SPLIT_FILE,index=False,encoding="utf-8-sig")
    return pol,split


def promotion_readiness(output_dir: str | Path, pattern_stab: pd.DataFrame, pattern_wf: pd.DataFrame, vol_split: pd.DataFrame, ma_split: pd.DataFrame, universe_avail: pd.DataFrame, denom_cov: pd.DataFrame) -> pd.DataFrame:
    out=_out(output_dir)
    valid_days=int(universe_avail.get("status",pd.Series(dtype=str)).astype(str).eq("VALID_CAUSAL_ASOF").sum()) if not universe_avail.empty else 0
    total_days=int(universe_avail["signal_date"].nunique()) if not universe_avail.empty and "signal_date" in universe_avail.columns else 0
    known_pct=float(denom_cov.iloc[0].get("known_cell_coverage_pct",np.nan)) if not denom_cov.empty else np.nan
    rows=[]
    # PAPER-only gate. It deliberately cannot mutate LIVE.
    p24=pattern_stab[pattern_stab["window"].eq("24W")].iloc[0] if not pattern_stab.empty and pattern_stab["window"].eq("24W").any() else pd.Series(dtype=object)
    poos=pattern_wf[pattern_wf["window"].eq("OOS_LAST_1_3")].iloc[0] if not pattern_wf.empty and pattern_wf["window"].eq("OOS_LAST_1_3").any() else pd.Series(dtype=object)
    def positive(v): return math.isfinite(_num(v)) and _num(v)>0
    pattern_checks={
        "n30":_num(p24.get("n"))>=30,"dates10":_num(p24.get("signal_days"))>=10,
        "median_pos":positive(p24.get("median")),"trim_pos":positive(p24.get("trim10")),"top5_pos":positive(p24.get("top5_removed")),
        "cost50_pos":positive(p24.get("cost50_mean")),"oos_median_pos":positive(poos.get("median")),"oos_top5_pos":positive(poos.get("top5_removed")),
        "causal_universe_10d":valid_days>=10,
    }
    rows.append({"candidate":"PATTERN_ONLY_SEQUENCE","gate":"PAPER_TRIAL_ONLY","checks_pass":sum(pattern_checks.values()),"checks_total":len(pattern_checks),"status":"PAPER_TRIAL_ELIGIBLE" if all(pattern_checks.values()) else "RESEARCH_ONLY","details":json.dumps(pattern_checks,ensure_ascii=False)})
    for candidate,df in [("VOL_BREAK_TP3_D1",vol_split),("MA_GC_HOLD_D5",ma_split)]:
        o=df[(df.get("split",pd.Series(dtype=str)).astype(str)=="OOS_LAST_1_3") & (pd.to_numeric(df.get("cost_bp"),errors="coerce")==50)] if not df.empty else pd.DataFrame()
        r=o.iloc[0] if not o.empty else pd.Series(dtype=object)
        checks={"oos_n10":_num(r.get("n"))>=10,"oos_dates4":_num(r.get("signal_days"))>=4,"oos_median_pos":positive(r.get("median")),"oos_trim_pos":positive(r.get("trim10")),"oos_top5_pos":positive(r.get("top5_removed")),"causal_universe_10d":valid_days>=10}
        rows.append({"candidate":candidate,"gate":"PAPER_TRIAL_ONLY","checks_pass":sum(checks.values()),"checks_total":len(checks),"status":"PAPER_TRIAL_ELIGIBLE" if all(checks.values()) else "RESEARCH_ONLY","details":json.dumps(checks,ensure_ascii=False)})
    q=pd.DataFrame(rows)
    q["valid_causal_universe_days"]=valid_days; q["universe_days"]=total_days; q["formula_known_cell_pct"]=known_pct
    q["live_auto_promotion_allowed"]=False; q["real_order_changed"]=False
    q.to_csv(out/PROMOTION_FILE,index=False,encoding="utf-8-sig")
    return q


def strip_stale_blocks(text: str) -> str:
    """Hide V22 runtime block after V23/V24 authority is present; do not delete its CSV artifacts."""
    s=str(text or "")
    old="⚡ [TOP500 4-Shard 병렬 × Newest-First Cache Prime × Fast-Gate Audit · RESEARCH_ONLY]"
    if old in s:
        start=s.find(old)
        # V23 block is the next authority boundary in current report order.
        nxt=s.find("🚄 [TOP500 6-Shard Materialized Result × Merge-Only Parent × Zero-Recompute · RESEARCH_ONLY]",start)
        if nxt>start:
            s=(s[:start]+"⚡ [V22 병렬진단] SUPERSEDED_BY_V23_V24 · CSV 감사원장은 유지, 최종판정에서는 제외\n\n"+s[nxt:])
    return s


def _fmt(v: Any, digits: int=2) -> str:
    x=_num(v); return "N/A" if not math.isfinite(x) else f"{x:+.{digits}f}%"


def build_report(
    output_dir: str | Path,
    universe_avail: pd.DataFrame,
    universe_recon: pd.DataFrame,
    denom_summary: pd.DataFrame,
    denom_cov: pd.DataFrame,
    pattern_stab: pd.DataFrame,
    pattern_wf: pd.DataFrame,
    vol_pol: pd.DataFrame,
    vol_split: pd.DataFrame,
    ma_pol: pd.DataFrame,
    ma_split: pd.DataFrame,
    promotion: pd.DataFrame,
) -> str:
    out=_out(output_dir)
    valid=int(universe_avail.get("status",pd.Series(dtype=str)).astype(str).eq("VALID_CAUSAL_ASOF").sum()) if not universe_avail.empty else 0
    total=int(universe_avail["signal_date"].nunique()) if not universe_avail.empty and "signal_date" in universe_avail.columns else 0
    fallback=max(0,total-valid)
    cov=denom_cov.iloc[0] if not denom_cov.empty else pd.Series(dtype=object)
    lines=[HEADER,
           f"📌 {VERSION} · LIVE/실주문 변경 0 · PAPER 승격도 자동주입 금지",
           f"📦 [Universe 권한복원] materialized 날짜 {len(universe_recon)}/{total or len(universe_recon)} · causal-asof {valid}일 · fallback {fallback}일",
           f"🧾 [전체분모 66식] stock-date {int(_num(cov.get('attempt_rows'),0))} · 공식식 {int(_num(cov.get('formula_count'),0))} · truth-cell {int(_num(cov.get('truth_cells'),0))} · known {int(_num(cov.get('known_cells'),0))} · unknown {int(_num(cov.get('unknown_cells'),0))}",
           f"- ACTIVE PRE {int(_num(cov.get('active_pre_attempts'),0))} · pre-COMBO shadow 복원 {int(_num(cov.get('shadow_bypass_attempts'),0))} · 끝까지 미해결 {int(_num(cov.get('unresolved_attempts'),0))} · UNKNOWN을 FALSE로 위장하지 않음",
           "- Shadow는 기존 저가/유동성 prefilter만 RESEARCH_ONLY로 우회해 COMBO 직전 truth를 기록합니다. LIVE 후보·점수·순위에는 반영하지 않습니다."]
    if not pattern_stab.empty:
        p24=pattern_stab[pattern_stab["window"].eq("24W")]
        po=pattern_wf[pattern_wf["window"].eq("OOS_LAST_1_3")] if not pattern_wf.empty else pd.DataFrame()
        if not p24.empty:
            r=p24.iloc[0]; lines += ["🧬 [PATTERN_ONLY 안정성]",f"- 24W n{int(_num(r.get('n'),0))}/일{int(_num(r.get('signal_days'),0))} | D3 평균 {_fmt(r.get('mean'))} · 중앙 {_fmt(r.get('median'))} · 절사 {_fmt(r.get('trim10'))} · 상5제거 {_fmt(r.get('top5_removed'))} · 50bp후 {_fmt(r.get('cost50_mean'))}"]
        if not po.empty:
            r=po.iloc[0]; lines.append(f"- OOS 뒤 1/3 n{int(_num(r.get('n'),0))}/일{int(_num(r.get('signal_days'),0))} | 중앙 {_fmt(r.get('median'))} · 절사 {_fmt(r.get('trim10'))} · 상5제거 {_fmt(r.get('top5_removed'))}")
    if not vol_pol.empty:
        lines.append("🚀 [거래량폭발초동돌파 · D1 청산가설]")
        for name in ["HOLD_D3_CLOSE","HOLD_D1_CLOSE","TP3_D1_ELSE_D1_CLOSE"]:
            r=vol_pol[(vol_pol["policy"].eq(name)) & (pd.to_numeric(vol_pol["cost_bp"],errors="coerce").eq(50))]
            if not r.empty:
                z=r.iloc[0]; lines.append(f"- {name}: n{int(_num(z.get('n'),0))} · 중앙 {_fmt(z.get('median'))} · 절사 {_fmt(z.get('trim10'))} · 상5 {_fmt(z.get('top5_removed'))}")
        o=vol_split[(vol_split.get("split",pd.Series(dtype=str)).astype(str)=="OOS_LAST_1_3") & (pd.to_numeric(vol_split.get("cost_bp"),errors="coerce")==50)] if not vol_split.empty else pd.DataFrame()
        if not o.empty:
            z=o.iloc[0]; lines.append(f"- 고정가설 TP3-D1 OOS 50bp: n{int(_num(z.get('n'),0))}/일{int(_num(z.get('signal_days'),0))} · 중앙 {_fmt(z.get('median'))} · 상5 {_fmt(z.get('top5_removed'))}")
    if not ma_pol.empty:
        lines.append("💛 [MA골크 · D3→D5 continuation]")
        for name in ["HOLD_D3_CLOSE","HOLD_D5_CLOSE","D3_TO_D5_INCREMENT"]:
            r=ma_pol[(ma_pol["policy"].eq(name)) & (pd.to_numeric(ma_pol["cost_bp"],errors="coerce").eq(0))]
            if not r.empty:
                z=r.iloc[0]; lines.append(f"- {name}: n{int(_num(z.get('n'),0))} · 평균 {_fmt(z.get('mean'))} · 중앙 {_fmt(z.get('median'))} · 절사 {_fmt(z.get('trim10'))} · 상5 {_fmt(z.get('top5_removed'))}")
        o=ma_split[(ma_split.get("split",pd.Series(dtype=str)).astype(str)=="OOS_LAST_1_3") & (pd.to_numeric(ma_split.get("cost_bp"),errors="coerce")==50)] if not ma_split.empty else pd.DataFrame()
        if not o.empty:
            z=o.iloc[0]; lines.append(f"- 고정가설 D5 OOS 50bp: n{int(_num(z.get('n'),0))}/일{int(_num(z.get('signal_days'),0))} · 중앙 {_fmt(z.get('median'))} · 상5 {_fmt(z.get('top5_removed'))}")
    lines.append("🚦 [PAPER 승격 게이트 · 자동 실전주입 없음]")
    if promotion.empty:
        lines.append("- 평가불가")
    else:
        for _,r in promotion.iterrows():
            lines.append(f"- {r.get('candidate')}: {r.get('status')} · {int(_num(r.get('checks_pass'),0))}/{int(_num(r.get('checks_total'),0))} 통과")
    lines += [
        "- PAPER_TRIAL_ELIGIBLE가 나와도 다음 forward OOS/PAPER 원장에서 확인한 뒤 수동 승격합니다. 실제 주문은 계속 0입니다.",
        f"- Actions: {UNIVERSE_RECON_FILE} · {FULL_DENOM_SUMMARY_FILE} · {FULL_DENOM_COVERAGE_FILE} · {ATTEMPT_OUTCOME_FILE} · {PATTERN_STABILITY_FILE} · {PATTERN_WF_FILE} · {VOL_EXIT_FILE} · {MA_CONT_FILE} · {PROMOTION_FILE}",
    ]
    block="\n".join(lines)
    (out/REPORT_FILE).write_text(block+"\n",encoding="utf-8")
    return block


def run_backtest(
    output_dir: str | Path = "reports",
    base_report: str = "",
    *,
    materialized_module: Any = None,
    historical_universe_module: Any = None,
    price_reader: Callable[..., pd.DataFrame] | None = None,
    combo_table: Iterable[dict] | None = None,
) -> tuple[str, dict[str, pd.DataFrame]]:
    out=_out(output_dir)
    payloads=collect_materialized_payloads(out,materialized_module)
    mem,summ,avail,recon=rebuild_universe_from_materialized(out,payloads)
    attempts,captures,shadow=collect_attempt_capture_shadow(payloads)
    formulas=_registry(combo_table)
    truth,dsum,dcov=build_full_denominator_truth(out,attempts,captures,shadow,formulas)
    outcomes=evaluate_attempt_outcomes(out,attempts,price_reader)
    dsum=attach_formula_performance(out,truth,outcomes)
    pevents,pstab,pwf=pattern_only_stability(out,outcomes)
    vpol,vsplit=volume_breakout_exit_lab(out,truth,outcomes)
    mpol,msplit=ma_gc_continuation_lab(out,truth,outcomes)
    promo=promotion_readiness(out,pstab,pwf,vsplit,msplit,avail,dcov)
    da=pd.DataFrame([{
        "version":VERSION,"materialized_payloads":len(payloads),"universe_days":int(mem["signal_date"].nunique()) if not mem.empty else 0,
        "attempt_rows":len(attempts),"capture_rows":len(captures),"shadow_rows":len(shadow),"formula_count":len(formulas),
        "attempt_outcomes_ok":int(outcomes.get("outcome_status",pd.Series(dtype=str)).eq("OK").sum()) if not outcomes.empty else 0,
        "pattern_only_events":len(pevents),"research_only":True,"live_logic_changed":False,"real_order_changed":False,
    }]); da.to_csv(out/DATA_AVAIL_FILE,index=False,encoding="utf-8-sig")
    # Rebuild the legacy Historical-AsOf report block from all 24 materialized sidecars without
    # truncating the V20/V23 blocks that follow it in the legacy report order.
    report=strip_stale_blocks(str(base_report or ""))
    if historical_universe_module is not None and callable(getattr(historical_universe_module,"finalize_audit",None)):
        try:
            h=historical_universe_module.HEADER
            hist_only,_=historical_universe_module.finalize_audit(out,base_report="")
            hist_only=str(hist_only or "").strip()
            if h in report and hist_only:
                start=report.find(h)
                boundaries=[
                    "⚡ [TOP500 Direct Replay 캐시·재개·진행률 진단 · RESEARCH_ONLY]",
                    "⚡ [V22 병렬진단] SUPERSEDED_BY_V23_V24",
                    "🚄 [TOP500 6-Shard Materialized Result × Merge-Only Parent × Zero-Recompute · RESEARCH_ONLY]",
                ]
                ends=[report.find(b,start+len(h)) for b in boundaries]
                ends=[x for x in ends if x>start]
                end=min(ends) if ends else len(report)
                report=(report[:start].rstrip()+"\n\n"+hist_only+"\n\n"+report[end:].lstrip()).strip()
            elif hist_only:
                report=(report.rstrip()+"\n\n"+hist_only).strip() if report.strip() else hist_only
        except Exception:
            pass
    block=build_report(out,avail,recon,dsum,dcov,pstab,pwf,vpol,vsplit,mpol,msplit,promo)
    if HEADER in report:
        report=report.split(HEADER)[0].rstrip()
    fixed=(report.rstrip()+"\n\n"+block).strip() if report.strip() else block
    return fixed,{"universe_membership":mem,"universe_summary":summ,"universe_availability":avail,"universe_reconciliation":recon,"truth_long":truth,"formula_summary":dsum,"formula_coverage":dcov,"attempt_outcomes":outcomes,"pattern_only_events":pevents,"pattern_stability":pstab,"pattern_walkforward":pwf,"volume_exit":vpol,"volume_split":vsplit,"ma_continuation":mpol,"ma_split":msplit,"promotion":promo,"data_availability":da}


def force_report(text: str, output_dir: str | Path = "reports") -> str:
    raw=strip_stale_blocks(str(text or ""))
    p=_out(output_dir)/REPORT_FILE
    if not p.exists(): return raw
    try: block=p.read_text(encoding="utf-8").strip()
    except Exception: return raw
    if HEADER in raw: raw=raw.split(HEADER)[0].rstrip()
    return (raw.rstrip()+"\n\n"+block).strip() if raw.strip() else block
