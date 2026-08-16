from __future__ import annotations

import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6.19"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🪜 [검색식별 분할매수 × 동일위험 1R × 청산정책 진단 · RESEARCH_ONLY]"
REPORT_FILE = "v73_scale_in_policy_report.txt"

EVENT_POLICY_FILE = "v73_scale_in_event_policy.csv"
POLICY_SUMMARY_FILE = "v73_scale_in_policy_summary.csv"
FORMULA_POLICY_FILE = "v73_scale_in_formula_policy_summary.csv"
ADD_TRIGGER_FILE = "v73_scale_in_add_trigger_audit.csv"
CONFLICT_FILE = "v73_scale_in_conflict_audit.csv"
RISK_AUDIT_FILE = "v73_scale_in_risk_parity_audit.csv"
READINESS_FILE = "v73_scale_in_readiness.csv"

MIN_POLICY_ROWS = 30
MIN_POLICY_DATES = 10
TARGET_PCT = 3.0
DEFAULT_STOP_PCT = -5.0
MAX_HOLD_DAYS = 3

POLICIES: dict[str, tuple[float, ...]] = {
    "LUMP_SUM_100": (1.0,),
    "IMPULSE_70_30": (0.70, 0.30),
    "CONFIRM_40_30_30": (0.40, 0.30, 0.30),
    "PULLBACK_30_30_40": (0.30, 0.30, 0.40),
    "PULLBACK_20_30_50": (0.20, 0.30, 0.50),
    "DIP_ONCE_70_30_RESEARCH": (0.70, 0.30),
}


def _out(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _read(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, low_memory=False, encoding=enc)
        except Exception:
            continue
    return pd.DataFrame()


def _write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_csv(path, index=False, encoding="utf-8-sig")


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

def _num(v: Any, default: float = np.nan) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _truth(v: Any) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def _finite(v: Any) -> bool:
    return math.isfinite(_num(v))


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
    if len(z) <= n:
        return np.nan
    return float(z.iloc[n:].mean())


def _source(output_dir: Path, fallback_df: pd.DataFrame | None) -> tuple[pd.DataFrame, str]:
    formula_fp = output_dir / "v72_search_formula_universe_exploded_eval.csv"
    direct_mode = str(os.environ.get("V1081_BACKTEST_SOURCE", "DIRECT_REPLAY")).strip().upper() in {"DIRECT_REPLAY", "DIRECT", "REPLAY", "LIVE_REPLAY"}
    if direct_mode:
        if not formula_fp.exists():
            return pd.DataFrame(), "INVALID_UPSTREAM_DEPENDENCY:FORMULA_EXPLODED_MISSING"
        q = _read(formula_fp)
        return (q, "FORMULA_EXPLODED") if not q.empty else (pd.DataFrame(), "INVALID_UPSTREAM_DEPENDENCY:FORMULA_EXPLODED_EMPTY")
    for name, fp in [
        ("FORMULA_EXPLODED", formula_fp),
        ("CONTEXT_EVENT", output_dir / "v73_backtest_event_master.csv"),
    ]:
        q = _read(fp)
        if not q.empty:
            return q, name
    return (fallback_df.copy(), "CALLER_DF") if isinstance(fallback_df, pd.DataFrame) else (pd.DataFrame(), "NO_INPUT")


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    q = df.copy()
    date_col = next((c for c in ["signal_date", "date", "신호일"] if c in q.columns), None)
    code_col = next((c for c in ["code", "Code", "종목코드"] if c in q.columns), None)
    if not date_col or not code_col:
        return pd.DataFrame()
    q["signal_date"] = pd.to_datetime(q[date_col], errors="coerce").dt.normalize()
    q["code"] = q[code_col].map(_norm_code)
    q = q[q["signal_date"].notna() & q["code"].ne("")].copy()
    formula_col = next((c for c in ["formula", "검색식", "primary_formula", "search_pattern_primary"] if c in q.columns), None)
    name_col = next((c for c in ["name", "Name", "종목명"] if c in q.columns), None)
    q["formula"] = q[formula_col].fillna("UNCLASSIFIED").astype(str) if formula_col else "UNCLASSIFIED"
    q["name"] = q[name_col].fillna("").astype(str) if name_col else ""
    q["entry_price"] = pd.to_numeric(q.get("entry_price_eval", q.get("entry_price", q.get("signal_close", np.nan))), errors="coerce")
    q["stop_price"] = pd.to_numeric(q.get("signal_stop_price", q.get("official_stop_price", q.get("stop_price", np.nan))), errors="coerce")
    q["stop_pct_source"] = pd.to_numeric(q.get("signal_stop_pct_eval", np.nan), errors="coerce")
    q = q.drop_duplicates(["signal_date", "code", "formula"], keep="first").reset_index(drop=True)
    return q


def _daily_from_row(row: pd.Series, price_fetcher: Callable | None = None) -> tuple[list[dict[str, Any]], str, float]:
    entry = _num(row.get("entry_price"))
    days: list[dict[str, Any]] = []
    for i in range(1, 6):
        o, h, l, c = (_num(row.get(f"next{i}_{k}_ret")) for k in ("open", "high", "low", "close"))
        if all(_finite(v) for v in (o, h, l, c)):
            days.append({"day": i, "date": str(row.get(f"next{i}_date", "")), "open_ret": o, "high_ret": h, "low_ret": l, "close_ret": c, "volume": _num(row.get(f"next{i}_volume"))})
    if days:
        return days, "EVAL_DAILY_FIELDS", entry
    if not callable(price_fetcher):
        return [], "DAILY_FIELDS_MISSING", entry
    try:
        px = price_fetcher(row.get("code"), row.get("signal_date"), hold_days=5)
    except TypeError:
        try:
            px = price_fetcher(row.get("code"), row.get("signal_date"), 5)
        except Exception:
            px = pd.DataFrame()
    except Exception:
        px = pd.DataFrame()
    if px is None or not isinstance(px, pd.DataFrame) or px.empty:
        return [], "FETCH_EMPTY", entry
    px = px.copy()
    px.index = pd.to_datetime(px.index, errors="coerce")
    px = px[px.index.notna()].sort_index()
    sig = pd.to_datetime(row.get("signal_date"), errors="coerce")
    if not _finite(entry) or entry <= 0:
        prior = px[px.index <= sig]
        if not prior.empty:
            entry = _num(prior.iloc[-1].get("Close"))
    fut = px[px.index > sig].head(5)
    if fut.empty or not _finite(entry) or entry <= 0:
        return [], "FETCH_NO_ENTRY_OR_FUTURE", entry
    def r(v: Any) -> float:
        return (_num(v) / entry - 1.0) * 100.0
    for i, (idx, z) in enumerate(fut.iterrows(), start=1):
        close = z.get("Close")
        days.append({
            "day": i, "date": pd.Timestamp(idx).strftime("%Y-%m-%d"),
            "open_ret": r(z.get("Open", close)), "high_ret": r(z.get("High", close)),
            "low_ret": r(z.get("Low", close)), "close_ret": r(close), "volume": _num(z.get("Volume")),
        })
    return days, "PRICE_FETCHER", entry


def _stop(entry: float, row: pd.Series) -> tuple[float, float, str]:
    sp = _num(row.get("stop_price"))
    if _finite(sp) and sp > 0 and sp < entry:
        pct = (sp / entry - 1.0) * 100.0
        if -15.0 <= pct <= -1.0:
            return sp, pct, "OFFICIAL_STOP"
    pct = _num(row.get("stop_pct_source"))
    if _finite(pct) and -15.0 <= pct <= -1.0:
        return entry * (1.0 + pct / 100.0), pct, "EVALUATOR_STOP_PCT"
    return entry * (1.0 + DEFAULT_STOP_PCT / 100.0), DEFAULT_STOP_PCT, "FALLBACK_-5_RESEARCH"


def _formula_policy_allowed(formula: str, policy: str) -> bool:
    f = str(formula)
    if policy == "LUMP_SUM_100":
        return True
    if "거래량폭발초동돌파" in f:
        return policy in {"IMPULSE_70_30", "DIP_ONCE_70_30_RESEARCH"}
    if "종베단독" in f or "MA골크" in f:
        return policy in {"CONFIRM_40_30_30", "DIP_ONCE_70_30_RESEARCH"}
    if "BB40 2차파동" in f:
        return policy in {"PULLBACK_30_30_40", "DIP_ONCE_70_30_RESEARCH"}
    if "수박" in f or "PULLBACK" in f or "눌림" in f:
        return policy in {"PULLBACK_30_30_40", "PULLBACK_20_30_50", "DIP_ONCE_70_30_RESEARCH"}
    return policy in {"CONFIRM_40_30_30"}


def _price(entry: float, ret_pct: float) -> float:
    return entry * (1.0 + ret_pct / 100.0)


def _close_position(day: dict[str, Any]) -> float:
    h, l, c = day["high_ret"], day["low_ret"], day["close_ret"]
    if not all(_finite(v) for v in (h, l, c)) or h <= l:
        return np.nan
    return (c - l) / (h - l)


def _add_rule(policy: str, tranche_idx: int, day: dict[str, Any], previous_day: dict[str, Any] | None, entry: float, avg_entry: float, stop_price: float) -> tuple[bool, str]:
    c, l, h = day["close_ret"], day["low_ret"], day["high_ret"]
    if _price(entry, l) <= stop_price:
        return False, "STOP_TOUCHED"
    pos = _close_position(day)
    if policy == "IMPULSE_70_30":
        ok = tranche_idx == 1 and 0.0 <= c <= 2.5 and (_finite(pos) and pos >= 0.55)
        return ok, "D1_STRENGTH_CONFIRM" if ok else "D1_CONFIRM_FAIL"
    if policy == "CONFIRM_40_30_30":
        if tranche_idx == 1:
            ok = c >= 0.0 and (_finite(pos) and pos >= 0.50)
            return ok, "D1_CLOSE_CONFIRM" if ok else "D1_CONFIRM_FAIL"
        pc = previous_day["close_ret"] if previous_day else np.nan
        ok = tranche_idx == 2 and c >= 0.0 and (_finite(pc) and c >= pc)
        return ok, "D2_HIGHER_CLOSE_CONFIRM" if ok else "D2_CONFIRM_FAIL"
    if policy in {"PULLBACK_30_30_40", "PULLBACK_20_30_50"}:
        if tranche_idx == 1:
            ok = -2.5 <= c <= 1.5 and (_finite(pos) and pos >= 0.50)
            return ok, "D1_NORMAL_PULLBACK_RECOVERY" if ok else "D1_PULLBACK_FAIL"
        pc = previous_day["close_ret"] if previous_day else np.nan
        ok = tranche_idx == 2 and c >= -0.5 and (_finite(pc) and c >= pc) and (_finite(pos) and pos >= 0.55)
        return ok, "D2_RESTART_CONFIRM" if ok else "D2_RESTART_FAIL"
    if policy == "DIP_ONCE_70_30_RESEARCH":
        ok = tranche_idx == 1 and -2.0 <= c <= -0.5 and (_finite(pos) and pos >= 0.55)
        return ok, "D1_ALIVE_DIP_ONCE" if ok else "DIP_CONDITION_FAIL"
    return False, "NO_RULE"


def _simulate(row: pd.Series, policy: str, days: list[dict[str, Any]], entry: float, data_source: str, cost_bp: float) -> dict[str, Any]:
    weights = POLICIES[policy]
    stop_price, stop_pct, stop_source = _stop(entry, row)
    fills: list[dict[str, Any]] = [{"tranche": 1, "weight": weights[0], "price": entry, "day": 0, "reason": "SIGNAL_CLOSE"}]
    weighted_units = weights[0] / entry
    invested_weight = weights[0]
    avg_entry = invested_weight / weighted_units
    exit_price = np.nan
    exit_day = np.nan
    exit_reason = "D3_CLOSE"
    conflict = False
    add_attempts = 0
    add_fills = 0
    add_block_reason = ""
    max_planned_weight = sum(weights)

    for d in days[:MAX_HOLD_DAYS]:
        high_price = _price(entry, d["high_ret"])
        low_price = _price(entry, d["low_ret"])
        close_price = _price(entry, d["close_ret"])
        target_price = avg_entry * (1.0 + TARGET_PCT / 100.0)
        hit_stop = low_price <= stop_price
        hit_target = high_price >= target_price
        if hit_stop and hit_target:
            conflict = True
            exit_reason = "PATH_CONFLICT_DAILY_BAR"
            exit_day = d["day"]
            break
        if hit_stop:
            exit_price, exit_day, exit_reason = stop_price, d["day"], "STOP_FIXED"
            break
        if hit_target:
            exit_price, exit_day, exit_reason = target_price, d["day"], "TP3_WEIGHTED_AVG"
            break

        next_tranche_idx = len(fills)
        if next_tranche_idx < len(weights) and d["day"] == next_tranche_idx:
            add_attempts += 1
            previous = days[d["day"] - 2] if d["day"] >= 2 and len(days) >= d["day"] - 1 else None
            ok, reason = _add_rule(policy, next_tranche_idx, d, previous, entry, avg_entry, stop_price)
            if ok:
                w = weights[next_tranche_idx]
                fills.append({"tranche": next_tranche_idx + 1, "weight": w, "price": close_price, "day": d["day"], "reason": reason})
                weighted_units += w / close_price
                invested_weight += w
                avg_entry = invested_weight / weighted_units
                add_fills += 1
            else:
                add_block_reason = reason

        if d["day"] == min(MAX_HOLD_DAYS, len(days)):
            exit_price, exit_day, exit_reason = close_price, d["day"], "D3_CLOSE"

    if conflict:
        gross_capital_ret = np.nan
        net20 = np.nan
        net50 = np.nan
        r_multiple = np.nan
    else:
        if not _finite(exit_price):
            last = days[min(MAX_HOLD_DAYS, len(days)) - 1]
            exit_price = _price(entry, last["close_ret"])
            exit_day = last["day"]
            exit_reason = "LAST_AVAILABLE_CLOSE"
        pnl = weighted_units * exit_price - invested_weight
        gross_capital_ret = pnl / max(invested_weight, 1e-12) * 100.0
        net20 = gross_capital_ret - (cost_bp / 100.0)
        net50 = gross_capital_ret - 0.50
        stop_loss_capital = invested_weight - weighted_units * stop_price
        r_multiple = pnl / stop_loss_capital if stop_loss_capital > 1e-12 else np.nan

    planned_stop_risk_pct = (max_planned_weight - sum(w / entry * stop_price for w in weights)) / max(max_planned_weight, 1e-12) * 100.0
    realized_stop_risk_pct = (invested_weight - weighted_units * stop_price) / max(invested_weight, 1e-12) * 100.0
    return {
        "signal_date": row.get("signal_date"), "code": row.get("code"), "name": row.get("name"), "formula": row.get("formula"),
        "policy": policy, "entry_price": entry, "stop_price_fixed": stop_price, "stop_pct": stop_pct, "stop_source": stop_source,
        "target_pct_weighted_avg": TARGET_PCT, "hold_limit_days": MAX_HOLD_DAYS,
        "filled_tranches": len(fills), "planned_tranches": len(weights), "invested_weight": invested_weight,
        "weighted_avg_entry": avg_entry, "exit_price": exit_price, "exit_day": exit_day, "exit_reason": exit_reason,
        "gross_capital_return_pct": gross_capital_ret, "net20_return_pct": net20, "net50_return_pct": net50,
        "r_multiple": r_multiple, "planned_stop_risk_pct": planned_stop_risk_pct, "realized_stop_risk_pct": realized_stop_risk_pct,
        "add_attempts": add_attempts, "add_fills": add_fills, "add_triggered": add_fills > 0,
        "add_block_reason": add_block_reason, "path_conflict": conflict, "eligible_primary": not conflict,
        "daily_data_source": data_source, "fill_ledger_json": json.dumps(fills, ensure_ascii=False, default=str),
        "fixed_stop_not_widened": True, "averaging_after_stop": False,
        "research_only": True, "live_logic_changed": False, "real_order_changed": False,
    }


def _summary(g: pd.DataFrame, dimension: str, label: str) -> dict[str, Any]:
    z = g[g["eligible_primary"].fillna(False)].copy()
    r = pd.to_numeric(z["net20_return_pct"], errors="coerce")
    r50 = pd.to_numeric(z["net50_return_pct"], errors="coerce")
    rr = pd.to_numeric(z["r_multiple"], errors="coerce")
    date_mean = z.assign(_r=r).groupby("signal_date")["_r"].mean() if not z.empty else pd.Series(dtype=float)
    return {
        "dimension": dimension, "label": label, "n": len(z), "raw_rows": len(g),
        "stocks": z["code"].nunique() if len(z) else 0, "signal_days": z["signal_date"].nunique() if len(z) else 0,
        "net20_mean": r.mean(), "net20_median": r.median(), "net20_trim10": _trim_mean(r), "net20_top5_removed": _top_removed(r),
        "net50_mean": r50.mean(), "win_rate": float((r > 0).mean() * 100) if len(r.dropna()) else np.nan,
        "profit_factor": float(r[r > 0].sum() / abs(r[r < 0].sum())) if (r < 0).any() else (np.inf if (r > 0).any() else np.nan),
        "r_multiple_mean": rr.mean(), "r_multiple_median": rr.median(),
        "add_trigger_rate": float(z["add_triggered"].mean() * 100) if len(z) else np.nan,
        "avg_invested_weight": pd.to_numeric(z["invested_weight"], errors="coerce").mean(),
        "avg_realized_stop_risk_pct": pd.to_numeric(z["realized_stop_risk_pct"], errors="coerce").mean(),
        "path_conflict_rate_raw": float(g["path_conflict"].mean() * 100) if len(g) else np.nan,
        "positive_signal_day_rate": float((date_mean > 0).mean() * 100) if len(date_mean) else np.nan,
    }


def _compare_to_lump(formula_summary: pd.DataFrame) -> pd.DataFrame:
    if formula_summary.empty:
        return formula_summary
    q = formula_summary.copy()
    base = q[q["policy"].eq("LUMP_SUM_100")][["formula", "net20_mean", "net20_median", "net20_top5_removed", "r_multiple_mean"]].rename(columns={
        "net20_mean": "lump_net20_mean", "net20_median": "lump_net20_median", "net20_top5_removed": "lump_top5_removed", "r_multiple_mean": "lump_r_multiple_mean",
    })
    q = q.merge(base, on="formula", how="left")
    q["delta_mean_vs_lump"] = q["net20_mean"] - q["lump_net20_mean"]
    q["delta_median_vs_lump"] = q["net20_median"] - q["lump_net20_median"]
    q["delta_top5_vs_lump"] = q["net20_top5_removed"] - q["lump_top5_removed"]
    q["delta_r_vs_lump"] = q["r_multiple_mean"] - q["lump_r_multiple_mean"]
    q["policy_status"] = np.where(
        (q["n"] >= MIN_POLICY_ROWS) & (q["signal_days"] >= MIN_POLICY_DATES) &
        (q["net20_mean"] > 0) & (q["net20_median"] > 0) & (q["net20_top5_removed"] > 0) &
        ((q["policy"].eq("LUMP_SUM_100")) | ((q["delta_mean_vs_lump"] > 0) & (q["delta_r_vs_lump"] >= 0))),
        "POLICY_CANDIDATE", "RESEARCH_ONLY"
    )
    return q


def _insert(text: str, block: str) -> str:
    s = str(text or "")
    if HEADER in s:
        st = s.find(HEADER)
        nxt = [s.find(h, st + len(HEADER)) for h in ["\n🧭 [시장 ×", "\n🏆 [V48", "\n🧬 ["]]
        nxt = [i for i in nxt if i >= 0]
        en = min(nxt) if nxt else len(s)
        s = (s[:st].rstrip() + "\n\n" + s[en:].lstrip()).strip()
    return s.rstrip() + "\n\n" + block


def _report(events: pd.DataFrame, formula_summary: pd.DataFrame, source: str, status: str) -> str:
    lines = [
        HEADER,
        f"📌 {VERSION} · SCALE_IN_RISK_PARITY_DIAGNOSTIC · RESEARCH_ONLY=True",
        "- 물타기와 확인형 추가매수를 분리하고, 고정 손절·가중평균단가 +3%·D+3 종료를 동일 위험 R 기준으로 비교합니다.",
        f"🧾 입력: {source} | 정책행 {len(events)} | 종목 {events['code'].nunique() if len(events) else 0} | 독립일 {events['signal_date'].nunique() if len(events) else 0} | 상태 {status}",
        f"⚠️ 일봉 경로충돌: {int(events['path_conflict'].sum()) if len(events) else 0}행은 목표/손절 선후를 알 수 없어 정책 주성과에서 제외",
        "🏆 [검색식별 일괄매수 ↔ 분할매수 비교 상위]",
    ]
    cand = formula_summary[(formula_summary["policy"].ne("LUMP_SUM_100")) & (formula_summary["n"].ge(5))].sort_values(["policy_status", "delta_mean_vs_lump", "net20_top5_removed"], ascending=[True, False, False]).head(8) if not formula_summary.empty else pd.DataFrame()
    if cand.empty:
        lines.append("- 평가 가능한 분할매수 표본이 아직 부족합니다.")
    else:
        for _, r in cand.iterrows():
            lines.append(
                f"- {r['formula']} · {r['policy']}: n{int(r['n'])}/일{int(r['signal_days'])} | 20bp 평균 {r['net20_mean']:+.2f}%·중앙 {r['net20_median']:+.2f}% | "
                f"일괄대비 {r['delta_mean_vs_lump']:+.2f}%p | R {r['r_multiple_mean']:+.2f} | 추가실행 {r['add_trigger_rate']:.1f}% | 상5제거 {r['net20_top5_removed']:+.2f}% | {r['policy_status']}"
            )
    lines += [
        "🛡️ [고정 안전계약]",
        "- 추가매수 뒤 손절선을 아래로 넓히지 않습니다. 손절 접촉·기준 무효화 뒤 추가매수는 0건이어야 합니다.",
        "- DIP_ONCE는 가격 하락만으로 사는 물타기가 아니라 -2~-0.5% 정상 눌림·종가회복 조건의 연구 모델이며 LIVE 연결은 금지합니다.",
        "- 일봉에서는 장중 선후를 만들지 않고 PATH_CONFLICT로 격리합니다. 09:30·14:40·15:03 분할은 향후 분봉 원장으로 별도 검증합니다.",
        f"🔒 승격: {MIN_POLICY_ROWS}행·{MIN_POLICY_DATES}독립일 + 20/50bp 양수 + 중앙/절사/상위5제거 양수 + 일괄대비 개선 + Walk-forward 유지.",
        f"- Actions CSV: {EVENT_POLICY_FILE} · {POLICY_SUMMARY_FILE} · {FORMULA_POLICY_FILE} · {ADD_TRIGGER_FILE} · {CONFLICT_FILE} · {RISK_AUDIT_FILE} · {READINESS_FILE}",
    ]
    return "\n".join(lines)


def run_backtest(eval_df: pd.DataFrame | None = None, output_dir: str | Path = "reports", base_report: str = "", price_fetcher: Callable | None = None) -> tuple[str, dict[str, pd.DataFrame]]:
    out = _out(output_dir)
    raw, source = _source(out, eval_df)
    q = _prepare(raw)
    rows: list[dict[str, Any]] = []
    for _, row in q.iterrows():
        days, data_source, entry = _daily_from_row(row, price_fetcher)
        if not days or not _finite(entry) or entry <= 0:
            continue
        for policy in POLICIES:
            if _formula_policy_allowed(str(row.get("formula")), policy):
                rows.append(_simulate(row, policy, days, entry, data_source, 20.0))
    events = pd.DataFrame(rows)
    if events.empty:
        for f in [EVENT_POLICY_FILE, POLICY_SUMMARY_FILE, FORMULA_POLICY_FILE, ADD_TRIGGER_FILE, CONFLICT_FILE, RISK_AUDIT_FILE]:
            _write(out / f, pd.DataFrame())
        ready = pd.DataFrame([{"version": VERSION, "status": "NO_DAILY_PATH_INPUT", "policy_rows": 0, "policy_ready": False, "research_only": True, "live_logic_changed": False, "real_order_changed": False}])
        _write(out / READINESS_FILE, ready)
        block = _report(pd.DataFrame(columns=["code", "signal_date", "path_conflict"]), pd.DataFrame(), source, "NO_DAILY_PATH_INPUT")
        (out / REPORT_FILE).write_text(block, encoding="utf-8")
        return _insert(base_report, block), {"readiness": ready}

    generated_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    events["event_policy_id"] = events.apply(lambda r: hashlib.sha256(f"{r.signal_date}|{r.code}|{r.formula}|{r.policy}".encode()).hexdigest()[:24], axis=1)
    snapshot = hashlib.sha256(events[["signal_date", "code", "formula", "policy", "net20_return_pct", "exit_reason"]].astype(str).to_csv(index=False).encode()).hexdigest()[:20]

    policy_rows = [_summary(g, "POLICY", p) for p, g in events.groupby("policy")]
    policy_summary = pd.DataFrame(policy_rows)
    formula_rows = []
    for (formula, policy), g in events.groupby(["formula", "policy"]):
        z = _summary(g, "FORMULA_X_POLICY", f"{formula}|{policy}")
        z.update({"formula": formula, "policy": policy})
        formula_rows.append(z)
    formula_summary = _compare_to_lump(pd.DataFrame(formula_rows))

    add_trigger = (events.groupby(["formula", "policy", "add_block_reason"], dropna=False)
                   .agg(rows=("event_policy_id", "size"), add_fills=("add_fills", "sum"), net20_mean=("net20_return_pct", "mean"))
                   .reset_index())
    conflict = events[events["path_conflict"]].copy()
    risk = (events.groupby(["policy", "stop_source"], dropna=False)
            .agg(rows=("event_policy_id", "size"), fixed_stop_rate=("fixed_stop_not_widened", "mean"), averaging_after_stop_count=("averaging_after_stop", "sum"),
                 invested_weight_mean=("invested_weight", "mean"), realized_stop_risk_pct_mean=("realized_stop_risk_pct", "mean"), r_multiple_mean=("r_multiple", "mean"))
            .reset_index())
    risk["fixed_stop_rate"] *= 100.0

    for table in [events, policy_summary, formula_summary, add_trigger, conflict, risk]:
        table["version"] = VERSION
        table["snapshot_id"] = snapshot
        table["generated_at"] = generated_at
        table["research_only"] = True
        table["live_logic_changed"] = False
        table["real_order_changed"] = False

    _write(out / EVENT_POLICY_FILE, events)
    _write(out / POLICY_SUMMARY_FILE, policy_summary)
    _write(out / FORMULA_POLICY_FILE, formula_summary)
    _write(out / ADD_TRIGGER_FILE, add_trigger)
    _write(out / CONFLICT_FILE, conflict)
    _write(out / RISK_AUDIT_FILE, risk)

    candidates = int(formula_summary["policy_status"].eq("POLICY_CANDIDATE").sum()) if not formula_summary.empty else 0
    input_days = int(events["signal_date"].nunique())
    status = "VALID_SHADOW" if len(events) else "NO_INPUT"
    policy_ready = bool(candidates > 0 and input_days >= MIN_POLICY_DATES and int((~events["path_conflict"]).sum()) >= MIN_POLICY_ROWS)
    ready = pd.DataFrame([{
        "version": VERSION, "status": status, "source": source, "policy_rows": len(events), "eligible_rows": int((~events["path_conflict"]).sum()),
        "signal_days": input_days, "formula_count": events["formula"].nunique(), "policy_count": events["policy"].nunique(),
        "path_conflict_rows": int(events["path_conflict"].sum()), "policy_candidate_count": candidates,
        "daily_field_rows": int(events["daily_data_source"].eq("EVAL_DAILY_FIELDS").sum()), "fetcher_rows": int(events["daily_data_source"].eq("PRICE_FETCHER").sum()),
        "fixed_stop_violation_count": int((~events["fixed_stop_not_widened"]).sum()), "averaging_after_stop_count": int(events["averaging_after_stop"].sum()),
        "policy_ready": policy_ready, "snapshot_id": snapshot, "generated_at": generated_at,
        "research_only": True, "live_logic_changed": False, "real_order_changed": False,
    }])
    _write(out / READINESS_FILE, ready)
    block = _report(events, formula_summary, source, status)
    (out / REPORT_FILE).write_text(block, encoding="utf-8")
    return _insert(base_report, block), {
        "event_policy": events, "policy_summary": policy_summary, "formula_policy_summary": formula_summary,
        "add_trigger": add_trigger, "conflict": conflict, "risk_audit": risk, "readiness": ready,
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
