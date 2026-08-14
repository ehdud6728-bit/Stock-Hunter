from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

import catalyst_source_adapters as adapters

VERSION = "V73.3.6.6.12"
BASE_VERSION = "V73.3.6.6.11"
RESEARCH_ONLY = True
HEADER = "🧬 [패턴 시퀀스 × 시장·섹터 × 재료 생명주기 완성형 · RESEARCH_ONLY]"
REPORT_FILE = "v73_sequence_context_catalyst_report_block.txt"

SEQUENCE_FILE = "v73_sequence_state_machine_audit.csv"
EVENT_RAW_FILE = "v73_catalyst_event_raw_normalized.csv"
EVENT_CLUSTER_FILE = "v73_catalyst_event_cluster_audit.csv"
EVENT_LIFECYCLE_FILE = "v73_catalyst_lifecycle_ledger.csv"
JOIN_FILE = "v73_sequence_context_catalyst_join.csv"
EVENT_EVAL_FILE = "v73_sequence_context_catalyst_event_eval.csv"
PERF_FILE = "v73_sequence_context_catalyst_performance.csv"
REGIME_FILE = "v73_sequence_context_catalyst_regime_performance.csv"
SOURCE_AUDIT_FILE = "v73_catalyst_source_provenance_audit.csv"
REPRO_FILE = "v73_sequence_context_reproducibility_manifest.json"
READINESS_FILE = "v73_sequence_context_catalyst_readiness.csv"
MANUAL_FILE = "v73_sequence_context_manual_chart_manifest.csv"
QUERY_FILE = "v73_catalyst_query_universe.csv"
MARKET_SECTOR_LEDGER_FILE = "v73_market_sector_context_ledger.csv"
MARKET_SECTOR_COVERAGE_FILE = "v73_market_sector_context_coverage.csv"
DENOMINATOR_AUDIT_FILE = "v73_sequence_zero_denominator_guard_audit.csv"
MARKET_SECTOR_COLUMNS = [
    "signal_date", "signal_cutoff_at", "code", "name", "sector", "market_regime",
    "market_return_5d", "market_turnover_ratio", "market_investor_flow",
    "sector_return_5d", "sector_turnover_ratio", "sector_up_ratio_pct", "sector_positive",
    "source_name", "true_sector_index", "captured_at", "causal_mode",
]

MIN_POLICY_ROWS = 30
MIN_POLICY_DATES = 10
CAUSAL_MODES = {"FORWARD_CAUSAL", "OFFICIAL_ARCHIVE_CAUSAL"}
OFFICIAL_NAMES = {"OPENDART", "DART", "KIND", "KRX", "GOV", "GOVERNMENT", "COMPANY_IR", "COMPANY_OFFICIAL"}

STOPWORDS = {
    "관련", "대한", "통해", "위한", "이번", "향후", "최근", "지난", "오는", "기업", "회사", "시장", "사업",
    "발표", "전망", "기대", "가능", "추진", "계획", "확대", "강화", "뉴스", "단독", "종합", "속보",
}


def _path(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _norm_code(v: Any) -> str:
    d = re.sub(r"\D", "", str(v or ""))
    return d.zfill(6)[-6:] if d else ""


def _num(v: Any) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else float("nan")
    except Exception:
        return float("nan")


def _safe_div(numerator: Any, denominator: Any, default: float = float("nan"), min_abs: float = 1e-12) -> float:
    """Finite fail-closed division used for all price/volume ratios.

    Market data occasionally contains zero placeholders.  A single malformed
    historical bar must never abort the whole RESEARCH_ONLY report block.
    """
    n = _num(numerator)
    d = _num(denominator)
    if not math.isfinite(n) or not math.isfinite(d) or abs(d) <= min_abs:
        return default
    value = n / d
    return value if math.isfinite(value) else default


def _fmt(v: Any, d: int = 2) -> str:
    x = _num(v)
    return "N/A" if not math.isfinite(x) else f"{x:+.{d}f}%"


def _rate(v: Any) -> str:
    x = _num(v)
    return "N/A" if not math.isfinite(x) else f"{x:.1f}%"


def _sha_obj(obj: Any) -> str:
    text = json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _clean_text(v: Any) -> str:
    s = re.sub(r"<[^>]+>", " ", str(v or ""))
    return re.sub(r"\s+", " ", s).strip()


def _tokens(text: str) -> list[str]:
    t = _clean_text(text).lower()
    words = re.findall(r"[가-힣A-Za-z0-9.%]+", t)
    return [w for w in words if len(w) >= 2 and w not in STOPWORDS]


def _event_family(text: str) -> str:
    s = _clean_text(text)
    rules = [
        ("CONTRACT_ORDER", r"수주|공급계약|판매계약|납품|계약 체결"),
        ("EARNINGS", r"실적|매출|영업이익|순이익|흑자|적자|가이던스"),
        ("CAPEX_PRODUCTION", r"증설|시설투자|공장|생산능력|가동률|양산"),
        ("POLICY_GLOBAL", r"정책|법안|규제|보조금|관세|표준|국가전략|글로벌|세계"),
        ("TECH_PRODUCT", r"신제품|기술개발|인증|승인|특허|상용화|테스트"),
        ("BIO_CLINICAL", r"임상|허가|FDA|식약처|적응증|환자"),
        ("OWNERSHIP_FINANCE", r"유상증자|전환사채|자사주|최대주주|합병|분할|지분"),
        ("PRICE_INDUSTRY", r"가격 인상|제품가격|원자재|운임|업황|재고|수요"),
    ]
    for name, pat in rules:
        if re.search(pat, s, flags=re.I):
            return name
    return "UNCLASSIFIED"


def _material_numbers(text: str) -> str:
    vals = re.findall(r"\d[\d,]*(?:\.\d+)?\s*(?:억|조|만|%|원|달러|MW|GW|톤|대|건)", _clean_text(text), flags=re.I)
    return "|".join(vals[:12])


def _normalize_history(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    q = df.copy()
    if not isinstance(q.index, pd.DatetimeIndex):
        dcol = next((c for c in ("Date", "date", "날짜", "일자") if c in q.columns), None)
        q.index = pd.to_datetime(q[dcol] if dcol else q.index, errors="coerce")
    q = q[q.index.notna()].sort_index()
    aliases = {
        "Open": ("Open", "open", "시가"), "High": ("High", "high", "고가"),
        "Low": ("Low", "low", "저가"), "Close": ("Close", "close", "종가"),
        "Volume": ("Volume", "volume", "거래량"),
    }
    out = pd.DataFrame(index=q.index)
    for target, choices in aliases.items():
        c = next((x for x in choices if x in q.columns), None)
        out[target] = pd.to_numeric(q[c], errors="coerce") if c else np.nan
    raw_rows = len(out)
    out = out.dropna(subset=["High", "Low", "Close"])
    numeric_price = out["High"].gt(0) & out["Low"].gt(0) & out["Close"].gt(0)
    coherent_bar = out["High"].ge(out["Low"])
    invalid_price_rows = int((~(numeric_price & coherent_bar)).sum())
    out = out[numeric_price & coherent_bar].copy()
    out["Volume"] = out["Volume"].fillna(0.0).clip(lower=0.0)
    out = out[~out.index.duplicated(keep="last")].tail(160)
    out.attrs["raw_price_rows"] = raw_rows
    out.attrs["valid_price_rows"] = len(out)
    out.attrs["invalid_nonpositive_or_incoherent_price_rows"] = invalid_price_rows
    return out


def sequence_state_v1(history: pd.DataFrame, signal_date: Any) -> dict:
    h = _normalize_history(history)
    raw_price_rows = int(h.attrs.get("raw_price_rows", len(h)))
    valid_price_rows = int(h.attrs.get("valid_price_rows", len(h)))
    invalid_price_rows = int(h.attrs.get("invalid_nonpositive_or_incoherent_price_rows", 0))
    sd = pd.Timestamp(signal_date).normalize()
    h = h[h.index.normalize() <= sd].tail(100)
    base = {
        "sequence_status": "HISTORY_UNAVAILABLE", "sequence_stage": "NONE", "stage_count": 0,
        "impulse_date": "", "impulse_low_date": "", "pullback_low_date": "", "restart_date": "",
        "impulse_return_pct": np.nan, "impulse_volume_ratio": np.nan, "impulse_close_location": np.nan,
        "pullback_retrace_pct": np.nan, "pullback_volume_ratio": np.nan, "pullback_volume_slope": np.nan,
        "down_volume_expansion": False, "range_contraction_ratio": np.nan, "support_hold": False,
        "restart_volume_ratio": np.nan, "restart_close_location": np.nan, "resistance_room_pct": np.nan,
        "impulse_ok": False, "accepted_breakout": False, "first_pullback": False, "supply_drying": False,
        "price_compression": False, "restart_trigger": False, "temporal_invariant": "UNKNOWN",
        "temporal_reason": "HISTORY_UNAVAILABLE", "sequence_key": "NONE", "sequence_quality_score": 0.0,
        "raw_price_rows": raw_price_rows, "valid_price_rows": valid_price_rows,
        "invalid_price_rows_excluded": invalid_price_rows, "sequence_error": "",
    }
    if len(h) < 25:
        return base
    h = h.copy()
    h["ret1"] = h["Close"].pct_change() * 100.0
    h["ret3"] = h["Close"].pct_change(3) * 100.0
    h["vma20"] = h["Volume"].shift(1).rolling(20, min_periods=8).mean()
    h["vol_ratio"] = h["Volume"] / h["vma20"].replace(0, np.nan)
    h["prior20_high"] = h["High"].shift(1).rolling(20, min_periods=8).max()
    h["close_loc"] = (h["Close"] - h["Low"]) / (h["High"] - h["Low"]).replace(0, np.nan)
    work = h.iloc[:-1] if len(h) > 1 else h
    candidates = work[(work["vol_ratio"] >= 1.45) & ((work["ret1"] >= 2.5) | (work["ret3"] >= 5.0))]
    if candidates.empty:
        base["sequence_status"] = "NO_VOLUME_IMPULSE"
        return base
    # Prefer the latest causal impulse that leaves at least one pullback session.
    impulse_date = candidates.index[-1]
    if impulse_date >= h.index[-1] and len(candidates) > 1:
        impulse_date = candidates.index[-2]
    pos = h.index.get_loc(impulse_date)
    pre = h.iloc[max(0, pos - 20): pos + 1]
    low_date = pre["Low"].idxmin()
    impulse_low = float(pre.loc[low_date, "Low"])
    impulse_high = float(h.loc[impulse_date, "High"])
    impulse_close = float(h.loc[impulse_date, "Close"])
    wave = max(impulse_high - impulse_low, 1e-9)
    after = h.loc[h.index > impulse_date]
    if after.empty:
        base.update({"sequence_status": "IMPULSE_ONLY", "sequence_stage": "VOLUME_IMPULSE", "stage_count": 1})
        return base
    # The signal/restart candle is not part of the preceding pullback-compression phase.
    # This keeps SUPPLY_DRYING and PRICE_COMPRESSION causal and semantically separate
    # from the expansion candle that confirms RESTART_TRIGGER.
    pull_phase = after.iloc[:-1] if len(after) >= 2 else after
    if pull_phase.empty:
        base.update({"sequence_status": "IMPULSE_ONLY", "sequence_stage": "VOLUME_IMPULSE", "stage_count": 1})
        return base
    pb_date = pull_phase["Low"].idxmin()
    pb_low = float(pull_phase.loc[pb_date, "Low"])
    retrace = _safe_div(impulse_high - pb_low, wave) * 100.0
    impulse_ratio = _safe_div(impulse_close, impulse_low)
    if not math.isfinite(impulse_ratio):
        base.update({
            "sequence_status": "INPUT_INVALID_ZERO_DENOMINATOR",
            "sequence_error": "impulse_low_nonpositive_or_nonfinite",
        })
        return base
    impulse_ret = (impulse_ratio - 1.0) * 100.0
    close_loc = _num(h.loc[impulse_date, "close_loc"])
    breakout = bool(impulse_close >= _num(h.loc[impulse_date, "prior20_high"]) * 0.995) if math.isfinite(_num(h.loc[impulse_date, "prior20_high"])) else bool(impulse_ret >= 6.0)
    accepted = bool(breakout and close_loc >= 0.55)
    pull = pull_phase.loc[:sd]
    impulse_vol = max(float(h.loc[impulse_date, "Volume"]), 1.0)
    pb_vol_ratio = _safe_div(pull["Volume"].mean(), impulse_vol) if len(pull) else np.nan
    if len(pull) >= 2:
        x = np.arange(len(pull), dtype=float)
        v = pull["Volume"].to_numpy(dtype=float)
        slope = _safe_div(np.polyfit(x, v, 1)[0], max(np.mean(v), 1.0))
    else:
        slope = np.nan
    down = pull[pull["Close"].pct_change().fillna(0) < 0]
    down_expand = bool(len(down) and down["Volume"].max() > impulse_vol * 0.9)
    recent3 = pull.tail(3)
    early3 = pull.head(3)
    range_recent = float(((recent3["High"] - recent3["Low"]) / recent3["Close"].replace(0, np.nan)).mean()) if len(recent3) else np.nan
    range_early = float(((early3["High"] - early3["Low"]) / early3["Close"].replace(0, np.nan)).mean()) if len(early3) else np.nan
    range_contract = _safe_div(range_recent, range_early) if math.isfinite(range_recent) and math.isfinite(range_early) else np.nan
    last = h.iloc[-1]
    prior = h.iloc[:-1].tail(5)
    restart_vol = _safe_div(last["Volume"], max(prior["Volume"].mean(), 1.0)) if len(prior) else np.nan
    restart_loc = _safe_div(last["Close"] - last["Low"], max(last["High"] - last["Low"], 1e-9))
    restart = bool(last["Close"] > prior["High"].tail(3).max() and last["Close"] > last["Open"] and restart_vol >= 1.10 and restart_loc >= 0.60) if len(prior) else False
    support_line = impulse_low + wave * 0.382
    support_hold = bool(pb_low >= impulse_low * 0.98 and float(last["Close"]) >= support_line * 0.97)
    supply = bool(pb_vol_ratio <= 0.65 and (not math.isfinite(slope) or slope <= 0.08) and not down_expand)
    compression = bool(math.isfinite(range_contract) and range_contract <= 0.82 and len(pull) >= 3)
    first_pull = bool(1 <= len(pull) <= 15 and 15 <= retrace <= 90)
    impulse_ok = bool(impulse_ret >= 5.0 and _num(h.loc[impulse_date, "vol_ratio"]) >= 1.45)
    flags = [impulse_ok, accepted, first_pull, supply, compression, support_hold, restart]
    stages = ["VOLUME_IMPULSE", "ACCEPTED_BREAKOUT", "FIRST_PULLBACK", "SUPPLY_DRYING", "PRICE_COMPRESSION", "SUPPORT_HOLD", "RESTART_TRIGGER"]
    count = 0
    current = "NONE"
    for f, stage in zip(flags, stages):
        if f:
            count += 1
            current = stage
        else:
            break
    # Also retain non-contiguous diagnostic count, but sequence_key is strict order.
    total_true = sum(bool(x) for x in flags)
    supply_quality = max(0.0, min(15.0, (1 - min(pb_vol_ratio, 1.0)) * 20)) if math.isfinite(pb_vol_ratio) else 0.0
    quality = min(100.0, total_true / 7 * 70 + max(0.0, min(15.0, impulse_ret / 2)) + supply_quality)
    resistance = h.iloc[:-1]["High"].tail(60).max()
    resistance_ratio = _safe_div(resistance, last["Close"])
    room = (resistance_ratio - 1) * 100 if math.isfinite(resistance_ratio) else np.nan
    low_ts = pd.Timestamp(low_date).normalize()
    impulse_ts = pd.Timestamp(impulse_date).normalize()
    pullback_ts = pd.Timestamp(pb_date).normalize()
    if low_ts < impulse_ts < pullback_ts <= sd:
        temporal = "PASS"
        temporal_reason = "STRICT_DAILY_LOW_BEFORE_IMPULSE_BEFORE_PULLBACK"
        sequence_status = "OK"
    elif low_ts == impulse_ts and impulse_ts < pullback_ts <= sd:
        # Daily OHLC cannot establish whether the low or high occurred first inside
        # the impulse candle. Fail closed for strategy eligibility, but do not
        # classify this data-resolution limit as a causal/temporal violation.
        temporal = "UNKNOWN"
        temporal_reason = "SAME_DAY_IMPULSE_LOW_HIGH_INTRADAY_ORDER_UNRESOLVED"
        sequence_status = "TEMPORAL_UNKNOWN_INTRADAY"
    else:
        temporal = "FAIL"
        temporal_reason = "CHRONOLOGICAL_ORDER_VIOLATION"
        sequence_status = "TEMPORAL_FAIL"
    base.update({
        "sequence_status": sequence_status,
        "sequence_stage": current, "stage_count": count, "diagnostic_true_count": total_true,
        "impulse_date": str(pd.Timestamp(impulse_date).date()), "impulse_low_date": str(pd.Timestamp(low_date).date()),
        "pullback_low_date": str(pd.Timestamp(pb_date).date()), "restart_date": str(sd.date()) if restart else "",
        "impulse_return_pct": impulse_ret, "impulse_volume_ratio": _num(h.loc[impulse_date, "vol_ratio"]),
        "impulse_close_location": close_loc, "pullback_retrace_pct": retrace,
        "pullback_volume_ratio": pb_vol_ratio, "pullback_volume_slope": slope,
        "down_volume_expansion": down_expand, "range_contraction_ratio": range_contract,
        "support_hold": support_hold, "restart_volume_ratio": restart_vol,
        "restart_close_location": restart_loc, "resistance_room_pct": room,
        "impulse_ok": impulse_ok, "accepted_breakout": accepted, "first_pullback": first_pull,
        "supply_drying": supply, "price_compression": compression, "restart_trigger": restart,
        "temporal_invariant": temporal, "temporal_reason": temporal_reason,
        "sequence_key": "→".join(stages[:count]) if count else "NONE",
        "sequence_quality_score": quality,
    })
    return base


def _capture_df(capture_rows: Iterable[dict]) -> pd.DataFrame:
    rows = []
    for r in capture_rows or []:
        z = dict(r or {})
        z["code"] = _norm_code(z.get("code"))
        z["signal_date"] = pd.to_datetime(z.get("signal_date"), errors="coerce").normalize()
        if z["code"] and pd.notna(z["signal_date"]):
            rows.append(z)
    if not rows:
        return pd.DataFrame()
    q = pd.DataFrame(rows)
    if "combo_invocation" not in q.columns:
        q["combo_invocation"] = 0
    return q.sort_values(["signal_date", "code", "combo_invocation"], kind="stable").drop_duplicates(["signal_date", "code"], keep="last")


def _sequence_table(capture_rows: Iterable[dict], history_map: dict) -> pd.DataFrame:
    cap = _capture_df(capture_rows)
    rows = []
    for _, r in cap.iterrows():
        ds = pd.Timestamp(r["signal_date"]).normalize()
        code = _norm_code(r["code"])
        h = (history_map or {}).get((ds.strftime("%Y-%m-%d"), code), pd.DataFrame())
        try:
            seq = sequence_state_v1(h, ds)
        except Exception as exc:
            seq = {
                "sequence_status": "ROW_ERROR_FAIL_CLOSED", "sequence_stage": "NONE", "stage_count": 0,
                "impulse_date": "", "impulse_low_date": "", "pullback_low_date": "", "restart_date": "",
                "impulse_return_pct": np.nan, "impulse_volume_ratio": np.nan, "impulse_close_location": np.nan,
                "pullback_retrace_pct": np.nan, "pullback_volume_ratio": np.nan, "pullback_volume_slope": np.nan,
                "down_volume_expansion": False, "range_contraction_ratio": np.nan, "support_hold": False,
                "restart_volume_ratio": np.nan, "restart_close_location": np.nan, "resistance_room_pct": np.nan,
                "impulse_ok": False, "accepted_breakout": False, "first_pullback": False, "supply_drying": False,
                "price_compression": False, "restart_trigger": False, "temporal_invariant": "UNKNOWN",
                "temporal_reason": "ROW_ERROR_FAIL_CLOSED", "sequence_key": "NONE", "sequence_quality_score": 0.0,
                "raw_price_rows": len(h) if isinstance(h, pd.DataFrame) else 0, "valid_price_rows": 0,
                "invalid_price_rows_excluded": 0,
                "sequence_error": f"{type(exc).__name__}:{exc}",
            }
        seq.update({
            "signal_date": ds, "signal_cutoff_at": ds + pd.Timedelta(hours=15, minutes=30),
            "code": code, "name": str(r.get("name", "") or ""),
            "analyze_returned": bool(r.get("analyze_returned", False)),
            "candidate_selected_legacy": bool(r.get("analyze_returned", False)),
            "source_scope": "DIRECT_REPLAY_COMBO_REACHED",
        })
        rows.append(seq)
    return pd.DataFrame(rows)


def _discover_ai_comment_sources(output_dir: Path) -> pd.DataFrame:
    fields = ["AI재료분류", "AI강한근거", "AI심판요약", "AI코멘트", "AI요약", "news_sentiment", "최근뉴스", "뉴스", "공시내용", "공시태그"]
    date_fields = ["source_captured_at", "captured_at", "AI코멘트생성시각", "created_at", "signal_date", "신호일", "추천일"]
    rows: list[dict] = []
    for p in sorted(output_dir.glob("*.csv")):
        if p.name.startswith("v73_catalyst_") or p.stat().st_size > 80_000_000:
            continue
        try:
            q = pd.read_csv(p, dtype=str, encoding="utf-8-sig", nrows=100000).fillna("")
        except Exception:
            continue
        content_cols = [c for c in fields if c in q.columns]
        if not content_cols:
            continue
        code_col = next((c for c in ("code", "Code", "종목코드") if c in q.columns), None)
        name_col = next((c for c in ("name", "Name", "종목명") if c in q.columns), None)
        time_col = next((c for c in date_fields if c in q.columns), None)
        for _, r in q.iterrows():
            text = " | ".join(_clean_text(r.get(c)) for c in content_cols if _clean_text(r.get(c)))
            if not text:
                continue
            published = str(r.get(time_col, "")) if time_col else ""
            causal = "FORWARD_CAUSAL" if time_col and time_col not in {"signal_date", "신호일", "추천일"} else "RETROSPECTIVE_RESEARCH"
            rows.append({
                "source_type": "AI_COMMENT", "source_name": f"AI_COMMENT:{p.name}",
                "code": _norm_code(r.get(code_col, "")) if code_col else "",
                "name": str(r.get(name_col, "")) if name_col else "",
                "title": text[:240], "summary": text,
                "published_at": published, "first_seen_at": published,
                "causal_mode": causal, "official_source": False, "independent_source": False,
                "event_type": "AI_COMMENT_HINT", "materiality": "UNKNOWN", "direct_benefit": "UNKNOWN",
                "thesis_validity": "UNVERIFIED", "new_fact": "UNKNOWN",
            })
    return adapters.normalize_rows(rows, causal_mode="RETROSPECTIVE_RESEARCH")


def _load_sources(output_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    templates = adapters.ensure_templates(output_dir)
    explicit = adapters.load_csv_ledgers(templates)
    ai = _discover_ai_comment_sources(output_dir)
    raw = pd.concat([explicit, ai], ignore_index=True) if len(ai) or len(explicit) else adapters.empty_ledger()
    if raw.empty:
        return raw, pd.DataFrame()
    # V73.3.6.6.12: source ledgers are append-only and may be present both in the
    # canonical master and a component ledger.  Collapse them by the stable source_key
    # before event clustering so repeated cache restores never inflate source counts.
    if "source_key" in raw.columns:
        raw = raw.sort_values(["first_seen_at", "retrieved_at"], kind="stable").drop_duplicates("source_key", keep="last")
    else:
        raw = raw.drop_duplicates([c for c in ("source_id", "source_url", "code", "title", "published_at") if c in raw.columns], keep="last")
    for c in ("published_at", "updated_at", "first_seen_at", "event_occurred_at", "official_at", "retrieved_at"):
        raw[c + "_ts"] = pd.to_datetime(raw[c], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    raw["code"] = raw["code"].map(_norm_code)
    raw["event_family"] = raw.apply(lambda r: _event_family(str(r.get("event_type", "")) + " " + str(r.get("title", "")) + " " + str(r.get("summary", ""))), axis=1)
    raw["event_numbers"] = raw.apply(lambda r: _material_numbers(str(r.get("title", "")) + " " + str(r.get("summary", ""))), axis=1)
    raw["token_key"] = raw.apply(lambda r: "|".join(sorted(set(_tokens(str(r.get("title", "")) + " " + str(r.get("summary", "")))))[:18]), axis=1)
    raw["official_source"] = raw["official_source"].astype(str).str.lower().isin(["true", "1", "yes"]) | raw["source_name"].str.upper().isin(OFFICIAL_NAMES)
    raw["independent_source"] = raw["independent_source"].astype(str).str.lower().isin(["true", "1", "yes"])
    raw["global_scope"] = raw["global_scope"].astype(str).str.lower().isin(["true", "1", "yes"])
    raw["event_time"] = raw["official_at_ts"].combine_first(raw["event_occurred_at_ts"]).combine_first(raw["published_at_ts"]).combine_first(raw["first_seen_at_ts"])
    # Causal availability is not the same as the article's publication/event date.
    # FORWARD_CAUSAL rows become usable only when this system actually first stored them.
    # Official archives may use their official timestamp; retrospective rows are never causal.
    raw["available_at"] = raw["first_seen_at_ts"].combine_first(raw["retrieved_at_ts"])
    official_archive = raw["causal_mode"].eq("OFFICIAL_ARCHIVE_CAUSAL") & raw["official_source"].astype(bool)
    raw.loc[official_archive, "available_at"] = raw.loc[official_archive, "official_at_ts"].combine_first(raw.loc[official_archive, "published_at_ts"]).combine_first(raw.loc[official_archive, "available_at"])
    raw["canonical_seed"] = raw.apply(lambda r: f"{r.get('code','')}|{r.get('event_family','')}|{r.get('token_key','')}|{r.get('event_numbers','')}", axis=1)
    # Deterministic approximate clustering: same code/global family + overlapping strong tokens.
    clusters: list[dict] = []
    assigned: dict[int, str] = {}
    for idx, r in raw.sort_values("event_time", na_position="last").iterrows():
        rt = set(str(r.get("token_key", "")).split("|")) - {""}
        best_id = ""
        best_score = 0.0
        for cl in clusters[-500:]:
            if str(cl["code"]) != str(r.get("code", "")) and not (bool(cl["global_scope"]) and bool(r.get("global_scope"))):
                continue
            if cl["family"] != r.get("event_family"):
                continue
            ct = cl["tokens"]
            score = len(rt & ct) / max(1, len(rt | ct))
            same_numbers = bool(str(r.get("event_numbers", "")) and str(r.get("event_numbers", "")) == cl["numbers"])
            if same_numbers:
                score += 0.35
            if score > best_score and score >= 0.28:
                best_id, best_score = cl["id"], score
        if not best_id:
            best_id = "EVT-" + _sha_obj({"seed": r.get("canonical_seed"), "n": len(clusters)})[:16]
            clusters.append({"id": best_id, "code": r.get("code", ""), "family": r.get("event_family"), "tokens": rt, "numbers": str(r.get("event_numbers", "")), "global_scope": bool(r.get("global_scope")), "sector": str(r.get("sector", "") or ""), "theme": str(r.get("theme", "") or "")})
        else:
            cl = next(x for x in clusters if x["id"] == best_id)
            cl["tokens"] |= rt
        assigned[idx] = best_id
    raw["canonical_event_id"] = pd.Series(assigned)
    cluster_rows = []
    for eid, g in raw.groupby("canonical_event_id", dropna=False):
        g = g.sort_values("event_time", na_position="last")
        earliest = g["event_time"].dropna().min() if g["event_time"].notna().any() else pd.NaT
        latest = g["event_time"].dropna().max() if g["event_time"].notna().any() else pd.NaT
        domains = {x for x in g["source_domain"].astype(str) if x}
        independents = {x for x in g.loc[g["independent_source"], "source_domain"].astype(str) if x}
        official = bool(g["official_source"].any())
        numbers = [x for x in g["event_numbers"].astype(str) if x]
        number_versions = len(set(numbers))
        confirmed = official or len(independents) >= 2
        cluster_rows.append({
            "canonical_event_id": eid, "code": next((x for x in g["code"] if x), ""),
            "name": next((x for x in g["name"] if x), ""), "event_family": g["event_family"].mode().iloc[0],
            "earliest_event_at": earliest, "latest_event_at": latest, "source_count": len(g),
            "source_domain_count": len(domains), "independent_source_count": len(independents),
            "official_confirmation": official, "cross_validated": confirmed,
            "global_scope": bool(g["global_scope"].any()),
            "sector": next((str(x) for x in g.get("sector", pd.Series(dtype=str)) if str(x).strip()), ""),
            "theme": next((str(x) for x in g.get("theme", pd.Series(dtype=str)) if str(x).strip()), ""),
            "number_version_count": number_versions,
            "material_update_detected": number_versions >= 2,
            "representative_title": str(g.iloc[0].get("title", "")),
            "source_names": "|".join(sorted(set(g["source_name"].astype(str)))),
            "causal_modes": "|".join(sorted(set(g["causal_mode"].astype(str)))),
            "event_cluster_sha256": _sha_obj(g[["source_id", "source_name", "source_url", "title", "published_at", "raw_payload_sha256"]].fillna("").to_dict("records")),
        })
    clusters_df = pd.DataFrame(cluster_rows)
    return raw, clusters_df


def _ensure_market_sector_template(output_dir: Path) -> Path:
    p = output_dir / MARKET_SECTOR_LEDGER_FILE
    if not p.exists():
        pd.DataFrame(columns=MARKET_SECTOR_COLUMNS).to_csv(p, index=False, encoding="utf-8-sig")
    return p


def _load_market_sector_ledger(output_dir: Path) -> pd.DataFrame:
    p = _ensure_market_sector_template(output_dir)
    try:
        q = pd.read_csv(p, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception:
        return pd.DataFrame(columns=MARKET_SECTOR_COLUMNS)
    if q.empty:
        return q
    for c in MARKET_SECTOR_COLUMNS:
        if c not in q.columns:
            q[c] = ""
    q["signal_date"] = pd.to_datetime(q["signal_date"], errors="coerce").dt.normalize()
    q["signal_cutoff_at"] = pd.to_datetime(q["signal_cutoff_at"], errors="coerce")
    q["captured_at"] = pd.to_datetime(q["captured_at"], errors="coerce")
    q["code"] = q["code"].map(_norm_code)
    for c in ("market_return_5d", "market_turnover_ratio", "sector_return_5d", "sector_turnover_ratio", "sector_up_ratio_pct"):
        q[c] = pd.to_numeric(q[c], errors="coerce")
    q["sector_positive"] = q["sector_positive"].astype(str).str.lower().isin(["true", "1", "yes", "y"])
    q["true_sector_index"] = q["true_sector_index"].astype(str).str.lower().isin(["true", "1", "yes", "y"])
    q = q[q["signal_date"].notna()].copy()
    # Forward rows require capture by the signal cutoff. Official archive rows may use the dated record.
    official = q["causal_mode"].eq("OFFICIAL_ARCHIVE_CAUSAL")
    cutoff = q["signal_cutoff_at"].copy()
    cutoff = cutoff.where(cutoff.notna(), q["signal_date"] + pd.Timedelta(hours=15, minutes=30))
    forward_ok = q["causal_mode"].eq("FORWARD_CAUSAL") & q["captured_at"].notna() & (q["captured_at"] <= cutoff)
    q["causal_eligible"] = official | forward_ok
    return q


def _market_regime_map(output_dir: Path) -> dict[pd.Timestamp, str]:
    mapping: dict[pd.Timestamp, str] = {}
    official = _load_market_sector_ledger(output_dir)
    if not official.empty:
        for _, r in official[official["causal_eligible"] & official["market_regime"].astype(str).ne("")].iterrows():
            mapping[pd.Timestamp(r["signal_date"]).normalize()] = str(r.get("market_regime") or "UNKNOWN")
    candidates = [output_dir / "v72_market_excess_benchmark_daily.csv", output_dir / "v72_market_excess_signal_audit.csv"]
    for p in candidates:
        if not p.exists():
            continue
        try:
            q = pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            continue
        dcol = next((c for c in ("signal_date", "date", "날짜") if c in q.columns), None)
        rcol = next((c for c in ("market_regime", "regime", "시장국면") if c in q.columns), None)
        if dcol and rcol:
            for _, r in q.iterrows():
                ds = pd.to_datetime(r.get(dcol), errors="coerce")
                if pd.notna(ds):
                    key = pd.Timestamp(ds).normalize()
                    if key not in mapping:
                        mapping[key] = str(r.get(rcol) or "UNKNOWN")
    return mapping


def _sector_map(listing_df: pd.DataFrame) -> dict[str, str]:
    if not isinstance(listing_df, pd.DataFrame) or listing_df.empty:
        return {}
    ccol = next((c for c in ("Code", "code", "종목코드") if c in listing_df.columns), None)
    scol = next((c for c in ("Sector", "sector", "Industry", "업종") if c in listing_df.columns), None)
    if not ccol or not scol:
        return {}
    return {_norm_code(r[ccol]): str(r[scol] or "UNKNOWN") for _, r in listing_df.iterrows()}


def _sector_proxy(seq: pd.DataFrame, history_map: dict, listing_df: pd.DataFrame) -> pd.DataFrame:
    smap = _sector_map(listing_df)
    rows = []
    for _, r in seq.iterrows():
        ds, code = pd.Timestamp(r["signal_date"]).normalize(), _norm_code(r["code"])
        sec = smap.get(code, "UNKNOWN")
        peer_returns = []
        peer_vol = []
        invalid_denominator_rows = 0
        for (d, c), h0 in (history_map or {}).items():
            if str(d) != ds.strftime("%Y-%m-%d") or smap.get(_norm_code(c), "UNKNOWN") != sec:
                continue
            h = _normalize_history(h0)
            if len(h) >= 6:
                close_ratio = _safe_div(h["Close"].iloc[-1], h["Close"].iloc[-6])
                if not math.isfinite(close_ratio):
                    invalid_denominator_rows += 1
                    continue
                peer_returns.append((close_ratio - 1) * 100)
                prev = max(float(h["Volume"].iloc[-6:-1].mean()), 1.0)
                vol_ratio_one = _safe_div(h["Volume"].iloc[-1], prev)
                if math.isfinite(vol_ratio_one):
                    peer_vol.append(vol_ratio_one)
        median_ret = float(np.median(peer_returns)) if peer_returns else np.nan
        up_ratio = float(np.mean(np.array(peer_returns) > 0) * 100) if peer_returns else np.nan
        vol_ratio = float(np.median(peer_vol)) if peer_vol else np.nan
        strong = bool(len(peer_returns) >= 3 and median_ret > 0 and up_ratio >= 55 and (not math.isfinite(vol_ratio) or vol_ratio >= 0.9))
        rows.append({
            "signal_date": ds, "code": code, "sector": sec, "sector_context_source": "INTERNAL_PEER_PROXY",
            "true_sector_index": False, "sector_peer_n": len(peer_returns), "sector_5d_median_pct": median_ret,
            "sector_up_ratio_pct": up_ratio, "sector_volume_ratio": vol_ratio, "sector_positive": strong,
            "sector_invalid_denominator_rows": invalid_denominator_rows,
        })
    return pd.DataFrame(rows)


def _sector_context(seq: pd.DataFrame, history_map: dict, listing_df: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    proxy = _sector_proxy(seq, history_map, listing_df)
    official = _load_market_sector_ledger(output_dir)
    if official.empty:
        return proxy
    official = official[official["causal_eligible"]].copy()
    if official.empty:
        return proxy
    rows = []
    for _, r in proxy.iterrows():
        ds, code, sec = pd.Timestamp(r["signal_date"]).normalize(), _norm_code(r["code"]), str(r.get("sector", "UNKNOWN"))
        cand = official[official["signal_date"].eq(ds)]
        exact = cand[cand["code"].eq(code)]
        if exact.empty and sec and sec != "UNKNOWN":
            exact = cand[cand["sector"].eq(sec)]
        z = r.to_dict()
        if not exact.empty:
            x = exact.sort_values("captured_at", na_position="last").iloc[-1]
            z.update({
                "sector": str(x.get("sector") or sec),
                "sector_context_source": str(x.get("source_name") or "OFFICIAL_MARKET_SECTOR_LEDGER"),
                "true_sector_index": bool(x.get("true_sector_index")),
                "sector_5d_median_pct": _num(x.get("sector_return_5d")),
                "sector_up_ratio_pct": _num(x.get("sector_up_ratio_pct")),
                "sector_volume_ratio": _num(x.get("sector_turnover_ratio")),
                "sector_positive": bool(x.get("sector_positive")),
            })
        rows.append(z)
    return pd.DataFrame(rows)


def _event_at_signal(seq_row: pd.Series, clusters: pd.DataFrame, raw: pd.DataFrame) -> dict:
    ds = pd.Timestamp(seq_row["signal_date"]).normalize()
    cutoff = pd.Timestamp(seq_row["signal_cutoff_at"])
    code = _norm_code(seq_row["code"])
    base = {
        "canonical_event_id": "", "event_family": "NONE", "catalyst_state": "NO_CATALYST",
        "catalyst_first_at": pd.NaT, "catalyst_latest_at": pd.NaT, "catalyst_age_days": np.nan,
        "cross_validated": False, "official_confirmation": False, "independent_source_count": 0,
        "global_scope": False, "material_update_detected": False, "causal_eligible": False,
        "retrospective_only": False, "thesis_validity": "UNKNOWN", "direct_benefit": "UNKNOWN",
        "news_freshness": "NONE", "price_activation": "NOT_YET", "activation_reason": "",
        "representative_title": "", "source_names": "", "asof_source_count": 0,
        "future_source_count_excluded": 0,
    }
    if clusters.empty or raw.empty:
        return base
    direct_mask = clusters["code"].eq(code)
    global_mask = clusters["global_scope"].astype(bool)
    seq_sector = _clean_text(seq_row.get("sector", "")).lower()
    if "sector" in clusters.columns and seq_sector:
        cluster_sector = clusters["sector"].fillna("").astype(str).map(_clean_text).str.lower()
        scoped = cluster_sector.eq("") | cluster_sector.eq(seq_sector) | cluster_sector.map(lambda x: bool(x and (x in seq_sector or seq_sector in x)))
        global_mask = global_mask & scoped
    elif "sector" in clusters.columns and not seq_sector:
        # A sector-scoped global thesis must not be attached to a stock whose sector is unknown.
        global_mask = global_mask & clusters["sector"].fillna("").astype(str).map(_clean_text).eq("")
    candidates = clusters[direct_mask | global_mask].copy()
    if candidates.empty:
        return base

    # Recompute every evidence statistic strictly as of the signal cutoff.  Cluster-level
    # summaries may contain later follow-up stories or disclosures and therefore must not
    # be reused for historical cross-validation/material-update decisions.
    asof_rows: list[dict] = []
    for _, c in candidates.iterrows():
        eid = c.get("canonical_event_id", "")
        src_all = raw[raw["canonical_event_id"].eq(eid)].copy()
        causal_mask = src_all["causal_mode"].isin(CAUSAL_MODES) & src_all["available_at"].notna() & (src_all["available_at"] <= cutoff)
        retrospective_mask = (~src_all["causal_mode"].isin(CAUSAL_MODES)) & src_all["event_time"].notna() & (src_all["event_time"] <= cutoff)
        observed = src_all[causal_mask | retrospective_mask].copy()
        if observed.empty:
            continue
        causal_src = src_all[causal_mask].copy()
        retrospective = len(causal_src) == 0
        evidence = causal_src if not causal_src.empty else observed
        official = bool(evidence["official_source"].fillna(False).astype(bool).any()) if not evidence.empty else False
        independent_domains = {
            str(x) for x in evidence.loc[evidence["independent_source"].fillna(False).astype(bool), "source_domain"].astype(str)
            if str(x).strip()
        }
        cross = bool(official or len(independent_domains) >= 2)
        numbers = [str(x) for x in evidence.get("event_numbers", pd.Series(dtype=str)).astype(str) if str(x).strip()]
        updated = len(set(numbers)) >= 2
        first = evidence["event_time"].dropna().min() if evidence["event_time"].notna().any() else pd.NaT
        latest = evidence["event_time"].dropna().max() if evidence["event_time"].notna().any() else pd.NaT
        global_scope = bool(evidence["global_scope"].fillna(False).astype(bool).any()) or bool(c.get("global_scope"))
        direct = str(c.get("code", "")) == code and bool(code)
        representative = str(evidence.sort_values("event_time", na_position="last").iloc[0].get("title", "")) if not evidence.empty else ""
        asof_rows.append({
            "cluster": c, "eid": eid, "observed": observed, "evidence": evidence,
            "retrospective": retrospective, "causal": not causal_src.empty,
            "official": official, "independent_count": len(independent_domains), "cross": cross,
            "updated": updated, "first": first, "latest": latest, "global_scope": global_scope,
            "direct": direct, "representative": representative,
            "source_names": "|".join(sorted(set(evidence["source_name"].astype(str)))),
            "asof_source_count": len(evidence), "future_excluded": max(0, len(src_all) - len(observed)),
        })
    if not asof_rows:
        return base

    # Prefer a company-specific event, then an as-of material update, then verified evidence,
    # then the most recent evidence available by the cutoff.  No future row participates.
    asof_rows.sort(key=lambda x: (
        bool(x["direct"]), bool(x["updated"]), bool(x["cross"]),
        pd.Timestamp(x["latest"]).value if pd.notna(x["latest"]) else -1,
    ), reverse=True)
    a = asof_rows[0]
    c = a["cluster"]
    first, latest = pd.to_datetime(a["first"], errors="coerce"), pd.to_datetime(a["latest"], errors="coerce")
    age = (ds - first.normalize()).days if pd.notna(first) else np.nan
    fresh = "FRESH_0_5D" if math.isfinite(_num(age)) and age <= 5 else ("RECENT_6_30D" if math.isfinite(_num(age)) and age <= 30 else "OLD_31D_PLUS")
    price_active = bool(seq_row.get("impulse_ok") or seq_row.get("restart_trigger"))
    if a["retrospective"]:
        state = "RETROSPECTIVE_ONLY"
    elif not a["causal"]:
        state = "UNVERIFIED"
    elif a["updated"]:
        state = "MATERIAL_UPDATE"
    elif (a["global_scope"] or (math.isfinite(_num(age)) and age > 30)) and not price_active:
        state = "LATENT_CATALYST"
    elif (a["global_scope"] or (math.isfinite(_num(age)) and age > 30)) and price_active:
        state = "LATENT_CATALYST_REACTIVATED"
    elif fresh == "FRESH_0_5D":
        state = "NEW_EVENT"
    elif a["cross"]:
        state = "CONFIRMED_EVENT"
    else:
        state = "REPEAT_OR_UNVERIFIED"
    reason = []
    if seq_row.get("impulse_ok"): reason.append("VOLUME_IMPULSE")
    if seq_row.get("restart_trigger"): reason.append("RESTART_TRIGGER")
    if seq_row.get("supply_drying"): reason.append("SUPPLY_DRYING")
    return {
        **base,
        "canonical_event_id": a["eid"], "event_family": c.get("event_family", "UNCLASSIFIED"),
        "catalyst_state": state, "catalyst_first_at": first, "catalyst_latest_at": latest,
        "catalyst_age_days": age, "cross_validated": bool(a["cross"]),
        "official_confirmation": bool(a["official"]),
        "independent_source_count": int(a["independent_count"]),
        "global_scope": bool(a["global_scope"]), "material_update_detected": bool(a["updated"]),
        "causal_eligible": bool(a["causal"]), "retrospective_only": bool(a["retrospective"]),
        "thesis_validity": "VALID_UNTIL_CONTRADICTED" if a["causal"] else "UNVERIFIED",
        "direct_benefit": "DIRECT" if a["direct"] else ("GLOBAL_THESIS" if a["global_scope"] else "UNKNOWN"),
        "news_freshness": fresh, "price_activation": "ACTIVE" if price_active else "NOT_YET",
        "activation_reason": "|".join(reason), "representative_title": a["representative"],
        "source_names": a["source_names"], "asof_source_count": int(a["asof_source_count"]),
        "future_source_count_excluded": int(a["future_excluded"]),
    }


def _join(seq: pd.DataFrame, clusters: pd.DataFrame, raw: pd.DataFrame, history_map: dict, listing_df: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    if seq.empty:
        return pd.DataFrame()
    market = _market_regime_map(output_dir)
    sector = _sector_context(seq, history_map, listing_df, output_dir)
    rows = []
    sector_lookup = {}
    if isinstance(sector, pd.DataFrame) and not sector.empty:
        for _, sr in sector.iterrows():
            sector_lookup[(pd.Timestamp(sr.get("signal_date")).normalize(), _norm_code(sr.get("code")))] = sr.to_dict()
    for _, r in seq.iterrows():
        key = (pd.Timestamp(r["signal_date"]).normalize(), _norm_code(r["code"]))
        sr = sector_lookup.get(key, {})
        event_row = r.copy()
        event_row["sector"] = str(sr.get("sector", "") or "")
        event = _event_at_signal(event_row, clusters, raw)
        z = r.to_dict()
        z.update(event)
        z["market_regime"] = market.get(pd.Timestamp(r["signal_date"]).normalize(), "UNKNOWN")
        rows.append(z)
    q = pd.DataFrame(rows)
    if not sector.empty:
        q = q.merge(sector, on=["signal_date", "code"], how="left")
    q["pattern_sequence_ready"] = q["stage_count"].fillna(0).astype(int).ge(5) & q["temporal_invariant"].eq("PASS")
    q["market_allowed"] = q["market_regime"].isin(["RECOVERY", "PANIC", "BULL", "NEUTRAL", "UNKNOWN"])
    q["sector_positive"] = q.get("sector_positive", False).fillna(False).astype(bool)
    q["catalyst_usable"] = q["causal_eligible"].fillna(False).astype(bool) & q["cross_validated"].fillna(False).astype(bool) & ~q["catalyst_state"].isin(["OLD_REPOST", "RETROSPECTIVE_ONLY", "UNVERIFIED", "NO_CATALYST", "REPEAT_OR_UNVERIFIED"])
    q["full_alignment"] = q["pattern_sequence_ready"] & q["market_allowed"] & q["sector_positive"] & q["catalyst_usable"]
    q["research_bucket"] = "NO_SEQUENCE"
    q.loc[q["pattern_sequence_ready"], "research_bucket"] = "PATTERN_ONLY"
    q.loc[q["pattern_sequence_ready"] & q["sector_positive"], "research_bucket"] = "PATTERN_MARKET_SECTOR"
    q.loc[q["pattern_sequence_ready"] & q["catalyst_usable"], "research_bucket"] = "PATTERN_CATALYST"
    q.loc[q["full_alignment"], "research_bucket"] = "FULL_ALIGNMENT"
    q.loc[q["catalyst_state"].eq("LATENT_CATALYST_REACTIVATED") & q["pattern_sequence_ready"], "research_bucket"] = "LATENT_REACTIVATED"
    q["live_score_delta"] = 0
    q["live_candidate_changed"] = False
    q["real_order_changed"] = False
    return q


def _evaluate(join: pd.DataFrame, evaluator: Callable[[pd.DataFrame], pd.DataFrame] | None) -> pd.DataFrame:
    if join.empty or not callable(evaluator):
        return pd.DataFrame()
    q = join.copy()
    q["source"] = "SEQUENCE_CONTEXT_CATALYST_SHADOW"
    q["entry_price"] = 0.0
    try:
        e = evaluator(q)
    except Exception as exc:
        q["eval_status"] = f"EVALUATOR_ERROR:{type(exc).__name__}:{exc}"
        return q
    if not isinstance(e, pd.DataFrame):
        return pd.DataFrame()
    keep = [c for c in join.columns if c not in e.columns]
    if keep:
        meta = join[["signal_date", "code"] + keep].drop_duplicates(["signal_date", "code"])
        e = e.merge(meta, on=["signal_date", "code"], how="left")
    return e


def _trim(s: pd.Series, frac: float = 0.1) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    if x.empty: return np.nan
    k = int(len(x) * frac)
    y = x.iloc[k:len(x)-k] if len(x)-2*k > 0 else x
    return float(y.mean())


def _ex2(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values(ascending=False)
    return float(x.iloc[2:].mean()) if len(x) > 2 else np.nan


def _perf(eval_df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if eval_df.empty:
        return pd.DataFrame()
    rows = []
    for key, g in eval_df.groupby(group_cols, dropna=False):
        if not isinstance(key, tuple): key = (key,)
        r = dict(zip(group_cols, key))
        r.update({"n": len(g), "stocks": g["code"].nunique(), "signal_days": g["signal_date"].nunique()})
        return_aliases = {
            "d1": ("next1_close_ret", "ret_1d", "return_1d", "day1_ret", "ret1", "d1_ret", "d1_return", "d1", "D1"),
            "d3": ("next3_close_ret", "v7219_d3_close_ret", "ret_3d", "return_3d", "day3_ret", "ret3", "d3_ret", "d3_return", "d3", "D3"),
            "d5": ("next5_close_ret", "ret_5d", "return_5d", "day5_ret", "ret5", "d5_ret", "d5_return", "d5", "D5"),
            "d10": ("next10_close_ret", "ret_10d", "return_10d", "day10_ret", "ret10", "d10_ret", "d10_return", "d10", "D10"),
        }
        for horizon, aliases in return_aliases.items():
            col = next((c for c in aliases if c in g.columns), None)
            if col:
                values = pd.to_numeric(g[col], errors="coerce")
                r[f"{horizon}_mean"] = float(values.mean())
                r[f"{horizon}_median"] = float(values.median())
                r[f"{horizon}_trim10"] = _trim(values)
                r[f"{horizon}_ex_top2"] = _ex2(values)
                r[f"{horizon}_source_col"] = col
        plus_col = next((c for c in ("plus3_first", "plus3_before_stop", "+3먼저") if c in g.columns), None)
        stop_col = next((c for c in ("stop_first", "stop_before_plus3", "손절먼저") if c in g.columns), None)
        r["plus3_first_rate"] = float(g[plus_col].astype(bool).mean()*100) if plus_col else np.nan
        r["stop_first_rate"] = float(g[stop_col].astype(bool).mean()*100) if stop_col else np.nan
        rows.append(r)
    return pd.DataFrame(rows)


def _snapshot_manifest(capture_rows: Iterable[dict], history_map: dict, listing_df: pd.DataFrame, raw: pd.DataFrame, clusters: pd.DataFrame) -> dict:
    cap = _capture_df(capture_rows)
    history_items = []
    for key in sorted((history_map or {}).keys()):
        h = _normalize_history(history_map[key])
        history_items.append({"key": list(key), "rows": len(h), "sha": _sha_obj(h.reset_index().astype(str).to_dict("records"))})
    listing_records = listing_df.fillna("").astype(str).to_dict("records") if isinstance(listing_df, pd.DataFrame) else []
    return {
        "version": VERSION,
        "capture_rows": len(cap), "capture_sha256": _sha_obj(cap.fillna("").astype(str).to_dict("records")) if not cap.empty else _sha_obj([]),
        "history_keys": len(history_items), "history_manifest_sha256": _sha_obj(history_items),
        "listing_rows": len(listing_records), "listing_sha256": _sha_obj(listing_records),
        "source_rows": len(raw), "source_sha256": _sha_obj(raw.fillna("").astype(str).to_dict("records")) if not raw.empty else _sha_obj([]),
        "event_clusters": len(clusters), "cluster_sha256": _sha_obj(clusters.fillna("").astype(str).to_dict("records")) if not clusters.empty else _sha_obj([]),
        "snapshot_id": _sha_obj({"cap": _sha_obj(cap.fillna("").astype(str).to_dict("records")) if not cap.empty else "", "hist": _sha_obj(history_items), "src": _sha_obj(raw.fillna("").astype(str).to_dict("records")) if not raw.empty else ""})[:20],
        "live_logic_changed": False,
    }


def _insert_block(text: str, block: str) -> str:
    raw = str(text or "")
    if HEADER in raw:
        start = raw.find(HEADER)
        next_header = raw.find("\n\n", start + len(HEADER))
        # force_report is idempotent; keep existing generated block.
        return raw
    marker = "\n\n🌙 [전일 야간환경"
    pos = raw.find(marker)
    if pos >= 0:
        return raw[:pos] + "\n\n" + block + raw[pos:]
    return raw.rstrip() + "\n\n" + block


def run_backtest(capture_rows: Iterable[dict], attempt_rows: Iterable[dict], history_map: dict,
                 output_dir: str = "reports", base_report: str = "",
                 evaluator: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
                 listing_df: pd.DataFrame | None = None) -> tuple[str, dict]:
    out = _path(output_dir)
    listing_df = listing_df if isinstance(listing_df, pd.DataFrame) else pd.DataFrame()
    _ensure_market_sector_template(out)
    seq = _sequence_table(capture_rows, history_map)
    seq.to_csv(out / SEQUENCE_FILE, index=False, encoding="utf-8-sig")
    query_cols = [c for c in ("code", "name") if c in seq.columns]
    queries = seq[query_cols].drop_duplicates().copy() if query_cols else pd.DataFrame(columns=["code", "name"])
    if not queries.empty:
        queries["query"] = queries.apply(lambda r: f"{r.get('name','')} {r.get('code','')} 수주 실적 정책 공급계약", axis=1)
        queries["causal_mode"] = "FORWARD_CAUSAL"
    queries.to_csv(out / QUERY_FILE, index=False, encoding="utf-8-sig")

    raw, clusters = _load_sources(out)
    raw.to_csv(out / EVENT_RAW_FILE, index=False, encoding="utf-8-sig")
    clusters.to_csv(out / EVENT_CLUSTER_FILE, index=False, encoding="utf-8-sig")
    clusters.to_csv(out / EVENT_LIFECYCLE_FILE, index=False, encoding="utf-8-sig")

    joined = _join(seq, clusters, raw, history_map, listing_df, out)
    joined.to_csv(out / JOIN_FILE, index=False, encoding="utf-8-sig")
    market_sector_ledger = _load_market_sector_ledger(out)
    coverage = pd.DataFrame([{
        "version": VERSION, "ledger_rows": len(market_sector_ledger),
        "causal_eligible_rows": int(market_sector_ledger.get("causal_eligible", pd.Series(dtype=bool)).sum()) if not market_sector_ledger.empty else 0,
        "joined_rows": len(joined),
        "true_sector_index_rows": int(joined.get("true_sector_index", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not joined.empty else 0,
        "internal_peer_proxy_rows": int(joined.get("sector_context_source", pd.Series(dtype=str)).eq("INTERNAL_PEER_PROXY").sum()) if not joined.empty else 0,
    }])
    coverage.to_csv(out / MARKET_SECTOR_COVERAGE_FILE, index=False, encoding="utf-8-sig")

    denominator_audit = pd.DataFrame([{
        "version": VERSION,
        "sequence_rows": len(seq),
        "invalid_price_rows_excluded": int(pd.to_numeric(seq.get("invalid_price_rows_excluded", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not seq.empty else 0,
        "sequence_zero_denominator_rows": int(seq.get("sequence_status", pd.Series(dtype=str)).eq("INPUT_INVALID_ZERO_DENOMINATOR").sum()) if not seq.empty else 0,
        "sequence_row_error_rows": int(seq.get("sequence_status", pd.Series(dtype=str)).eq("ROW_ERROR_FAIL_CLOSED").sum()) if not seq.empty else 0,
        "sector_invalid_denominator_rows": int(pd.to_numeric(joined.get("sector_invalid_denominator_rows", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not joined.empty else 0,
        "pipeline_aborted": False,
        "live_logic_changed": False,
    }])
    denominator_audit.to_csv(out / DENOMINATOR_AUDIT_FILE, index=False, encoding="utf-8-sig")
    ev = _evaluate(joined, evaluator)
    # V24: persist the event-level evaluated sequence ledger so downstream stability/OOS
    # uses the same PATTERN_ONLY rows and actual forward returns instead of an empty aggregate.
    ev.to_csv(out / EVENT_EVAL_FILE, index=False, encoding="utf-8-sig")
    perf = _perf(ev, ["research_bucket", "catalyst_state"])
    regime = _perf(ev, ["research_bucket", "market_regime"])
    perf.to_csv(out / PERF_FILE, index=False, encoding="utf-8-sig")
    regime.to_csv(out / REGIME_FILE, index=False, encoding="utf-8-sig")

    source_audit = pd.DataFrame([{
        "version": VERSION, "source_rows": len(raw), "event_clusters": len(clusters),
        "official_rows": int(raw["official_source"].sum()) if not raw.empty else 0,
        "forward_causal_rows": int(raw["causal_mode"].isin(CAUSAL_MODES).sum()) if not raw.empty else 0,
        "retrospective_rows": int(raw["causal_mode"].eq("RETROSPECTIVE_RESEARCH").sum()) if not raw.empty else 0,
        "cross_validated_clusters": int(clusters["cross_validated"].sum()) if not clusters.empty else 0,
        "latent_clusters": int(clusters["global_scope"].sum()) if not clusters.empty else 0,
        "google_adapter_ready": bool(__import__("os").environ.get("GOOGLE_CSE_API_KEY") and __import__("os").environ.get("GOOGLE_CSE_ID")),
        "kakao_adapter_ready": bool(__import__("os").environ.get("KAKAO_REST_API_KEY")),
        "opendart_adapter_ready": bool(__import__("os").environ.get("OPENDART_API_KEY")),
        "network_default": "OFF",
        "market_sector_ledger_rows": len(market_sector_ledger),
        "true_sector_index_rows": int(joined.get("true_sector_index", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not joined.empty else 0,
    }])
    source_audit.to_csv(out / SOURCE_AUDIT_FILE, index=False, encoding="utf-8-sig")

    manifest = _snapshot_manifest(capture_rows, history_map, listing_df, raw, clusters)
    (out / REPRO_FILE).write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    seq_valid = int(seq["sequence_status"].eq("OK").sum()) if not seq.empty else 0
    temporal_fail = int(seq["temporal_invariant"].eq("FAIL").sum()) if not seq.empty else 0
    temporal_unknown = int(seq["temporal_invariant"].eq("UNKNOWN").sum()) if not seq.empty else 0
    temporal_same_day_unknown = int(seq.get("temporal_reason", pd.Series(dtype=str)).eq("SAME_DAY_IMPULSE_LOW_HIGH_INTRADAY_ORDER_UNRESOLVED").sum()) if not seq.empty else 0
    sequence_unavailable = int(seq["sequence_status"].isin(["HISTORY_UNAVAILABLE", "INPUT_INVALID_ZERO_DENOMINATOR"]).sum()) if not seq.empty else 0
    sequence_row_errors = int(seq["sequence_status"].eq("ROW_ERROR_FAIL_CLOSED").sum()) if not seq.empty else 0
    invalid_price_rows = int(pd.to_numeric(seq.get("invalid_price_rows_excluded", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not seq.empty else 0
    sequence_ready = int(joined["pattern_sequence_ready"].sum()) if not joined.empty else 0
    catalyst_usable = int(joined["catalyst_usable"].sum()) if not joined.empty else 0
    full = int(joined["full_alignment"].sum()) if not joined.empty else 0
    reactivated = int(joined["catalyst_state"].eq("LATENT_CATALYST_REACTIVATED").sum()) if not joined.empty else 0
    eval_ok = int(ev.get("eval_status", pd.Series(dtype=str)).eq("OK").sum()) if not ev.empty else 0
    eval_days = int(ev["signal_date"].nunique()) if not ev.empty and "signal_date" in ev.columns else 0
    known_statuses = ["OK", "NO_VOLUME_IMPULSE", "IMPULSE_ONLY", "TEMPORAL_UNKNOWN_INTRADAY", "HISTORY_UNAVAILABLE", "INPUT_INVALID_ZERO_DENOMINATOR"]
    contract_valid = bool(len(seq) > 0 and int(seq["sequence_status"].isin(known_statuses).sum()) == len(seq) and temporal_fail == 0 and sequence_row_errors == 0)
    source_warmup = len(raw) == 0 or catalyst_usable == 0 or sequence_unavailable > 0
    policy_ready = bool(eval_ok >= MIN_POLICY_ROWS and eval_days >= MIN_POLICY_DATES and full >= 10)
    status = "VALID_SHADOW_DATA_WARMUP" if contract_valid and source_warmup else ("VALID_SHADOW" if contract_valid else "INVALID")
    ready = pd.DataFrame([{
        "version": VERSION, "sequence_rows": len(seq), "sequence_valid_rows": seq_valid,
        "sequence_ready_rows": sequence_ready, "temporal_fail_rows": temporal_fail,
        "temporal_unknown_rows": temporal_unknown, "temporal_same_day_intraday_unknown_rows": temporal_same_day_unknown,
        "sequence_unavailable_rows": sequence_unavailable, "sequence_row_error_rows": sequence_row_errors,
        "invalid_price_rows_excluded": invalid_price_rows,
        "source_rows": len(raw), "event_clusters": len(clusters), "catalyst_usable_rows": catalyst_usable,
        "full_alignment_rows": full, "latent_reactivated_rows": reactivated,
        "evaluated_rows": eval_ok, "evaluated_signal_days": eval_days,
        "contract_valid": contract_valid, "source_warmup": source_warmup, "policy_ready": policy_ready,
        "status": status, "live_logic_changed": False, "real_order_changed": False,
        "snapshot_id": manifest["snapshot_id"],
    }])
    ready.to_csv(out / READINESS_FILE, index=False, encoding="utf-8-sig")

    manual_cols = [c for c in ["signal_date", "code", "name", "sequence_stage", "stage_count", "sequence_key", "impulse_date", "pullback_low_date", "pullback_volume_ratio", "range_contraction_ratio", "restart_trigger", "market_regime", "sector", "sector_positive", "catalyst_state", "representative_title", "full_alignment"] if c in joined.columns]
    joined[manual_cols].sort_values(["full_alignment", "stage_count"], ascending=[False, False]).head(100).to_csv(out / MANUAL_FILE, index=False, encoding="utf-8-sig") if manual_cols else pd.DataFrame().to_csv(out / MANUAL_FILE, index=False)

    state_counts = joined["catalyst_state"].value_counts().to_dict() if not joined.empty else {}
    bucket_counts = joined["research_bucket"].value_counts().to_dict() if not joined.empty else {}
    lines = [
        HEADER,
        f"📌 {VERSION} · SEQUENCE_MARKET_SECTOR_CATALYST_PROVENANCE_REACTIVATION_PIPELINE · RESEARCH_ONLY=True",
        "- 목적: 단일 패턴이 아니라 거래량 확장→돌파수용→첫눌림→공급감소→가격수렴→지지→재시동의 시간순서와, 신호일 당시 확인 가능한 시장·섹터·재료를 결합합니다.",
        "- 오래된 세계적 재료는 폐기하지 않고 LATENT_CATALYST로 보존하며, 직접수혜·섹터자금·가격 활성화가 생기면 LATENT_CATALYST_REACTIVATED로 승격합니다.",
        "- 오늘 검색해 복원한 과거 뉴스는 RETROSPECTIVE_RESEARCH로 격리하고 성과·LIVE 근거에 사용하지 않습니다.",
        f"🧾 계약: sequence {len(seq)}행 | strict-pass {seq_valid} | intraday-unknown {temporal_same_day_unknown} | data-warmup {sequence_unavailable} | row-error {sequence_row_errors} | temporal fail {temporal_fail} | source {len(raw)} | event cluster {len(clusters)} | 상태 {'✅ '+status if contract_valid else '⛔ INVALID'}",
        f"⏱️ 일봉 시간해상도: 같은 봉 저점↔고점 순서 미확인 {temporal_same_day_unknown}행은 UNKNOWN으로 전략 표본 제외·계약 INVALID 비전파",
        f"🛡️ 0분모 가드: 비정상 가격행 제외 {invalid_price_rows} | 시퀀스 0분모 fail-closed {int(denominator_audit.iloc[0]['sequence_zero_denominator_rows'])} | 섹터 0분모 제외 {int(denominator_audit.iloc[0]['sector_invalid_denominator_rows'])} | 블록 중단 0",
        f"🧬 시퀀스: 5단계 이상 {sequence_ready}행 | 재료 사용가능 {catalyst_usable}행 | FULL_ALIGNMENT {full}행 | 잠재재료 재활성화 {reactivated}행",
        f"🔒 영향: LIVE 점수·순위·후보·AI Pick·진입·익절·손절·주문 변경 0 | snapshot {manifest['snapshot_id']}",
        f"🌐 시장·섹터 원장 {len(market_sector_ledger)}행 | TRUE 섹터지수 {int(joined.get('true_sector_index', pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not joined.empty else 0}행 | 없으면 INTERNAL_PEER_PROXY",
        "🧭 [패턴 시퀀스 상태]",
    ]
    if seq.empty:
        lines.append("- 시퀀스 입력 없음")
    else:
        for name, n in seq["sequence_stage"].value_counts().head(8).items():
            lines.append(f"- {name}: {int(n)}행")
        bad = int(seq["down_volume_expansion"].fillna(False).sum())
        dry = int(seq["supply_drying"].fillna(False).sum())
        lines.append(f"- 눌림 거래량 감소 SUPPLY_DRYING {dry}행 | 눌림 중 거래량 재확대 {bad}행")
    lines.append("🌍 [재료 생명주기]")
    if not state_counts:
        lines.append("- 원천 원장이 아직 비어 있습니다. 템플릿·어댑터는 준비됐으며 DATA_WARMUP으로 유지합니다.")
    else:
        for name, n in sorted(state_counts.items(), key=lambda x: (-x[1], x[0]))[:10]:
            lines.append(f"- {name}: {int(n)}행")
    lines.append("🧩 [정렬 수준]")
    for name, n in sorted(bucket_counts.items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"- {name}: {int(n)}행")
    if not perf.empty:
        lines.append("📊 [정렬 수준별 성과 · 견고지표 우선]")
        for _, r in perf.sort_values("n", ascending=False).head(10).iterrows():
            lines.append(f"- {r.get('research_bucket')} × {r.get('catalyst_state')}: n{int(r.get('n',0))}·날짜{int(r.get('signal_days',0))} | D3 평균 {_fmt(r.get('d3_mean'))}·중앙 {_fmt(r.get('d3_median'))}·절사 {_fmt(r.get('d3_trim10'))}·상2제외 {_fmt(r.get('d3_ex_top2'))} | +3 {_rate(r.get('plus3_first_rate'))}/SL {_rate(r.get('stop_first_rate'))}")
    lines += [
        "📰 [원천 교차검증 규칙]",
        "- 공식 공시·정부·기업발표 1개 또는 독립 원천 2개 이상만 cross_validated로 인정합니다. 같은 보도자료 복제는 도메인 수가 많아도 하나의 사건으로 군집합니다.",
        "- freshness(기사 신선도), thesis_validity(재료 유효성), price_activation(가격 인식)을 분리합니다. OLD여도 유효성과 가격 활성화가 생기면 재활성화 후보입니다.",
        "- AI 코멘트는 출처·생성시각이 확인되면 보조 원천, 없으면 RETROSPECTIVE/UNVERIFIED 힌트로만 사용합니다.",
        "🔐 [승격 규칙]",
        f"- 현재 정책: {'READY' if policy_ready else 'NOT_READY'} · 최소 {MIN_POLICY_ROWS}행·{MIN_POLICY_DATES}독립일·FULL_ALIGNMENT 10행 전 LIVE 승격 금지",
        "- TRUE 섹터지수가 없으면 INTERNAL_PEER_PROXY를 섹터 초과수익으로 위장하지 않습니다.",
        f"- Actions: {SEQUENCE_FILE} · {EVENT_RAW_FILE} · {EVENT_CLUSTER_FILE} · {JOIN_FILE} · {EVENT_EVAL_FILE} · {PERF_FILE} · {REGIME_FILE} · {SOURCE_AUDIT_FILE} · {MARKET_SECTOR_LEDGER_FILE} · {MARKET_SECTOR_COVERAGE_FILE} · {DENOMINATOR_AUDIT_FILE} · {REPRO_FILE} · {READINESS_FILE} · {MANUAL_FILE}",
    ]
    block = "\n".join(lines)
    (out / REPORT_FILE).write_text(block, encoding="utf-8")
    return _insert_block(base_report, block), {
        "sequence": seq, "raw_events": raw, "clusters": clusters, "joined": joined,
        "performance": perf, "regime": regime, "readiness": ready,
        "denominator_audit": denominator_audit, "manifest": manifest,
    }


def force_report(text: str, output_dir: str = "reports") -> str:
    p = Path(output_dir or "reports") / REPORT_FILE
    if not p.exists():
        return str(text or "")
    try:
        return _insert_block(str(text or ""), p.read_text(encoding="utf-8"))
    except Exception:
        return str(text or "")
