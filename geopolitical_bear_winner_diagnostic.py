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

VERSION = "V73.3.6.6.18"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🌍🐻 [지정학 이벤트 × 하락장 진짜 상승주 × 분할매수 진단 · RESEARCH_ONLY]"
REPORT_FILE = "v73_geo_bear_winner_report.txt"

EVENT_LEDGER_FILE = "v73_geo_event_ledger.csv"
STAGE_MACHINE_FILE = "v73_geo_event_stage_machine.csv"
EVENT_SECTOR_MAP_FILE = "v73_geo_event_to_sector_map.csv"
BENEFICIARY_FILE = "v73_geo_beneficiary_directness.csv"
BENEFICIARY_OVERRIDE_FILE = "v73_geo_beneficiary_override.csv"
WINNER_MASTER_FILE = "v73_bear_geo_winner_event_master.csv"
MATCHED_CONTROL_FILE = "v73_bear_geo_matched_control.csv"
COMMONALITY_FILE = "v73_bear_geo_commonality.csv"
FORMULA_SCORECARD_FILE = "v73_geo_formula_scorecard.csv"
SCALE_IN_FILE = "v73_geo_scale_in_policy.csv"
DEESCALATION_FILE = "v73_geo_deescalation_risk.csv"
AVAILABILITY_FILE = "v73_geo_data_availability.csv"
READINESS_FILE = "v73_geo_readiness.csv"

MIN_PERFORMANCE_ROWS = 30
MIN_SIGNAL_DAYS = 10
MIN_GEO_LINKED_ROWS = 10
DEFAULT_EVENT_LOOKBACK_DAYS = 20

CAUSAL_MODES = {"FORWARD_CAUSAL", "OFFICIAL_ARCHIVE_CAUSAL"}
BEAR_REGIMES = {"PANIC", "BEAR", "RISK_OFF", "RISK_OFF_BROAD", "RISK_OFF_NARROW"}

# Taxonomy only. It does not assert that a named company is an economic beneficiary.
EVENT_FAMILIES: dict[str, dict[str, Any]] = {
    "HORMUZ_ENERGY_DISRUPTION": {
        "event_keywords": ["호르무즈", "hormuz", "페르시아만", "persian gulf", "해협 봉쇄", "strait closure"],
        "chain": "CRUDE_OIL>REFINED_PRODUCT>TANKER_FREIGHT>WAR_RISK_INSURANCE",
        "positive_sectors": ["정유", "에너지", "석유", "가스", "LPG", "유조선", "탱커", "해운", "방산", "플랜트"],
        "negative_sectors": ["항공", "운송", "화학", "여행"],
        "default_duration_days": 14,
    },
    "WAR_ESCALATION": {
        "event_keywords": ["전쟁", "공습", "미사일", "침공", "무력 충돌", "war", "airstrike", "missile", "invasion", "military conflict"],
        "chain": "DEFENSE>ENERGY>SECURITY>LOGISTICS",
        "positive_sectors": ["방산", "드론", "위성", "보안", "에너지", "해운", "금"],
        "negative_sectors": ["항공", "여행", "소비", "운송"],
        "default_duration_days": 10,
    },
    "CEASEFIRE_RECONSTRUCTION": {
        "event_keywords": ["휴전", "종전", "평화 협상", "재건", "복구", "ceasefire", "peace talks", "reconstruction", "rebuild"],
        "chain": "CEASEFIRE>MASTERPLAN>TENDER>CONTRACT>EXECUTION",
        "positive_sectors": ["건설", "건설기계", "시멘트", "철강", "전력", "송배전", "통신", "철도", "공항", "상하수도"],
        "negative_sectors": ["방산", "원유테마"],
        "default_duration_days": 30,
    },
    "SHIPPING_ROUTE_DISRUPTION": {
        "event_keywords": ["홍해", "수에즈", "해상 봉쇄", "선박 공격", "red sea", "suez", "shipping disruption", "vessel attack"],
        "chain": "ROUTE_DISRUPTION>FREIGHT>INSURANCE>DELIVERY_DELAY",
        "positive_sectors": ["해운", "조선", "물류", "방산"],
        "negative_sectors": ["항공", "유통", "화학", "자동차"],
        "default_duration_days": 14,
    },
    "SANCTIONS_EXPORT_CONTROL": {
        "event_keywords": ["제재", "수출 통제", "금수", "관세 보복", "sanction", "export control", "embargo"],
        "chain": "POLICY>SUPPLY_RESTRICTION>SUBSTITUTION>LOCALIZATION",
        "positive_sectors": ["대체재", "국산화", "소재", "방산", "에너지"],
        "negative_sectors": ["수출", "반도체", "화학", "자동차"],
        "default_duration_days": 30,
    },
}

STAGE_RULES: list[tuple[str, list[str], int]] = [
    ("CONTRACT_AWARD", ["수주", "계약 체결", "낙찰", "contract award", "awarded contract"], 95),
    ("EXECUTION", ["착공", "집행", "납품", "공급 개시", "construction begins", "delivery begins", "execution"], 92),
    ("PHYSICAL_DISRUPTION", ["실제 봉쇄", "통항 중단", "공격 발생", "운항 중단", "closed", "blocked", "halted", "attack confirmed"], 90),
    ("TENDER_OR_MOU", ["mou", "업무협약", "입찰", "마스터플랜", "컨소시엄", "tender", "master plan"], 75),
    ("DE_ESCALATION", ["긴장 완화", "봉쇄 해제", "휴전 합의", "철군", "de-escalation", "reopened", "ceasefire agreement", "withdrawal"], 70),
    ("VERIFIED_THREAT", ["공식 경고", "정부 발표", "군 발표", "봉쇄 위협", "official warning", "threatens to close", "military statement"], 65),
    ("RECONSTRUCTION_EXPECTATION", ["재건 논의", "복구 계획", "재건 협력", "reconstruction plan", "rebuild plan"], 55),
    ("MARKET_CONFIRMATION", ["유가 급등", "운임 급등", "에너지주 급등", "oil surges", "freight surges"], 50),
    ("RUMOR", ["가능성", "우려", "관측", "검토", "rumor", "may", "could", "considering"], 25),
]

DIRECTNESS_ORDER = ["GEO_DIRECT", "GEO_CHAIN", "GEO_SECTOR", "GEO_NARRATIVE", "GEO_NEGATIVE", "UNKNOWN"]


def _out(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _read(path: Path, dtype: dict[str, Any] | None = None) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, low_memory=False, encoding=enc, dtype=dtype)
        except Exception:
            continue
    return pd.DataFrame()


def _write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_csv(path, index=False, encoding="utf-8-sig")


def _norm_code(v: Any) -> str:
    d = re.sub(r"\D", "", str(v or ""))
    return d.zfill(6)[-6:] if d else ""


def _clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


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


def _sha_frame(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "EMPTY"
    c = [x for x in cols if x in df.columns]
    raw = df[c].astype(str).sort_values(c, kind="stable").to_csv(index=False) if c else str(len(df))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _insert(text: str, block: str) -> str:
    s = str(text or "")
    if HEADER in s:
        st = s.find(HEADER)
        candidates = [s.find(h, st + len(HEADER)) for h in ["\n🪜 [", "\n🏆 [V48", "\n🧭 [시장 ×"]]
        candidates = [x for x in candidates if x >= 0]
        en = min(candidates) if candidates else len(s)
        s = (s[:st].rstrip() + "\n\n" + s[en:].lstrip()).strip()
    return s.rstrip() + "\n\n" + block


def _stage_machine() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    common = [
        ("RUMOR", 10, False, False),
        ("VERIFIED_THREAT", 20, True, False),
        ("PHYSICAL_DISRUPTION", 30, True, True),
        ("MARKET_CONFIRMATION", 40, True, True),
        ("DOMESTIC_BREADTH", 50, True, True),
        ("EXHAUSTION", 60, True, False),
        ("DE_ESCALATION", 70, True, False),
    ]
    rebuild = [
        ("RUMOR", 10, False, False),
        ("VERIFIED_THREAT", 20, True, False),
        ("RECONSTRUCTION_EXPECTATION", 30, True, False),
        ("TENDER_OR_MOU", 40, True, False),
        ("CONTRACT_AWARD", 50, True, True),
        ("EXECUTION", 60, True, True),
        ("DE_ESCALATION", 70, True, False),
    ]
    for family in EVENT_FAMILIES:
        seq = rebuild if family == "CEASEFIRE_RECONSTRUCTION" else common
        for stage, order, verified, fundamental in seq:
            rows.append({
                "event_family": family, "event_stage": stage, "stage_order": order,
                "verified_stage": verified, "fundamental_link_possible": fundamental,
                "research_only": True,
            })
    return pd.DataFrame(rows)


def _sector_map() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family, spec in EVENT_FAMILIES.items():
        for s in spec["positive_sectors"]:
            rows.append({"event_family": family, "sector_keyword": s, "impact": "POSITIVE", "default_directness": "GEO_SECTOR", "transmission_chain": spec["chain"]})
        for s in spec["negative_sectors"]:
            rows.append({"event_family": family, "sector_keyword": s, "impact": "NEGATIVE", "default_directness": "GEO_NEGATIVE", "transmission_chain": spec["chain"]})
    return pd.DataFrame(rows)


def _ensure_override_template(out: Path) -> pd.DataFrame:
    fp = out / BENEFICIARY_OVERRIDE_FILE
    schema = ["code", "name", "event_family", "directness", "evidence_type", "valid_from", "valid_to", "note", "enabled"]
    if not fp.exists():
        # The first row records the user's example as a narrative map, not as proof of direct earnings benefit.
        seed = pd.DataFrame([{
            "code": "024060", "name": "흥구석유", "event_family": "HORMUZ_ENERGY_DISRUPTION",
            "directness": "GEO_NARRATIVE", "evidence_type": "USER_SEED_EXAMPLE",
            "valid_from": "", "valid_to": "", "note": "유가/호르무즈 테마 반응 연구용. 직접 실적수혜로 간주하지 않음.", "enabled": True,
        }], columns=schema)
        _write(fp, seed)
    q = _read(fp, dtype={"code": str})
    for c in schema:
        if c not in q.columns:
            q[c] = ""
    q["code"] = q["code"].map(_norm_code)
    q["enabled"] = q["enabled"].map(_bool)
    return q[q["enabled"] & q["code"].ne("")].copy()


def _event_family(text: str, hinted: str = "") -> tuple[str, int]:
    t = f"{hinted} {text}".lower()
    best = ("UNCLASSIFIED_GEO", 0)
    for family, spec in EVENT_FAMILIES.items():
        score = sum(1 for k in spec["event_keywords"] if k.lower() in t)
        if score > best[1]:
            best = (family, score)
    return best


def _event_stage(text: str, family: str) -> tuple[str, int]:
    t = text.lower()
    for stage, keys, score in STAGE_RULES:
        if any(k.lower() in t for k in keys):
            return stage, score
    if family == "CEASEFIRE_RECONSTRUCTION" and any(k in t for k in ["재건", "복구", "reconstruction", "rebuild"]):
        return "RECONSTRUCTION_EXPECTATION", 45
    return "RUMOR", 10


def _source_quality(row: pd.Series) -> tuple[str, int]:
    official = _bool(row.get("official_source")) or str(row.get("source_type", "")).upper().startswith("OFFICIAL")
    independent = _bool(row.get("independent_source"))
    domain = str(row.get("source_domain", "")).lower()
    if official:
        return "OFFICIAL", 3
    if independent and domain:
        return "INDEPENDENT_SOURCE", 2
    return "UNVERIFIED", 1


def _load_source_ledger(out: Path) -> pd.DataFrame:
    names = [
        "v73_catalyst_source_ledger.csv", "v73_catalyst_event_raw_normalized.csv",
        "v73_official_disclosure_ledger.csv", "v73_geo_official_archive_ledger.csv", "v73_news_source_ledger.csv", "v73_global_catalyst_ledger.csv",
    ]
    frames: list[pd.DataFrame] = []
    for n in names:
        q = _read(out / n, dtype={"code": str})
        if not q.empty:
            q["_source_file"] = n
            frames.append(q)
    if not frames:
        return pd.DataFrame()
    q = pd.concat(frames, ignore_index=True, sort=False)
    key = _series_str(q, ["source_key", "source_id", "reference_id"], "")
    q["_dedup_key"] = key.where(key.ne(""), q.index.astype(str))
    return q.drop_duplicates("_dedup_key", keep="first").reset_index(drop=True)


def _build_event_ledger(raw: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "geo_event_id", "source_key", "source_type", "source_name", "source_domain", "source_url", "code", "name",
        "sector", "theme", "title", "summary", "event_family", "event_stage", "stage_score", "severity_score",
        "transmission_chain", "event_at", "published_at", "first_seen_at", "causal_mode", "causal_source_eligible",
        "same_day_causal_eligible", "source_quality", "source_quality_score", "official_source", "independent_source",
        "direct_benefit_hint", "materiality", "retrospective_quarantined", "raw_payload_sha256",
    ]
    if raw.empty:
        return pd.DataFrame(columns=cols)
    rows: list[dict[str, Any]] = []
    for i, r in raw.iterrows():
        text = " ".join([_clean(r.get("title")), _clean(r.get("summary")), _clean(r.get("theme")), _clean(r.get("event_type")), _clean(r.get("query_text"))])
        family, keyword_score = _event_family(text, _clean(r.get("event_type")))
        if family == "UNCLASSIFIED_GEO":
            continue
        stage, stage_score = _event_stage(text, family)
        quality, qscore = _source_quality(r)
        causal_mode = str(r.get("causal_mode") or "RETROSPECTIVE_RESEARCH").upper()
        published = pd.to_datetime(r.get("published_at"), errors="coerce", utc=True)
        official = pd.to_datetime(r.get("official_at"), errors="coerce", utc=True)
        first_seen = pd.to_datetime(r.get("first_seen_at"), errors="coerce", utc=True)
        occurred = pd.to_datetime(r.get("event_occurred_at"), errors="coerce", utc=True)
        candidates = [x for x in [official, published, occurred, first_seen] if pd.notna(x)]
        event_at = min(candidates) if candidates else pd.NaT
        source_key = str(r.get("source_key") or r.get("source_id") or r.get("reference_id") or f"ROW-{i}")
        geo_id = "GEO-" + hashlib.sha256(f"{family}|{stage}|{source_key}".encode("utf-8")).hexdigest()[:22]
        spec = EVENT_FAMILIES[family]
        severity = min(100, keyword_score * 15 + stage_score // 2 + qscore * 5)
        rows.append({
            "geo_event_id": geo_id, "source_key": source_key, "source_type": r.get("source_type", ""),
            "source_name": r.get("source_name", ""), "source_domain": r.get("source_domain", ""), "source_url": r.get("source_url", ""),
            "code": _norm_code(r.get("code")), "name": _clean(r.get("name")), "sector": _clean(r.get("sector")), "theme": _clean(r.get("theme")),
            "title": _clean(r.get("title")), "summary": _clean(r.get("summary")), "event_family": family, "event_stage": stage,
            "stage_score": stage_score, "severity_score": severity, "transmission_chain": spec["chain"],
            "event_at": event_at.isoformat() if pd.notna(event_at) else "", "published_at": str(r.get("published_at") or ""),
            "first_seen_at": str(r.get("first_seen_at") or ""), "causal_mode": causal_mode,
            "causal_source_eligible": causal_mode in CAUSAL_MODES and pd.notna(event_at),
            "same_day_causal_eligible": _bool(r.get("same_day_causal_eligible")), "source_quality": quality,
            "source_quality_score": qscore, "official_source": _bool(r.get("official_source")), "independent_source": _bool(r.get("independent_source")),
            "direct_benefit_hint": str(r.get("direct_benefit") or "UNKNOWN"), "materiality": str(r.get("materiality") or "UNKNOWN"),
            "retrospective_quarantined": causal_mode not in CAUSAL_MODES, "raw_payload_sha256": str(r.get("raw_payload_sha256") or ""),
        })
    q = pd.DataFrame(rows, columns=cols)
    if q.empty:
        return q
    return q.sort_values(["event_at", "geo_event_id"], kind="stable").drop_duplicates("geo_event_id", keep="first").reset_index(drop=True)


def _prepare_signals(eval_df: pd.DataFrame | None, out: Path) -> tuple[pd.DataFrame, str]:
    sources = [
        ("FORMULA_EXPLODED", out / "v72_search_formula_universe_exploded_eval.csv"),
        ("CONTEXT_EVENT", out / "v73_backtest_event_master.csv"),
        ("MARKET_EXCESS", out / "v72_market_excess_signal_audit.csv"),
    ]
    base = pd.DataFrame()
    source = "CALLER_DF"
    for label, p in sources:
        q = _read(p, dtype={"code": str})
        if not q.empty:
            base = q
            source = label
            break
    if base.empty and isinstance(eval_df, pd.DataFrame):
        base = eval_df.copy()
    if base.empty:
        return base, "NO_INPUT"

    q = base.copy()
    dc = _pick(q, ["signal_date", "date", "신호일"])
    cc = _pick(q, ["code", "Code", "종목코드"])
    if not dc or not cc:
        return pd.DataFrame(), "INVALID_SIGNAL_SCHEMA"
    q["signal_date"] = pd.to_datetime(q[dc], errors="coerce").dt.normalize()
    q["code"] = q[cc].map(_norm_code)
    q = q[q["signal_date"].notna() & q["code"].ne("")].copy()
    # Formula-expanded rows preserve every actual search formula. Market audit is joined by date/code
    # so market regime and forward benchmark returns are not lost or duplicated by guesswork.
    mx = _read(out / "v72_market_excess_signal_audit.csv", dtype={"code": str})
    if not mx.empty and source != "MARKET_EXCESS":
        mdc = _pick(mx, ["signal_date", "date", "신호일"])
        mcc = _pick(mx, ["code", "Code", "종목코드"])
        if mdc and mcc:
            mx["signal_date"] = pd.to_datetime(mx[mdc], errors="coerce").dt.normalize()
            mx["code"] = mx[mcc].map(_norm_code)
            keep_map = {
                "market_regime_causal": "mx_market_regime_causal", "market_regime": "mx_market_regime",
                "market": "mx_market", "Market": "mx_Market", "market_fwd_ret3": "mx_market_fwd_ret3",
                "market_ret3": "mx_market_ret3", "market_excess3": "mx_market_excess3", "excess3": "mx_excess3",
            }
            keep = ["signal_date", "code"] + [c for c in keep_map if c in mx.columns]
            mx = mx[keep].drop_duplicates(["signal_date", "code"], keep="last").rename(columns=keep_map)
            q = q.merge(mx, on=["signal_date", "code"], how="left")
    q["name"] = _series_str(q, ["name", "Name", "종목명"], "")
    q["formula"] = _series_str(q, ["formula", "검색식", "primary_formula", "search_pattern_primary", "pattern_combo"], "UNCLASSIFIED").replace({"": "UNCLASSIFIED", "nan": "UNCLASSIFIED"})
    q["sector"] = _series_str(q, ["sector_label", "sector", "Sector", "섹터", "업종"], "UNKNOWN").replace({"": "UNKNOWN", "nan": "UNKNOWN"})
    q["theme"] = _series_str(q, ["theme", "Theme", "테마"], "UNKNOWN").replace({"": "UNKNOWN", "nan": "UNKNOWN"})
    q["market"] = _series_str(q, ["market", "Market", "exchange", "mx_market", "mx_Market"], "UNKNOWN")
    q["market_regime"] = _series_str(q, ["market_regime_causal", "market_regime", "market_state", "mx_market_regime_causal", "mx_market_regime"], "UNKNOWN").str.upper()
    q["ret1"] = _series_num(q, ["ret1", "next1_close_ret", "day1_ret"])
    q["ret3"] = _series_num(q, ["ret3", "next3_close_ret", "day3_ret"])
    q["ret5"] = _series_num(q, ["ret5", "next5_close_ret", "day5_ret"])
    q["mfe"] = _series_num(q, ["mfe", "max_up_5d", "MFE_5D", "mfe_5d"])
    q["mae"] = _series_num(q, ["mae", "max_down_5d", "MAE_5D", "mae_5d"])
    q["market_fwd_ret3"] = _series_num(q, ["market_fwd_ret3", "market_ret3", "benchmark_ret3", "mx_market_fwd_ret3", "mx_market_ret3"])
    q["market_excess3"] = _series_num(q, ["market_excess3", "excess3", "d3_market_excess", "mx_market_excess3", "mx_excess3"])
    # Derive only when both are causally available.
    miss_excess = q["market_excess3"].isna() & q["ret3"].notna() & q["market_fwd_ret3"].notna()
    q.loc[miss_excess, "market_excess3"] = q.loc[miss_excess, "ret3"] - q.loc[miss_excess, "market_fwd_ret3"]
    q["plus3_first"] = _series_str(q, ["plus3_first", "hit_plus3_first", "plus3_first_10d", "plus3"], "").map(_bool)
    q["stop_first"] = _series_str(q, ["stop_first", "hit_stop_first", "stop_first_10d", "minus3_first"], "").map(_bool)
    for c, names in {
        "turnover": ["turnover", "trading_value", "거래대금"],
        "volume_ratio": ["volume_ratio", "vol_ratio", "거래량비율"],
        "distance_low60": ["distance_low60", "distance_from_60d_low_pct", "low60_distance_pct"],
        "upper_space": ["upper_space", "upper_resistance_distance_pct", "upper_space_pct"],
        "rsi": ["rsi", "RSI"],
        "ma_gap": ["ma_gap", "distance_ma", "이격"],
        "relative_strength_5d": ["relative_strength_5d", "stock_market_rel5", "market_excess_past5"],
        "close_location": ["close_location", "close_position_pct", "종가위치"],
        "pullback_volume_ratio": ["pullback_volume_ratio", "눌림거래량비율"],
    }.items():
        q[c] = _series_num(q, names)
    q = q.sort_values(["signal_date", "code", "formula"], kind="stable").drop_duplicates(["signal_date", "code", "formula"], keep="first")
    return q.reset_index(drop=True), source


def _winner_class(r: pd.Series) -> str:
    regime = str(r.get("market_regime") or "UNKNOWN").upper()
    if regime not in BEAR_REGIMES:
        return "NON_BEAR_REGIME"
    ret3 = _num(r.get("ret3"))
    excess = _num(r.get("market_excess3"))
    mfe = _num(r.get("mfe"))
    ret5 = _num(r.get("ret5"))
    if not math.isfinite(ret3) or not math.isfinite(excess):
        return "BEAR_MARKET_BENCHMARK_MISSING"
    if _bool(r.get("plus3_first")) and not _bool(r.get("stop_first")) and ret3 > 0 and excess > 0:
        return "BEAR_FAST_WINNER"
    if math.isfinite(mfe) and mfe >= 10 and ((math.isfinite(ret5) and ret5 > 0) or ret3 > 0) and excess > 0:
        return "BEAR_BIG_WINNER"
    if ret3 > 0 and excess > 0:
        return "BEAR_TRUE_WINNER"
    if ret3 <= 0 and excess > 0:
        return "BEAR_RELATIVE_SURVIVOR"
    if ret3 > 0 and excess <= 0:
        return "BEAR_FALSE_WINNER"
    return "BEAR_FAILURE"


def _causal_event_candidates(events: pd.DataFrame, signal_date: pd.Timestamp, lookback_days: int) -> pd.DataFrame:
    if events.empty:
        return events
    q = events[events["causal_source_eligible"].fillna(False)].copy()
    q["_event_ts"] = pd.to_datetime(q["event_at"], errors="coerce", utc=True)
    q["_event_date"] = q["_event_ts"].dt.tz_convert(None).dt.normalize()
    lo = signal_date - pd.Timedelta(days=lookback_days)
    # Date-only or same-day sources are not assumed known before the close unless explicitly eligible.
    before = q["_event_date"].lt(signal_date)
    same = q["_event_date"].eq(signal_date) & q["same_day_causal_eligible"].fillna(False)
    return q[(q["_event_date"].ge(lo)) & (before | same)].copy()


def _text_match(text: str, keyword: str) -> bool:
    return keyword.lower() in text.lower()


def _directness(signal: pd.Series, event: pd.Series, sector_map: pd.DataFrame, overrides: pd.DataFrame) -> tuple[str, str, str]:
    code = _norm_code(signal.get("code"))
    family = str(event.get("event_family"))
    if not overrides.empty:
        m = overrides[(overrides["code"].eq(code)) & (overrides["event_family"].eq(family))]
        if not m.empty:
            r = m.iloc[-1]
            return str(r.get("directness") or "UNKNOWN"), str(r.get("evidence_type") or "OVERRIDE"), str(r.get("note") or "")
    if code and code == _norm_code(event.get("code")):
        hint = str(event.get("direct_benefit_hint") or "").upper()
        return ("GEO_DIRECT" if hint in {"YES", "TRUE", "DIRECT", "HIGH"} else "GEO_CHAIN", "SOURCE_CODE_LINK", "source ledger exact-code association")
    text = " ".join([_clean(signal.get("sector")), _clean(signal.get("theme")), _clean(signal.get("name"))])
    m = sector_map[sector_map["event_family"].eq(family)]
    positives = m[m["impact"].eq("POSITIVE")]
    negatives = m[m["impact"].eq("NEGATIVE")]
    for _, r in negatives.iterrows():
        if _text_match(text, str(r["sector_keyword"])):
            return "GEO_NEGATIVE", "SECTOR_KEYWORD", str(r["sector_keyword"])
    for _, r in positives.iterrows():
        if _text_match(text, str(r["sector_keyword"])):
            if family in {"HORMUZ_ENERGY_DISRUPTION", "SHIPPING_ROUTE_DISRUPTION", "SANCTIONS_EXPORT_CONTROL"}:
                return "GEO_CHAIN", "SECTOR_CHAIN_KEYWORD", str(r["sector_keyword"])
            return "GEO_SECTOR", "SECTOR_KEYWORD", str(r["sector_keyword"])
    return "UNKNOWN", "NO_CAUSAL_BENEFICIARY_MAP", ""


def _link_events(signals: pd.DataFrame, events: pd.DataFrame, sector_map: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    lookback = max(1, int(os.environ.get("GEO_EVENT_LOOKBACK_DAYS", DEFAULT_EVENT_LOOKBACK_DAYS)))
    for _, s in signals.iterrows():
        sig_date = pd.Timestamp(s["signal_date"]).normalize()
        candidates = _causal_event_candidates(events, sig_date, lookback)
        ranked: list[tuple[int, int, int, pd.Series, str, str, str]] = []
        for _, e in candidates.iterrows():
            directness, evidence, note = _directness(s, e, sector_map, overrides)
            if directness == "UNKNOWN":
                continue
            direct_rank = max(0, len(DIRECTNESS_ORDER) - DIRECTNESS_ORDER.index(directness)) if directness in DIRECTNESS_ORDER else 0
            age = int((sig_date - pd.to_datetime(e["event_at"], errors="coerce", utc=True).tz_convert(None).normalize()).days)
            ranked.append((direct_rank, int(_num(e.get("severity_score"), 0)), -age, e, directness, evidence, note))
        ranked.sort(key=lambda z: (z[0], z[1], z[2]), reverse=True)
        e = ranked[0][3] if ranked else pd.Series(dtype=object)
        directness = ranked[0][4] if ranked else "UNKNOWN"
        evidence = ranked[0][5] if ranked else "NO_EVENT_LINK"
        note = ranked[0][6] if ranked else ""
        rec = s.to_dict()
        rec.update({
            "winner_class": _winner_class(s),
            "geo_linked": bool(ranked), "geo_event_id": e.get("geo_event_id", ""), "event_family": e.get("event_family", "NO_CAUSAL_GEO_EVENT"),
            "event_stage": e.get("event_stage", "UNKNOWN"), "event_at": e.get("event_at", ""),
            "event_age_days": int((sig_date - pd.to_datetime(e.get("event_at"), errors="coerce", utc=True).tz_convert(None).normalize()).days) if ranked else np.nan,
            "directness": directness, "directness_evidence": evidence, "beneficiary_note": note,
            "source_quality": e.get("source_quality", "MISSING"), "severity_score": _num(e.get("severity_score")),
            "transmission_chain": e.get("transmission_chain", ""), "retrospective_quarantined": False,
        })
        win = rec["winner_class"]
        if win.startswith("BEAR_") and "WINNER" in win:
            rec["bear_geo_bucket"] = {
                "GEO_DIRECT": "BEAR_GEO_DIRECT_WINNER", "GEO_CHAIN": "BEAR_GEO_CHAIN_WINNER",
                "GEO_SECTOR": "BEAR_GEO_SECTOR_WINNER", "GEO_NARRATIVE": "BEAR_GEO_NARRATIVE_WINNER",
                "GEO_NEGATIVE": "BEAR_GEO_NEGATIVE_EXPOSURE_WINNER",
            }.get(directness, "BEAR_TECHNICAL_WINNER")
        elif win == "BEAR_RELATIVE_SURVIVOR":
            rec["bear_geo_bucket"] = "BEAR_RELATIVE_SURVIVOR"
        elif win == "BEAR_FAILURE":
            rec["bear_geo_bucket"] = "BEAR_FAILURE"
        else:
            rec["bear_geo_bucket"] = win
        rows.append(rec)
    return pd.DataFrame(rows)


def _matched_controls(master: pd.DataFrame) -> pd.DataFrame:
    if master.empty:
        return pd.DataFrame()
    winners = master[master["winner_class"].isin(["BEAR_TRUE_WINNER", "BEAR_FAST_WINNER", "BEAR_BIG_WINNER"])].copy()
    failures = master[master["winner_class"].eq("BEAR_FAILURE")].copy()
    rows: list[dict[str, Any]] = []
    used: set[str] = set()
    for _, w in winners.iterrows():
        cand = failures[failures["signal_date"].eq(w["signal_date"])].copy()
        if cand.empty:
            continue
        cand["match_level"] = 0
        same_market = cand["market"].astype(str).eq(str(w.get("market")))
        same_formula = cand["formula"].astype(str).eq(str(w.get("formula")))
        same_sector = cand["sector"].astype(str).eq(str(w.get("sector"))) & cand["sector"].ne("UNKNOWN")
        cand["match_level"] = same_market.astype(int) + same_formula.astype(int) * 2 + same_sector.astype(int) * 4
        wt = _num(w.get("turnover"))
        if math.isfinite(wt) and wt > 0:
            cand["distance"] = (np.log1p(pd.to_numeric(cand["turnover"], errors="coerce").fillna(0)) - math.log1p(wt)).abs()
        else:
            cand["distance"] = 0.0
        cand["_id"] = cand["signal_date"].astype(str) + "|" + cand["code"] + "|" + cand["formula"]
        cand = cand[~cand["_id"].isin(used)].sort_values(["match_level", "distance", "code"], ascending=[False, True, True])
        if cand.empty:
            continue
        c = cand.iloc[0]
        used.add(str(c["_id"]))
        rows.append({
            "match_id": "MATCH-" + hashlib.sha256(f"{w.signal_date}|{w.code}|{c.code}|{w.formula}".encode()).hexdigest()[:20],
            "signal_date": w["signal_date"], "winner_code": w["code"], "winner_name": w.get("name", ""), "control_code": c["code"], "control_name": c.get("name", ""),
            "winner_formula": w["formula"], "control_formula": c["formula"], "winner_sector": w.get("sector", ""), "control_sector": c.get("sector", ""),
            "match_level": int(c["match_level"]), "winner_ret3": w.get("ret3"), "control_ret3": c.get("ret3"),
            "winner_excess3": w.get("market_excess3"), "control_excess3": c.get("market_excess3"),
            "winner_directness": w.get("directness", "UNKNOWN"), "winner_event_family": w.get("event_family", ""),
        })
    return pd.DataFrame(rows)


def _commonality(master: pd.DataFrame, matched: pd.DataFrame) -> pd.DataFrame:
    if master.empty:
        return pd.DataFrame()
    win = master[master["winner_class"].isin(["BEAR_TRUE_WINNER", "BEAR_FAST_WINNER", "BEAR_BIG_WINNER"])]
    fail = master[master["winner_class"].eq("BEAR_FAILURE")]
    features = ["turnover", "volume_ratio", "distance_low60", "upper_space", "rsi", "ma_gap", "relative_strength_5d", "close_location", "pullback_volume_ratio", "mfe", "mae"]
    rows: list[dict[str, Any]] = []
    for f in features:
        if f not in master.columns:
            continue
        a = pd.to_numeric(win[f], errors="coerce").dropna()
        b = pd.to_numeric(fail[f], errors="coerce").dropna()
        if a.empty and b.empty:
            continue
        wm = a.median() if len(a) else np.nan
        fm = b.median() if len(b) else np.nan
        rows.append({
            "feature": f, "winner_n": len(a), "failure_n": len(b), "winner_median": wm, "failure_median": fm,
            "median_diff": wm - fm if math.isfinite(_num(wm)) and math.isfinite(_num(fm)) else np.nan,
            "winner_mean": a.mean() if len(a) else np.nan, "failure_mean": b.mean() if len(b) else np.nan,
            "comparison": "BEAR_WINNER_VS_BEAR_FAILURE",
        })
    # Discrete event dimensions.
    for dim in ["geo_linked", "directness", "event_family", "event_stage", "source_quality", "bear_geo_bucket"]:
        if dim not in master.columns:
            continue
        for label, g in master.groupby(dim, dropna=False):
            n = len(g)
            if not n:
                continue
            rows.append({
                "feature": dim, "label": str(label), "winner_n": int(g["winner_class"].isin(["BEAR_TRUE_WINNER", "BEAR_FAST_WINNER", "BEAR_BIG_WINNER"]).sum()),
                "failure_n": int(g["winner_class"].eq("BEAR_FAILURE").sum()), "winner_median": pd.to_numeric(g.loc[g["winner_class"].str.contains("WINNER", na=False), "ret3"], errors="coerce").median(),
                "failure_median": pd.to_numeric(g.loc[g["winner_class"].eq("BEAR_FAILURE"), "ret3"], errors="coerce").median(),
                "comparison": "EVENT_DIMENSION",
            })
    return pd.DataFrame(rows)


def _perf(g: pd.DataFrame, label: str, dimension: str) -> dict[str, Any]:
    r = pd.to_numeric(g["ret3"], errors="coerce")
    ex = pd.to_numeric(g["market_excess3"], errors="coerce")
    date_mean = g.assign(_r=r).groupby("signal_date")["_r"].mean() if len(g) else pd.Series(dtype=float)
    return {
        "dimension": dimension, "label": label, "n": len(g), "stocks": g["code"].nunique(), "signal_days": g["signal_date"].nunique(),
        "d1_mean": pd.to_numeric(g["ret1"], errors="coerce").mean(), "d3_mean": r.mean(), "d3_median": r.median(), "d3_trim10": _trim_mean(r), "d3_top5_removed": _top_removed(r),
        "d5_mean": pd.to_numeric(g["ret5"], errors="coerce").mean(), "excess3_mean": ex.mean(), "excess3_median": ex.median(), "excess3_top5_removed": _top_removed(ex),
        "true_winner_rate": float(g["winner_class"].isin(["BEAR_TRUE_WINNER", "BEAR_FAST_WINNER", "BEAR_BIG_WINNER"]).mean() * 100) if len(g) else np.nan,
        "failure_rate": float(g["winner_class"].eq("BEAR_FAILURE").mean() * 100) if len(g) else np.nan,
        "positive_signal_day_rate": float((date_mean > 0).mean() * 100) if len(date_mean) else np.nan,
    }


def _formula_scorecard(master: pd.DataFrame) -> pd.DataFrame:
    if master.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    bear = master[master["market_regime"].isin(BEAR_REGIMES)]
    for formula, g in bear.groupby("formula"):
        rows.append(_perf(g, str(formula), "FORMULA_BEAR"))
    for (formula, directness), g in bear[bear["geo_linked"]].groupby(["formula", "directness"]):
        rows.append(_perf(g, f"{formula}|{directness}", "FORMULA_X_DIRECTNESS"))
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["policy_status"] = np.where(
        (out["n"] >= MIN_PERFORMANCE_ROWS) & (out["signal_days"] >= MIN_SIGNAL_DAYS) &
        (out["d3_median"] > 0) & (out["d3_trim10"] > 0) & (out["d3_top5_removed"] > 0) &
        (out["excess3_median"] > 0) & (out["excess3_top5_removed"] > 0),
        "POLICY_CANDIDATE", "RESEARCH_ONLY"
    )
    return out.sort_values(["policy_status", "excess3_median", "n"], ascending=[True, False, False], na_position="last")


def _scale_in(master: pd.DataFrame, out: Path) -> pd.DataFrame:
    scale = _read(out / "v73_scale_in_event_policy.csv", dtype={"code": str})
    if scale.empty or master.empty:
        return pd.DataFrame()
    scale["signal_date"] = pd.to_datetime(scale["signal_date"], errors="coerce").dt.normalize()
    scale["code"] = scale["code"].map(_norm_code)
    keys = master[["signal_date", "code", "formula", "market_regime", "winner_class", "bear_geo_bucket", "geo_linked", "event_family", "event_stage", "directness"]].drop_duplicates(["signal_date", "code", "formula"])
    q = scale.merge(keys, on=["signal_date", "code", "formula"], how="inner")
    q = q[q["market_regime"].isin(BEAR_REGIMES)] if "market_regime" in q.columns else q
    rows: list[dict[str, Any]] = []
    for (bucket, policy), g in q.groupby(["bear_geo_bucket", "policy"], dropna=False):
        r = pd.to_numeric(g.get("net20_return_pct"), errors="coerce")
        rr = pd.to_numeric(g.get("r_multiple"), errors="coerce")
        rows.append({
            "bear_geo_bucket": bucket, "policy": policy, "n": len(g), "signal_days": g["signal_date"].nunique(),
            "net20_mean": r.mean(), "net20_median": r.median(), "net20_trim10": _trim_mean(r), "net20_top5_removed": _top_removed(r),
            "r_multiple_mean": rr.mean(), "add_trigger_rate": float(g.get("add_triggered", pd.Series(False, index=g.index)).map(_bool).mean() * 100) if len(g) else np.nan,
            "avg_invested_weight": pd.to_numeric(g.get("invested_weight"), errors="coerce").mean(),
        })
    return pd.DataFrame(rows)


def _deescalation(events: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family in EVENT_FAMILIES:
        fam = events[events["event_family"].eq(family)] if not events.empty else pd.DataFrame()
        de = fam[fam["event_stage"].eq("DE_ESCALATION")] if not fam.empty else pd.DataFrame()
        linked = master[master["event_family"].eq(family)] if not master.empty else pd.DataFrame()
        narrative = linked[linked["directness"].eq("GEO_NARRATIVE")] if not linked.empty else pd.DataFrame()
        rows.append({
            "event_family": family, "deescalation_event_rows": len(de), "linked_signal_rows": len(linked), "narrative_signal_rows": len(narrative),
            "narrative_d3_mean": pd.to_numeric(narrative.get("ret3"), errors="coerce").mean() if not narrative.empty else np.nan,
            "risk_rule": "DE_ESCALATION_AFTER_THEME_SPIKE_NO_ADD" if len(de) else "WATCH_DEESCALATION_SOURCE",
            "note": "긴장 완화·봉쇄 해제·휴전 합의 뒤 GEO_NARRATIVE 추가매수 금지; 직접수혜도 계약/실적 증거 재확인",
        })
    return pd.DataFrame(rows)


def _availability(signals: pd.DataFrame, events: pd.DataFrame, master: pd.DataFrame, matched: pd.DataFrame, scale: pd.DataFrame) -> pd.DataFrame:
    total = len(signals)
    fields = [
        ("market_regime", int(signals["market_regime"].ne("UNKNOWN").sum()) if total else 0, total, "하락장 코호트"),
        ("market_excess3", int(signals["market_excess3"].notna().sum()) if total else 0, total, "절대·초과수익 분리"),
        ("causal_geo_event", int(events["causal_source_eligible"].sum()) if len(events) else 0, len(events), "FORWARD/공식 아카이브만"),
        ("geo_linked_signal", int(master["geo_linked"].sum()) if len(master) else 0, len(master), "사건→수혜경로 결합"),
        ("true_bear_winner", int(master["winner_class"].isin(["BEAR_TRUE_WINNER", "BEAR_FAST_WINNER", "BEAR_BIG_WINNER"]).sum()) if len(master) else 0, len(master), "하락장 진짜 승자"),
        ("matched_control", len(matched), int(master["winner_class"].str.contains("WINNER", na=False).sum()) if len(master) else 0, "동일일 패자 대조군"),
        ("geo_scale_in", len(scale), len(scale), "분할매수 정책 결합"),
    ]
    rows = []
    for field, available, denom, note in fields:
        coverage = available / denom * 100 if denom else np.nan
        rows.append({"field": field, "available": available, "total": denom, "coverage_pct": coverage, "status": "OK" if denom and coverage >= 70 else ("PARTIAL" if available else "MISSING"), "note": note})
    return pd.DataFrame(rows)


def _report(master: pd.DataFrame, events: pd.DataFrame, matched: pd.DataFrame, formula: pd.DataFrame, scale: pd.DataFrame, source: str, status: str) -> str:
    bear = master[master["market_regime"].isin(BEAR_REGIMES)] if not master.empty else pd.DataFrame()
    counts = bear["winner_class"].value_counts().to_dict() if not bear.empty else {}
    lines = [
        HEADER,
        f"📌 {VERSION} · GEOPOLITICAL_BEAR_WINNER_DIAGNOSTIC · RESEARCH_ONLY=True",
        "- 하락장 상승주를 기술적 강세·지정학 직접수혜·공급망 연동·섹터 확산·테마 반응으로 분리하고, 같은 날 패자와 비교합니다.",
        f"🧾 입력: {source} | 신호 {len(master)}행 · 하락장 {len(bear)}행 · 인과 지정학 이벤트 {int(events['causal_source_eligible'].sum()) if len(events) else 0}행 · 매칭대조 {len(matched)}쌍 | 상태 {status}",
        "🐻 [하락장 절대수익 × 지수초과 분류]",
        f"- 진짜승자 {counts.get('BEAR_TRUE_WINNER',0)} · 빠른승자 {counts.get('BEAR_FAST_WINNER',0)} · 큰승자 {counts.get('BEAR_BIG_WINNER',0)} · 상대방어 {counts.get('BEAR_RELATIVE_SURVIVOR',0)} · 시장베타 {counts.get('BEAR_FALSE_WINNER',0)} · 실패 {counts.get('BEAR_FAILURE',0)}",
        "🌍 [지정학 연결 승자]",
    ]
    geo_win = bear[bear["winner_class"].isin(["BEAR_TRUE_WINNER", "BEAR_FAST_WINNER", "BEAR_BIG_WINNER"]) & bear["geo_linked"]] if not bear.empty else pd.DataFrame()
    if geo_win.empty:
        lines.append("- 인과 시점이 확인된 지정학 이벤트와 결합된 하락장 진짜 승자 표본이 아직 없습니다.")
    else:
        for (family, directness), g in geo_win.groupby(["event_family", "directness"]):
            lines.append(f"- {family} · {directness}: n{len(g)}·날짜{g['signal_date'].nunique()} | D3 {pd.to_numeric(g['ret3'],errors='coerce').mean():+.2f}%·중앙 {pd.to_numeric(g['ret3'],errors='coerce').median():+.2f}% | 지수초과 {pd.to_numeric(g['market_excess3'],errors='coerce').median():+.2f}%")
    lines.append("🏆 [하락장 검색식 × 지정학 경로 견고성]")
    show = formula[(formula["n"] >= 3)].head(8) if not formula.empty else pd.DataFrame()
    if show.empty:
        lines.append("- 최소표본 부족")
    else:
        for _, r in show.iterrows():
            lines.append(f"- {r['label']}: n{int(r['n'])}/일{int(r['signal_days'])} | D3 중앙 {_num(r['d3_median']):+.2f}%·절사 {_num(r['d3_trim10']):+.2f}%·상5제외 {_num(r['d3_top5_removed']):+.2f}% | 초과 중앙 {_num(r['excess3_median']):+.2f}% | 진짜승자 { _num(r['true_winner_rate']):.1f}% | {r['policy_status']}")
    lines.append("🪜 [지정학 하락장 승자 × 분할매수]")
    ss = scale[scale["n"] >= 3].sort_values(["net20_median", "n"], ascending=[False, False]).head(6) if not scale.empty else pd.DataFrame()
    if ss.empty:
        lines.append("- 분할매수 원장과 결합된 최소표본이 부족합니다.")
    else:
        for _, r in ss.iterrows():
            lines.append(f"- {r['bear_geo_bucket']} · {r['policy']}: n{int(r['n'])}/일{int(r['signal_days'])} | 20bp 평균 {_num(r['net20_mean']):+.2f}%·중앙 {_num(r['net20_median']):+.2f}% | R {_num(r['r_multiple_mean']):+.2f} | 추가실행 {_num(r['add_trigger_rate']):.1f}%")
    lines += [
        "🛡️ [인과·직접성 안전계약]",
        "- 오늘 검색해 복원한 과거 뉴스는 RETROSPECTIVE_RESEARCH로 격리하고 성과 근거에 사용하지 않습니다.",
        "- 흥구석유 예시는 GEO_NARRATIVE로 시작하며, 원유 생산·직접 실적수혜로 자동 승격하지 않습니다.",
        "- 전쟁 확대와 휴전·재건은 별도 이벤트 단계입니다. 재건은 MOU→입찰→수주→집행을 구분합니다.",
        "- 지정학 테마는 가격 하락만으로 추가매수하지 않고, 사건 확인·전파자산 반응·국내 breadth·재지지 순서가 확인될 때만 연구합니다.",
        f"🔒 승격: {MIN_PERFORMANCE_ROWS}행·{MIN_SIGNAL_DAYS}독립일 + 절대/초과수익 중앙·절사·상위5제거 양수 + walk-forward 유지. LIVE 변경 0.",
        f"- Actions CSV: {EVENT_LEDGER_FILE} · {STAGE_MACHINE_FILE} · {EVENT_SECTOR_MAP_FILE} · {BENEFICIARY_FILE} · {WINNER_MASTER_FILE} · {MATCHED_CONTROL_FILE} · {COMMONALITY_FILE} · {FORMULA_SCORECARD_FILE} · {SCALE_IN_FILE} · {DEESCALATION_FILE} · {AVAILABILITY_FILE} · {READINESS_FILE}",
    ]
    return "\n".join(lines)


def run_backtest(eval_df: pd.DataFrame | None = None, output_dir: str | Path = "reports", base_report: str = "") -> tuple[str, dict[str, pd.DataFrame]]:
    out = _out(output_dir)
    generated_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    stage = _stage_machine()
    sector_map = _sector_map()
    overrides = _ensure_override_template(out)
    raw = _load_source_ledger(out)
    events = _build_event_ledger(raw)
    signals, source = _prepare_signals(eval_df, out)
    master = _link_events(signals, events, sector_map, overrides) if not signals.empty else pd.DataFrame()
    matched = _matched_controls(master)
    commonality = _commonality(master, matched)
    formula = _formula_scorecard(master)
    scale = _scale_in(master, out)
    deesc = _deescalation(events, master)
    availability = _availability(signals, events, master, matched, scale)

    snapshot = _sha_frame(master, ["signal_date", "code", "formula", "winner_class", "geo_event_id", "directness"])
    tables = [events, stage, sector_map, overrides, master, matched, commonality, formula, scale, deesc, availability]
    for q in tables:
        if isinstance(q, pd.DataFrame):
            q["version"] = VERSION
            q["snapshot_id"] = snapshot
            q["generated_at"] = generated_at
            q["research_only"] = True
            q["live_logic_changed"] = False
            q["real_order_changed"] = False

    causal_events = int(events["causal_source_eligible"].sum()) if len(events) else 0
    geo_linked = int(master["geo_linked"].sum()) if len(master) else 0
    bear_days = int(master.loc[master["market_regime"].isin(BEAR_REGIMES), "signal_date"].nunique()) if len(master) else 0
    true_winners = int(master["winner_class"].isin(["BEAR_TRUE_WINNER", "BEAR_FAST_WINNER", "BEAR_BIG_WINNER"]).sum()) if len(master) else 0
    policy_candidates = int(formula["policy_status"].eq("POLICY_CANDIDATE").sum()) if len(formula) else 0
    if signals.empty:
        status = "NO_SIGNAL_INPUT"
    elif causal_events == 0 or geo_linked < MIN_GEO_LINKED_ROWS:
        status = "VALID_SHADOW_DATA_WARMUP"
    else:
        status = "VALID_SHADOW"
    policy_ready = bool(policy_candidates and bear_days >= MIN_SIGNAL_DAYS and true_winners >= MIN_PERFORMANCE_ROWS)
    readiness = pd.DataFrame([{
        "version": VERSION, "status": status, "source": source, "signal_rows": len(signals), "bear_signal_rows": int(master["market_regime"].isin(BEAR_REGIMES).sum()) if len(master) else 0,
        "bear_signal_days": bear_days, "source_rows": len(raw), "geo_event_rows": len(events), "causal_geo_event_rows": causal_events,
        "retrospective_quarantined_rows": int(events["retrospective_quarantined"].sum()) if len(events) else 0,
        "geo_linked_signal_rows": geo_linked, "true_bear_winner_rows": true_winners, "matched_control_rows": len(matched),
        "formula_policy_candidate_count": policy_candidates, "policy_ready": policy_ready, "snapshot_id": snapshot, "generated_at": generated_at,
        "research_only": True, "live_logic_changed": False, "real_order_changed": False,
    }])

    file_map = {
        EVENT_LEDGER_FILE: events, STAGE_MACHINE_FILE: stage, EVENT_SECTOR_MAP_FILE: sector_map,
        BENEFICIARY_FILE: overrides, WINNER_MASTER_FILE: master, MATCHED_CONTROL_FILE: matched,
        COMMONALITY_FILE: commonality, FORMULA_SCORECARD_FILE: formula, SCALE_IN_FILE: scale,
        DEESCALATION_FILE: deesc, AVAILABILITY_FILE: availability, READINESS_FILE: readiness,
    }
    for name, q in file_map.items():
        _write(out / name, q)
    block = _report(master, events, matched, formula, scale, source, status)
    (out / REPORT_FILE).write_text(block, encoding="utf-8")
    return _insert(base_report, block), {
        "event_ledger": events, "stage_machine": stage, "event_sector_map": sector_map, "beneficiary_directness": overrides,
        "winner_master": master, "matched_control": matched, "commonality": commonality, "formula_scorecard": formula,
        "scale_in": scale, "deescalation": deesc, "availability": availability, "readiness": readiness,
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
