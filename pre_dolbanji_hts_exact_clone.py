# -*- coding: utf-8 -*-
"""
예비돌반지 HTS 정확복제형 (스크린샷 기반)
-------------------------------------------------------
목표
- 사용자가 올린 영웅문 HTS 조건식을 기술적으로 최대한 1:1에 가깝게 복제
- 기존 예비돌반지(해석형/완화형)와 별도로 운영
- 더 엄격하고 더 적게 잡히는 '정확복제형' 패턴으로 사용

주의
- 스크린샷 해상도상 불명확한 재무/제외 조건(O 일부)은 주석으로 남기고
  읽히는 조건은 그대로 구현
- 기술조건 A~J는 그대로 반영
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional
import math
import pandas as pd


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.replace(",", "").strip()
            if x == "":
                return default
        v = float(x)
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def safe_str(x: Any, default: str = "") -> str:
    try:
        if x is None:
            return default
        return str(x).strip()
    except Exception:
        return default


def bool_or_false(x: Any) -> bool:
    try:
        return bool(x)
    except Exception:
        return False


@dataclass
class HTSExactResult:
    passed: bool
    score: int
    max_score: int
    tags: list
    detail: Dict[str, Any]


def _ensure_ma180(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "MA180" not in out.columns:
        if "Close" in out.columns:
            out["MA180"] = out["Close"].rolling(180, min_periods=30).mean()
        else:
            out["MA180"] = 0.0
    return out


def _ensure_bb40_upper(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "BB40_Upper" not in out.columns and "Close" in out.columns:
        ma40 = out["Close"].rolling(40, min_periods=20).mean()
        std40 = out["Close"].rolling(40, min_periods=20).std()
        out["BB40_Upper"] = ma40 + std40 * 2.0
    return out


def _avg_turnover_prev10(df: pd.DataFrame) -> float:
    """
    A 조건
    1봉전기준 10봉 평균 거래대금
    """
    if "Close" not in df.columns or "Volume" not in df.columns or len(df) < 11:
        return 0.0
    prev10 = df.iloc[-11:-1].copy()
    turnover = (prev10["Close"] * prev10["Volume"]).mean()
    return float(turnover)


def _cond_inverse_arrangement_recent10(df: pd.DataFrame) -> bool:
    """
    B 조건 해석
    10봉 이내 1~10회 발생, 종가기준 지수 1/180/224/448 이평이 역배열
    => 현재가(=MA1) < MA180 < MA224 < MA448 가 최근 10봉 내 1회 이상
    """
    if len(df) < 10:
        return False
    recent10 = df.tail(10).copy()
    cond = (
        (recent10["Close"] < recent10["MA180"]) &
        (recent10["MA180"] < recent10["MA224"]) &
        (recent10["MA224"] < recent10["MA448"])
    )
    return bool(cond.any())


def _cond_ma224_lt_ma448_persist50(df: pd.DataFrame) -> bool:
    """
    C 조건
    최근 50봉 동안 MA224 < MA448 지속
    """
    if len(df) < 50:
        return False
    recent50 = df.tail(50).copy()
    cond = (recent50["MA224"] < recent50["MA448"])
    return bool(cond.all())


def _cond_close_vs_ma224_now(row: pd.Series) -> bool:
    """
    D 조건
    MA224 대비 종가 등락률 -3% ~ +5%
    """
    close_p = safe_float(row.get("Close"))
    ma224 = safe_float(row.get("MA224"))
    if ma224 <= 0:
        return False
    pct = (close_p - ma224) / ma224 * 100.0
    return -3.0 <= pct <= 5.0


def _cond_close_lt_ma448_now(row: pd.Series) -> bool:
    """
    E 조건
    종가 < MA448
    """
    close_p = safe_float(row.get("Close"))
    ma448 = safe_float(row.get("MA448"))
    return ma448 > 0 and close_p < ma448


def _cond_ma5_ge_ma112_now(row: pd.Series) -> bool:
    """
    F 조건
    MA5 >= MA112
    """
    ma5 = safe_float(row.get("MA5"))
    ma112 = safe_float(row.get("MA112"))
    return ma5 > 0 and ma112 > 0 and ma5 >= ma112


def _cond_close_near_bb40_upper(row: pd.Series) -> bool:
    """
    G 조건
    BB40 상단선 대비 종가 -7% ~ +3% 이내 근접
    """
    close_p = safe_float(row.get("Close"))
    bb40u = safe_float(row.get("BB40_Upper"))
    if bb40u <= 0:
        return False
    pct = (close_p - bb40u) / bb40u * 100.0
    return -7.0 <= pct <= 3.0


def _cond_past_surge_recent50(df: pd.DataFrame) -> bool:
    """
    H 조건
    최근 50봉 내, 전일 종가 대비 당일 고가 상승률 12%~30%
    """
    if len(df) < 51:
        return False
    recent50 = df.tail(50).copy()
    prev_close = df["Close"].shift(1).loc[recent50.index]
    surge_pct = ((recent50["High"] / prev_close) - 1.0) * 100.0
    cond = surge_pct.between(12.0, 30.0)
    return bool(cond.any())


def _cond_past_volume_burst_recent50(df: pd.DataFrame) -> bool:
    """
    I 조건
    최근 50봉 내, 전일 거래량 대비 당일 거래량 변동률 300% 이상
    """
    if len(df) < 51:
        return False
    recent50 = df.tail(50).copy()
    prev_vol = df["Volume"].shift(1).loc[recent50.index].replace(0, pd.NA)
    vol_pct = (recent50["Volume"] / prev_vol) * 100.0
    cond = vol_pct.between(300.0, 999999.0)
    return bool(cond.any())


def _cond_past_ma448_high_break_recent50(df: pd.DataFrame) -> bool:
    """
    J 조건
    최근 50봉 내, 고가가 MA448 상향 돌파
    """
    if len(df) < 51:
        return False
    recent50 = df.tail(50).copy()
    cond = recent50["High"] > recent50["MA448"]
    return bool(cond.any())


def _cond_market_cap(fund: Dict[str, Any]) -> Optional[bool]:
    """
    K 조건
    시가총액 1,000억 이상
    """
    v = safe_float(fund.get("market_cap_krw", 0.0))
    if v <= 0:
        return None
    return v >= 100_000_000_000


def _cond_sales(fund: Dict[str, Any]) -> Optional[bool]:
    """
    L 조건
    연간 매출액 1,000억 이상
    """
    v = safe_float(fund.get("annual_sales_krw", 0.0))
    if v <= 0:
        return None
    return v >= 100_000_000_000


def _cond_net_income(fund: Dict[str, Any]) -> Optional[bool]:
    """
    M 조건
    연간 당기순이익 30억 이상
    """
    v = safe_float(fund.get("annual_net_income_krw", 0.0))
    if v == 0:
        return None
    return v >= 3_000_000_000


def _cond_interest_coverage(fund: Dict[str, Any]) -> Optional[bool]:
    """
    N 조건 해석
    이자보상배율 1.0배 이상
    """
    v = safe_float(fund.get("interest_coverage_ratio", 0.0))
    if v == 0:
        return None
    return v >= 1.0


def _cond_common_stock(fund: Dict[str, Any]) -> Optional[bool]:
    """
    P 조건
    보통주
    """
    v = safe_str(fund.get("stock_type", ""))
    if not v:
        return None
    return v in ("보통주", "COMMON", "Common", "common")


def _cond_not_special_issue(fund: Dict[str, Any]) -> Optional[bool]:
    """
    Q 조건
    ETF/ETN/기업인수목적회사(SPAC) 제외
    """
    name = safe_str(fund.get("name", ""))
    kind = safe_str(fund.get("security_kind", ""))
    if not name and not kind:
        return None

    text = f"{name} {kind}".upper()
    bad = ["ETF", "ETN", "스팩", "SPAC", "기업인수목적회사"]
    return not any(x.upper() in text for x in bad)


def evaluate_pre_dolbanji_hts_exact(
    df: pd.DataFrame,
    fund: Optional[Dict[str, Any]] = None,
    require_fundamentals: bool = False
) -> Dict[str, Any]:
    """
    HTS 정확복제형 평가
    - 기술조건 A~J는 강하게 반영
    - 재무/대상 조건 K/L/M/N/P/Q는 fund가 있을 때만 평가
    - require_fundamentals=True면 재무조건도 모두 통과해야 최종 passed
    """
    fund = fund or {}
    work = _ensure_ma180(df)
    work = _ensure_bb40_upper(work)

    required_cols = ["Close", "High", "Volume", "MA5", "MA112", "MA224", "MA448", "MA180", "BB40_Upper"]
    missing = [c for c in required_cols if c not in work.columns]
    if missing or len(work) < 60:
        return {
            "passed": False,
            "score": 0,
            "max_score": 16,
            "tags": [],
            "detail": {"error": f"필수 컬럼/봉수 부족: {missing}, len={len(work)}"}
        }

    row = work.iloc[-1]
    turnover_ma10_prev = _avg_turnover_prev10(work)

    conds: Dict[str, Any] = {
        "A_turnover_ma10_prev_between_3e8_and_max": 300_000_000 <= turnover_ma10_prev <= 999_999_999_999_999,
        "B_inverse_arrangement_recent10": _cond_inverse_arrangement_recent10(work),
        "C_ma224_lt_ma448_persist50": _cond_ma224_lt_ma448_persist50(work),
        "D_close_vs_ma224_-3_to_5": _cond_close_vs_ma224_now(row),
        "E_close_lt_ma448_now": _cond_close_lt_ma448_now(row),
        "F_ma5_ge_ma112_now": _cond_ma5_ge_ma112_now(row),
        "G_close_near_bb40_upper": _cond_close_near_bb40_upper(row),
        "H_past_surge_recent50": _cond_past_surge_recent50(work),
        "I_past_volume_burst_recent50": _cond_past_volume_burst_recent50(work),
        "J_past_high_break_ma448_recent50": _cond_past_ma448_high_break_recent50(work),

        "K_market_cap_ge_1000eok": _cond_market_cap(fund),
        "L_sales_ge_1000eok": _cond_sales(fund),
        "M_net_income_ge_30eok": _cond_net_income(fund),
        "N_interest_coverage_ge_1": _cond_interest_coverage(fund),
        "P_common_stock": _cond_common_stock(fund),
        "Q_not_etf_etn_spac": _cond_not_special_issue(fund),
    }

    # 기술조건 점수
    tech_keys = ["A_turnover_ma10_prev_between_3e8_and_max", "B_inverse_arrangement_recent10",
                 "C_ma224_lt_ma448_persist50", "D_close_vs_ma224_-3_to_5", "E_close_lt_ma448_now",
                 "F_ma5_ge_ma112_now", "G_close_near_bb40_upper", "H_past_surge_recent50",
                 "I_past_volume_burst_recent50", "J_past_high_break_ma448_recent50"]
    tech_score = sum(1 for k in tech_keys if bool(conds.get(k)))

    fund_keys = ["K_market_cap_ge_1000eok", "L_sales_ge_1000eok", "M_net_income_ge_30eok",
                 "N_interest_coverage_ge_1", "P_common_stock", "Q_not_etf_etn_spac"]
    known_fund = [k for k in fund_keys if conds.get(k) is not None]
    fund_score = sum(1 for k in known_fund if bool(conds.get(k)))

    tags = []
    if tech_score >= 10:
        tags.append("💍HTS정확복제형")
    elif tech_score >= 8:
        tags.append("💍HTS유사강형")
    elif tech_score >= 6:
        tags.append("💍HTS유사약형")

    if conds["C_ma224_lt_ma448_persist50"]:
        tags.append("🧱224<448_50봉지속")
    if conds["G_close_near_bb40_upper"]:
        tags.append("🟣BB40상단근접")
    if conds["H_past_surge_recent50"] and conds["I_past_volume_burst_recent50"]:
        tags.append("🚀과거폭발흔적")
    if conds["J_past_high_break_ma448_recent50"]:
        tags.append("📈448상향돌파이력")

    # passed 판단
    tech_pass = tech_score == len(tech_keys)
    if require_fundamentals:
        if known_fund:
            fund_pass = all(bool(conds[k]) for k in known_fund)
        else:
            fund_pass = False
        passed = tech_pass and fund_pass
    else:
        passed = tech_pass

    detail = {
        **conds,
        "turnover_ma10_prev": round(turnover_ma10_prev),
        "tech_score": tech_score,
        "tech_max": len(tech_keys),
        "fund_score": fund_score,
        "fund_known_count": len(known_fund),
        "require_fundamentals": require_fundamentals,
        "note": "기술조건 A~J는 정확복제형, 재무조건 K/L/M/N/P/Q는 데이터가 있을 때만 평가"
    }

    return {
        "passed": passed,
        "score": tech_score + fund_score,
        "max_score": len(tech_keys) + len(fund_keys),
        "tags": tags,
        "detail": detail,
    }


def build_pre_dolbanji_hts_exact_bundle(
    df: pd.DataFrame,
    fund: Optional[Dict[str, Any]] = None,
    require_fundamentals: bool = False
) -> Dict[str, Any]:
    res = evaluate_pre_dolbanji_hts_exact(df, fund=fund, require_fundamentals=require_fundamentals)
    return {
        "pre_dolbanji_hts_exact": bool(res["passed"]),
        "pre_dolbanji_hts_exact_score": int(res["score"]),
        "pre_dolbanji_hts_exact_tags": res["tags"],
        "pre_dolbanji_hts_exact_detail": res["detail"],
    }


if __name__ == "__main__":
    print("pre_dolbanji_hts_exact_clone.py loaded")
