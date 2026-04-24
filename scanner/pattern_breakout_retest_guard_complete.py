# -*- coding: utf-8 -*-
"""
Stock-Hunter pattern breakout/retest guard complete module.

목적
- 유니드처럼 '상승삼각형돌파/파란저항돌파/돌반지완성'으로 잡혔지만
  이미 급등 후 윗꼬리와 되돌림이 나온 종목을 신규 돌파 후보로 과대평가하지 않도록 보정합니다.
- dataclass를 쓰지 않습니다. importlib spec 로딩 환경에서 발생했던
  sys.modules/dataclass 오류를 피하기 위한 런타임 안전형 모듈입니다.

적용 위치
1) scanner/pattern_breakout_retest_guard_complete.py 로 저장
2) 기존 패턴 산출 직후 아래처럼 호출

    from scanner.pattern_breakout_retest_guard_complete import apply_breakout_retest_guard, format_guard_lines

    pattern_signal = apply_breakout_retest_guard(df, pattern_signal)

3) 리포트 출력 시 pattern_signal 안의 guard_lines 또는 format_guard_lines(pattern_signal)를 출력

입력 df 컬럼 허용
- 영문: Open, High, Low, Close, Volume
- 소문자: open, high, low, close, volume
- 한글: 시가, 고가, 저가, 종가, 거래량

반환값
- dict 형태 pattern_signal
- 주요 키: main_pattern, state, score, action, entry_allowed, guard_tags, warnings, guard_lines
"""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore


BREAKOUT_LABEL_KEYWORDS = (
    "상승삼각형",
    "삼각수렴",
    "파란저항",
    "파란점선",
    "수평저항",
    "박스상단",
    "쌍바닥돌파",
    "진짜쌍바닥돌파",
    "돌반지",
)

DEFAULT_CONFIG: Dict[str, float] = {
    # 돌파선 대비 현재가 위치
    "wait_near_low_pct": -5.0,
    "wait_near_high_pct": 0.0,
    "breakout_confirm_min_pct": 0.0,
    "breakout_confirm_max_pct": 7.0,
    "support_retest_min_pct": 0.0,
    "support_retest_max_pct": 5.0,
    "failed_breakout_pct": -1.5,

    # 급등 후 되돌림 판정
    "high_extension_for_late_pct": 10.0,
    "pullback_from_high_for_retest_pct": -8.0,
    "pullback_from_high_for_warning_pct": -5.0,

    # 윗꼬리 판정
    "upper_wick_ratio_warn": 0.45,
    "upper_wick_close_drop_pct": 5.0,

    # 거래량 보조
    "volume_spike_ratio": 1.8,
    "volume_calm_ratio": 0.9,

    # 스코어 상한
    "score_cap_retest": 48.0,
    "score_cap_failed": 12.0,
    "score_cap_late_warning": 42.0,
    "score_cap_wait": 45.0,
    "score_cap_confirm": 88.0,
}


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.replace(",", "").replace("%", "").strip()
            if v == "":
                return default
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(round(_safe_float(v, float(default))))
    except Exception:
        return default


def _first_existing(cols: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    colset = {str(c).strip(): c for c in cols}
    lower_map = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        if cand in colset:
            return str(colset[cand])
        low = cand.lower()
        if low in lower_map:
            return str(lower_map[low])
    return None


def normalize_ohlcv(df: Any) -> Optional[Any]:
    """Return df with open/high/low/close/volume columns. None if impossible."""
    if pd is None or df is None:
        return None
    try:
        if len(df) < 30:
            return None
        out = df.copy()
        open_col = _first_existing(out.columns, ["Open", "open", "시가", "OPEN", "o"])
        high_col = _first_existing(out.columns, ["High", "high", "고가", "HIGH", "h"])
        low_col = _first_existing(out.columns, ["Low", "low", "저가", "LOW", "l"])
        close_col = _first_existing(out.columns, ["Close", "close", "종가", "현재가", "CLOSE", "c"])
        volume_col = _first_existing(out.columns, ["Volume", "volume", "거래량", "VOLUME", "vol", "v"])
        if not all([open_col, high_col, low_col, close_col]):
            return None
        rename = {
            open_col: "open",
            high_col: "high",
            low_col: "low",
            close_col: "close",
        }
        if volume_col:
            rename[volume_col] = "volume"
        out = out.rename(columns=rename)
        for c in ["open", "high", "low", "close"]:
            out[c] = pd.to_numeric(out[c], errors="coerce")
        if "volume" in out.columns:
            out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0)
        else:
            out["volume"] = 0
        out = out.dropna(subset=["open", "high", "low", "close"])
        if len(out) < 30:
            return None
        return out
    except Exception:
        return None


def _pct(a: float, b: float, default: float = 0.0) -> float:
    if b == 0:
        return default
    return (a / b - 1.0) * 100.0


def _rolling_dynamic_resistance(ndf: Any, lookback: int = 20) -> Tuple[float, Optional[int]]:
    """
    동적 저항선 추정.
    - 기본: 전일까지의 최근 20일 고가 최대값
    - 돌파일: 최근 15일 중 종가가 rolling resistance를 처음/최근 돌파한 위치
    """
    if pd is None or ndf is None or len(ndf) < 30:
        return 0.0, None
    close = ndf["close"]
    high = ndf["high"]
    res_series = high.shift(1).rolling(lookback, min_periods=max(5, lookback // 2)).max()
    current_res = _safe_float(res_series.iloc[-1], 0.0)

    breakout_idx: Optional[int] = None
    recent_start = max(0, len(ndf) - 15)
    for i in range(recent_start, len(ndf)):
        r = _safe_float(res_series.iloc[i], 0.0)
        c = _safe_float(close.iloc[i], 0.0)
        if r > 0 and c >= r * 1.005:
            breakout_idx = i
    return current_res, breakout_idx


def _recent_upper_wick_info(ndf: Any, window: int = 5) -> Dict[str, float]:
    tail = ndf.tail(window).copy()
    rng = (tail["high"] - tail["low"]).replace(0, float("nan"))
    upper = tail["high"] - tail[["open", "close"]].max(axis=1)
    ratio = (upper / rng).fillna(0.0)
    close_drop = (tail["high"] / tail["close"].replace(0, float("nan")) - 1.0).fillna(0.0) * 100.0
    return {
        "max_upper_wick_ratio_5": round(_safe_float(ratio.max(), 0.0), 3),
        "last_upper_wick_ratio": round(_safe_float(ratio.iloc[-1], 0.0), 3),
        "max_high_to_close_drop_pct_5": round(_safe_float(close_drop.max(), 0.0), 2),
        "last_high_to_close_drop_pct": round(_safe_float(close_drop.iloc[-1], 0.0), 2),
    }


def _volume_info(ndf: Any) -> Dict[str, float]:
    if "volume" not in ndf.columns or len(ndf) < 25:
        return {"vol_ratio20": 0.0, "vol_ratio5": 0.0}
    vol = ndf["volume"].fillna(0)
    last = _safe_float(vol.iloc[-1], 0.0)
    ma20 = _safe_float(vol.tail(21).head(20).mean(), 0.0)
    ma5 = _safe_float(vol.tail(6).head(5).mean(), 0.0)
    return {
        "vol_ratio20": round(last / ma20, 2) if ma20 > 0 else 0.0,
        "vol_ratio5": round(last / ma5, 2) if ma5 > 0 else 0.0,
    }


def _extract_signal_value(signal: Optional[Dict[str, Any]], keys: Iterable[str], default: Any = None) -> Any:
    if not isinstance(signal, dict):
        return default
    for k in keys:
        if k in signal and signal[k] not in (None, ""):
            return signal[k]
    meta = signal.get("meta")
    if isinstance(meta, dict):
        for k in keys:
            if k in meta and meta[k] not in (None, ""):
                return meta[k]
    return default


def _as_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if str(x).strip()]
    if isinstance(v, tuple):
        return [str(x) for x in v if str(x).strip()]
    if isinstance(v, str):
        if "|" in v:
            return [x.strip() for x in v.split("|") if x.strip()]
        if "," in v:
            return [x.strip() for x in v.split(",") if x.strip()]
        return [v.strip()] if v.strip() else []
    return [str(v)]


def _contains_any_label(signal: Optional[Dict[str, Any]], label: str, tags: List[str]) -> bool:
    hay = " ".join([
        str(label or ""),
        str(signal.get("main_pattern", "") if isinstance(signal, dict) else ""),
        str(signal.get("pattern", "") if isinstance(signal, dict) else ""),
        " ".join(tags),
    ])
    return any(k in hay for k in BREAKOUT_LABEL_KEYWORDS)


def _rename_pattern_for_guard(label: str, guard_state: str, is_dolbanji_done: bool = False) -> str:
    raw = str(label or "").strip()
    if not raw:
        raw = "🔵파란저항"

    if is_dolbanji_done and "돌반지" in raw:
        return "💍돌반지발사완료"

    if guard_state == "돌파 실패":
        if "상승삼각" in raw:
            return "⚠️상승삼각형돌파실패"
        if "삼각수렴" in raw:
            return "⚠️삼각수렴돌파실패"
        if "파란" in raw:
            return "⚠️파란저항돌파실패"
        if "쌍바닥" in raw:
            return "⚠️쌍바닥목선이탈"
        return "⚠️돌파실패"

    if guard_state in ("돌파 후 눌림확인", "돌파선 재이탈주의"):
        if "상승삼각" in raw:
            return "📈상승삼각형돌파후눌림"
        if "삼각수렴" in raw:
            return "📐삼각수렴돌파후눌림"
        if "파란" in raw:
            return "🔵파란저항돌파후눌림"
        if "쌍바닥" in raw:
            return "👣쌍바닥목선리테스트"
        if "돌반지" in raw:
            return "💍돌반지발사완료"
        return "🔵돌파후눌림확인"

    if guard_state == "매수 대기":
        if "상승삼각" in raw:
            return "📈상승삼각형대기"
        if "삼각수렴" in raw:
            return "📐삼각수렴대기"
        if "파란" in raw:
            return "🔵파란저항대기"
        return raw.replace("돌파", "대기") if "돌파" in raw else raw

    if guard_state == "추격주의":
        if "상승삼각" in raw:
            return "📈상승삼각형돌파추격주의"
        if "파란" in raw:
            return "🔵파란저항돌파추격주의"
        if "돌반지" in raw:
            return "💍돌반지발사주의"
        return raw + "추격주의" if "주의" not in raw else raw

    return raw


def detect_breakout_retest_guard(
    df: Any,
    signal: Optional[Dict[str, Any]] = None,
    *,
    label: str = "",
    base_score: Optional[float] = None,
    dynamic_resistance: Optional[float] = None,
    neckline: Optional[float] = None,
    breakout_line: Optional[float] = None,
    config: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    유니드형 과대평가 방지용 핵심 판정 함수.
    기존 패턴 점수와 별개로 현재 위치를 '신규 돌파/대기/리테스트/실패/추격주의'로 재분류합니다.
    """
    cfg = dict(DEFAULT_CONFIG)
    if config:
        cfg.update(config)

    ndf = normalize_ohlcv(df)
    if ndf is None:
        return {
            "guard_applied": False,
            "guard_state": "데이터부족",
            "guard_reason": "OHLCV 데이터 부족 또는 컬럼 인식 실패",
            "score_cap": None,
            "penalty": 0,
            "entry_allowed": False,
            "action": "관망",
            "guard_tags": ["⚙️패턴가드데이터부족"],
            "warnings": [],
            "metrics": {},
        }

    signal = signal if isinstance(signal, dict) else {}
    label = str(label or _extract_signal_value(signal, ["main_pattern", "pattern", "label"], "") or "")
    tags = _as_list(_extract_signal_value(signal, ["tags", "tag", "sub_patterns", "assist_patterns"], []))

    # 기존 신호가 돌파/저항 계열이 아니면 너무 강하게 개입하지 않습니다.
    is_breakout_family = _contains_any_label(signal, label, tags)

    close = _safe_float(ndf["close"].iloc[-1], 0.0)
    high = _safe_float(ndf["high"].iloc[-1], 0.0)
    low = _safe_float(ndf["low"].iloc[-1], 0.0)
    open_ = _safe_float(ndf["open"].iloc[-1], 0.0)

    inferred_res, inferred_breakout_idx = _rolling_dynamic_resistance(ndf, lookback=20)

    # 명시 저항선 우선순위: breakout_line > dynamic_resistance > neckline > signal 내부 > 추정치
    explicit_line = (
        breakout_line
        or dynamic_resistance
        or neckline
        or _safe_float(_extract_signal_value(signal, ["breakout_line", "dynamic_resistance", "blue_resistance", "neckline", "resistance"], 0.0), 0.0)
    )
    line = _safe_float(explicit_line, 0.0) or inferred_res
    if line <= 0:
        return {
            "guard_applied": False,
            "guard_state": "저항선부족",
            "guard_reason": "돌파선/저항선 계산 실패",
            "score_cap": None,
            "penalty": 0,
            "entry_allowed": False,
            "action": "관망",
            "guard_tags": ["⚙️저항선부족"],
            "warnings": [],
            "metrics": {},
        }

    # 최근 돌파일 기준 이후 고점. 돌파일이 없으면 최근 10일 고점.
    start_idx = inferred_breakout_idx if inferred_breakout_idx is not None else max(0, len(ndf) - 10)
    recent_after = ndf.iloc[start_idx:].copy()
    if len(recent_after) < 3:
        recent_after = ndf.tail(10).copy()
    recent_high = _safe_float(recent_after["high"].max(), high)

    line_gap_pct = _pct(close, line)
    high_extension_pct = _pct(recent_high, line)
    pullback_from_high_pct = _pct(close, recent_high)
    wick = _recent_upper_wick_info(ndf, window=5)
    vol = _volume_info(ndf)

    max_wick = wick["max_upper_wick_ratio_5"]
    max_high_close_drop = wick["max_high_to_close_drop_pct_5"]
    long_upper_wick = (
        max_wick >= cfg["upper_wick_ratio_warn"]
        or max_high_close_drop >= cfg["upper_wick_close_drop_pct"]
    )
    volume_spike = vol["vol_ratio20"] >= cfg["volume_spike_ratio"]

    guard_state = "관찰"
    action = "관망"
    entry_allowed = False
    score_cap: Optional[float] = None
    penalty = 0
    guard_tags: List[str] = []
    warnings: List[str] = []

    # 핵심 상태 분리
    if high_extension_pct >= cfg["high_extension_for_late_pct"] and line_gap_pct <= cfg["failed_breakout_pct"]:
        guard_state = "돌파 실패"
        action = "신규진입금지"
        entry_allowed = False
        score_cap = cfg["score_cap_failed"]
        penalty = -55
        guard_tags.extend(["⚠️돌파실패", "⛔신규진입금지"])
        warnings.append("돌파선 아래로 재이탈")
    elif (
        high_extension_pct >= cfg["high_extension_for_late_pct"]
        and pullback_from_high_pct <= cfg["pullback_from_high_for_retest_pct"]
        and cfg["support_retest_min_pct"] <= line_gap_pct <= cfg["support_retest_max_pct"]
    ):
        guard_state = "돌파 후 눌림확인"
        action = "리테스트대기"
        entry_allowed = False
        score_cap = cfg["score_cap_retest"]
        penalty = -32
        guard_tags.extend(["🛡️돌파후눌림", "🔁리테스트대기"])
        warnings.append("급등 후 되돌림, 돌파선 지지 확인 필요")
    elif (
        high_extension_pct >= cfg["high_extension_for_late_pct"]
        and pullback_from_high_pct <= cfg["pullback_from_high_for_retest_pct"]
        and -3.0 <= line_gap_pct < cfg["support_retest_min_pct"]
    ):
        guard_state = "돌파선 재이탈주의"
        action = "확인전진입금지"
        entry_allowed = False
        score_cap = cfg["score_cap_late_warning"]
        penalty = -38
        guard_tags.extend(["⚠️돌파선재이탈주의", "🔁리테스트실패위험"])
        warnings.append("돌파선 근처까지 되밀림, 종가 재안착 확인 필요")
    elif cfg["wait_near_low_pct"] <= line_gap_pct < cfg["wait_near_high_pct"]:
        guard_state = "매수 대기"
        action = "돌파확인대기"
        entry_allowed = False
        score_cap = cfg["score_cap_wait"]
        penalty = -8
        guard_tags.extend(["🔵파란저항대기"])
    elif cfg["breakout_confirm_min_pct"] <= line_gap_pct <= cfg["breakout_confirm_max_pct"]:
        if long_upper_wick and high_extension_pct >= cfg["high_extension_for_late_pct"]:
            guard_state = "윗꼬리 돌파주의"
            action = "종가안착확인"
            entry_allowed = False
            score_cap = cfg["score_cap_late_warning"]
            penalty = -28
            guard_tags.extend(["⚠️윗꼬리돌파주의"])
            warnings.append("최근 긴 윗꼬리 또는 고가 대비 밀림 발생")
        else:
            guard_state = "돌파 확인"
            action = "분할관찰"
            entry_allowed = True
            score_cap = cfg["score_cap_confirm"]
            penalty = 0
            guard_tags.extend(["✅돌파확인"])
    elif line_gap_pct > cfg["breakout_confirm_max_pct"]:
        guard_state = "추격주의"
        action = "눌림대기"
        entry_allowed = False
        score_cap = cfg["score_cap_late_warning"]
        penalty = -25
        guard_tags.extend(["⚠️추격주의"])
        warnings.append("돌파선 대비 이격 과대")
    else:
        guard_state = "관찰"
        action = "관망"
        entry_allowed = False
        penalty = -5 if is_breakout_family else 0

    # 긴 윗꼬리는 어떤 상태에도 별도 주의 태그로 붙입니다.
    if long_upper_wick and "⚠️윗꼬리돌파주의" not in guard_tags:
        guard_tags.append("⚠️윗꼬리확인")
        if "최근 긴 윗꼬리 또는 고가 대비 밀림 발생" not in warnings:
            warnings.append("최근 긴 윗꼬리 또는 고가 대비 밀림 발생")

    # 돌반지 발사 완료 보정
    is_dolbanji = "돌반지" in (label + " " + " ".join(tags))
    dolbanji_done = False
    if is_dolbanji and high_extension_pct >= cfg["high_extension_for_late_pct"] and pullback_from_high_pct <= cfg["pullback_from_high_for_warning_pct"]:
        dolbanji_done = True
        if "💍돌반지발사완료" not in guard_tags:
            guard_tags.append("💍돌반지발사완료")
        if score_cap is None or score_cap > cfg["score_cap_late_warning"]:
            score_cap = cfg["score_cap_late_warning"]
        penalty = min(penalty, -25)
        entry_allowed = False
        if action not in ("신규진입금지", "확인전진입금지"):
            action = "눌림대기"
        warnings.append("돌반지 완성 후 이미 발사된 구간")

    metrics = {
        "close": round(close, 3),
        "open": round(open_, 3),
        "high": round(high, 3),
        "low": round(low, 3),
        "breakout_line": round(line, 3),
        "line_gap_pct": round(line_gap_pct, 2),
        "recent_high": round(recent_high, 3),
        "high_extension_pct": round(high_extension_pct, 2),
        "pullback_from_high_pct": round(pullback_from_high_pct, 2),
        "vol_ratio20": vol["vol_ratio20"],
        "vol_ratio5": vol["vol_ratio5"],
        "volume_spike": bool(volume_spike),
        "long_upper_wick": bool(long_upper_wick),
        "max_upper_wick_ratio_5": wick["max_upper_wick_ratio_5"],
        "max_high_to_close_drop_pct_5": wick["max_high_to_close_drop_pct_5"],
        "inferred_breakout_idx": inferred_breakout_idx if inferred_breakout_idx is not None else -1,
        "is_breakout_family": bool(is_breakout_family),
        "dolbanji_done": bool(dolbanji_done),
    }

    detail = (
        f"돌파선 {line:,.0f}원 / 현재 이격 {line_gap_pct:+.1f}% / "
        f"최근고점 대비 {pullback_from_high_pct:+.1f}% / 고점확장 {high_extension_pct:+.1f}%"
    )

    return {
        "guard_applied": True,
        "guard_state": guard_state,
        "guard_reason": detail,
        "score_cap": score_cap,
        "penalty": penalty,
        "entry_allowed": entry_allowed,
        "action": action,
        "guard_tags": guard_tags,
        "warnings": list(dict.fromkeys(warnings)),
        "metrics": metrics,
        "dolbanji_done": dolbanji_done,
    }


def apply_breakout_retest_guard(
    df: Any,
    pattern_signal: Optional[Dict[str, Any]] = None,
    *,
    label: str = "",
    base_score: Optional[float] = None,
    dynamic_resistance: Optional[float] = None,
    neckline: Optional[float] = None,
    breakout_line: Optional[float] = None,
    config: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    기존 pattern_signal dict에 가드 결과를 병합합니다.
    pattern_signal이 None이어도 독립적으로 사용할 수 있습니다.
    """
    sig: Dict[str, Any] = dict(pattern_signal or {})
    raw_label = str(label or sig.get("main_pattern") or sig.get("pattern") or sig.get("label") or "")
    raw_score = base_score
    if raw_score is None:
        raw_score = _safe_float(sig.get("score", sig.get("pattern_score", 0)), 0.0)

    guard = detect_breakout_retest_guard(
        df,
        sig,
        label=raw_label,
        base_score=raw_score,
        dynamic_resistance=dynamic_resistance,
        neckline=neckline,
        breakout_line=breakout_line,
        config=config,
    )

    sig["breakout_retest_guard"] = guard
    sig["guard_state"] = guard.get("guard_state")
    sig["guard_action"] = guard.get("action")
    sig["entry_allowed"] = guard.get("entry_allowed", False)

    existing_tags = _as_list(sig.get("tags", []))
    merged_tags = existing_tags + list(guard.get("guard_tags", []))
    sig["tags"] = list(dict.fromkeys([t for t in merged_tags if t]))

    score_cap = guard.get("score_cap")
    penalty = _safe_float(guard.get("penalty", 0), 0.0)
    score = raw_score
    if score_cap is not None:
        score = min(score, _safe_float(score_cap, score))
    score = max(0.0, score + penalty)
    sig["score_before_guard"] = raw_score
    sig["score"] = int(round(score))
    sig["pattern_score"] = int(round(score))

    guard_state = str(guard.get("guard_state", ""))
    dolbanji_done = bool(guard.get("dolbanji_done", False))
    new_label = _rename_pattern_for_guard(raw_label, guard_state, dolbanji_done)
    if new_label:
        sig["main_pattern_before_guard"] = raw_label
        sig["main_pattern"] = new_label
        sig["pattern"] = new_label

    # 상태/판정 재정렬: 신규 추격 신호를 눌림/리테스트로 낮춤
    if guard_state in ("돌파 후 눌림확인", "돌파선 재이탈주의"):
        sig["state"] = "리테스트 대기"
        sig["type"] = "돌파후눌림확인"
        sig["final_judgement"] = "신규 추격 금지, 돌파선 지지 확인"
    elif guard_state == "돌파 실패":
        sig["state"] = "돌파 실패"
        sig["type"] = "제외"
        sig["final_judgement"] = "신규 진입 금지"
    elif guard_state == "윗꼬리 돌파주의":
        sig["state"] = "종가 안착 확인"
        sig["type"] = "주의관찰"
        sig["final_judgement"] = "윗꼬리 안정 확인 전 추격 금지"
    elif guard_state == "추격주의":
        sig["state"] = "추격주의"
        sig["type"] = "후행형"
        sig["final_judgement"] = "눌림 대기"
    elif guard_state == "매수 대기":
        sig["state"] = "매수 대기"
        sig["type"] = "저항대기형"
        sig["final_judgement"] = "저항선 돌파 확인 후 대응"
    elif guard_state == "돌파 확인":
        sig["state"] = sig.get("state") or "돌파 확인"
        sig["final_judgement"] = sig.get("final_judgement") or "분할 관찰 가능"

    sig["guard_lines"] = format_guard_lines(sig)
    return sig


def format_guard_lines(pattern_signal: Dict[str, Any]) -> List[str]:
    guard = pattern_signal.get("breakout_retest_guard", {}) if isinstance(pattern_signal, dict) else {}
    if not isinstance(guard, dict) or not guard.get("guard_applied"):
        return []
    state = str(guard.get("guard_state", ""))
    action = str(guard.get("action", ""))
    reason = str(guard.get("guard_reason", ""))
    metrics = guard.get("metrics", {}) if isinstance(guard.get("metrics"), dict) else {}
    warnings = guard.get("warnings", []) if isinstance(guard.get("warnings"), list) else []

    icon = "🛡️"
    if state == "돌파 확인":
        icon = "✅"
    elif state in ("돌파 실패", "돌파선 재이탈주의"):
        icon = "⚠️"
    elif state in ("돌파 후 눌림확인", "매수 대기"):
        icon = "🔁"

    lines = [
        f"{icon} 패턴가드:{state} | {action}",
        f"   └ {reason}",
    ]
    if warnings:
        lines.append("   └ 주의: " + " / ".join(list(dict.fromkeys([str(w) for w in warnings]))))
    if metrics:
        br = _safe_float(metrics.get("breakout_line"), 0.0)
        gap = _safe_float(metrics.get("line_gap_pct"), 0.0)
        pb = _safe_float(metrics.get("pullback_from_high_pct"), 0.0)
        wick = _safe_float(metrics.get("max_upper_wick_ratio_5"), 0.0)
        lines.append(f"   └ 기준선 {br:,.0f}원 / 기준선 이격 {gap:+.1f}% / 고점대비 {pb:+.1f}% / 윗꼬리 {wick:.2f}")
    return lines


def simple_unid_style_verdict(df: Any, breakout_line: Optional[float] = None) -> Dict[str, Any]:
    """
    빠른 단독 테스트용.
    유니드처럼 돌파 후 급등/되돌림이 나온 종목인지 확인할 때 사용합니다.
    """
    sig = {"main_pattern": "📈상승삼각형돌파", "score": 88, "tags": ["💍돌반지완성", "🔵파란저항돌파"]}
    return apply_breakout_retest_guard(df, sig, breakout_line=breakout_line)


__all__ = [
    "DEFAULT_CONFIG",
    "normalize_ohlcv",
    "detect_breakout_retest_guard",
    "apply_breakout_retest_guard",
    "format_guard_lines",
    "simple_unid_style_verdict",
]
