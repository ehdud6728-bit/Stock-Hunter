from __future__ import annotations

from typing import Any, Iterable


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.replace(',', '').strip()
            if x == '':
                return default
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(round(safe_float(x, default)))
    except Exception:
        return default


def boolish(v: Any) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in {'1', 'true', 'y', 'yes', 't'}
    return bool(v)


def text_join(parts: Iterable[str], sep: str = ' / ') -> str:
    out = []
    seen = set()
    for p in parts:
        s = str(p or '').strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return sep.join(out)


def row_get(row, *keys, default=None):
    for key in keys:
        try:
            if isinstance(row, dict) and key in row:
                v = row.get(key)
                if v is not None:
                    return v
            elif hasattr(row, 'get'):
                v = row.get(key, None)
                if v is not None:
                    return v
        except Exception:
            pass
        try:
            v = row[key]
            if v is not None:
                return v
        except Exception:
            pass
    return default


def row_num(row, *keys, default=0.0) -> float:
    return safe_float(row_get(row, *keys, default=default), default)


def row_text(row, *keys, default='') -> str:
    v = row_get(row, *keys, default=default)
    try:
        return '' if v is None else str(v)
    except Exception:
        return default


def score_band_preempt(v: int) -> str:
    if v >= 65:
        return '선취 가능 우수'
    if v >= 50:
        return '선취 관찰 우수'
    if v >= 35:
        return '선취 참고'
    return '선취 낮음'


def score_band_breakout(v: int) -> str:
    if v >= 90:
        return '돌파 확인 우수'
    if v >= 70:
        return '돌파 대응 양호'
    if v >= 50:
        return '돌파 참고'
    return '돌파 낮음'


def score_band_safe(v: int) -> str:
    if v >= 450:
        return '매우 양호'
    if v >= 300:
        return '양호'
    if v >= 180:
        return '보통'
    return '주의'


def score_band_n(v: int) -> str:
    if v >= 700:
        return '매우 양호'
    if v >= 500:
        return '양호'
    if v >= 300:
        return '보통'
    return '낮음'
