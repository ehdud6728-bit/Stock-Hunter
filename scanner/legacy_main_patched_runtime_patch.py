# -*- coding: utf-8 -*-
"""
Stock-Hunter runtime stabilization patch for legacy_main_patched.py

Purpose
- Prevent NameError/ImportError crashes from missing globals/utilities.
- Force TEST mode to skip AI/market-story/tournament style blocks.
- Provide render helpers for watermelon/breakout/dante state blocks.
- Add optional wave labels (소파동/중파동) helpers.
- Reduce repeated boilerplate in state summary blocks.

How to use
1) Put this file under scanner/ as scanner/legacy_main_patched_runtime_patch.py
2) Near the top of legacy_main_patched.py add:

    from legacy_main_patched_runtime_patch import apply_runtime_patch
    apply_runtime_patch(globals())

This mutates the caller globals() with safe fallbacks only when missing.
Existing project functions/variables win over patch defaults.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple


def apply_runtime_patch(ns: MutableMapping[str, Any]) -> None:
    """Inject safe helpers/stubs into the target module namespace."""

    def _setdefault(name: str, value: Any) -> Any:
        if name not in ns:
            ns[name] = value
        return ns[name]

    # -------------------------------------------------
    # Generic safe helpers
    # -------------------------------------------------
    def _g(name: str, default: Any = None) -> Any:
        return ns.get(name, default)

    def _b(value: Any) -> bool:
        try:
            return bool(value)
        except Exception:
            return False

    def _safe_int(v: Any, d: int = 0) -> int:
        try:
            return int(round(float(v)))
        except Exception:
            return d

    def _safe_float(v: Any, d: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return d

    def _safe_str(v: Any, d: str = "") -> str:
        try:
            if v is None:
                return d
            s = str(v).strip()
            return s if s else d
        except Exception:
            return d

    def _is_empty_df(df: Any) -> bool:
        try:
            return df is None or len(df) == 0
        except Exception:
            return True

    def _first_existing(row: Any, keys: Sequence[str], default: Any = "") -> Any:
        getter = getattr(row, "get", None)
        for key in keys:
            try:
                if getter is not None:
                    val = getter(key, None)
                else:
                    val = row[key]
                if val is not None and str(val).strip() != "":
                    return val
            except Exception:
                continue
        return default

    def _parse_chat_ids(raw: Any) -> List[str]:
        try:
            if raw is None:
                return []
            s = str(raw).strip()
            if not s:
                return []
            parts = [x.strip() for x in s.replace(";", ",").split(",")]
            return [x for x in parts if x]
        except Exception:
            return []

    _setdefault("_g", _g)
    _setdefault("_b", _b)
    _setdefault("_safe_int", _safe_int)
    _setdefault("_safe_float", _safe_float)
    _setdefault("_safe_str", _safe_str)
    _setdefault("_is_empty_df", _is_empty_df)
    _setdefault("_first_existing", _first_existing)
    _setdefault("_parse_chat_ids", _parse_chat_ids)

    # -------------------------------------------------
    # Missing globals fallback
    # -------------------------------------------------
    _setdefault("TEST_MODE", False)
    _setdefault("DEBUG", False)
    _setdefault("OPENAI_API_KEY", "")
    _setdefault("DART_ENABLED", False)
    _setdefault("TELEGRAM_TOKEN", "")
    _setdefault("TELEGRAM_CHAT_ID", "")
    _setdefault("TELEGRAM_REAL_CHAT_ID", "")
    _setdefault("TEST_CHAT_ID_OVERRIDE", "")
    _setdefault(
        "TELEGRAM_CHAT_ID_EFFECTIVE",
        ns.get("TELEGRAM_CHAT_ID") or ns.get("TEST_CHAT_ID_OVERRIDE", ""),
    )

    # -------------------------------------------------
    # Mode gating
    # -------------------------------------------------
    def is_test_mode() -> bool:
        return _b(_g("TEST_MODE", False))

    def allow_ai_blocks() -> bool:
        return not is_test_mode()

    def allow_tournament_blocks() -> bool:
        return not is_test_mode()

    def allow_market_story_blocks() -> bool:
        return not is_test_mode()

    _setdefault("is_test_mode", is_test_mode)
    _setdefault("allow_ai_blocks", allow_ai_blocks)
    _setdefault("allow_tournament_blocks", allow_tournament_blocks)
    _setdefault("allow_market_story_blocks", allow_market_story_blocks)

    # -------------------------------------------------
    # Logging helpers
    # -------------------------------------------------
    def log_info(msg: Any) -> None:
        try:
            print(msg)
        except Exception:
            pass

    def log_debug(msg: Any) -> None:
        try:
            if _b(_g("DEBUG", False)):
                print(msg)
        except Exception:
            pass

    def log_hit(name: Any, score: Any = None, tags: Any = None) -> None:
        try:
            s = f" | score={score}" if score is not None else ""
            t = f" | tags={tags}" if tags else ""
            log_debug(f"✅ HIT {name}{s}{t}")
        except Exception:
            pass

    def log_progress(done: Any, total: Any) -> None:
        try:
            di = _safe_int(done, 0)
            ti = _safe_int(total, 0)
            if ti > 0:
                print(f"📊 진행: {di}/{ti}")
        except Exception:
            pass

    _setdefault("log_info", log_info)
    _setdefault("log_debug", log_debug)
    _setdefault("log_hit", log_hit)
    _setdefault("log_progress", log_progress)

    # -------------------------------------------------
    # Optional dependency stubs
    # -------------------------------------------------
    if "analyze_kki_and_wave" not in ns:
        def analyze_kki_and_wave(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return {}
        ns["analyze_kki_and_wave"] = analyze_kki_and_wave

    if "collect_market_news" not in ns:
        def collect_market_news() -> Dict[str, Any]:
            return {}
        ns["collect_market_news"] = collect_market_news

    if "analyze_news_to_korea_theme" not in ns:
        def analyze_news_to_korea_theme(*args: Any, **kwargs: Any) -> str:
            return ""
        ns["analyze_news_to_korea_theme"] = analyze_news_to_korea_theme

    if "fetch_us_market_snapshot" not in ns:
        def fetch_us_market_snapshot(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return {}
        ns["fetch_us_market_snapshot"] = fetch_us_market_snapshot

    if "analyze_us_to_kor_with_gpt" not in ns:
        def analyze_us_to_kor_with_gpt(*args: Any, **kwargs: Any) -> str:
            return ""
        ns["analyze_us_to_kor_with_gpt"] = analyze_us_to_kor_with_gpt

    if "run_ai_tournament" not in ns:
        def run_ai_tournament(*args: Any, **kwargs: Any) -> str:
            return "TEST/대체 모드: 토너먼트 생략"
        ns["run_ai_tournament"] = run_ai_tournament

    # -------------------------------------------------
    # Text/render helpers
    # -------------------------------------------------
    def _text_block_header(icon: str, title: str) -> str:
        return "\n".join([
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"{icon} [{title}]",
            "",
        ]).rstrip()

    def build_page_header(page_no: int, total_pages: int, test_tag: bool = True) -> str:
        rows = []
        if test_tag:
            rows.append("[TEST]")
        rows.append(f"({page_no}/{total_pages})")
        rows.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return "\n".join(rows)

    _setdefault("_text_block_header", _text_block_header)
    _setdefault("build_page_header", build_page_header)

    # -------------------------------------------------
    # Wave helpers
    # -------------------------------------------------
    def get_wave_labels_from_row(row: Any) -> Tuple[str, str]:
        try:
            sw = _first_existing(row, ["소파동", "small_wave", "small_wave_label"], "")
            mw = _first_existing(row, ["중파동", "mid_wave", "mid_wave_label"], "")
            return _safe_str(sw), _safe_str(mw)
        except Exception:
            return "", ""

    def attach_wave_labels(row: Any, price_df: Any = None) -> Tuple[str, str]:
        sw, mw = get_wave_labels_from_row(row)
        if sw or mw:
            return sw, mw
        try:
            wave_info = ns["analyze_kki_and_wave"](price_df) or {}
            sw = _safe_str(wave_info.get("small_wave", wave_info.get("small_wave_label", "")))
            mw = _safe_str(wave_info.get("mid_wave", wave_info.get("mid_wave_label", "")))
        except Exception:
            sw, mw = "", ""
        return sw, mw

    def build_wave_line(row: Any, price_df: Any = None) -> str:
        sw, mw = attach_wave_labels(row, price_df)
        if not sw and not mw:
            return ""
        return f"🌊 파동: 소={sw or '-'} / 중={mw or '-'}"

    _setdefault("get_wave_labels_from_row", get_wave_labels_from_row)
    _setdefault("attach_wave_labels", attach_wave_labels)
    _setdefault("build_wave_line", build_wave_line)

    # -------------------------------------------------
    # Score hint line
    # -------------------------------------------------
    def build_score_hint(row: Any) -> str:
        safety = _safe_int(_first_existing(row, ["안전점수", "safety_score"], 0))
        nscore = _safe_int(_first_existing(row, ["N점수", "n_score"], 0))
        kki = _safe_int(_first_existing(row, ["끼점수", "kki_score"], 0))

        safety_txt = "손절선 비교적 명확" if safety >= 500 else "기본 안정" if safety >= 300 else "변동성 주의"
        n_txt = "구조·수급 조합 강함" if nscore >= 700 else "구조 보통" if nscore >= 500 else "확인 필요"
        k_txt = "단기 탄력 가능" if kki >= 20 else "탄력 보통" if kki >= 8 else "탄력 약함"
        return f"🧮 실전 해석: {safety_txt} / {n_txt} / {k_txt}"

    _setdefault("build_score_hint", build_score_hint)

    # -------------------------------------------------
    # Telegram target helper
    # -------------------------------------------------
    def get_target_chat_ids() -> Tuple[List[str], str]:
        real_ids = _parse_chat_ids(ns.get("TELEGRAM_REAL_CHAT_ID"))
        override_ids = _parse_chat_ids(ns.get("TELEGRAM_CHAT_ID_EFFECTIVE") or ns.get("TEST_CHAT_ID_OVERRIDE"))
        default_ids = _parse_chat_ids(ns.get("TELEGRAM_CHAT_ID"))

        if is_test_mode():
            if override_ids:
                return override_ids, "TEST_OVERRIDE"
            if default_ids:
                return default_ids, "TEST_DEFAULT"
            return [], "TEST_NONE"

        if real_ids:
            return real_ids, "REAL"
        if default_ids:
            return default_ids, "DEFAULT"
        return [], "NONE"

    _setdefault("get_target_chat_ids", get_target_chat_ids)

    # -------------------------------------------------
    # Compact state renderers
    # -------------------------------------------------
    def _summary_common_lines(row: Any) -> List[str]:
        state = _first_existing(row, ["상태", "wm_state", "state"], "")
        grade = _first_existing(row, ["등급", "grade"], "")
        cloud = _first_existing(row, ["저항구름", "cloud_text", "저항구름요약"], "")
        refine = _first_existing(row, ["정제", "filter_text", "정제요약"], "")
        verify = _first_existing(row, ["정제검증", "filter_check_text"], "")
        easy = _first_existing(row, ["쉬운 해설", "easy_comment", "쉬운해설"], "")
        need = _first_existing(row, ["확인 필요", "need_check_text", "확인기준"], "")
        action = _first_existing(row, ["최종 판정", "final_action", "최종판정"], "")
        caution = _first_existing(row, ["주의 포인트", "risk_comment", "주의포인트"], "")
        score_text = _first_existing(row, ["점수 해석", "score_text"], "")

        lines: List[str] = []
        if state or grade:
            lines.append(f"- 상태: {state}" + (f" | 등급:{grade}" if grade else ""))
        if cloud:
            lines.append(f"- 저항구름: {cloud}")
        if refine:
            lines.append(f"- 정제: {refine}")
        if verify:
            lines.append(f"- 정제검증: {verify}")
        if easy:
            lines.append(f"- 쉬운 해설: {easy}")
        if need:
            lines.append(f"- 확인 필요: {need}")
        if action:
            lines.append(f"- 최종 판정: {action}")
        if caution:
            lines.append(f"- 주의 포인트: {caution}")
        if score_text:
            lines.append(f"- 점수 해석: {score_text}")
        else:
            lines.append(f"- {build_score_hint(row)}")
        return lines

    def build_state_summary_item(idx: int, row: Any) -> str:
        name = _first_existing(row, ["종목명", "name", "Name"], "이름없음")
        code = _first_existing(row, ["code", "종목코드", "Code"], "")
        head = f"{idx}) {name}({code})" if code else f"{idx}) {name}"
        return "\n".join([head] + _summary_common_lines(row))

    def _iter_rows(df: Any) -> List[Any]:
        try:
            if df is None:
                return []
            if hasattr(df, "iterrows"):
                return [row for _, row in df.iterrows()]
            if isinstance(df, list):
                return df
            return list(df)
        except Exception:
            return []

    def build_simple_state_block(icon: str, title: str, df: Any) -> str:
        lines = [_text_block_header(icon, title)]
        rows = _iter_rows(df)
        if not rows:
            lines.extend(["- 해당 종목 없음", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"])
            return "\n".join(lines)
        for i, row in enumerate(rows[:5], 1):
            lines.append(build_state_summary_item(i, row))
            if i < min(len(rows), 5):
                lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return "\n".join(lines)

    def build_watermelon_state_block(title: str, df: Any) -> str:
        return build_simple_state_block("🍉", title, df)

    def build_dante_state_block(title: str, df: Any) -> str:
        return build_simple_state_block("🧭", title, df)

    def build_breakout_state_block(title: str, df: Any) -> str:
        return build_simple_state_block("🚀", title, df)

    _setdefault("build_state_summary_item", build_state_summary_item)
    _setdefault("build_simple_state_block", build_simple_state_block)
    _setdefault("build_watermelon_state_block", build_watermelon_state_block)
    _setdefault("build_dante_state_block", build_dante_state_block)
    _setdefault("build_breakout_state_block", build_breakout_state_block)

    # -------------------------------------------------
    # TEST-mode block cleaner
    # -------------------------------------------------
    def suppress_ai_story_blocks() -> None:
        if not allow_market_story_blocks():
            for name in [
                "market_briefing_block",
                "oil_briefing_block",
                "news_theme_block",
                "us_kor_block",
                "market_integration_block",
                "market_story_block",
                "market_ai_comment_block",
            ]:
                ns[name] = ""
        if not allow_tournament_blocks():
            ns["tournament_block"] = ""
            ns["ai_tournament_block"] = ""

    _setdefault("suppress_ai_story_blocks", suppress_ai_story_blocks)

    # Normalize once immediately
    suppress_ai_story_blocks()
