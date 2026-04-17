"""Drop-in scanner helpers for kki(pattern recurrence) and wave analysis.

이 패키지는 기존 legacy_main_patched.py 안으로 바로 복사/붙여넣기해서
연결할 수 있도록, 외부 의존성을 최소화한 보조 모듈 모음입니다.
"""

from .indicators import enrich_indicators, build_band_snapshot
from .watermelon_engine import normalize_meta, decide_action, build_easy_context
from .kki_wave import analyze_kki_profile, analyze_wave_profile, analyze_kki_and_wave
from .render_helpers import (
    build_kki_block,
    build_wave_block,
    build_kki_wave_bundle,
    build_easy_commentary_block,
)

__all__ = [
    "enrich_indicators",
    "build_band_snapshot",
    "normalize_meta",
    "decide_action",
    "build_easy_context",
    "analyze_kki_profile",
    "analyze_wave_profile",
    "analyze_kki_and_wave",
    "build_kki_block",
    "build_wave_block",
    "build_kki_wave_bundle",
    "build_easy_commentary_block",
]
