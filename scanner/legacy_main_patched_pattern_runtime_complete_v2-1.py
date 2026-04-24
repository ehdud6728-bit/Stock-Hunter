# -*- coding: utf-8 -*-
"""
legacy_main_patched_pattern_runtime_complete.py
============================================================

목적
----
기존 scanner/legacy_main_patched.py 를 직접 뜯어고치지 않고,
실행 시점에 패턴 오버홀 모듈을 주입해서

- 진짜쌍바닥 / 유사쌍바닥 분리
- 파란점선(저항선) 공통화
- 상승삼각형 / 박스상단 / 동적저항 / 재안착 돌파
- 주패턴 1개 + 보조태그 2~3개
- 매수 대기 / 돌파 확인 / 실행 가능 상태 분리

를 기존 결과 row / block / 출력 흐름에 통합하는
"실행형 통합 패치본" 입니다.

사용 방법
--------
1) 이 파일을 scanner 폴더에 둡니다.
2) pattern_overhaul_complete.py 도 같은 폴더에 둡니다.
3) 실행은 이 파일로 하거나,
   main7_bugfix_2.py 에서 legacy_main_patched.py 대신
   이 파일을 run_path 하도록 바꾸면 됩니다.

권장 교체:
    run_path(str(Path(__file__).resolve().parent / "scanner" / "legacy_main_patched_pattern_runtime_complete.py"), run_name="__main__")

핵심 방식
--------
- 원본 legacy_main_patched.py 를 importlib 로 로드
- analyze_final / 개별 블록 출력 함수 / 문자열 빌더를 감싸서
  row에 패턴 결과를 주입
- 원본 엔진이 살아있으면 그대로 사용하고,
  패턴 부분만 후처리로 덧입힘
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from typing import Any, Dict, Callable


BOOT_TAG = "PATTERN_RUNTIME_COMPLETE_V1"


def _log(msg: str) -> None:
    try:
        print(msg)
    except Exception:
        pass


BASE_DIR = Path(__file__).resolve().parent
LEGACY_PATH = BASE_DIR / "legacy_main_patched.py"
PATTERN_PATH = BASE_DIR / "pattern_overhaul_complete.py"

if not LEGACY_PATH.exists():
    raise FileNotFoundError(f"원본 파일이 없습니다: {LEGACY_PATH}")

# ✅ 주의:
# 이 파일은 legacy_main_patched.py 원본을 보존한 상태에서 별도 실행하는 런타임 패치 파일입니다.
# 만약 이 파일 내용을 legacy_main_patched.py에 덮어썼다면 자기 자신을 다시 로드할 수 있으므로,
# 원본 legacy_main_patched.py를 복구하고 main7_bugfix_2.py에서 이 파일을 실행하도록 바꾸세요.
if Path(__file__).resolve().name == "legacy_main_patched.py":
    raise RuntimeError(
        "이 런타임 패치 파일을 legacy_main_patched.py에 덮어쓴 상태입니다. "
        "원본 legacy_main_patched.py를 복구하고, "
        "main7_bugfix_2.py에서 legacy_main_patched_pattern_runtime_complete.py를 실행하세요."
    )

if not PATTERN_PATH.exists():
    raise FileNotFoundError(f"패턴 모듈이 없습니다: {PATTERN_PATH}")


# ---------------------------------------------------------
# 1) pattern module load
# ---------------------------------------------------------
_pattern_spec = importlib.util.spec_from_file_location("pattern_overhaul_complete_runtime", str(PATTERN_PATH))
_pattern_mod = importlib.util.module_from_spec(_pattern_spec)
assert _pattern_spec and _pattern_spec.loader

# ✅ dataclass 사용 모듈은 exec_module 전에 sys.modules 등록이 필요합니다.
# GitHub Actions / Python 3.10 환경에서 미등록 상태면
# AttributeError: 'NoneType' object has no attribute '__dict__' 오류가 발생할 수 있습니다.
sys.modules[_pattern_spec.name] = _pattern_mod

_pattern_spec.loader.exec_module(_pattern_mod)

enrich_row_with_pattern_overhaul = getattr(_pattern_mod, "enrich_row_with_pattern_overhaul")
build_pattern_summary_text = getattr(_pattern_mod, "build_pattern_summary_text")
build_pattern_one_line = getattr(_pattern_mod, "build_pattern_one_line")


# ---------------------------------------------------------
# 2) legacy module load
# ---------------------------------------------------------
_legacy_spec = importlib.util.spec_from_file_location("legacy_main_patched_runtime_base", str(LEGACY_PATH))
legacy = importlib.util.module_from_spec(_legacy_spec)
assert _legacy_spec and _legacy_spec.loader

# ✅ 원본 legacy 모듈도 안전하게 sys.modules에 먼저 등록합니다.
sys.modules[_legacy_spec.name] = legacy

_legacy_spec.loader.exec_module(legacy)


# ---------------------------------------------------------
# 3) 유틸
# ---------------------------------------------------------
def _safe_call(fn: Callable, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        return None


def _safe_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _append_pattern_to_row_and_block(row: Dict[str, Any], df, block: str = ""):
    try:
        row2 = enrich_row_with_pattern_overhaul(row, df)
    except Exception:
        row2 = dict(row)

    try:
        pattern_block = build_pattern_summary_text(row2)
    except Exception:
        pattern_block = ""

    block2 = block or ""
    if pattern_block:
        if block2 and not block2.endswith("\n"):
            block2 += "\n"
        block2 += pattern_block

    return row2, block2


def _guess_df_from_result(result: Any):
    if hasattr(result, "get"):
        for key in ["df", "price_df", "chart_df", "ohlcv_df", "raw_df"]:
            if key in result:
                return result.get(key)
    return None


def _guess_row_from_result(result: Any):
    if isinstance(result, dict):
        for key in ["row", "result_row", "hit_row", "payload"]:
            val = result.get(key)
            if isinstance(val, dict):
                return val, key
        if "종목명" in result or "ticker" in result or "N점수" in result:
            return result, None
    return None, None


def _guess_block_from_result(result: Any):
    if isinstance(result, dict):
        for key in ["block", "report_block", "message_block", "text_block"]:
            val = result.get(key)
            if isinstance(val, str):
                return val, key
    if isinstance(result, str):
        return result, None
    return "", None


# ---------------------------------------------------------
# 4) analyze_final 래핑
# ---------------------------------------------------------
if hasattr(legacy, "analyze_final"):
    _orig_analyze_final = legacy.analyze_final

    def analyze_final_patched(*args, **kwargs):
        result = _orig_analyze_final(*args, **kwargs)

        # 경우 1: dict 결과
        if isinstance(result, dict):
            row, row_key = _guess_row_from_result(result)
            df = _guess_df_from_result(result)
            block, block_key = _guess_block_from_result(result)

            if row and df is not None:
                row2, block2 = _append_pattern_to_row_and_block(row, df, block)

                if row_key:
                    result[row_key] = row2
                else:
                    result = row2

                if isinstance(result, dict):
                    # 주패턴 정보는 루트에도 복사
                    for k in [
                        "주패턴코드", "주패턴명", "패턴상태", "파란점선유형", "파란점선가격",
                        "트리거가격", "패턴최종점수", "패턴보조태그", "패턴리스크태그",
                        "쌍바닥분류", "돌반지재정의", "패턴액션"
                    ]:
                        if k in row2:
                            result[k] = row2[k]

                    if block_key:
                        result[block_key] = block2
                    elif block2:
                        result["pattern_block"] = block2

            return result

        return result

    legacy.analyze_final = analyze_final_patched
    _log("✅ analyze_final 패턴 통합 래핑 완료")
else:
    _log("⚠️ analyze_final 을 찾지 못했습니다. 부분 패치 모드로 진행합니다.")


# ---------------------------------------------------------
# 5) 개별 종목 문자열 빌더 래핑
# ---------------------------------------------------------
CANDIDATE_FUNC_NAMES = [
    "build_single_stock_block",
    "build_single_stock_report_block",
    "format_single_stock_block",
    "format_hit_block",
    "build_stock_block",
]

for _name in CANDIDATE_FUNC_NAMES:
    if hasattr(legacy, _name):
        _orig = getattr(legacy, _name)

        def _make_wrapper(orig_fn):
            def _wrapped(*args, **kwargs):
                out = orig_fn(*args, **kwargs)

                # row, df 조합을 인자에서 추정
                row = None
                df = None
                for a in args:
                    if isinstance(a, dict) and ("종목명" in a or "ticker" in a or "N점수" in a):
                        row = a
                    if hasattr(a, "columns") and hasattr(a, "__len__"):
                        if all(c in list(a.columns) for c in ["Open", "High", "Low", "Close"]):
                            df = a

                if row is not None and df is not None and isinstance(out, str):
                    row2, block2 = _append_pattern_to_row_and_block(row, df, out)
                    try:
                        row.update(row2)
                    except Exception:
                        pass
                    return block2
                return out
            return _wrapped

        setattr(legacy, _name, _make_wrapper(_orig))
        _log(f"✅ {_name} 래핑 완료")


# ---------------------------------------------------------
# 6) 후보 정렬 패치
#    패턴 과대중복 때문에 후순위 밀리는 문제를 완화
#    기존 정렬 이후 패턴최종점수를 약하게 보정만 함
# ---------------------------------------------------------
if hasattr(legacy, "build_and_sort_candidates"):
    _orig_sort = legacy.build_and_sort_candidates

    def build_and_sort_candidates_patched(*args, **kwargs):
        df = _orig_sort(*args, **kwargs)
        try:
            import pandas as pd

            if df is None or len(df) == 0 or not isinstance(df, pd.DataFrame):
                return df

            out = df.copy()

            # 패턴 점수 컬럼 없으면 낮은 가중치로 신규 계산 시도
            if "패턴최종점수" not in out.columns:
                scores = []
                for _, row in out.iterrows():
                    scores.append(int(row.get("패턴최종점수", 0) or 0))
                out["패턴최종점수"] = scores

            # 기존 안전점수/N점수 우선 유지 + 패턴점수는 약보정
            if "안전점수" in out.columns:
                out["_pattern_rank_bonus"] = (out["패턴최종점수"].fillna(0).clip(lower=0, upper=80) * 0.12)
                out = out.sort_values(
                    by=["안전점수", "_pattern_rank_bonus", "N점수"] if "N점수" in out.columns else ["안전점수", "_pattern_rank_bonus"],
                    ascending=False
                ).drop(columns=["_pattern_rank_bonus"], errors="ignore")
            return out
        except Exception:
            return df

    legacy.build_and_sort_candidates = build_and_sort_candidates_patched
    _log("✅ build_and_sort_candidates 약보정 패치 완료")


# ---------------------------------------------------------
# 7) 단일종목 분석 함수 후처리
# ---------------------------------------------------------
if hasattr(legacy, "analyze_single_stock_with_main7_engine"):
    _orig_single = legacy.analyze_single_stock_with_main7_engine

    def analyze_single_stock_with_main7_engine_patched(*args, **kwargs):
        result = _orig_single(*args, **kwargs)
        if isinstance(result, dict):
            row, row_key = _guess_row_from_result(result)
            df = _guess_df_from_result(result)
            if row and df is not None:
                row2, block2 = _append_pattern_to_row_and_block(row, df, result.get("block", ""))
                if row_key:
                    result[row_key] = row2
                result["pattern_block"] = block2
        return result

    legacy.analyze_single_stock_with_main7_engine = analyze_single_stock_with_main7_engine_patched
    _log("✅ analyze_single_stock_with_main7_engine 패치 완료")


# ---------------------------------------------------------
# 8) 엔트리포인트
# ---------------------------------------------------------
def main():
    _log(f"✅ {BOOT_TAG} LOADED")
    _log("✅ 패턴 통합: 진짜쌍바닥/유사쌍바닥/파란점선/상승삼각/동적저항/재안착")
    _log("✅ 상태 분리: 매수 대기 / 돌파 확인 / 실행 가능")
    _log("✅ 점수 체계: 주패턴 1개 + 약한 보조 보정")

    if hasattr(legacy, "main"):
        return legacy.main()

    # 원본이 스크립트형일 수 있으므로, __main__ 블록이 기대되면 runpy 방식 대신
    # 여기서는 알려진 main이 없을 때 최소한 모듈이 유지되도록 함
    _log("⚠️ legacy.main() 을 찾지 못했습니다. 원본 모듈이 스크립트형이면 기존 launcher에서 이 파일을 run_path 하세요.")
    return None


if __name__ == "__main__":
    main()
