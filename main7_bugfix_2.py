# -*- coding: utf-8 -*-
from runpy import run_path
from pathlib import Path

if __name__ == "__main__":
    print("✅ MAIN7_PATTERN_VISIBILITY_SCOUT_V3 LOADED")
    print("✅ 미완성 후보/하단밴드/파란저항/매집봉후눌림 표시 강화")
    target = Path(__file__).resolve().parent / "scanner" / "legacy_main_patched.py"
    print(f"✅ 실행 대상: {target}")
    run_path(str(target), run_name="__main__")
