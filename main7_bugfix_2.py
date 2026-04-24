# -*- coding: utf-8 -*-
"""
Stock-Hunter direct launcher v2
============================================================
이 파일은 runtime patch 우회 없이 scanner/legacy_main_patched.py 를 직접 실행합니다.
GitHub Actions 환경에서도 stdout/stderr를 그대로 보여주면서 logs/ 폴더에 실행 로그를 남깁니다.
"""

from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from runpy import run_path


class _Tee:
    """stdout/stderr를 콘솔과 파일에 동시에 기록합니다."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            try:
                stream.write(data)
                stream.flush()
            except Exception:
                pass

    def flush(self):
        for stream in self.streams:
            try:
                stream.flush()
            except Exception:
                pass


def _setup_log(base_dir: Path):
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"stockhunter_direct_legacy_{ts}.log"
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)

    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = _Tee(old_stdout, log_file)
    sys.stderr = _Tee(old_stderr, log_file)

    os.environ["STOCKHUNTER_RUN_LOG"] = str(log_path)
    return log_path, log_file, old_stdout, old_stderr


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    target = base_dir / "scanner" / "legacy_main_patched.py"

    log_path, log_file, old_stdout, old_stderr = _setup_log(base_dir)

    try:
        print("✅ MAIN7_DIRECT_LEGACY_MODE LOADED")
        print("✅ runtime patch 우회: legacy_main_patched.py 직접 실행")
        print(f"✅ 실행 대상: {target}")
        print(f"✅ 실행 로그: {log_path}")

        if not target.exists():
            raise FileNotFoundError(f"실행할 파일이 없습니다: {target}")

        run_path(str(target), run_name="__main__")
        print("✅ MAIN7_DIRECT_LEGACY_MODE FINISHED")

    except SystemExit:
        raise
    except Exception:
        print("❌ MAIN7_DIRECT_LEGACY_MODE ERROR")
        traceback.print_exc()
        raise
    finally:
        try:
            log_file.flush()
            log_file.close()
        except Exception:
            pass
        sys.stdout = old_stdout
        sys.stderr = old_stderr


if __name__ == "__main__":
    main()
