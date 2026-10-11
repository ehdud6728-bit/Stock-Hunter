#!/usr/bin/env python3
from pathlib import Path

p = Path(".github/workflows/run_scanner.yml")
if not p.exists():
    raise SystemExit("run_scanner.yml not found")

s = p.read_text(encoding="utf-8")
needle = """      - name: Refresh current CORE224 LIVE seed (lightweight no-hold, one date)
        if: env.STOCKHUNTER_CORE224_LIVE_ONLY == '1'
        timeout-minutes: 45
"""
replacement = """      - name: Refresh current CORE224 LIVE seed (lightweight no-hold, one date)
        if: env.STOCKHUNTER_CORE224_LIVE_ONLY == '1'
        timeout-minutes: 90
"""

if needle not in s:
    if "timeout-minutes: 90" in s and "Refresh current CORE224 LIVE seed (lightweight no-hold, one date)" in s:
        print("ALREADY_PATCHED")
        raise SystemExit(0)
    raise SystemExit("expected CORE224 timeout block not found; refuse broad replacement")

p.write_text(s.replace(needle, replacement, 1), encoding="utf-8")
print("PATCHED run_scanner.yml CORE224 seed timeout 45 -> 90")
