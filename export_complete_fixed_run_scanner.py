#!/usr/bin/env python3
from pathlib import Path

RUN = Path(".github/workflows/run_scanner.yml")
GEN = Path("apply_real_full_reliable_overlay_patch.py")

if not RUN.exists():
    raise SystemExit(f"MISSING:{RUN}")
if not GEN.exists():
    raise SystemExit(f"MISSING:{GEN}")

s = RUN.read_text(encoding="utf-8")

broken_rfind = 'cut=text.rfind("' + "\n" + '",0,3500)'
fixed_rfind  = 'cut=text.rfind("\\n",0,3500)'
broken_lstrip = 'text=text[cut:].lstrip("' + "\n" + '")'
fixed_lstrip  = 'text=text[cut:].lstrip("\\n")'

n1=s.count(broken_rfind)
n2=s.count(broken_lstrip)
s=s.replace(broken_rfind, fixed_rfind)
s=s.replace(broken_lstrip, fixed_lstrip)

if broken_rfind in s or broken_lstrip in s:
    raise SystemExit("RUN_SCANNER_MULTILINE_ESCAPE_STILL_BROKEN")

for cron in [
    "cron: '30 00 * * 1-5'",
    "cron: '20 06 * * 1-5'",
    "cron: '45 06 * * 1-5'",
]:
    if cron not in s:
        raise SystemExit(f"REQUIRED_SCHEDULE_MISSING:{cron}")

if "Build + send REAL_FULL Research Overlay immediately" not in s:
    raise SystemExit("REAL_FULL_OVERLAY_BLOCK_MISSING")

RUN.write_text(s, encoding="utf-8")

g = GEN.read_text(encoding="utf-8")

old_r = 'cut=text.rfind("\\n",0,3500)'
new_r = 'cut=text.rfind("\\\\n",0,3500)'
old_l = 'text=text[cut:].lstrip("\\n")'
new_l = 'text=text[cut:].lstrip("\\\\n")'

# Harden only if the generator is still in the unsafe state.
if new_r not in g:
    g = g.replace(old_r, new_r)
if new_l not in g:
    g = g.replace(old_l, new_l)

if new_r not in g or new_l not in g:
    raise SystemExit("GENERATOR_ESCAPE_HARDENING_FAILED")

GEN.write_text(g, encoding="utf-8")

# Final guards
q = RUN.read_text(encoding="utf-8")
gg = GEN.read_text(encoding="utf-8")
assert 'cut=text.rfind("\\n",0,3500)' in q
assert 'text=text[cut:].lstrip("\\n")' in q
assert new_r in gg
assert new_l in gg

print("COMPLETE_FIXED_FILES_READY")
print("run_scanner replacements", n1, n2)
print("run_scanner bytes", RUN.stat().st_size)
print("generator bytes", GEN.stat().st_size)
print("production search/score/rank/order unchanged")
