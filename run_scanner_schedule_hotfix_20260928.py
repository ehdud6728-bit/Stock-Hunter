#!/usr/bin/env python3
from pathlib import Path

RUN = Path(".github/workflows/run_scanner.yml")
GEN = Path("apply_real_full_reliable_overlay_patch.py")

if not RUN.exists():
    raise SystemExit(f"MISSING:{RUN}")
if not GEN.exists():
    raise SystemExit(f"MISSING:{GEN}")

# 1) Repair already-broken YAML/Python here-doc content in run_scanner.yml.
s = RUN.read_text(encoding="utf-8")
orig = s

broken_rfind = 'cut=text.rfind("' + "\n" + '",0,3500)'
fixed_rfind  = 'cut=text.rfind("\\n",0,3500)'
broken_lstrip = 'text=text[cut:].lstrip("' + "\n" + '")'
fixed_lstrip  = 'text=text[cut:].lstrip("\\n")'

n1=s.count(broken_rfind)
n2=s.count(broken_lstrip)
s=s.replace(broken_rfind, fixed_rfind)
s=s.replace(broken_lstrip, fixed_lstrip)

# Fail closed if the known broken shape remains.
if broken_rfind in s or broken_lstrip in s:
    raise SystemExit("RUN_SCANNER_MULTILINE_ESCAPE_STILL_BROKEN")

RUN.write_text(s, encoding="utf-8")

# 2) Repair the generator so a future "install patch" cannot recreate the bug.
g = GEN.read_text(encoding="utf-8")
gorig = g

# Source code lives inside a Python triple-quoted block.
# It must contain \\n so runtime output contains literal \n inside the YAML here-doc.
g = g.replace('cut=text.rfind("\\n",0,3500)',
              'cut=text.rfind("\\\\n",0,3500)')
g = g.replace('text=text[cut:].lstrip("\\n")',
              'text=text[cut:].lstrip("\\\\n")')

GEN.write_text(g, encoding="utf-8")

# Validation
q=RUN.read_text(encoding="utf-8")
gg=GEN.read_text(encoding="utf-8")

assert 'cut=text.rfind("\\n",0,3500)' in q, "fixed rfind missing from run_scanner"
assert 'text=text[cut:].lstrip("\\n")' in q, "fixed lstrip missing from run_scanner"
assert broken_rfind not in q
assert broken_lstrip not in q

assert 'cut=text.rfind("\\\\n",0,3500)' in gg, "generator rfind escaping not hardened"
assert 'text=text[cut:].lstrip("\\\\n")' in gg, "generator lstrip escaping not hardened"

# Preserve required schedules.
for cron in [
    "cron: '30 00 * * 1-5'",
    "cron: '20 06 * * 1-5'",
    "cron: '45 06 * * 1-5'",
]:
    assert cron in q, f"required schedule missing: {cron}"

assert "Build + send REAL_FULL Research Overlay immediately" in q
assert "REAL_FULL_RELIABLE_OVERLAY_PATCH_GUARDS PASS" in gg

print("RUN_SCANNER_SCHEDULE_HOTFIX PASS")
print("run_scanner replacements", n1, n2)
print("generator_changed", g != gorig)
print("production search/score/rank/order logic untouched")
