#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pathlib import Path
import shutil

RUN_SCANNER=Path(".github/workflows/run_scanner.yml")
R14=Path(".github/workflows/real_full_r1c1_oos_r14.yml")

def repl(s,old,new,label,minn=1):
    n=s.count(old)
    if n<minn: raise SystemExit(f"R141_PATCH_FAIL {label}: expected>={minn} found={n}")
    return s.replace(old,new),n

def patch_run():
    s=RUN_SCANNER.read_text(encoding="utf-8")
    old='python -u real_full_capture_runner.py --script "${SCRIPT}" 2>&1 | tee reports/real_full_main_run.log'
    new='python -u real_full_r1c1_capture_wrapper.py --script "${SCRIPT}" 2>&1 | tee reports/real_full_main_run.log'
    s,n=repl(s,old,new,"runner command")

    anchor='test -f real_full_source_postrun_guard.py || { echo "❌ REAL_FULL_POSTRUN_GUARD_MISSING"; exit 243; }'
    add=anchor+'\n          test -f real_full_r1c1_capture_wrapper.py || { echo "❌ REAL_FULL_R1C1_R141_WRAPPER_MISSING"; exit 247; }'
    if "REAL_FULL_R1C1_R141_WRAPPER_MISSING" not in s:
        s,_=repl(s,anchor,add,"preflight")

    cold="python -m py_compile real_full_trust_audit.py real_full_capture_runner.py real_full_source_postrun_guard.py real_full_decision_engine.py real_full_decision_board.py"
    cnew="python -m py_compile real_full_trust_audit.py real_full_capture_runner.py real_full_r1c1_capture_wrapper.py real_full_source_postrun_guard.py real_full_decision_engine.py real_full_decision_board.py\n          python real_full_r1c1_capture_wrapper.py --self-test"
    if "real_full_r1c1_capture_wrapper.py --self-test" not in s:
        s,_=repl(s,cold,cnew,"compile")
    RUN_SCANNER.write_text(s,encoding="utf-8")
    return n

def patch_r14():
    s=R14.read_text(encoding="utf-8")
    old="for base in real_full_trust_source.csv real_full_current_universe.csv v1080_stockhunter_signals.csv; do"
    new="for base in real_full_r1c1_v72_runtime_sidecar.csv real_full_trust_source.csv real_full_current_universe.csv v1080_stockhunter_signals.csv; do"
    if "real_full_r1c1_v72_runtime_sidecar.csv real_full_trust_source.csv" not in s:
        s,_=repl(s,old,new,"R14 candidate precedence")
    R14.write_text(s,encoding="utf-8")

def main():
    for p in (RUN_SCANNER,R14):
        if not p.exists(): raise SystemExit(f"R141_PATCH_FAIL missing {p}")
        bak=p.with_suffix(p.suffix+".pre_r141.bak")
        if not bak.exists(): shutil.copy2(p,bak)
    n=patch_run(); patch_r14()
    rs=RUN_SCANNER.read_text(encoding="utf-8")
    rr=R14.read_text(encoding="utf-8")
    assert 'real_full_r1c1_capture_wrapper.py --script "${SCRIPT}"' in rs
    assert "real_full_r1c1_capture_wrapper.py --self-test" in rs
    assert "real_full_r1c1_v72_runtime_sidecar.csv real_full_trust_source.csv" in rr
    print(f"R141_PATCH_PASS runner_replacements={n}")
    print("REAL_FULL search/score/rank/order logic changed=0")
    print("R1.4 candidate precedence=runtime sidecar first")

if __name__=="__main__": main()
