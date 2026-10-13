#!/usr/bin/env python3
"""Fix the R3 env command scope mistake in run_scanner.yml, fail closed."""
from pathlib import Path
import argparse, hashlib

OLD = '''            V23_MATERIALIZED_DIR=reports/v25_live_seed_build/v23_materialized \\
            python -u core224_live_date_selector_r3.py --patch-main "$SCRIPT"
            python -u "$SCRIPT"'''
NEW = '''            V23_MATERIALIZED_DIR=reports/v25_live_seed_build/v23_materialized \\
            python -u "$SCRIPT"'''
# The patch-main invocation MUST be outside the env command, before env \\
PRE = '''          env \\
            TEST_PROFILE=WEEKLY_BACKTEST \\
'''
PRE_NEW = '''          echo "CORE224_R3_PATCH_MAIN_PREPARE target=$current_live_end"
          python -u core224_live_date_selector_r3.py --patch-main "$SCRIPT"
          echo "CORE224_R3_PATCH_MAIN_READY target=$current_live_end"
          env \\
            TEST_PROFILE=WEEKLY_BACKTEST \\
'''

def patch(src, dst):
    source=Path(src).read_text(encoding='utf-8')
    if source.count(OLD)!=1:
        raise ValueError('R3_ENV_SCOPE_OLD_ANCHOR_NOT_UNIQUE: '+str(source.count(OLD)))
    if source.count(PRE)!=1:
        raise ValueError('R3_ENV_SCOPE_PRE_ANCHOR_NOT_UNIQUE: '+str(source.count(PRE)))
    fixed=source.replace(OLD,NEW,1).replace(PRE,PRE_NEW,1)
    assert fixed.count('python -u core224_live_date_selector_r3.py --patch-main "$SCRIPT"')==1
    assert fixed.count('V23_MATERIALIZED_DIR=reports/v25_live_seed_build/v23_materialized \\\n            python -u "$SCRIPT"')==1
    assert fixed.count('CORE224_R3_PATCH_MAIN_READY')==1
    dest=Path(dst);dest.parent.mkdir(parents=True,exist_ok=True);dest.write_text(fixed,encoding='utf-8')
    print('CORE224_R3_ENV_SCOPE_FIX_PASS', 'source_sha256',hashlib.sha256(source.encode()).hexdigest(), 'output_sha256',hashlib.sha256(fixed.encode()).hexdigest(), 'output',dst)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--src',default='.github/workflows/run_scanner.yml');p.add_argument('--dst',default='reports/core224_r3_fix/run_scanner.yml');a=p.parse_args();patch(a.src,a.dst)
