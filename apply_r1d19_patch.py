#!/usr/bin/env python3
"""Apply the R1-D19 isolated Research Lab patch to an EXISTING Stock-Hunter repo.

Run at repository root. Does not create a new workflow, modify scanner/order
source code, or alter any historical artifacts. Reproduces visible root YML twin.
Fail-closed on unsupported workflow revisions. Idempotent for exactly one patch.
"""
from pathlib import Path
import shutil
import sys

HERE = Path(__file__).resolve().parent
WORKFLOW = Path('.github/workflows/stockhunter_r1d3_strong_winners_external_context.yml')
TWIN = Path('stockhunter_r1d3_strong_winners_external_context.yml')
TARGET = 'r1d19_protocol_lock:'
ADDITIONAL = 'r1d19_historical_reclaim:'


def main():
    if not WORKFLOW.is_file() or not TWIN.is_file():
        raise SystemExit('STOP: Run from Stock-Hunter repository root. Existing two Research Lab YML copies are required.')
    old = WORKFLOW.read_bytes()
    if old != TWIN.read_bytes():
        raise SystemExit('STOP: Existing workflow and root YML copies differ; no files changed.')
    workflow = old.decode('utf-8')
    if TARGET in workflow or ADDITIONAL in workflow:
        if TARGET not in workflow or ADDITIONAL not in workflow:
            raise SystemExit('STOP: Partial R1-D19 patch present, inspect manually.')
        print('R1-D19 workflow already contains jobs, keeping existing research workflow unchanged.')
        updated = workflow
    else:
        if workflow.count('          - r1d18_capture_new_signals\n') != 1:
            raise SystemExit('STOP: Scope anchor drift. No files changed.')
        if workflow.count("!startsWith(inputs.scope, 'r1d18_')") != 1:
            raise SystemExit('STOP: Source-check conditional anchor drift. No files changed.')
        if not workflow.startswith('name: StockHunter Research Lab R1-D3 to R1-D18 (RESEARCH ONLY)\n'):
            raise SystemExit('STOP: Original workflow title drift. No files changed.')
        workflow=workflow.replace('name: StockHunter Research Lab R1-D3 to R1-D18 (RESEARCH ONLY)',
                                  'name: StockHunter Research Lab R1-D3 to R1-D19 (RESEARCH ONLY)',1)
        workflow=workflow.replace('          - r1d18_capture_new_signals\n',
                                  '          - r1d18_capture_new_signals\n'
                                  '          - r1d19_protocol_lock\n'
                                  '          - r1d19_historical_reclaim\n',1)
        workflow=workflow.replace("!startsWith(inputs.scope, 'r1d18_')",
                                  "!startsWith(inputs.scope, 'r1d18_') && !startsWith(inputs.scope, 'r1d19_')",1)
        snippet=(HERE/'R1D19_WORKFLOW_APPEND.txt').read_text(encoding='utf-8')
        updated=workflow.rstrip('\n')+snippet+'\n'
        if updated.count('  '+TARGET)!=1 or updated.count('  '+ADDITIONAL)!=1:
            raise SystemExit('STOP: Bad workflow injection; no files changed.')
    files=[('R1D19_PREDECLARED_PROTOCOL.json','R1D19_PREDECLARED_PROTOCOL.json'),
           ('research_formulas/stockhunter_r1d19_bb_reclaim.py','research_formulas/stockhunter_r1d19_bb_reclaim.py'),
           ('tests/test_stockhunter_r1d19_bb_reclaim.py','tests/test_stockhunter_r1d19_bb_reclaim.py')]
    # Verify no unknown contents will be overwritten, with all checks before writes.
    for src,dst in files:
        source=HERE/src
        target=Path(dst)
        if not source.is_file():raise SystemExit('STOP: Payload missing '+src)
        if target.is_file() and target.read_bytes()!=source.read_bytes():
            raise SystemExit('STOP: Existing file differs, no files changed: '+dst)
    for src,dst in files:
        target=Path(dst);target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(HERE/src,target)
    WORKFLOW.write_text(updated,encoding='utf-8')
    TWIN.write_text(updated,encoding='utf-8')
    assert WORKFLOW.read_bytes()==TWIN.read_bytes()
    print('SUCCESS: R1-D19 isolated research installed. Existing workflow updated in-place + exact root YML twin.')
    print('Run python -m unittest discover -s tests -p test_stockhunter_r1d19_bb_reclaim.py -v')
    print('GitHub Actions > StockHunter Research Lab > scope=r1d19_protocol_lock (no other inputs).')


if __name__=='__main__':
    main()
