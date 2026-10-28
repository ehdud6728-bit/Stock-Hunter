#!/usr/bin/env python3
"""Opt-in install: integrate R1D27C AUTO into the EXISTING single Research Hub YAML.

Usage: python install_into_existing_hub.py /path/to/Stock-Hunter
This script edits only research_formulas/stockhunter_r1d27c_flow60.py,
adds the auto runner/tests, and appends a job/scope to the existing Research Hub.
It NEVER modifies the LIVE scanner, scoring, ranking, or order code.
"""
from __future__ import annotations
import pathlib
import shutil
import sys

ROOT=pathlib.Path(__file__).resolve().parent
HUB='stockhunter_r1d19_bb203040_reclaim_research_only_FIXED_20261025.yml'
AUTO='stockhunter_r1d27c_auto_checkpoint_research_only.yml'
SCOPE='r1d27c_auto_to_300'


def install(repo: pathlib.Path):
    repo=repo.resolve()
    assert repo.is_dir() and (repo/'.github/workflows'/HUB).is_file(), 'Existing Research Hub YAML not found'
    dest=repo/'.github/workflows'/HUB
    txt=dest.read_text(encoding='utf-8')
    if SCOPE in txt:
        raise RuntimeError('AUTO scope already installed; refusing duplicate insertion')
    anchor='          - r1d27c_flow60_resume_checkpoint\n'
    assert txt.count(anchor)==1, 'Scope list drift: manual review required'
    txt=txt.replace(anchor,anchor+'          - '+SCOPE+'\n',1)
    before='\npermissions:\n'
    inputs='''      seed_run_id:
        description: "AUTO checkpoint run ID; initial 40617919975, or last AUTO run"
        type: string
        required: false
        default: '40617919975'
      seed_part:
        description: "AUTO: 0 for previous RESUME or 1-8 for completed AUTO part"
        type: choice
        required: false
        default: '0'
        options: ['0', '1', '2', '3', '4', '5', '6', '7', '8']
      batch_limit:
        description: "AUTO: 1-30 new events per durable segment"
        type: string
        required: false
        default: '30'
'''
    assert txt.count(before)==1, 'Workflow permissions anchor changed'
    txt=txt.replace(before,'\n'+inputs+before.lstrip('\n'),1)
    standalone=(ROOT/'.github/workflows'/AUTO).read_text(encoding='utf-8')
    assert standalone.count('\njobs:\n')==1
    job=standalone.split('\njobs:\n',1)[1]
    first='    name: "R1-D27C AUTO verified 87 -> up to 300 (8 safe checkpoints)"\n'
    assert first in job
    job=job.replace(first,first+"    if: ${{ inputs.scope == 'r1d27c_auto_to_300' }}\n",1)
    assert '  r1d27c_auto_checkpoint:' in job
    txt+='\n  # R1-D27C integrated AUTO checkpoint source only; no future outcomes.\n'+job
    # Ensure source matches the specific, previously audited recovery version.
    src=repo/'research_formulas/stockhunter_r1d27c_flow60.py'
    code=src.read_text(encoding='utf-8')
    old='def collect_resume(checkpoint_root,krx_root,out,protocol,batch_limit=30,fetch=None,sleep_events=0.9,max_fails=3):'
    assert code.count(old)==1 and 'skip_known_calendar_mismatch' not in code, 'Recovery source drift detected; manual review required'
    newcode=(ROOT/'research_formulas/stockhunter_r1d27c_flow60.py').read_text(encoding='utf-8')
    # Strict expected baseline SHA: the ZIP includes the exact fetched original
    # and the patch was developed against that specific baseline.
    import hashlib
    original=(ROOT/'research_formulas/stockhunter_r1d27c_flow60.py.original.sha256').read_text().strip()
    assert hashlib.sha256(src.read_bytes()).hexdigest()==original, 'Base source SHA changed; refusing overwrite'
    # Prepare all edits before writing; backup only the 2 existing research files.
    backup=repo/'.r1d27c_auto_backup'
    backup.mkdir(exist_ok=True)
    shutil.copy2(dest,backup/HUB)
    shutil.copy2(src,backup/src.name)
    src.write_text(newcode,encoding='utf-8')
    dest.write_text(txt,encoding='utf-8')
    (repo/HUB).write_text(txt,encoding='utf-8')
    for relative in ['research_formulas/stockhunter_r1d27c_auto.py','tests/test_stockhunter_r1d27c_auto.py']:
        path=repo/relative
        if path.exists():raise RuntimeError('Refusing to overwrite existing new AUTO file: '+relative)
        path.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(ROOT/relative,path)
    # Remove only the unused standalone workflow if it was copied from ZIP.
    separate=repo/'.github/workflows'/AUTO
    if separate.is_file() and separate.read_bytes()==(ROOT/'.github/workflows'/AUTO).read_bytes():
        separate.unlink()
    standalone_copy=repo/AUTO
    if standalone_copy.is_file() and standalone_copy.read_bytes()==(ROOT/AUTO).read_bytes():
        standalone_copy.unlink()
    print('R1D27C_SINGLE_HUB_INTEGRATED_RESEARCH_ONLY',dest)
    print('ROOT_YAML_COPY',repo/HUB)
    print('Existing files backed up under',backup)

if __name__=='__main__':
    if len(sys.argv)!=2:
        print('Usage: python install_into_existing_hub.py /path/to/Stock-Hunter',file=sys.stderr)
        sys.exit(2)
    install(pathlib.Path(sys.argv[1]))
