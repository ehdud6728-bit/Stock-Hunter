#!/usr/bin/env python3
"""R1-D27C research-only 30-event sequential auto checkpoint runner.

Each Actions step uploads a DISTINCT immutable artifact after completing this
command, so an interrupted later step does not erase earlier persisted parts.
No future return data, LIVE scan, rank, scoring, or trade paths are imported.
"""
from __future__ import annotations
import argparse
import json
import os
import pathlib
import shutil
from research_formulas import stockhunter_r1d27c_flow60 as r
from research_formulas import formula66_r1d6_oos_holdout as d6

MAX_BATCHES=8

def run_batch(index, seed_root, krx_root, result_root, batch_limit=30, *, fetch=None, sleep_events=0.9):
    r.check(1<=index<=MAX_BATCHES, 'AUTO_BATCH_INDEX')
    r.check(1<=batch_limit<=30, 'AUTO_BATCH_SIZE')
    result_root=pathlib.Path(result_root)
    cal,_=d6.index_asof(krx_root)
    r.check(len(cal)==1907 and cal[0]=='2019-01-02','AUTO_ORIGINAL_CALENDAR')
    if index==1:
        checkpoint=pathlib.Path(seed_root)
    else:
        state_file=result_root.parent/'R1D27C_AUTO_STAGE_STATE.json'
        r.check(state_file.is_file(),'AUTO_PREVIOUS_BATCH_MISSING')
        state=json.loads(state_file.read_text(encoding='utf-8'))
        r.check(state['last_batch']==index-1 and state['version']==r.VERSION,'AUTO_OUT_OF_SEQUENCE')
        # Verify both frozen and flow manifests *before* creating the next checkpoint.
        checkpoint=result_root.parent/'auto_input_checkpoint'
        target=checkpoint/'reports/r1d27c/result'
        for section in ('freeze','flow'):
            src=result_root/section
            r.check(src.is_dir(),'AUTO_LAST_OUTPUT_MISSING_'+section)
            dest=target/section
            if dest.exists():shutil.rmtree(dest)
            dest.parent.mkdir(parents=True,exist_ok=True)
            shutil.copytree(src,dest)
    # Import prior successful rows, validate every actor, and collect *only*
    # unverified signals. Prior calendar mismatch is left quarantined.
    _,_,_,_,prev=r.audit_checkpoint(checkpoint,cal)
    m=r.collect_resume(checkpoint,krx_root,result_root/'flow',{},
                       batch_limit=batch_limit,fetch=fetch,sleep_events=sleep_events,
                       skip_known_calendar_mismatch=True)
    # Audit output using expected nested path under a temporary container root.
    output_checkpoint=result_root.parent/'auto_output_checkpoint'
    output_target=output_checkpoint/'reports/r1d27c/result'
    for section in ('freeze','flow'):
        dest=output_target/section
        if dest.exists():shutil.rmtree(dest)
        dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copytree(result_root/section,dest)
    _,_,_,_,checked=r.audit_checkpoint(output_checkpoint,cal)
    r.check(checked['n_source_complete']>=prev['n_source_complete'],'AUTO_SOURCE_REGRESSION')
    r.check(checked['n_source_complete']==m['n_source_complete'],'AUTO_MANIFEST_MISMATCH')
    r.check(m['n_cached_reused']==prev['n_source_complete'],'AUTO_CACHED_COUNT')
    r.check(m['batch_attempted']<=batch_limit,'AUTO_QUERY_BUDGET')
    r.check(not m['live_changed'] and m['orders_placed']==0 and m['research_only'],'AUTO_RESEARCH_ONLY')
    keep_going=not m['consecutive_failure_abort'] and m['n_unattempted']>0 and index<MAX_BATCHES
    state=dict(version=r.VERSION,last_batch=index,n_frozen=300,n_source_complete=m['n_source_complete'],
               batch_attempted=m['batch_attempted'],batch_new_success=m['batch_new_success'],
               n_unattempted=m['n_unattempted'],n_incomplete=m['n_source_incomplete'],
               freeze_sha256=r.PINNED_FULL_FREEZE_SHA256,
               flow_sha256=m['files_sha256'][r.FLOW],
               last_source_digest=m['checkpoint_source_digest'],
               continued=keep_going,provider_abort=m['consecutive_failure_abort'],
               research_only=True,strategy_ready=False,orders_placed=0,
               result_type='SOURCE_CHECKPOINT_NOT_FORWARD_RETURN_RESEARCH')
    path=result_root.parent/'R1D27C_AUTO_STAGE_STATE.json'
    tmp=path.with_suffix('.json.tmp')
    r.dump(tmp,state);tmp.replace(path)
    print('R1D27C_AUTO_STAGE',json.dumps(state,ensure_ascii=False),flush=True)
    output=os.environ.get('GITHUB_OUTPUT')
    if output:
        with open(output,'a',encoding='utf8') as f:
            f.write('go='+str(keep_going).lower()+'\n')
            f.write('provider_abort='+str(m['consecutive_failure_abort']).lower()+'\n')
            f.write('complete='+str(m['n_source_complete'])+'\n')
    return state

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--batch-index',type=int,required=True)
    p.add_argument('--seed-root',default='reports/r1d27c/checkpoint')
    p.add_argument('--krx-root',default='reports/r1d27c/input/krx')
    p.add_argument('--result-root',default='reports/r1d27c/result')
    p.add_argument('--batch-limit',type=int,default=30)
    a=p.parse_args()
    run_batch(a.batch_index,a.seed_root,a.krx_root,a.result_root,a.batch_limit)
if __name__=='__main__':main()
