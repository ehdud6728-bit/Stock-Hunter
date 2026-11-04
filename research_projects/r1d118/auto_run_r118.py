#!/usr/bin/env python3
"""One-command real-data R118 auto preparer + canonical R117 replay + research.

Do not import LIVE/SEARCH/WATCH; do not write outside R118 results/cache.
"""
from __future__ import annotations
import argparse,json,subprocess,sys,time,traceback
from pathlib import Path
import shutil
from auto_data import load_pinned_sources,build_inputs
from auto_trendlines import generate_all
from research_r1d118 import main as research_main

HERE=Path(__file__).resolve().parent

def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument('--stocks',type=int,default=200,choices=[200,500],help='PIT market cap snapshot cohort size')
    parser.add_argument('--out',default=str(HERE/'results'))
    parser.add_argument('--cache',default=str(HERE/'.local_source_cache'))
    args=parser.parse_args(argv)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    # Heavy raw inputs / multi-million daily trendlines stay outside artifact results.
    work=HERE/'.auto_real_work'
    manifest_dir=work/'real_inputs'
    status_path=out/'R118_AUTO_EXECUTION_STAGE.json'
    status={'study':'R118_AUTO_REAL_HISTORICAL','stage':'PREPARING_ORIGINAL_SOURCES',
            'requested_stocks':args.stocks,'success':False}
    def save():status_path.write_text(json.dumps(status,ensure_ascii=False,indent=2),encoding='utf-8')
    save()
    try:
        years,indices,source_sha=load_pinned_sources(args.cache)
        inputs=build_inputs(years,indices,manifest_dir,count=args.stocks,sha_sources=source_sha)
        del years,indices
        status['stage']='GENERATING_R111_R112_R116_R117_LINES';status['cohort_asof']=inputs['audit']['selection_asof'];save()
        states,roles,trend=generate_all(inputs['manifest'],work/'auto_lines')
        shutil.copy2(manifest_dir/'R118_AUTO_SOURCE_AUDIT.json',out/'R118_AUTO_SOURCE_AUDIT.json')
        shutil.copy2(work/'auto_lines'/'R118_ORIGINAL_TRENDLINE_REPLAY_AUDIT.json',out/'R118_ORIGINAL_TRENDLINE_REPLAY_AUDIT.json')
        if trend['stock_count']!=args.stocks:raise RuntimeError('CANDIDATE_TRENDLINE_COVERAGE_FAIL')
        status['stage']='RUNNING_REAL_DATA_RESEARCH';save()
        research_main(['--manifest',str(inputs['manifest']),'--out',str(out),
                       '--line-states',str(states),'--line-mode','R117',
                       '--role-events',str(roles),'--market-daily',str(inputs['market']),
                       '--sector-pit',str(inputs['sector']),
                       '--study-start','2023-01-01','--target-stocks',str(args.stocks)])
        status['stage']='COMPLETED_REAL_HISTORICAL_RESEARCH';status['success']=True;save()
        print('R118_REALDATA_RUN_COMPLETE',args.stocks,'historical cohort; SEE READINESS AUDIT; NOT LIVE',flush=True)
        return 0
    except Exception as e:
        status['stage']='FAILED_'+status['stage'];status['error_type']=type(e).__name__;status['error']=str(e);save()
        print('::error::R118_AUTO_FAIL_CLOSED '+type(e).__name__+': '+str(e),file=sys.stderr,flush=True)
        raise

if __name__=='__main__':sys.exit(main())
