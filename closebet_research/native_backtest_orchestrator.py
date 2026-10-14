#!/usr/bin/env python3
"""Launch existing v49.76 read-only native backtest through Github Actions.

Requires GH_TOKEN with actions:write.  Never dispatches LIVE lane.
All downloads are bounded to the exact run this launcher identifies.
"""
from __future__ import annotations
import argparse, datetime as dt, json, os, pathlib, subprocess, sys, time


def gh(*args: str, json_result=False):
    cmd=['gh',*args]
    p=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    if p.returncode:
        raise RuntimeError('GH_COMMAND_FAILED: '+ ' '.join(cmd[:4]) + ' | '+p.stderr[-900:])
    return json.loads(p.stdout) if json_result else p.stdout


def runs(repo):
    return gh('api',f'repos/{repo}/actions/workflows/v49.76.yml/runs','-f','per_page=100','-f','event=workflow_dispatch','-X','GET',json_result=True).get('workflow_runs',[])


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--start',required=True)
    ap.add_argument('--end',required=True)
    ap.add_argument('--hold-days',type=int,default=20)
    ap.add_argument('--max-wait-minutes',type=int,default=120)
    ap.add_argument('--out',required=True)
    args=ap.parse_args()
    repo=os.getenv('GITHUB_REPOSITORY','')
    if not repo or '/' not in repo: raise SystemExit('GITHUB_REPOSITORY_MISSING')
    if not os.getenv('GH_TOKEN'):raise SystemExit('GH_TOKEN_MISSING')
    start=dt.date.fromisoformat(args.start); end=dt.date.fromisoformat(args.end)
    if start>end or end>dt.date.today(): raise SystemExit('INVALID_BACKTEST_DATE_RANGE')
    if args.hold_days!=20:raise SystemExit('HOLD_DAYS_MUST_BE_20_FOR_RESEARCH')
    out=pathlib.Path(args.out);out.mkdir(parents=True,exist_ok=True)
    manifest={'status':'INIT','parent_run_id':os.getenv('GITHUB_RUN_ID'),
              'repo':repo,'requested_start':args.start,'requested_end':args.end,
              'hold_days':args.hold_days,'production_modified':False,
              'mode':'NATIVE_V4976_BACKTEST','child_run_id':None}
    path=out/'native_source_manifest.json'
    def save():path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    save()
    ref=os.getenv('GITHUB_REF_NAME','main'); sha=os.getenv('GITHUB_SHA','')
    before={int(x['id']) for x in runs(repo)}
    print('BACKTEST_DISPATCH',repo,ref,args.start,args.end,flush=True)
    gh('workflow','run','v49.76.yml','--ref',ref,
       '-f','action_mode=backtest__성과검증','-f','strategy_only=none__전체/일반실행',
       '-f',f'backtest_start={args.start}','-f',f'backtest_end={args.end}',
       '-f','backtest_months=3','-f','hold_days=20','-f','backtest_min_eval_days=1',
       '-f','all_candidates=false','-f','send_telegram=false')
    deadline=time.monotonic()+args.max_wait_minutes*60
    child=None
    while time.monotonic()<deadline:
        fresh=[x for x in runs(repo) if int(x['id']) not in before and x.get('head_branch')==ref and (not sha or x.get('head_sha')==sha)]
        if len(fresh)>1:raise SystemExit('MULTIPLE_NEW_BACKTEST_RUNS_AMBIGUOUS: '+str([x['id'] for x in fresh]))
        if fresh:
            child=fresh[0];break
        time.sleep(20)
    if not child: raise SystemExit('CHILD_BACKTEST_RUN_NOT_FOUND')
    run_id=int(child['id']);manifest['child_run_id']=run_id;manifest['status']='DISPATCHED';save()
    print(f'CHILD_BACKTEST_URL=https://github.com/{repo}/actions/runs/{run_id}',flush=True)
    last=''; n=0
    while time.monotonic()<deadline:
        info=gh('api',f'repos/{repo}/actions/runs/{run_id}',json_result=True)
        status=str(info.get('status','')); conclusion=str(info.get('conclusion',''))
        if status+conclusion!=last or n%10==0:
            print('CHILD_STATUS',status,conclusion,flush=True);last=status+conclusion
        n+=1
        if status=='completed':
            manifest['child_conclusion']=conclusion
            if conclusion!='success':
                manifest['status']='CHILD_FAILED';save()
                raise SystemExit(f'NATIVE_BACKTEST_FAILED: child {run_id}={conclusion}')
            break
        time.sleep(30)
    else:
        manifest['status']='CHILD_TIMEOUT';save();raise SystemExit('NATIVE_BACKTEST_TIMEOUT')
    arts=gh('api',f'repos/{repo}/actions/runs/{run_id}/artifacts','-f','per_page=100','-X','GET',json_result=True).get('artifacts',[])
    choices=[a for a in arts if '-global-performance-' in a.get('name','') and not a.get('expired')]
    if len(choices)!=1:
        manifest['status']='GLOBAL_ARTIFACT_NOT_UNIQUE';manifest['artifacts']=[a.get('name') for a in arts];save()
        raise SystemExit(f'GLOBAL_PERFORMANCE_ARTIFACT_COUNT={len(choices)}')
    art=choices[0];manifest['source_artifact_id']=art['id'];manifest['source_artifact_name']=art['name'];
    target=out/'native';target.mkdir(exist_ok=True)
    gh('run','download',str(run_id),'--repo',repo,'--name',art['name'],'--dir',str(target))
    required='v49_76_global_selected_top5.csv'
    matches=list(target.rglob(required))
    if len(matches)!=1:
        manifest['status']='SELECTED_CSV_MISSING';save()
        raise SystemExit('NO_UNAMBIGUOUS_NATIVE_SELECTED_CSV')
    manifest['selected_csv_path']=str(matches[0]);manifest['status']='NATIVE_READY';save()
    print('NATIVE_SELECTED',matches[0],flush=True)

if __name__=='__main__':main()
