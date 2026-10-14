#!/usr/bin/env python3
"""Read-only inventory of existing v49.76 GitHub Actions artifacts.

Does NOT claim historical replay, import arbitrary signals, or infer D+20 success.
"""
import argparse
import csv
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError

REPO = 'ehdud6728-bit/Stock-Hunter'
WORKFLOW = 'v49.76.yml'
HINTS = ('merge', 'research', 'selected', 'outcome', 'history', 'csv', 'result', 'backtest')


def get_json(url, token):
    req = Request(url, headers={'Authorization': 'Bearer '+token,
                                'Accept': 'application/vnd.github+json',
                                'X-GitHub-Api-Version': '2022-11-28',
                                'User-Agent': 'closebet-d20-research-audit'})
    try:
        with urlopen(req, timeout=25) as resp:
            return json.load(resp)
    except HTTPError as exc:
        raise RuntimeError('GITHUB_API_HTTP_%s %s' % (exc.code,url.split('?')[0])) from exc


def discover(token, max_runs=20, run_id=''):
    base='https://api.github.com/repos/'+REPO+'/actions'
    if run_id:
        runs=[get_json(base+'/runs/'+str(run_id),token)]
    else:
        data=get_json(base+'/workflows/'+WORKFLOW+'/runs?per_page='+str(max_runs)+'&status=success',token)
        runs=data.get('workflow_runs',[])
    rows=[]
    for run in runs:
        rid=run.get('id')
        if not rid: continue
        artifacts=get_json(base+'/runs/'+str(rid)+'/artifacts?per_page=100',token).get('artifacts',[])
        for a in artifacts:
            name=str(a.get('name',''))
            rows.append({'run_id':rid,'run_created_at':run.get('created_at',''),
                         'head_sha':run.get('head_sha',''),
                         'run_conclusion':run.get('conclusion',''),
                         'artifact_id':a.get('id',''),'artifact_name':name,
                         'artifact_size_bytes':a.get('size_in_bytes',0),
                         'artifact_expired':bool(a.get('expired')),
                         'artifact_created_at':a.get('created_at',''),
                         'candidate_by_name':any(x in name.lower() for x in HINTS),
                         'content_schema_verified':False,
                         'historical_search_replay_verified':False})
    return rows, runs


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--out',required=True)
    p.add_argument('--max-runs',type=int,default=20)
    p.add_argument('--run-id',default='')
    args=p.parse_args()
    if not 1<=args.max_runs<=50:raise SystemExit('MAX_RUNS_OUT_OF_RANGE_1_TO_50')
    token=os.getenv('GH_TOKEN','')
    if not token:raise SystemExit('MISSING_GH_TOKEN')
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    rows,runs=discover(token,args.max_runs,args.run_id)
    keys=['run_id','run_created_at','head_sha','run_conclusion','artifact_id',
          'artifact_name','artifact_size_bytes','artifact_expired','artifact_created_at',
          'candidate_by_name','content_schema_verified','historical_search_replay_verified']
    with (out/'historical_source_inventory.csv').open('w',newline='',encoding='utf-8-sig') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(rows)
    candidates=[x for x in rows if x['candidate_by_name'] and not x['artifact_expired']]
    status={'status':'INVENTORY_ONLY_NOT_REPLAY', 'source_workflow':WORKFLOW,
            'runs_checked':len(runs),'artifacts_found':len(rows),
            'potentially_relevant_artifacts':len(candidates),
            'next_step':'Inspect candidate artifact CSV schemas and obtain point-in-time signals plus historical daily OHLCV before computing D+20.',
            'no_fabricated_historical_signals':True,'research_only':True}
    (out/'historical_source_audit.json').write_text(json.dumps(status,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(status,ensure_ascii=False))

if __name__=='__main__':main()
