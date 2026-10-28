"""Source-only, fail-closed R1-D27D GitHub Actions auto-continuation.

Does not read outcomes, alter search rules, place orders or push repository content.
Requires GITHUB_TOKEN with actions:write for workflow_dispatch only.
"""
from __future__ import annotations
import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

WF = 'stockhunter_r1d27c_auto_checkpoint_research_only.yml'
WF_PATH = '.github/workflows/' + WF
ART_PREFIX = 'stockhunter-R1D27D-FLOW60-AUTO-PART-'
FREEZE = '545f73cf0cdee0b1c3d2912edcc7a05fac1f9e11c72d19d10eba956ac50bbfba'
COUNT = 1500
MAX_DEPTH = 9  # At most 10 total runs for any one manual chain.


def check(value, reason):
    if not value:
        raise ValueError('R1D27D_AUTOCHAIN_' + reason)


class GitHubApi:
    def __init__(self, repo, token, opener=None):
        check(repo.count('/') == 1 and all(repo.split('/')), 'REPO')
        check(bool(token), 'TOKEN_REQUIRED')
        self.repo = repo
        self.token = token
        self.opener = opener or urllib.request.urlopen
        self.base = 'https://api.github.com/repos/' + repo

    def request(self, relative, method='GET', data=None):
        b = None if data is None else json.dumps(data, separators=(',', ':')).encode()
        req = urllib.request.Request(self.base + relative, data=b, method=method,
           headers={'Authorization':'Bearer '+self.token,
                    'Accept':'application/vnd.github+json',
                    'X-GitHub-Api-Version':'2022-11-28',
                    **({'Content-Type':'application/json'} if b else {})})
        with self.opener(req, timeout=60) as response:
            raw = response.read()
            if method == 'POST':
                check(response.status == 204, 'DISPATCH_NOT_ACCEPTED')
                return {}
            return json.loads(raw)

    def get(self, path): return self.request(path)
    def dispatch(self, branch, inputs):
        # Only our existing sealed research workflow; never an arbitrary workflow.
        return self.request('/actions/workflows/' + WF + '/dispatches', 'POST',
                            {'ref':branch, 'inputs':inputs})


def checkpoints(api, run_id):
    payload=api.get('/actions/runs/'+str(run_id)+'/artifacts?per_page=100')
    result=[]
    for x in payload.get('artifacts', []):
        name=x.get('name', '')
        if not name.startswith(ART_PREFIX) or x.get('expired'):
            continue
        try: part=int(name.removeprefix(ART_PREFIX))
        except ValueError: continue
        if not (1 <= part <= 8): continue
        digest=x.get('digest', '')
        if not (digest.startswith('sha256:') and len(digest)==71): continue
        if x.get('size_in_bytes', 0) <= 0: continue
        result.append((part, x))
    return result


def completed_candidates(api, branch, limit=100):
    # Explicit page cap; no invisible unbounded search/requests.
    query=urllib.parse.urlencode({'per_page':limit, 'branch':branch, 'status':'completed'})
    runs=api.get('/actions/workflows/'+WF+'/runs?'+query)
    for x in runs.get('workflow_runs', []):
        if x.get('repository', {}).get('full_name') != api.repo:continue
        if x.get('path') != WF_PATH:continue
        if x.get('head_branch') != branch:continue
        if x.get('conclusion') not in ('success','failure','timed_out','cancelled'):continue
        yield x


def select_seed(api, branch, value, part_text):
    """Return run_id, part. 'auto' finds the largest completed checkpoint.
    A deliberate '0' is accepted ONLY when no existing complete research artifact exists.
    """
    check(part_text.isdecimal() and 1 <= int(part_text) <= 8, 'PART')
    part=int(part_text)
    check(value == 'auto' or (value.isdecimal() and len(value)<=20), 'SEED_INPUT')
    if value not in ('auto','0'):
        r=api.get('/actions/runs/'+value)
        check(r.get('repository',{}).get('full_name') == api.repo and r.get('path')==WF_PATH
              and r.get('status')=='completed' and r.get('head_branch')==branch,'EXPLICIT_SEED_ORIGIN')
        # An explicitly supplied part must exist.
        ck=checkpoints(api, value)
        check(any(p==part for p,_ in ck), 'EXPLICIT_SEED_PART_MISSING')
        return value,part

    # Prefer the largest completed, sealed checkpoint by compressed artifact size.
    # This also prevents a later accidental fresh re-run from masking 480/720/... cached events.
    choices=[]
    for run in completed_candidates(api,branch):
        if run.get('id') is None: continue
        for p,x in checkpoints(api,run['id']):
            choices.append((x['size_in_bytes'],int(run['id']),p))
    if value == '0':
        check(not choices, 'FRESH_RESET_BLOCKED_EXISTING_CHECKPOINT')
        return '0', part
    if not choices:return '0',part
    _,rid,p=max(choices)
    return str(rid),p


def stage_ready(path):
    p=Path(path)
    check(p.is_file(), 'STAGE_MISSING')
    m=json.loads(p.read_text())
    check(m.get('n_frozen')==COUNT and m.get('freeze_sha256')==FREEZE, 'FREEZE_DRIFT')
    counts=[m.get('n_complete'),m.get('n_quarantined'),m.get('n_unattempted')]
    check(all(isinstance(x,int) and x>=0 for x in counts) and sum(counts)==COUNT,'COUNT_PARITY')
    check(m.get('research_only') is True and m.get('strategy_ready') is False
          and m.get('orders_placed')==0 and m.get('prospective_OOS') is False,'RESEARCH_LOCK')
    check(isinstance(m.get('provider_abort'), bool), 'PROVIDER_ABORT_FLAG')
    check(m.get('batch_attempted',0) > 0, 'NO_PROGRESS')
    return m


def dispatch_if_needed(api, run_id, branch, depth_text, batch_text, stage, attempt_text='1', abort_streak_text='0', pause_seconds=180, sleeper=None):
    m=stage_ready(stage)
    check(depth_text.isdecimal() and 0<=int(depth_text)<=MAX_DEPTH,'DEPTH')
    check(batch_text.isdecimal() and 1<=int(batch_text)<=30,'BATCH_LIMIT')
    check(str(run_id).isdecimal(), 'RUN_ID')
    check(str(attempt_text)=='1', 'RERUN_NOT_AUTO_DISPATCHED')
    check(abort_streak_text.isdecimal() and 0<=int(abort_streak_text)<=2,'ABORT_STREAK_INPUT')
    new_abort_streak=(int(abort_streak_text)+1) if m['provider_abort'] else 0
    if m['n_unattempted']==0:
        return {'status':'COMPLETE','complete':m['n_complete'],'quarantined':m['n_quarantined']}
    # Cannot silently end chain while data remains. Explicitly raise actionable error.
    check(int(depth_text)<MAX_DEPTH, 'CHAIN_DEPTH_EXHAUSTED_MANUAL_REVIEW')
    check(new_abort_streak<=2, 'THREE_CONSECUTIVE_PROVIDER_ABORTS_MANUAL_REVIEW')
    parts=checkpoints(api, run_id)
    check(parts, 'CURRENT_DURABLE_ARTIFACT_MISSING')
    part=max(p for p,_ in parts)
    # Only uploaded parts can be used as seeds, never unsaved runner files.
    inputs={
      'scope':'r1d27d_flow60_auto_1500',
      'seed_run_id':'40617919975', 'seed_part':'0', 'batch_limit':'30',
      'r1d27d_seed_run_id':str(run_id),
      'r1d27d_seed_part':str(part),
      'r1d27d_batch_limit':str(batch_text),
      'r1d27d_auto_chain':'true',
      'r1d27d_chain_depth':str(int(depth_text)+1),
      'r1d27d_abort_streak':str(new_abort_streak)
    }
    check(len(inputs)<=10, 'GITHUB_INPUTS_LIMIT')
    if m['provider_abort']:
        # A provider pause is not a signal success. Preserve quarantines and try a
        # fresh runner after cooling down; at most two consecutive auto-retries.
        print('R1D27D_PROVIDER_PAUSE_BEFORE_DISPATCH',pause_seconds,'seconds',flush=True)
        (sleeper or time.sleep)(pause_seconds)
    api.dispatch(branch, inputs)
    return {'status':'DISPATCHED','seed_run_id':str(run_id),'seed_part':part,
            'next_depth':int(depth_text)+1,'unattempted':m['n_unattempted'],
            'provider_abort':m['provider_abort'],'next_abort_streak':new_abort_streak}


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--action',choices=('select','dispatch'),required=True)
    ap.add_argument('--output');ap.add_argument('--stage',default='reports/r1d27d/R1D27D_STAGE_STATE.json')
    args=ap.parse_args()
    api=GitHubApi(os.environ['GITHUB_REPOSITORY'],os.environ['GH_TOKEN'])
    if args.action=='select':
        run,part=select_seed(api,os.environ['GITHUB_REF_NAME'],
           os.environ['R1D27D_SEED_RUN'].strip(),os.environ['R1D27D_SEED_PART'].strip())
        if args.output:
            with open(args.output,'a') as f:
                f.write('has_seed='+str(run!='0').lower()+'\n')
                if run!='0':f.write('seed_name='+ART_PREFIX+str(part).zfill(2)+'\nseed_run='+run+'\n')
        print('R1D27D_AUTO_SEED_SELECTED',json.dumps({'run':run,'part':part}))
    else:
        ans=dispatch_if_needed(api,os.environ['GITHUB_RUN_ID'],os.environ['GITHUB_REF_NAME'],
             os.environ['R1D27D_CHAIN_DEPTH'],os.environ['R1D27D_BATCH_LIMIT'],args.stage,
             os.environ.get('GITHUB_RUN_ATTEMPT','1'),
             os.environ.get('R1D27D_ABORT_STREAK','0'))
        print('R1D27D_AUTOCONTINUE',json.dumps(ans))

if __name__=='__main__':main()
