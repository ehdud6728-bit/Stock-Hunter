#!/usr/bin/env python3
"""CORE224 LIVE diagnostic preflight (read-only, fail-closed).
No production code changes; source inspection + cache freshness assessment only.
"""
import argparse, ast, csv, datetime as dt, json, os, re
from pathlib import Path

KEYS = ('V1081_DIRECT_MAX_DATES', 'V1081_DIRECT_DATE_MODE', 'WEEK_LAST',
        'V1080_BACKTEST_HOLD_DAYS', 'V25_CURRENT_LIVE_END_DATE',
        'V25_TWO_YEAR_END_DATE', 'selected_dates', 'date_mode', 'max_dates')
FILES = ('main7_bugfix_2.py', 'direct_replay_performance_v20.py',
         'direct_replay_materialized_v23.py', 'original_thesis_reconstruction.py')

def inspect_file(path):
    if not path.exists():
        return {'path': str(path), 'status': 'MISSING', 'bytes': 0, 'hits': []}
    hits = []
    # Read lines incrementally; main7 may be very large.
    with path.open(encoding='utf-8', errors='replace') as f:
        for no, line in enumerate(f, 1):
            found = [k for k in KEYS if k in line]
            if found:
                hits.append({'line': no, 'keys': found, 'snippet': line.strip()[:360]})
    return {'path': str(path), 'status': 'PRESENT', 'bytes': path.stat().st_size,
            'hits_count': len(hits), 'hits': hits[:250], 'truncated': len(hits) > 250}

def read_manifest(path):
    if not path.exists():
        return {'status':'MISSING', 'path':str(path)}
    try:
        m=json.loads(path.read_text(encoding='utf-8'))
        return {'status':m.get('status','UNKNOWN'), 'path':str(path),
                'selected_dates':m.get('selected_dates',[]),
                'selected_date_count':m.get('selected_date_count'),
                'cohort_id':m.get('cohort_id'), 'source_fingerprint':m.get('source_fingerprint'),
                'config_fingerprint':m.get('config_fingerprint')}
    except Exception as e:
        return {'status':'INVALID', 'path':str(path), 'error':type(e).__name__+':'+str(e)}

def run(args):
    target = dt.date.fromisoformat(args.target)
    files = [inspect_file(Path(p)) for p in FILES]
    paths = [Path('reports/v25_live_seed_build/v73_v23_shard_manifest.json'),
             Path('reports/v73_v23_shard_manifest.json')]
    manifests=[read_manifest(p) for p in paths]
    # Presence of existing current manifest is a strict condition, but cannot prove
    # the *next* worker date-generator will select the same date.
    current = [m for m in manifests if m.get('status')=='COMPLETE' and
               m.get('selected_dates')==[args.target]]
    guard = 'EXISTING_EXACT_MANIFEST_ONLY' if current else 'NO_CURRENT_COMPLETE_MANIFEST'
    summary = {
        'version':'CORE224_LIVE_DATE_PREFLIGHT_R1', 'research_only':True,
        'production_changed':False, 'target':str(target),
        'decision':'DIAGNOSTIC_ONLY_NOT_APPROVED_TO_RUN_LIVE_WORKER',
        'guard_status':guard,
        'causal_note':'An exact-date cache manifest does not prove future-date generator selection or PIT universe.',
        'source_files':files, 'manifests':manifests,
        'required_next_check':'Locate date-selection implementation in main7_bugfix_2.py and supply deterministic one-day override before worker.'
    }
    out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
    (out/'core224_date_preflight.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    with (out/'core224_source_hits.csv').open('w',newline='',encoding='utf-8-sig') as f:
        w=csv.writer(f);w.writerow(['file','line','keys','snippet'])
        for item in files:
            for h in item['hits']:
                w.writerow([item['path'],h['line'],';'.join(h['keys']),h['snippet']])
    print('CORE224_DATE_PREFLIGHT', 'target='+str(target),'guard='+guard,
          'decision=DIAGNOSTIC_ONLY', 'production_changed=0')
    for item in files:
        print('SOURCE',item['path'],item['status'],'hits',item.get('hits_count',0))
    for m in manifests:
        print('MANIFEST',m['path'],m['status'],m.get('selected_dates'))
    return 0

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--target',required=True);p.add_argument('--output',default='reports/core224_date_forensic_r1')
    raise SystemExit(run(p.parse_args()))
