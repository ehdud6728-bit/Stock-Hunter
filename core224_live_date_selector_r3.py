#!/usr/bin/env python3
"""R3 LIVE-only exact date selection. No historical/production changes."""
from __future__ import annotations
import argparse, datetime as dt, os, pathlib, re, sys

EXACT_OLD = ('        replay_dates = _v1081_latest_trading_calendar(weeks=weeks, hold_days=hold_days)\n'
             '        selected = _v7336623_mat.partition_dates(replay_dates, shard_index, shard_count, newest_first=True)')
EXACT_NEW = '''        replay_dates = _v1081_latest_trading_calendar(weeks=weeks, hold_days=hold_days)
        if (os.environ.get('CORE224_EXACT_TARGET','').strip()
                and os.environ.get('V25_COHORT_ID','').strip() == 'CORE224_CURRENT_ROLLING'):
            from core224_live_date_selector_r3 import select_exact_live_date_r3
            replay_dates = select_exact_live_date_r3(replay_dates, fdr.DataReader)
        selected = _v7336623_mat.partition_dates(replay_dates, shard_index, shard_count, newest_first=True)'''
MARKER = 'from core224_live_date_selector_r3 import select_exact_live_date_r3'


def select_exact_live_date_r3(previous, data_reader):
    """Fail closed unless exact target is independently present in KRX index session dates."""
    required = {
        'CORE224_EXACT_TARGET': os.environ.get('CORE224_EXACT_TARGET','').strip(),
        'V25_COHORT_ID': os.environ.get('V25_COHORT_ID','').strip(),
        'V1080_BACKTEST_HOLD_DAYS': os.environ.get('V1080_BACKTEST_HOLD_DAYS','').strip(),
        'V23_SHARD_COUNT': os.environ.get('V23_SHARD_COUNT','').strip(),
        'V23_SHARD_INDEX': os.environ.get('V23_SHARD_INDEX','').strip(),
        'V1081_DIRECT_MAX_DATES': os.environ.get('V1081_DIRECT_MAX_DATES','').strip(),
        'V23_SHARD_WORKER_ONLY': os.environ.get('V23_SHARD_WORKER_ONLY','').strip(),
        'STOCKHUNTER_WEEKLY_BACKTEST_ONLY': os.environ.get('STOCKHUNTER_WEEKLY_BACKTEST_ONLY','').strip(),
        'V25_CURRENT_LIVE_END_DATE': os.environ.get('V25_CURRENT_LIVE_END_DATE','').strip(),
    }
    expected={'V25_COHORT_ID':'CORE224_CURRENT_ROLLING','V1080_BACKTEST_HOLD_DAYS':'0',
              'V23_SHARD_COUNT':'1','V23_SHARD_INDEX':'0','V1081_DIRECT_MAX_DATES':'1',
              'V23_SHARD_WORKER_ONLY':'1','STOCKHUNTER_WEEKLY_BACKTEST_ONLY':'1'}
    mismatch=[k for k,v in expected.items() if required[k]!=v]
    if mismatch: raise RuntimeError('CORE224_R3_LIVE_SCOPE_MISMATCH:'+','.join(mismatch))
    target=dt.date.fromisoformat(required['CORE224_EXACT_TARGET'])
    if required['V25_CURRENT_LIVE_END_DATE'] != target.isoformat():
        raise RuntimeError('CORE224_R3_TARGET_ENV_MISMATCH')
    if target > dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).date():
        raise RuntimeError('CORE224_R3_FUTURE_TARGET')
    start=(target-dt.timedelta(days=21)).isoformat()
    end=(target+dt.timedelta(days=1)).isoformat()
    import pandas as pd
    errors=[]
    for symbol in ('KS11','KOSPI'):
        try:
            frame=data_reader(symbol,start,end)
            if frame is None or frame.empty: raise ValueError('EMPTY_INDEX_CALENDAR')
            days={pd.Timestamp(x).date() for x in pd.to_datetime(frame.index, errors='coerce') if pd.notna(x)}
            if target not in days: raise ValueError('EXACT_TARGET_NOT_IN_KRX_SESSIONS')
            print('CORE224_R3_EXACT_DATE_PASS target='+target.isoformat()+
                  ' original='+','.join(str(x)[:10] for x in previous)+
                  ' index='+symbol+' mode=LIVE_ONLY research_only=1',flush=True)
            return [pd.Timestamp(target)]
        except Exception as exc:
            errors.append(symbol+':'+type(exc).__name__+':'+str(exc)[:80])
    raise RuntimeError('CORE224_R3_EXACT_SESSION_AUTHORITY_FAILED:'+'|'.join(errors))


def patch_main(path):
    p=pathlib.Path(path); raw=p.read_text(encoding='utf-8')
    if MARKER in raw:
        if raw.count(MARKER)!=1: raise RuntimeError('CORE224_R3_DUPLICATE_MARKER')
        print('CORE224_R3_MAIN_ALREADY_PATCHED'); return
    if raw.count(EXACT_OLD)!=1:
        raise RuntimeError('CORE224_R3_SELECTOR_ANCHOR_NOT_UNIQUE count='+str(raw.count(EXACT_OLD)))
    replacement=raw.replace(EXACT_OLD,EXACT_NEW,1)
    compile(replacement,str(p),'exec')
    p.write_text(replacement,encoding='utf-8')
    print('CORE224_R3_MAIN_RUNTIME_PATCH_PASS; production_source_commit_unchanged=1',flush=True)


def patch_workflow(path):
    p=pathlib.Path(path); s=p.read_text(encoding='utf-8')
    needle='            python -u "$SCRIPT"\n\n          echo "V25_CURRENT_LIVE_SEED_LOADER_PREFLIGHT_BEGIN"'
    ins='            python -u core224_live_date_selector_r3.py --patch-main "$SCRIPT"\n            python -u "$SCRIPT"\n\n          echo "V25_CURRENT_LIVE_SEED_LOADER_PREFLIGHT_BEGIN"'
    if '--patch-main "$SCRIPT"' in s:
        print('CORE224_R3_WORKFLOW_ALREADY_PATCHED');return
    if s.count(needle)!=1:raise RuntimeError('CORE224_R3_WORKFLOW_ANCHOR_NOT_UNIQUE count='+str(s.count(needle)))
    s=s.replace(needle,ins,1)
    p.write_text(s,encoding='utf-8')
    print('CORE224_R3_WORKFLOW_PATCH_PASS path='+str(p))


def selftest():
    import tempfile, pandas as pd
    with tempfile.TemporaryDirectory() as td:
        p=pathlib.Path(td)/'main.py'
        p.write_text('def f():\n'+EXACT_OLD+'\n',encoding='utf-8')
        patch_main(p);patch_main(p)
        assert MARKER in p.read_text()
    old=dict(os.environ)
    try:
        os.environ.update({'CORE224_EXACT_TARGET':'2026-10-12','V25_CURRENT_LIVE_END_DATE':'2026-10-12',
            'V25_COHORT_ID':'CORE224_CURRENT_ROLLING','V1080_BACKTEST_HOLD_DAYS':'0',
            'V23_SHARD_COUNT':'1','V23_SHARD_INDEX':'0','V1081_DIRECT_MAX_DATES':'1',
            'V23_SHARD_WORKER_ONLY':'1','STOCKHUNTER_WEEKLY_BACKTEST_ONLY':'1'})
        cal=pd.DataFrame({'close':[1,1]},index=pd.to_datetime(['2026-10-08','2026-10-12']))
        got=select_exact_live_date_r3(['2026-09-17'],lambda *_: cal)
        assert str(got[0].date())=='2026-10-12'
        try: select_exact_live_date_r3([],lambda *_: pd.DataFrame(index=pd.to_datetime(['2026-10-08'])))
        except RuntimeError as e:assert 'AUTHORITY_FAILED' in str(e)
        else: raise AssertionError('expected fail-closed')
    finally:
        os.environ.clear();os.environ.update(old)
    print('CORE224_R3_SELF_TEST_PASS')

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--patch-main');ap.add_argument('--patch-workflow');ap.add_argument('--self-test',action='store_true');a=ap.parse_args()
    if a.self_test:selftest()
    elif a.patch_main:patch_main(a.patch_main)
    elif a.patch_workflow:patch_workflow(a.patch_workflow)
    else:ap.error('choose patch or self-test')
