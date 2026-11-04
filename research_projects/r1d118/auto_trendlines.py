#!/usr/bin/env python3
"""Recompute TS_BODY_WICK_PROXY R111 -> R112 -> R116 -> R117 original source-code lineage.

No homemade MA proxy. Generates original immutable two-anchor geometry and 6 causal
R117 lanes for each new stock using the same complete OHLCV history/indices as R118.
One algorithm lane (TS_BODY_WICK_PROXY), not all four R111 comparisons.
"""
from __future__ import annotations
import hashlib,json,sys,time
from pathlib import Path
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
for stage in ('r1d111','r1d112','r1d116','r1d117'):
    folder=ROOT/stage
    if not folder.exists():raise FileNotFoundError(f'ORIGINAL_R111_R117_SOURCE_MISSING {folder}')
    sys.path.insert(0,str(folder))
from algorithms import prepare,make_anchors,generate_candidates
from multiscale import monitor_multiscale
from recent_turn import replay_one as r116_replay
from stable_roles import replay_one as r117_replay
from research_r1d118 import verified_files

ALGORITHM='TS_BODY_WICK_PROXY'
ROLE_COLS=['code','algorithm','scale','broken_line_id','old_kind','new_role_candidate',
           'break_idx','break_date','retest_idx','retest_date','days_after_break',
           'retest_count','correct_new_side_close','status','line_level','close']


def generate_one(code,raw):
    d=prepare(raw)
    if len(d)<55:raise ValueError('INSUFFICIENT_FULL_HISTORY '+code)
    anc=make_anchors(d)
    candidates=generate_candidates(d,ALGORITHM,anchors=anc)
    # Zero immutable candidates is a valid observed state: all six lanes are NO_ACTIVE_LINE.
    s112,_,roles=monitor_multiscale(d,ALGORITHM,candidates)
    if len(s112)!=len(d)*6:raise AssertionError('R112_6_LANES_NOT_COMPLETE '+code)
    s116,_,top3=r116_replay(code,ALGORITHM,candidates,s112[['idx','date','close']],top_n=3)
    if len(s116)!=len(d)*6:raise AssertionError('R116_6_LANES_NOT_COMPLETE '+code)
    raw_with_idx=d.copy();raw_with_idx.insert(0,'idx',range(len(d)))
    s117,_,_=r117_replay(code,ALGORITHM,s116,top3,candidates,raw=raw_with_idx)
    if len(s117)!=len(d)*6:raise AssertionError('R117_6_LANES_NOT_COMPLETE '+code)
    if s117[['code','date','scale','kind','idx']].duplicated().any():raise AssertionError('DUPLICATE_ORIGINAL_LANE '+code)
    if set(s117.scale)!=set(['SHORT','MEDIUM','LONG']) or set(s117.kind)!=set(['UPPER','LOWER']):
        raise AssertionError('INCOMPLETE_SCALES_OR_SIDES '+code)
    if len(roles):roles.insert(0,'code',code)
    return s117,roles,{'candidate_lines':len(candidates),'R112_role_events':len(roles),
                       'R116_top3':len(top3),'R117_lane_rows':len(s117),'original_algorithm':ALGORITHM}


def generate_all(manifest,out,*,max_stocks=None):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    states_path=out/'R118_R117_SOURCE_RECOMPUTED_DAILY_PIT.csv'
    role_path=out/'R118_R112_SOURCE_RECOMPUTED_EXACT_RETESTS_PIT.csv'
    if states_path.exists() or role_path.exists():raise FileExistsError('OUTPUT_ALREADY_EXISTS_RISK '+str(out))
    files=verified_files(manifest)
    files=files[:max_stocks] if max_stocks else files
    details={};begin=time.monotonic()
    with states_path.open('w',encoding='utf-8-sig',newline='') as f_states,role_path.open('w',encoding='utf-8-sig',newline='') as f_roles:
        # Keep memory bounded; append per-stock. Header even if no retests.
        pd.DataFrame(columns=ROLE_COLS).to_csv(f_roles,index=False)
        wrote=False
        for n,(code,path,sha) in enumerate(files,1):
            raw=pd.read_csv(path,dtype={'code':str},encoding='utf-8-sig')
            if raw.code.astype(str).str.zfill(6).nunique()!=1 or raw.code.astype(str).str.zfill(6).iloc[0]!=code:
                raise ValueError('PINNED_CODE_DATA_MISMATCH '+code)
            states,roles,stat=generate_one(code,raw)
            states.to_csv(f_states,index=False,header=not wrote)
            wrote=True
            if len(roles):roles.reindex(columns=ROLE_COLS).to_csv(f_roles,index=False,header=False)
            details[code]=stat
            if n<=3 or n%10==0 or n==len(files):
                print(f'R118_R117_SOURCE_REPLAY {n}/{len(files)} {code} candidate={stat["candidate_lines"]} lanes={stat["R117_lane_rows"]}',flush=True)
    audit={'mode':'R111_R112_R116_R117_CANONICAL_SOURCES_REPLAY','algorithm_lane':ALGORITHM,
           'stock_count':len(details),'stocks':details,'states_path':str(states_path),
           'roles_path':str(role_path),'elapsed_seconds':round(time.monotonic()-begin,1),
           'sources_frozen_original_not_modified':True,
           'limitations':['Other R111 algorithm lanes not recomputed in this pass.',
                          'Historical stability is exploratory, not proven profitable.']}
    (out/'R118_ORIGINAL_TRENDLINE_REPLAY_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
    return states_path,role_path,audit
