"""R1-D99 non-expiring WATCH / interaction-history study on frozen R1-D96 and D98.

Python ONLY. No YML/workflow is included or modified. No production edits.
Execute in Stock-Hunter checkout root:
  python research_projects/r1d99/research_r1d99.py
"""
from __future__ import annotations
from pathlib import Path
from collections import Counter
import hashlib,json,sys
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
BASE=ROOT/'r1d97'
R98=ROOT/'r1d98'
sys.path[:0]=[str(BASE),str(R98),str(HERE)]
from research_r1d96 import load,choose
from state_machine import enrich as classify98,pick as pick98
from recency_dedup import enrich,assert_unmodified,presentation_groups

CASES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-04'),('006400','삼성SDI','2024-02-07')]
EXPECTED_SHA256={
 '039840_KRX.csv':'330ffdc5febbbf76f3f959d72181a2a37cc144ec567e6e7f691985b7dc82e3e4',
 '071840_KRX.csv':'aba4ffb8702961fbc8d3d411138b755092551f2fd6ef1a2ac3ce02da2c221c1b',
 '006400_KRX.csv':'d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566',
}
OUT=HERE/'results'


def verify_inputs():
    for source in (BASE/'research_r1d96.py',R98/'state_machine.py'):
        if not source.exists():raise FileNotFoundError('Missing frozen research module: '+str(source))
    for filename,sha in EXPECTED_SHA256.items():
        p=BASE/'data'/filename
        if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=sha:
            raise RuntimeError('Missing or modified KRX input: '+filename)


def signature(rows):
    return sorted((r['line_id'],r['kind'],r['tier'],str(r['status']),str(r['bucket']),
                   str(r.get('first_event_date','')),str(r.get('last_event_date','')),
                   str(r.get('d99_last_contact_date','')),str(r.get('d99_last_reclaim_date','')),
                   str(r.get('d99_last_interaction_age_bars','')))
                  for r in rows)


def output(filename,records):
    pd.DataFrame(records).to_csv(OUT/filename,index=False,encoding='utf-8-sig')


def run():
    verify_inputs()
    OUT.mkdir(parents=True,exist_ok=True)
    catalog=[];oldselection=[];selected=[];watch_all=[];presentation=[];bycase=[];pit=[]
    for code,name,asof in CASES:
        d=load(code)
        base,_,_=choose(d,asof)
        classified=classify98(base,d,asof)
        annotated=enrich(classified,d,asof)
        assert_unmodified(classified,annotated)
        r98selection=pick98(classified)
        ids={z['line_id'] for group in ('PRIMARY','WATCH') for z in r98selection[group]}
        old_ids=[z['line_id'] for group in ('PRIMARY','WATCH') for z in r98selection[group]]
        selected99=[r for r in annotated if r['line_id'] in ids]
        for r in annotated:
            catalog.append(dict(code=code,name=name,**r))
            if r['bucket']=='WATCH':watch_all.append(dict(code=code,name=name,**r))
        for r in selected99:selected.append(dict(code=code,name=name,**r))
        presentation.extend(presentation_groups([dict(code=code,name=name,**r) for r in selected99]))
        for r in r98selection['PRIMARY']+r98selection['WATCH']:
            oldselection.append(dict(code=code,name=name,**r))
        after_ids={z['line_id'] for z in selected99}
        if after_ids!=ids or len(selected99)!=len(ids):raise AssertionError('R98 selection changed')
        old_watch={z['line_id'] for z in r98selection['WATCH']}
        new_watch={z['line_id'] for z in selected99 if z['bucket']=='WATCH'}
        if old_watch != new_watch:raise AssertionError('old WATCH lost or expired')
        t=int(d.index[d.date.eq(pd.Timestamp(asof))][0])
        tampered=d.copy()
        for col in ('open','high','low','close'):
            tampered[col]=tampered[col].astype(float)
            tampered.loc[tampered.index>t,col]*=1.217
        altered=enrich(classify98(choose(tampered,asof)[0],tampered,asof),tampered,asof)
        if signature(annotated)!=signature(altered):raise AssertionError('PIT future lookahead: '+code)
        pit.append(dict(code=code,asof=asof,passed=True,old_watch=len(old_watch),new_watch=len(new_watch)))
        bycase.append(dict(code=code,name=name,asof=asof,
            full_candidates=len(classified),
            r98_primary=len(r98selection['PRIMARY']),r99_primary=sum(r['bucket']=='PRIMARY' for r in selected99),
            r98_watch=len(r98selection['WATCH']),r99_watch=len(new_watch),
            all_watch_candidates=sum(r['bucket']=='WATCH' for r in annotated),
            unique_presentation_representatives=sum(m['is_display_representative'] for m in presentation if m['code']==code),
            preserved_watch=True,no_watch_expiration=True))
        print(code,name,'R98 P/W:',len(r98selection['PRIMARY']),len(old_watch),
              'R99 P/W:',bycase[-1]['r99_primary'],len(new_watch),
              'ALL WATCH CANDIDATES:',bycase[-1]['all_watch_candidates'])
    for fn,rows in [
      ('R1D99_ALL_111_CANDIDATES_METADATA.csv',catalog),
      ('R1D99_WATCH_FULL_CATALOG_NO_EXPIRY.csv',watch_all),
      ('R1D99_R98_SELECTED_FROZEN.csv',oldselection),
      ('R1D99_SELECTED_UNCHANGED_WITH_HISTORY.csv',selected),
      ('R1D99_SIMILAR_LINE_DISPLAY_GROUPS_NO_REMOVAL.csv',presentation),
      ('R1D99_CASE_SUMMARY.csv',bycase),('R1D99_PIT_FUTURE_TAMPER.csv',pit)]:
        output(fn,rows)
    meta=dict(version='R1D99_NONEXPIRING_WATCH_HISTORY_RESEARCH_ONLY',cases=len(bycase),
        full_candidates=len(catalog),r98_selected=len(oldselection),r99_selected=len(selected),
        r98_watch=sum(x['r98_watch'] for x in bycase),r99_watch=sum(x['r99_watch'] for x in bycase),
        all_watch_candidate_count=len(watch_all), no_watch_expired=True,
        r98_selection_unchanged=True, future_tamper_passed=all(x['passed'] for x in pit),
        structure_labels=dict(Counter(x['status'] for x in catalog)),
        cases_detail=bycase, data_hash_checked=True,
        price_basis='original KRX OHLCV; corporate action completeness not verified',
        limitations=['3 fixed stock examples, not full market','no forward-performance or PnL conclusion',
        'contact with a line does not prove support','similarity tolerance is display-only and not calibrated',
        'PIT historical scan only; corporate actions still unverified'],
        readiness='CASE_STUDY_ONLY_NOT_LIVE_NOT_FULLMARKET')
    (OUT/'R1D99_AUDIT.json').write_text(json.dumps(meta,indent=2,ensure_ascii=False),encoding='utf-8')
    print('SUMMARY',json.dumps({k:meta[k] for k in ('full_candidates','r98_selected','r99_selected','r98_watch','r99_watch','no_watch_expired')},ensure_ascii=False))
    return meta

if __name__=='__main__':run()
