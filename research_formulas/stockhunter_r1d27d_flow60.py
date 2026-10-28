"""R1D27D source-only incremental investigator: 1500 disjoint historical events.

DO NOT USE for LIVE or score/rank. Fail-closed on changed freeze or cached raw.
Each invocation collects at most --batch-size new events and seals one checkpoint.
"""
from __future__ import annotations
import argparse, gzip, hashlib, io, json, pathlib, time
import pandas as pd
from research_formulas import stockhunter_r1d27c_flow60 as c27

FROZEN_SHA='545f73cf0cdee0b1c3d2912edcc7a05fac1f9e11c72d19d10eba956ac50bbfba'
FROZEN_NAME='R1D27D_FROZEN_1500_TICKER_DISJOINT_OUTCOME_BLIND.csv'
AUDIT='R1D27D_FLOW_SOURCE_AUDIT.csv'
RAW='R1D27D_FLOW60_DAILY.csv.gz'
MAN='R1D27D_FLOW_MANIFEST.json'
KEY=['date','code','bb_period','event_type']
COLS=[*KEY,'trade_date','actor','buy_krw','sell_krw','net_krw','total_krw','buy_shares','sell_shares','net_shares','total_shares']
ACOLS=[*KEY,'status','reason_code','source_complete']
STATUS_OK='COMPLETE_60_GROSS_NET_BOTH_UNITS_NOT_PIT'
STATUS_BAD='SOURCE_QUARANTINED_NO_IMPUTATION'

def sha(b):return hashlib.sha256(b).hexdigest()
def check(p,err):
    if not p:raise ValueError('R1D27D_'+err)
def keyrow(it):return tuple(str(getattr(it,k)) for k in KEY)
def read_event_index(freeze_dir):
    freeze_dir=pathlib.Path(freeze_dir);fb=(freeze_dir/FROZEN_NAME).read_bytes();m=json.loads((freeze_dir/'R1D27D_FROZEN_MANIFEST.json').read_text())
    check(sha(fb)==FROZEN_SHA and m['selected_rows_sha256']==FROZEN_SHA,'FROZEN_TAMPER')
    f=pd.read_csv(io.BytesIO(fb),dtype={'date':str,'code':str,'bb_period':str,'event_type':str})
    check(len(f)==1500 and f.code.nunique()==1500,'FROZEN_N1500')
    check(f.groupby(['year','bb_period','event_type']).size().eq(50).all(),'FROZEN_BALANCE')
    check(not f[KEY].duplicated().any(),'FROZEN_DUP')
    return f

def load_old(seed_root,f):
    if not seed_root:return [],{}
    root=pathlib.Path(seed_root)/'reports/r1d27d/result/flow'
    m=json.loads((root/MAN).read_text())
    check(m['freeze_sha256']==FROZEN_SHA and m['n_frozen']==1500,'CHECKPOINT_ORIGIN')
    for name in [RAW,AUDIT]:check(sha((root/name).read_bytes())==m['files_sha256'][name],'CHECKPOINT_TAMPER_'+name)
    audit=pd.read_csv(root/AUDIT,dtype={k:str for k in KEY}).fillna('')
    raw=pd.read_csv(root/RAW,dtype={k:str for k in [*KEY,'trade_date','actor']})
    check(not audit[KEY].duplicated().any(),'OLD_DUP_AUDIT')
    check(not raw[[*KEY,'trade_date','actor']].duplicated().any(),'OLD_DUP_SOURCE')
    index={keyrow(x):x for x in f.itertuples(index=False)}
    raw_keys=[keyrow(x) for x in raw.itertuples(index=False)]
    audit_keys=[keyrow(x) for x in audit.itertuples(index=False)]
    check(set(raw_keys).issubset(index) and set(audit_keys).issubset(index),'SOURCE_OUTSIDE_FREEZE')
    old={k:dict(zip(ACOLS,v)) for k,v in zip(audit_keys,audit[ACOLS].itertuples(index=False,name=None))}
    passed={k for k,v in old.items() if int(v['source_complete'])==1}
    check(len(raw)==240*len(passed),'OLD_SOURCE_ROWS')
    check(set(raw_keys)==passed,'OLD_SOURCE_KEYSET')
    # Verify strict 60 dates by each source event, four actor groups, arithmetic both units.
    by_code={k: g for k,g in raw.groupby(KEY,sort=False)}
    check(len(by_code)==len(passed),'OLD_SOURCE_GROUPS')
    for k,grp in by_code.items():
        check(len(grp)==240,'SOURCE_GROUP_LEN')
        check(set(grp.actor)==set(c27.ACTORS),'SOURCE_ACTOR_TYPES')
        check(grp.trade_date.nunique()==60,'SOURCE_DAY_COUNT')
        check((grp.groupby('trade_date').actor.nunique()==4).all(),'SOURCE_ACTORS_PER_DAY')
        e=index[k]
        check(grp.trade_date.min()>=str(e.pre60_from) and grp.trade_date.max()==str(e.pre60_to),'SOURCE_TIME_LEAK')
        for suf in ('krw','shares'):
            lhs=pd.to_numeric(grp[f'buy_{suf}'])-pd.to_numeric(grp[f'sell_{suf}'])
            rhs=pd.to_numeric(grp[f'net_{suf}'])
            check(((lhs-rhs).abs()<=2).all(),'SOURCE_GROSS_NET_MATH')
    return raw.to_dict('records'),old

def emit_csv(path,df,compressed=False):
    if compressed:
        buf=io.BytesIO()
        with gzip.GzipFile(fileobj=buf,mode='wb',mtime=0,filename='') as gz:
            gz.write(df.to_csv(index=False,lineterminator='\n').encode('utf-8'))
        path.write_bytes(buf.getvalue())
    else:df.to_csv(path,index=False,lineterminator='\n')

def collect_batch(freeze_dir,krx_root,out,batch_size=30,seed_root=None,fetch=None,cooldown=0.8,max_fail=3):
    check(1<=batch_size<=30,'BATCH_SIZE')
    f=read_event_index(freeze_dir)
    # Reuses existing trusted KRX session reader and 6-branch provider verification.
    from research_formulas import formula66_r1d6_oos_holdout as d6
    cal,_=d6.index_asof(krx_root)
    check(len(cal)==1907 and cal[0]=='2019-01-02','KRX_CALENDAR')
    raw,old=load_old(seed_root,f)
    began=0;success=0;consecutive=0;abort=False
    try:
        for e in f.itertuples(index=False):
            k=keyrow(e)
            if k in old:continue
            if began>=batch_size:break
            ix=int(e.signal_ix);days=cal[ix-60:ix]
            check(len(days)==60 and days[0]==e.pre60_from and days[-1]==e.pre60_to and cal[ix]==e.date,'CALENDAR_LEAK_OR_DRIFT')
            began+=1
            try:
                feeds=c27.get_flow(days[0].replace('-',''),days[-1].replace('-',''),e.code,fetch=fetch)
                got=c27.verify_six(feeds,days)
                raw.extend([{**dict(zip(KEY,k)),**q} for q in got])
                old[k]=dict(zip(ACOLS,[*k,STATUS_OK,'',1]));success+=1;consecutive=0
            except Exception as ex:
                reason=str(ex).removeprefix('R1D27C_') if isinstance(ex,c27.AuditError) else type(ex).__name__
                old[k]=dict(zip(ACOLS,[*k,STATUS_BAD,reason,0]));consecutive+=1
            if began%5==0:print('R1D27D_BATCH',began,'complete',success,'known',len(old),'consecutive_errors',consecutive,flush=True)
            if consecutive>=max_fail:abort=True;break
            if cooldown:time.sleep(cooldown)
    finally:
        # Persist/checkpoint even after provider failure; atomic files are committed before next batch.
        output=pathlib.Path(out);output.mkdir(parents=True,exist_ok=True)
        p=output/'reports/r1d27d/result/flow';p.mkdir(parents=True,exist_ok=True)
        a=pd.DataFrame([old[k] for k in sorted(old)],columns=ACOLS)
        x=pd.DataFrame(raw,columns=COLS)
        if len(x):x=x.sort_values([*KEY,'trade_date','actor'],kind='stable').reset_index(drop=True)
        emit_csv(p/AUDIT,a);emit_csv(p/RAW,x,compressed=True)
        n_ok=int(a.source_complete.astype(int).sum()) if len(a) else 0
        check(len(x)==240*n_ok,'NEW_ROWS_COUNT')
        sm=dict(version='R1D27D_FLOW60_V1',phase='collect',n_frozen=1500,freeze_sha256=FROZEN_SHA,
            n_complete=n_ok,n_unattempted=1500-len(a),n_quarantined=len(a)-n_ok,
            batch_attempted=began,batch_success=success,provider_abort=abort,
            strategy_ready=False,research_only=True,prospective_OOS=False,orders_placed=0,
            fields_verified=['60_CLOSED_SESSIONS','4_ACTORS','6_GROSS_NET_BRANCHES','NO_IMPUTATION','NO_POST_SIGNAL_SOURCE'],
            files_sha256={k:sha((p/k).read_bytes()) for k in [RAW,AUDIT]})
        (p/MAN).write_text(json.dumps(sm,ensure_ascii=False,indent=2,sort_keys=True)+'\n')
        state=output/'reports/r1d27d/R1D27D_STAGE_STATE.json';state.parent.mkdir(parents=True,exist_ok=True)
        state.write_text(json.dumps(sm,ensure_ascii=False,indent=2,sort_keys=True)+'\n')
    return sm

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--freeze-dir',required=True);p.add_argument('--krx-root',required=True)
    p.add_argument('--out',required=True);p.add_argument('--seed-root');p.add_argument('--batch-size',type=int,default=30)
    a=p.parse_args();m=collect_batch(a.freeze_dir,a.krx_root,a.out,a.batch_size,a.seed_root)
    print('R1D27D_CHECKPOINT',json.dumps(m,ensure_ascii=False),flush=True)
    if m['provider_abort']:raise SystemExit(3)
