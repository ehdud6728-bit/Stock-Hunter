#!/usr/bin/env python3
"""R1-D26H: NEW ticker-disjoint 200 historical ASOF signals (C100/D100).

RESEARCH ONLY: first freeze events without returns; fetch only historical pre-signal
KRX investor KRW; seal source bytes; only then evaluate ONE previously identified
D institution recent5 acceleration against +15% intraday D30 hit. Historical
holdout, NOT prospectively timestamped OOS; NOT PIT-certified; no LIVE changes.
"""
from __future__ import annotations
import argparse, csv, datetime as dt, gzip, hashlib, io, json, math, time, zipfile
from collections import Counter, defaultdict
from pathlib import Path
import numpy as np
import pandas as pd

VERSION='R1D26H_TICKER_DISJOINT_200_HISTORICAL_HOLDOUT_SOURCE_AND_FIXED_PRIMARY_V1'
C='C_ABOVE224_RISING_WAVE_PULLBACK';D='D_LEADER_CONSOLIDATION_BREAKOUT'
B25='reports/r1d25/result/historical/'
B26='reports/r1d26/result/pilot/'
BF='reports/r1d26f/result/source_only/'
BE='reports/r1d26e/result/historical/'
ASOF=B25+'R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz'
INDEX=B26+'official_indices_from_frozen_shard0.csv'
SEED_EVENTS=BF+'R1D26F_FROZEN_100_SIGNAL_EVENTS_ASOF_ONLY.csv'
SEED_FLOW=BF+'R1D26F_COMPLETE_EVENT_PRIOR20_TRUE_INVESTOR_KRW_NOT_PIT.csv'
SEED_MAN=BF+'R1D26F_MANIFEST.json'
PATH=BE+'R1D26E_C_D_ALL_COMPLETE_D30_INTERVAL_FIRST_TOUCH.csv.gz'
FIELDS=['date','code','strategy','year','signal_ix','pre20_from','pre20_to','event_source_hash','origin']
FLOW_FIELDS=['signal_date','code','strategy','trade_date','foreign_net_krw','institution_net_krw','unit','source','collection_timestamp_utc','origin','event_source_hash']
AUDIT_FIELDS=['date','code','strategy','year','origin','status','sessions','reason']
PROTO='R1D26H_PREDECLARED_PROTOCOL.json'

class QA(ValueError):pass
def require(test,why):
    if not test:raise QA('R1D26H_'+why)
def sh(b):return hashlib.sha256(b).hexdigest()
def eh(day,code,st):return sh(f'{day}|{code}|{st}'.encode())
def read_csv(z,mem,**kw):
    try:b=z.read(mem)
    except KeyError:raise QA('MEMBER_MISSING:'+mem)
    if mem.endswith('.gz'):b=gzip.decompress(b)
    dtype=dict(kw.pop('dtype',{}));dtype['code']=str
    return pd.read_csv(io.BytesIO(b),dtype=dtype,low_memory=False,**kw)
def zopen(path,sha):
    b=Path(path).read_bytes();require(sh(b)==sha,'ZIP_SHA_DRIFT');return zipfile.ZipFile(io.BytesIO(b))
def i64(v):
    s=str(v).strip();require(bool(s) and (s.isdigit() or (s[0]=='-' and s[1:].isdigit())),'NONINTEGER_KRW')
    return int(s)
def emit(path,fields,rows):
    with Path(path).open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
def protocol_ok(p):
    require(p['version']==VERSION and p['research_only'] and not p['live_changed'] and not p['PIT_certified'],'PROTOCOL_FLAGS')
    require(p['holdout_new']==200 and p['holdout_per_year_strategy']==20 and p['pre_signal_sessions']==20,'SAMPLE_PROTOCOL')
    require(p['primary_strategy']==D and p['primary_flag']=='institution_recent5_accel_pos' and p['primary_target']=='hit15_by_d30','PRIMARY_DRIFT')
    require(p['primary_hypothesis_locked_from']=='R1D26G_EXPLORATORY_NOT_VALIDATION' and p['test_only_new_ticker_disjoint_holdout'],'OVERFIT_GUARD')

def source(p,paths):
    protocol_ok(p)
    z25=zopen(paths['r1d25'],p['r1d25_sha256']);z26=zopen(paths['r1d26'],p['r1d26_sha256']);zf=zopen(paths['r1d26f'],p['r1d26f_sha256'])
    try:
        signal=read_csv(z25,ASOF)
        signal=signal[signal.strategy.isin((C,D))].copy()
        require(len(signal)==10310 and not signal.duplicated(['date','code','strategy']).any(),'SOURCE_10310_DRIFT')
        numeric=signal.code.str.fullmatch(r'[0-9]{6}')
        require(int((~numeric).sum())==38,'UNSUPPORTED_KRX_TICKER_DRIFT')
        signal=signal[numeric].copy()
        raw=read_csv(z26,INDEX)
        require(not raw.duplicated(['date','index']).any(),'CALENDAR_DUPLICATE')
        matrix=raw.pivot(index='date',columns='index',values='close').sort_index()
        require(len(matrix)==1907 and not matrix.isna().any().any() and set(matrix.columns)=={'KOSPI','KOSDAQ'},'CALENDAR_DRIFT')
        calendar=list(matrix.index); ixmap={d:i for i,d in enumerate(calendar)}
        seed_events=read_csv(zf,SEED_EVENTS)
        seed_flow=read_csv(zf,SEED_FLOW,dtype={'signal_date':str,'trade_date':str,'foreign_net_krw':str,'institution_net_krw':str})
        require(sh(zf.read(SEED_EVENTS))==p['r1d26f_events_sha256'] and sh(zf.read(SEED_FLOW))==p['r1d26f_flow_sha256'],'SEED_CSV_HASH')
        seed_man=json.loads(zf.read(SEED_MAN))
        require(seed_man['events_frozen']==100 and seed_man['complete_events']==100 and seed_man['complete_event_rows']==2000,'SEED_COMPLETE')
        require(len(seed_events)==100 and seed_events.code.nunique()==95 and len(seed_flow)==2000,'SEED_SHAPE')
        seed_ids=set(zip(seed_events.date,seed_events.code,seed_events.strategy));baseline_tickers=set(seed_events.code)
        require(len(seed_ids)==100 and len(baseline_tickers)==95,'SEED_IDENTITIES')
        out=[];taken=set(baseline_tickers)
        for year in range(2021,2026):
            for st in (C,D):
                subset=signal[(signal.date.str[:4]==str(year))&(signal.strategy==st)&(~signal.code.isin(taken))].copy()
                subset=subset[subset.signal_ix.astype(int)>=20]
                # Stable salted hash rank; outcomes are absent from these inputs.
                subset['rank']=subset.apply(lambda r:sh(f"R1D26H_LOCK_V1|{r['date']}|{r['code']}|{st}".encode()),axis=1)
                subset=subset.sort_values(['rank','date','code'])
                choice=subset.drop_duplicates('code').head(20)
                require(len(choice)==20,'HOLDOUT_INSUFFICIENT_'+str(year)+'_'+st)
                for r in choice.itertuples():
                    day=r.date;code=r.code;ix=int(r.signal_ix)
                    require(ixmap.get(day)==ix and ix>=20 and ix<len(calendar),'ASOF_IX')
                    out.append(dict(date=day,code=code,strategy=st,year=year,signal_ix=ix,pre20_from=calendar[ix-20],pre20_to=calendar[ix-1],event_source_hash=eh(day,code,st),origin='R1D26H_NEW_HASH_TICKER_DISJOINT'))
                    taken.add(code)
        require(len(out)==200 and len(set(r['code'] for r in out))==200,'NEW200_UNIQUE_CODES')
        require(not (set(r['code'] for r in out)&baseline_tickers),'LEAK_OVERLAP_CODE')
        # Preserve prior 100 from frozen source and append 200 new.
        old=seed_events.to_dict('records')
        for row in old:
            require(row['event_source_hash']==eh(row['date'],row['code'],row['strategy']),'BASE_EVENT_HASH')
            ix=int(row['signal_ix']);require(ixmap[row['date']]==ix,'BASE_CAL')
            require(row['pre20_from']==calendar[ix-20] and row['pre20_to']==calendar[ix-1],'BASE20_WINDOW')
        combined=old+out
        require(len(combined)==300 and len(set((r['date'],r['code'],r['strategy']) for r in combined))==300,'300_IDENTITY')
        # Origin and new sample exclusively ASOF. Strict no targets/features imported.
        return combined,seed_flow,calendar,out
    finally:
        z25.close();z26.close();zf.close()

def seed_cache(flow,old,calendar):
    ev={(r['date'],r['code'],r['strategy']):r for r in old};cache={};seen=defaultdict(list)
    require(len(ev)==100 and len(flow)==2000,'SEED100')
    for r in flow.to_dict('records'):
        key=(r['signal_date'],r['code'],r['strategy']);require(key in ev,'SEED_FOREIGN_EVENT')
        require(r['unit']=='KRW' and r['event_source_hash']==ev[key]['event_source_hash'],'SEED_BAD_ORIGIN')
        day=r['trade_date'];require(day<key[0],'SEED_FUTURE')
        pair=(key[1],day);v=(i64(r['foreign_net_krw']),i64(r['institution_net_krw']))
        if pair in cache:require(cache[pair][:2]==v,'SEED_CONFLICT')
        cache[pair]=(v[0],v[1],str(r['collection_timestamp_utc']),'R1D26F_PINNED_SOURCE')
        seen[key].append(day)
    for k,evr in ev.items():
        ix=int(evr['signal_ix']);require(len(seen[k])==20 and len(set(seen[k]))==20 and set(seen[k])==set(calendar[ix-20:ix]),'SEED_NOT20')
    return cache

def validate_provider(df,code,days,stamp):
    require(isinstance(df,pd.DataFrame) and not df.empty,'PROVIDER_EMPTY')
    require({'기관합계','외국인합계'}.issubset(df.columns),'PROVIDER_SCHEMA')
    out={}
    for key,row in zip(df.index,df.to_dict('records')):
        d=pd.Timestamp(key).date().isoformat()
        require(d not in out,'PROVIDER_DUP_DATE')
        out[d]=(i64(row['외국인합계']),i64(row['기관합계']))
    require(set(out)==set(days),'PROVIDER_NOT_EXACT_DATES')
    require(any(f or i for f,i in out.values()),'PROVIDER_ALL_ZERO')
    return {(code,day):(f,i,stamp,'R1D26H_PYKRX_KRX_PROVISIONAL') for day,(f,i) in out.items()}

def collect(paths,p,out,mode='prepare',fetch=None,sleep=0.4,abort_after=4):
    require(mode in ('prepare','collect'),'MODE')
    require(0<=sleep<=5 and 1<=abort_after<=10,'LIMITS')
    all_events,seed,cal,new=source(p,paths)
    root=Path(out);root.mkdir(parents=True,exist_ok=True)
    emit(root/'R1D26H_FROZEN_300_EVENTS_NO_OUTCOMES.csv',FIELDS,all_events)
    emit(root/'R1D26H_FROZEN_NEW_200_HOLDOUT_ASOF_ONLY.csv',FIELDS,new)
    cache=seed_cache(seed,all_events[:100],cal)
    plan=[{'code':r['code'],'start':r['pre20_from'],'end':r['pre20_to'],'expected_sessions':20} for r in new]
    emit(root/'R1D26H_NEW200_DEDUP_FETCH_PLAN.csv',['code','start','end','expected_sessions'],plan)
    require(len(plan)==200 and len({r['code'] for r in plan})==200,'FETCH_PLAN200')
    audit=[];attempted=0;failed=0;stopped=False
    stamp=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')
    if mode=='collect':
        require(callable(fetch),'NEED_FETCH')
        for r in new:
            attempted+=1
            days=cal[int(r['signal_ix'])-20:int(r['signal_ix'])]
            try:
                fetched=fetch(days[0].replace('-',''),days[-1].replace('-',''),r['code'])
                got=validate_provider(fetched,r['code'],days,stamp)
                require(not(set(got)&set(cache)),'RETRIEVAL_ALREADY_CACHED')
                cache.update(got);failed=0
                audit.append({'code':r['code'],'start':days[0],'end':days[-1],'status':'FETCH_20_VERIFIED_PROVISIONAL','error_type':''})
            except Exception as e:
                # Never store the provider exception string: could contain credentials or user data.
                failed+=1
                audit.append({'code':r['code'],'start':days[0],'end':days[-1],'status':'QUARANTINED_PROVIDER_OR_SCHEMA','error_type':type(e).__name__})
            if failed>=abort_after:
                stopped=True;break
            if sleep and attempted<len(new):time.sleep(sleep)
    emit(root/'R1D26H_REQUEST_AUDIT.csv',['code','start','end','status','error_type'],audit)
    full=[];coverage=[]
    for r in all_events:
        days=cal[int(r['signal_ix'])-20:int(r['signal_ix'])]
        got=[day for day in days if (r['code'],day) in cache]
        complete=len(got)==20
        coverage.append(dict(date=r['date'],code=r['code'],strategy=r['strategy'],year=r['year'],origin=r['origin'],status='COMPLETE_PROVISIONAL_NOT_PIT' if complete else 'INCOMPLETE_NO_IMPUTATION',sessions=len(got),reason='' if complete else 'MISSING_KRX_DATES'))
        if not complete:continue
        for day in days:
            f,i,at,origin=cache[(r['code'],day)]
            full.append(dict(signal_date=r['date'],code=r['code'],strategy=r['strategy'],trade_date=day,foreign_net_krw=f,institution_net_krw=i,unit='KRW',source='KRX_VIA_PYKRX_RETROSPECTIVE_NOT_PIT',collection_timestamp_utc=at,origin=origin,event_source_hash=r['event_source_hash']))
    require(len(full)==20*sum(r['status']=='COMPLETE_PROVISIONAL_NOT_PIT' for r in coverage),'OUTPUT_20X')
    require(len({(r['signal_date'],r['code'],r['strategy'],r['trade_date']) for r in full})==len(full),'OUTPUT_DUP')
    emit(root/'R1D26H_COMPLETE_300_PRIOR20_SIGNED_KRW_NOT_PIT.csv',FLOW_FIELDS,full)
    emit(root/'R1D26H_COVERAGE_BY_EVENT.csv',AUDIT_FIELDS,coverage)
    per=[]
    for y in range(2021,2026):
        for st in (C,D):
            s=[r for r in coverage if r['year']==y and r['strategy']==st];require(len(s)==30,'STAGE_30_PER_CELL')
            old=[r for r in s if r['origin']!='R1D26H_NEW_HASH_TICKER_DISJOINT'];n=[r for r in s if r['origin']=='R1D26H_NEW_HASH_TICKER_DISJOINT']
            require(len(old)==10 and len(n)==20,'10PLUS20_STRATA')
            per.append(dict(year=y,strategy=st,old_complete=sum(x['sessions']==20 for x in old),new_complete=sum(x['sessions']==20 for x in n),new_frozen=20))
    emit(root/'R1D26H_COVERAGE_YEAR_STRATEGY.csv',['year','strategy','old_complete','new_complete','new_frozen'],per)
    new_complete=sum(r['status']=='COMPLETE_PROVISIONAL_NOT_PIT' and r['origin']=='R1D26H_NEW_HASH_TICKER_DISJOINT' for r in coverage)
    # Deterministic independent KRX Data Marketplace audit worksheet.
    # Existing pykrx numbers are only the reference: official columns stay EMPTY
    # until an independent official export is supplied and reconciled.
    aa=[]
    for yr in range(2021,2026):
        for st in (C,D):
            eligible=[r for r in all_events if r['year']==yr and r['strategy']==st and r['origin']=='R1D26H_NEW_HASH_TICKER_DISJOINT']
            eligible=sorted(eligible,key=lambda r:sh(f"OFFICIAL_AUDIT_LOCK|{r['date']}|{r['code']}".encode()))
            for ev in eligible:
                dd=cal[int(ev['signal_ix'])-5]
                val=cache.get((ev['code'],dd))
                if val is None:continue
                aa.append(dict(signal_date=ev['date'],code=ev['code'],strategy=st,trade_date=dd,
                    pykrx_foreign_net_krw=val[0],pykrx_institution_net_krw=val[1],
                    official_KRX_foreign_net_krw='',official_KRX_institution_net_krw='',
                    source_official_export_name='',independently_verified='NO_OFFICIAL_INPUT'))
                break
    emit(root/'R1D26H_KRX_INDEPENDENT_AMOUNT_CROSSCHECK_10POINT_TEMPLATE.csv',
         ['signal_date','code','strategy','trade_date','pykrx_foreign_net_krw','pykrx_institution_net_krw',
          'official_KRX_foreign_net_krw','official_KRX_institution_net_krw','source_official_export_name','independently_verified'],aa)
    files={q:sh((root/q).read_bytes()) for q in ['R1D26H_KRX_INDEPENDENT_AMOUNT_CROSSCHECK_10POINT_TEMPLATE.csv','R1D26H_FROZEN_300_EVENTS_NO_OUTCOMES.csv','R1D26H_FROZEN_NEW_200_HOLDOUT_ASOF_ONLY.csv','R1D26H_NEW200_DEDUP_FETCH_PLAN.csv','R1D26H_COMPLETE_300_PRIOR20_SIGNED_KRW_NOT_PIT.csv','R1D26H_COVERAGE_BY_EVENT.csv','R1D26H_COVERAGE_YEAR_STRATEGY.csv']}
    m=dict(version=VERSION,mode=mode,old_completed=100,holdout_frozen=200,holdout_distinct_tickers=200,holdout_existing_ticker_overlap=0,
           holdout_new_completed=new_complete,holdout_new_incomplete=200-new_complete,combined_complete=100+new_complete,combined_rows=len(full),
           fetch_planned=200,fetch_attempted=attempted,consecutive_failure_stop=stopped,predeclared_primary='D institution_recent5_accel_pos versus hit15_by_d30',
           no_outcomes_used_for_selection=True,source_values_are_retrieved_historically=True,independent_official_amount_crosscheck='PENDING',
           historical_publication_PIT='NOT_VERIFIED',independent_official_samples_verified=0,official_audit_template_rows=len(aa),prospective_OOS=False,synthetic_rows_inserted=0,primary_evaluated=False,
           source_archives_sha256={k:p[k+'_sha256'] for k in ('r1d25','r1d26','r1d26f')},files_sha256=files,live_changed=False,orders_placed=0,strategy_ready=False)
    (root/'R1D26H_COLLECTION_MANIFEST.json').write_text(json.dumps(m,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    print('R1D26H_REAL_SOURCE_COVERAGE',json.dumps({k:m[k] for k in ('mode','holdout_new_completed','combined_complete','combined_rows','fetch_attempted','strategy_ready')},ensure_ascii=False),flush=True)
    return m

def eval_locked(p,collection_dir,r1d26e_zip,out,bootstrap=2000,perms=4000):
    protocol_ok(p)
    require(bootstrap==p['bootstrap_draws'] and perms==p['permutation_draws'],'RESAMPLING_PROTOCOL')
    directory=Path(collection_dir)
    m=json.loads((directory/'R1D26H_COLLECTION_MANIFEST.json').read_text(encoding='utf-8'))
    require(m['mode']=='collect' and m['holdout_new_completed']==200 and m['combined_rows']==6000 and not m['primary_evaluated'],'SOURCE_NOT_SEALED_FULL')
    for name,digest in m['files_sha256'].items():
        require(sh((directory/name).read_bytes())==digest,'SOURCE_INPUT_TAMPER_'+name)
    ev=pd.read_csv(directory/'R1D26H_FROZEN_NEW_200_HOLDOUT_ASOF_ONLY.csv',dtype={'code':str})
    flow=pd.read_csv(directory/'R1D26H_COMPLETE_300_PRIOR20_SIGNED_KRW_NOT_PIT.csv',dtype={'code':str,'foreign_net_krw':str,'institution_net_krw':str})
    require(len(ev)==200 and ev.code.nunique()==200 and len(flow)==6000,'INPUT_COUNT')
    newkeys=set(zip(ev.date,ev.code,ev.strategy))
    sub=flow[flow.apply(lambda r:(r['signal_date'],r['code'],r['strategy']) in newkeys,axis=1)]
    require(len(sub)==4000,'HOLDOUT4000')
    features=[]
    for r in ev.itertuples():
        x=sub[(sub.signal_date==r.date)&(sub.code==r.code)&(sub.strategy==r.strategy)].sort_values('trade_date')
        require(len(x)==20 and x.trade_date.nunique()==20 and x.trade_date.iloc[0]==r.pre20_from and x.trade_date.iloc[-1]==r.pre20_to,'NOT20')
        require((x.trade_date<r.date).all() and x.unit.eq('KRW').all() and x.event_source_hash.eq(r.event_source_hash).all(),'BAD_FLOW_PROVENANCE')
        inst=[i64(v) for v in x.institution_net_krw];foreign=[i64(v) for v in x.foreign_net_krw]
        # exact integer inequality: mean(last 5) > mean(previous 15)
        features.append(dict(date=r.date,code=r.code,strategy=r.strategy,year=int(r.year),event_source_hash=r.event_source_hash,
            institution_recent5_accel_pos=int(3*sum(inst[-5:])>sum(inst[:-5])),institution_net20_pos=int(sum(inst)>0),
            foreign_recent5_accel_pos=int(3*sum(foreign[-5:])>sum(foreign[:-5])),foreign_net20_pos=int(sum(foreign)>0),
            institution_recent5_minus_prev15_scaled_krw=int(3*sum(inst[-5:])-sum(inst[:-5]))))
    feat=pd.DataFrame(features)
    z=zopen(r1d26e_zip,p['r1d26e_sha256'])
    try: price=read_csv(z,PATH)
    finally:z.close()
    require(len(price)==10198 and not price.duplicated(['date','code','strategy']).any(),'PRICE_PATH_DRIFT')
    for h in ('hit5_by_d30','hit10_by_d30','hit15_by_d30'):
        require(price[h].notna().all() and price[h].isin([0,1]).all(),'PRICE_HIT_INVALID')
    joined=feat.merge(price[['date','code','strategy','hit5_by_d30','hit10_by_d30','hit15_by_d30','net_20bps_pct','first_hit15_bucket']],on=['date','code','strategy'],how='left',validate='one_to_one',indicator=True)
    joined['price_complete']=joined['_merge'].eq('both');joined=joined.drop(columns='_merge')
    require(len(joined)==200 and feat.code.nunique()==200,'HOLDOUT_JOIN')
    complete=joined[joined.price_complete].copy()
    require((complete.hit5_by_d30>=complete.hit10_by_d30).all() and (complete.hit10_by_d30>=complete.hit15_by_d30).all(),'HIT_MONOTONE')
    # Diagnostic missingness; never replace censored D30 with negative signal.
    frozen=[{'strategy':s,'selected':int((joined.strategy==s).sum()),'complete':int((complete.strategy==s).sum()),'censored':int(((joined.strategy==s)&(~joined.price_complete)).sum())} for s in (C,D)]
    target=complete[complete.strategy==D].copy();flag='institution_recent5_accel_pos';hit='hit15_by_d30'
    def diff(df):
        a=df[df[flag]==1];b=df[df[flag]==0]
        return float(a[hit].mean()-b[hit].mean()) if len(a) and len(b) else np.nan
    a=target[target[flag]==1];b=target[target[flag]==0]
    effect=diff(target)
    rng=np.random.default_rng(p['random_seed'])
    boots=[]
    strata=[group for _,group in target.groupby('year')]
    for _ in range(bootstrap):
        draw=pd.concat([group.iloc[rng.integers(0,len(group),len(group))] for group in strata],ignore_index=True)
        d=diff(draw)
        if np.isfinite(d):boots.append(d)
    ci=[float(np.quantile(boots,0.025)),float(np.quantile(boots,0.975))] if len(boots)>=bootstrap*.90 else [None,None]
    # Shuffle pre-signal exposure within calendar year, retaining outcome and group sizes.
    scores=[]
    if np.isfinite(effect):
        for _ in range(perms):
            perm=target.copy()
            for yr,inds in perm.groupby('year').groups.items():
                perm.loc[inds,flag]=rng.permutation(perm.loc[inds,flag].to_numpy())
            scores.append(diff(perm))
    pval=float((1+sum(abs(x)>=abs(effect) for x in scores if np.isfinite(x)))/(1+sum(np.isfinite(x) for x in scores))) if np.isfinite(effect) else None
    byyear=[]
    for yr,tt in target.groupby('year'):
        xx=tt[tt[flag]==1];yy=tt[tt[flag]==0]
        byyear.append(dict(year=int(yr),n=len(tt),accelerating=len(xx),not_accelerating=len(yy),hit15_accel=int(xx[hit].sum()),hit15_other=int(yy[hit].sum()),effect_pp=round(100*diff(tt),4) if len(xx) and len(yy) else None))
    group_stats=[]
    for st in (C,D):
        ss=complete[complete.strategy==st]
        for state in (0,1):
            xx=ss[ss[flag]==state]
            group_stats.append(dict(strategy=st,acceleration_state=state,n=len(xx),unique_tickers=xx.code.nunique(),hit5=int(xx.hit5_by_d30.sum()),hit10=int(xx.hit10_by_d30.sum()),hit15=int(xx.hit15_by_d30.sum()),hit15_pct=round(100*xx.hit15_by_d30.mean(),5) if len(xx) else None))
    primary=dict(hypothesis_source='R1D26G_exploration_unrelated_tickers',scope='200_NEW_HISTORICAL_EVENTS_ONLY_D_SUBGROUP',test_direction='accelerating_higher_+15pct_hit',
       strategy=D,flag=flag,threshold_pct=15,n_D_price_complete=len(target),n_accel=len(a),n_not_accel=len(b),
       hits_accel=int(a[hit].sum()),hits_not_accel=int(b[hit].sum()),p15_accel=float(a[hit].mean()) if len(a) else None,
       p15_not_accel=float(b[hit].mean()) if len(b) else None,difference_percentage_points=round(100*effect,5) if np.isfinite(effect) else None,
       year_stratified_ticker_disjoint_bootstrap_95pct_ci_difference_pp=[round(100*x,5) if x is not None else None for x in ci],
       year_stratified_permutation_two_sided_p=pval,
       bootstrap_draws=len(boots),permutation_draws=perms,year_coverage_both_levels=sum(int(s['accelerating']>0 and s['not_accelerating']>0) for s in byyear),
       censored_events_not_imputed=int((~joined.price_complete).sum()),official_KRX_independent_crosscheck='PENDING',historical_same_date_publication_PIT='NOT_VERIFIED',
       category='historical_independent_ticker_holdout_not_new_future_OOS',strategy_ready=False,orders_placed=0)
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    joined.to_csv(out/'R1D26H_200_TICKER_DISJOINT_FEATURES_AND_D30.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(group_stats).to_csv(out/'R1D26H_C_D_HIT5_10_15_BY_INST_ACCEL.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(byyear).to_csv(out/'R1D26H_D_PRIMARY_WITHIN_YEAR.csv',index=False,encoding='utf-8-sig')
    (out/'R1D26H_D_PRIMARY_LOCKED_CONTRAST.json').write_text(json.dumps(primary,indent=2,ensure_ascii=False,allow_nan=False)+'\n',encoding='utf-8')
    manifest=dict(version=VERSION,holdout_events=200,holdout_distinct_tickers=200,control_training_ticker_overlap=0,D30_complete=len(complete),D30_censored=int((~joined.price_complete).sum()),
       primary=primary,source_seal_sha256=sh((directory/'R1D26H_COLLECTION_MANIFEST.json').read_bytes()),
       source_amount_PIT='NOT_VERIFIED',historical_holdout_not_prospective_OOS=True,independent_official_KRX_crosscheck='PENDING',
       no_live_change=True,orders_placed=0,statistical_conclusion_not_preauthorized=True,strategy_ready=False)
    manifest['output_file_sha256']={x.name:sh(x.read_bytes()) for x in out.iterdir() if x.is_file()}
    (out/'R1D26H_QA_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    print('R1D26H_HOLDOUT_PRIMARY_RESULT_NOT_PIT_NOT_OOS',json.dumps(primary,ensure_ascii=False,allow_nan=False),flush=True)
    return manifest

def main():
    a=argparse.ArgumentParser();a.add_argument('--protocol',default=PROTO);a.add_argument('--mode',choices=['prepare','collect','evaluate'],required=True)
    a.add_argument('--r1d25');a.add_argument('--r1d26');a.add_argument('--r1d26f');a.add_argument('--r1d26e');a.add_argument('--out',default='reports/r1d26h/result/source');a.add_argument('--source-dir',default='reports/r1d26h/result/source');a.add_argument('--sleep-seconds',type=float,default=.4)
    arg=a.parse_args();p=json.loads(Path(arg.protocol).read_text(encoding='utf-8'));protocol_ok(p)
    if arg.mode=='evaluate':
        require(bool(arg.r1d26e),'E_PATH_REQUIRED')
        eval_locked(p,arg.source_dir,arg.r1d26e,arg.out,p['bootstrap_draws'],p['permutation_draws'])
    else:
        for k in ('r1d25','r1d26','r1d26f'):require(bool(getattr(arg,k)),k+'_REQUIRED')
        fetch=None
        if arg.mode=='collect':
            from pykrx import stock
            fetch=lambda start,end,code:stock.get_market_trading_value_by_date(start,end,code,on='순매수')
        collect({k:getattr(arg,k) for k in ('r1d25','r1d26','r1d26f')},p,arg.out,arg.mode,fetch,arg.sleep_seconds)
if __name__=='__main__':main()
