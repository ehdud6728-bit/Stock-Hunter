#!/usr/bin/env python3
"""R1-D27C: frozen R1-D27A cohort / 60-session gross investor flow study.

Historical RESEARCH_ONLY; no orders, production scanner, live rank or model change.
The old R1-D27A 300 events are selected BEFORE reading any post-signal path.
Original unadjusted KRX daily and investor flow are retrospective, NOT PIT certified.
"""
from __future__ import annotations
import argparse, collections, csv, gzip, hashlib, io, json, math, os, pathlib, shutil, time, zipfile
import pandas as pd

VERSION='R1D27C_FROZEN300_60SESS_GROSS_BUY_SELL_NET_VALUE_VOLUME_V1'
A_FROZEN='reports/r1d27a/result/census/R1D27A_FROZEN_FLOW_SAMPLE_OUTCOME_BLIND.csv'
A_ASOF='reports/r1d27a/result/census/R1D27A_ALLMARKET_BB203040_ASOF_ONLY.csv.gz'
A_PATHS='reports/r1d27a/result/evaluate/R1D27A_ALLMARKET_D5_10_15_20_30_OUTCOMES.csv.gz'
A_CENSUS_MAN='reports/r1d27a/result/census/R1D27A_PHASE_MANIFEST.json'
A_EVAL_MAN='reports/r1d27a/result/evaluate/R1D27A_PHASE_MANIFEST.json'
KEY=['date','code','bb_period','event_type']
ACTORS=['기관합계','외국인합계','개인','기타법인']
PERIODS=(5,10,20,40,60)
BRANCHES=('VALUE','VOLUME')
SIDES=('매수','매도','순매수')
FROZEN='R1D27C_FROZEN_OUTCOME_BLIND_EVENTS.csv'
FLOW='R1D27C_INVESTOR_DAILY_60_SESSIONS.csv.gz'
AUDIT='R1D27C_FLOW_PROVIDER_AUDIT.csv'
DAILY='R1D27C_SIGNED_BUY_SELL_60_SESSION_FEATURES.csv'
JOINED='R1D27C_HISTORICAL_D5_D30_JOIN_EXPLORATORY.csv'
SUMMARY='R1D27C_PRESPECIFIED_GROUP_COMPARISONS.csv'
MATCHED='R1D27C_YEAR_BAND_EVENT_STRATUM_MATCHED_CONTRASTS.csv'

class AuditError(ValueError):pass
def check(cond, reason):
    if not cond:raise AuditError('R1D27C_'+str(reason))
def sha(b):return hashlib.sha256(b).hexdigest()
def read_csv_bytes(b,compressed=False,**kwargs):
    if compressed:b=gzip.decompress(b)
    return pd.read_csv(io.BytesIO(b),dtype={'code':str,'date':str,'trade_date':str,'bb_period':str},low_memory=False,**kwargs)
def emit(path, rows, cols=None):
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    if isinstance(rows,pd.DataFrame):
        if path.suffix=='.gz':rows.to_csv(path,index=False,encoding='utf-8-sig',compression='gzip')
        else:rows.to_csv(path,index=False,encoding='utf-8-sig')
    else:
        with (gzip.open(path,'wt',encoding='utf-8-sig',newline='') if path.suffix=='.gz' else path.open('w',encoding='utf-8-sig',newline='')) as f:
            w=csv.DictWriter(f,fieldnames=cols or list(rows[0]) if rows else cols or []);w.writeheader();w.writerows(rows)
def dump(path,o):
    pathlib.Path(path).write_text(json.dumps(o,ensure_ascii=False,indent=2,sort_keys=True,default=str)+'\n',encoding='utf-8')
def manifest(path,phase,**kw):
    root=pathlib.Path(path);m=dict(version=VERSION,phase=phase,research_only=True,live_changed=False,
      orders_placed=0,strategy_ready=False,prospective_OOS=False,PIT_certified=False,
      investor_KRX_official_crosscheck='PENDING',data_provenance='KRX_VIA_PYKRX_RETROSPECTIVE_NOT_PIT',**kw)
    m['files_sha256']={f.name:sha(f.read_bytes()) for f in sorted(root.iterdir()) if f.is_file() and f.name!='R1D27C_PHASE_MANIFEST.json'}
    dump(root/'R1D27C_PHASE_MANIFEST.json',m);return m
def verify(root,phase):
    root=pathlib.Path(root);m=json.loads((root/'R1D27C_PHASE_MANIFEST.json').read_text(encoding='utf-8'))
    check(m['version']==VERSION and m['phase']==phase,'PHASE_OR_VERSION')
    check(m['research_only'] and not m['live_changed'] and not m['orders_placed'],'RESEARCH_BOUNDARY')
    for f,d in m['files_sha256'].items():check(sha((root/f).read_bytes())==d,'TAMPER_'+f)
    return m

class SourceReader:
    """Read a pinned original Actions artifact ZIP or its GitHub-extracted file tree.

    GitHub's download-artifact unpacks and loses the outer ZIP byte identity; the
    workflow pins the original GitHub artifact digest BEFORE download, and this
    reader verifies each extracted data payload against the immutable inner manifest.
    """
    def __init__(self,path,protocol):
        p=pathlib.Path(path);self.dir=p if p.is_dir() else None;self.zip=None
        if self.dir is None:
            b=p.read_bytes()
            check(sha(b)==protocol['r1d27a_artifact_sha256'],'A27_OUTER_ZIP_SHA')
            self.zip=zipfile.ZipFile(io.BytesIO(b))
        c=json.loads(self.read(A_CENSUS_MAN))
        e=json.loads(self.read(A_EVAL_MAN))
        check(c['phase']=='census' and c['n_events']==133180 and c['flow_sample_selected']==300,'A27_ORIGINAL_CENSUS')
        check(e['phase']=='evaluate' and e['n_d30_completed']==132122,'A27_ORIGINAL_EVAL')
        for member,mani in [(A_FROZEN,c),(A_ASOF,c),(A_PATHS,e)]:
            check(sha(self.read(member))==mani['files_sha256'][member.split('/')[-1]],'A27_INNER_SOURCE_SHA_'+member.split('/')[-1])
    def read(self,name):
        return (self.dir/name).read_bytes() if self.dir is not None else self.zip.read(name)

def get_fixed_source(a_zip,protocol,calendar):
    z=SourceReader(a_zip,protocol);cman=json.loads(z.read(A_CENSUS_MAN))
    check(cman['phase']=='census' and cman['n_events']==133180 and cman['flow_sample_selected']==300,'R1D27A_CENSUS_COUNT')
    fm=z.read(A_FROZEN);check(sha(fm)==cman['files_sha256'][A_FROZEN.split('/')[-1]],'ORIGINAL_FROZEN_SHA')
    f=read_csv_bytes(fm);check(len(f)==300 and f['code'].nunique()==300,'FROZEN300_UNIQUE')
    check(f[KEY].duplicated().sum()==0,'DUP_EVENT');check(f['select_hash'].nunique()==300,'DUP_SELECT_HASH')
    f['signal_ix']=pd.to_numeric(f['signal_ix'],errors='raise').astype(int)
    for row in f.itertuples():
        t=row.signal_ix
        check(60<=t<len(calendar) and calendar[t]==row.date,'CALENDAR_EVENT_MISMATCH')
        check(calendar[t-20]==row.pre20_from and calendar[t-1]==row.pre20_to,'R1D27A_PRE20_MISMATCH')
        check(row.year==int(row.date[:4]) and row.bb_period in ('20','30','40') and row.event_type in ('BREAK','RECLAIM'),'STRATUM_DRIFT')
    check(f.groupby(['year','bb_period','event_type']).size().eq(10).all() and len(f.groupby(['year','bb_period','event_type']))==30,'STRATUM_10_EACH')
    return z,f.sort_values('select_hash').reset_index(drop=True)

def subset(f,limit):
    if limit=='pilot':
        s=f.sort_values('select_hash').groupby(['year','bb_period','event_type'],sort=True,group_keys=False).head(1)
        check(len(s)==30,'PILOT30')
        return s.sort_values('select_hash').reset_index(drop=True)
    check(limit=='full','SCOPE_INVALID')
    return f.sort_values('select_hash').reset_index(drop=True)

def freeze(a_zip,krx_root,out,protocol,scope):
    from research_formulas import formula66_r1d6_oos_holdout as d6
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    cal,_=d6.index_asof(krx_root)
    check(len(cal)==1907 and cal[0]=='2019-01-02','KRX_CALENDAR')
    z,all_f=get_fixed_source(a_zip,protocol,cal)
    f=subset(all_f,scope).copy()
    f['pre60_from']=[cal[int(t)-60] for t in f.signal_ix]
    f['pre60_to']=[cal[int(t)-1] for t in f.signal_ix]
    emit(out/FROZEN,f)
    n=len(f)
    return manifest(out,'freeze',scope=scope,n_frozen=n,original_total=300,
        original_R1D27A_artifact_sha256=protocol['r1d27a_artifact_sha256'],
        original_R1D27A_selection_file_sha256=sha(z.read(A_FROZEN)),
        frozen_event_sha256=sha((out/FROZEN).read_bytes()),
        freeze_before_opening_outcomes=True,lookback_sessions=60,
        expected_source_rows_per_actor=n*60,source_start=cal[0],source_end=cal[-1])

def parse_provider(df,expected,actors=ACTORS):
    check(isinstance(df,pd.DataFrame) and not df.empty,'PROVIDER_EMPTY')
    check(set(actors).issubset(df.columns) and '전체' in df.columns,'PROVIDER_COLUMNS')
    check(not df.index.has_duplicates,'PROVIDER_DUP_INDEX')
    got={}
    for day,r in df.iterrows():
        k=pd.Timestamp(day).date().isoformat()
        check(k not in got,'DUP_DAY')
        vals={}
        for col in [*actors,'전체']:
            v=r[col]
            if isinstance(v,str):v=v.replace(',','').strip()
            try:
                q=float(v)
                check(math.isfinite(q) and q.is_integer() and abs(q)<1e19,'PROVIDER_NONINTEGER')
            except (ValueError,TypeError,OverflowError) as ex:raise AuditError('R1D27C_PROVIDER_NONINTEGER') from ex
            vals[col]=int(q)
        got[k]=vals
    check(set(got)==set(expected),'PROVIDER_60_DAY_CALENDAR_MISMATCH')
    return got

def verify_six(feeds,expected):
    # Each input requires 60 exact unique KRX sessions and 4 grouped participants.
    parsed={(kind,side):parse_provider(feeds[kind,side],expected) for kind in BRANCHES for side in SIDES}
    check(any(parsed['VALUE','매수'][day]['전체']>0 and parsed['VOLUME','매수'][day]['전체']>0 for day in expected),
          'ZERO_ALL_GROSS_PROVIDER')
    out=[]
    for day in expected:
        # 전체: gross buy and sell volume should balance and net should be zero.
        for kind in BRANCHES:
            b=parsed[kind,'매수'][day];s=parsed[kind,'매도'][day];n=parsed[kind,'순매수'][day]
            check(b['전체']>=0 and s['전체']>=0,'TOTAL_NEGATIVE_GROSS')
            check(abs(b['전체']-s['전체'])<=max(2,abs(b['전체'])*0.00001),'TOTAL_BUY_SELL_BALANCE_'+kind)
            check(abs(n['전체'])<=max(2,abs(b['전체'])*0.00001),'TOTAL_NET_BALANCE_'+kind)
            for actor in ACTORS:
                bv=b[actor];sv=s[actor];nv=n[actor]
                check(bv>=0 and sv>=0,'NEGATIVE_GROSS_'+actor)
                check(abs(bv-sv-nv)<=max(2,(bv+sv)*0.000001),'NET_ARITHMETIC_'+kind+'_'+actor)
            # Group totals may include accounting categories/rounding, validate with tolerance
            check(abs(sum(b[a] for a in ACTORS)-b['전체'])<=max(1000,b['전체']*.005),'BUY_GROUP_TOTAL_'+kind)
            check(abs(sum(s[a] for a in ACTORS)-s['전체'])<=max(1000,s['전체']*.005),'SELL_GROUP_TOTAL_'+kind)
        for actor in ACTORS:
            row=dict(trade_date=day,actor=actor)
            for kind,suff in [('VALUE','krw'),('VOLUME','shares')]:
                for side,name in [('매수','buy'),('매도','sell'),('순매수','net')]:
                    row[f'{name}_{suff}']=parsed[kind,side][day][actor]
                row[f'total_{suff}']=parsed[kind,'매수'][day]['전체']
            out.append(row)
    check(len(out)==60*len(ACTORS),'VERIFY_SIX_ROWS')
    return out

def get_flow(start,end,code,fetch=None,sleep_calls=0.2):
    if fetch is None:
        from pykrx import stock
        fetch=lambda kind,side,s,e,c: (stock.get_market_trading_value_by_date(s,e,c,on=side) if kind=='VALUE' else stock.get_market_trading_volume_by_date(s,e,c,on=side))
    feeds={}
    for kind in BRANCHES:
        for side in SIDES:
            feeds[kind,side]=fetch(kind,side,start,end,code)
            if sleep_calls:time.sleep(sleep_calls)
    return feeds

def collect(freeze_dir,krx_root,out,protocol,fetch=None,sleep_events=0.65,max_fails=3):
    from research_formulas import formula66_r1d6_oos_holdout as d6
    fm=verify(freeze_dir,'freeze');out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    f=pd.read_csv(pathlib.Path(freeze_dir)/FROZEN,dtype={'code':str,'bb_period':str,'date':str,'pre60_from':str,'pre60_to':str})
    cal,_=d6.index_asof(krx_root)
    check(len(f)==fm['n_frozen'],'FROZEN_COUNT')
    records=[];audit=[];ok=0;consec=0;aborted=False
    try:
        for e in f.itertuples(index=False):
            t=int(e.signal_ix);days=cal[t-60:t]
            check(len(days)==60 and days[0]==e.pre60_from and days[-1]==e.pre60_to,'EVENT_SOURCE_TIME')
            reason='';status=''
            try:
                frames=get_flow(days[0].replace('-',''),days[-1].replace('-',''),e.code,fetch)
                parsed=verify_six(frames,days)
                for q in parsed:
                    records.append({**dict(zip(KEY,[e.date,e.code,e.bb_period,e.event_type])),**q})
                status='COMPLETE_60_GROSS_NET_BOTH_UNITS_NOT_PIT';ok+=1;consec=0
            except Exception as err:
                reason=type(err).__name__ if not isinstance(err,AuditError) else str(err).removeprefix('R1D27C_')
                status='SOURCE_QUARANTINED_NO_IMPUTATION';consec+=1
            audit.append(dict(date=e.date,code=e.code,bb_period=e.bb_period,event_type=e.event_type,
                              status=status,reason_code=reason,expected_days=60,source_complete=int(status.startswith('COMPLETE'))))
            if len(audit)%5==0:print('R1D27C_SOURCE',len(audit),'/',len(f),'complete',ok,'consecutive_errors',consec,flush=True)
            if consec>=max_fails:
                aborted=True;break
            if sleep_events:time.sleep(sleep_events)
    finally:
        cols=[*KEY,'trade_date','actor','buy_krw','sell_krw','net_krw','total_krw',
               'buy_shares','sell_shares','net_shares','total_shares']
        emit(out/FLOW,records,cols)
        emit(out/AUDIT,audit,['date','code','bb_period','event_type','status','reason_code','expected_days','source_complete'])
        m=manifest(out,'collect',scope=fm['scope'],n_frozen=len(f),n_attempted=len(audit),n_source_complete=ok,
          n_source_incomplete=len(audit)-ok,n_unattempted=len(f)-len(audit),n_daily_actor_rows=len(records),
          consecutive_failure_abort=aborted,source_frozen_sha256=fm['files_sha256'][FROZEN],
          gross_net_both_units_validation=True,unavailable_is_not_zero=True)
        print('R1D27C_SOURCE_MANIFEST',json.dumps({k:m[k] for k in ('n_frozen','n_attempted','n_source_complete','n_unattempted','consecutive_failure_abort')},ensure_ascii=False),flush=True)
    return m

def actor_features(rows):
    # 60 sessions, exactly 1 row/day/actor. Input contains no future bars.
    seq=rows.sort_values('trade_date');check(len(seq)==60 and seq.trade_date.nunique()==60,'ACTOR_HISTORY_NOT60')
    out={}
    for k in PERIODS:
        q=seq.tail(k)
        for kind,suffix in [('krw','krw'),('shares','shares')]:
            b=int(q[f'buy_{suffix}'].sum());s=int(q[f'sell_{suffix}'].sum());n=int(q[f'net_{suffix}'].sum())
            t=int(q[f'total_{suffix}'].sum());check(b-s==n,'FEATURE_NET_PARITY')
            out[f'net{k}_{suffix}']=n;out[f'buy{k}_{suffix}']=b;out[f'sell{k}_{suffix}']=s
            out[f'net{k}_total_pct_{suffix}']=100*n/t if t else None
            out[f'buy_dominance{k}_pct_{suffix}']=100*n/(b+s) if b+s else None
        out[f'positive_days{k}']=int((q.net_krw>0).sum())
        out[f'negative_days{k}']=int((q.net_krw<0).sum())
    for suffix in ('krw','shares'):
        l=seq[f'net_{suffix}'].to_numpy()
        out[f'net_recent5_vs_prev5_{suffix}']=int(l[-5:].sum()-l[-10:-5].sum())
        out[f'net_recent10_vs_prev10_{suffix}']=int(l[-10:].sum()-l[-20:-10].sum())
        out[f'net_recent20_vs_prev20_{suffix}']=int(l[-20:].sum()-l[-40:-20].sum())
        out[f'net_60to41_{suffix}']=int(l[-60:-40].sum())
        out[f'net_40to21_{suffix}']=int(l[-40:-20].sum())
        out[f'net_20to11_{suffix}']=int(l[-20:-10].sum())
        out[f'net_10to6_{suffix}']=int(l[-10:-5].sum())
        out[f'net_5to1_{suffix}']=int(l[-5:].sum())
    return out

def analyze(freeze_dir,source_dir,a_zip,out,protocol):
    fm=verify(freeze_dir,'freeze');sm=verify(source_dir,'collect')
    check(fm['files_sha256'][FROZEN]==sm['source_frozen_sha256'],'FREEZE_SOURCE_SHA')
    check(sm['n_source_complete']>0,'NO_SOURCE_COMPLETE')
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    # Source seal verified; only NOW open historical forward-price outcomes.
    z,_=get_fixed_source_without_calendar(a_zip,protocol)
    f=pd.read_csv(pathlib.Path(freeze_dir)/FROZEN,dtype={'code':str,'bb_period':str,'date':str})
    rows=pd.read_csv(pathlib.Path(source_dir)/FLOW,dtype={'code':str,'bb_period':str,'date':str,'trade_date':str})
    check(not rows.empty,'FLOW_DATA_EMPTY')
    def event_key(x):return tuple(str(x[v]) for v in KEY)
    complete=set()
    details=[];cols=['buy_krw','sell_krw','net_krw','total_krw','buy_shares','sell_shares','net_shares','total_shares']
    for (day,code,bb,typ,actor),g in rows.groupby([*KEY,'actor'],sort=False):
        check(len(g)==60 and g.trade_date.nunique()==60,'SOURCE_NOT60')
        rr=dict(zip(KEY,[day,code,bb,typ]));rr['actor']=actor;rr.update(actor_features(g))
        details.append(rr);complete.add((day,code,bb,typ))
    check(len(details)==len(complete)*4 and len(complete)==sm['n_source_complete'],'FEATURE_COMPLETE_ACTORS')
    emit(out/DAILY,details)
    by_actor=collections.defaultdict(dict)
    for r in details:by_actor[event_key(r)][r['actor']]=r
    hypotheses=[]
    for key,actors in by_actor.items():
        a=actors['기관합계'];b=actors['외국인합계'];p=actors['개인']
        obj=dict(zip(KEY,key))
        for k in (5,10,20,40,60):
            obj[f'both_inst_foreign_net{k}_pos']=int(a[f'net{k}_krw']>0 and b[f'net{k}_krw']>0)
            obj[f'inst_foreign_pos_personal_neg{k}']=int((a[f'net{k}_krw']+b[f'net{k}_krw'])>0 and p[f'net{k}_krw']<0)
            obj[f'inst_foreign_net{k}_total_pct']=a[f'net{k}_total_pct_krw']+b[f'net{k}_total_pct_krw'] if a[f'net{k}_total_pct_krw'] is not None and b[f'net{k}_total_pct_krw'] is not None else None
        obj['institution_net10_accel_pos']=int(a['net_recent10_vs_prev10_krw']>0)
        obj['foreign_net10_accel_pos']=int(b['net_recent10_vs_prev10_krw']>0)
        obj['both_net10_accel_pos']=int(obj['institution_net10_accel_pos']==1 and obj['foreign_net10_accel_pos']==1)
        hypotheses.append(obj)
    flag_df=pd.DataFrame(hypotheses)
    # Read only frozen original ASOF and historical outcome AFTER flow source features sealed.
    asof=read_csv_bytes(z.read(A_ASOF),True)[KEY+['BB20_width_pct','BB30_width_pct','BB40_width_pct','BB20_width_change5_pct','BB30_width_change5_pct','BB40_width_change5_pct',
        'MA20_above','MA40_above','MA60_above','MA112_above','MA224_above','MA448_above','joint_index_20d_regime']]
    px=read_csv_bytes(z.read(A_PATHS),True)
    # All joins are exact event identity, no ticker aliases / no duplicate event rows.
    sample=f[KEY+['year','signal_ix']].merge(flag_df,on=KEY,how='left',validate='one_to_one')
    sample=sample.merge(asof,on=KEY,how='left',validate='one_to_one')
    joined=sample.merge(px,on=KEY,how='left',validate='one_to_many')
    check(len(joined)==len(f)*5 and not joined['horizon'].isna().any(),'HORIZON_JOIN_CARDINALITY')
    joined['source_complete']=joined[KEY].apply(lambda row: tuple(row.astype(str)) in complete,axis=1)
    check(joined.source_complete.sum()==len(complete)*5,'SOURCE_HORIZON_CARDINALITY')
    joined['price_complete']=joined.status_out.eq('PRICE_PATH_COMPLETE_NOT_FILL_CERTIFIED')
    emit(out/JOINED,joined)
    stats=[]
    for flag in ['both_inst_foreign_net20_pos','inst_foreign_pos_personal_neg20',
                 'institution_net10_accel_pos','foreign_net10_accel_pos','both_net10_accel_pos',
                 'both_inst_foreign_net60_pos','inst_foreign_pos_personal_neg60']:
        for h in (5,10,15,20,30):
            p=joined[(joined.source_complete)&(joined.price_complete)&(joined.horizon==h)]
            for val in (0,1):
                grp=p[p[flag]==val];v=pd.to_numeric(grp.net_20bps_pct,errors='coerce')
                stats.append(dict(flag=flag,flag_value=val,horizon=h,n=len(grp),
                   mean_net_pct=round(float(v.mean()),6) if len(grp) else '',
                   median_net_pct=round(float(v.median()),6) if len(grp) else '',
                   win_rate_pct=round(100*float((v>0).mean()),6) if len(grp) else '',
                   hit15_pct=round(100*float(grp.hit15.mean()),6) if len(grp) else '',
                   sample_scope=fm['scope'],not_confirmatory=True))
    emit(out/SUMMARY,stats)
    # Historical descriptive matched strata: each year × BB period × event type has
    # at most 10 frozen cases; no flags are tuned using these outcomes.
    matched=[]
    for flag in ['both_inst_foreign_net20_pos','inst_foreign_pos_personal_neg20',
                 'institution_net10_accel_pos','foreign_net10_accel_pos','both_net10_accel_pos',
                 'both_inst_foreign_net60_pos','inst_foreign_pos_personal_neg60']:
        for h in (5,10,15,20,30):
            sub=joined[(joined.source_complete)&(joined.price_complete)&(joined.horizon==h)].copy()
            sub['net_20bps_pct']=pd.to_numeric(sub.net_20bps_pct,errors='coerce')
            diffs=[]
            for strata,g in sub.groupby(['year','bb_period','event_type']):
                t=g[g[flag]==1];c=g[g[flag]==0]
                if len(t)<2 or len(c)<2:continue
                diffs.append((float(t.net_20bps_pct.mean()-c.net_20bps_pct.mean()),
                    float(100*((t.net_20bps_pct>0).mean()-(c.net_20bps_pct>0).mean())),len(t)+len(c)))
            matched.append(dict(flag=flag,horizon=h,year_band_event_matched_strata=len(diffs),
                events_in_matched_strata=sum(r[2] for r in diffs),
                mean_equal_stratum_return_delta_pp=round(sum(d[0] for d in diffs)/len(diffs),6) if diffs else '',
                mean_equal_stratum_win_delta_pp=round(sum(d[1] for d in diffs)/len(diffs),6) if diffs else '',
                status='DESCRIPTIVE_NOT_PROSPECTIVE_OOS' if diffs else 'INSUFFICIENT_MATCHED_STRATA'))
    emit(out/MATCHED,matched)
    return manifest(out,'analyze',scope=fm['scope'],n_frozen=len(f),n_flow_complete=len(complete),
        n_event_horizon_join=len(joined),n_complete_d30=int(((joined.horizon==30)&joined.price_complete&joined.source_complete).sum()),
        price_source_R1D27A_artifact_sha256=protocol['r1d27a_artifact_sha256'],
        historical_exploratory=True,no_new_prospective_OOS=True,no_same_sample_rule_selection=True,
        source_frozen_before_outcome_open=True)

def get_fixed_source_without_calendar(a_zip,protocol):
    z=SourceReader(a_zip,protocol);m=json.loads(z.read(A_CENSUS_MAN))
    check(m['n_events']==133180 and m['flow_sample_selected']==300,'R1D27A_POPULATION')
    f=read_csv_bytes(z.read(A_FROZEN));check(len(f)==300,'R1D27A_SAMPLE')
    return z,f


# RECOVERY ADDENDUM: checkpoint raw input is validated without relying on the
# success/failure of the old workflow. Never resample, fill missing, or read
# future returns during these requests.
PINNED_FULL_FREEZE_SHA256='c6dcccdc5ecd5d98d3f1ceae441c34814c062710118148832271ff9191474093'


def audit_checkpoint(checkpoint_root, calendar):
    root=pathlib.Path(checkpoint_root)/'reports/r1d27c/result'
    fm=verify(root/'freeze','freeze')
    sm=verify(root/'flow','collect')
    check(fm['scope']=='full' and fm['n_frozen']==300,'RESUME_NOT_FROZEN300')
    check(fm['files_sha256'][FROZEN]==PINNED_FULL_FREEZE_SHA256,'RESUME_FREEZE_IDENTITY_DRIFT')
    check(sm['scope']=='full' and sm['n_frozen']==300,'RESUME_SOURCE_NOT_FULL')
    check(sm['source_frozen_sha256']==fm['files_sha256'][FROZEN],'RESUME_MISMATCH_FROZEN')
    check(sm['gross_net_both_units_validation'] and sm['unavailable_is_not_zero'],'RESUME_SOURCE_QA_FLAGS')
    frozen=pd.read_csv(root/'freeze'/FROZEN,dtype={'date':str,'code':str,'bb_period':str,'event_type':str})
    check(len(frozen)==300 and not frozen[KEY].duplicated().any(),'RESUME_FROZEN_COUNT')
    keyset=set(tuple(x) for x in frozen[KEY].itertuples(index=False,name=None))
    check(len(keyset)==300,'RESUME_FROZEN_KEYS')
    fidx={tuple(str(getattr(q,k)) for k in KEY):q for q in frozen.itertuples(index=False)}
    archive_audit=pd.read_csv(root/'flow'/AUDIT,dtype=str).fillna('')
    check(not archive_audit[KEY].duplicated().any(),'RESUME_DUP_AUDIT')
    audited={tuple(x[k] for k in KEY):x.to_dict() for _,x in archive_audit.iterrows()}
    check(set(audited).issubset(keyset),'RESUME_AUDIT_KEYS')
    archived=pd.read_csv(root/'flow'/FLOW,dtype={'date':str,'code':str,'bb_period':str,'event_type':str,'trade_date':str,'actor':str},compression='gzip')
    check(len(archived)==sm['n_daily_actor_rows'],'RESUME_RAW_ROW_COUNT')
    check(not archived[[*KEY,'trade_date','actor']].duplicated().any(),'RESUME_DUP_DAILY_ACTOR')
    check(set(tuple(x) for x in archived[KEY].itertuples(index=False,name=None)).issubset(keyset),'RESUME_DAILY_KEYS')
    num=['buy_krw','sell_krw','net_krw','total_krw','buy_shares','sell_shares','net_shares','total_shares']
    for col in num:
        x=pd.to_numeric(archived[col],errors='raise')
        check(x.notna().all() and (x%1==0).all(),'RESUME_NONINTEGER_'+col)
        if col.startswith(('buy_','sell_','total_')):check((x>=0).all(),'RESUME_NEGATIVE_'+col)
        archived[col]=x.astype('int64')
    for suffix in ('krw','shares'):
        check((archived['buy_'+suffix]-archived['sell_'+suffix]==archived['net_'+suffix]).all(),'RESUME_NET_MISMATCH_'+suffix)
    entries={};keys=[]
    for key,g in archived.groupby(KEY,sort=False):
        key=tuple(str(x) for x in key)
        check(key in fidx and len(g)==240 and g.trade_date.nunique()==60,'RESUME_ACTOR_CARDINALITY')
        check(set(g.actor)==set(ACTORS) and (g.groupby('trade_date').actor.nunique()==4).all(),'RESUME_ACTOR_MISSING')
        r=fidx[key];t=int(r.signal_ix)
        expected=calendar[t-60:t]
        check(len(expected)==60 and set(g.trade_date)==set(expected) and min(g.trade_date)==r.pre60_from and max(g.trade_date)==r.pre60_to,'RESUME_DATES')
        for day,v in g.groupby('trade_date'):
            for suffix in ('krw','shares'):
                total=v['total_'+suffix].to_numpy()
                check(len(set(map(int,total)))==1,'RESUME_DAY_TOTAL_MISMATCH')
                for gross in ('buy_','sell_'):
                    agg=int(v[gross+suffix].sum());ref=int(total[0])
                    check(abs(agg-ref)<=max(1000,ref*.005),'RESUME_ACTOR_SUM_'+gross+suffix)
        check(key in audited and audited[key]['status'].startswith('COMPLETE'),'RESUME_AUDIT_COMPLETED')
        entries[key]=g
        keys.append(key)
    check(len(entries)==sm['n_source_complete'],'RESUME_COMPLETE_COUNT')
    check(sum(x['status'].startswith('COMPLETE') for x in audited.values())==len(entries),'RESUME_AUDIT_COMPLETE_COUNT')
    check(len(audited)==sm['n_attempted'],'RESUME_AUDIT_SIZE')
    return root,frozen,entries,audited,sm


def collect_resume(checkpoint_root,krx_root,out,protocol,batch_limit=30,fetch=None,sleep_events=0.9,max_fails=3,skip_known_calendar_mismatch=False):
    check(1<=batch_limit<=40,'RESUME_BATCH_LIMIT')
    check(0<=sleep_events<=5,'RESUME_SLEEP')
    from research_formulas import formula66_r1d6_oos_holdout as d6
    cal,_=d6.index_asof(krx_root)
    root,frozen,cached,prior_audit,old=audit_checkpoint(checkpoint_root,cal)
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    # Copy frozen bytes exactly; no new selectors, no outcomes.
    freeze_out=out.parent/'freeze';freeze_out.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(root/'freeze'/FROZEN,freeze_out/FROZEN)
    shutil.copyfile(root/'freeze'/'R1D27C_PHASE_MANIFEST.json',freeze_out/'R1D27C_PHASE_MANIFEST.json')
    check(verify(freeze_out,'freeze')['files_sha256'][FROZEN]==PINNED_FULL_FREEZE_SHA256,'RESUME_COPY_SHA')
    records=[]
    for g in cached.values():records.extend(g.to_dict('records'))
    audit=prior_audit.copy()
    n_started=0;n_success=0;consec=0;aborted=False
    # Do not let cached errors falsely count as current consecutive failures.
    for e in frozen.itertuples(index=False):
        key=tuple(str(getattr(e,k)) for k in KEY)
        if key in cached:continue
        # Existing provider-calendar mismatch remains quarantined in AUTO mode;
        # it is not a zero-flow observation and must not monopolize every batch.
        prev=prior_audit.get(key, {})
        if (skip_known_calendar_mismatch and
            prev.get('status')=='SOURCE_QUARANTINED_NO_IMPUTATION' and
            prev.get('reason_code')=='PROVIDER_60_DAY_CALENDAR_MISMATCH'):
            continue
        if n_started>=batch_limit:break
        t=int(e.signal_ix);days=cal[t-60:t]
        check(len(days)==60 and days[0]==e.pre60_from and days[-1]==e.pre60_to,'RESUME_EVENT_DATES')
        n_started+=1
        try:
            frames=get_flow(days[0].replace('-',''),days[-1].replace('-',''),e.code,fetch)
            parsed=verify_six(frames,days)
            records.extend([{**dict(zip(KEY,key)),**q} for q in parsed])
            status='COMPLETE_60_GROSS_NET_BOTH_UNITS_NOT_PIT';reason='';n_success+=1;consec=0
        except Exception as err:
            reason=type(err).__name__ if not isinstance(err,AuditError) else str(err).removeprefix('R1D27C_')
            status='SOURCE_QUARANTINED_NO_IMPUTATION';consec+=1
        audit[key]=dict(zip(KEY,key),status=status,reason_code=reason,expected_days=60,source_complete=int(status.startswith('COMPLETE')))
        if n_started%5==0:print('R1D27C_RESUME_BATCH',n_started,'of',batch_limit,'new_complete',n_success,'total_complete',len(cached)+n_success,'consecutive_errors',consec,flush=True)
        if consec>=max_fails:aborted=True;break
        if sleep_events:time.sleep(sleep_events)
    cols=[*KEY,'trade_date','actor','buy_krw','sell_krw','net_krw','total_krw','buy_shares','sell_shares','net_shares','total_shares']
    emit(out/FLOW,records,cols)
    emit(out/AUDIT,list(audit.values()),[*KEY,'status','reason_code','expected_days','source_complete'])
    n_all=len(cached)+n_success
    check(len(records)==n_all*240,'RESUME_OUTPUT_ACTOR_ROWS')
    return manifest(out,'collect',scope='full',n_frozen=300,n_attempted=len(audit),n_source_complete=n_all,
        n_source_incomplete=len(audit)-n_all,n_unattempted=300-len(audit),n_daily_actor_rows=len(records),
        consecutive_failure_abort=aborted,source_frozen_sha256=PINNED_FULL_FREEZE_SHA256,
        gross_net_both_units_validation=True,unavailable_is_not_zero=True,checkpoint_only=n_all<240,
        batch_limit=batch_limit,batch_attempted=n_started,batch_new_success=n_success,n_cached_reused=len(cached),
        permanent_calendar_mismatch_quarantine=skip_known_calendar_mismatch,
        checkpoint_source_complete_before=old['n_source_complete'],checkpoint_source_digest=old['files_sha256'][FLOW],
        status='SOURCE_CHECKPOINT_NOT_FINAL_RESEARCH' if n_all<240 else 'FLOW_SOURCE_COVERAGE_REACHED_FOR_EXPLORATORY_ANALYSIS')

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--mode',required=True,choices=('freeze','collect','analyze','resume'))
    ap.add_argument('--a-zip',default='reports/r1d27c/input/R1D27A_ARTIFACT.zip')
    ap.add_argument('--krx-root',default='reports/r1d27c/input/krx')
    ap.add_argument('--freeze-dir',default='reports/r1d27c/result/freeze')
    ap.add_argument('--flow-dir',default='reports/r1d27c/result/flow')
    ap.add_argument('--out',required=True);ap.add_argument('--scope',choices=('pilot','full'),default='pilot')
    ap.add_argument('--protocol',default='R1D27C_PREDECLARED_PROTOCOL.json')
    ap.add_argument('--checkpoint-root',default='reports/r1d27c/checkpoint')
    ap.add_argument('--batch-limit',type=int,default=30)
    args=ap.parse_args();p=json.loads(pathlib.Path(args.protocol).read_text(encoding='utf-8'))
    check(p['version']==VERSION and p['lookback_sessions']==60 and p['frozen_population']==300,'PROTOCOL')
    if args.mode=='freeze':v=freeze(args.a_zip,args.krx_root,args.out,p,args.scope)
    elif args.mode=='collect':v=collect(args.freeze_dir,args.krx_root,args.out,p)
    elif args.mode=='resume':v=collect_resume(args.checkpoint_root,args.krx_root,args.out,p,batch_limit=args.batch_limit)
    else:v=analyze(args.freeze_dir,args.flow_dir,args.a_zip,args.out,p)
    print('R1D27C_PHASE',json.dumps({k:v[k] for k in ['version','phase','research_only','strategy_ready']},ensure_ascii=False),flush=True)
if __name__=='__main__':main()
