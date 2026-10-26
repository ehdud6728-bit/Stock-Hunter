#!/usr/bin/env python3
"""R1-D26G: retrospective, non-PIT descriptive audit of 100 locked C/D investor-flow signals.

Prohibited: optimizing rules on outcomes, converting price proxies to investor identity,
claiming future fills, calling this clean prospective OOS, accessing LIVE/order modules.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import io
import json
import math
import zipfile
from collections import Counter
from pathlib import Path
import numpy as np
import pandas as pd

VERSION='R1D26G_PRESPECIFIED_100_C_D_KRX_FLOW_HIT5_10_15_EXPLORATORY_V1'
FROZEN_F='reports/r1d26f/result/source_only/'
FROZEN_E='reports/r1d26e/result/historical/'
FROZEN_25='reports/r1d25/result/historical/'
FLOW_NAME=FROZEN_F+'R1D26F_COMPLETE_EVENT_PRIOR20_TRUE_INVESTOR_KRW_NOT_PIT.csv'
EVENT_NAME=FROZEN_F+'R1D26F_FROZEN_100_SIGNAL_EVENTS_ASOF_ONLY.csv'
PATH_NAME=FROZEN_E+'R1D26E_C_D_ALL_COMPLETE_D30_INTERVAL_FIRST_TOUCH.csv.gz'
ASOF_NAME=FROZEN_25+'R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz'
KEY=['date','code','strategy']
STRATEGIES=['C_ABOVE224_RISING_WAVE_PULLBACK','D_LEADER_CONSOLIDATION_BREAKOUT']
FLAGS=['foreign_net20_pos','institution_net20_pos','both_net20_pos','foreign_net5_pos','institution_net5_pos','foreign_recent5_accel_pos','institution_recent5_accel_pos','rs20_above_both_index']
PRIMARY=['foreign_net20_pos','institution_net20_pos','both_net20_pos','rs20_above_both_index']
HITS=['hit5_by_d30','hit10_by_d30','hit15_by_d30']


def ensure(ok, msg):
    if not ok:raise ValueError('R1D26G_FAIL_CLOSED_'+msg)


def sha(b):return hashlib.sha256(b).hexdigest()

def read_csv(z,name,**kw):
    raw=z.read(name)
    if name.endswith('.gz'):
        raw=gzip.decompress(raw)
    return pd.read_csv(io.BytesIO(raw),dtype={'code':str},**kw)


def normalize(df):
    df=df.copy()
    df['code']=df['code'].astype(str).str.zfill(6)
    return df


def load(protocol,files):
    ensure(protocol['research_only'] and protocol['outcome_blind_100_sample'] and not protocol['strategy_ready'], 'PROTOCOL_UNSAFE')
    for p, key in zip(files,('r1d26f','r1d26e','r1d25')):
        ensure(sha(Path(p).read_bytes())==protocol[key+'_zip_sha256'],key.upper()+'_ZIP_SHA')
    f,e,a=[zipfile.ZipFile(p) for p in files]
    try:
        event=normalize(read_csv(f,EVENT_NAME));flow=normalize(read_csv(f,FLOW_NAME));path=normalize(read_csv(e,PATH_NAME));asof=normalize(read_csv(a,ASOF_NAME))
        fman=json.loads(f.read(FROZEN_F+'R1D26F_MANIFEST.json'))
        eman=json.loads(e.read(FROZEN_E+'R1D26E_QA_MANIFEST.json'))
        aman=json.loads(a.read(FROZEN_25+'R1D25_ASOF_SEAL.json'))
        ensure(fman['events_frozen']==100 and fman['complete_event_rows']==2000 and fman['complete_events']==100,'F_MANIFEST')
        ensure(eman['all_C_D_signal_count']==10310 and eman['D30_censored']==112,'E_MANIFEST')
        ensure(sha(gzip.decompress(a.read(ASOF_NAME)))==aman['features_sha256_plaintext'],'ASOF_PLAINTEXT_SHA')
        ensure(len(path)==eman['D30_complete_C']+eman['D30_complete_D'],'PATH_COUNT')
        ensure(sha(f.read(FLOW_NAME))==protocol['r1d26f_flow_csv_sha256'],'FLOW_CSV_PAYLOAD_SHA')
        ensure(sha(f.read(EVENT_NAME))==protocol['r1d26f_events_csv_sha256'],'EVENT_CSV_PAYLOAD_SHA')
        return event,flow,path,asof,fman,eman,aman
    finally:
        for z in [f,e,a]:z.close()


def validate_and_aggregate(event,flow,path,asof):
    ensure(len(event)==100 and set(event.strategy)==set(STRATEGIES),'EVENT_100')
    ensure(not event.duplicated(KEY).any(),'EVENT_DUPLICATE')
    ensure(all((event.groupby(['year','strategy']).size()==10).values) and len(event.groupby(['year','strategy']))==10,'EVENT_YEAR_STRATEGY_BALANCE')
    ensure(len(flow)==2000 and len(path)==10198,'SOURCE_CARDINALITY')
    ensure(set(flow.unit)=={'KRW'} and set(flow.source)=={'KRX_VIA_PYKRX_RETROSPECTIVE_NOT_PIT'},'UNIT_OR_PROVIDER')
    ensure(flow[['foreign_net_krw','institution_net_krw']].notna().all().all(),'MISSING_AMOUNTS')
    ensure(all(pd.api.types.is_integer_dtype(flow[c]) for c in ('foreign_net_krw','institution_net_krw')),'INT_KRW')
    ensure(not flow.duplicated(['signal_date','code','strategy','trade_date']).any(),'FLOW_DUPLICATE')
    ensure(not path.duplicated(KEY).any() and not asof.duplicated(KEY).any(),'PATH_ASOF_DUP')
    ef=flow.rename(columns={'signal_date':'date'})
    ensure(set(map(tuple,ef[KEY].itertuples(index=False,name=None)))==set(map(tuple,event[KEY].itertuples(index=False,name=None))),'EVENT_FLOW_KEY_SET')
    ensure(set(flow.event_source_hash)==set(event.event_source_hash),'FLOW_EVENT_HASH_SET')
    agg=[]
    for ev in event.itertuples(index=False):
        sub=flow[(flow.signal_date==ev.date)&(flow.code==ev.code)&(flow.strategy==ev.strategy)].sort_values('trade_date')
        ensure(len(sub)==20 and sub.trade_date.is_unique,'EVENT_20_SESSIONS')
        ensure(sub.trade_date.iloc[0]==ev.pre20_from and sub.trade_date.iloc[-1]==ev.pre20_to and (sub.trade_date<ev.date).all(),'EVENT_PRIOR_ONLY')
        ensure((sub.event_source_hash==ev.event_source_hash).all(),'EVENT_LINEAGE_HASH')
        row={'date':ev.date,'code':ev.code,'strategy':ev.strategy,'year':ev.year,'event_source_hash':ev.event_source_hash,'pre20_from':ev.pre20_from,'pre20_to':ev.pre20_to,'signal_ix':ev.signal_ix}
        for typ,col in [('foreign','foreign_net_krw'),('institution','institution_net_krw')]:
            x=sub[col].to_numpy(dtype=np.int64)
            for n in (5,10,20):row[typ+'_net'+str(n)+'_krw']=int(x[-n:].sum())
            row[typ+'_positive_days20']=int((x>0).sum())
            row[typ+'_signed_abs_ratio20']=float(x.sum()/np.abs(x).sum()) if np.abs(x).sum()>0 else np.nan
            row[typ+'_recent5_minus_prev15_avg_krw_per_day']=float(x[-5:].mean()-x[:-5].mean())
            row[typ+'_recent5_accel_pos']=bool(row[typ+'_recent5_minus_prev15_avg_krw_per_day']>0)
            row[typ+'_net5_pos']=bool(x[-5:].sum()>0)
            row[typ+'_net20_pos']=bool(x.sum()>0)
        row['both_net20_pos']=row['foreign_net20_pos'] and row['institution_net20_pos']
        agg.append(row)
    signals=pd.DataFrame(agg)
    full=signals.merge(path[KEY+['entry_day','exit_day','net_20bps_pct','mfe_intraday_pct','mae_intraday_pct']+HITS+['first_hit5_bucket','first_hit10_bucket','first_hit15_bucket']],on=KEY,how='left',validate='one_to_one',indicator=True)
    full['D30_completed']=full['_merge'].eq('both')
    full=full.drop(columns='_merge')
    ensure(int(full.D30_completed.sum())==99 and int((~full.D30_completed).sum())==1,'99_COMPLETED_ONE_CENSORED')
    pp=asof[KEY+['signal_ix','asof_lookback_last_index','rs20_vs_kospi_pctp','rs20_vs_kosdaq_pctp','rs20_above_both_index','ma224_slope20_recomputed','distance448_pct','amount_pre5_vs_pre20']]
    full=full.merge(pp,on=KEY,how='left',validate='one_to_one',indicator=True)
    ensure(full['_merge'].eq('both').all(),'MISSING_ASOF')
    ensure(full.signal_ix_x.eq(full.signal_ix_y).all(),'ASOF_SIGNAL_IX_MISMATCH')
    ensure((full.asof_lookback_last_index<=full.signal_ix_x).all(),'ASOF_LOOKAHEAD')
    full=full.drop(columns='_merge')
    ensure((full.loc[full.D30_completed,'entry_day']>full.loc[full.D30_completed,'date']).all(),'ENTRY_BEFORE_OR_ON_SIGNAL')
    for h in HITS:
        s=full.loc[full.D30_completed,h]
        ensure(s.notna().all() and s.isin([0,1]).all(),'INVALID_HIT_VALUES')
    complete=full[full.D30_completed].copy()
    ensure(((complete[HITS[0]]>=complete[HITS[1]]) & (complete[HITS[1]]>=complete[HITS[2]])).all(),'NONMONOTONE_HITS')
    complete['rs20_above_both_index']=complete['rs20_above_both_index'].astype(bool)
    full['rs20_above_both_index']=full['rs20_above_both_index'].astype(bool)
    ensure(set(full['D30_completed'])=={True,False},'COMPLETION_INDICATOR')
    return full


def metric_counts(df):
    n=len(df)
    r={'n':n,'unique_tickers':df.code.nunique(), 'n_hit5':int(df[HITS[0]].sum()),'n_hit10':int(df[HITS[1]].sum()),'n_hit15':int(df[HITS[2]].sum())}
    for k in (5,10,15):r['hit'+str(k)+'_pct']=round(100*r['n_hit'+str(k)]/n,5) if n else None
    r['hit10_given_hit5_pct']=round(100*r['n_hit10']/r['n_hit5'],5) if r['n_hit5'] else None
    r['hit15_given_hit10_pct']=round(100*r['n_hit15']/r['n_hit10'],5) if r['n_hit10'] else None
    r['median_net30_pct']=round(float(df.net_20bps_pct.median()),5) if n else None
    return r


def make_summaries(full):
    co=full[full.D30_completed].copy()
    global_rows=[]
    for st in STRATEGIES:
        sub=co[co.strategy==st]
        global_rows.append({'strategy':st,'scope':'100_SIGNAL_SAMPLE','all_selected':int((full.strategy==st).sum()),'censored':int((~full.D30_completed & (full.strategy==st)).sum()),**metric_counts(sub)})
    by_year=[]
    for (y,st),sub in co.groupby(['year','strategy'],sort=True):by_year.append({'year':y,'strategy':st,**metric_counts(sub)})
    groups=[];contrasts=[]
    for st in STRATEGIES:
        sub=co[co.strategy==st]
        for f in FLAGS:
            a=sub[sub[f].eq(True)];b=sub[sub[f].eq(False)]
            ensure(len(a)+len(b)==len(sub),'FLAG_NULL_'+f)
            for state,ss in [('positive_or_true',a),('nonpositive_or_false',b)]:groups.append({'strategy':st,'feature':f,'state':state,**metric_counts(ss)})
            if f in PRIMARY:
                record={'strategy':st,'feature':f,'positive_n':len(a),'nonpositive_n':len(b),'unique_tickers_pos':a.code.nunique(),'unique_tickers_nonpos':b.code.nunique(),'year_coverage':sum(any(t[f]) and any(~t[f]) for _,t in sub.groupby('year'))}
                for h in HITS:
                    record[h+'_difference_pctp']=round(100*(a[h].mean()-b[h].mean()),5) if len(a) and len(b) else None
                contrasts.append(record)
    return pd.DataFrame(global_rows),pd.DataFrame(by_year),pd.DataFrame(groups),pd.DataFrame(contrasts)


def bootstrap_cluster_year_ticker(full, contrast, iterations=1000,seed=26026):
    """Year-stratified (year,ticker) cluster resampling, exploratory intervals only."""
    rng=np.random.default_rng(seed)
    complete=full[full.D30_completed].copy()
    out=[]
    for strategy in STRATEGIES:
        sub=complete[complete.strategy==strategy].reset_index(drop=True)
        n=len(sub)
        weights=np.zeros((iterations,n),dtype=np.int32)
        for year,grp in sub.groupby('year'):
            ticks=sorted(grp.code.unique())
            # Fixed RNG / chronological/lexical ordering, all signals of a
            # year-ticker selected together; year strata preserved.
            idx={t:grp.index[grp.code==t].to_numpy() for t in ticks}
            samples=rng.integers(0,len(ticks),size=(iterations,len(ticks)))
            for i,choice in enumerate(samples):
                vals=np.bincount(choice,minlength=len(ticks))
                for j,count in enumerate(vals):
                    if count:weights[i,idx[ticks[j]]]+=int(count)
        for rr in contrast.itertuples(index=False):
            if rr.strategy!=strategy:continue
            flag=sub[rr.feature].astype(bool).to_numpy()
            pos_weight=weights[:,flag]
            neg_weight=weights[:,~flag]
            pos_den=pos_weight.sum(axis=1)
            neg_den=neg_weight.sum(axis=1)
            valid=(pos_den>0)&(neg_den>0)
            for threshold in (5,10,15):
                h='hit'+str(threshold)+'_by_d30'
                values=sub[h].to_numpy(dtype=float)
                pos_num=pos_weight@values[flag]
                neg_num=neg_weight@values[~flag]
                diffs=100*(pos_num[valid]/pos_den[valid]-neg_num[valid]/neg_den[valid])
                observed=rr.__getattribute__(h+'_difference_pctp')
                out.append({'strategy':strategy,'feature':rr.feature,'target_pct':threshold,
                    'observed_diff_pctp':observed,
                    'bootstrap_year_ticker_cluster_p2_5':round(float(np.percentile(diffs,2.5)),5) if len(diffs)>10 else None,
                    'bootstrap_year_ticker_cluster_p97_5':round(float(np.percentile(diffs,97.5)),5) if len(diffs)>10 else None,
                    'bootstrap_valid_draws':int(valid.sum()),'draws_requested':iterations,
                    'multi_testing_not_corrected':True,'historical_non_OOS':True})
    return pd.DataFrame(out)


def leave_one_ticker_out(full,contrast):
    complete=full[full.D30_completed];out=[]
    for rr in contrast.itertuples(index=False):
        sub=complete[complete.strategy==rr.strategy];f=rr.feature
        arr=[]
        for ticker in sub.code.unique():
            d=sub[sub.code!=ticker];pos=d[d[f]];neg=d[~d[f]]
            if len(pos)>0 and len(neg)>0:arr.append(100*(pos.hit15_by_d30.mean()-neg.hit15_by_d30.mean()))
        out.append({'strategy':rr.strategy,'feature':f,'ticker_drop_trials':len(arr),'min_hit15_diff_pctp':round(min(arr),5) if arr else None,'max_hit15_diff_pctp':round(max(arr),5) if arr else None})
    return pd.DataFrame(out)


def run(out,zip_f,zip_e,zip_25,protocol_path,bootstrap_n=1000):
    protocol=json.loads(Path(protocol_path).read_text(encoding='utf-8'))
    event,flow,path,asof,fman,eman,aman=load(protocol,[zip_f,zip_e,zip_25])
    full=validate_and_aggregate(event,flow,path,asof)
    global_,year,groups,contrast=make_summaries(full)
    year_feature=[]
    for (yr,st),sub in full[full.D30_completed].groupby(['year','strategy']):
        for feature in PRIMARY:
            pos=sub[sub[feature].eq(True)];neg=sub[sub[feature].eq(False)]
            year_feature.append({'year':yr,'strategy':st,'feature':feature,
                'n_positive':len(pos),'n_nonpositive':len(neg),
                'hit15_positive':int(pos.hit15_by_d30.sum()),'hit15_nonpositive':int(neg.hit15_by_d30.sum()),
                'hit15_difference_pctp':round(100*(pos.hit15_by_d30.mean()-neg.hit15_by_d30.mean()),5) if len(pos) and len(neg) else None})
    bs=bootstrap_cluster_year_ticker(full,contrast,iterations=bootstrap_n)
    sensitivity=leave_one_ticker_out(full,contrast)
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    outputs={'R1D26G_ALL_100_SELECTED_EVENT_FLOW_FEATURES_WITH_99_D30.csv':full,
             'R1D26G_C_D_100_SAMPLE_HIT_LADDER.csv':global_,
             'R1D26G_YEAR_STRATEGY_HIT_LADDER.csv':year,
             'R1D26G_FIXED_BINARY_FLOW_FEATURE_DESCRIPTIVE.csv':groups,
             'R1D26G_WITHIN_YEAR_PRIMARY_FLOW_HIT15_CONTRASTS.csv':pd.DataFrame(year_feature),
             'R1D26G_PREDECLARED_PRIMARY_FEATURE_CONTRASTS.csv':contrast,
             'R1D26G_EXPLORATORY_YEAR_TICKER_CLUSTER_BOOTSTRAP.csv':bs,
             'R1D26G_LEAVE_ONE_TICKER_OUT_HIT15_DIFFERENCE.csv':sensitivity}
    file_hash={}
    for name,df in outputs.items():
        file=out/name;df.to_csv(file,index=False,encoding='utf-8-sig');file_hash[name]=sha(file.read_bytes())
    summary={'version':VERSION,'protocol_sha256':sha(Path(protocol_path).read_bytes()),
             'upstream_archive_sha256':{k:protocol[k+'_zip_sha256'] for k in ('r1d26f','r1d26e','r1d25')},
             'source_event_count':int(len(event)),'real_source_rows':int(len(flow)),
             'D30_complete':int(full.D30_completed.sum()),'D30_censored':int((~full.D30_completed).sum()),
             'C_complete':int(((full.strategy==STRATEGIES[0])&full.D30_completed).sum()),
             'D_complete':int(((full.strategy==STRATEGIES[1])&full.D30_completed).sum()),
             'whole_market_baseline_C_D_complete':len(path),'actual_investor_coverage_of_market_signals_only_100_of_10310':True,
             'source_selection_is_asof_frozen_before_outcomes':True,
             'investor_krw_actual_KRX_pykrx_retrieval':True,'independent_KRX_original_amount_crosscheck':'PENDING',
             'source_publication_point_in_time':'NOT_VERIFIED','official_sector_PIT':'NOT_VERIFIED',
             'turnover_normalized_flow':'NOT_AVAILABLE_WITHOUT_PER_TICKER_DAILY_TOTAL_TURNOVER',
             'signed_abs_ratio_is_not_turnover_ratio':True,
             'first_hit_exact_day':'NOT_AVAILABLE_INTERVAL_ONLY',
             'hit_is_intraday_high_not_executed_exit':True,
             'retrospective_exploratory_not_predictive':True,'prospective_OOS':False,
             'small_sample_low_power':True,'multiple_comparisons_not_corrected':True,
             'strategy_ready':False,'live_changed':False,'orders_placed':0,
             'output_file_sha256':file_hash}
    (out/'R1D26G_QA_MANIFEST.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('R1D26G_REAL_100_EVENT_EXPLORATORY_QA_PASS_NOT_PIT_NOT_OOS',json.dumps({k:summary[k] for k in ('source_event_count','real_source_rows','D30_complete','D30_censored','C_complete','D_complete','strategy_ready')},ensure_ascii=False),flush=True)
    return summary


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--flow-zip',required=True);ap.add_argument('--ladder-zip',required=True);ap.add_argument('--asof-zip',required=True)
    ap.add_argument('--protocol',default='R1D26G_PREDECLARED_PROTOCOL.json');ap.add_argument('--out',default='reports/r1d26g/result/historical')
    ap.add_argument('--bootstrap-draws',type=int,default=1000)
    a=ap.parse_args();ensure(100<=a.bootstrap_draws<=2000,'BOOTSTRAP_BOUND')
    run(a.out,a.flow_zip,a.ladder_zip,a.asof_zip,a.protocol,a.bootstrap_draws)
if __name__=='__main__':main()
