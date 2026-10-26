#!/usr/bin/env python3
"""R1D26E / RESEARCH_ONLY: historical D+5/10/15/20/30 high-touch ladder.

The R1D25 price path dataset stores *cumulative MFE* only at five checkpoints.
Thus first-touch is an interval-censored 5/10/15/20/30 bucket, NOT the
exact session of first touch. Never interpret high touch as order fills.
2021-25 historical data are NOT new prospective OOS. 20 selected investor
flow events are a tiny source-quality pilot, NOT full-market flow coverage.
"""
from __future__ import annotations
import argparse, csv, hashlib, io, json, zipfile
from pathlib import Path
import numpy as np
import pandas as pd

ST_C='C_ABOVE224_RISING_WAVE_PULLBACK'
ST_D='D_LEADER_CONSOLIDATION_BREAKOUT'
STRATEGIES=(ST_C,ST_D)
HORIZONS=(5,10,15,20,30)
THRESHOLDS=(5,10,15)
KEY=['strategy','date','code']
PATH='reports/r1d25/result/historical/R1D25_D5_D10_D15_D20_D30_FUTURE_PRICE_PATHS.csv.gz'
FLOW='reports/r1d26d/result/historical/R1D26D_20EVENTS_TRUE_FLOW_ASOF_AND_D30_EXPLORATORY.csv'
OK='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
BUCKETS={5:'D1-D5',10:'D6-D10',15:'D11-D15',20:'D16-D20',30:'D21-D30'}

class SourceQAError(ValueError):pass

def need(ok,msg):
    if not bool(ok):raise SourceQAError(msg)

def sha(blob):return hashlib.sha256(blob).hexdigest()

def verified_zip(p,expect):
    raw=Path(p).read_bytes()
    need(sha(raw)==expect,'PINNED_ZIP_SHA256_MISMATCH: '+Path(p).name)
    return zipfile.ZipFile(io.BytesIO(raw),'r')

def read_csv_bytes(z,p,**kwargs):
    b=z.read(p)
    if p.endswith('.gz'):return pd.read_csv(io.BytesIO(b),compression='gzip',**kwargs)
    return pd.read_csv(io.BytesIO(b),**kwargs)

def base_input(pathdf,expected_c=7499,expected_d=2811):
    cols=set(KEY+['horizon','status_out','mfe_intraday_pct','net_20bps_pct','period','entry_day','exit_day'])
    need(cols.issubset(pathdf.columns),'PATH_MISSING_FIELDS')
    d=pathdf[pathdf.strategy.isin(STRATEGIES)].copy()
    d['code']=d.code.astype(str).str.zfill(6)
    d['date']=d.date.astype(str)
    need(not d.duplicated(KEY+['horizon']).any(),'DUPLICATE_STRATEGY_DATE_CODE_HORIZON')
    need(set(d.horizon.astype(int).unique())==set(HORIZONS),'HORIZONS_NOT_EXACT')
    n=d[d.horizon==30].groupby('strategy').size().to_dict()
    need(n=={ST_C:expected_c,ST_D:expected_d},'SOURCE_C_D_SIGNAL_COUNTS_CHANGED')
    counts=d.groupby(KEY,sort=False)['horizon'].nunique()
    need(counts.eq(len(HORIZONS)).all(),'MISSING_HORIZON_ROWS')
    need(len(d)==(expected_c+expected_d)*len(HORIZONS),'SOURCE_ROW_COUNT_CHANGED')
    return d

def build_event_table(d):
    # Only D+30-complete events qualify for a common denominator. Earlier
    # censored horizons are also forbidden for these events.
    end=d[d.horizon==30].copy()
    ok=end[end.status_out.eq(OK)].copy()
    keys=ok[KEY]
    x=d.merge(keys,on=KEY,how='inner',validate='many_to_one')
    need(x.status_out.eq(OK).all(),'EARLY_HORIZON_CENSORED_IN_D30_COMPLETE')
    need(x.mfe_intraday_pct.notna().all(),'MISSING_MFE_IN_COMPLETE')
    wide=x.pivot(index=KEY,columns='horizon',values='mfe_intraday_pct')
    wide=wide.loc[:,list(HORIZONS)]
    need(not wide.isna().any().any(),'INCOMPLETE_HORIZON_MATRIX')
    need(((wide.diff(axis=1).iloc[:,1:] >= -1e-9).all().all()),'MFE_NOT_NONDECREASING')
    out=wide.reset_index().rename(columns={h:f'mfe_by_d{h}_pct' for h in HORIZONS})
    endmeta=ok[KEY+['period','entry_day','exit_day','net_20bps_pct','mfe_intraday_pct','mae_intraday_pct']].copy()
    out=out.merge(endmeta,on=KEY,how='left',validate='one_to_one')
    need(np.allclose(out['mfe_by_d30_pct'],out.mfe_intraday_pct,atol=1e-10),'D30_MFE_NOT_EQUAL')
    out['year']=out.date.str[:4].astype(int)
    for thr in THRESHOLDS:
        mask=out['mfe_by_d30_pct']>=thr
        out[f'hit{thr}_by_d30']=mask.astype(int)
        val=[]
        for row in out[[f'mfe_by_d{h}_pct' for h in HORIZONS]].itertuples(index=False,name=None):
            found=next((BUCKETS[h] for h,mfe in zip(HORIZONS,row) if mfe>=thr),'NO_HIT_BY_D30')
            val.append(found)
        out[f'first_hit{thr}_bucket']=val
        need(int(mask.sum())==int((out[f'first_hit{thr}_bucket']!='NO_HIT_BY_D30').sum()),'BUCKET_CROSSCHECK')
    need((out.hit15_by_d30<=out.hit10_by_d30).all(),'15_ABOVE_10')
    need((out.hit10_by_d30<=out.hit5_by_d30).all(),'10_ABOVE_5')
    return out.sort_values(KEY).reset_index(drop=True),end

def summary_one(x):
    result={'signals':len(x),'different_tickers':int(x.code.nunique())}
    for t in THRESHOLDS:
        cnt=int(x[f'hit{t}_by_d30'].sum())
        result[f'n_hit{t}']=cnt
        result[f'pct_hit{t}']=round(cnt/len(x)*100,4) if len(x) else None
    n5,n10,n15=[result[f'n_hit{t}'] for t in THRESHOLDS]
    result['pct_hit10_given_hit5']=round(n10/n5*100,4) if n5 else None
    result['pct_hit15_given_hit10']=round(n15/n10*100,4) if n10 else None
    result['pct_hit15_given_hit5']=round(n15/n5*100,4) if n5 else None
    result['pct_only_5_to_under10']=round((n5-n10)/len(x)*100,4) if len(x) else None
    result['pct_10_to_under15']=round((n10-n15)/len(x)*100,4) if len(x) else None
    result['pct_below5']=round((len(x)-n5)/len(x)*100,4) if len(x) else None
    return result

def save_csv(df,path):
    path.parent.mkdir(parents=True,exist_ok=True)
    df.to_csv(path,index=False,encoding='utf-8-sig',float_format='%.10g')

def produce(paths_zip,flows_zip,protocol,out):
    p=json.loads(Path(protocol).read_text(encoding='utf-8'))
    need(p.get('research_only') and p.get('no_live_or_orders') and p.get('historical_not_prospective_OOS'),'UNSAFE_PROTOCOL')
    need(p['exact_first_hit_date_available'] is False,'FALSE_EXACT_DAY_CLAIM')
    a=verified_zip(paths_zip,p['r1d25_zip_sha256'])
    b=verified_zip(flows_zip,p['r1d26d_zip_sha256'])
    # R1D25 as-of source sha byte-seal from the same frozen repository source.
    sigpath='reports/r1d25/result/historical/R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz'
    original=a.read(sigpath)
    # source seal is plaintext gzip member SHA, not compressed-member SHA
    import gzip
    need(sha(gzip.decompress(original))==p['r1d25_asof_plaintext_sha256'],'ASOF_PLAINTEXT_SHA_DRIFT')
    pathdf=read_csv_bytes(a,PATH,dtype={'code':str})
    d=base_input(pathdf)
    events,ending=build_event_table(d)
    nvalid={st:len(events[events.strategy==st]) for st in STRATEGIES}
    need(nvalid=={ST_C:7416,ST_D:2782},'D30_COMPLETENESS_CHANGED')
    output=Path(out);output.mkdir(parents=True,exist_ok=True)
    # Compress large row-level CSV inside .gz to keep artifact compact.
    events.to_csv(output/'R1D26E_C_D_ALL_COMPLETE_D30_INTERVAL_FIRST_TOUCH.csv.gz',index=False,compression='gzip',encoding='utf-8',float_format='%.10g')
    cohort=[]
    for st in STRATEGIES:
        all_st=d[(d.strategy==st)&(d.horizon==30)]
        good=events[events.strategy==st]
        for tag, subset in [('ALL',good)]+[(f'YEAR_{y}',good[good.year==y]) for y in range(2021,2026)]:
            row={'strategy':st,'cohort':tag,'all_frozen_signals':len(all_st) if tag=='ALL' else int((all_st.date.str[:4].astype(int)==int(tag[-4:])).sum()),'D30_censored':None}
            row.update(summary_one(subset))
            row['D30_censored']=row['all_frozen_signals']-row['signals']
            cohort.append(row)
    summary=pd.DataFrame(cohort)
    save_csv(summary,output/'R1D26E_ALL_SIGNAL_5_10_15_TOUCH_CONVERSION.csv')
    hit=[]
    order=list(BUCKETS.values())+['NO_HIT_BY_D30']
    for st in STRATEGIES:
        s=events[events.strategy==st]
        for th in THRESHOLDS:
            for bucket in order:
                n=int(s[f'first_hit{th}_bucket'].eq(bucket).sum())
                hit.append({'strategy':st,'threshold_pct':th,'first_observed_hit_interval':bucket,'events':n,'denominator_completed_D30':len(s),'pct_of_completed':round(n/len(s)*100,4)})
    save_csv(pd.DataFrame(hit),output/'R1D26E_FIRST_HIT_INTERVAL_D5_D10_D15_D20_D30.csv')
    # 20-event investor pilot, D26D is frozen & previously reviewed. Strictly descriptive.
    fl=read_csv_bytes(b,FLOW,dtype={'code':str})
    need(len(fl)==20 and not fl.duplicated(KEY).any(),'PILOT_NOT_20_UNIQUE')
    need(set(KEY+['foreign_net20_krw','institution_net20_krw','foreign_net20_positive','institution_net20_positive','mfe_intraday_pct','net_20bps_pct']).issubset(fl.columns),'INVESTOR_PILOT_SCHEMA')
    need(fl['point_in_time_publication_verified'].astype(str).str.lower().isin(['false','0']).all(),'FALSE_PIT_CLAIM')
    sample=fl.merge(events[KEY+['entry_day','mfe_intraday_pct','net_20bps_pct']+[f'hit{t}_by_d30' for t in THRESHOLDS]+[f'first_hit{t}_bucket' for t in THRESHOLDS]],on=KEY,how='left',validate='one_to_one',suffixes=('_d26d','_d26e'),indicator=True)
    need(sample['_merge'].eq('both').all(),'PILOT_NOT_JOINED_TO_ALL_SIGNALS')
    need(np.allclose(sample.mfe_intraday_pct_d26d,sample.mfe_intraday_pct_d26e,rtol=0,atol=1e-7),'PILOT_D30_MFE_PARITY')
    need(np.allclose(sample.net_20bps_pct_d26d,sample.net_20bps_pct_d26e,rtol=0,atol=1e-7),'PILOT_D30_NET_PARITY')
    sample=sample.drop(columns=['_merge'])
    save_csv(sample,output/'R1D26E_20PILOT_KRX_INVESTOR_FLOW_HIT_LADDER_NOT_OOS.csv')
    rows=[]
    for st in STRATEGIES:
        sg=sample[sample.strategy==st]
        for flag in ['foreign_net20_positive','institution_net20_positive','both_net20_positive']:
            for val in (True,False):
                sub=sg[sg[flag].astype(bool).eq(val)]
                rows.append({'strategy':st,'feature':flag,'value':val,'group_n':len(sub),**summary_one(sub)})
    save_csv(pd.DataFrame(rows),output/'R1D26E_20PILOT_FLOW_SIGN_DESCRIPTIVE_NO_FILTERING.csv')
    mark={'version':p['version'],'source_r1d25_archive_sha256':p['r1d25_zip_sha256'],'source_r1d26d_archive_sha256':p['r1d26d_zip_sha256'],
          'source_path_csv_gz_sha256':sha(a.read(PATH)),'source_flow_20event_csv_sha256':sha(b.read(FLOW)),
          'all_C_D_signal_count':10310,'D30_complete_C':7416,'D30_complete_D':2782,
          'D30_censored':112,'full_market_actual_investor_flow_covered':20,'actual_investor_flow_available_for_other_signals':False,
          'first_hit_resolution':'interval_censored_1to5_6to10_11to15_16to20_21to30_NOT_EXACT_DAY',
          'thresholds_pct':[5,10,15],'hit_event':'intraday_cumulative_high_MFE_vs_next_trading_day_open_NOT_EXECUTED_FILL',
          'conditional_conversion_is_retrospective_not_predictive':True,
          'R1D26D_20sample_is_not_representative':True,'PIT_certified':False,'source_independent_amount_crosscheck':'PENDING',
          'corporate_action_PIT_certified':False,'prospective_OOS':False,'strategy_ready':False,'live_changed':False,'orders_placed':0}
    (output/'R1D26E_QA_MANIFEST.json').write_text(json.dumps(mark,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('R1D26E_INTERVAL_TOUCH_AND_PILOT_QA_COMPLETE_NOT_OOS',json.dumps(mark,ensure_ascii=False),flush=True)
    return summary,events,mark

def main():
    p=argparse.ArgumentParser();p.add_argument('--r1d25-zip',required=True);p.add_argument('--r1d26d-zip',required=True);p.add_argument('--protocol',required=True);p.add_argument('--out',required=True);a=p.parse_args()
    produce(a.r1d25_zip,a.r1d26d_zip,a.protocol,a.out)

if __name__=='__main__':main()
