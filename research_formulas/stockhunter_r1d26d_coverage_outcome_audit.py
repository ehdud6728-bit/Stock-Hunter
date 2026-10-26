#!/usr/bin/env python3
"""R1-D26D RESEARCH ONLY. Frozen, 20-event, historical, source-to-outcome DESCRIPTIVE QA.

Source authentication: GitHub archive SHA, R1-D26C row hash, dates + event calendar,
R1-D25 as-of SHA and one-to-one join. KRX independent original/PIT is NOT certified.
No return-based filter optimization and no live modifications or orders.
"""
from __future__ import annotations
import argparse,csv,gzip,hashlib,io,json,re,zipfile
from collections import defaultdict
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

FLOW_CSV='reports/r1d26c/result/source_collection/R1D26C_VERIFIED_COMPLETE_EVENTS_INVESTOR_PRIOR20_KRW.csv'
AUDIT_CSV='reports/r1d26c/result/source_collection/R1D26C_SOURCE_EVENT_AUDIT.csv'
FLOW_MANIFEST='reports/r1d26c/result/source_collection/R1D26C_COLLECT_MANIFEST.json'
EVENT_CSV='reports/r1d26/result/pilot/R1D26_FROZEN_PILOT_EVENTS_ASOF_ONLY.csv'
CALENDAR_CSV='reports/r1d26/result/pilot/official_indices_from_frozen_shard0.csv'
ASOF_GZ='reports/r1d25/result/historical/R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz'
PATH_GZ='reports/r1d25/result/historical/R1D25_D5_D10_D15_D20_D30_FUTURE_PRICE_PATHS.csv.gz'
ASOF_MANIFEST='reports/r1d25/result/historical/R1D25_MANIFEST.json'
KEYS=['strategy','date','code']; STRATEGIES=['C_ABOVE224_RISING_WAVE_PULLBACK','D_LEADER_CONSOLIDATION_BREAKOUT']
SOURCE='KRX_VIA_PYKRX_INVESTOR_NET_BUY_KRW_HISTORICAL_RETRIEVAL_NOT_PIT_CERTIFIED'
READY='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
class QAError(Exception):pass

def check(ok,msg):
    if not ok:raise QAError(msg)

def sha(b):return hashlib.sha256(b).hexdigest()

def zget(z,n):
    check(n in z.namelist(),'ZIP_MEMBER_MISSING_'+n)
    return z.read(n)

def frame(b,compression=None):return pd.read_csv(io.BytesIO(b),compression=compression,dtype={'code':str},keep_default_na=True)

def stablecode(x):
    s=str(x).strip()
    check(re.fullmatch(r'\d{1,6}',s) is not None,'INVALID_CODE')
    return s.zfill(6)

def key(x):return (str(x['strategy']),str(x['date']),stablecode(x['code']))

def outputs(p):
    p=Path(p);p.mkdir(parents=True,exist_ok=True);return p

def save_csv(p,df):
    df.to_csv(p,index=False,encoding='utf-8-sig',float_format='%.10g')

def read_validated(protocol,flowzip,eventzip,pricezip):
    p=json.loads(Path(protocol).read_text('utf-8'))
    check(p['research_only'] is True and p['source_results_only'] is True,'PROTOCOL_NOT_RESEARCH_ONLY')
    for k,f in [('r1d26c_zip_sha256',flowzip),('r1d26_zip_sha256',eventzip),('r1d25_zip_sha256',pricezip)]:
        check(sha(Path(f).read_bytes())==p[k],'PINNED_ARCHIVE_DIGEST_'+k)
    with zipfile.ZipFile(flowzip) as z,zipfile.ZipFile(eventzip) as e,zipfile.ZipFile(pricezip) as y:
        m=json.loads(zget(z,FLOW_MANIFEST))
        check(m['events_frozen']==20 and m['events_complete']==20 and m['verified_complete_event_rows']==400,'PILOT_NOT_FULLY_COMPLETE')
        check(m['official_value_cross_source_verified'] is False and m['official_source_download_PIT_verified'] is False,'UPSTREAM_CERTIFICATION_CHANGED')
        raw=zget(z,FLOW_CSV);audit_raw=zget(z,AUDIT_CSV)
        check(sha(raw)==m['flow_csv_sha256'] and sha(audit_raw)==m['audit_csv_sha256'],'UPSTREAM_MANIFEST_SHA_MISMATCH')
        rows=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))));check(len(rows)==400,'FLOW_NOT_400')
        audit=frame(audit_raw);check(len(audit)==20,'AUDIT_NOT_20')
        auditkeys={key({'strategy':r.strategy,'date':r.signal_date,'code':r.code}) for r in audit.itertuples()}
        check(len(auditkeys)==20 and (audit['verified_sessions']==20).all(),'AUDIT_NOT_COMPLETE')
        frozen=frame(zget(e,EVENT_CSV));check(len(frozen)==20,'FROZEN_EVENT_NOT_20')
        ed={key(r):r for r in frozen.to_dict('records')};check(len(ed)==20,'DUPLICATE_FROZEN_EVENT')
        check(set(ed)==auditkeys,'AUDIT_EVENTS_DRIFT')
        idx=frame(zget(e,CALENDAR_CSV));dates=sorted(set(idx['date'].astype(str)))
        check(len(dates)==p['source_krx_days']==1907,'KRX_CALENDAR_COUNT_DRIFT')
        bydate={d:i for i,d in enumerate(dates)}
        grouped=defaultdict(list)
        for r in rows:
            k=(r['strategy'],r['signal_date'],stablecode(r['code']))
            check(k in ed,'EXTRA_EVENT_IN_FLOW')
            check(r['source']==SOURCE and r['unit']=='KRW','SOURCE_UNIT_DRIFT')
            check(r['event_source_hash']==ed[k]['source_event_sha256'],'EVENT_SOURCE_HASH_MISMATCH')
            check(bool(re.fullmatch(r'-?\d+',r['foreign_net_krw'])) and bool(re.fullmatch(r'-?\d+',r['institution_net_krw'])),'NONINTEGER_KRW')
            check(r['trade_date']<r['signal_date'],'FUTURE_LEAK')
            check(re.fullmatch(r'\d{4}-\d{2}-\d{2}',r['trade_date']) is not None,'INVALID_DATE')
            check(re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00',r['collection_timestamp_utc']) is not None,'INVALID_COLLECTION_TIMESTAMP')
            grouped[k].append(r)
        check(set(grouped)==set(ed),'EVENT_KEY_DRIFT')
        for k,rr in grouped.items():
            check(len(rr)==20 and len({x['trade_date'] for x in rr})==20,'INCOMPLETE_OR_DUPLICATE_EVENT')
            s=ed[k]
            ix=bydate.get(str(s['date']));check(ix is not None and ix>=20,'BAD_FROZEN_SIGNAL_DATE')
            expected=dates[ix-20:ix]
            got=sorted(x['trade_date'] for x in rr)
            check(got==expected,'NOT_EXACT_PRIOR20_SESSIONS')
            check(expected[0]==s['pre20_from'] and expected[-1]==s['pre20_to'],'EVENT_WINDOW_DRIFT')
            check(len({x['collection_timestamp_utc'] for x in rr})==1,'TIMESTAMP_DRIFT')
        ym=json.loads(zget(y,ASOF_MANIFEST))
        sigbytes=gzip.decompress(zget(y,ASOF_GZ))
        check(sha(sigbytes)==ym['asof_plaintext_sha256']==p['r1d25_asof_plaintext_sha256'],'ASOF_SEAL_DRIFT')
        feats=frame(sigbytes)
        check(len(feats)==23266 and not feats.duplicated(KEYS).any(),'PARENT_ASOF_DUPLICATES')
        paths=frame(zget(y,PATH_GZ),'gzip')
        check(len(paths)==116330 and not paths.duplicated(KEYS+['horizon']).any(),'PARENT_PRICE_PATH_DRIFT')
        paths=paths[paths['horizon']==30].copy()
        check(len(paths)==23266,'PARENT_D30_COUNT_DRIFT')
        return p,grouped,ed,feats,paths,m,ym

def generate(protocol,flowzip,eventzip,pricezip,out):
    p,grouped,ed,feats,paths,m,ym=read_validated(protocol,flowzip,eventzip,pricezip)
    out=outputs(out)
    # Feature computation uses strictly pre-signal actual observed foreign/institutional KRW values.
    records=[]
    for k,rs in sorted(grouped.items()):
        rs=sorted(rs,key=lambda r:r['trade_date'])
        f=np.array([int(r['foreign_net_krw']) for r in rs],dtype='float64')
        i=np.array([int(r['institution_net_krw']) for r in rs],dtype='float64')
        s=ed[k]
        rec={'strategy':k[0],'date':k[1],'code':k[2],'year':int(s['year']),
             'source_event_sha256':s['source_event_sha256'],
             'pre20_first':rs[0]['trade_date'],'pre20_last':rs[-1]['trade_date'],
             'foreign_net20_krw':int(sum(int(r['foreign_net_krw']) for r in rs)),
             'institution_net20_krw':int(sum(int(r['institution_net_krw']) for r in rs)),
             'foreign_net10_krw':int(sum(int(r['foreign_net_krw']) for r in rs[-10:])),
             'institution_net10_krw':int(sum(int(r['institution_net_krw']) for r in rs[-10:])),
             'foreign_net5_krw':int(sum(int(r['foreign_net_krw']) for r in rs[-5:])),
             'institution_net5_krw':int(sum(int(r['institution_net_krw']) for r in rs[-5:])),
             'foreign_positive_days20':int(sum(f>0)),
             'institution_positive_days20':int(sum(i>0)),
             'foreign_net20_positive':int(sum(f)>0),
             'institution_net20_positive':int(sum(i)>0),
             'both_net20_positive':int(sum(f)>0 and sum(i)>0),
             # Bounded -1 to +1, scale-independent signed-to-gross investor flow (not investor share of total trading).
             'foreign_signed_abs_ratio20':float(f.sum()/abs(f).sum()) if abs(f).sum() else np.nan,
             'institution_signed_abs_ratio20':float(i.sum()/abs(i).sum()) if abs(i).sum() else np.nan,
             'foreign_last5_positive':int(f[-5:].sum()>0),
             'institution_last5_positive':int(i[-5:].sum()>0),
             'foreign_last5_avg_minus_prev15_avg_krw':float(f[-5:].mean()-f[:-5].mean()),
             'institution_last5_avg_minus_prev15_avg_krw':float(i[-5:].mean()-i[:-5].mean()),
             'source_original_official_download_independently_verified':False,
             'point_in_time_publication_verified':False}
        records.append(rec)
    f=pd.DataFrame(records);check(len(f)==20,'FEAT_NOT_20')
    join=f.merge(feats,on=KEYS,validate='one_to_one',how='left',indicator=True)
    check((join['_merge']=='both').all(),'PARENT_ASOF_JOIN_FAILED')
    join=join.drop(columns=['_merge'])
    # check record identity against frozen pre-20 asof seal, not outcomes.
    for r in join.itertuples(index=False):
        e=ed[(r.strategy,r.date,r.code)]
        check(abs(float(r.signal_close)-float(e['signal_close']))<=max(.00001,abs(float(e['signal_close']))*1e-8),'PRICE_ASOF_DRIFT')
        check(abs(float(r.amount_vs_prior20)-float(e['amount_vs_prior20']))<1e-9,'ASOF_AMOUNT_DRIFT')
    join=join.merge(paths,on=KEYS,how='left',validate='one_to_one',suffixes=('', '_D30'),indicator=True)
    check((join['_merge']=='both').all(),'D30_JOIN_FAILED');join=join.drop(columns=['_merge'])
    join['D30_completed']=join['status_out'].eq(READY)
    join['plus15_intraday_touched_by_D30']=np.where(join['D30_completed'],join['mfe_intraday_pct']>=15,np.nan)
    join['plus15_D30_close_net']=np.where(join['D30_completed'],join['net_20bps_pct']>=15,np.nan)
    join['minus5_D30_close_net']=np.where(join['D30_completed'],join['net_20bps_pct']<=-5,np.nan)
    join['pre_signal_only_data_verified']=True
    join['not_new_independent_OOS']=True
    save_csv(out/'R1D26D_20EVENTS_TRUE_FLOW_ASOF_AND_D30_EXPLORATORY.csv',join)
    # Strictly descriptive: tiny 10+10 sample, no pvalues or inferred signal selection.
    scores=[]
    for name,g in [('C',join[join.strategy==STRATEGIES[0]]),('D',join[join.strategy==STRATEGIES[1]]),('ALL',join)]:
        d=g[g.D30_completed].copy()
        scores.append({'stratum':name,'n_asof':len(g),'n_D30_complete':len(d),
           'foreign20_positive_n':int(g.foreign_net20_positive.sum()),
           'institution20_positive_n':int(g.institution_net20_positive.sum()),
           'both20_positive_n':int(g.both_net20_positive.sum()),
           'foreign20_median_krw':float(g.foreign_net20_krw.median()),
           'institution20_median_krw':float(g.institution_net20_krw.median()),
           'D30_net_mean_pct':float(d.net_20bps_pct.mean()) if len(d) else None,
           'D30_net_median_pct':float(d.net_20bps_pct.median()) if len(d) else None,
           'plus15_high_touch_n':int(d.plus15_intraday_touched_by_D30.sum()),
           'plus15_terminal_n':int(d.plus15_D30_close_net.sum()),
           'minus5_terminal_n':int(d.minus5_D30_close_net.sum())})
    score=pd.DataFrame(scores);save_csv(out/'R1D26D_C_D_DESCRIPTIVE_20CASE_SUMMARY.csv',score)
    groups=[]
    for strategy in STRATEGIES:
        allg=join[(join.strategy==strategy)&join.D30_completed]
        for factor in ['foreign_net20_positive','institution_net20_positive','both_net20_positive','foreign_last5_positive','institution_last5_positive']:
            for flag in [0,1]:
                g=allg[allg[factor]==flag]
                groups.append({'strategy':strategy,'factor':factor,'flag':flag,'n':len(g),
                  'D30_avg_pct':float(g.net_20bps_pct.mean()) if len(g) else None,
                  'plus15_high_touch_n':int(g.plus15_intraday_touched_by_D30.sum()) if len(g) else 0,
                  'plus15_terminal_n':int(g.plus15_D30_close_net.sum()) if len(g) else 0,
                  'minus5_terminal_n':int(g.minus5_D30_close_net.sum()) if len(g) else 0,
                  'NO_STRATEGY_CONCLUSION':True})
    save_csv(out/'R1D26D_TINY_GROUPS_DO_NOT_USE_FOR_FILTERING.csv',pd.DataFrame(groups))
    loo=[]
    for strategy in STRATEGIES:
        g=join[(join.strategy==strategy)&join.D30_completed].copy()
        if len(g)>=2:
            top=g.sort_values('net_20bps_pct',ascending=False).iloc[0]
            reduced=g[g.code.ne(top.code) | g.date.ne(top.date)]
            loo.append({'strategy':strategy,'n_complete':len(g),'original_mean_D30_pct':float(g.net_20bps_pct.mean()),
                        'original_median_D30_pct':float(g.net_20bps_pct.median()),
                        'largest_return_D30_pct':float(top.net_20bps_pct),
                        'remaining_n':len(reduced),
                        'mean_without_largest_event_pct':float(reduced.net_20bps_pct.mean()),
                        'not_representative_no_OOS':True})
    save_csv(out/'R1D26D_OUTLIER_LEAVE_ONE_OUT_CAUTION.csv',pd.DataFrame(loo))
    index=pd.DataFrame([{'signal_date':r.date,'code':r.code,'strategy':r.strategy,'trade_date':r.pre20_last,
           'source_event_sha256':r.source_event_sha256,'official_cross_provider_amount_match':'NOT_CHECKED',
           'historical_publication_timestamp':'NOT_VERIFIED','external_provider_authentication':'NOT_VERIFIED'}
           for r in f.itertuples(index=False)])
    save_csv(out/'R1D26D_OFFICIAL_INDEPENDENT_CROSSCHECK_PENDING.csv',index)
    check(score.loc[score.stratum=='C','n_asof'].iloc[0]==10 and score.loc[score.stratum=='D','n_asof'].iloc[0]==10,'STRATEGY_BALANCE_FAILED')
    check(len(join)==20 and join.pre_signal_only_data_verified.all(),'JOIN_QA_FAIL')
    manifest={'version':'R1D26D_ASOF_ACTUAL_KRX_INVESTOR_FLOW_D30_DESCRIPTIVE_LOCK_V1',
       'protocol_sha256':sha(Path(protocol).read_bytes()),'r1d26c_source_archive_sha256':p['r1d26c_zip_sha256'],
       'r1d25_source_archive_sha256':p['r1d25_zip_sha256'],
       'r1d26_original_source_archive_sha256':p['r1d26_zip_sha256'],
       'r1d25_asof_sha256':ym['asof_plaintext_sha256'],
       'r1d26c_flow_csv_sha256':m['flow_csv_sha256'],
       'source_raw_rows':400,'frozen_signal_events':20,'C_events':10,'D_events':10,
       'D30_complete':int(join.D30_completed.sum()),'D30_censored':int((~join.D30_completed).sum()),
       'future_outcomes_used_only_after_source_seal':True,
       'this_protocol_is_retrospective_not_new_OOS':True,
       'small_sample_unreliable_for_strategy_selection':True,
       'KRX_historical_retrieval_not_pit':True,
       'independent_official_amount_crosscheck':'PENDING',
       'official_timestamp_pit_provenance':'PENDING',
       'real_money_amount_source':'pykrx/KRX KRW signed institutional foreign',
       'sector_PIT_verified':False,'strategy_ready':False,'live_changed':False,'orders_placed':0}
    (out/'R1D26D_QA_MANIFEST.json').write_text(json.dumps(manifest,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print('R1D26D_DESCRIPTIVE_QA_PASS',json.dumps(manifest,ensure_ascii=False),flush=True)
    return manifest

def main():
    q=argparse.ArgumentParser();q.add_argument('--protocol',default='R1D26D_PREDECLARED_PROTOCOL.json');q.add_argument('--flow-zip',required=True);q.add_argument('--event-zip',required=True);q.add_argument('--price-zip',required=True);q.add_argument('--out',required=True)
    a=q.parse_args();generate(a.protocol,a.flow_zip,a.event_zip,a.price_zip,a.out)
if __name__=='__main__':main()
