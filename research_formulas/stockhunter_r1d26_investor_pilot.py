#!/usr/bin/env python3
"""R1-D26: strictly historical C/D investor-flow SOURCE AVAILABILITY pilot.

Nothing here suggests that KRX data were actually downloaded until `collect` reports
nonempty, 20-session continuous real provider rows. No trading, no return fitting,
no forward-price loads, no imitation of institutional flows from price/volume.
"""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import math
import re
import time
import zipfile
from pathlib import Path
import pandas as pd

PREFIX='reports/r1d25/result/historical/'
ASOF_NAME=PREFIX+'R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz'
MANIFEST_NAME=PREFIX+'R1D25_MANIFEST.json'
C='C_ABOVE224_RISING_WAVE_PULLBACK'
D='D_LEADER_CONSOLIDATION_BREAKOUT'
EVENT_COLS=['strategy','date','code','year','signal_ix','signal_close','amount_vs_prior20',
            'ma224_slope20_recomputed','rs20_vs_kospi_pctp','rs20_vs_kosdaq_pctp','preflow_rising_and_price_soft',
            'pre20_from','pre20_to','source_event_sha256']
FLOW_COLS=['signal_date','code','strategy','trade_date','foreign_net_krw','institution_net_krw',
           'source','collection_timestamp_utc','event_source_hash']
AUDIT_COLS=['strategy','date','code','status','observed_sessions','expected_sessions','issue']
SRC='KRX_VIA_PYKRX_INVESTOR_TRADING_VALUE_BY_DATE_UNVERIFIED_PUBLICATION_TIMESTAMP'

class Reject(ValueError):pass

def require(b,msg):
    if not b:raise Reject('R1D26_FAIL_CLOSED_'+msg)

def filehash(blob):return hashlib.sha256(blob).hexdigest()

def write_json(v,p):Path(p).write_text(json.dumps(v,ensure_ascii=False,indent=2,sort_keys=True,default=str)+'\n',encoding='utf-8')

def datecheck(x):
    try:
        a=dt.date.fromisoformat(str(x))
        require(a.isoformat()==str(x),'INVALID_DATE_FORMAT')
    except (ValueError,TypeError) as ex:raise Reject('INVALID_DATE '+str(x)) from ex
    return a

def calendar_from_indices(filename):
    df=pd.read_csv(filename,dtype={'date':str,'index':str})
    require({'date','index','close'}.issubset(df.columns),'INDEX_SCHEMA')
    require(not df[['date','index']].duplicated().any(),'INDEX_DUPLICATE')
    v=df.pivot(index='date',columns='index',values='close').sort_index()
    require(len(v)==1907 and set(v.columns)=={'KOSPI','KOSDAQ'},'INDEX_CALENDAR_1907_OR_NAMES')
    require(not v.isna().any().any(),'INDEX_MISSING_PRICE')
    return list(v.index)

def freeze_source(zip_path,protocol):
    b=Path(zip_path).read_bytes()
    require(filehash(b)==protocol['r1d25_artifact_zip_sha256'],'UPSTREAM_ZIP_SHA')
    with zipfile.ZipFile(io.BytesIO(b)) as z:
        require(ASOF_NAME in z.namelist() and MANIFEST_NAME in z.namelist(),'UPSTREAM_MEMBER_MISSING')
        manifest=json.loads(z.read(MANIFEST_NAME))
        require(manifest['frozen_signal_rows']==23266 and manifest['orders_placed']==0,'UPSTREAM_MANIFEST_DRIFT')
        plain=gzip.decompress(z.read(ASOF_NAME))
        require(filehash(plain)==protocol['r1d25_asof_plaintext_sha256'],'ASOF_PLAINTEXT_SHA')
        require(filehash(plain)==manifest['asof_plaintext_sha256'],'ASOF_MANIFEST_SHA_MISMATCH')
        f=pd.read_csv(io.BytesIO(plain),dtype={'code':str,'date':str},low_memory=False)
    require(len(f)==23266,'UPSTREAM_ROWCOUNT')
    require(not f[['date','code','strategy']].duplicated().any(),'UPSTREAM_DUPLICATE_KEYS')
    return f,manifest

def select_pilot(df,calendar,n_per_group=2):
    require(n_per_group>0 and n_per_group<=10,'UNBOUNDED_SAMPLE')
    filtered=df[df.strategy.isin([C,D])].copy()
    filtered['year']=filtered.date.str[:4].astype(int)
    groups=[]
    for y in range(2021,2026):
        for strategy in [C,D]:
            f=filtered[(filtered.year==y)&(filtered.strategy==strategy)].copy()
            require(len(f)>=n_per_group,f'NO_EVENTS_{y}_{strategy}')
            # Deterministic selection strictly on ASOF keys, NEVER returns/forward paths.
            f['_sample_key']=[filehash(f'{r.date}|{r.code}|{strategy}'.encode()) for r in f.itertuples()]
            f=f.sort_values('_sample_key').head(n_per_group)
            groups.append(f)
    out=pd.concat(groups,ignore_index=True).sort_values(['year','strategy','date','code']).reset_index(drop=True)
    for i,r in out.iterrows():
        ix=int(r.signal_ix)
        require(ix>=20 and ix<len(calendar) and calendar[ix]==r.date,'SIGNAL_IX_CALENDAR_DRIFT')
        out.loc[i,'pre20_from']=calendar[ix-20]
        out.loc[i,'pre20_to']=calendar[ix-1]
        out.loc[i,'source_event_sha256']=filehash(f'{r.date}|{r.code}|{r.strategy}'.encode())
    require(len(out)==5*2*n_per_group,'PILOT_COUNT')
    return out[EVENT_COLS]

def to_integer(v):
    if pd.isna(v):raise Reject('FLOW_VALUE_MISSING')
    if isinstance(v,str):
        v=v.strip().replace(',','')
        require(bool(re.fullmatch(r'[+-]?\d+',v)),'NON_INTEGER_FLOW')
        return int(v)
    if isinstance(v,(int,)) or (hasattr(v,'dtype') and str(v.dtype).startswith('int')):return int(v)
    if isinstance(v,float) and math.isfinite(v) and v.is_integer():return int(v)
    raise Reject('UNSAFE_FLOAT_OR_BAD_FLOW')

def normalize_pykrx(df,event,expected,at):
    """Require all exact 20 exchange sessions, no future day, and actual numeric values."""
    require(isinstance(df,pd.DataFrame) and not df.empty,'PROVIDER_EMPTY')
    require('기관합계' in df.columns and '외국인합계' in df.columns,'PROVIDER_SCHEMA')
    dts=[]
    for v in df.index:
        try:dts.append(pd.Timestamp(v).date().isoformat())
        except Exception as ex:raise Reject('PROVIDER_DATE') from ex
    require(len(dts)==len(set(dts)),'PROVIDER_DUPLICATE_DATES')
    require(set(dts)==set(expected),'PROVIDER_MISSING_OR_EXTRANEOUS_SESSIONS')
    require(all(d<event['date'] for d in dts),'PROVIDER_LOOKAHEAD')
    vals=[]
    for day,rr in zip(dts,df.itertuples(index=False,name=None)):
        v=dict(zip(df.columns,rr))
        vals.append({'signal_date':event['date'],'code':event['code'],'strategy':event['strategy'],
                     'trade_date':day,'foreign_net_krw':to_integer(v['외국인합계']),
                     'institution_net_krw':to_integer(v['기관합계']),
                     'source':SRC,'collection_timestamp_utc':at,'event_source_hash':event['source_event_sha256']})
    require(any(r['foreign_net_krw'] or r['institution_net_krw'] for r in vals),'SUSPICIOUS_ALL_ZERO_SERIES')
    return sorted(vals,key=lambda r:r['trade_date'])

def prepare(args,protocol):
    require(filehash(Path(args.krx_shard0_zip).read_bytes())==protocol['krx_shard0_artifact_zip_sha256'],'KRX_SHARD0_SHA')
    with zipfile.ZipFile(args.krx_shard0_zip) as z:
        calname='shard_00/official_indices.csv'
        require(calname in z.namelist(),'OFFICIAL_INDEX_MISSING')
        temp=Path(args.out)/'official_indices_from_frozen_shard0.csv'
        temp.write_bytes(z.read(calname))
    cal=calendar_from_indices(temp)
    f,m=freeze_source(args.r1d25_zip,protocol)
    events=select_pilot(f,cal,protocol['pilot_per_year_strategy'])
    epath=Path(args.out)/'R1D26_FROZEN_PILOT_EVENTS_ASOF_ONLY.csv'
    events.to_csv(epath,index=False,encoding='utf-8-sig')
    write_json({'stage':'PILOT_EVENTS_FROZEN','real_investor_records':0,'real_sector_records':0,
                'events':len(events),'pilot_selection_uses_future_outcomes':False,
                'all_dates_before_signal':True,'source_is_verified_KRX_R1D25_SHA':True,
                'pilot_csv_sha256':filehash(epath.read_bytes()),
                'provider_reachability':'NOT_TESTED_LOCAL'},Path(args.out)/'R1D26_PREPARE_MANIFEST.json')
    (Path(args.out)/'R1D26_PIT_SECTOR_MEMBERSHIP_INPUT_TEMPLATE.csv').write_text(
        'code,sector_code,sector_name,valid_from,valid_to,source_id,source_asof_timestamp\n',encoding='utf-8')
    return events,cal

def collect(args,protocol):
    ev=pd.read_csv(Path(args.out)/'R1D26_FROZEN_PILOT_EVENTS_ASOF_ONLY.csv',dtype={'code':str,'date':str})
    cal=calendar_from_indices(Path(args.out)/'official_indices_from_frozen_shard0.csv')
    require(len(ev)==5*2*protocol['pilot_per_year_strategy'],'PILOT_INPUT_COUNT')
    records=[];audit=[]
    try:
        from pykrx import stock
        lib_error=None
    except Exception as ex:
        stock=None;lib_error=type(ex).__name__+': '+str(ex)[:160]
    at=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')
    for r in ev.to_dict('records'):
        ix=int(r['signal_ix']);expected=cal[ix-20:ix]
        require(len(expected)==20 and expected[0]==r['pre20_from'] and expected[-1]==r['pre20_to'],'PILOT_WINDOW_DRIFT')
        vals=[];issue='';status=''
        if stock is None:
            status='SOURCE_CLIENT_UNAVAILABLE';issue=lib_error or 'pykrx unavailable'
        else:
            try:
                z=stock.get_market_trading_value_by_date(expected[0].replace('-',''),expected[-1].replace('-',''),r['code'],on='순매수')
                vals=normalize_pykrx(z,r,expected,at)
                status='COMPLETE_20_PRIOR_TRADING_SESSIONS'
            except Exception as ex:
                status='NOT_VERIFIED_PROVIDER_FAILURE';issue=type(ex).__name__+': '+str(ex)[:155]
        if vals:records.extend(vals)
        audit.append({'strategy':r['strategy'],'date':r['date'],'code':r['code'],'status':status,
                      'observed_sessions':len(vals),'expected_sessions':20,'issue':issue})
        if args.sleep_seconds>0:time.sleep(args.sleep_seconds)
    out=Path(args.out)
    with (out/'R1D26_VERIFIED_INVESTOR_PRIOR20_ONLY.csv').open('w',newline='',encoding='utf-8-sig') as h:
        w=csv.DictWriter(h,fieldnames=FLOW_COLS);w.writeheader();w.writerows(records)
    with (out/'R1D26_SOURCE_FETCH_AUDIT.csv').open('w',newline='',encoding='utf-8-sig') as h:
        w=csv.DictWriter(h,fieldnames=AUDIT_COLS);w.writeheader();w.writerows(audit)
    ok=sum(a['status']=='COMPLETE_20_PRIOR_TRADING_SESSIONS' for a in audit)
    per_group={}
    for y in range(2021,2026):
        for strategy in [C,D]:
            tag=f'{y}_{strategy[0]}'
            per_group[tag]={'complete':sum(a['status'].startswith('COMPLETE') and a['date'].startswith(str(y)) and a['strategy']==strategy for a in audit),
                            'attempted':sum(a['date'].startswith(str(y)) and a['strategy']==strategy for a in audit)}
    manifest={'stage':'EXTERNAL_KRX_INVESTOR_PILOT','provider':'pykrx historical ticker by date',
              'collection_utc':at,'attempted_events':len(ev),'complete_events':ok,'incomplete_events':len(ev)-ok,
              'actual_real_observations':len(records),'coverage_pct':round(100*ok/len(ev),2),
              'by_year_strategy':per_group,'source_verified':'YES_AT_20_SESSION_ROWLEVEL' if ok else 'NO_REAL_ROWS_VERIFIED',
              'all_source_dates_strictly_pre_signal':True,
              'no_investor_data_imputed_or_fabricated':True,'sector_PIT_verified':False,
              'no_forward_outcomes_read':True,'strategy_effect_estimated':False,
              'future_prospective_OOS':False,'strategy_ready':False,'orders_placed':0,
              'next_step':'EXPAND_PIT_COVERAGE_BEFORE_HYPOTHESIS_TESTING' if ok else 'REVIEW_SOURCE_FETCH_AUDIT'}
    write_json(manifest,out/'R1D26_COLLECT_MANIFEST.json')
    print('R1D26_REAL_PROVIDER_PILOT',json.dumps(manifest,ensure_ascii=False),flush=True)
    return manifest

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--mode',choices=['prepare','collect'],required=True)
    p.add_argument('--protocol',default='R1D26_PREDECLARED_PROTOCOL.json')
    p.add_argument('--r1d25-zip',default='reports/r1d26/input/R1D25_original_artifact.zip')
    p.add_argument('--krx-shard0-zip',default='reports/r1d26/input/KRX_shard0_original_artifact.zip')
    p.add_argument('--out',default='reports/r1d26/result/pilot')
    p.add_argument('--sleep-seconds',type=float,default=1.0)
    a=p.parse_args();require(a.sleep_seconds>=0,'NEGATIVE_THROTTLE')
    proto=json.loads(Path(a.protocol).read_text('utf-8'))
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    if a.mode=='prepare':
        ev,_=prepare(a,proto)
        print('R1D26_SOURCE_PILOT_PREPARED_NO_INVESTOR_DATA_YET',len(ev),flush=True)
    else:
        collect(a,proto)
if __name__=='__main__':main()
