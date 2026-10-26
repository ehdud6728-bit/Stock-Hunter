#!/usr/bin/env python3
"""R1-D26C RESEARCH ONLY: collect real historical KRX investor KRW flows
for 20 locked ASOF C/D events, 20 earlier KRX trading sessions per event.

Never load any forward-price result or optimize strategy rules here. Valid
provider rows are historical values retrieved *today*, not certified as
point-in-time publication data or official original-archive snapshots.
"""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import re
import time
import zipfile
from pathlib import Path
import pandas as pd

VERSION='R1D26C_LOCKED_C_D_20X20_REAL_KRX_INVESTOR_KRW_PILOT_V1'
BASE='reports/r1d26/result/pilot/'
EVENT_FILE=BASE+'R1D26_FROZEN_PILOT_EVENTS_ASOF_ONLY.csv'
INDEX_FILE=BASE+'official_indices_from_frozen_shard0.csv'
C='C_ABOVE224_RISING_WAVE_PULLBACK'
D='D_LEADER_CONSOLIDATION_BREAKOUT'
SRC='KRX_VIA_PYKRX_INVESTOR_NET_BUY_KRW_HISTORICAL_RETRIEVAL_NOT_PIT_CERTIFIED'
FIELDS=['signal_date','code','strategy','trade_date','foreign_net_krw','institution_net_krw',
        'source','unit','collection_timestamp_utc','event_source_hash']
AUDIT=['signal_date','code','strategy','status','expected_sessions','verified_sessions',
       'reason_code','provider_exception_type','window_first','window_last']

class Invalid(ValueError): pass

def require(cond,reason):
    if not cond:raise Invalid('R1D26C_FAIL_CLOSED_'+reason)

def sha(b):return hashlib.sha256(b).hexdigest()

def load_inputs(archive,proto):
    b=Path(archive).read_bytes()
    require(sha(b)==proto['source_zip_sha256'],'SOURCE_ARTIFACT_SHA')
    with zipfile.ZipFile(io.BytesIO(b)) as f:
        require(EVENT_FILE in f.namelist() and INDEX_FILE in f.namelist(),'SOURCE_MEMBERS')
        events_bytes=f.read(EVENT_FILE); idx_bytes=f.read(INDEX_FILE)
    require(sha(events_bytes)==proto['events_sha256'],'EVENTS_SHA')
    require(sha(idx_bytes)==proto['calendar_sha256'],'CALENDAR_SHA')
    events=pd.read_csv(io.BytesIO(events_bytes),dtype={'code':str,'date':str,'pre20_from':str,'pre20_to':str,'strategy':str})
    idx=pd.read_csv(io.BytesIO(idx_bytes),dtype={'date':str,'index':str})
    require(set(['date','index','close']).issubset(idx.columns),'INDEX_COLUMNS')
    require(not idx[['date','index']].duplicated().any(),'INDEX_DUPLICATE')
    v=idx.pivot(index='date',columns='index',values='close').sort_index()
    require(list(v.index)==sorted(v.index),'INDEX_SORT')
    require(len(v)==1907 and set(v.columns)=={'KOSPI','KOSDAQ'},'CALENDAR_1907')
    require(not v.isna().any().any(),'INDEX_MISSING')
    calendar=list(v.index)
    require(len(events)==20,'PILOT_COUNT_20')
    require(set(events.strategy)=={C,D},'PILOT_STRATEGIES')
    require(not events[['date','code','strategy']].duplicated().any(),'DUPLICATE_EVENT')
    for yr in range(2021,2026):
        for st in [C,D]:
            require(int(((events['date'].str[:4]==str(yr)) & (events['strategy']==st)).sum())==2,'GROUP_2_PER_YEAR_STRATEGY')
    events=events.sort_values(['date','strategy','code']).reset_index(drop=True)
    windows=[]
    for row in events.to_dict('records'):
        code=str(row['code']).zfill(6)
        require(bool(re.fullmatch(r'\d{6}',code)),'TICKER_SIX_DIGITS')
        sig=str(row['date']);ix=int(row['signal_ix'])
        require(20<=ix<len(calendar) and calendar[ix]==sig,'ASOF_DAY_INDEX')
        expected=calendar[ix-20:ix]
        require(len(expected)==20,'WINDOW_SIZE')
        require(expected[0]==row['pre20_from'] and expected[-1]==row['pre20_to'],'FROZEN_WINDOW_DRIFT')
        require(all(day<sig for day in expected),'FORWARD_DATE')
        hash0=sha(f'{sig}|{code}|{row["strategy"]}'.encode())
        require(hash0==row['source_event_sha256'],'EVENT_IDENTITY_SHA')
        windows.append((dict(row,code=code),expected))
    # First check the exact event which passed the actual 5-session KRX/pykrx
    # connectivity probe in run 40265029666. That success did not certify
    # 20-session history or PIT, but avoids wasting requests on an older symbol
    # that may have since been delisted.
    anchor=[v for v in windows if v[0]['date']=='2025-08-29' and v[0]['code']=='101490' and v[0]['strategy']==C]
    require(len(anchor)==1,'KNOWN_PROVIDER_ANCHOR_MISSING')
    windows=anchor+[v for v in windows if v is not anchor[0]]
    return windows

def safe_int(v):
    """KRW integer precision without lossy coercion or silent missing values."""
    import math
    if pd.isna(v):raise Invalid('INVALID_MISSING_AMOUNT')
    if isinstance(v,str):
        v=v.replace(',','').strip()
        require(bool(re.fullmatch(r'[+-]?\d+',v)),'NONINTEGER_AMOUNT');return int(v)
    if isinstance(v,(int,)) or type(v).__name__ in ('int64','int32','int16'):
        return int(v)
    if isinstance(v,(float,)) or type(v).__name__ in ('float64','float32'):
        require(math.isfinite(float(v)) and abs(float(v))<2**53 and float(v).is_integer(),'NONINTEGER_OR_UNSAFE_FLOAT')
        return int(v)
    raise Invalid('BAD_AMOUNT_TYPE')

def validate_provider(df,row,expected,timestamp):
    require(isinstance(df,pd.DataFrame) and not df.empty,'PROVIDER_EMPTY_OR_TYPE')
    require({'기관합계','외국인합계'}.issubset(df.columns),'PROVIDER_REQUIRED_COLUMNS')
    dates=[]
    for d in df.index:
        try:dates.append(pd.Timestamp(d).date().isoformat())
        except Exception:raise Invalid('PROVIDER_BAD_DATE')
    require(len(dates)==len(set(dates)),'PROVIDER_DUPLICATE_DAYS')
    require(len(dates)==20 and set(dates)==set(expected),'PROVIDER_MISSING_OR_EXTRA_DAYS')
    require(max(dates)<row['date'],'PROVIDER_LEAK')
    results=[]
    for d,record in zip(dates,df.to_dict('records')):
        results.append(dict(signal_date=row['date'],code=row['code'],strategy=row['strategy'],trade_date=d,
            foreign_net_krw=safe_int(record['외국인합계']),institution_net_krw=safe_int(record['기관합계']),
            source=SRC,unit='KRW',collection_timestamp_utc=timestamp,event_source_hash=row['source_event_sha256']))
    require(any(r['foreign_net_krw']!=0 or r['institution_net_krw']!=0 for r in results),'SUSPICIOUS_ALL_ZERO')
    return sorted(results,key=lambda r:r['trade_date'])

def make_files(out,records,audit,manifest):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    for name,cols,data in [
      ('R1D26C_VERIFIED_COMPLETE_EVENTS_INVESTOR_PRIOR20_KRW.csv',FIELDS,records),
      ('R1D26C_SOURCE_EVENT_AUDIT.csv',AUDIT,audit)]:
        with (out/name).open('w',newline='',encoding='utf-8-sig') as h:
            wr=csv.DictWriter(h,fieldnames=cols);wr.writeheader();wr.writerows(data)
    manifest['flow_csv_sha256']=sha((out/'R1D26C_VERIFIED_COMPLETE_EVENTS_INVESTOR_PRIOR20_KRW.csv').read_bytes())
    manifest['audit_csv_sha256']=sha((out/'R1D26C_SOURCE_EVENT_AUDIT.csv').read_bytes())
    (out/'R1D26C_COLLECT_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')

def collect(windows,fetch,out,max_events=20,sleep_seconds=2,stop_after_source_failures=1):
    require(len(windows)==20,'PILOT_COUNT')
    require(1<=max_events<=20,'MAX_EVENTS')
    require(0<=sleep_seconds<=15,'SLEEP_CAP')
    require(1<=stop_after_source_failures<=3,'ERROR_STOP_CAP')
    timestamp=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')
    records=[];audit=[];fail_in_row=0
    for i,(row,dates) in enumerate(windows):
        if i>=max_events:break
        status='';exc_cls='';reason='';vals=[]
        try:
            data=fetch(dates[0].replace('-',''),dates[-1].replace('-',''),row['code'])
            vals=validate_provider(data,row,dates,timestamp)
            status='COMPLETE_EXACT_20_REAL_KRW_ROWS_PROVISIONAL';fail_in_row=0
        except Invalid as ex:
            status='REJECTED_NOT_VERIFIED';reason=str(ex)[:120]
            # A suspended/delisted ticker with incomplete historical sessions is
            # an event-specific rejection, not evidence the entire provider is down.
            if 'PROVIDER_MISSING_OR_EXTRA_DAYS' in reason or 'SUSPICIOUS_ALL_ZERO' in reason:
                fail_in_row=0
            else:
                fail_in_row+=1
        except Exception as ex:
            status='PROVIDER_EXCEPTION_NOT_VERIFIED';exc_cls=type(ex).__name__[:50];reason='PROVIDER_ERROR_NO_VALUES';fail_in_row+=1
        if vals: records.extend(vals)
        audit.append(dict(signal_date=row['date'],code=row['code'],strategy=row['strategy'],status=status,
            expected_sessions=20,verified_sessions=len(vals),reason_code=reason,provider_exception_type=exc_cls,
            window_first=dates[0],window_last=dates[-1]))
        # Do not hammer KRX if source is obviously not accessible. No fake zeros.
        if fail_in_row>=stop_after_source_failures:break
        if sleep_seconds and i+1<max_events:time.sleep(sleep_seconds)
    completed=sum(a['status'].startswith('COMPLETE') for a in audit)
    require(len(records)==20*completed,'RECORD_CARDINALITY')
    require(len({(r['signal_date'],r['strategy'],r['code'],r['trade_date']) for r in records})==len(records),'DUPLICATE_RECORDS')
    manifest=dict(version=VERSION,stage='REAL_SOURCE_COVERAGE_RESEARCH_ONLY_NOT_STRATEGY',
      provider='pykrx.get_market_trading_value_by_date(on=순매수)',unit='KRW',
      events_frozen=20,expected_sessions_per_event=20,events_attempted=len(audit),
      events_not_attempted=20-len(audit),events_complete=completed,events_incomplete=len(audit)-completed,
      verified_complete_event_rows=len(records),coverage_pct_of_all_frozen=round(100*completed/20,2),
      source_data_retrieved_now_not_historical_point_in_time_certified=True,
      provider_schema_dates_units_checked=True,official_value_cross_source_verified=False,
      official_source_download_PIT_verified=False,sector_PIT_verified=False,
      no_forward_return_used=True,strategy_effect_estimated=False,
      historical_research_only=True,not_new_OOS=True,strategy_ready=False,
      live_changed=False,orders_placed=0,
      fail_closed_without_source=True,
      next_step='SOURCE_CROSS_CHECK_AND_PIT_PROVENANCE' if completed else 'INSPECT_SOURCE_FAILURE')
    make_files(out,records,audit,manifest)
    print('R1D26C_REAL_SOURCE_COVERAGE',json.dumps({k:manifest[k] for k in ['events_attempted','events_complete','events_not_attempted','verified_complete_event_rows','coverage_pct_of_all_frozen','strategy_ready']},ensure_ascii=False),flush=True)
    return manifest

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--protocol',default='R1D26C_PREDECLARED_PROTOCOL.json')
    p.add_argument('--source-zip',required=True)
    p.add_argument('--out',default='reports/r1d26c/result/source_collection')
    p.add_argument('--sleep-seconds',type=float,default=2)
    a=p.parse_args()
    proto=json.loads(Path(a.protocol).read_text(encoding='utf-8'))
    require(proto['version']==VERSION,'PROTOCOL_VERSION')
    windows=load_inputs(a.source_zip,proto)
    # Credentials are provided to the environment by GitHub Secrets on the one
    # source collection step. Never log, write, or interpolate secret values.
    from pykrx import stock
    collect(windows,lambda beg,end,code:stock.get_market_trading_value_by_date(beg,end,code,on='순매수'),
            a.out,sleep_seconds=a.sleep_seconds)

if __name__=='__main__':main()
