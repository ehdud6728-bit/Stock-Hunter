#!/usr/bin/env python3
"""R1D26F: historical investor KRW source coverage, 100 frozen outcome-blind C/D signals.
Source-only RESEARCH_ONLY. This program must never open a forward-price outcomes file.
Historical values retrieved now do not certify contemporaneous availability/PIT.
"""
import argparse, csv, datetime as dt, gzip, hashlib, io, json, re, time, zipfile
from collections import defaultdict, Counter
from pathlib import Path
import pandas as pd

VERSION='R1D26F_100_ASOF_STRATIFIED_DEDUP_KRX_KRW_SOURCE_V1'
C='C_ABOVE224_RISING_WAVE_PULLBACK'; D='D_LEADER_CONSOLIDATION_BREAKOUT'
BASE25='reports/r1d25/result/historical/'
BASE26='reports/r1d26/result/pilot/'
BASE26C='reports/r1d26c/result/source_collection/'
SIGNAL=BASE25+'R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz'
INDEX=BASE26+'official_indices_from_frozen_shard0.csv'
ANCHOR=BASE26+'R1D26_FROZEN_PILOT_EVENTS_ASOF_ONLY.csv'
SEED=BASE26C+'R1D26C_VERIFIED_COMPLETE_EVENTS_INVESTOR_PRIOR20_KRW.csv'
FIELDS=['signal_date','code','strategy','trade_date','foreign_net_krw','institution_net_krw','unit','source','collection_timestamp_utc','origin','event_source_hash']
EVENT_FIELDS=['date','code','strategy','year','signal_ix','pre20_from','pre20_to','event_source_hash','origin']
AUDIT_FIELDS=['date','code','strategy','year','status','verified_sessions','expected_sessions','reason']

class QA(Exception):pass
def need(cond,reason):
    if not cond:raise QA(reason)
def sha(blob):return hashlib.sha256(blob).hexdigest()
def zipped(z,path):
    try:return z.read(path)
    except KeyError:raise QA('FROZEN_MEMBER_MISSING:'+path)
def filezip(path,expect):
    b=Path(path).read_bytes();need(sha(b)==expect,'ZIP_SHA_DRIFT');return zipfile.ZipFile(io.BytesIO(b))
def norm_code(v):
    s=str(v).strip();need(bool(re.fullmatch(r'\d{1,6}',s)),'CODE_SCHEMA');return s.zfill(6)
def calendar_from_zip(z):
    t=pd.read_csv(io.BytesIO(zipped(z,INDEX)),dtype={'date':str,'index':str})
    need(not t[['date','index']].duplicated().any(),'INDEX_DUPLICATE')
    p=t.pivot(index='date',columns='index',values='close').sort_index()
    need(len(p)==1907 and set(p.columns)=={'KOSPI','KOSDAQ'} and not p.isna().any().any(),'ORIGINAL_CALENDAR_DRIFT')
    return list(p.index)
def eventhash(date,code,strategy):return sha(f'{date}|{code}|{strategy}'.encode())
def load_sources(paths,protocol):
    z25=filezip(paths['r1d25_zip'],protocol['r1d25_sha256'])
    z26=filezip(paths['r1d26_zip'],protocol['r1d26_sha256'])
    z26c=filezip(paths['r1d26c_zip'],protocol['r1d26c_sha256'])
    cal=calendar_from_zip(z26)
    sig=pd.read_csv(io.BytesIO(gzip.decompress(zipped(z25,SIGNAL))),dtype={'date':str,'code':str,'strategy':str},low_memory=False)
    sig=sig[sig.strategy.isin([C,D])]
    need(len(sig)==10310 and sig[['date','code','strategy']].drop_duplicates().shape[0]==10310,'CD_ALL_SIGNAL_IDENTITY')
    # 38 six-character preferred-share class tickers ending in K cannot pass
    # this pilot's numeric-only KRX provider contract; preserve the count.
    unsupported=sig[~sig.code.str.fullmatch(r'[0-9]{6}')]
    need(len(unsupported)==38 and unsupported.code.str.fullmatch(r'[0-9]{5}K').all(),'UNSUPPORTED_CLASS_COUNT_DRIFT')
    sig=sig[sig.code.str.fullmatch(r'[0-9]{6}')]
    sg={}
    for x in sig[['date','code','strategy','signal_ix']].itertuples(index=False,name=None):
        day,code,strategy,ix=x;code=norm_code(code);ix=int(ix)
        need(20<=ix<len(cal) and cal[ix]==day,'SIGNAL_INDEX_DRIFT')
        sg[(day,code,strategy)]=ix
    anchors=pd.read_csv(io.BytesIO(zipped(z26,ANCHOR)),dtype={'date':str,'code':str,'strategy':str,'pre20_from':str,'pre20_to':str,'source_event_sha256':str})
    need(len(anchors)==20,'ANCHOR_COUNT')
    A={}
    for row in anchors.to_dict('records'):
        key=(row['date'],norm_code(row['code']),row['strategy'])
        need(key in sg and key not in A,'ANCHOR_IDENTITY')
        ix=sg[key];expect=cal[ix-20:ix]
        need(row['source_event_sha256']==eventhash(*key) and row['pre20_from']==expect[0] and row['pre20_to']==expect[-1],'ANCHOR_WINDOW_DRIFT')
        A[key]=row
    by=defaultdict(list)
    for key,ix in sg.items():by[(int(key[0][:4]),key[2])].append(key)
    result=[]
    for year in range(2021,2026):
        for strategy in (C,D):
            keys=by[(year,strategy)];need(len(keys)>=10,'INSUFFICIENT_STRATUM')
            anchor=[k for k in keys if k in A]
            need(len(anchor)==2,'ANCHOR_STRATUM_2')
            extra=sorted((k for k in keys if k not in A),key=lambda k:(eventhash(*k),k))[:8]
            need(len(extra)==8,'EXTRA_STRATUM_8')
            for key in anchor+extra:
                day,code,st=key;ix=sg[key];days=cal[ix-20:ix]
                result.append(dict(date=day,code=code,strategy=st,year=year,signal_ix=ix,pre20_from=days[0],pre20_to=days[-1],event_source_hash=eventhash(*key),origin='R1D26C_20_SEED' if key in A else 'R1D26F_OUTCOME_BLIND_HASH'))
    result.sort(key=lambda x:(x['year'],x['strategy'],x['date'],x['code']))
    need(len(result)==100 and len(set((r['date'],r['code'],r['strategy']) for r in result))==100,'SAMPLE_SIZE_DRIFT')
    return result,cal,z26c

def seed_rows(z26c,events,calendar):
    data=pd.read_csv(io.BytesIO(zipped(z26c,SEED)),dtype=str).fillna('')
    need(len(data)==400,'SEED_400_DRIFT')
    ev={(r['date'],r['code'],r['strategy']):r for r in events if r['origin']=='R1D26C_20_SEED'}
    need(len(ev)==20,'SEED_EVENTS_20')
    grouped=defaultdict(list);out={}
    for row in data.to_dict('records'):
        key=(row['signal_date'],norm_code(row['code']),row['strategy']);need(key in ev,'SEED_EXTRA_EVENT')
        need(row['unit']=='KRW' and row['event_source_hash']==ev[key]['event_source_hash'],'SEED_PROVENANCE')
        day=row['trade_date'];need(day<row['signal_date'],'SEED_FORWARD_DATE')
        k=(key[1],day);f=read_int(row['foreign_net_krw']);i=read_int(row['institution_net_krw'])
        if k in out:need((f,i)==out[k][:2],'SEED_CONFLICT_AMOUNT')
        out[k]=(f,i,row['collection_timestamp_utc'],'R1D26C_PINNED_SOURCE')
        grouped[key].append(day)
    for key,row in ev.items():
        expected=calendar[row['signal_ix']-20:row['signal_ix']]
        need(len(grouped[key])==20 and set(grouped[key])==set(expected),'SEED_CALENDAR_NOT20')
    need(sum(map(len,grouped.values()))==400,'SEED_ROWS_DRIFT')
    return out

def read_int(v):
    s=str(v).strip();need(bool(re.fullmatch(r'[+-]?\d+',s)),'INVALID_SIGNED_KRW');return int(s)

def make_groups(events,calendar,cache,limit_span=75):
    need(20<=limit_span<=150,'SPAN_LIMIT')
    dayix={d:i for i,d in enumerate(calendar)}
    required=defaultdict(set)
    for r in events:
        for day in calendar[r['signal_ix']-20:r['signal_ix']]:
            if (r['code'],day) not in cache:required[r['code']].add(day)
    groups=[]
    for code,dates in sorted(required.items()):
        ordered=sorted(dates,key=lambda d:dayix[d]);current=[]
        for day in ordered:
            if current and dayix[day]-dayix[current[0]]+1>limit_span:
                groups.append((code,current));current=[]
            current.append(day)
        if current:groups.append((code,current))
    return groups

def validate_fetch(df,code,need_days,calendar,now):
    need(isinstance(df,pd.DataFrame) and not df.empty,'EMPTY_PROVIDER')
    need({'기관합계','외국인합계'}.issubset(df.columns),'MISSING_PROVIDER_COLUMNS')
    got={}
    for day,record in zip(df.index,df.to_dict('records')):
        day=pd.Timestamp(day).date().isoformat()
        need(day not in got,'DUPLICATE_PROVIDER_DATE')
        got[day]=(read_int(record['외국인합계']),read_int(record['기관합계']))
    need(set(got)==set(need_days),'PROVIDER_DATE_COVERAGE_MISMATCH')
    need(any(x or y for x,y in got.values()),'ALL_ZERO_SUSPICIOUS')
    return {(code,d):(x,y,now,'R1D26F_PYKRX_KRX_RETRIEVAL') for d,(x,y) in got.items()}

def write_csv(path,columns,rows):
    path=Path(path)
    with path.open('w',newline='',encoding='utf-8-sig') as f:
        w=csv.DictWriter(f,fieldnames=columns,extrasaction='ignore');w.writeheader();w.writerows(rows)

def run(paths,protocol,out,mode='prepare',fetch=None,sleep=1.0,stop_after=3):
    need(mode in ('prepare','collect'),'MODE');need(0<=sleep<=10 and 1<=stop_after<=5,'RATE_LIMITS')
    events,calendar,z26c=load_sources(paths,protocol)
    cache=seed_rows(z26c,events,calendar)
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    write_csv(out/'R1D26F_FROZEN_100_SIGNAL_EVENTS_ASOF_ONLY.csv',EVENT_FIELDS,events)
    groups=make_groups(events,calendar,cache)
    need(len(groups)<=120,'REQUEST_GROUP_LIMIT')
    plans=[{'code':code,'start':days[0],'end':days[-1],'unique_trading_days':len(days)} for code,days in groups]
    write_csv(out/'R1D26F_REQUEST_PLAN_NO_OUTCOMES.csv',['code','start','end','unique_trading_days'],plans)
    attempted=0;errors=[];now=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')
    if mode=='collect':
        need(callable(fetch),'PROVIDER_REQUIRED')
        failures=0
        for code,days in groups:
            attempted+=1
            try:
                # Nonadjacent requested days inside a group are permitted; fetch range
                # covers all calendar sessions, not just the selected subset.
                full=calendar[calendar.index(days[0]):calendar.index(days[-1])+1]
                df=fetch(days[0].replace('-',''),days[-1].replace('-',''),code)
                checked=validate_fetch(df,code,full,calendar,now)
                for k,v in checked.items():
                    if k in cache:need(cache[k][:2]==v[:2],'SEED_VERSUS_NEW_CONFLICT')
                    cache[k]=v
                failures=0
            except (QA,Exception) as ex:
                typ=type(ex).__name__
                errors.append(dict(code=code,start=days[0],end=days[-1],reason='PROVIDER_OR_SCHEMA_REJECTED',error_type=typ))
                failures+=1
            if failures>=stop_after:break
            if sleep and attempted<len(groups):time.sleep(sleep)
    write_csv(out/'R1D26F_SOURCE_REQUEST_AUDIT.csv',['code','start','end','reason','error_type'],errors)
    complete=[];audit=[];incomplete=[]
    for r in events:
        days=calendar[r['signal_ix']-20:r['signal_ix']];found=[day for day in days if (r['code'],day) in cache]
        ok=len(found)==20;name='COMPLETE_PROVISIONAL_KRW' if ok else 'INCOMPLETE_NO_IMPUTATION'
        audit.append(dict(date=r['date'],code=r['code'],strategy=r['strategy'],year=r['year'],status=name,verified_sessions=len(found),expected_sessions=20,reason='' if ok else 'UNVERIFIED_DATES'))
        if not ok:incomplete.append((r,len(found)));continue
        for day in days:
            f,i,at,origin=cache[(r['code'],day)]
            complete.append(dict(signal_date=r['date'],code=r['code'],strategy=r['strategy'],trade_date=day,foreign_net_krw=f,institution_net_krw=i,unit='KRW',source='KRX_VIA_PYKRX_RETROSPECTIVE_NOT_PIT',collection_timestamp_utc=at,origin=origin,event_source_hash=r['event_source_hash']))
    need(len(complete)==20*sum(x['status']=='COMPLETE_PROVISIONAL_KRW' for x in audit),'FULL_ROWS_ONLY')
    need(len(complete)==len({(x['signal_date'],x['code'],x['strategy'],x['trade_date']) for x in complete}),'OUTPUT_DUPLICATES')
    write_csv(out/'R1D26F_COMPLETE_EVENT_PRIOR20_TRUE_INVESTOR_KRW_NOT_PIT.csv',FIELDS,complete)
    write_csv(out/'R1D26F_100_EVENT_COVERAGE_AUDIT.csv',AUDIT_FIELDS,audit)
    cells=Counter((x['year'],x['strategy'],x['status']) for x in audit)
    yearly=[]
    for y in range(2021,2026):
        for s in (C,D):
            n=cells[(y,s,'COMPLETE_PROVISIONAL_KRW')];yearly.append(dict(year=y,strategy=s,total_frozen=10,completed=n,incomplete=10-n,coverage_pct=100*n/10))
    write_csv(out/'R1D26F_YEAR_STRATEGY_COVERAGE.csv',['year','strategy','total_frozen','completed','incomplete','coverage_pct'],yearly)
    full=sum(x['status']=='COMPLETE_PROVISIONAL_KRW' for x in audit)
    man=dict(version=VERSION,mode=mode,events_frozen=100,anchors_from_R1D26C=20,hash_sample_new_events=80,
        unique_ticker_day_seed_rows=len(seed_rows(z26c,events,calendar)),planned_fetch_groups=len(groups),attempted_fetch_groups=attempted,
        stopped_before_all_groups=attempted<len(groups) if mode=='collect' else False,
        complete_events=full,incomplete_events=100-full,complete_event_rows=len(complete),coverage_pct=full,
        source_sha256={k:protocol[k+'_sha256'] for k in ('r1d25','r1d26','r1d26c')},
        event_selection='2021_2025_per_year_C10_D10_WITH_2_ANCHORS_AND_8_SHA_SORTED_ASOF_ONLY',
        no_outcome_columns_loaded=True,source_values_retrieved_today_not_pit_certified=True,
        independent_KRX_official_original_amount_crosscheck='PENDING',historical_publication_timestamp='PENDING',
        corporate_actions_price_PIT='NOT_TESTED',investor_flow_effect_estimated=False,prospective_OOS=False,
        not_live=True,strategy_ready=False,orders_placed=0)
    (out/'R1D26F_MANIFEST.json').write_text(json.dumps(man,indent=2,ensure_ascii=False,sort_keys=True)+'\n',encoding='utf-8')
    print('R1D26F_SOURCE_COVERAGE',json.dumps({k:man[k] for k in ('mode','planned_fetch_groups','attempted_fetch_groups','complete_events','incomplete_events','complete_event_rows','coverage_pct','strategy_ready')},ensure_ascii=False),flush=True)
    return man

def main():
    p=argparse.ArgumentParser();p.add_argument('--protocol',required=True);p.add_argument('--r1d25-zip',required=True);p.add_argument('--r1d26-zip',required=True);p.add_argument('--r1d26c-zip',required=True);p.add_argument('--mode',choices=['prepare','collect'],default='prepare');p.add_argument('--out',default='reports/r1d26f/result/source_only');p.add_argument('--sleep-seconds',type=float,default=1)
    a=p.parse_args();proto=json.loads(Path(a.protocol).read_text(encoding='utf-8'))
    need(proto['version']==VERSION,'PROTOCOL_VERSION_DRIFT')
    fetch=None
    if a.mode=='collect':
        from pykrx import stock
        fetch=lambda b,e,c:stock.get_market_trading_value_by_date(b,e,c,on='순매수')
    run({'r1d25_zip':a.r1d25_zip,'r1d26_zip':a.r1d26_zip,'r1d26c_zip':a.r1d26c_zip},proto,a.out,a.mode,fetch,a.sleep_seconds)
if __name__=='__main__':main()
