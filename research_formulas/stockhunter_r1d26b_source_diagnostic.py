#!/usr/bin/env python3
"""R1D26B / RESEARCH_ONLY: ONE frozen historical C/D case, source diagnostics only.

No outcomes, no fabricated institutional/foreign net buying. Any Naver share
volumes MUST NOT be represented as KRX KRW net-buying data. This module performs
at most one pykrx call and a small, capped public alternative-page probe.
"""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import re
import zipfile
from pathlib import Path

import pandas as pd

VERSION='R1D26B_SINGLE_EVENT_SOURCE_DIAGNOSTIC_RESEARCH_ONLY_V1'
ORIGINAL_ZIP_SHA='bdb40b5e805d3dde6fe6f824826cc1be160497ef3410cb2adee4478c4c7c14c4'
BASE='reports/r1d26/result/pilot/'
FILE_EVENTS=BASE+'R1D26_FROZEN_PILOT_EVENTS_ASOF_ONLY.csv'
FILE_CAL=BASE+'official_indices_from_frozen_shard0.csv'
FILE_MANIFEST=BASE+'R1D26_COLLECT_MANIFEST.json'
FOOTER={'source_is_investor_KRW_certified':False,
        'historical_investor_model_ready':False,
        'sector_point_in_time_verified':False,
        'strategy_effect_estimated':False,
        'live_changed':False,'orders_placed':0,
        'n_external_observation_imputed':0}

class Reject(ValueError):pass

def require(ok,why):
    if not ok:raise Reject('R1D26B_FAIL_CLOSED_'+why)

def hsh(data):return hashlib.sha256(data).hexdigest()

def asof_case(source_bytes):
    require(hsh(source_bytes)==ORIGINAL_ZIP_SHA,'SOURCE_ZIP_DIGEST_MISMATCH')
    with zipfile.ZipFile(io.BytesIO(source_bytes)) as z:
        names=set(z.namelist())
        require({FILE_EVENTS,FILE_CAL,FILE_MANIFEST}.issubset(names),'SOURCE_FILES_MISSING')
        m=json.loads(z.read(FILE_MANIFEST))
        require(m['attempted_events']==20 and m['actual_real_observations']==0,'SOURCE_MANIFEST_IDENTITY')
        d=pd.read_csv(io.BytesIO(z.read(FILE_EVENTS)),dtype={'date':str,'code':str},low_memory=False)
        ix=pd.read_csv(io.BytesIO(z.read(FILE_CAL)),dtype={'date':str,'index':str})
    require(len(d)==20 and d[['date','code','strategy']].duplicated().sum()==0,'PILOT_POPULATION')
    require(repr(d['code'].dtype)=='dtype(\'O\')','TICKER_NOT_STRING')
    require(d['code'].map(lambda s:bool(re.fullmatch(r'\d{6}',s))).all(),'TICKER_FORMAT')
    days=ix.pivot(index='date',columns='index',values='close').sort_index()
    require(len(days)==1907 and set(days.columns)=={'KOSPI','KOSDAQ'},'CALENDAR_DRIFT')
    dates=list(days.index)
    # Use most recent source case for a short single-sample connectivity check.
    case=d.sort_values(['date','code'],ascending=[False,True]).iloc[0].to_dict()
    signal_ix=int(case['signal_ix'])
    require(dates[signal_ix]==case['date'],'CASE_INDEX_DRIFT')
    require(dates[signal_ix-20]==case['pre20_from'] and dates[signal_ix-1]==case['pre20_to'],'WINDOW_DRIFT')
    probe_days=dates[signal_ix-5:signal_ix]
    require(len(probe_days)==5 and all(x<case['date'] for x in probe_days),'FUTURE_LEAK')
    return case,probe_days

def safe_error(ex):
    s=re.sub(r'(?i)(token|key|secret|password|authorization)\s*[=:]\s*\S+',r'\1=REDACTED',str(ex))
    return (type(ex).__name__+': '+s)[:250]

def official_landing_probe(get):
    url='https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd?screenId=MDCSTAT023'
    try:
        r=get(url,timeout=12,allow_redirects=True,headers={'Accept':'text/html,application/xhtml+xml','User-Agent':'StockHunter-ResearchQA/1.0'})
        ct=r.headers.get('content-type','').lower(); status=r.status_code
        is_html=('html' in ct and '<html' in r.text[:3000].lower())
        return {'endpoint':'KRX_PUBLIC_LANDING_PAGE','http_status':status,'content_type':ct[:100],
                'html_verified':bool(status==200 and is_html),'bytes_received':len(r.content),
                'data_api_verified':False,'note':'LANDING_PAGE_IS_NOT_INVESTOR_JSON_API'}
    except Exception as ex:
        return {'endpoint':'KRX_PUBLIC_LANDING_PAGE','http_status':None,'html_verified':False,
                'data_api_verified':False,'error':safe_error(ex)}

def pykrx_probe(case,days,fetch):
    try:
        df=fetch(days[0].replace('-',''),days[-1].replace('-',''),case['code'])
        require(isinstance(df,pd.DataFrame) and not df.empty,'PYKRX_EMPTY_OR_WRONG_TYPE')
        require({'기관합계','외국인합계'}.issubset(df.columns),'PYKRX_MISSING_COLUMNS')
        got=[pd.Timestamp(x).date().isoformat() for x in df.index]
        require(len(got)==len(set(got)) and set(got)==set(days),'PYKRX_NOT_EXACT_5_SESSIONS')
        for k in ['기관합계','외국인합계']:
            v=pd.to_numeric(df[k],errors='coerce')
            require(v.notna().all() and (v%1==0).all(),'PYKRX_INVALID_INTEGER_KRW')
        require(sum(pd.to_numeric(df[k],errors='coerce').abs().sum() for k in ['기관합계','외국인합계'])>0,'PYKRX_SUSPICIOUS_ALL_ZERO')
        return {'provider':'KRX_VIA_PYKRX','status':'FIVE_ROWS_VALIDATED_PROVISIONAL',
                'verified_sessions':5,'unit':'KRW','same_date_publication_PIT_verified':False,
                'provider_values_present':True,'data_api_verified':True}
    except Exception as ex:
        return {'provider':'KRX_VIA_PYKRX','status':'FAILED_OR_UNVERIFIED',
                'verified_sessions':0,'unit':'KRW','provider_values_present':False,
                'same_date_publication_PIT_verified':False,'data_api_verified':False,'reason':safe_error(ex)}

def extract_naver_share_rows(html):
    from bs4 import BeautifulSoup
    soup=BeautifulSoup(html,'html.parser')
    out=[]
    for t in soup.select('table.type2'):
        for tr in t.select('tr'):
            cols=[c.get_text(' ',strip=True).replace('\xa0','') for c in tr.find_all('td')]
            if len(cols)<7:continue
            date=cols[0].replace('.','-')
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',date):continue
            try:
                values=[]
                for v in [cols[5],cols[6]]:
                    v=re.sub(r'[\s,]','',v)
                    if not re.fullmatch(r'[+-]?\d+',v):raise ValueError('not int')
                    values.append(int(v))
                out.append({'trade_date':date,'institution_net_shares':values[0],
                            'foreign_net_shares':values[1]})
            except ValueError:continue
    return out

def naver_probe(case,days,get,max_pages):
    require(0<=max_pages<=3,'NAVER_PAGE_CAP')
    if max_pages==0:
        return {'provider':'NAVER_FINANCE_SHARES_ONLY','status':'SKIPPED','rows':[],
                'unit':'SHARES_NOT_KRW','eligible_for_KRW_model':False}
    result=[];pages=[]
    try:
        for page in range(1,max_pages+1):
            r=get('https://finance.naver.com/item/frgn.naver',params={'code':case['code'],'page':page},
                  timeout=12,headers={'User-Agent':'StockHunter-ResearchQA/1.0','Accept-Language':'ko-KR'})
            ct=r.headers.get('content-type','').lower()
            pages.append({'page':page,'http_status':r.status_code,'content_type':ct[:100],
                          'bytes_received':len(r.content)})
            if r.status_code!=200 or 'html' not in ct:break
            raw=r.content.decode('euc-kr',errors='replace') if 'euc-kr' in ct or 'euc_kr' in ct else r.text
            result+=extract_naver_share_rows(raw)
            if set(days).issubset(set(x['trade_date'] for x in result)):break
        distinct={r['trade_date']:r for r in result}
        matched=[distinct[d] for d in days if d in distinct]
        return {'provider':'NAVER_FINANCE_SHARES_ONLY','status':('ALL_5_DAYS_SHARES_ONLY' if len(matched)==5 else 'NO_COMPLETE_HISTORICAL_WINDOW'),
                'unit':'SHARES_NOT_KRW','eligible_for_KRW_model':False,
                'publication_PIT_verified':False,'pages':pages,'parsed_total_days':len(distinct),
                'exact_requested_days_found':len(matched),'rows':matched}
    except Exception as ex:
        return {'provider':'NAVER_FINANCE_SHARES_ONLY','status':'FETCH_OR_PARSE_FAILURE',
                'unit':'SHARES_NOT_KRW','eligible_for_KRW_model':False,
                'publication_PIT_verified':False,'pages':pages,'rows':[], 'reason':safe_error(ex)}

def produce(source_zip,out,krx_get=None,pykrx_fetch=None,naver_get=None,naver_pages=1):
    import requests
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    case,days=asof_case(Path(source_zip).read_bytes())
    get=krx_get or requests.get
    pg=official_landing_probe(get)
    if pykrx_fetch is None:
        try:
            from pykrx import stock
            pykrx_fetch=lambda a,b,c:stock.get_market_trading_value_by_date(a,b,c,on='순매수')
        except Exception as ex:
            lib_error=safe_error(ex)
            def pykrx_fetch(a,b,c):raise RuntimeError(lib_error)
    py=pykrx_probe(case,days,pykrx_fetch)
    nv=naver_probe(case,days,naver_get or requests.get,naver_pages)
    at=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')
    diag={'version':VERSION,'source_zip_sha256':ORIGINAL_ZIP_SHA,
          'source_artifact_run_id':40256526209,'pilot_event_count':20,
          'case':{k:case[k] for k in ('date','code','strategy','pre20_from','pre20_to')},
          'requested_5_trading_dates':days,'run_timestamp_utc':at,'landing_probe':pg,
          'pykrx_krw_probe':py,'alternative_share_volume_probe':{k:v for k,v in nv.items() if k!='rows'},
          'verified_full_historical_KRW_20_sessions':0,
          'source_pilot_20of20_recovered':False,**FOOTER}
    # Never export incomplete 5 days to the verified 20-day historical money-flow table.
    (out/'R1D26B_SINGLE_CASE_SOURCE_DIAGNOSTIC.json').write_text(json.dumps(diag,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    path=out/'R1D26B_NAVER_SHARES_DIAGNOSTIC_NOT_KRW.csv'
    with path.open('w',newline='',encoding='utf-8-sig') as f:
        fields=['code','signal_date','trade_date','institution_net_shares','foreign_net_shares','unit','qualified_for_KRW_analysis']
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader()
        for v in nv.get('rows',[]):writer.writerow({'code':case['code'],'signal_date':case['date'],**v,'unit':'SHARES','qualified_for_KRW_analysis':False})
    print('R1D26B_SOURCE_DIAGNOSTIC_NO_STRATEGY_RESULTS',json.dumps(diag,ensure_ascii=False),flush=True)
    return diag

def main():
    pa=argparse.ArgumentParser()
    pa.add_argument('--source-zip',required=True)
    pa.add_argument('--out',default='reports/r1d26b/result/source_diagnostic')
    pa.add_argument('--naver-pages',type=int,default=1)
    a=pa.parse_args()
    require(0<=a.naver_pages<=3,'NAVER_PAGE_CAP')
    produce(a.source_zip,a.out,naver_pages=a.naver_pages)
if __name__=='__main__':main()
