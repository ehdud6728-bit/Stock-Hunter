#!/usr/bin/env python3
"""Research-only chronological KRX-login futures contract collector.

Collect a predeclared calendar quarter of KOSPI200 and KOSDAQ150 futures contract
rows using the same pykrx surface verified by GitHub run 40967892874.
Authentication is via KRX_ID/KRX_PW ENV, never in logs or output.
No LIVE imports, trade orders, continuous roll assumptions or future outcomes.
"""
from __future__ import annotations
import argparse
import contextlib
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import sys
import time

VERSION='R1D40_POST_CANARY_QUARTER_SOURCE_V1'
PRODUCTS={'KOSPI200':'KRDRVFUK2I','KOSDAQ150':'KRDRVFUKQI'}
REQUIRED=('종목명','시가','고가','저가','종가','거래량')
ALLOWED_YEARS=tuple(range(2021,2026))


def atomic_json(path:Path, obj):
    path.parent.mkdir(parents=True,exist_ok=True)
    payload=json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2,allow_nan=False)+'\n'
    tmp=path.with_suffix(path.suffix+'.part')
    tmp.write_text(payload,encoding='utf8')
    tmp.replace(path)


def sha_file(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for block in iter(lambda:f.read(1<<20),b''):h.update(block)
    return h.hexdigest()


def num(x):
    try:
        v=float(str(x).replace(',','').strip())
        return v if math.isfinite(v) else None
    except (ValueError,TypeError,OverflowError):return None


def validate_contracts(frame, day, product):
    import pandas as pd
    if not isinstance(frame,pd.DataFrame) or frame.empty:
        raise ValueError('EMPTY_OR_NONDATAFRAME')
    if not set(REQUIRED).issubset(frame.columns):raise ValueError('REQUIRED_COLUMNS_MISSING')
    target='코스피200' if product=='KOSPI200' else '코스닥150'
    rows=[];bad=0;excluded=0
    for code,r in frame.iterrows():
        name=str(r['종목명']).strip(); norm=name.replace(' ','').upper()
        if target not in norm or 'SP' in norm or '옵션' in norm or ('F' not in norm and '선물' not in norm):
            excluded+=1;continue
        n={k:num(r[k]) for k in ('시가','고가','저가','종가','거래량')}
        if (any(n[k] is None or n[k]<=0 for k in ('시가','고가','저가','종가')) or
            n['거래량'] is None or n['거래량']<0 or
            n['고가']+1e-8<max(n['시가'],n['종가']) or
            n['저가']-1e-8>min(n['시가'],n['종가']) or
            abs(n['거래량']-round(n['거래량']))>1e-7):
            bad+=1;continue
        code=str(code).strip()
        if not code or code.lower()=='nan' or len(code)>48:bad+=1;continue
        rows.append({'date':day,'product':product,'product_code':PRODUCTS[product],
                     'contract_code':code,'contract_name':name[:120],
                     'open':n['시가'],'high':n['고가'],'low':n['저가'],
                     'close':n['종가'],'volume':int(n['거래량'])})
    if not rows:raise ValueError('NO_VALID_TARGET_CONTRACTS')
    codes=[x['contract_code'] for x in rows]
    if len(codes)!=len(set(codes)):raise ValueError('DUPLICATE_CONTRACT_CODES')
    rows.sort(key=lambda x:(-x['volume'],x['contract_code']))
    return rows,{'provider_row_count':len(frame),'target_valid_contracts':len(rows),
                 'target_invalid_rows':bad,'excluded_nontarget_rows':excluded}


def quarter_dates(calendar_path:Path,year,quarter):
    if year not in ALLOWED_YEARS or quarter not in (1,2,3,4):raise ValueError('INVALID_QUARTER')
    dates=[]
    with calendar_path.open(encoding='utf8',newline='') as f:
        reader=csv.DictReader(f)
        if reader.fieldnames!=['date']:raise ValueError('FROZEN_CALENDAR_SCHEMA')
        for r in reader:
            s=r['date']
            if not re.fullmatch(r'20\d\d-\d\d-\d\d',s):raise ValueError('INVALID_CALENDAR_DATE')
            dt.date.fromisoformat(s)
            dates.append(s)
    if len(dates)!=1225 or sorted(dates)!=dates or len(set(dates))!=len(dates):
        raise ValueError('FROZEN_CALENDAR_NOT_1225_UNIQUE_SORTED')
    if dates[0]<'2021-01-01' or dates[-1]>'2025-12-31':raise ValueError('CALENDAR_BOUNDS')
    part=[s.replace('-','') for s in dates if int(s[:4])==year and (int(s[5:7])-1)//3+1==quarter]
    if len(part)<50 or len(part)>67:raise ValueError('QUARTER_SESSION_COUNT')
    return part


def run(calendar_path,year,quarter,out,provider=None,env=None,pause=0.25):
    env=os.environ if env is None else env
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    dates=quarter_dates(Path(calendar_path),year,quarter)
    report={'version':VERSION,'research_only':True,'live_changed':False,'orders_placed':0,
            'prospective_oos':False,'source':'PYKRX_KRX_LOGIN_ENV',
            'prior_successful_canary_run':40967892874,
            'quarter':f'{year}-Q{quarter}','calendar_sha256':sha_file(calendar_path),
            'session_count':len(dates),'expected_requests':len(dates)*2,
            'secret_presence':{'KRX_ID':bool(str(env.get('KRX_ID','')).strip()),
                               'KRX_PW':bool(str(env.get('KRX_PW','')).strip())},
            'credentials_logged':False,'status':'NOT_RUN','completed_requests':0,
            'failed_requests':[],'day_contract_manifest':[]}
    if not all(report['secret_presence'].values()):
        report['status']='CREDENTIALS_NOT_INJECTED'
        atomic_json(out/'COLLECTION_AUDIT.json',report)
        return report
    if provider is None:
        try:
            from pykrx import stock
            provider=stock.get_future_ohlcv_by_ticker
        except Exception:
            report['status']='FUTURES_PROVIDER_NOT_AVAILABLE'
            atomic_json(out/'COLLECTION_AUDIT.json',report)
            return report
    all_rows=[]
    for day in dates:
        for product,prod_code in PRODUCTS.items():
            target=out/'daily'/f'{day}_{product}.json'
            stats=None;rows=None;exception_class=None
            if target.exists():
                try:
                    saved=json.loads(target.read_text(encoding='utf8'))
                    if saved.get('date')==day and saved.get('product')==product and saved.get('version')==VERSION:
                        test=saved.get('rows')
                        if isinstance(test,list) and test and all(x['date']==day and x['product']==product and x['product_code']==prod_code for x in test):
                            rows=test;stats=saved['stats']
                except (ValueError,KeyError,TypeError):pass
            if rows is None:
                try:
                    with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
                        df=provider(day,prod_code,alternative=False)
                    rows,stats=validate_contracts(df,day,product)
                    atomic_json(target,{'version':VERSION,'date':day,'product':product,'rows':rows,'stats':stats})
                except Exception as exc:
                    # Never persist exception messages, session cookies, URLs or raw responses.
                    exception_class=type(exc).__name__[:60]
                    report['failed_requests'].append({'date':day,'product':product,'failure_class':exception_class})
                if pause:time.sleep(pause)
            if rows is not None:
                report['completed_requests']+=1
                report['day_contract_manifest'].append({'date':day,'product':product,
                    'contract_count':len(rows),'sha256':sha_file(target),**stats})
                all_rows.extend(rows)
            atomic_json(out/'COLLECTION_AUDIT.json',report)
    # Write only fully validated quarter data; incomplete quarters remain audit-only.
    if report['completed_requests']!=report['expected_requests']:
        report['status']='INCOMPLETE_FAIL_CLOSED'
        stale=out/'VALIDATED_CONTRACTS.csv.gz'
        if stale.exists(): stale.unlink()
        atomic_json(out/'COLLECTION_AUDIT.json',report)
        return report
    keys=['date','product','product_code','contract_code','contract_name','open','high','low','close','volume']
    target=out/'VALIDATED_CONTRACTS.csv.gz'
    with gzip.open(target,'wt',encoding='utf8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=keys,lineterminator='\n');w.writeheader();w.writerows(all_rows)
    report['validated_contract_rows']=len(all_rows)
    report['validated_data_sha256']=sha_file(target)
    report['status']='COMPLETE_SOURCE_ONLY_NO_STRATEGY_INFERENCE'
    atomic_json(out/'COLLECTION_AUDIT.json',report)
    return report


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--calendar',default='frozen_krx_sessions_2021_2025.csv')
    ap.add_argument('--year',type=int,required=True)
    ap.add_argument('--quarter',type=int,required=True)
    ap.add_argument('--out',default='reports/r1d40-quarter-source')
    ap.add_argument('--pause',type=float,default=0.25)
    a=ap.parse_args()
    res=run(a.calendar,a.year,a.quarter,a.out,pause=a.pause)
    print('R1D40_FUTURES_QUARTER',res['status'],'validated',res['completed_requests'],'/',res['expected_requests'],
          'contracts',res.get('validated_contract_rows',0),'secrets_not_logged=true')
    return 0 if res['status']=='COMPLETE_SOURCE_ONLY_NO_STRATEGY_INFERENCE' else 2

if __name__=='__main__':sys.exit(main())
