#!/usr/bin/env python3
"""Research-only historical KRX exact-date market-cap+industry source collector.
Requires authenticated pykrx via Actions KRX_ID and KRX_PW. No secret/output-body logs.
Fail closed on missing dates, invalid data, authentication, duplicates, and code identity.
"""
from __future__ import annotations
import argparse, os, csv, hashlib, json, gzip, re, time
from datetime import datetime
from pathlib import Path
import pandas as pd

FIELDS=('date','code','company_name','market','industry','mcap_krw','close')
def norm(raw, market, date):
    required={'종목명','업종명','시가총액','종가'}
    if not isinstance(raw,pd.DataFrame) or raw.empty or not required.issubset(raw.columns):
        raise ValueError('EMPTY_OR_SCHEMA_INVALID_'+market)
    x=raw.copy()
    x['code']=[str(c).strip().zfill(6) for c in x.index]
    if not all(re.fullmatch(r'\d{6}',s) for s in x.code): raise ValueError('CODE_NOT_6_DIGITS')
    if x.code.duplicated().any():raise ValueError('DUPLICATE_CODE_'+market)
    caps=pd.to_numeric(x['시가총액'].astype(str).str.replace(',','',regex=False),errors='coerce')
    closes=pd.to_numeric(x['종가'].astype(str).str.replace(',','',regex=False),errors='coerce')
    ok=caps.notna() & (caps>0) & (caps%1==0) & closes.notna() & (closes>0) & x['업종명'].notna() & (x['업종명'].astype(str).str.strip()!='')
    rejected=int((~ok).sum())
    if ok.mean()<.98:raise ValueError('TOO_MANY_UNTRADEABLE_OR_MISSING_ROWS_'+market)
    x=x.loc[ok].copy();caps=caps.loc[ok];closes=closes.loc[ok]
    result=pd.DataFrame({'date':date,'code':x.code.values,'company_name':x['종목명'].astype(str).values,'market':market,'industry':x['업종명'].astype(str).values,'mcap_krw':caps.astype('int64').values,'close':closes.astype('float64').values},columns=list(FIELDS))
    result.attrs['excluded_invalid_source_rows']=rejected
    return result

def select_dates(path,year,quarter):
    assert year in range(2021,2026) and quarter in range(1,5)
    a=pd.read_csv(path,dtype=str)
    if list(a.columns)!=['date'] or a.date.isna().any():raise ValueError('BAD_FROZEN_CALENDAR')
    dates=pd.to_datetime(a.date,format='%Y-%m-%d',errors='raise')
    if dates.duplicated().any() or not dates.is_monotonic_increasing:raise ValueError('BAD_CALENDAR_ORDER')
    sel=a.loc[(dates.dt.year==year)&(dates.dt.quarter==quarter),'date'].tolist()
    if len(sel)<40 or len(sel)>70:raise ValueError('QUARTER_DATE_COUNT_OUTSIDE_EXPECTED')
    return sel

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def save(date,dfs,root):
    if len(dfs)!=2:raise ValueError('MARKET_SOURCE_COUNT')
    x=pd.concat(dfs,ignore_index=True)
    if x.code.duplicated().any():raise ValueError('MARKET_TICKER_COLLISION')
    if len(x)<800:raise ValueError('UNLIKELY_FEW_TICKERS')
    root=Path(root);dest=root/'daily'/f'{date.replace("-","")}.csv.gz';dest.parent.mkdir(parents=True,exist_ok=True)
    # Canonical text, deterministic gzip output (gzip mtime=0)
    by=x.sort_values(['market','code']).to_csv(index=False,lineterminator='\n').encode('utf-8')
    import io
    bio=io.BytesIO()
    with gzip.GzipFile(filename='',mode='wb',fileobj=bio,mtime=0,compresslevel=6) as f:f.write(by)
    tmp=dest.with_suffix('.tmp');tmp.write_bytes(bio.getvalue());tmp.replace(dest)
    return {'date':date,'name':dest.name,'sha256':sha(dest),'rows':len(x),'kospi':sum(x.market=='KOSPI'),'kosdaq':sum(x.market=='KOSDAQ')}

def run(year,quarter,calendar,out,fetch=None,sleep=0.3):
    root=Path(out);root.mkdir(parents=True,exist_ok=True)
    dates=select_dates(calendar,year,quarter)
    if not os.getenv('KRX_ID') or not os.getenv('KRX_PW'):raise ValueError('KRX_LOGIN_SECRETS_MISSING')
    if fetch is None:
        from pykrx import stock
        fetch=stock.get_market_sector_classifications
    done=[]
    for d in dates:
        filename=root/'daily'/f'{d.replace("-","")}.csv.gz'
        if filename.exists():
            val=pd.read_csv(filename,dtype={'code':str})
            if len(val)<800 or val['date'].nunique()!=1 or val.date.iloc[0]!=d or val.code.duplicated().any():raise ValueError('BAD_EXISTING_CHECKPOINT')
            result={'date':d,'name':filename.name,'sha256':sha(filename),'rows':len(val),'kospi':sum(val.market=='KOSPI'),'kosdaq':sum(val.market=='KOSDAQ')}
        else:
            # pykrx is the same KRX login provider as the successful 2-day futures canary.
            try:
                dfs=[norm(fetch(d.replace('-',''),m),m,d) for m in ('KOSPI','KOSDAQ')]
            except Exception as exc:
                (root/'STATUS.json').write_text(json.dumps({'status':'INCOMPLETE_PROVIDER_FAILURE','successful_days':len(done),'requested_days':len(dates),'failed_date':d,'error_type':type(exc).__name__},indent=2))
                raise RuntimeError('SOURCE_FAIL_CLOSED_'+type(exc).__name__) from None
            result=save(d,dfs,root)
            result['excluded_invalid_source_rows']=sum(df.attrs.get('excluded_invalid_source_rows',0) for df in dfs)
        done.append(result)
        (root/'MANIFEST_PARTIAL.json').write_text(json.dumps({'status':'PARTIAL','research_only':True,'days':done},indent=2,ensure_ascii=False),encoding='utf-8')
        print('R1D44_PIT_DATE_OK',d,'KOSPI',result['kospi'],'KOSDAQ',result['kosdaq'],flush=True)
        if sleep:time.sleep(sleep)
    (root/'MANIFEST_FINAL.json').write_text(json.dumps({'status':'COMPLETE','research_only':True,'n_dates':len(done),'days':done},indent=2,ensure_ascii=False),encoding='utf-8')
    (root/'STATUS.json').write_text(json.dumps({'status':'COMPLETE','n_dates':len(done),'n_rows':sum(r['rows'] for r in done)},indent=2))
    return done

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--year',type=int,required=True);ap.add_argument('--quarter',type=int,required=True);ap.add_argument('--calendar',required=True);ap.add_argument('--out',required=True);args=ap.parse_args();run(args.year,args.quarter,args.calendar,args.out)
if __name__=='__main__':main()
