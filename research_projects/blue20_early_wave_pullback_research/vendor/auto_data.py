#!/usr/bin/env python3
"""Immutable-source, point-in-time cohort preparation for R118 RESEARCH_ONLY.

Public FinanceData marcap Parquet (KRX-derived OHLCVA) and FDR index cache
are downloaded at PINNED commit and verified against Git blob SHA-1.
Selection is at 2022 final trading close; no post-2022 winner selection.
These are third-party archival copies, not a verified native KRX login pull.
"""
from __future__ import annotations
import argparse,csv,hashlib,json,os,time,urllib.request
from pathlib import Path
import pandas as pd

MARCAP_REPO="FinanceData/marcap"
MARCAP_COMMIT="50f4d1a74e87c720a03e576218036b1b8414afa1"
MARCAP_SHA={
 2022:"60ee136c6309671fb112b32ff190f7b774c3fad8",
 2023:"ecc188b0bad196701da3a0defded60c13b50ff43",
 2024:"b69c5222d015c81f19f90f581faabe4dd1a919b4",
}
INDEX_REPO="FinanceData/fdr_krx_data_cache"
INDEX_COMMIT="a06c025cd5783fc4c4488d605ab4fddfe7ae5b2d"
INDEX_SHA={
 ('KOSPI',2022):"9b1c1d6c1094f4d1c7d4491fc52897c2bbbb525c",
 ('KOSPI',2023):"17de21b6086d67a84205a04d1bfab7e40494a13a",
 ('KOSPI',2024):"2b4faadcb3a345f8615d73ef235d7c711b8d7f25",
 ('KOSDAQ',2022):"cb6c8dceb1e5dfc31390b17687ed7b31fcc3717f",
 ('KOSDAQ',2023):"7014e80631eb1507fb0f555d9e4d530e127fe34e",
 ('KOSDAQ',2024):"b861bd92c7f69370f94fd2a9307ca34bf32cf2f2",
}
YEARS=(2022,2023,2024)


def blob_hash(data):return hashlib.sha1(b"blob "+str(len(data)).encode()+b"\0"+data).hexdigest()
def sha256(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def download_locked(url,path,sha,fetcher=None):
    """Source binary must exactly match Git blob at independently recorded commit."""
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():
        b=path.read_bytes()
        if blob_hash(b)==sha:return path
        raise ValueError(f"PINNED_SOURCE_CHANGED: cached file {path}")
    if fetcher is None:
        def fetcher(u):
            req=urllib.request.Request(u,headers={"User-Agent":"StockHunter-R118-ResearchOnly/1.0"})
            with urllib.request.urlopen(req,timeout=90) as res:
                if res.status!=200:raise RuntimeError(f"HTTP {res.status}")
                return res.read()
    error=None
    for attempt in range(3):
        try:
            b=fetcher(url)
            if blob_hash(b)!=sha:raise ValueError(f"PINNED_SOURCE_SHA_MISMATCH {path.name}: got {blob_hash(b)} expected {sha}")
            tmp=path.with_name(path.name+'.tmp');tmp.write_bytes(b);tmp.replace(path)
            return path
        except ValueError:raise
        except Exception as e:
            error=e
            if attempt<2:time.sleep(1+attempt)
    raise RuntimeError(f"SOURCE_DOWNLOAD_FAILED {url}: {error}")


def normalize_marcap(frame,year):
    d=frame.reset_index().copy() if 'Date' not in frame.columns else frame.copy()
    if 'Date' not in d.columns:
        raise ValueError(f"Marcap year {year} lacks historical Date column")
    expected={'Code','Market','Open','High','Low','Close','Volume','Amount','Marcap'}
    if not expected.issubset(d):raise ValueError(f"Marcap schema {year}: missing {sorted(expected-set(d.columns))}")
    d=d[list(expected|{'Date'})].copy()
    d['date']=pd.to_datetime(d['Date'],errors='raise').dt.strftime('%Y-%m-%d')
    d['code']=d['Code'].astype(str).str.strip().str.zfill(6)
    d['market']=d['Market'].astype(str).str.strip().str.upper()
    d=d[d.market.isin(['KOSPI','KOSDAQ']) & d.code.str.fullmatch(r'\d{6}')].copy()
    for k in ['Open','High','Low','Close','Volume','Amount','Marcap']:
        d[k]=pd.to_numeric(d[k],errors='coerce')
    invalid=(d[['Open','High','Low','Close']].le(0).any(axis=1)
        | d[['Open','High','Low','Close','Volume','Amount','Marcap']].isna().any(axis=1)
        | d[['Volume','Amount']].lt(0).any(axis=1)
        | d['High'].lt(d[['Open','Close','Low']].max(axis=1))
        | d['Low'].gt(d[['Open','Close','High']].min(axis=1)))
    count=int(invalid.sum());d=d.loc[~invalid].copy()
    if d.duplicated(['code','date']).any():raise ValueError(f"Marcap duplicate code/date {year}")
    if not d.date.str.startswith(str(year)).all():raise ValueError(f"Year/date mismatch {year}")
    return d,count


def make_cohort(annual,*,count=200,cutoff='2022-12-31',start='2023-01-01',end='2024-12-31',min_history=120):
    """Select ranked stocks using ONLY <2023-01-01 information."""
    if not 1<=count<=500:raise ValueError('1-500 stocks only')
    if start<=cutoff:raise ValueError('PIT_COHORT_SELECTION_LEAKAGE: research start must be after cutoff')
    old=annual[2022];snapshot=old[old.date<=cutoff]
    if snapshot.empty:raise ValueError('NO_PRE_STUDY_SNAPSHOT')
    asof=snapshot.date.max();snap=snapshot[snapshot.date==asof].copy()
    hist=(old[(old.date<=asof)].groupby('code').size()).to_dict()
    snap=snap[snap.code.map(lambda c:hist.get(c,0)>=min_history) & snap.Marcap.gt(0)].copy()
    snap=snap.sort_values(['Marcap','code'],ascending=[False,True],kind='stable')
    if len(snap)<count:raise ValueError(f'ASOF_ELIGIBLE_BELOW_TARGET {len(snap)} < {count}')
    picks=snap.head(count).code.astype(str).tolist()
    return picks,asof


def build_inputs(annual,indices,out,*,count=200,start='2023-01-01',end='2024-12-31',sha_sources=None):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    codes,asof=make_cohort(annual,count=count,start=start,end=end)
    valid=pd.concat([annual[y] for y in YEARS],ignore_index=True)
    valid=valid[valid.code.isin(codes) & (valid.date<=end)].copy()
    raw=out/'ohlcv';raw.mkdir(exist_ok=True)
    manifest=[];member=[];lengths={}
    for c in sorted(codes):
        a=valid[valid.code==c].sort_values('date').copy()
        if a.empty:raise ValueError('NO_OHLCV_FOR_ASOF_STOCK '+c)
        d=a.rename(columns={'Open':'open','High':'high','Low':'low','Close':'close','Volume':'volume','Amount':'amount'})
        d=d[['code','date','open','high','low','close','volume','amount']]
        if d.duplicated('date').any():raise ValueError('duplicate stock/date '+c)
        f=raw/(c+'.csv');d.to_csv(f,index=False,encoding='utf-8-sig')
        manifest.append({'code':c,'path':str(Path('ohlcv')/(c+'.csv')),'sha256':sha256(f)})
        length0=len(d);lengths[c]=length0
        one=a[['code','date','market']].copy();one['sector']='UNVERIFIED_SECTOR'
        member.append(one[['code','date','sector','market']])
    m=pd.DataFrame(manifest)
    if len(m)!=count:raise AssertionError('Cohort lost a selected name')
    m.to_csv(out/'manifest.csv',index=False)
    s=pd.concat(member,ignore_index=True).sort_values(['code','date'])
    if s.duplicated(['code','date']).any():raise ValueError('PIT market membership duplicate')
    s.to_csv(out/'sector_pit.csv',index=False)
    mk=pd.concat([indices[k] for k in sorted(indices)],ignore_index=True)
    if mk.duplicated(['market','date']).any():raise ValueError('Index duplicate market/date')
    mk=mk.sort_values(['market','date'])
    mk.to_csv(out/'market_daily.csv',index=False)
    audit={'mode':'REAL_HISTORICAL_KRX_DERIVED_PUBLIC_PINNED','source':'FinanceData/marcap + FinanceData/fdr_krx_data_cache',
           'source_git_commits':{'marcap':MARCAP_COMMIT,'index':INDEX_COMMIT},
           'selection_method':'TOP_MARKET_CAP_AT_PRE_STUDY_CUTOFF_WITH_PAST_ONLY_HISTORY',
           'selection_asof':asof,'first_study_date':start,'last_study_date':end,
           'selected_count':len(codes),'selected_codes':sorted(codes),
           'signal_selection_uses_no_post_cutoff_winner_information':True,
           'source_file_sha256':sha_sources or {},'ohlcv_rows_by_code':lengths,
           'industry_sector_status':'UNVERIFIED_SECTOR_ALL_ROWS_NOT_A_REAL_SECTOR_PIT',
           'market_status':'HISTORICAL_MARKET_FROM_MARCAP_DAILY',
           'limitations':['This fixed 2022 cohort excludes future listings after cutoff; not daily dynamic TOP500.',
             'Adjusted/unadjusted corporate actions and dividends are NOT independently certified.',
             'Archive providers are third-party KRX-derived records, NOT live KRX authentication.',
             'No real historical industry sector classification in input.',
             'Historical 2023-2024 performance is not pre-registered OOS.']}
    (out/'R118_AUTO_SOURCE_AUDIT.json').write_text(json.dumps(audit,indent=2,ensure_ascii=False),encoding='utf-8')
    return {'manifest':out/'manifest.csv','sector':out/'sector_pit.csv','market':out/'market_daily.csv','audit':audit}


def load_pinned_sources(cache):
    cache=Path(cache);years={};sources={};missing={}
    for y,sha in MARCAP_SHA.items():
        rel=f'data/marcap-{y}.parquet'
        url=f'https://raw.githubusercontent.com/{MARCAP_REPO}/{MARCAP_COMMIT}/{rel}'
        f=download_locked(url,cache/f'marcap-{y}.parquet',sha)
        raw=pd.read_parquet(f)
        d,bad=normalize_marcap(raw,y);years[y]=d
        sources[rel]={'git_blob_sha1':sha,'sha256':sha256(f),'rows':len(d),'invalid_removed':bad}
    idx={}
    for (market,y),sha in INDEX_SHA.items():
        bn='year_ks11' if market=='KOSPI' else 'year_kq11'
        rel=f'data/index/{bn}/{y}.csv';url=f'https://raw.githubusercontent.com/{INDEX_REPO}/{INDEX_COMMIT}/{rel}'
        f=download_locked(url,cache/f'{market}_{y}.csv',sha)
        x=pd.read_csv(f,encoding='utf-8-sig')
        if not {'Date','Close','Amount'}.issubset(x):raise ValueError('index schema mismatch '+rel)
        d=x.rename(columns={'Date':'date','Close':'close','Amount':'amount'})[['date','close','amount']].copy()
        d['date']=pd.to_datetime(d.date).dt.strftime('%Y-%m-%d');d['market']=market
        d['close']=pd.to_numeric(d.close,errors='raise');d['amount']=pd.to_numeric(d.amount,errors='raise')
        if d[['close','amount']].isna().any().any() or d.close.le(0).any() or d.amount.lt(0).any():raise ValueError('invalid index')
        idx[market,y]=d[['market','date','close','amount']]
        sources[rel]={'git_blob_sha1':sha,'sha256':sha256(f),'rows':len(d)}
    return years,idx,sources


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--out',required=True)
    p.add_argument('--cache',default='research_projects/r1d118/.local_source_cache')
    p.add_argument('--count',type=int,default=200)
    args=p.parse_args(argv)
    years,indices,sources=load_pinned_sources(args.cache)
    result=build_inputs(years,indices,args.out,count=args.count,sha_sources=sources)
    print(json.dumps({'selected_count':result['audit']['selected_count'],'asof':result['audit']['selection_asof'],
                      'manifest':str(result['manifest']),'source_files':len(sources)},ensure_ascii=False))

if __name__=='__main__':main()
