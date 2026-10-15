#!/usr/bin/env python3
"""R1.7.1 research-only KRX per-session market snapshots -> offline per-ticker SQLite.

The old direct pykrx get_market_ohlcv_by_date(ticker, adjusted=False) resolves
current symbol->ISIN via a separate API, failing intermittently under Actions.
The verified get_market_ohlcv_by_ticker(session,market='ALL') path is used ONCE
per exchange session; stock histories are assembled offline. Any missing market
session, source-file tampering, field corruption, or calendar gap is fatal.
"""
from __future__ import annotations
import argparse, csv, gzip, hashlib, io, json, pathlib, sqlite3, time
import pandas as pd

COLS=['code','date','open','high','low','close','volume','amount']
MIN_HISTORY_START='2019-01-01' # original authority first observed 2019-06-21; 5+ months extra warmup


def sha(data):return hashlib.sha256(data).hexdigest()
def code(v):return str(v).strip().split('.')[0].zfill(6)


def official_sessions(stock,start,end):
    q=stock.get_index_ohlcv_by_date(start.replace('-',''),end.replace('-',''),'1001')
    if q is None or q.empty:raise RuntimeError('KRX_OFFICIAL_INDEX_CALENDAR_EMPTY')
    days=pd.DatetimeIndex(pd.to_datetime(q.index,errors='raise')).normalize().sort_values()
    days=[d.strftime('%Y-%m-%d') for d in days if pd.Timestamp(start)<=d<=pd.Timestamp(end)]
    if not days or days!=sorted(set(days)):raise RuntimeError('KRX_OFFICIAL_INDEX_CALENDAR_INVALID')
    return days


def normalize_day(raw,session,codes):
    if raw is None or not isinstance(raw,pd.DataFrame) or len(raw)<500:
        raise RuntimeError(f'KRX_SESSION_TOO_FEW_TICKERS:{0 if raw is None else len(raw)}')
    rename={'시가':'open','고가':'high','저가':'low','종가':'close','거래량':'volume','거래대금':'amount'}
    missing=set(rename)-set(raw.columns)
    if missing:raise RuntimeError('KRX_SESSION_MISSING_COLUMNS:'+','.join(sorted(missing)))
    t=raw.rename(columns=rename)[list(rename.values())].copy()
    t.insert(0,'code',[code(v) for v in raw.index]);t.insert(1,'date',session)
    if t.code.duplicated().any():raise RuntimeError('KRX_SESSION_DUPLICATE_TICKER')
    t=t[t.code.isin(codes)].copy()
    for k in COLS[2:]:t[k]=pd.to_numeric(t[k],errors='coerce')
    if t[COLS[2:]].isna().any().any():raise RuntimeError('KRX_SESSION_NUMERIC_INVALID')
    if (t[COLS[2:]]<0).any().any():raise RuntimeError('KRX_SESSION_NEGATIVE_BAR')
    if (t.high<t.low).any():raise RuntimeError('KRX_SESSION_BAD_RANGE')
    return t[COLS].sort_values('code').reset_index(drop=True)


def encode_day(frame):
    text=frame.to_csv(index=False,lineterminator='\n')
    return gzip.compress(text.encode('utf-8'),mtime=0)


def load_universe(p):
    obj=json.loads(pathlib.Path(p).read_text(encoding='utf-8'))
    v=obj.get('codes')
    if not isinstance(v,list) or len(v)<200:raise RuntimeError('KRX_UNIVERSE_INVALID')
    codes={code(x) for x in v}
    if not all(c.isdigit() and len(c)==6 for c in codes):raise RuntimeError('KRX_UNIVERSE_BAD_TICKER')
    return codes


def fetch_shard(stock,days,codes,index,count,output,retries=4):
    root=pathlib.Path(output);root.mkdir(parents=True,exist_ok=True)
    manifest={'format':'R171_DATED_KRX_DAILY_V1','history_start':days[0], 'asof':days[-1],
              'source':'PYKRX_KRX_DAILY_CROSS_SECTION_ALL', 'universe_codes':len(codes),
              'shard_index':index,'shard_count':count,'official_days':len(days),
              'official_calendar_sha256':sha('\n'.join(days).encode()),'files':[], 'failures':[]}
    (root/f'krx_history_manifest_{index:02d}.json').write_text(json.dumps(manifest,indent=2))
    for n,day in enumerate(days):
        if n%count!=index:continue
        stamp=day.replace('-','');target=root/f'{stamp}.csv.gz'
        failure=''
        for attempt in range(1,retries+1):
            try:
                source=stock.get_market_ohlcv_by_ticker(stamp,market='ALL')
                normalized=normalize_day(source,day,codes)
                data=encode_day(normalized)
                target.write_bytes(data)
                manifest['files'].append({'day':day,'file':target.name,'sha256':sha(data),'rows':len(normalized),'all_market_rows':len(source)})
                failure='';break
            except Exception as e:
                failure=f'{type(e).__name__}:{str(e)[:180]}'
                if attempt<retries:time.sleep(min(2*attempt,8))
        if failure:
            manifest['failures'].append({'day':day,'error':failure})
            print('KRX_HISTORY_DAY_FAIL',day,failure,flush=True)
        if len(manifest['files'])%25==0 and not failure:print(f"KRX_HISTORY_PROGRESS shard={index} sessions={len(manifest['files'])}/{(len(days)+count-1)//count} day={day}",flush=True)
        (root/f'krx_history_manifest_{index:02d}.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    expected=len(days[index::count]);actual=len(manifest['files'])
    if actual!=expected or manifest['failures']:
        raise RuntimeError(f'KRX_MATERIALIZE_INCOMPLETE:{actual}/{expected};failures={len(manifest["failures"])}')
    manifest['status']='COMPLETE'
    (root/f'krx_history_manifest_{index:02d}.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    return manifest


def build_db(root,database,expected_shards=4,universe_file=None):
    root=pathlib.Path(root);database=pathlib.Path(database);database.parent.mkdir(parents=True,exist_ok=True)
    manifests=[json.loads((root/f'krx_history_manifest_{i:02d}.json').read_text(encoding='utf-8')) for i in range(expected_shards)]
    common={(m['history_start'],m['asof'],m['official_days'],m['official_calendar_sha256'],m['universe_codes'],m['shard_count']) for m in manifests}
    if len(common)!=1 or any(m.get('status')!='COMPLETE' or m['shard_index']!=i for i,m in enumerate(manifests)):
        raise RuntimeError('KRX_MATERIALIZE_MANIFEST_INCONSISTENT')
    catalog=[f for m in manifests for f in m['files']]
    expected=manifests[0]['official_days']
    if len(catalog)!=expected or len({v['day'] for v in catalog})!=expected:
        raise RuntimeError('KRX_MATERIALIZE_SESSION_COVERAGE_INCOMPLETE')
    days=sorted(x['day'] for x in catalog)
    if sha('\n'.join(days).encode())!=manifests[0]['official_calendar_sha256']:
        raise RuntimeError('KRX_MATERIALIZE_CALENDAR_SHA_MISMATCH')
    universe=load_universe(universe_file) if universe_file else None
    if universe is not None and len(universe)!=manifests[0]['universe_codes']:
        raise RuntimeError('KRX_MATERIALIZE_UNIVERSE_DRIFT')
    if database.exists():database.unlink()
    cx=sqlite3.connect(str(database));inserted=0
    try:
        cx.execute('CREATE TABLE bars (code TEXT NOT NULL, date TEXT NOT NULL, open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, volume REAL NOT NULL, amount REAL NOT NULL, PRIMARY KEY (code,date)) WITHOUT ROWID')
        for n,meta in enumerate(sorted(catalog,key=lambda x:x['day']),1):
            data=(root/meta['file']).read_bytes()
            if sha(data)!=meta['sha256']:raise RuntimeError('KRX_MATERIALIZE_FILE_SHA_MISMATCH:'+meta['file'])
            obj=pd.read_csv(io.BytesIO(gzip.decompress(data)),dtype={'code':str,'date':str},low_memory=False)
            if list(obj.columns)!=COLS or len(obj)!=meta['rows'] or obj.date.nunique()!=1 or obj.date.iloc[0]!=meta['day']:
                raise RuntimeError('KRX_MATERIALIZE_DAILY_SCHEMA_DRIFT:'+meta['day'])
            if universe is not None and not set(obj.code).issubset(universe):raise RuntimeError('KRX_MATERIALIZE_OUT_OF_UNIVERSE')
            vals=list(obj[COLS].itertuples(index=False,name=None))
            cx.executemany('INSERT INTO bars VALUES (?,?,?,?,?,?,?,?)', vals)
            inserted+=len(vals)
            if n%100==0:print(f'KRX_SQLITE_INGEST {n}/{len(catalog)} rows={inserted}',flush=True)
        cx.execute('CREATE TABLE evidence (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        for k,v in {'format':'R171_DATED_KRX_DAILY_V1','history_start':days[0], 'asof':days[-1],
                    'official_days':str(expected),'calendar_sha256':manifests[0]['official_calendar_sha256'],
                    'universe_codes':str(manifests[0]['universe_codes']),'price_rows':str(inserted)}.items():
            cx.execute('INSERT INTO evidence VALUES(?,?)',(k,v))
        cx.commit()
    except Exception:
        cx.close()
        if database.exists():database.unlink()
        raise
    cx.close()
    return {'price_rows':inserted,'official_days':expected,'history_start':days[0],'asof':days[-1], 'db':str(database)}


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--mode',choices=['fetch','build'],required=True);p.add_argument('--universe',required=True)
    p.add_argument('--start',default=MIN_HISTORY_START);p.add_argument('--asof',default='2026-10-12')
    p.add_argument('--shard',type=int,default=0);p.add_argument('--shards',type=int,default=4)
    p.add_argument('--root',required=True);p.add_argument('--database',default='krx_history.db')
    args=p.parse_args()
    if args.mode=='fetch':
        if not 0<=args.shard<args.shards:raise SystemExit('BAD_HISTORY_SHARD_INDEX')
        from pykrx import stock
        dates=official_sessions(stock,args.start,args.asof)
        r=fetch_shard(stock,dates,load_universe(args.universe),args.shard,args.shards,args.root)
    else:r=build_db(args.root,args.database,args.shards,args.universe)
    print('R171_KRX_HISTORY',json.dumps(r,ensure_ascii=False,default=str),flush=True)

if __name__=='__main__':main()
