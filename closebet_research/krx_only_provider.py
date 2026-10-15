"""R1.7.1 research-only provider. Read verified KRX day-scoped SQLite; no per-code HTTP.

Activated only by the separate KRX Actions research workflow sitecustomize flag.
Always uses original unadjusted KRX cross-sectional daily observations and never
claims the data are certified 15:30-only session prices.
"""
from __future__ import annotations
import atexit, datetime as dt, json, os, pathlib, sqlite3, threading
from contextlib import closing
_LOCK=threading.Lock();_INSTALLED=False;_PERSIST=None
_STATS={'patch_active':False,'fdr_calls':0,'sqlite_reads':0,'krx_price_calls':0,'index_calls':0,
        'fdr_rows':0,'fdr_failures':0,'direct_raw_errors':0,'fdr_empty_history':0,
        'forced_adjusted_false':0,'out_of_scope_end_clipped':0,'early_history_requests':0,
        'errors':[],'codes':[],'start_end_examples':[]}

def _inc(k,n=1):
    with _LOCK:_STATS[k]=_STATS.get(k,0)+n

def _fail(kind,ticker,error):
    _inc(kind)
    with _LOCK:
        if len(_STATS['errors'])<75:_STATS['errors'].append(f'{ticker}:{type(error).__name__}:{str(error)[:155]}')
    if _PERSIST:_PERSIST()

def _parse(v):
    if isinstance(v,dt.datetime):return v.date()
    if isinstance(v,dt.date):return v
    x=str(v).strip()[:10]; fmt='%Y%m%d' if x.isdigit() and len(x)==8 else '%Y-%m-%d'
    return dt.datetime.strptime(x,fmt).date()

def _dates(start,end):
    if not os.environ.get('CLOSING_BET_KRX_REQUEST_ASOF'):raise RuntimeError('KRX_RESEARCH_ASOF_UNSET')
    last=_parse(os.environ['CLOSING_BET_KRX_REQUEST_ASOF'])
    if start is None:raise RuntimeError('KRX_FDR_START_DATE_REQUIRED')
    d0=_parse(start);d1=_parse(end) if end is not None else last
    if d1>last:_inc('out_of_scope_end_clipped');d1=last
    if d0>d1:return None,None
    return d0.strftime('%Y%m%d'),d1.strftime('%Y%m%d')

def _convert(raw):
    import pandas as pd
    if raw is None or raw.empty:raise ValueError('KRX_EMPTY_HISTORY')
    ren={'시가':'Open','고가':'High','저가':'Low','종가':'Close','거래량':'Volume','거래대금':'Amount'}
    x=raw.rename(columns=ren).copy()
    if not set(ren.values()).issubset(x.columns):raise ValueError('KRX_HISTORY_BAD_SCHEMA:'+str(list(x.columns)))
    for k in ren.values():x[k]=pd.to_numeric(x[k],errors='coerce')
    x.index=pd.to_datetime(x.index,errors='raise').normalize()
    x=x.loc[~x.index.duplicated(keep='last')].sort_index().dropna(subset=list(ren.values()))
    if x.empty:raise ValueError('KRX_ALL_NULL_HISTORY')
    if (x[list(ren.values())]<0).any().any():raise ValueError('KRX_NEGATIVE_OHLCV')
    x['Change']=x['Close'].pct_change() # FDR expected FRACTION not percent
    return x

def _db_meta():
    path=os.environ.get('CLOSING_BET_KRX_DATABASE','')
    if not path or not pathlib.Path(path).is_file():raise RuntimeError('KRX_PREMATERIALIZED_DB_MISSING')
    with closing(sqlite3.connect(f'file:{pathlib.Path(path).resolve().as_posix()}?mode=ro',uri=True)) as cx:
        meta=dict(cx.execute('SELECT key,value FROM evidence').fetchall())
    if meta.get('format')!='R171_DATED_KRX_DAILY_V1':raise RuntimeError('KRX_DB_FORMAT_INVALID')
    if meta.get('asof')!=os.environ.get('CLOSING_BET_KRX_REQUEST_ASOF'):
        raise RuntimeError('KRX_DB_ASOF_MISMATCH')
    return path,meta

def _fetch_sqlite(start,end,ticker):
    import pandas as pd
    _inc('sqlite_reads')
    a,b=_dates(start,end)
    if a is None:return pd.DataFrame()
    if not (isinstance(ticker,str) and len(ticker)==6 and ticker.isdigit()):
        raise RuntimeError('KRX_NON_KRX_TICKER_FORBIDDEN:'+str(ticker))
    path,meta=_db_meta()
    lo=meta['history_start'].replace('-','')
    if a<lo:
        _inc('early_history_requests')
        raise RuntimeError(f'KRX_HISTORY_START_NOT_COVERED:{a}<{lo}; enlarge materialized start')
    with closing(sqlite3.connect(f'file:{pathlib.Path(path).resolve().as_posix()}?mode=ro',uri=True)) as cx:
        q=pd.read_sql_query('SELECT date,open,high,low,close,volume,amount FROM bars WHERE code=? AND date>=? AND date<=? ORDER BY date',cx,
            params=[ticker,_parse(a).isoformat(),_parse(b).isoformat()])
    if q.empty:return pd.DataFrame()
    q=q.set_index('date');q.index=pd.to_datetime(q.index)
    return q.rename(columns={'open':'시가','high':'고가','low':'저가','close':'종가','volume':'거래량','amount':'거래대금'})

def install():
    global _INSTALLED,_PERSIST
    if _INSTALLED or os.environ.get('CLOSING_BET_KRX_ONLY_RESEARCH')!='1':return
    _db_meta()  # reject environment without a fully materialized KRX database
    from pykrx import stock
    import FinanceDataReader as fdr
    original_index=stock.get_index_ohlcv_by_date
    def get_raw(start,end,ticker,*args,**kwargs):
        _inc('krx_price_calls')
        if args and not isinstance(args[0],bool):raise RuntimeError('KRX_UNEXPECTED_ADJUSTED_POSITIONAL')
        if len(args)>1 or any(k not in ('adjusted','name_display') for k in kwargs):
            raise RuntimeError('KRX_UNSUPPORTED_OHLCV_KWARGS')
        if args and args[0] is not False or kwargs.get('adjusted',False) is not False:_inc('forced_adjusted_false')
        try:return _fetch_sqlite(start,end,str(ticker).zfill(6))
        except Exception as e:_fail('direct_raw_errors',ticker,e);raise
    def reader(symbol,start=None,end=None,*args,**kwargs):
        _inc('fdr_calls')
        try:
            if args or kwargs:raise RuntimeError('KRX_FDR_UNEXPECTED_ARGUMENTS')
            symbol=str(symbol)
            a,b=_dates(start,end)
            if a is None:
                import pandas as pd
                return pd.DataFrame(columns=['Open','High','Low','Close','Volume','Amount','Change'])
            if symbol in ('KS11','KOSPI','KS200','KQ11','KOSDAQ'):
                idx={'KS11':'1001','KOSPI':'1001','KS200':'1028','KQ11':'2001','KOSDAQ':'2001'}[symbol]
                _inc('index_calls')
                # Index is KRX native, not individual stock; index API is separately audited.
                frame=original_index(a,b,idx)
            elif len(symbol)==6 and symbol.isdigit():frame=_fetch_sqlite(a,b,symbol)
            else:raise RuntimeError('KRX_NON_KRX_SYMBOL_FORBIDDEN:'+symbol)
            if frame is None or frame.empty:
                _inc('fdr_empty_history')
                import pandas as pd
                return pd.DataFrame(columns=['Open','High','Low','Close','Volume','Amount','Change'])
            val=_convert(frame)
            _inc('fdr_rows',len(val))
            with _LOCK:
                if symbol not in _STATS['codes'] and len(_STATS['codes'])<1500:_STATS['codes'].append(symbol)
                if len(_STATS['start_end_examples'])<8:_STATS['start_end_examples'].append([symbol,a,b,len(val)])
            return val
        except Exception as e:_fail('fdr_failures',symbol,e);raise
        finally:
            if _PERSIST and _STATS['fdr_calls']%10==1:_PERSIST()
    stock.get_market_ohlcv_by_date=get_raw
    fdr.DataReader=reader
    _INSTALLED=True
    _,meta=_db_meta()
    _STATS.update({'patch_active':True,'asof':meta['asof'],'provider':'PYKRX_KRX_DAILY_SESSION_SQLITE',
                   'official_days':int(meta['official_days']),'calendar_sha256':meta['calendar_sha256']})
    print('R171_MATERIALIZED_KRX_PROVIDER_ACTIVE days',meta['official_days'],flush=True)
    folder=os.environ.get('CLOSING_BET_KRX_TRACE_DIR')
    if folder:
        def dump():
            try:
                p=pathlib.Path(folder);p.mkdir(parents=True,exist_ok=True)
                name=f'krx_provider_trace_{os.getpid()}.json';tmp=p/(name+'.tmp')
                with _LOCK:v=dict(_STATS)
                tmp.write_text(json.dumps(v,ensure_ascii=False,indent=2),encoding='utf-8')
                tmp.replace(p/name)
            except Exception as e:print('R171_TRACE_WRITE_ERROR',type(e).__name__,flush=True)
        _PERSIST=dump;atexit.register(dump);dump()
