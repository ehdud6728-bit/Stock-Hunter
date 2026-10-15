"""R1.7 experimental KRX-only OHLCV bridge for native v49.76 *research shards*.

DO NOT import this from LIVE. Activated via PYTHONPATH/sitecustomize only in
an isolated Actions research workflow. It never silently falls back to NAVER.
It is not a certified 15:30 regular-session-specific source: pykrx/KRX endpoint
session aggregation must be verified separately if KRX changes its semantics.
"""
from __future__ import annotations
import atexit
import datetime as dt
import json
import os
from pathlib import Path
import threading
import time

_LOCK=threading.Lock()
_STATS={'patch_active':False,'fdr_calls':0,'krx_price_calls':0,'index_calls':0,
        'fdr_rows':0,'fdr_failures':0,'direct_raw_errors':0,'fdr_empty_history':0,'forced_adjusted_false':0,'out_of_scope_end_clipped':0,
        'errors':[], 'codes':[], 'start_end_examples':[]}
_INSTALLED=False
_PERSIST=None


def _remember(k,n=1):
    with _LOCK:_STATS[k]=_STATS.get(k,0)+n
    if _PERSIST and k=='fdr_calls' and _STATS[k]%10==1:
        _PERSIST() # survive a native engine hard-exit without atexit


def _failure(where,symbol,exc):
    _remember('fdr_failures')
    with _LOCK:
        if len(_STATS['errors'])<60:_STATS['errors'].append(f'{where}:{str(symbol)}:{type(exc).__name__}:{str(exc)[:150]}')
    if _PERSIST:_PERSIST()


def _dates(start,end):
    asof=os.environ.get('CLOSING_BET_KRX_REQUEST_ASOF','')
    if not asof:raise RuntimeError('KRX_RESEARCH_ASOF_UNSET')
    last=dt.datetime.strptime(asof,'%Y-%m-%d').date()
    if start is None:raise ValueError('KRX_FDR_START_DATE_REQUIRED')
    def parse(v):
        if isinstance(v,dt.datetime):return v.date()
        if isinstance(v,dt.date):return v
        token=str(v).strip()[:10]
        fmt='%Y%m%d' if len(token)==8 and token.isdigit() else '%Y-%m-%d'
        return dt.datetime.strptime(token,fmt).date()
    d0=parse(start)
    d1=parse(end) if end is not None else last
    if d1>last:
        _remember('out_of_scope_end_clipped');d1=last
    if d0>d1:return None,None
    return d0.strftime('%Y%m%d'),d1.strftime('%Y%m%d')


def _convert(raw):
    import pandas as pd
    if raw is None or not isinstance(raw,pd.DataFrame) or raw.empty:raise ValueError('KRX_EMPTY_HISTORY')
    ren={'시가':'Open','고가':'High','저가':'Low','종가':'Close','거래량':'Volume',
         '거래대금':'Amount','등락률':'Change'}
    x=raw.rename(columns=ren).copy()
    req=['Open','High','Low','Close','Volume']
    if not set(req).issubset(x.columns):raise ValueError(f'KRX_HISTORY_BAD_SCHEMA:{list(x.columns)}')
    for k in req:x[k]=pd.to_numeric(x[k],errors='coerce')
    x.index=pd.to_datetime(x.index,errors='raise').normalize()
    x=x.loc[~x.index.duplicated(keep='last')].sort_index()
    x=x.dropna(subset=req)
    if x.empty:raise ValueError('KRX_ALL_NULL_HISTORY')
    if (x[req[:4]]<0).any().any() or (x.Volume<0).any():raise ValueError('KRX_NEGATIVE_OHLCV')
    if 'Amount' not in x:x['Amount']=pd.NA # no manufactured 거래대금
    # FDR Change is a fractional return; KRX 등락률 is reported in percent.
    # Derive from close to avoid a unit mismatch (KRX percent vs FDR fraction).
    x['Change']=x['Close'].pct_change()
    return x


def install():
    global _INSTALLED,_PERSIST
    if _INSTALLED:return
    if os.environ.get('CLOSING_BET_KRX_ONLY_RESEARCH')!='1':return
    from pykrx import stock
    import FinanceDataReader as fdr
    original=stock.get_market_ohlcv_by_date
    original_index=stock.get_index_ohlcv_by_date
    def get_raw(start,end,ticker,*args,**kwargs):
        import pandas as pd
        _remember('krx_price_calls')
        if (args and args[0] is not False) or (not args and kwargs.get('adjusted') is not False):
            _remember('forced_adjusted_false')
        try:
            a,b=_dates(start,end)
            if a is None:return pd.DataFrame()
            # pykrx accepts 'adjusted' positionally: remove to avoid duplicate values.
            if args:
                args=args[1:]
                kwargs.pop('adjusted',None)
                return original(a,b,ticker,False,*args,**kwargs)
            kwargs['adjusted']=False
            return original(a,b,ticker,**kwargs)
        except Exception as exc:
            _remember('direct_raw_errors')
            with _LOCK:
                if len(_STATS['errors'])<60:_STATS['errors'].append(f'KRX_RAW:{ticker}:{type(exc).__name__}:{str(exc)[:150]}')
            if _PERSIST:_PERSIST()
            raise
    def data_reader(symbol,start=None,end=None,*args,**kwargs):
        _remember('fdr_calls')
        try:
            if args or kwargs:
                raise ValueError(f'KRX_FDR_UNSUPPORTED_CALL_ARGS:positional={len(args)},kwargs={sorted(kwargs)}')
            if not isinstance(symbol,str):symbol=str(symbol)
            a,b=_dates(start,end)
            if a is None:
                import pandas as pd
                return pd.DataFrame(columns=['Open','High','Low','Close','Volume'])
            if symbol in ('KS11','KOSPI','KS200','KQ11','KOSDAQ'):
                index_code={'KS11':'1001','KOSPI':'1001','KS200':'1028','KQ11':'2001','KOSDAQ':'2001'}[symbol]
                _remember('index_calls')
                frame=original_index(a,b,index_code)
            elif len(symbol)==6 and symbol.isdigit():
                frame=get_raw(a,b,symbol)
            else:raise ValueError('KRX_NON_KRX_SYMBOL_FORBIDDEN:'+symbol)
            if frame is None or frame.empty:
                # Historical universe includes post-IPO / delisted names with no data
                # within the requested range. Preserve the empty result explicitly;
                # never invent prices or use a secondary source.
                _remember('fdr_empty_history')
                import pandas as pd
                return pd.DataFrame(columns=['Open','High','Low','Close','Volume','Amount','Change'])
            converted=_convert(frame)
            _remember('fdr_rows',len(converted))
            with _LOCK:
                if symbol not in _STATS['codes'] and len(_STATS['codes'])<1500:_STATS['codes'].append(symbol)
                if len(_STATS['start_end_examples'])<8:_STATS['start_end_examples'].append([symbol,a,b,len(converted)])
            return converted
        except Exception as e:
            _failure('DataReader',symbol,e)
            raise
    stock.get_market_ohlcv_by_date=get_raw
    fdr.DataReader=data_reader
    _INSTALLED=True
    _STATS['patch_active']=True
    _STATS['asof']=os.environ['CLOSING_BET_KRX_REQUEST_ASOF']
    _STATS['provider']='PYKRX_KRX_ADJUSTED_FALSE'
    print('R17_KRX_ONLY_PROVIDER_ACTIVE',flush=True)
    trace_dir=os.environ.get('CLOSING_BET_KRX_TRACE_DIR','')
    if trace_dir:
        def dump():
            try:
                folder=Path(trace_dir);folder.mkdir(parents=True,exist_ok=True)
                p=folder/('krx_provider_trace_'+str(os.getpid())+'.json')
                with _LOCK:data=dict(_STATS)
                tmp=folder/('krx_provider_trace_'+str(os.getpid())+'.json.tmp')
                tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
                tmp.replace(p)
            except Exception as exc:print('R17_TRACE_WRITE_FAILURE',repr(exc),flush=True)
        _PERSIST=dump
        atexit.register(dump)
        dump()
