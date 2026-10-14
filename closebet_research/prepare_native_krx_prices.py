#!/usr/bin/env python3
"""R1.5: obtain full-session historical OHLCV for native v49.76 D+20.

Research only. Uses *dated* KRX all-ticker session prices, validates session
completeness, verifies scanner-close parity, and never substitutes an outcome
price or a future adjusted close for a missing quote. Partial source failures
are fatal rather than silently biasing a success rate.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import time
from datetime import datetime

import numpy as np
import pandas as pd

ALIASES = {
    'open': ('시가','Open','open'),
    'high': ('고가','High','high'),
    'low': ('저가','Low','low'),
    'close': ('종가','Close','close'),
    'volume': ('거래량','Volume','volume'),
}

def normalize_ticker(v):
    return str(v).strip().split('.')[0].zfill(6)

def standardize_market(d):
    if d is None or not isinstance(d,pd.DataFrame) or d.empty:
        raise ValueError('EMPTY_KRX_MARKET_SESSION')
    out = pd.DataFrame(index=d.index.map(normalize_ticker))
    for field, candidates in ALIASES.items():
        col=next((c for c in candidates if c in d.columns),None)
        if col is None:raise ValueError(f'KRX_MISSING_{field.upper()}_COLUMN:{list(d.columns)}')
        out[field] = pd.to_numeric(d[col].to_numpy(), errors='coerce')
    out.index.name='code'
    out = out[~out.index.duplicated(keep='first')]
    return out

def official_dates(stock, start, end):
    fromdate=pd.Timestamp(start).strftime('%Y%m%d')
    todate=pd.Timestamp(end).strftime('%Y%m%d')
    idx = stock.get_index_ohlcv_by_date(fromdate,todate,'1001')
    if idx is None or idx.empty:
        raise RuntimeError('OFFICIAL_KRX_INDEX_CALENDAR_EMPTY; do not infer business days')
    dates=pd.DatetimeIndex(pd.to_datetime(idx.index,errors='coerce')).normalize()
    dates=dates[(dates>=pd.Timestamp(start))&(dates<=pd.Timestamp(end))]
    if not len(dates) or dates.has_duplicates:
        raise RuntimeError('OFFICIAL_KRX_CALENDAR_INVALID')
    return dates.sort_values()

def fetch_prices(stock, dates, selected_codes, min_universe=500, max_retries=3, pause=.15):
    frames=[];audit=[]
    for position, session in enumerate(dates,1):
        day=pd.Timestamp(session).strftime('%Y%m%d')
        err=''; result=None
        for attempt in range(1,max_retries+1):
            try:
                raw=stock.get_market_ohlcv_by_ticker(day,market='ALL')
                full=standardize_market(raw)
                # Catch wrong-market, authentication and response-truncation issues.
                if len(full)<min_universe:
                    raise RuntimeError(f'KRX_TOO_FEW_TICKERS:{len(full)}<{min_universe}')
                subset=full.loc[full.index.isin(selected_codes)].copy()
                subset=subset[subset['close'].gt(0) & subset['open'].ge(0)]
                subset['code']=subset.index
                subset['date']=pd.Timestamp(session).strftime('%Y-%m-%d')
                result=subset.reset_index(drop=True)[['code','date','open','high','low','close','volume']]
                audit.append({'date':str(session.date()),'status':'VALID_SESSION','all_tickers':len(full),'tracked_tickers':len(subset),'attempts':attempt})
                break
            except Exception as exc:
                err=f'{type(exc).__name__}:{exc}'
                if attempt<max_retries:time.sleep(min(attempt*1.5,5))
        if result is None:
            audit.append({'date':str(session.date()),'status':'INVALID_SESSION','all_tickers':0,'tracked_tickers':0,'attempts':max_retries,'reason':err})
        else:frames.append(result)
        if position%10==0 or result is None:
            print(f'KRX_SESSION {position}/{len(dates)} date={day} tickers={0 if result is None else len(result)} error={err if result is None else ""}',flush=True)
        if pause:time.sleep(pause)
    if len(frames)!=len(dates):
        raise RuntimeError(f'KRX_SESSION_COVERAGE_FAILED: {len(frames)}/{len(dates)}; incomplete daily market data cannot make unbiased D+20')
    return pd.concat(frames,ignore_index=True),pd.DataFrame(audit)

def audit_signal_bars(prices,selected,max_price_mismatch_pct=0.50,max_volume_mismatch_pct=1.0):
    """Full OHLCV parity, by unique date/code, NOT just native close/entry copies."""
    mapping={'_open':'open','_high':'high','_low':'low','_close':'close','_vol':'volume'}
    required={'signal_date','code','entry_price',*mapping}
    if not required.issubset(selected.columns):
        raise ValueError('SOURCE_MISSING_SIGNAL_OHLCV:'+repr(required-set(selected.columns)))
    s=selected[list(required)].copy()
    s['code']=s.code.map(normalize_ticker)
    s['signal_date']=pd.to_datetime(s.signal_date,errors='coerce').dt.strftime('%Y-%m-%d')
    for col in [*mapping,'entry_price']:
        s[col]=pd.to_numeric(s[col],errors='coerce')
    # Same-day same-code multi-strategy rows must NOT disguise conflicting candles.
    inconsistent=0
    for col in [*mapping,'entry_price']:
        inconsistent+=int((s.groupby(['signal_date','code'],dropna=False)[col].nunique(dropna=False)>1).sum())
    if inconsistent:raise ValueError('SAME_DATE_CODE_CONFLICTING_SIGNAL_BAR')
    s=s.drop_duplicates(['signal_date','code'])
    q=s.merge(prices[['date','code','open','high','low','close','volume']],how='left',left_on=['signal_date','code'],right_on=['date','code'],validate='one_to_one',suffixes=('_native','_krx'))
    ok=[]
    for from_col,to_col in mapping.items():
        source=pd.to_numeric(q[from_col],errors='coerce')
        actual=pd.to_numeric(q[to_col],errors='coerce')
        if to_col=='volume':
            ok_col=source.gt(0)&actual.gt(0)&((actual/source-1).abs()*100<=max_volume_mismatch_pct)
        else:
            ok_col=source.gt(0)&actual.gt(0)&((actual/source-1).abs()*100<=max_price_mismatch_pct)
        q['match_'+to_col]=ok_col.fillna(False)
        ok.append(q['match_'+to_col])
    q['match_entry_close']=q.entry_price.gt(0)&q.close.gt(0)&((q.close/q.entry_price-1).abs()*100<=max_price_mismatch_pct)
    q['match_all_ohlcv']=pd.concat(ok+[q['match_entry_close']],axis=1).all(axis=1)
    missing=int(q.close.isna().sum())
    parity={'unique_date_code_bars':int(len(q)),'matched_all_ohlcv':int(q.match_all_ohlcv.sum()),
        'missing_krx_close':missing,'mismatched_or_missing':int((~q.match_all_ohlcv).sum()),
        'match_pct':float(q.match_all_ohlcv.mean()*100) if len(q) else 0.0,
        'price_pct_tolerance':max_price_mismatch_pct,'volume_pct_tolerance':max_volume_mismatch_pct,
        'components':{field:int(q['match_'+field].sum()) for field in ['open','high','low','close','volume','entry_close']}}
    invalid=q.loc[~q.match_all_ohlcv,['signal_date','code','entry_price','_open','_high','_low','_close','_vol','open','high','low','close','volume','match_open','match_high','match_low','match_close','match_volume','match_entry_close']]
    return parity,invalid

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--selected',required=True)
    p.add_argument('--asof',required=True)
    p.add_argument('--out',required=True)
    p.add_argument('--min-universe',type=int,default=500)
    p.add_argument('--min-match-pct',type=float,default=98.0)
    args=p.parse_args()
    selected=pd.read_csv(args.selected,dtype={'code':str},low_memory=False)
    if not {'signal_date','code','entry_price'}.issubset(selected.columns):raise SystemExit('NATIVE_SELECTED_REQUIRED_SCHEMA_MISSING')
    selected['signal_date']=pd.to_datetime(selected.signal_date,errors='coerce')
    start=selected.signal_date.min()
    end=pd.Timestamp(args.asof)
    if pd.isna(start) or start>end:raise SystemExit('INVALID_EVALUATION_DATES')
    from pykrx import stock
    dates=official_dates(stock,start,end)
    codes=set(selected.code.map(normalize_ticker))
    # Price data is required for every KRX session, not only days where signals occurred.
    prices,day_audit=fetch_prices(stock,dates,codes,min_universe=args.min_universe)
    parity,invalid=audit_signal_bars(prices,selected)
    out=pathlib.Path(args.out);out.mkdir(parents=True,exist_ok=True)
    prices.to_csv(out/'native_krx_input_prices.csv',index=False,encoding='utf-8-sig')
    day_audit.to_csv(out/'native_krx_session_coverage.csv',index=False,encoding='utf-8-sig')
    invalid.to_csv(out/'native_krx_signal_price_mismatches.csv',index=False,encoding='utf-8-sig')
    result={'status':'VALIDATED' if parity['match_pct']>=args.min_match_pct else 'INVALID_SOURCE_PARITY',
            'source':'PYKRX_KRX_MARKET_ALL_UNADJUSTED','start':str(start.date()),'asof':args.asof,
            'official_session_count':len(dates),'valid_session_count':len(day_audit),
            'selected_unique_codes':len(codes),'price_rows':len(prices),'minimum_quality_pass_pct':args.min_match_pct,**parity}
    (out/'native_krx_price_audit.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print('KRX_PRICE_AUDIT',json.dumps(result,ensure_ascii=False),flush=True)
    if result['status']!='VALIDATED':raise SystemExit('KRX_OHLCV_PARITY_FAIL_CLOSED; report withheld')
if __name__=='__main__':main()
