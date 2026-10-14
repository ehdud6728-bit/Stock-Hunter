#!/usr/bin/env python3
"""R1.6.2 READ-ONLY: investigate a historical-price regime break, never approve signals.

Inputs: immutable original v49.76 selected events and independently downloaded
KRX cross-sectional daily OHLCV. Optionally compare pykrx's per-ticker, by-date
adjusted=True (NAVER in upstream pykrx) vs adjusted=False (KRX) paths.

A provider match explains likely provenance, not that the trading signal was
valid as-of 15:30. No instrument with a failed parity check is auto repaired.
"""
from __future__ import annotations
import argparse
import json
import pathlib
import time
import traceback
from datetime import datetime
import pandas as pd
import numpy as np

SIGNAL_COLS={'_open':'open','_high':'high','_low':'low','_close':'close','_vol':'volume'}
JUMP_DATE='2026-09-14'


def _ticker(v):
    return str(v).strip().split('.')[0].zfill(6)


def _num(x):
    return pd.to_numeric(x,errors='coerce')


def _date(x):
    return pd.to_datetime(x,errors='coerce').dt.strftime('%Y-%m-%d')


def _safe(df,p):
    df.to_csv(p,index=False,encoding='utf-8-sig')


def build_analysis(selected_path, prices_path, asof, out_dir, history_path=None):
    out=pathlib.Path(out_dir);out.mkdir(parents=True,exist_ok=True)
    native=pd.read_csv(selected_path,dtype={'code':str},low_memory=False)
    prices=pd.read_csv(prices_path,dtype={'code':str},low_memory=False)
    required={'signal_date','code','mode','entry_price',*SIGNAL_COLS.keys()}
    if not required.issubset(native.columns):
        raise ValueError(f'NATIVE_SOURCE_SCHEMA_MISSING:{sorted(required-set(native.columns))}')
    if not {'date','code','open','high','low','close','volume'}.issubset(prices.columns):
        raise ValueError('INDEPENDENT_KRX_PRICE_SCHEMA_MISSING')
    native['signal_date']=_date(native.signal_date)
    prices['date']=_date(prices.date)
    native['code']=native.code.map(_ticker);prices['code']=prices.code.map(_ticker)
    for f in [*SIGNAL_COLS.keys(),'entry_price']:
        native[f]=_num(native[f])
    for f in ('open','high','low','close','volume'):
        prices[f]=_num(prices[f])
    if native.signal_date.isna().any():raise ValueError('NATIVE_SIGNAL_DATE_INVALID')
    if native.signal_date.gt(asof).any():raise ValueError('FUTURE_NATIVE_SIGNAL_UNEXPECTED')
    if prices.date.gt(asof).any():raise ValueError('FUTURE_INDEPENDENT_KRX_BAR_UNEXPECTED')
    if prices.duplicated(['date','code']).any():raise ValueError('KRX_DUPLICATED_DATE_CODE')
    # Multiple strategy rows for one ticker/session must have same source bar.
    for f in [*SIGNAL_COLS.keys(),'entry_price']:
        if (native.groupby(['signal_date','code'],dropna=False)[f].nunique(dropna=False)>1).any():
            raise ValueError(f'CONFLICTING_BAR_BETWEEN_STRATEGIES:{f}')
    bars=native[['signal_date','code','entry_price',*SIGNAL_COLS.keys()]].drop_duplicates(['signal_date','code'])
    full=bars.merge(prices,left_on=['signal_date','code'],right_on=['date','code'],how='left',validate='one_to_one')
    for src,k in SIGNAL_COLS.items():
        tol=0.01 if k=='volume' else 0.005
        full['match_'+k]=(_num(full[src]).gt(0) & _num(full[k]).gt(0) & (full[src]/full[k]-1).abs().le(tol)).fillna(False)
    full['match_entry_close']=full.entry_price.gt(0) & full.close.gt(0) & (full.entry_price/full.close-1).abs().le(.005)
    flags=['match_'+k for k in ('open','high','low','close','volume','entry_close')]
    full['parity_pass']=full[flags].all(axis=1)
    full['regime']=np.where(full.signal_date<JUMP_DATE,'BEFORE_2026_09_14','AFTER_2026_09_14')
    full['native_close_vs_KRX_regular_pct']=(full._close/full.close-1)*100
    full['native_close_outside_KRX_range']=(full._close>full.high)|(full._close<full.low)
    full['only_close_diff']=full.match_open&full.match_high&full.match_low&full.match_volume&~full.match_close
    full['all_price_except_close_match']=full.match_open&full.match_high&full.match_low&full.match_volume
    full['abs_close_diff_pct']=full.native_close_vs_KRX_regular_pct.abs()
    full=full.sort_values(['signal_date','code']).reset_index(drop=True)
    _safe(full,out/'r162_source_vs_krx_all_bars.csv')
    _safe(full.loc[~full.parity_pass],out/'r162_source_vs_krx_disagreements.csv')
    reg=full.groupby('regime').agg(unique_bars=('code','size'),matched=('parity_pass','sum'),only_close_diff=('only_close_diff','sum'),outside_krx_range=('native_close_outside_KRX_range','sum')).reset_index()
    reg['mismatched']=reg.unique_bars-reg.matched;reg['match_pct']=reg.matched/reg.unique_bars*100
    _safe(reg,out/'r162_parity_by_regime.csv')
    d=full.groupby('signal_date').agg(unique_bars=('code','size'),matched=('parity_pass','sum'),only_close_diff=('only_close_diff','sum')).reset_index()
    d['mismatched']=d.unique_bars-d.matched;d['match_pct']=d.matched/d.unique_bars*100
    _safe(d,out/'r162_parity_by_date.csv')
    modes=native[['signal_date','code','mode']].drop_duplicates().merge(full[['signal_date','code','parity_pass']],on=['signal_date','code'],how='left',validate='many_to_one')
    summary=modes.groupby('mode').agg(events=('code','size'),matched=('parity_pass','sum')).reset_index()
    summary['mismatched']=summary.events-summary.matched; summary['match_pct']=summary.matched/summary.events*100
    _safe(summary,out/'r162_parity_by_strategy.csv')
    hist={'history_file_available':False}
    if history_path and pathlib.Path(history_path).is_file():
        h=pd.read_csv(history_path,dtype={'code':str},low_memory=False)
        if {'code','last_date','history_rows'}.issubset(h.columns):
            h['last_date']=_date(h.last_date)
            loaded=h[_num(h.history_rows).gt(0)]
            hist={'history_file_available':True,'history_codes':int(len(h)),
                  'history_loaded_codes':int(len(loaded)),
                  'loaded_history_extending_beyond_requested_asof':int(loaded.last_date.gt(asof).sum()),
                  'history_max_last_date':str(loaded.last_date.max()) if len(loaded) else None,
                  'warning':'Input history extending beyond signal/evaluation cutoff is a causal provenance risk; even if features are sliced, their fetch authority must be audited.'}
    # Source discovery only; preserve strict fail-closed, no repaired signals or final strategy rank.
    doc={'status':'FORENSIC_ONLY_NOT_VALIDATED','asof':asof,
         'original_events':int(len(native)),'unique_signal_code_date':int(len(full)),
         'KRX_cross_sectional_source':'pykrx.get_market_ohlcv_by_ticker(date,market=ALL)',
         'before_after_launch_date':JUMP_DATE,'by_regime':reg.to_dict(orient='records'),
         'overall_matching_bars':int(full.parity_pass.sum()),'overall_mismatched_bars':int((~full.parity_pass).sum()),
         'only_close_diff_bars':int(full.only_close_diff.sum()),
         'native_close_outside_krx_range':int(full.native_close_outside_KRX_range.sum()),
         'outside_KRX_range_AMONG_parity_failures':int((full.native_close_outside_KRX_range & ~full.parity_pass).sum()),
         'max_abs_close_diff_pct':float(full.abs_close_diff_pct.max()),
         'source_window_audit':hist,
         'hypothesis':'A price-session/provider regime break aligns with the 2026-09-14 KRX aftermarket launch, but temporal coincidence does not identify historical loader or confirm an aftermarket print.',
         'validity_guard':'CANNOT_APPROVE_EVEN_IF_NAVER_MATCHES: requires causal re-screen with regular 15:30 OHLCV and point-in-time universe (plus 10/12 asof cutoff).',
         'strategy_performance_status':'WITHHELD'}
    (out/'r162_price_regime_forensics.json').write_text(json.dumps(doc,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R162_FORENSIC_OFFLINE',json.dumps({k:doc[k] for k in ['overall_matching_bars','overall_mismatched_bars','only_close_diff_bars','native_close_outside_krx_range']},ensure_ascii=False),flush=True)
    return full,doc


def _normalize_one_day(df,day):
    if df is None or df.empty:return None
    t=pd.Timestamp(day)
    x=df.copy()
    x.index=pd.to_datetime(x.index,errors='coerce').normalize()
    if t not in x.index:return None
    r=x.loc[t]
    if isinstance(r,pd.DataFrame):return None
    fields={'open':['시가','open','Open'],'high':['고가','high','High'],'low':['저가','low','Low'],'close':['종가','close','Close'],'volume':['거래량','volume','Volume']}
    d={}
    for k,aliases in fields.items():
        name=next((a for a in aliases if a in x.columns),None)
        if name is None:return None
        d[k]=pd.to_numeric(r[name],errors='coerce')
    return d


def probe_providers(full,out_dir,max_cases=14):
    """Does native source align with by-date NAVER or by-date raw KRX?

    A few pinned examples plus controls; failure of optional providers is evidence
    of unavailable comparison, not evidence of agreement.
    """
    out=pathlib.Path(out_dir);out.mkdir(parents=True,exist_ok=True)
    bad=full.loc[~full.parity_pass].copy()
    after=bad.loc[bad.signal_date>=JUMP_DATE].copy()
    groups=[]
    # Max divergence and boundary day cases, plus early-adjustment controls.
    groups.extend([bad.loc[bad.signal_date<JUMP_DATE].sort_values('abs_close_diff_pct',ascending=False).head(2)])
    groups.extend([after.loc[after.signal_date==JUMP_DATE].sort_values('abs_close_diff_pct',ascending=False).head(3)])
    groups.extend([after.sort_values('abs_close_diff_pct',ascending=False).head(5)])
    groups.extend([after.loc[after.signal_date=='2026-10-12'].sort_values('abs_close_diff_pct',ascending=False).head(2)])
    groups.extend([full.loc[(full.signal_date>=JUMP_DATE)&full.parity_pass].head(2)])
    cases=pd.concat(groups,ignore_index=True).drop_duplicates(['signal_date','code']).head(max_cases)
    reports=[]
    try:
        from pykrx import stock
        import_state='OK'
    except Exception as e:
        stock=None;import_state=f'IMPORT_ERROR:{type(e).__name__}'
    for _,row in cases.iterrows():
        code=str(row.code);day=str(row.signal_date);d=day.replace('-','')
        rec={'code':code,'signal_date':day,'native_close':float(row._close),'krx_cross_section_close':float(row.close),
             'abs_source_vs_krx_cross_pct':float(row.abs_close_diff_pct),'cross_krx_parity_pass':bool(row.parity_pass)}
        for adjusted,which in [(True,'NAVER_ADJUSTED_TRUE'),(False,'KRX_BY_DATE_RAW_FALSE')]:
            res=None;error=''
            if stock is None:error=import_state
            else:
                for attempt in range(2):
                    try:
                        x=stock.get_market_ohlcv_by_date(d,d,code,adjusted=adjusted)
                        res=_normalize_one_day(x,day)
                        if res is None:error='NO_DAY_BAR'
                        else:error=''
                        break
                    except Exception as e:
                        error=f'{type(e).__name__}:{str(e)[:110]}'
                        if attempt==0:time.sleep(.8)
            rec[which+'_status']='OK' if res is not None else error
            if res is not None:
                for k,v in res.items():rec[which+'_'+k]=float(v)
                rec[which+'_NATIVE_CLOSE_EXACT']=bool(np.isclose(float(res['close']),float(row._close),rtol=0,atol=.001))
                rec[which+'_KRX_CROSS_CLOSE_EXACT']=bool(np.isclose(float(res['close']),float(row.close),rtol=0,atol=.001))
                rec[which+'_NATIVE_ALL_OHLCV_EXACT']=all(np.isclose(float(res[k]),float(row['_'+{'volume':'vol'}.get(k,k)]),rtol=0,atol=.001) for k in ('open','high','low','close','volume'))
        reports.append(rec)
        print('PROBE',day,code,'NATIVE',rec['native_close'],'KRX',rec['krx_cross_section_close'],
              'NAVER',rec.get('NAVER_ADJUSTED_TRUE_close','N/A'),
              'KRX_BY_DATE',rec.get('KRX_BY_DATE_RAW_FALSE_close','N/A'),flush=True)
        time.sleep(.15)
    df=pd.DataFrame(reports)
    _safe(df,out/'r162_price_source_provider_probe.csv')
    result={'status':'PROBE_DIAGNOSTIC_ONLY','probed_cases':len(df),'max_cases':max_cases,
            'naver_ok':int((df.get('NAVER_ADJUSTED_TRUE_status',pd.Series(dtype=str))=='OK').sum()),
            'krx_per_code_ok':int((df.get('KRX_BY_DATE_RAW_FALSE_status',pd.Series(dtype=str))=='OK').sum()),
            'naver_matches_native_close':int(df.get('NAVER_ADJUSTED_TRUE_NATIVE_CLOSE_EXACT',pd.Series(dtype=bool)).fillna(False).sum()),
            'raw_krx_matches_native_close':int(df.get('KRX_BY_DATE_RAW_FALSE_NATIVE_CLOSE_EXACT',pd.Series(dtype=bool)).fillna(False).sum()),
            'warning':'A match is only a provider/field provenance clue. Never reinterpret 20:00 trade price as 15:30 closing auction fill.'}
    (out/'r162_provider_probe_summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    return result


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--selected',required=True)
    p.add_argument('--prices',required=True)
    p.add_argument('--history',default='')
    p.add_argument('--asof',required=True)
    p.add_argument('--out',required=True)
    p.add_argument('--probe-providers',action='store_true')
    p.add_argument('--max-probes',type=int,default=14)
    args=p.parse_args()
    full,doc=build_analysis(args.selected,args.prices,args.asof,args.out,args.history)
    if args.probe_providers:
        summary=probe_providers(full,args.out,max_cases=min(max(1,args.max_probes),40))
        print('R162_PROBE_SUMMARY',json.dumps(summary,ensure_ascii=False),flush=True)

if __name__=='__main__':main()
