#!/usr/bin/env python3
"""Compute horizon-verified metrics from ORIGINAL v49.76 native selected rows.

Every D+N value is calculated from future daily close observations, never substituted
with legacy 'rule35_pnl'.  Incomplete horizons remain null. Not a fill simulation.
"""
from __future__ import annotations
import argparse, json, math, pathlib, re, warnings
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import date,timedelta
import numpy as np, pandas as pd

HORIZONS=[1,3,5,10,15,20]
PRICE_FIELDS=['entry_close','signal_close','entry_price','close','_close','buy_price']

def num(s):return pd.to_numeric(s,errors='coerce')
def code(s):return str(s).strip().split('.')[0].zfill(6)
def safe_csv(df,p):df.to_csv(p,index=False,encoding='utf-8-sig')

def get_price(code_text,start,end,reference):
    import yfinance as yf
    choices=[]
    for suffix in ('.KS','.KQ'):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                x=yf.download(code_text+suffix,start=start.strftime('%Y-%m-%d'),end=end.strftime('%Y-%m-%d'),
                    progress=False,auto_adjust=False,actions=False,threads=False,timeout=18)
            if x is None or x.empty:continue
            if isinstance(x.columns,pd.MultiIndex):
                x.columns=[str(c[0]).lower() for c in x.columns]
            else:x.columns=[str(c).lower() for c in x.columns]
            if not {'close','open','high','low','volume'}.issubset(set(x.columns)):continue
            x=x[['open','high','low','close','volume']].copy()
            x.index=pd.to_datetime(x.index).tz_localize(None).normalize()
            x=x[~x.index.duplicated(keep='last')].sort_index()
            if len(x)>0:choices.append((code_text+suffix,x))
        except Exception:pass
    if not choices:return code_text,None,'NO_DAILY_PRICE'
    # Never guess listing exchange if both symbols exist with different prices.
    if len(choices)==1:return code_text,choices[0][1],choices[0][0]
    for sym,x in choices:
        good=0
        for signal_date,p in reference:
            if signal_date in x.index and np.isfinite(p) and p>0:
                if abs(float(x.loc[signal_date,'close'])/p-1)<0.04:good+=1
        if good>0 and good>max((sum(1 for d,p in reference if d in other.index and np.isfinite(p) and p>0 and abs(float(other.loc[d,'close'])/p-1)<0.04) for sy,other in choices if sy!=sym),default=0):
            return code_text,x,sym
    return code_text,None,'EXCHANGE_AMBIGUOUS'

def extract_events(path):
    df=pd.read_csv(path,dtype={'code':str},low_memory=False)
    required={'signal_date','code','mode'}
    if not required.issubset(df.columns):raise SystemExit(f'NATIVE_SCHEMA_MISSING:{required-set(df.columns)}')
    price_col=next((c for c in PRICE_FIELDS if c in df.columns and num(df[c]).notna().any()),None)
    if price_col is None:raise SystemExit('NO_SOURCE_CLOSE_COLUMN_NOT_ALLOWED_TO_GUESS')
    x=df.copy()
    x['signal_date']=pd.to_datetime(x.signal_date,errors='coerce').dt.normalize()
    x['code']=x.code.map(code)
    x['entry_reference']=num(x[price_col])
    x=x[x.signal_date.notna()&x.code.str.fullmatch(r'\d{6}')&x.entry_reference.gt(0)].copy()
    if x.empty:raise SystemExit('NO_VALID_NATIVE_EVENTS')
    x=x.sort_values(['signal_date','code','mode']).drop_duplicates(['signal_date','code','mode'])
    x['source_price_column']=price_col
    x['source_type']='NATIVE_BACKTEST_SELECTED_NOT_DELIVERED'
    return x

def process(events, prices, labels, asof, sessions=None):
    """Evaluate at exact KRX exchange trading sessions; never slide a halt forward."""
    if sessions is None:
        sessions=sorted({d for x in prices.values() if x is not None for d in x.index})
    dates=pd.DatetimeIndex(pd.to_datetime(sessions)).normalize().unique().sort_values()
    rows=[]
    for _,r in events.iterrows():
        d=r.signal_date;c=r.code;ref=float(r.entry_reference)
        base={k:r[k] for k in ('signal_date','code','mode','entry_reference','source_price_column','source_type')}
        base['signal_date']=str(d.date())
        for optional in ('name','primary_strategy','rule35_pnl','ma224_dist_pct','ma200_dist_pct','pullback_from_high','score'):
            if optional in events.columns:base[optional]=r[optional]
        x=prices.get(c)
        base.update({'price_source':labels.get(c,''),'data_status':'UNKNOWN','max_high_observed_upto20_pct':np.nan,'min_low_observed_upto20_pct':np.nan,'observed_future_bars_upto20':0})
        future_days=dates[(dates>d)&(dates<=asof)]
        for h in HORIZONS:
            base[f'D{h}_ret_pct']=np.nan
            base[f'D{h}_complete']=0
            base[f'D{h}_time_eligible']=int(len(future_days)>=h)
        if x is None:base['data_status']='NO_VALID_PRICE';rows.append(base);continue
        if d not in x.index:base['data_status']='SIGNAL_DATE_MISSING';rows.append(base);continue
        close=float(x.loc[d,'close'])
        vol=float(x.loc[d,'volume'])
        if not (np.isfinite(close) and np.isfinite(vol) and vol>0 and abs(close/ref-1)<=0.04):
            base['data_status']='SOURCE_CLOSE_MISMATCH_OR_HALT';rows.append(base);continue
        base['data_status']='VALID_ENTRY'
        for h in HORIZONS:
            if len(future_days)<h:continue
            target_day=future_days[h-1]
            if target_day not in x.index:continue
            target=x.loc[target_day]
            px=float(target['close']); tv=float(target['volume'])
            if not (np.isfinite(px) and px>0 and np.isfinite(tv) and tv>0):continue
            base[f'D{h}_ret_pct']=(px/ref-1)*100
            base[f'D{h}_complete']=1
        cap=x.loc[x.index.intersection(future_days[:20])].copy()
        cap=cap[pd.to_numeric(cap['volume'],errors='coerce').gt(0)]
        base['observed_future_bars_upto20']=len(cap)
        if len(cap):
            base['max_high_observed_upto20_pct']=(float(cap.high.max())/ref-1)*100
            base['min_low_observed_upto20_pct']=(float(cap.low.min())/ref-1)*100
        rows.append(base)
    return pd.DataFrame(rows)

def metrics(rows):
    out=[]
    for mode,z in [('ALL_EVENTS',rows),('UNIQUE_CODE_DATE',rows.drop_duplicates(['signal_date','code']))]+list(rows.groupby('mode',dropna=False)):
        row={'mode':mode,'events':len(z),'distinct_signal_dates':z.signal_date.nunique()}
        for h in HORIZONS:
            completed=z[z[f'D{h}_complete']==1]
            r=num(completed[f'D{h}_ret_pct']).dropna()
            eligible=int(z[f'D{h}_time_eligible'].sum())
            row.update({f'D{h}_n':len(r),f'D{h}_incomplete_n':len(z)-len(r),
                        f'D{h}_mature_n':eligible,f'D{h}_missing_price_after_maturity_n':eligible-len(r),
                        f'D{h}_mature_coverage_pct':float(len(r)/eligible*100) if eligible else np.nan,
                        f'D{h}_win_pct':float(r.gt(0).mean()*100) if len(r) else np.nan,
                        f'D{h}_mean_gross_pct':float(r.mean()) if len(r) else np.nan,
                        f'D{h}_median_gross_pct':float(r.median()) if len(r) else np.nan,
                        f'D{h}_mean_net50_pct':float(r.mean()-0.50) if len(r) else np.nan})
        out.append(row)
    return pd.DataFrame(out)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--source-root',required=True);ap.add_argument('--asof',required=True)
    ap.add_argument('--out',required=True);ap.add_argument('--workers',type=int,default=4)
    ap.add_argument('--max-codes',type=int,default=1000)
    ap.add_argument('--prices-csv',default='',help='Optional verified daily OHLCV price data for local audit')
    ap.add_argument('--require-krx-parity-json',default='',help='Fail-closed verified OHLCV gate for official results')
    args=ap.parse_args();asof=pd.Timestamp(args.asof);out=pathlib.Path(args.out);out.mkdir(parents=True,exist_ok=True)
    if args.require_krx_parity_json:
        gate=json.loads(pathlib.Path(args.require_krx_parity_json).read_text(encoding='utf-8'))
        if gate.get('status')!='VALIDATED' or float(gate.get('match_pct',0))<98.0:
            raise SystemExit('FAIL_CLOSED_OFFICIAL_KRX_OHLCV_PARITY_NOT_PROVEN')
        if not args.prices_csv:raise SystemExit('OFFICIAL_PARITY_GATE_REQUIRES_KRX_PRICES_CSV')

    selected=list(pathlib.Path(args.source_root).rglob('v49_76_global_selected_top5.csv'))
    if len(selected)!=1:raise SystemExit('NATIVE_SELECTED_SOURCE_NOT_UNIQUE')
    events=extract_events(selected[0]);events=events[events.signal_date<=asof].copy()
    codes=sorted(events.code.unique())
    if len(codes)>args.max_codes:raise SystemExit(f'TOO_MANY_UNIQUE_CODES:{len(codes)}>{args.max_codes}; no silent downsampling')
    refs={c:[(d,float(p)) for d,p in zip(e.signal_date,e.entry_reference)] for c,e in events.groupby('code')}
    prices={};labels={};sessions=None
    if args.prices_csv:
        frame=pd.read_csv(args.prices_csv,dtype={'code':str});fields={'code','date','open','high','low','close','volume'}
        if not fields.issubset(frame.columns):raise SystemExit(f'PRICE_CSV_SCHEMA_MISSING:{fields-set(frame.columns)}')
        frame['code']=frame.code.map(code)
        frame['date']=pd.to_datetime(frame.date,errors='coerce').dt.normalize()
        sessions=frame['date'].dropna().sort_values().unique()
        for c,z in frame.groupby('code'):
            z=z.dropna(subset=['date']).set_index('date').sort_index()
            prices[c]=z;labels[c]='SUPPLIED_PRICE_CSV'
    else:
        first=events.signal_date.min().date()-timedelta(days=14);stop=asof.date()+timedelta(days=1)
        with ThreadPoolExecutor(max_workers=max(1,min(args.workers,5))) as pool:
            for fut in as_completed([pool.submit(get_price,c,first,stop,refs[c]) for c in codes]):
                c,frame,source=fut.result();prices[c]=frame;labels[c]=source
        print('PRICE_FETCH',len(codes),sum(x is not None for x in prices.values()),flush=True)
    if sessions is None:
        # Yahoo market calendar is only an observed union; not official.
        sessions=sorted({d for x in prices.values() if x is not None for d in x.index})
    result=process(events,prices,labels,asof,sessions)
    if args.require_krx_parity_json:
        mismatch_file=pathlib.Path(args.require_krx_parity_json).with_name('native_krx_signal_price_mismatches.csv')
        if not mismatch_file.is_file():raise SystemExit('OFFICIAL_OHLCV_MISMATCH_EVIDENCE_MISSING')
        bad=pd.read_csv(mismatch_file,dtype={'code':str})
        if len(bad):
            bad_keys=set(zip(bad.signal_date.astype(str),bad.code.map(code)))
            invalid=result.apply(lambda r:(str(r.signal_date),code(r.code)) in bad_keys,axis=1)
            result.loc[invalid,'data_status']='KRX_OHLCV_PARITY_MISMATCH'
            for h in HORIZONS:
                result.loc[invalid,f'D{h}_complete']=0
                result.loc[invalid,f'D{h}_ret_pct']=np.nan
        else:invalid=pd.Series(False,index=result.index)
        print(f'SIGNAL_ROWS_WITH_INDEPENDENT_OHLCV_DISAGREEMENT_EXCLUDED={int(invalid.sum())}',flush=True)
        # Native returns are used solely as a source for signal identities, never as D+N outcomes.

        original=pd.read_csv(selected[0],dtype={'code':str},low_memory=False)
        if len(result)!=len(original[original.signal_date.le(str(asof.date()))]):
            raise SystemExit('OFFICIAL_PRICE_EVAL_SIGNAL_COUNT_MISMATCH')

    table=metrics(result)
    safe_csv(result,out/'native_d20_event_diagnostics.csv')
    safe_csv(table,out/'native_d20_strategy_metrics.csv')
    safe_csv(result[result.data_status!='VALID_ENTRY'],out/'native_d20_rejected_evidence.csv')
    mature=int(result['D20_time_eligible'].sum());complete=int(result.D20_complete.sum())
    mature_coverage=complete/mature*100 if mature else 0.0
    valid_entry=float(result.data_status.eq('VALID_ENTRY').mean()*100)
    diag={'status': ('NATIVE_EVALUATED_WITH_LIMITATIONS' if complete>=30 and valid_entry>=90 and mature_coverage>=90 else 'INSUFFICIENT_D20_OR_PRICE_COVERAGE'),
          'asof':args.asof,'source_file':str(selected[0]),'signal_rows':len(events),
          'unique_codes':len(codes),'valid_entry_rows':int(result.data_status.eq('VALID_ENTRY').sum()),
          'D20_complete_rows':complete,'D20_incomplete_rows':int(len(result)-complete),
          'D20_mature_rows':mature,'D20_future_censored_rows':int(len(result)-mature),
          'D20_missing_prices_after_maturity_rows':mature-complete,
          'D20_complete_over_mature_pct':mature_coverage,
          'exchange_session_calendar':('KRX_VALIDATED_ALL_MARKET_CSV' if args.prices_csv else 'UNVERIFIED_YAHOO_UNION'),
          'price_source':('yfinance_raw_unadjusted_ohlc' if not args.prices_csv else 'supplied_csv'),
          'D20_complete_pct':float(result.D20_complete.mean()*100),
          'valid_entry_pct':valid_entry,
          'mode':'HISTORICAL_NATIVE_REPLAY_NOT_LIVE_DELIVERY',
          'official_OHLCV_parity_gated':bool(args.require_krx_parity_json),
          'historical_universe_reverse_replay':'NOT_DONE',
          'independent_historical_index_membership':'NOT_DONE',
          'overall_strategy_authority':'LIMITED_BY_HISTORICAL_UNIVERSE_PIT_UNRESOLVED' if args.require_krx_parity_json else 'PRICE_NOT_EXTERNALLY_VERIFIED',
          'warning':'D+N entry=historical scanner close; execution at closing print is not guaranteed. D+N=exact exchange trading sessions when supplied KRX daily CSV; halted day never shifts to a later bar. 0.5% roundtrip cost is an illustrative fixed assumption. Native rule35_pnl is NOT D+20. Signals must be audited for PIT market-cap/universe eligibility.'}
    (out/'native_d20_quality.json').write_text(json.dumps(diag,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(diag,ensure_ascii=False),flush=True)
    print(table[['mode','events','D5_n','D5_win_pct','D10_n','D10_win_pct','D15_n','D15_win_pct','D20_n','D20_win_pct','D20_mean_gross_pct']].to_string(index=False),flush=True)

if __name__=='__main__':main()
