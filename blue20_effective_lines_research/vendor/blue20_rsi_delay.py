#!/usr/bin/env python3
"""BLUE20 RSI confirmation-delay, anchored-horizon audit, RESEARCH_ONLY.

This module does not assert equivalence to legacy RSI_GC_D0_D3 methodology:
the precise RSI averaging and signal smoothing from the legacy study is unverified.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
from blue20_threeway_audit import read_prices, read_calendar, calculate_bands, audit, sha256

RSI_MODES = ('CUTLER_SMA9', 'CUTLER_EMA9', 'WILDER_SMA9', 'WILDER_EMA9')
DEADLINES = (0, 1, 2, 3, 5, 7, 10)
REFERENCE = {'UPPER': {'original':1992,'clear':1187}, 'LOWER':{'original':2240,'clear':1302}}


def cutler_rsi14(close: pd.Series) -> pd.Series:
    delta=close.diff()
    up=delta.clip(lower=0).rolling(14,min_periods=14).mean()
    down=(-delta.clip(upper=0)).rolling(14,min_periods=14).mean()
    rs=up/down.replace(0,np.nan)
    r=100-100/(1+rs)
    r=r.where(down.ne(0),100.0).where(up.ne(0),0.0)
    r=r.where((up.ne(0)) | (down.ne(0)), 50.0)
    r=r.mask(up.isna() | down.isna())
    return r


def wilder_rsi14(close: pd.Series) -> pd.Series:
    v=close.to_numpy(dtype=float)
    out=np.full(len(v),np.nan)
    if len(v)<15: return pd.Series(out,index=close.index)
    delta=np.diff(v)
    gains=np.maximum(delta,0.0); losses=np.maximum(-delta,0.0)
    avg_gain=float(gains[:14].mean()); avg_loss=float(losses[:14].mean())
    def score(u,d):
        if u==0 and d==0:return 50.0
        if d==0:return 100.0
        return 100.0-100.0/(1.0+u/d)
    out[14]=score(avg_gain,avg_loss)
    for i in range(15,len(v)):
        avg_gain=(13*avg_gain+gains[i-1])/14
        avg_loss=(13*avg_loss+losses[i-1])/14
        out[i]=score(avg_gain,avg_loss)
    return pd.Series(out,index=close.index)


def add_rsi_features(df:pd.DataFrame)->pd.DataFrame:
    parts=[]
    for code,g in df.groupby('code',sort=False):
        g=g.copy().sort_values('date')
        close=g['close']
        for basis,series in [('CUTLER',cutler_rsi14(close)),('WILDER',wilder_rsi14(close))]:
            for signal in ('SMA9','EMA9'):
                mode=f'{basis}_{signal}'
                ma = series.rolling(9,min_periods=9).mean() if signal=='SMA9' else series.ewm(span=9,adjust=False,min_periods=9).mean()
                cross=(series.shift(1)<=ma.shift(1))&(series>ma)&series.shift(1).notna()&ma.shift(1).notna()
                g[f'{mode}_RSI']=series
                g[f'{mode}_SIGNAL']=ma
                g[f'{mode}_GC']=cross.fillna(False)
        parts.append(g)
    return pd.concat(parts,ignore_index=True).sort_values(['code','date']).reset_index(drop=True)


def first_gc_delay(g:pd.DataFrame, days:pd.DatetimeIndex, mode:str):
    """Return (first observed exact GC offset, state, RSI at D0, RSI signal D0).

    All `days` (D0..D10) must be present; incomplete indicators are not counted
    as a negative or no-GC signal. EMA/SMA signal is never forward-shifted.
    """
    if not set(days).issubset(g.index):
        return None,'MISSING_DAYS',np.nan,np.nan
    z=g.loc[days]
    a=z[f'{mode}_RSI']; b=z[f'{mode}_SIGNAL']
    if a.isna().any() or b.isna().any():
        return None,'RSI_INSUFFICIENT',np.nan,np.nan
    hits=np.flatnonzero(z[f'{mode}_GC'].to_numpy(dtype=bool))
    rsi0=float(a.iloc[0]); sig0=float(b.iloc[0])
    if len(hits):
        return int(hits[0]),'GC_OBSERVED',rsi0,sig0
    return None,('ALREADY_ABOVE_NO_NEW_GC' if rsi0>sig0 else 'NO_GC_D0_D10'),rsi0,sig0


def simulate_anchored(g:pd.DataFrame, days:pd.DatetimeIndex, d0_idx:int, delay:int,
                      entry_timing:str='GC_CLOSE', cost_pct:float=.25)->dict:
    """Entry after GC on D+delay, exit deadline always fixed at D0+20.

    GC_CLOSE assumes a hypothetical fill at the confirmation day's close;
    GC_NEXT_OPEN is actionable only on the following day's opening auction.
    No price path preceding actual entry is used to choose a trade.
    """
    if entry_timing not in ('GC_CLOSE','GC_NEXT_OPEN'):raise ValueError(entry_timing)
    entry_idx=d0_idx+delay+(1 if entry_timing=='GC_NEXT_OPEN' else 0)
    end_idx=d0_idx+20
    if entry_idx>=end_idx or end_idx>=len(days):raise ValueError('Invalid entry/horizon')
    future_days=days[entry_idx+1:end_idx+1] if entry_timing=='GC_CLOSE' else days[entry_idx:end_idx+1]
    necessary=list(future_days)+[days[entry_idx]]
    if not set(necessary).issubset(g.index): raise ValueError('Missing market days inside D+20 horizon')
    entry_date=days[entry_idx]
    entry_px=float(g.loc[entry_date,'close' if entry_timing=='GC_CLOSE' else 'open'])
    tp=entry_px*1.10; sl=entry_px*.95
    future=g.loc[future_days]
    # D0-close: start testing following day; next-open: test intra-day on same date.
    max_high=float(future.high.max()); min_low=float(future.low.min())
    hit10_any=max_high>=tp
    exit_px=float(g.loc[days[end_idx],'close']); exit_reason='TIME_D20'
    exit_offset=20; ambiguous=False
    for date,r in future.iterrows():
        offset=int(np.searchsorted(days,date))-d0_idx
        o,h,l=(float(r[c]) for c in ('open','high','low'))
        if not (entry_timing=='GC_NEXT_OPEN' and date==entry_date):
            if o<=sl:
                exit_px=o;exit_reason='SL_GAP';exit_offset=offset;break
            if o>=tp:
                exit_px=o;exit_reason='TP_GAP';exit_offset=offset;break
        if l<=sl and h>=tp:
            exit_px=sl;exit_reason='SL_BOTH_HIT';exit_offset=offset;ambiguous=True;break
        if l<=sl:
            exit_px=sl;exit_reason='SL';exit_offset=offset;break
        if h>=tp:
            exit_px=tp;exit_reason='TP';exit_offset=offset;break
    return dict(entry_date=entry_date.strftime('%Y-%m-%d'),entry_price=entry_px,
                exit_date=days[d0_idx+exit_offset].strftime('%Y-%m-%d'),
                exit_offset=exit_offset,exit_reason=exit_reason, exit_price=exit_px,
                ambiguous=ambiguous, hit10_any=bool(hit10_any),
                net_pct=100*(exit_px/entry_px-1)-cost_pct,
                mfe_pct=100*(max_high/entry_px-1)-cost_pct,
                mae_pct=100*(min_low/entry_px-1)-cost_pct)


def run_rsi_analysis(ohlcv,cal,start_year=2023,end_year=2024,cost_pct=.25):
    # Callers pass raw input already read. This preserves the original threeway event universe.
    df=add_rsi_features(calculate_bands(ohlcv))
    ev,threeway_counts=audit(df,cal,start_year,end_year,cost_pct)
    if ev.empty:
        return pd.DataFrame(),threeway_counts
    source=ev.loc[ev['entry_type'].eq('D0_CLOSE')].copy()
    if len(source)!=len(ev)//2:raise RuntimeError('Threeway baseline event identity mismatch')
    idx_of={pd.Timestamp(d):i for i,d in enumerate(cal)}
    by_code={c:g.set_index('date').sort_index() for c,g in df.groupby('code')}
    rows=[]
    for event in source.itertuples(index=False):
        g=by_code[event.code]; day0=pd.Timestamp(event.date); d0_idx=idx_of[day0]
        interval=cal[d0_idx:d0_idx+11]
        # Existing threeway audit guarantees a complete 20-day market calendar path.
        d0close=float(g.loc[day0,'close'])
        for mode in RSI_MODES:
            delay,status,rsi0,sig0=first_gc_delay(g,interval,mode)
            out=dict(code=event.code, date=event.date, year=event.year,
                     band=event.band,label=event.label, rsi_mode=mode,
                     gc_status=status, gc_offset=delay, gc_date='',
                     rsi_d0=rsi0, signal_d0=sig0, already_above_d0=(rsi0>sig0 if np.isfinite(rsi0) else None),
                     d0_price=d0close, d0_base_net_pct=float(event.net_pct),
                     d0_base_exit_reason=event.exit_reason,
                     gc_price_move_from_d0_pct=np.nan,
                     tp_before_gc=False, sl_before_gc=False)
            if delay is not None:
                gc_date=cal[d0_idx+delay]
                out['gc_date']=gc_date.strftime('%Y-%m-%d')
                out['gc_price_move_from_d0_pct']=100*(float(g.loc[gc_date,'close'])/d0close-1)
                out['rsi_gc']=float(g.loc[gc_date,f'{mode}_RSI'])
                out['signal_gc']=float(g.loc[gc_date,f'{mode}_SIGNAL'])
                if delay>0:
                    before=g.loc[cal[d0_idx+1:d0_idx+delay+1]]
                    out['tp_before_gc']=bool((before.high>=d0close*1.10).any())
                    out['sl_before_gc']=bool((before.low<=d0close*.95).any())
                for timing in ('GC_CLOSE','GC_NEXT_OPEN'):
                    trade=simulate_anchored(g,cal,d0_idx,delay,timing,cost_pct)
                    for key,val in trade.items():out[f'{timing}_{key}']=val
            rows.append(out)
    return pd.DataFrame(rows),threeway_counts


def _score(x,prefix):
    if len(x)==0:return dict(n=0,net_mean_pct=np.nan,net_median_pct=np.nan,net_win_pct=np.nan,tp_rate_pct=np.nan,sl_rate_pct=np.nan,hit10_any_pct=np.nan,mae_median_pct=np.nan)
    v=x[f'{prefix}_net_pct'].astype(float)
    reason=x[f'{prefix}_exit_reason'].astype(str)
    return dict(n=len(x),net_mean_pct=float(v.mean()),net_median_pct=float(v.median()),
                net_win_pct=float((v>0).mean()*100),
                tp_rate_pct=float(reason.str.startswith('TP').mean()*100),
                sl_rate_pct=float(reason.str.startswith('SL').mean()*100),
                hit10_any_pct=float(x[f'{prefix}_hit10_any'].astype(float).mean()*100),
                mae_median_pct=float(x[f'{prefix}_mae_pct'].astype(float).median()))


def delay_table(rows):
    out=[]
    if rows.empty:return pd.DataFrame()
    cohorts=[('ALL',rows)]+[(str(y),rows[rows.year==y]) for y in sorted(rows.year.unique())] if 'year' in rows else [('ALL',rows)]
    for cohort,cohort_rows in cohorts:
      for (mode,band,label),sub in cohort_rows.groupby(['rsi_mode','band','label'],sort=True):
        for bucket in list(range(11))+['NONE']:
            s=sub[sub.gc_offset.eq(bucket)] if bucket!='NONE' else sub[sub.gc_offset.isna()]
            if bucket=='NONE':
                out.append(dict(cohort=cohort,rsi_mode=mode,band=band,label=label,gc_offset=bucket,n=len(s),
                                gc_close_net_mean_pct=np.nan,gc_next_open_net_mean_pct=np.nan,
                                d0_same_cohort_net_mean_pct=float(s.d0_base_net_pct.mean()) if len(s) else np.nan,
                                win_pct=np.nan,tp_rate_pct=np.nan,sl_rate_pct=np.nan,hit10_any_pct=np.nan,
                                median_pre_gc_price_change_pct=np.nan, pre_gc_d0_tp_pct=np.nan,pre_gc_d0_sl_pct=np.nan))
                continue
            t=_score(s,'GC_CLOSE')
            out.append(dict(cohort=cohort,rsi_mode=mode,band=band,label=label,gc_offset=bucket,
                 n=len(s),gc_close_net_mean_pct=t['net_mean_pct'],
                 gc_next_open_net_mean_pct=float(s.GC_NEXT_OPEN_net_pct.mean()) if len(s) else np.nan,
                 d0_same_cohort_net_mean_pct=float(s.d0_base_net_pct.mean()) if len(s) else np.nan,
                 win_pct=t['net_win_pct'],tp_rate_pct=t['tp_rate_pct'],sl_rate_pct=t['sl_rate_pct'],
                 hit10_any_pct=t['hit10_any_pct'],
                 median_pre_gc_price_change_pct=float(s.gc_price_move_from_d0_pct.median()) if len(s) else np.nan,
                 pre_gc_d0_tp_pct=float(s.tp_before_gc.mean()*100) if len(s) else np.nan,
                 pre_gc_d0_sl_pct=float(s.sl_before_gc.mean()*100) if len(s) else np.nan))
    return pd.DataFrame(out)


def cutoff_table(rows):
    out=[]
    if rows.empty:return pd.DataFrame()
    # Whole candidate universe denominator, not just favorable future-GC events.
    cohorts=[('ALL',rows)]+[(str(y),rows[rows.year==y]) for y in sorted(rows.year.unique())] if 'year' in rows else [('ALL',rows)]
    for cohort,cohort_rows in cohorts:
      for (mode,band,label),sub in cohort_rows.groupby(['rsi_mode','band','label'],sort=True):
        for limit in DEADLINES:
            taken=sub[sub.gc_offset.le(limit)]
            no_trade=len(sub)-len(taken)
            d0_mean=float(sub.d0_base_net_pct.mean())
            accepted_cl=float(taken.GC_CLOSE_net_pct.sum()/len(sub)) if len(sub) else np.nan
            accepted_nx=float(taken.GC_NEXT_OPEN_net_pct.sum()/len(sub)) if len(sub) else np.nan
            out.append(dict(cohort=cohort,rsi_mode=mode,band=band,label=label,cutoff_days=limit,
                     candidate_n=len(sub),trade_n=len(taken),no_trade_n=no_trade,
                     trade_rate_pct=100*len(taken)/len(sub) if len(sub) else np.nan,
                     trading_only_gc_close_net_mean_pct=float(taken.GC_CLOSE_net_pct.mean()) if len(taken) else np.nan,
                     trading_only_gc_next_open_net_mean_pct=float(taken.GC_NEXT_OPEN_net_pct.mean()) if len(taken) else np.nan,
                     universe_contribution_gc_close_pct=accepted_cl,
                     universe_contribution_gc_next_open_pct=accepted_nx,
                     all_candidates_d0_net_mean_pct=d0_mean,
                     selected_subset_d0_net_mean_pct=float(taken.d0_base_net_pct.mean()) if len(taken) else np.nan,
                     selected_gc_close_sl_rate_pct=float(taken.GC_CLOSE_exit_reason.astype(str).str.startswith('SL').mean()*100) if len(taken) else np.nan,
                     selected_gc_close_tp_rate_pct=float(taken.GC_CLOSE_exit_reason.astype(str).str.startswith('TP').mean()*100) if len(taken) else np.nan))
    return pd.DataFrame(out)


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ohlcv',required=True)
    p.add_argument('--calendar',required=True)
    p.add_argument('--out',required=True)
    p.add_argument('--start-year',type=int,default=2023)
    p.add_argument('--end-year',type=int,default=2024)
    p.add_argument('--strict-legacy-counts',action='store_true',help='Fail on reference mismatch; only use same frozen 500-stock cohort')
    a=p.parse_args(argv)
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    prices=read_prices(a.ohlcv);cal=read_calendar(a.calendar)
    events,counts=run_rsi_analysis(prices,cal,a.start_year,a.end_year)
    exact=delay_table(events);cutoffs=cutoff_table(events)
    observed={}
    for band,expect in REFERENCE.items():
        ref=events[(events.band==band)&(events.rsi_mode==RSI_MODES[0])]
        ob={'original':int(ref.label.isin(('MICRO_BODY','CLEAR_BODY')).sum()),'clear':int(ref.label.eq('CLEAR_BODY').sum())}
        observed[band]={'actual':ob,'reference':expect,'match':ob==expect}
    reference_ok=all(x['match'] for x in observed.values())
    status='REFERENCE_MATCHED_RSI_FORMULA_UNCERTIFIED' if reference_ok else 'REFERENCE_MISMATCH_OR_DIFFERENT_UNIVERSE'
    manifest={
        'status':status,'research_only':True,'not_production_ready':True,
        'bands':'SHIFT(BBandsUp/Down(20,2),26) ddof=0',
        'rsi_formula_modes':{
            'CUTLER_SMA9':'RSI14 based on rolling SMA gains/losses; 9-day SMA of RSI',
            'CUTLER_EMA9':'RSI14 rolling SMA; 9-day EMA RSI signal',
            'WILDER_SMA9':'Wilder RSI14 with initial SMA14 seed; 9-day SMA of RSI',
            'WILDER_EMA9':'Wilder RSI14; 9-day EMA RSI signal'},
        'gc_logic':'RSI[t-1]<=SIGNAL[t-1] and RSI[t]>SIGNAL[t]; no positive cross unless previous value exists',
        'legacy_rsi_gc_formula_reproduced':False,
        'gc_first_delay_window':'D0..D10 inclusive; first exact crossing only',
        'cutoff_market_days':list(DEADLINES),'fixed_d0_horizon_market_days':20,
        'entry_assumptions':['GC_CLOSE','GC_NEXT_OPEN'],
        'stop_target_fees':{'tp_pct':10,'sl_pct':-5,'roundtrip_cost_pp':.25,'same_day_tp_and_sl':'STOP_FIRST'},
        'source_sha256':{'ohlcv':sha256(a.ohlcv),'calendar':sha256(a.calendar)},
        'study_years':[a.start_year,a.end_year],
        'prior_4232_2489_body_reference':observed,
        'event_counts':counts,
        'warnings':['Legacy signal moving average SMA/EMA and RSI method not definitively recovered; analyze all four modes as sensitivity, never silently equate to previous +0.183%/+0.014% results',
                    'D0_CLOSE and GC_CLOSE are hypothetical close fills; next open is an execution sensitivity, not certified execution',
                    'A cutoff table no-trade 0 contribution is per-candidate arithmetic, NOT capital-allocated portfolio performance',
                    'Matched D0 returns within eventually GC-confirmed cohort are retrospective outcome comparisons; not an ex-ante D0 trade selection',
                    'No OOS/PIT corporate action certification, marketability or actual brokerage fills are performed',
                    'Signals on same date and same symbol across bands are not independent']}
    if a.strict_legacy_counts and not reference_ok:
        (out/'AUDIT_FAIL_CLOSED.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
        raise SystemExit('FAIL CLOSED: frozen 500-stock event reference did not reproduce')
    events.to_csv(out/'RSI_DELAY_EVENTS.csv',index=False,encoding='utf-8-sig')
    exact.to_csv(out/'RSI_DELAY_EXACT_D0_D10.csv',index=False,encoding='utf-8-sig')
    cutoffs.to_csv(out/'RSI_DELAY_CUTOFFS.csv',index=False,encoding='utf-8-sig')
    (out/'RSI_DELAY_AUDIT_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'original_events':len(events)//len(RSI_MODES),'event_formula_rows':len(events),
                      'reference_match':reference_ok, 'status':status,'out':str(out)},ensure_ascii=False,indent=2))

if __name__=='__main__':main()
