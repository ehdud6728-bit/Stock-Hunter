"""Deterministic OHLCV-only, point-in-time FULL event-day context.

All rolling indicators at bar t depend on bars <=t. Ichimoku cloud shown
*at current date* uses projected span computed 26 bars earlier, not t+26.
"""
from __future__ import annotations
import numpy as np
import pandas as pd

MA=(3,5,10,20,30,40,60,112,120,224,240,448,480)


def features(frame):
    d=frame.copy().reset_index(drop=True)
    d['date']=pd.to_datetime(d['date'])
    if d.date.duplicated().any() or not d.date.is_monotonic_increasing:
        raise ValueError('OHLCV must have unique increasing dates')
    for key in ('open','high','low','close','volume','amount'):
        d[key]=pd.to_numeric(d[key],errors='raise').astype(float)
    if ((d.high<d[['open','close','low']].max(axis=1))|(d.low>d[['open','close','high']].min(axis=1))).any():
        raise ValueError('inconsistent candlestick')
    c=d.close;h=d.high;l=d.low;o=d.open
    prev=c.shift(1)
    tr=pd.concat([(h-l),(h-prev).abs(),(l-prev).abs()],axis=1).max(axis=1)
    d['atr14']=tr.rolling(14,min_periods=14).mean()
    width=(h-l).replace(0,np.nan)
    body=(c-o).abs(); wick_hi=h-pd.concat([c,o],axis=1).max(axis=1)
    wick_lo=pd.concat([c,o],axis=1).min(axis=1)-l
    d['candle_body_pct']=100*body/width
    d['upper_wick_pct']=100*wick_hi/width
    d['lower_wick_pct']=100*wick_lo/width
    d['wick_to_body_upper']=wick_hi/body.replace(0,np.nan)
    d['wick_to_body_lower']=wick_lo/body.replace(0,np.nan)
    d['close_location_pct']=100*(c-l)/width
    d['candle_range_vs_atr14']=width/d.atr14
    d['close_vs_prev2_high_pct']=100*(c/h.shift(1).rolling(2,min_periods=2).max()-1)
    d['close_vs_prev3_high_pct']=100*(c/h.shift(1).rolling(3,min_periods=3).max()-1)
    d['close_vs_prev3_body_top_pct']=100*(c/pd.concat([c,o],axis=1).max(axis=1).shift(1).rolling(3,min_periods=3).max()-1)
    d['amount_vs_prev20']=d.amount/d.amount.shift(1).rolling(20,min_periods=20).mean().replace(0,np.nan)
    d['volume_vs_prev20']=d.volume/d.volume.shift(1).rolling(20,min_periods=20).mean().replace(0,np.nan)
    d['amount_avg5_vs_prev20']=d.amount.rolling(5,min_periods=5).mean()/d.amount.shift(1).rolling(20,min_periods=20).mean().replace(0,np.nan)
    for n in MA:
        m=c.rolling(n,min_periods=n).mean()
        d[f'ma{n}']=m
        d[f'ma{n}_gap_pct']=100*(c/m-1)
        for k in (5,20):
            d[f'ma{n}_slope{k}_pct']=100*(m/m.shift(k)-1)
        d[f'ma{n}_slope_accel5_pp']=d[f'ma{n}_slope20_pct']-d[f'ma{n}_slope20_pct'].shift(5)
    for n in (20,30,40):
        m=d[f'ma{n}'];sd=c.rolling(n,min_periods=n).std(ddof=0)
        u=m+2*sd;b=m-2*sd
        d[f'bb{n}_upper']=u;d[f'bb{n}_lower']=b
        d[f'bb{n}_width_pct']=100*(u-b)/m
        d[f'bb{n}_width_delta5_pp']=d[f'bb{n}_width_pct']-d[f'bb{n}_width_pct'].shift(5)
        d[f'bb{n}_upper_slope5_pct']=100*(u/u.shift(5)-1)
        d[f'bb{n}_lower_slope5_pct']=100*(b/b.shift(5)-1)
        d[f'bb{n}_position']=np.select([c>u,c<b],['ABOVE_UPPER','BELOW_LOWER'],default='INSIDE')
    for n in (20,40):
        m=d[f'ma{n}'];u=m*1.1;b=m*.9
        d[f'env{n}_10_upper']=u;d[f'env{n}_10_lower']=b
        d[f'env{n}_10_position']=np.select([c>u,c<b],['ABOVE_UPPER','BELOW_LOWER'],default='INSIDE')
        d[f'env{n}_10_upper_slope5_pct']=100*(u/u.shift(5)-1)
        d[f'env{n}_10_lower_slope5_pct']=100*(b/b.shift(5)-1)
        d[f'env{n}_10_upper_gap_pct']=100*(c/u-1)
        d[f'env{n}_10_lower_gap_pct']=100*(c/b-1)
    for key,windows in (('fast',(3,5,10,20)),('mid',(30,40,60,112,120)),('long',(224,240,448,480))):
        mat=d[[f'ma{n}' for n in windows]]
        # Requiring all averages avoids partial-cloud lookahead/definition drift.
        complete=mat.notna().all(axis=1)
        u=mat.max(axis=1).where(complete);b=mat.min(axis=1).where(complete)
        d[f'ma_cloud_{key}_upper']=u;d[f'ma_cloud_{key}_lower']=b
        d[f'ma_cloud_{key}_thickness_pct']=100*(u-b)/c
        d[f'ma_cloud_{key}_upper_slope5_pct']=100*(u/u.shift(5)-1)
        d[f'ma_cloud_{key}_lower_slope5_pct']=100*(b/b.shift(5)-1)
        d[f'ma_cloud_{key}_height_vs_price_pct']=100*((u+b)/2/c-1)
        d[f'ma_cloud_{key}_position']=np.select([c>u,c<b],['ABOVE','BELOW'],default='INSIDE')
        d.loc[~complete,f'ma_cloud_{key}_position']='NA'
    # Standard visible (already projected) Ichimoku cloud: spans formed at
    # t-26 are used at t; current/future spans are never read at signal t.
    tenkan=(h.rolling(9,min_periods=9).max()+l.rolling(9,min_periods=9).min())/2
    kijun=(h.rolling(26,min_periods=26).max()+l.rolling(26,min_periods=26).min())/2
    sa=((tenkan+kijun)/2).shift(26)
    sb=((h.rolling(52,min_periods=52).max()+l.rolling(52,min_periods=52).min())/2).shift(26)
    u=pd.concat([sa,sb],axis=1).max(axis=1).where(sa.notna() & sb.notna());b=pd.concat([sa,sb],axis=1).min(axis=1).where(sa.notna() & sb.notna())
    d['ichimoku_visible_upper']=u;d['ichimoku_visible_lower']=b
    d['ichimoku_visible_thickness_pct']=100*(u-b)/c
    d['ichimoku_visible_height_vs_price_pct']=100*((u+b)/2/c-1)
    d['ichimoku_visible_position']=np.select([c>u,c<b],['ABOVE','BELOW'],default='INSIDE')
    d.loc[sa.isna()|sb.isna(),'ichimoku_visible_position']='NA'
    d['idx']=np.arange(len(d))
    d['date']=d.date.dt.strftime('%Y-%m-%d')
    return d.copy()  # defragment once after constructing all features


def join_event_context(events,priceframes):
    """Left join EVERY R113 event, report missing raw bars explicitly."""
    if events.empty:return events.copy()
    a=events.copy()
    if a[['code','idx','pattern','basis','event']].isna().any().any():
        raise ValueError('incomplete event identity')
    extras=[]
    for code,ev in a.groupby('code',sort=True):
        if code not in priceframes:raise ValueError('missing raw stock: '+str(code))
        ctx=features(priceframes[code]);ctx['code']=code
        if any(int(i)>=len(ctx) or int(i)<0 for i in ev['idx']):
            raise ValueError('event index exceeds actual OHLCV: '+str(code))
        selected=ctx[ctx.idx.isin(ev.idx)].copy()
        selected=selected.drop(columns=['open','high','low','close','volume','amount','date'],errors='ignore')
        extras.append(selected)
    meta=pd.concat(extras,ignore_index=True)
    if meta.duplicated(['code','idx']).any():raise AssertionError('duplicate stock-day context')
    out=a.merge(meta,on=['code','idx'],how='left',validate='many_to_one',indicator='_raw_join')
    if not out['_raw_join'].eq('both').all():raise AssertionError('missing R113 event raw context')
    out=out.drop(columns=['_raw_join'])
    out['all_event_ohlcv_joined']=True
    # Warmup availability is NOT a data-coverage failure.
    out['ma224_history_ready']=out.idx>=223
    out['ma480_history_ready']=out.idx>=479
    out['ichimoku_visible_ready']=out.idx>=77
    return out
