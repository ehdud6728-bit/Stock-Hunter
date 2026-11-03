"""PIT candlestick geometry and 2/3-bar structure for R1-D108.

All feature rows at date t use OHLCV at or before t. No forecast rules here.
"""
from __future__ import annotations
import numpy as np
import pandas as pd

FIELDS=('date','open','high','low','close','volume','amount')

def _relation(x, tolerance_pct=0.3):
    if pd.isna(x): return 'NA'
    if x>tolerance_pct:return 'ABOVE'
    if x< -tolerance_pct:return 'BELOW'
    return 'SIMILAR'

def _div(a,b):
    return a/b.where(b>0,np.nan)

def candle_features(raw:pd.DataFrame, *, similar_pct:float=0.3)->pd.DataFrame:
    if not set(FIELDS).issubset(raw.columns):raise ValueError('Need OHLCV+date')
    d=raw.copy().reset_index(drop=True)
    d['date']=pd.to_datetime(d.date)
    if not d.date.is_monotonic_increasing or d.date.duplicated().any():raise ValueError('Unsorted/duplicate trading dates')
    for k in FIELDS[1:]:d[k]=pd.to_numeric(d[k],errors='raise').astype(float)
    if (d[['open','high','low','close']]<=0).any().any():raise ValueError('Nonpositive OHLC')
    if ((d.high<d[['open','low','close']].max(axis=1))|(d.low>d[['open','high','close']].min(axis=1))).any():
        raise ValueError('Bad OHLC range')
    span=d.high-d.low
    body=(d.close-d.open).abs()
    top=d[['close','open']].max(axis=1)
    bottom=d[['close','open']].min(axis=1)
    uw=d.high-top
    lw=bottom-d.low
    d['candle_direction']=np.where(d.close>d.open,'BULL',np.where(d.close<d.open,'BEAR','DOJI'))
    for key,series in (('candle_range',span),('candle_body',body),('candle_upper_wick',uw),('candle_lower_wick',lw),('candle_body_top',top),('candle_body_bottom',bottom)):
        d[key]=series
    d['candle_range_pct_open']=100*_div(span,d.open)
    d['candle_body_pct_open']=100*_div(body,d.open)
    d['candle_body_share_pct']=100*_div(body,span)
    d['candle_upper_wick_share_pct']=100*_div(uw,span)
    d['candle_lower_wick_share_pct']=100*_div(lw,span)
    d['candle_close_location_pct']=100*_div(d.close-d.low,span)
    # Doji with a zero body has undefined wick/body ratio, not infinity or fake zero.
    d['candle_upper_wick_body_ratio']=_div(uw,body)
    d['candle_lower_wick_body_ratio']=_div(lw,body)
    prev_close=d.close.shift(1)
    tr=pd.concat([span,(d.high-prev_close).abs(),(d.low-prev_close).abs()],axis=1).max(axis=1)
    prev_atr=tr.shift(1).rolling(14,min_periods=14).mean()
    d['candle_range_vs_prev14_atr']=_div(span,prev_atr)
    d['candle_body_vs_prev20_avg_body']=_div(body,body.shift(1).rolling(20,min_periods=20).mean())
    d['candle_gap_open_pct']=100*(d.open/prev_close-1)
    for n in (1,2,3):
        d[f'close_vs_tminus{n}_high_pct']=100*(d.close/d.high.shift(n)-1)
        d[f'close_vs_tminus{n}_bodytop_pct']=100*(d.close/top.shift(n)-1)
        d[f'high_vs_tminus{n}_high_pct']=100*(d.high/d.high.shift(n)-1)
        d[f'low_vs_tminus{n}_low_pct']=100*(d.low/d.low.shift(n)-1)
        d[f'close_vs_tminus{n}_high_class']=d[f'close_vs_tminus{n}_high_pct'].map(lambda x:_relation(x,similar_pct))
    for n in (2,3):
        prior_max=d.high.shift(1).rolling(n,min_periods=n).max()
        prior_bodytop=top.shift(1).rolling(n,min_periods=n).max()
        prior_min=d.low.shift(1).rolling(n,min_periods=n).min()
        d[f'prev{n}_high_max']=prior_max
        d[f'prev{n}_bodytop_max']=prior_bodytop
        d[f'prev{n}_low_min']=prior_min
        d[f'close_vs_prev{n}_high_max_pct']=100*(d.close/prior_max-1)
        d[f'close_vs_prev{n}_bodytop_max_pct']=100*(d.close/prior_bodytop-1)
        d[f'low_vs_prev{n}_low_min_pct']=100*(d.low/prior_min-1)
        d[f'close_vs_prev{n}_high_max_class']=d[f'close_vs_prev{n}_high_max_pct'].map(lambda x:_relation(x,similar_pct))
        d[f'bodytop_vs_prev{n}_bodytop_max_pct']=100*(top/prior_bodytop-1)
    # Candle geometry classes are hypotheses, not reversal labels or probabilities.
    def diagnostic(r):
        rg,bs,cl,uwr,lwr=r
        if not np.isfinite(rg) or rg<=0:return 'FLAT_RANGE'
        if np.isfinite(bs) and bs<=12:return 'SMALL_BODY_OR_DOJI'
        if np.isfinite(uwr) and uwr>=1 and cl<=60:return 'LONG_UPPER_WICK_REJECTION_CANDIDATE'
        if np.isfinite(lwr) and lwr>=1 and cl>=40:return 'LONG_LOWER_WICK_SUPPORT_CANDIDATE'
        if np.isfinite(bs) and bs>=65 and cl>=75:return 'BULL_BODY_DOMINANT_CLOSE_HIGH'
        if np.isfinite(bs) and bs>=65 and cl<=25:return 'BEAR_BODY_DOMINANT_CLOSE_LOW'
        return 'MIXED_CANDLE_NO_DIRECTIONAL_CLAIM'
    d['candle_shape_diagnostic']=[diagnostic(z) for z in zip(span,d.candle_body_share_pct,d.candle_close_location_pct,d.candle_upper_wick_body_ratio,d.candle_lower_wick_body_ratio)]
    return d

CANDLE_COLUMNS=('candle_direction','candle_range_pct_open','candle_body_pct_open','candle_body_share_pct',
 'candle_upper_wick_share_pct','candle_lower_wick_share_pct','candle_upper_wick_body_ratio',
 'candle_lower_wick_body_ratio','candle_close_location_pct','candle_range_vs_prev14_atr',
 'candle_body_vs_prev20_avg_body','candle_gap_open_pct','candle_shape_diagnostic',
 'close_vs_tminus2_high_pct','close_vs_tminus3_high_pct','close_vs_tminus2_bodytop_pct',
 'close_vs_tminus3_bodytop_pct','close_vs_tminus2_high_class','close_vs_tminus3_high_class',
 'close_vs_prev2_high_max_pct','close_vs_prev3_high_max_pct',
 'close_vs_prev2_high_max_class','close_vs_prev3_high_max_class',
 'close_vs_prev2_bodytop_max_pct','close_vs_prev3_bodytop_max_pct',
 'bodytop_vs_prev2_bodytop_max_pct','bodytop_vs_prev3_bodytop_max_pct',
 'low_vs_prev2_low_min_pct','low_vs_prev3_low_min_pct')
