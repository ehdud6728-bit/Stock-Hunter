"""R1-D102 PIT moving-average ribbons + Ichimoku Kumo geometry.

Cloud = a *range* between two boundaries, not a return forecast.
Two distinct constructs: MA ribbon clouds and traditional Ichimoku 9/26/52.
All values plotted at t are available at/before t; displayed current Kumo
uses span calculations from t-26, NOT data from the future.
"""
from __future__ import annotations
import numpy as np
import pandas as pd

MA_GROUPS = {
    'FAST': (3,5,10,20),
    'MID': (30,40,60,112,120),
    'LONG': (224,240,448,480),
    'ALL': (3,5,10,20,30,40,60,112,120,224,240,448,480),
}


def pct(series, n):
    return 100*(series/series.shift(n)-1)


def _position(price, low, high):
    """No invented position while a cloud is unformed."""
    return pd.Series(np.select([price>high,price<low],['ABOVE','BELOW'],default='INSIDE'),
                     index=price.index).where(low.notna() & high.notna(), 'NOT_AVAILABLE')


def _cloud_metrics(close, lo, hi, prefix):
    mid=(lo+hi)/2
    width=100*(hi-lo)/mid.replace(0,np.nan)
    center_gap=100*(mid/close-1)
    out={
        f'{prefix}_upper':hi, f'{prefix}_lower':lo, f'{prefix}_mid':mid,
        f'{prefix}_height_vs_price_pct':center_gap,
        f'{prefix}_upper_vs_price_pct':100*(hi/close-1),
        f'{prefix}_lower_vs_price_pct':100*(lo/close-1),
        f'{prefix}_thickness_pct':width,
        f'{prefix}_thickness_abs':hi-lo,
        f'{prefix}_position':_position(close,lo,hi),
        f'{prefix}_thickness_change5_pp':width-width.shift(5),
        f'{prefix}_thickness_change20_pp':width-width.shift(20),
        f'{prefix}_height_change5_pp':center_gap-center_gap.shift(5),
        f'{prefix}_height_change20_pp':center_gap-center_gap.shift(20),
        f'{prefix}_upper_slope5_pct':pct(hi,5),
        f'{prefix}_lower_slope5_pct':pct(lo,5),
        f'{prefix}_mid_slope5_pct':pct(mid,5),
    }
    # normalized position where 0=lower edge, 1=upper edge; may be outside [0,1]
    out[f'{prefix}_price_percentile_inside']=((close-lo)/(hi-lo).replace(0,np.nan))
    out[f'{prefix}_cross_above_today']=((close>hi)&(close.shift(1)<=hi.shift(1))&hi.notna()&hi.shift(1).notna()).fillna(False)
    out[f'{prefix}_cross_below_today']=((close<lo)&(close.shift(1)>=lo.shift(1))&lo.notna()&lo.shift(1).notna()).fillna(False)
    return out


def _midpoint(high,low,n):
    return (high.rolling(n,min_periods=n).max()+low.rolling(n,min_periods=n).min())/2


def enrich_clouds(features:pd.DataFrame)->pd.DataFrame:
    required={'date','open','high','low','close'}|{f'ma{i}' for group in MA_GROUPS.values() for i in group}
    if not required.issubset(features.columns):
        raise ValueError('missing required OHLCV/MA feature columns: '+str(sorted(required-set(features.columns))))
    d=features.copy().reset_index(drop=True)
    if not pd.to_datetime(d.date).is_monotonic_increasing or pd.to_datetime(d.date).duplicated().any():
        raise ValueError('dates must be unique and sorted')
    c=d.close.astype(float)
    f={}
    for name,windows in MA_GROUPS.items():
        mat=d[[f'ma{n}' for n in windows]]
        complete=mat.notna().all(axis=1)
        low=mat.min(axis=1).where(complete)
        high=mat.max(axis=1).where(complete)
        f.update(_cloud_metrics(c,low,high,'ma_cloud_'+name.lower()))
    # Price level of each cloud, which can be rising while thickness falls.
    tenkan=_midpoint(d.high,d.low,9)
    kijun=_midpoint(d.high,d.low,26)
    raw_a=(tenkan+kijun)/2
    raw_b=_midpoint(d.high,d.low,52)
    # CURRENT displayed cloud at chart date t is derived from observations <=t-26.
    # 'projected' is known today but plotted 26 bar positions ahead; it is not a future realization.
    current_a=raw_a.shift(26)
    current_b=raw_b.shift(26)
    f['ichimoku_tenkan']=tenkan
    f['ichimoku_kijun']=kijun
    f['ichimoku_current_span_a']=current_a
    f['ichimoku_current_span_b']=current_b
    f['ichimoku_projected_span_a_known_today']=raw_a
    f['ichimoku_projected_span_b_known_today']=raw_b
    for part,a,b in [('current',current_a,current_b),('projected_known_today',raw_a,raw_b)]:
        lo=pd.concat([a,b],axis=1).min(axis=1).where(a.notna() & b.notna())
        hi=pd.concat([a,b],axis=1).max(axis=1).where(a.notna() & b.notna())
        prefix=f'ichimoku_{part}'
        f.update(_cloud_metrics(c,lo,hi,prefix))
        sign=np.sign(a-b)
        f[f'{prefix}_bullish_cloud']=(a>b).where(a.notna()&b.notna())
        f[f'{prefix}_bearish_cloud']=(a<b).where(a.notna()&b.notna())
        f[f'{prefix}_twist_today']=((sign*sign.shift(1)<0)&a.notna()&b.notna()&a.shift(1).notna()&b.shift(1).notna()).fillna(False)
    for n in (3,5,10,20,30,40,60,112,120,224,240,448,480):
        slope=d[f'ma{n}_slope20_pct']
        f[f'ma{n}_slope20_acceleration5_pp']=slope-slope.shift(5)
        f[f'ma{n}_slope20_turn_up_today']=((slope>0)&(slope.shift(1)<=0)).fillna(False)
        f[f'ma{n}_slope20_turn_down_today']=((slope<0)&(slope.shift(1)>=0)).fillna(False)
    return pd.concat([d,pd.DataFrame(f,index=d.index)],axis=1)


def pattern_state(row)->dict:
    """Predeclared coarse signatures only; no learned thresholds or buy labels."""
    def get(k):
        return row[k] if k in row else np.nan
    def direction(v):
        return 'NA' if pd.isna(v) else ('UP' if v>0 else 'DOWN' if v<0 else 'FLAT')
    def band(v):
        return 'NA' if pd.isna(v) else ('EXPAND' if v>0 else 'CONTRACT' if v<0 else 'FLAT')
    return {
        'ma224_zone':('BELOW' if row.close<row.ma224 else 'ABOVE') if pd.notna(row.ma224) else 'NA',
        'fast_cloud_thickness_trend':band(get('ma_cloud_fast_thickness_change5_pp')),
        'mid_cloud_thickness_trend':band(get('ma_cloud_mid_thickness_change5_pp')),
        'long_cloud_thickness_trend':band(get('ma_cloud_long_thickness_change5_pp')),
        'fast_cloud_height_trend':direction(get('ma_cloud_fast_height_change5_pp')),
        'ma20_slope_accel':direction(get('ma20_slope20_acceleration5_pp')),
        'ma112_slope_accel':direction(get('ma112_slope20_acceleration5_pp')),
        'ma224_slope_accel':direction(get('ma224_slope20_acceleration5_pp')),
        'bb20_width_trend':band(get('bb20_width_change5_pp')),
        'volume_vs_prev20_zone':('ABOVE_1X' if get('volume_vs_prev20')>=1 else 'BELOW_1X') if pd.notna(get('volume_vs_prev20')) else 'NA',
        'amount_vs_prev20_zone':('ABOVE_1X' if get('amount_vs_prev20')>=1 else 'BELOW_1X') if pd.notna(get('amount_vs_prev20')) else 'NA',
        'ichimoku_current_position':get('ichimoku_current_position'),
        'ichimoku_projected_bullish':('YES' if bool(get('ichimoku_projected_known_today_bullish_cloud')) else 'NO') if pd.notna(get('ichimoku_projected_known_today_bullish_cloud')) else 'NA',
    }


def choose_unrestricted(features,labels,*,min_idx=541,horizon=20,max_per_group=8,cooldown=20):
    """PIT eligibility independent of MA224; future outcomes used ONLY for ex-post study stratification."""
    x=features[['idx','date','close','ma224']].merge(labels,on=['idx','date'],how='inner',validate='one_to_one')
    eligible=x[(x.idx>=min_idx)&x.ma224.notna()&x.forward20_peak_pct.notna()].sort_values('idx').copy()
    eligible['ma224_zone']=np.where(eligible.close<eligible.ma224,'BELOW','ABOVE')
    eligible['outcome']=np.select([eligible.is_sustained_surge,eligible.is_nonrise_control],['SURGE','CONTROL'],default='OTHER')
    # Single global cooldown across the two outcome groups, NOT two overlapping shortlists.
    last=-10**9
    count={}
    cases=[]
    for r in eligible.itertuples():
        group=(r.outcome,r.ma224_zone)
        if r.outcome=='OTHER' or r.idx-last<cooldown or count.get(group,0)>=max_per_group:
            continue
        cases.append({'idx':int(r.idx),'asof':str(r.date.date()),'label':r.outcome,'ma224_zone':r.ma224_zone,
                      'forward20_peak_pct':float(r.forward20_peak_pct),'forward20_close_pct':float(r.forward20_close_pct),
                      'future_peak_date':r.future_peak_date,'close_asof':float(r.close)})
        last=int(r.idx)
        count[group]=count.get(group,0)+1
    return eligible,pd.DataFrame(cases)
