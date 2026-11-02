"""R1-D101 PIT pre-rise features and outcome-separated research, RESEARCH_ONLY.

All input features use data on/before the observation date. Future price labels
are calculated in a disjoint function and NEVER affect historical features.
Do not interpret the in-sample descriptive differences as live efficacy.
"""
from __future__ import annotations
import itertools
import math
import numpy as np
import pandas as pd

# Explicit finite research universe; optional additional periods in config. "All"
# means all CONFIGURED MA periods, not every mathematically possible period.
MA_WINDOWS = (3, 5, 10, 20, 30, 40, 60, 112, 120, 224, 240, 448, 480)
BB_WINDOWS = (20, 30, 40)
ENV_WINDOWS = (20, 40)
ENV_FRAC = 0.10
FWD_BARS = 20


def validate(d: pd.DataFrame) -> pd.DataFrame:
    required={'date','open','high','low','close','volume','amount'}
    if not required.issubset(d.columns):
        raise ValueError('Missing OHLCV/amount: '+str(sorted(required-set(d.columns))))
    x=d.copy().reset_index(drop=True)
    x['date']=pd.to_datetime(x['date'])
    if x.date.isna().any() or x.date.duplicated().any() or not x.date.is_monotonic_increasing:
        raise ValueError('Dates must be unique, valid and increasing')
    for c in ('open','high','low','close','volume','amount'):
        x[c]=pd.to_numeric(x[c],errors='raise').astype(float)
        if not np.isfinite(x[c].to_numpy(dtype=float)).all():
            raise ValueError('Nonfinite '+c)
    if (x[['open','high','low','close']]<=0).any().any() or (x[['volume','amount']]<0).any().any():
        raise ValueError('Invalid OHLCV sign')
    if ((x['high'] < x[['open','close','low']].max(axis=1)) | (x['low'] > x[['open','close']].min(axis=1))).any():
        raise ValueError('OHLCV high/low invalid')
    return x


def _pct(x:pd.Series,back:int) -> pd.Series:
    return 100*(x/x.shift(back)-1)


def build_features(raw:pd.DataFrame, *, ma_windows=MA_WINDOWS) -> pd.DataFrame:
    """Build a wide PIT feature matrix with one column-concat (avoid fragmentation)."""
    x=validate(raw)
    periods=tuple(sorted(set(int(p) for p in ma_windows)))
    if not periods or any(p<2 for p in periods):
        raise ValueError('MA periods must be >=2')
    f={'idx':pd.Series(np.arange(len(x),dtype=int))}
    for n in periods:
        ma=x.close.rolling(n,min_periods=n).mean()
        prefix=f'ma{n}'
        f[prefix]=ma
        gap=100*(x.close/ma-1)
        f[f'{prefix}_price_gap_pct']=gap
        f[f'{prefix}_slope5_pct']=_pct(ma,5)
        slope20=_pct(ma,20)
        f[f'{prefix}_slope20_pct']=slope20
        f[f'{prefix}_slope20_accel_pp']=slope20-slope20.shift(20)
        f[f'{prefix}_price_gap_change5_pp']=gap-gap.shift(5)
    for a,b in itertools.combinations(periods,2):
        gap=100*(f[f'ma{a}']/f[f'ma{b}']-1)
        p=f'ma{a}_ma{b}'
        f[f'{p}_gap_pct']=gap
        f[f'{p}_gap_change5_pp']=gap-gap.shift(5)
        f[f'{p}_gap_change20_pp']=gap-gap.shift(20)
        f[f'{p}_cross_up_today']=((gap>0)&(gap.shift(1)<=0)).fillna(False)
        f[f'{p}_cross_down_today']=((gap<0)&(gap.shift(1)>=0)).fillna(False)
    for n in BB_WINDOWS:
        center=x.close.rolling(n,min_periods=n).mean()
        sd=x.close.rolling(n,min_periods=n).std(ddof=0)
        up,low=center+2*sd,center-2*sd
        p=f'bb{n}'
        f[f'{p}_mid']=center;f[f'{p}_upper']=up;f[f'{p}_lower']=low
        width=100*(up-low)/center
        f[f'{p}_width_pct']=width
        f[f'{p}_width_change5_pp']=width-width.shift(5)
        f[f'{p}_width_change20_pp']=width-width.shift(20)
        f[f'{p}_upper_slope5_pct']=_pct(up,5)
        f[f'{p}_lower_slope5_pct']=_pct(low,5)
        f[f'{p}_distance_upper_pct']=100*(x.close/up-1)
        f[f'{p}_distance_lower_pct']=100*(x.close/low-1)
        f[f'{p}_above_upper']=x.close>up
        f[f'{p}_below_lower']=x.close<low
        f[f'{p}_width_rank120_pct']=width.rolling(120,min_periods=40).apply(
            lambda vals: 100*np.mean(vals<=vals[-1]),raw=True)
    for n in ENV_WINDOWS:
        center=x.close.rolling(n,min_periods=n).mean()
        p=f'env{n}_10'
        up=center*(1+ENV_FRAC);low=center*(1-ENV_FRAC)
        abs_width=up-low
        f[f'{p}_mid']=center;f[f'{p}_upper']=up;f[f'{p}_lower']=low
        f[f'{p}_width_abs']=abs_width
        f[f'{p}_width_rel_pct']=100*abs_width/center  # constant 20%
        f[f'{p}_width_abs_change5_pct']=_pct(abs_width,5)
        f[f'{p}_upper_slope5_pct']=_pct(up,5)
        f[f'{p}_lower_slope5_pct']=_pct(low,5)
        f[f'{p}_distance_upper_pct']=100*(x.close/up-1)
        f[f'{p}_distance_lower_pct']=100*(x.close/low-1)
        f[f'{p}_above_upper']=x.close>up
        f[f'{p}_below_lower']=x.close<low
    for field in ('volume','amount'):
        prev20=x[field].shift(1).rolling(20,min_periods=20).mean()
        f[f'{field}_vs_prev20']=x[field]/prev20.replace(0,np.nan)
        for n in (5,20):
            f[f'{field}_avg{n}']=x[field].rolling(n,min_periods=n).mean()
        f[f'{field}_avg5_vs_prev20']=f[f'{field}_avg5']/prev20.replace(0,np.nan)
    is_up=x.close.diff().gt(0)
    f['up_day_amount_share20']=(x.amount.where(is_up,0).rolling(20,min_periods=20).sum()/
                                x.amount.rolling(20,min_periods=20).sum().replace(0,np.nan))
    f['up_day_volume_share20']=(x.volume.where(is_up,0).rolling(20,min_periods=20).sum()/
                                x.volume.rolling(20,min_periods=20).sum().replace(0,np.nan))
    signed=np.sign(x.close.diff().fillna(0))
    f['obv_change20_proxy']=(signed*x.volume).rolling(20,min_periods=20).sum()
    f['close_return5_pct']=_pct(x.close,5)
    f['close_return20_pct']=_pct(x.close,20)
    f['range20_pct']=100*(x.high.rolling(20,min_periods=20).max()/x.low.rolling(20,min_periods=20).min()-1)
    f['ma_above_price_count']=pd.concat([(x.close>f[f'ma{n}']).astype(float) for n in periods],axis=1).sum(axis=1)
    f['ma_positive_20d_slope_count']=pd.concat([(f[f'ma{n}_slope20_pct']>0).astype(float) for n in periods],axis=1).sum(axis=1)
    return pd.concat([x.drop(columns=['idx'],errors='ignore'),pd.DataFrame(f,index=x.index)],axis=1)


def future_labels(features:pd.DataFrame, *,horizon=FWD_BARS,surge_pct=10.,hold_pct=5.,control_max_pct=5.) -> pd.DataFrame:
    """FUTURE-ONLY outcome table, to be JOINED only for research grouping, never feature selection."""
    if horizon<1 or surge_pct<=0:raise ValueError('Invalid horizon or threshold')
    x=features[['idx','date','close','high']].copy()
    future_peak=np.full(len(x),np.nan)
    future_last=np.full(len(x),np.nan)
    peak_date=['']*len(x)
    for t in range(len(x)-horizon):
        future=x.iloc[t+1:t+1+horizon]
        k=future.high.idxmax()
        future_peak[t]=float(future.loc[k,'high'])
        future_last[t]=float(x.iloc[t+horizon].close)
        peak_date[t]=str(future.loc[k,'date'].date())
    out=x[['idx','date']].copy()
    out['forward20_peak_pct']=100*(future_peak/x.close-1)
    out['forward20_close_pct']=100*(future_last/x.close-1)
    out['future_peak_date']=peak_date
    out['is_sustained_surge']=((out.forward20_peak_pct>=surge_pct)&(out.forward20_close_pct>=hold_pct))
    out['is_nonrise_control']=((out.forward20_peak_pct<=control_max_pct)&(out.forward20_close_pct<=0))
    out.loc[out.forward20_peak_pct.isna(),['is_sustained_surge','is_nonrise_control']]=False
    return out


def choose_cases(features:pd.DataFrame, outcomes:pd.DataFrame, *,study_start=529,cooldown=20,max_cases=12) -> pd.DataFrame:
    """Deterministic de-overlap after all forward labels exist; nothing ranked by gain."""
    d=features[['idx','date','close','ma224']].merge(outcomes,on=['idx','date'],how='inner',validate='one_to_one')
    d=d[(d.idx>=study_start)&d.ma224.notna()&(d.close<d.ma224)].copy()
    chosen=[]
    for lab,condition in [('SURGE',d.is_sustained_surge),('CONTROL',d.is_nonrise_control)]:
        prev=-10**9
        candidates=d.loc[condition].sort_values('idx')
        for _,r in candidates.iterrows():
            t=int(r.idx)
            if t-prev<cooldown: continue
            chosen.append({'label':lab,'idx':t,'asof':str(r.date.date()),
                           'close_asof':float(r.close),'forward20_peak_pct':float(r.forward20_peak_pct),
                           'forward20_close_pct':float(r.forward20_close_pct),
                           'future_peak_date':r.future_peak_date})
            prev=t
            if sum(z['label']==lab for z in chosen)>=max_cases:break
    return pd.DataFrame(chosen)


def append_reference_case(cases:pd.DataFrame, features:pd.DataFrame, outcomes:pd.DataFrame, asof:str) -> pd.DataFrame:
    """Append a predeclared reference observation, independent of case sampling.

    It never reclassifies the SURGE/CONTROL samples; label REFERENCE is excluded
    from per-cohort descriptive statistics and cannot be a trained signal.
    """
    mask=features.date.eq(pd.Timestamp(asof))
    if int(mask.sum())!=1: raise ValueError('reference observation must exist once')
    t=int(features.loc[mask,'idx'].iloc[0])
    z=outcomes[outcomes.idx.eq(t)]
    if len(z)!=1 or pd.isna(z.iloc[0].forward20_peak_pct):
        raise ValueError('reference observation requires completed future20 outcomes')
    outcome=z.iloc[0]
    state=('SURGE' if bool(outcome.is_sustained_surge) else
           'CONTROL' if bool(outcome.is_nonrise_control) else 'OTHER')
    row={'label':'REFERENCE','idx':t,'asof':asof,
         'close_asof':float(features.iloc[t].close),
         'forward20_peak_pct':float(outcome.forward20_peak_pct),
         'forward20_close_pct':float(outcome.forward20_close_pct),
         'future_peak_date':str(outcome.future_peak_date),
         'reference_outcome_class':state,'selection_reason':'PREDECLARED_R1D100_REFERENCE'}
    ans=cases.copy()
    if not ans.empty:
        ans['selection_reason']='CHRONOLOGICAL_NONOVERLAP'
        ans['reference_outcome_class']=''
    return pd.concat([ans,pd.DataFrame([row])],ignore_index=True)


def event_snapshots(features:pd.DataFrame,cases:pd.DataFrame,*,back=(-20,-10,-5,-1,0)) -> pd.DataFrame:
    if cases.empty:return pd.DataFrame()
    rows=[]
    # IMPORTANT: Only pre-event/t0 observation features. Forward labels are kept outside.
    for c in cases.itertuples():
        for off in back:
            t=int(c.idx)+off
            if t<0:continue
            f=features.iloc[t]
            row={'label':c.label,'anchor_date':c.asof,'relative_bar':off,
                 'observation_date':str(f.date.date()),'idx':int(f.idx),
                 'close':float(f.close)}
            for key in features.columns:
                if key not in ('date','idx','code','open','high','low','close','volume','amount'):
                    v=f[key]
                    row[key]=None if pd.isna(v) else (bool(v) if isinstance(v,(np.bool_,bool)) else float(v))
            rows.append(row)
    return pd.DataFrame(rows)


def descriptive_compare(cases:pd.DataFrame,snapshots:pd.DataFrame) -> pd.DataFrame:
    if snapshots.empty:return pd.DataFrame()
    x=snapshots.loc[snapshots.relative_bar==0].copy()
    exclude={'label','anchor_date','relative_bar','observation_date','idx'}
    out=[]
    for c in (q for q in x.columns if q not in exclude and pd.api.types.is_numeric_dtype(x[q]) and not pd.api.types.is_bool_dtype(x[q])):
        a=x.loc[x.label=='SURGE',c].dropna();b=x.loc[x.label=='CONTROL',c].dropna()
        if len(a) and len(b):
            out.append({'feature':c,'surge_n':len(a),'control_n':len(b),
                        'surge_median':float(a.median()),'control_median':float(b.median()),
                        'median_diff':float(a.median()-b.median())})
    return pd.DataFrame(out)
