"""R1-D103: PIT end-of-day moving-average interaction research, NOT trade signals.

All event features use the bar t and earlier only. Future D+5/20 price is joined
in a separate ex-post outcome function. A touch is a *candidate* support/resistance
interaction, never proof that the MA actually caused the later price reaction.
"""
from __future__ import annotations
import numpy as np
import pandas as pd

PRIMARY_WINDOWS=(5,20,60,112,224)
ALL_WINDOWS=(3,5,10,20,30,40,60,112,120,224,240,448,480)
FUTURE_BARS=20
TOUCH_EPS=.005
COOLDOWN=20


def validate_features(features: pd.DataFrame) -> None:
    req={'date','idx','close','high','low','amount','volume',
         'bb20_width_change5_pp','amount_vs_prev20',
         'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct',
         'ichimoku_current_thickness_pct'}
    req.update(f'ma{n}' for n in ALL_WINDOWS)
    if not req.issubset(features.columns):
        raise ValueError('Missing feature columns: '+str(sorted(req-set(features.columns))))
    dates=pd.to_datetime(features.date)
    if dates.isna().any() or dates.duplicated().any() or not dates.is_monotonic_increasing:
        raise ValueError('date must be increasing, unique and complete')
    if not np.array_equal(features.idx.to_numpy(),np.arange(len(features))):
        raise ValueError('idx must match chronological row index')


def classify_regime(d: pd.DataFrame, t: int) -> dict:
    """Predeclared coarse contexts; thresholds are not fit to ex-post returns."""
    q=d.iloc[t]
    periods=(5,20,60,112,224)
    vals=[float(q[f'ma{n}']) for n in periods]
    if not all(np.isfinite(vals)):
        stack='NA'
    elif all(a>b for a,b in zip(vals,vals[1:])):
        stack='BULL_STACK_5_TO_224'
    elif all(a<b for a,b in zip(vals,vals[1:])):
        stack='BEAR_STACK_5_TO_224'
    else:stack='MIXED_STACK'
    a=q.get('ma112_slope20_pct',np.nan)
    b=q.get('ma224_slope20_pct',np.nan)
    if pd.isna(a) or pd.isna(b): long_trend='NA'
    elif a>0 and b>0:long_trend='BOTH_RISING'
    elif a<0 and b<0:long_trend='BOTH_FALLING'
    else:long_trend='DIVERGING_OR_FLAT'
    width=q['bb20_width_change5_pp']
    bb='NA' if pd.isna(width) else ('EXPANDING' if width>0 else 'CONTRACTING' if width<0 else 'FLAT')
    flow=q['amount_vs_prev20']
    flow_state='NA' if pd.isna(flow) else ('ABOVE_PRIOR_AVG' if flow>=1 else 'BELOW_PRIOR_AVG')
    return {'ma_stack_context':stack,'long_slope_context':long_trend,
            'bb20_width_context':bb,'amount_context':flow_state,
            'ma224_zone':'ABOVE' if float(q.close)>=float(q.ma224) else 'BELOW',
            'ma_cloud_fast_thickness_pct':float(q.ma_cloud_fast_thickness_pct),
            'ma_cloud_mid_thickness_pct':float(q.ma_cloud_mid_thickness_pct),
            'ichimoku_current_thickness_pct':float(q.ichimoku_current_thickness_pct),
            'bb20_width_pct':float(q.bb20_width_pct),
            'amount_vs_prev20':float(q.amount_vs_prev20),
            'up_day_amount_share20':float(q.up_day_amount_share20)}


def classify_interaction(d: pd.DataFrame,t:int,n:int,eps:float=TOUCH_EPS)->str|None:
    """Close-of-day analysis only; moving averages include today's close."""
    if t<1 or n not in ALL_WINDOWS or not (0<=eps<.1):raise ValueError('bad t/n/eps')
    q,p=d.iloc[t],d.iloc[t-1]
    v,prev=float(q[f'ma{n}']),float(p[f'ma{n}'])
    if not (np.isfinite(v) and np.isfinite(prev) and v>0 and prev>0):return None
    c,pc=float(q.close),float(p.close)
    if pc<=prev and c>v:return 'RECLAIM_ABOVE'
    if pc>=prev and c<v:return 'LOSE_BELOW'
    contacted=(float(q.low)<=v*(1+eps) and float(q.high)>=v*(1-eps))
    if not contacted:return None
    if pc>prev and c>=v:return 'TOUCH_ABOVE_CANDIDATE'
    if pc<prev and c<=v:return 'TOUCH_BELOW_CANDIDATE'
    return None


def inventory_events(features:pd.DataFrame, *,windows=PRIMARY_WINDOWS,
                     start_idx:int|None=None,horizon:int=FUTURE_BARS)->pd.DataFrame:
    """Every eligible MA interaction, regardless of future outcome or MA224 position."""
    validate_features(features)
    if not windows or not set(windows).issubset(ALL_WINDOWS):raise ValueError('unknown MA windows')
    if horizon<1:raise ValueError('horizon invalid')
    start=max(540,int(start_idx) if start_idx is not None else 540)
    rows=[]
    for t in range(start,len(features)-horizon):
        q=features.iloc[t]
        context=classify_regime(features,t)
        for n in windows:
            ev=classify_interaction(features,t,n)
            if ev is None:continue
            ma=float(q[f'ma{n}']);prev_ma=float(features.iloc[t-1][f'ma{n}'])
            slope5=float(q[f'ma{n}_slope5_pct'])
            slope20=float(q[f'ma{n}_slope20_pct'])
            accel=float(q[f'ma{n}_slope20_acceleration5_pp'])
            rows.append({'idx':t,'asof':str(q.date.date()),'ma_period':int(n),'event':ev,
                'close':float(q.close),'ma_value':ma,
                'close_ma_gap_pct':100*(float(q.close)/ma-1),
                'ma_slope5_pct':slope5,'ma_slope20_pct':slope20,
                'ma_slope_accel5_pp':accel,'ma_price_gap_change5_pp':float(q[f'ma{n}_price_gap_change5_pp']),
                'ma_price_direction_today':'UP' if ma>prev_ma else 'DOWN' if ma<prev_ma else 'FLAT',
                **context})
    return pd.DataFrame(rows)


def attach_outcomes(events:pd.DataFrame,labels:pd.DataFrame,features:pd.DataFrame)->pd.DataFrame:
    """Ex-post join. No future labels can modify event existence or attributes."""
    if events.empty:return events.copy()
    if events[['idx','ma_period','event']].duplicated().any():raise ValueError('duplicate MA events')
    if labels.idx.duplicated().any():raise ValueError('duplicate outcome dates')
    future_cols=['idx','forward20_peak_pct','forward20_close_pct','is_sustained_surge','is_nonrise_control']
    out=events.merge(labels[future_cols],on='idx',how='left',validate='many_to_one')
    x=features.reset_index(drop=True)
    f5=np.full(len(x),np.nan)
    for i in range(len(x)-5):f5[i]=100*(float(x.iloc[i+5].close)/float(x.iloc[i].close)-1)
    out['forward5_close_pct_EXPOST']=out.idx.map(dict(enumerate(f5)))
    out['outcome_EXPOST']=np.select([out.is_sustained_surge.fillna(False),out.is_nonrise_control.fillna(False)],['SURGE','CONTROL'],default='OTHER')
    return out


def cooldown_events(events:pd.DataFrame,*,cooldown:int=COOLDOWN)->pd.DataFrame:
    """Chronological, per MA/event cooldown; independent of future success."""
    if cooldown<1:raise ValueError('cooldown must be positive')
    if events.empty:return events.copy()
    selected=[]
    for _,g in events.sort_values('idx').groupby(['ma_period','event'],sort=True):
        last=-10**10
        for row in g.to_dict('records'):
            if int(row['idx'])-last>=cooldown:
                selected.append(row);last=int(row['idx'])
    return pd.DataFrame(selected).sort_values(['idx','ma_period','event']).reset_index(drop=True)


def summary_tables(sample:pd.DataFrame,*,keys=('ma_period','event','long_slope_context')):
    """Descriptive denominators with distinct DATE counts. No automatic best-MA claims."""
    if sample.empty:return pd.DataFrame()
    def summarize(group):
        p=group.forward20_close_pct.dropna()
        return pd.Series({
            'n_events':len(group),'n_unique_days':group['asof'].nunique(),
            'n_surge':int((group.outcome_EXPOST=='SURGE').sum()),
            'n_control':int((group.outcome_EXPOST=='CONTROL').sum()),
            'n_other':int((group.outcome_EXPOST=='OTHER').sum()),
            'positive_d20_rate':float((p>0).mean()) if len(p) else np.nan,
            'median_d5_pct':float(group.forward5_close_pct_EXPOST.median()),
            'median_d20_pct':float(p.median()) if len(p) else np.nan,
            'research_sample_warning':'SMALL_OR_CORRELATED_SAMPLE',
        })
    # Apply by groups; future labels used for comparison only.
    return sample.groupby(list(keys),dropna=False).apply(summarize,include_groups=False).reset_index()


def before_event(features:pd.DataFrame,cases:pd.DataFrame,*,offsets=(-20,-10,-5,-1,0))->pd.DataFrame:
    """Only contemporaneously observable features at each pre-event point."""
    cols=['date','close','amount','volume','bb20_width_pct','bb20_width_change5_pp',
          'env20_10_width_abs','amount_vs_prev20','volume_vs_prev20',
          'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct','ma_cloud_long_thickness_pct',
          'ichimoku_current_height_vs_price_pct','ichimoku_current_thickness_pct',
          'up_day_amount_share20']
    for n in ALL_WINDOWS:
        cols += [f'ma{n}',f'ma{n}_slope5_pct',f'ma{n}_slope20_pct',
                 f'ma{n}_slope20_acceleration5_pp',f'ma{n}_price_gap_pct']
    rows=[]
    for e in cases.itertuples():
        for offset in offsets:
            t=int(e.idx)+offset
            if t<0:raise ValueError('event predates required history')
            d=features.iloc[t]
            record={'anchor_date':e.asof,'ma_period':int(e.ma_period),'event':e.event,
                    'relative_bar':int(offset),'observation_date':str(d.date.date()),'idx':t}
            for k in cols:
                if k=='date':continue
                val=d[k]
                record[k]=float(val) if pd.notna(val) else None
            rows.append(record)
    return pd.DataFrame(rows)
