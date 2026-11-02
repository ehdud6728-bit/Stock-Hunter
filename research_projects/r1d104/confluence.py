"""R1-D104 unified PIT chart context. Descriptive only; no buy/sell or trained weights.

A chart observation is a STOCK-DATE, never a count of independent MA triggers.
The past-confirmed pivot lines are reused from frozen R1-D100. A line's change
is NOT a breakout if the previous close was before its confirmation.
"""
from __future__ import annotations
import math
import numpy as np
import pandas as pd

MA_KEYS=(3,5,10,20,30,40,60,112,120,224,240,448,480)
BB_KEYS=(20,30,40)
ENV_KEYS=(20,40)
CLOUD_KEYS=('ma_cloud_fast','ma_cloud_mid','ma_cloud_long','ma_cloud_all','ichimoku_current')
WATCH_MA_KEYS=(5,20,60,112,224)

def _float(x):
    return float(x) if pd.notna(x) else None

def _pct(x, base):
    return (100*(x/base-1) if x is not None and base is not None and base>0 else None)

def _position(price, lo, hi):
    if lo is None or hi is None:return 'NA'
    if price>hi:return 'ABOVE_UPPER'
    if price<lo:return 'BELOW_LOWER'
    return 'INSIDE'

def _direction(x):
    return 'NA' if x is None else ('UP' if x>0 else 'DOWN' if x<0 else 'FLAT')

def _width(x):
    return 'NA' if x is None else ('EXPAND' if x>0 else 'CONTRACT' if x<0 else 'FLAT')

def _as_row(fr, t, line_snapshot, *, old_snapshot=None):
    q=fr.iloc[t];date=str(q.date.date());price=float(q.close)
    out={'idx':int(t),'date':date,'close':price,
         'structure':line_snapshot['structure'], 'volume_vs_prev20':_float(q.volume_vs_prev20),
         'amount_vs_prev20':_float(q.amount_vs_prev20),
         'volume_avg5_vs_prev20':_float(q.volume_avg5_vs_prev20),
         'amount_avg5_vs_prev20':_float(q.amount_avg5_vs_prev20),
         'up_day_amount_share20':_float(q.up_day_amount_share20),
         'obv_change20_proxy':_float(q.obv_change20_proxy),
         'close_return5_pct':_float(q.close_return5_pct),
         'close_return20_pct':_float(q.close_return20_pct)}
    out['flow_today']=('NA' if out['amount_vs_prev20'] is None else
                       'ABOVE_AVG' if out['amount_vs_prev20']>=1 else 'BELOW_AVG')
    out['flow_5d']=('NA' if out['amount_avg5_vs_prev20'] is None else
                       'ABOVE_AVG' if out['amount_avg5_vs_prev20']>=1 else 'BELOW_AVG')
    for n in MA_KEYS:
        out[f'ma{n}_value']=_float(q[f'ma{n}'])
        out[f'ma{n}_gap_pct']=_float(q[f'ma{n}_price_gap_pct'])
        out[f'ma{n}_slope5_pct']=_float(q[f'ma{n}_slope5_pct'])
        out[f'ma{n}_slope20_pct']=_float(q[f'ma{n}_slope20_pct'])
        out[f'ma{n}_slope_accel5_pp']=_float(q[f'ma{n}_slope20_acceleration5_pp'])
    for n in BB_KEYS:
        prefix=f'bb{n}'
        lo,hi=_float(q[f'{prefix}_lower']),_float(q[f'{prefix}_upper'])
        out[f'{prefix}_upper']=hi;out[f'{prefix}_lower']=lo
        out[f'{prefix}_width_pct']=_float(q[f'{prefix}_width_pct'])
        out[f'{prefix}_width_delta5_pp']=_float(q[f'{prefix}_width_change5_pp'])
        out[f'{prefix}_width_delta20_pp']=_float(q[f'{prefix}_width_change20_pp'])
        out[f'{prefix}_upper_slope5_pct']=_float(q[f'{prefix}_upper_slope5_pct'])
        out[f'{prefix}_lower_slope5_pct']=_float(q[f'{prefix}_lower_slope5_pct'])
        out[f'{prefix}_position']=_position(price,lo,hi)
        out[f'{prefix}_upper_distance_pct']=_pct(price,hi)
        out[f'{prefix}_lower_distance_pct']=_pct(price,lo)
    for n in ENV_KEYS:
        prefix=f'env{n}_10';lo,hi=_float(q[f'{prefix}_lower']),_float(q[f'{prefix}_upper'])
        out[f'{prefix}_upper']=hi;out[f'{prefix}_lower']=lo
        out[f'{prefix}_width_abs']=_float(q[f'{prefix}_width_abs'])
        out[f'{prefix}_width_change5_pct']=_float(q[f'{prefix}_width_abs_change5_pct'])
        out[f'{prefix}_upper_slope5_pct']=_float(q[f'{prefix}_upper_slope5_pct'])
        out[f'{prefix}_lower_slope5_pct']=_float(q[f'{prefix}_lower_slope5_pct'])
        out[f'{prefix}_position']=_position(price,lo,hi)
        out[f'{prefix}_upper_distance_pct']=_pct(price,hi)
        out[f'{prefix}_lower_distance_pct']=_pct(price,lo)
    for k in CLOUD_KEYS:
        for field in ('upper','lower','thickness_pct','height_vs_price_pct',
                      'thickness_change5_pp','height_change5_pp',
                      'upper_slope5_pct','lower_slope5_pct','position'):
            val=q[f'{k}_{field}']
            out[f'{k}_{field}']=(val if isinstance(val,str) else _float(val))
    out['bb20_width_state']=_width(out['bb20_width_delta5_pp'])
    out['fast_cloud_width_state']=_width(out['ma_cloud_fast_thickness_change5_pp'])
    out['ma20_slope_acceleration_state']=_direction(out['ma20_slope_accel5_pp'])
    out['ma112_slope_acceleration_state']=_direction(out['ma112_slope_accel5_pp'])
    out['price_ma224_zone']='ABOVE' if price>=out['ma224_value'] else 'BELOW'
    out['five_main_ma_above_count']=sum(price>out[f'ma{n}_value'] for n in WATCH_MA_KEYS)
    # Keep line IDs and anchors: preserve distinct old lines; never overwrite catalog.
    lookup={(x['kind'],x['tier']):x for x in line_snapshot['lines']}
    old_lookup={(x['kind'],x['tier']):x for x in (old_snapshot or {}).get('lines',[])}
    for kind in ('HIGH','LOW'):
        for tier in ('SHORT','MEDIUM'):
            prefix=f'{tier.lower()}_{kind.lower()}_trend'
            line=lookup.get((kind,tier));old=old_lookup.get((kind,tier))
            out[prefix+'_line_id']=line['line_id'] if line else ''
            out[prefix+'_anchor1']=line['h1'] if line else ''
            out[prefix+'_anchor2']=line['h2'] if line else ''
            out[prefix+'_confirm_date']=line['h2_confirm_date'] if line else ''
            out[prefix+'_direction']=line['direction'] if line else 'NA'
            out[prefix+'_slope_pct_per_day']=line['slope_pct_per_day'] if line else None
            out[prefix+'_level']=line['line_asof'] if line else None
            out[prefix+'_price_distance_pct']=_pct(price,line['line_asof']) if line else None
            out[prefix+'_reanchored_today']=bool(line and (old is None or old['line_id']!=line['line_id']))
            out[prefix+'_validated_cross']='NA'
            if line is not None and old is not None and old['line_id']==line['line_id']:
                # Only a line known PREVIOUS bar can be treated as a price crossing.
                old_close=float(fr.iloc[t-1].close);old_level=old['line_asof'];new_level=line['line_asof']
                if old_close<=old_level and price>new_level:out[prefix+'_validated_cross']='CROSS_UP'
                elif old_close>=old_level and price<new_level:out[prefix+'_validated_cross']='CROSS_DOWN'
                else:out[prefix+'_validated_cross']='NO_CROSS'
    out['trendline_signal']=('HIGH_CROSS_UP' if out['short_high_trend_validated_cross']=='CROSS_UP' else
        'LOW_CROSS_DOWN' if out['short_low_trend_validated_cross']=='CROSS_DOWN' else
        'NO_NEW_CROSS_OR_REANCHOR')
    out['ma_bb_env_alignment']='BB_ENV_BOTH_ABOVE'  if (out['bb20_position']=='ABOVE_UPPER' and out['env20_10_position']=='ABOVE_UPPER') else (
        'BB_ENV_BOTH_BELOW' if out['bb20_position']=='BELOW_LOWER' and out['env20_10_position']=='BELOW_LOWER' else 'MIXED')
    return out

def integrate_dates(fr:pd.DataFrame, *,start:int=540,horizon:int=20,
                    structure_func=None, max_rows=None)->pd.DataFrame:
    """Enumerate every eligible date BEFORE future labels are attached.

    All recorded variables are functions of price/volume history ending at t.
    """
    if structure_func is None:
        from dynamic_structure import structure as structure_func
    if start<2 or horizon<1:raise ValueError('invalid start/horizon')
    dates=pd.to_datetime(fr.date)
    if dates.duplicated().any() or not dates.is_monotonic_increasing:raise ValueError('unsorted dates')
    last=len(fr)-horizon
    rows=[];prior=None
    iterator=range(start,last)
    if max_rows is not None:iterator=range(start,min(last,start+max_rows))
    # IMPORTANT: compute the prior-day structure even for the first selected date
    for t in iterator:
        date=str(fr.iloc[t].date.date())
        snap=structure_func(fr,date)
        prior=structure_func(fr,str(fr.iloc[t-1].date.date()))
        rows.append(_as_row(fr,t,snap,old_snapshot=prior))
    return pd.DataFrame(rows)

def attach_expost(context,labels):
    """Outcomes are appended ONLY AFTER all PIT features and cases exist."""
    if context.empty:return context.copy()
    if context.idx.duplicated().any():raise ValueError('duplicate dates')
    selected=labels[['idx','forward20_close_pct','forward20_peak_pct','is_sustained_surge','is_nonrise_control']].copy()
    if selected.idx.duplicated().any():raise ValueError('duplicate outcomes')
    out=context.merge(selected,on='idx',validate='one_to_one')
    out.rename(columns={'forward20_close_pct':'forward20_close_pct_EXPOST',
                        'forward20_peak_pct':'forward20_peak_pct_EXPOST'},inplace=True)
    out['outcome_EXPOST']=np.select([out.is_sustained_surge,out.is_nonrise_control],['SURGE','CONTROL'],default='OTHER')
    return out

def events_for_dates(all_context, events):
    """Aggregate same-day MA events as ONE stock-day, preserving raw event names."""
    if events.empty:return all_context.copy().assign(ma_event_count=0,ma_event_types='',ma_periods='')
    if not {'idx','ma_period','event'}.issubset(events.columns):raise ValueError('event columns missing')
    grouped=events.groupby('idx',as_index=False).agg(
        ma_event_count=('event','size'),
        ma_event_types=('event',lambda x:'|'.join(sorted(set(x)))),
        ma_periods=('ma_period',lambda x:'|'.join(map(str,sorted(set(x))))))
    result=all_context.merge(grouped,on='idx',how='left',validate='one_to_one').assign(
        ma_event_count=lambda x:x.ma_event_count.fillna(0).astype(int),
        ma_event_types=lambda x:x.ma_event_types.fillna(''),
        ma_periods=lambda x:x.ma_periods.fillna(''))
    def classify_event(names):
        if not names:return 'NO_MA_EVENT'
        values=names.split('|')
        if 'RECLAIM_ABOVE' in values and 'LOSE_BELOW' in values:return 'MIXED_RECLAIM_LOSE'
        if 'RECLAIM_ABOVE' in values:return 'RECLAIM_PRESENT'
        if 'LOSE_BELOW' in values:return 'LOSE_PRESENT'
        if 'TOUCH_ABOVE_CANDIDATE' in values:return 'TOUCH_ABOVE_CANDIDATE'
        if 'TOUCH_BELOW_CANDIDATE' in values:return 'TOUCH_BELOW_CANDIDATE'
        return 'OTHER_MA_EVENT'
    result['ma_event_category']=result.ma_event_types.map(classify_event)
    return result

def compare_groups(df,*,keys=None):
    """No best-combination ranking; only n + positive outcomes per stable coarse group."""
    keys=keys or ('structure','bb20_width_state','env20_10_position','flow_today')
    rows=[]
    for signature,g in df.groupby(list(keys),dropna=False):
        returns=g.forward20_close_pct_EXPOST.dropna()
        rows.append(dict(zip(keys,signature))|{
            'stock_dates':len(g),'different_dates':g.date.nunique(),
            'n_surge':int((g.outcome_EXPOST=='SURGE').sum()),
            'n_control':int((g.outcome_EXPOST=='CONTROL').sum()),
            'n_other':int((g.outcome_EXPOST=='OTHER').sum()),
            'positive20_rate':float((returns>0).mean()) if len(returns) else None,
            'median20_pct':float(returns.median()) if len(returns) else None,
            'CAUTION':'DESCRIPTIVE_ONLY_CORRELATED_THREE_STOCKS'})
    return pd.DataFrame(rows)
