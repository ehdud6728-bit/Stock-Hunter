"""PIT-only evolving high/low trendline and 112/224/448 MA diagnostics.

This module is research-only. It never calls LIVE pipelines or selects buy orders.
Price basis is the historical raw input supplied by the frozen R1-D97 dataset.
Upper anchors are confirmed local CLOSE peaks (R1-D96 convention),
lower anchors are confirmed local LOW troughs. Separate high/low series are
needed to recognize range expansion, compression, and evolving trend.
"""
from __future__ import annotations
from typing import Dict, List, Optional
import numpy as np
import pandas as pd

MA_WINDOWS = (112, 224, 448)
PIVOT_RIGHT = 2
PIVOT_LEFT = 2
FLAT_PCT = 0.6              # diagnostic tolerance only: not fit to returns
MIN_GAP = 4
MAX_GAP = 100
SHORT_MAX_GAP = 24
SHORT_MAX_AGE = 40
MEDIUM_MIN_GAP = 20
MEDIUM_MAX_AGE = 65


def index_at(d: pd.DataFrame, asof: str) -> int:
    dates = pd.to_datetime(d['date'])
    if not dates.is_monotonic_increasing or dates.duplicated().any():
        raise ValueError('dates must be unique and sorted ascending')
    z = np.flatnonzero(dates.eq(pd.Timestamp(asof)))
    if len(z) != 1:
        raise ValueError(f'asof missing or duplicated: {asof}')
    return int(z[0])


def pivots(d: pd.DataFrame, t: int, kind: str, *, radius: int = 2) -> List[dict]:
    """A pivot at index j can be used only after close of j+radius."""
    if kind not in ('HIGH', 'LOW'):
        raise ValueError('kind must be HIGH or LOW')
    v = d['close' if kind == 'HIGH' else 'low'].to_numpy(dtype=float)
    records = []
    for j in range(radius, t-radius+1):
        a = v[j-radius:j+radius+1]
        if not (np.isfinite(v[j]) and len(a) == 2*radius+1 and np.isfinite(a).all()):
            continue
        valid = (v[j] == np.max(a) if kind == 'HIGH' else v[j] == np.min(a))
        if not valid or np.sum(a == v[j]) != 1:
            continue
        records.append({'kind':kind,'idx':j, 'date':str(d.iloc[j].date.date()),
                        'value':float(v[j]),'confirm_idx':j+radius,
                        'confirm_date':str(d.iloc[j+radius].date.date())})
    return records


def _pair(p: List[dict], t: int, tier: str) -> Optional[dict]:
    """Closest newly CONFIRMED second pivot, then nearest valid earlier pivot.

    No slope-direction filter: a new ascending resistance or descending support
    must remain discoverable to identify structure change.
    """
    if tier == 'SHORT':
        lo,hi,age = MIN_GAP,SHORT_MAX_GAP,SHORT_MAX_AGE
    elif tier == 'MEDIUM':
        lo,hi,age = MEDIUM_MIN_GAP,MAX_GAP,MEDIUM_MAX_AGE
    else:
        raise ValueError(tier)
    for b in reversed(p):
        if t - b['idx'] > age:
            break
        for a in reversed(p):
            gap = b['idx'] - a['idx']
            if lo <= gap <= hi:
                delta = 100*(b['value']/a['value']-1)
                if delta > FLAT_PCT: trend='UP'
                elif delta < -FLAT_PCT: trend='DOWN'
                else: trend='FLAT'
                x = b['value'] + (b['value']-a['value'])*(t-b['idx'])/gap
                if x<=0:
                    continue
                return {'kind':b['kind'],'tier':tier,
                        'h1':a['date'],'h2':b['date'],
                        'h1_idx':a['idx'],'h2_idx':b['idx'],
                        'h1_value':a['value'],'h2_value':b['value'],
                        'h2_confirm_date':b['confirm_date'],
                        'h2_confirm_idx':b['confirm_idx'],
                        'line_id':f'{b["kind"]}|{tier}|{a["date"]}|{b["date"]}',
                        'pivot_progress_pct':round(delta,3),
                        'slope_pct_per_day':round(100*(b['value']-a['value'])/gap/a['value'],4),
                        'direction':trend,'gap_bars':gap,'age_bars':t-b['idx'],
                        'line_asof':round(float(x),3)}
    return None


def structure(d: pd.DataFrame, asof: str) -> dict:
    t = index_at(d,asof)
    rows=[]
    for typ in ('HIGH','LOW'):
        p = pivots(d,t,typ)
        for tier in ('SHORT','MEDIUM'):
            item = _pair(p,t,tier)
            if item is not None:
                item['asof']=asof
                item['close_asof']=float(d.iloc[t].close)
                item['distance_close_to_line_pct']=round(100*(item['close_asof']/item['line_asof']-1),3)
                rows.append(item)
    short={r['kind']:r for r in rows if r['tier']=='SHORT'}
    if 'HIGH' not in short or 'LOW' not in short:
        condition='INSUFFICIENT_CONFIRMED_PIVOTS'
    else:
        hi=short['HIGH']['direction']; lo=short['LOW']['direction']
        if hi=='UP' and lo=='UP': condition='HIGHER_HIGHS_HIGHER_LOWS'
        elif hi=='DOWN' and lo=='DOWN': condition='LOWER_HIGHS_LOWER_LOWS'
        elif hi=='DOWN' and lo=='UP': condition='CONVERGING_RANGE'
        elif hi=='UP' and lo=='DOWN': condition='EXPANDING_RANGE'
        else: condition='MIXED_OR_FLAT'
    return {'asof':asof,'idx':t,'lines':rows,'structure':condition}


def _ma(d: pd.DataFrame, t: int, n: int) -> Optional[float]:
    if t<n-1:return None
    vals=d['close'].iloc[t-n+1:t+1].to_numpy(dtype=float)
    return float(np.mean(vals)) if np.isfinite(vals).all() else None


def ma_report(d: pd.DataFrame, asof: str) -> dict:
    t=index_at(d,asof)
    close=float(d.iloc[t].close)
    if close<=0:raise ValueError('close must be positive')
    out={'asof':asof,'close':close,'idx':t}
    for n in MA_WINDOWS:
        cur,back5,back20,back40 = (_ma(d,t-k,n) for k in (0,5,20,40))
        out[f'MA{n}']=round(cur,3) if cur is not None else None
        out[f'MA{n}_slope5_pct']=(round(100*(cur/back5-1),4)
            if cur and back5 else None)
        out[f'MA{n}_slope20_pct']=(round(100*(cur/back20-1),4)
            if cur and back20 else None)
        out[f'MA{n}_slope_acceleration20_pp']=(round(100*(cur/back20-back20/back40),4)
            if cur and back20 and back40 else None)
        out[f'close_to_MA{n}_pct']=(round(100*(close/cur-1),4) if cur else None)
    for a,b in ((112,224),(224,448)):
        x,y = _ma(d,t,a),_ma(d,t,b)
        p,q = _ma(d,t-20,a),_ma(d,t-20,b)
        out[f'MA{a}_to_MA{b}_spread_pct']=(round(100*(x/y-1),4) if x and y else None)
        out[f'MA{a}_to_MA{b}_spread_change20_pp']=(round(100*(x/y-p/q),4)
            if x and y and p and q else None)
    ma_short,ma_mid,ma_long = (out[f'MA{n}'] for n in MA_WINDOWS)
    if None in (ma_short,ma_mid,ma_long):order='MISSING_MA'
    elif ma_short<ma_mid<ma_long:order='FULL_BEARISH_STACK'
    elif ma_short>ma_mid>ma_long:order='FULL_BULLISH_STACK'
    else:order='MIXED_STACK'
    out['ma_order']=order
    # Diagnostic states; not trade signals.
    slopes=[out[f'MA{n}_slope20_pct'] for n in MA_WINDOWS]
    out['all_ma_slopes_up']=all(x is not None and x>0 for x in slopes)
    out['all_ma_slopes_down']=all(x is not None and x<0 for x in slopes)
    return out


def timeline(d: pd.DataFrame, asof: str, *, lookback: int = 35) -> List[dict]:
    """Recompute each day, and emit re-anchor when the confirmed pivot changes.

    This is genuinely new daily selection; not redrawing a frozen line through
    future prices. Retained old lines should be handled by R1-D99 catalog.
    """
    t=index_at(d,asof)
    start=max(2*PIVOT_RIGHT+MIN_GAP, t-lookback)
    result=[]
    prev={}
    for k in range(start,t+1):
        date=str(d.iloc[k].date.date())
        current=structure(d,date)
        for line in current['lines']:
            key=(line['kind'],line['tier']);last=prev.get(key)
            if last is None or last['line_id']!=line['line_id']:
                reason=('INITIAL_IN_WINDOW' if last is None else
                        'NEW_CONFIRMED_SECOND_PIVOT' if line['h2_idx']!=last['h2_idx'] else
                        'RESELECTED_OLDER_ANCHOR')
                result.append({'asof':date,'kind':line['kind'],'tier':line['tier'],
                               'line_id':line['line_id'],'h1':line['h1'],'h2':line['h2'],
                               'h2_confirm_date':line['h2_confirm_date'],
                               'direction':line['direction'], 'reason':reason,
                               'previous_line_id':last['line_id'] if last else ''})
            prev[key]=line
    return result


def compare_asofs(d: pd.DataFrame, dates: List[str]) -> dict:
    """A predeclared after-date is a separate ex-post snapshot, never a signal input."""
    return {'ma':[ma_report(d,day) for day in dates],
            'structure':[structure(d,day) for day in dates]}
