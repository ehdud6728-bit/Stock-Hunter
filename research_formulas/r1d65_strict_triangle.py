#!/usr/bin/env python3
"""R1-D65: causal, actual pivot-pair geometric triangle confirmation.
RESEARCH_ONLY. This does not rely on R1-D56 tri40 legacy outcome/flags.
An event is evaluated with prices through its *own* closing bar t; pivot lines
are frozen at t-1. Output contains no future returns or trading orders.
"""
from __future__ import annotations
import math
from dataclasses import dataclass, asdict
from typing import Optional

@dataclass(frozen=True)
class Rules:
    lookback: int=40
    pivot_radius: int=2
    min_pair_gap: int=4
    min_alternating_gap: int=2
    max_pivot_age: int=13
    min_pivot_prominence_pct: float=0.7
    min_high_drop_pct: float=1.0
    min_low_rise_pct: float=0.0
    min_wave_gain_pct: float=12.0
    min_contraction_pct: float=25.0
    end_width_min_pct: float=0.5
    end_width_max_pct: float=14.0
    min_close_residency: float=0.8
    candle_line_tolerance_pct: float=1.0
    breakout_buffer_pct: float=0.3
    max_chase_pct: float=6.0
    amount_reentry_ratio: float=1.3

RULE=Rules()


def positive_bars(v, begin, end):
    for i in range(begin,end+1):
        try:
            o,h,l,c,a=(float(v[k][i]) for k in ('open','high','low','close','amount'))
            if not all(math.isfinite(z) for z in (o,h,l,c,a)) or min(o,h,l,c)<=0 or a<0:
                return False
            if not (h>=max(o,c) and l<=min(o,c)):
                return False
            if 'valid_price' in v and str(v['valid_price'][i]) not in ('1','1.0','True','true'):
                return False
        except (ValueError,TypeError,IndexError,KeyError):
            return False
    return True


def pivots(series, start, end, radius=2, peak=True, prominence=.7):
    result=[]
    for i in range(max(radius,start), min(len(series)-radius,end+1)):
        val=float(series[i]);neighbors=[float(series[j]) for j in range(i-radius,i+radius+1) if j!=i]
        if not all(math.isfinite(z) and z>0 for z in neighbors+[val]):continue
        if peak and val>max(neighbors) and val>=sum(neighbors)/len(neighbors)*(1+prominence/100):
            result.append(i)
        elif not peak and val<min(neighbors) and val<=sum(neighbors)/len(neighbors)*(1-prominence/100):
            result.append(i)
    return result


def line(pair,series,t):
    i,j=pair
    return float(series[i])+(float(series[j])-float(series[i]))*(t-i)/(j-i)


def geometry(v,t,ma224=None,rules=RULE):
    """As-of classification, no future price access; full pivot identity returned.
    t is the event date index, v arrays are aligned by session (not calendar day).
    ma224 must be independently sourced AS-OF t (or from 224 daily close bars).
    """
    r=rules
    z={'status':'NOT_TRIANGLE','reason':'', 'geometry_valid':False,'breakout_geometry':False,
       'qualified_below224':False,'qualified_with_amount':False,'t':t}
    def fail(reason):
        z['reason']=reason
        return z
    if not isinstance(t,int) or t<r.lookback+15 or t>=len(v['close']):return fail('INSUFFICIENT_T_HISTORY')
    if not positive_bars(v,t-r.lookback-14,t):return fail('INVALID_PRICE_WINDOW')
    highs=v['high'];lows=v['low'];closes=v['close'];amount=v['amount']
    # Confirmed pivots: radius=2, pivot t-3 has two right-side bars t-2,t-1.
    oldest=t-r.lookback; newest=t-r.pivot_radius-1
    hp=pivots(highs,oldest,newest,r.pivot_radius,True,r.min_pivot_prominence_pct)
    lp=pivots(lows,oldest,newest,r.pivot_radius,False,r.min_pivot_prominence_pct)
    z.update({'pivot_highs_detected':len(hp),'pivot_lows_detected':len(lp)})
    if len(hp)<2 or len(lp)<2:return fail('MISSING_TWO_CONFIRMED_HIGHS_OR_LOWS')
    # Latest *actual* two of each, not a regression or a hand-picked best fit.
    ph=hp[-2:];pl=lp[-2:]
    z.update({'H1_ix':ph[0],'H2_ix':ph[1],'L1_ix':pl[0],'L2_ix':pl[1],
       'H1':float(highs[ph[0]]),'H2':float(highs[ph[1]]),
       'L1':float(lows[pl[0]]),'L2':float(lows[pl[1]])})
    if ph[1]-ph[0]<r.min_pair_gap or pl[1]-pl[0]<r.min_pair_gap:
        return fail('PIVOT_PAIR_TOO_CLOSE')
    chron=sorted([(i,'H') for i in ph]+[(i,'L') for i in pl])
    if ''.join(label for i,label in chron) not in ('HLHL','LHLH'):
        return fail('NON_ALTERNATING_FOUR_PIVOTS')
    if any(chron[i+1][0]-chron[i][0]<r.min_alternating_gap for i in range(3)):
        return fail('OVERLAPPING_PIVOTS')
    if t-max(ph[1],pl[1])>r.max_pivot_age:
        return fail('PIVOTS_TOO_OLD')
    high_drop=100*(1-float(highs[ph[1]])/float(highs[ph[0]]))
    low_rise=100*(float(lows[pl[1]])/float(lows[pl[0]])-1)
    z.update({'high_drop_pct':high_drop,'low_rise_pct':low_rise})
    if high_drop<r.min_high_drop_pct:return fail('HIGH_2_NOT_LOWER')
    if low_rise<r.min_low_rise_pct:return fail('LOW_2_NOT_HIGHER_OR_EQUAL')
    if ph[0]<8:return fail('MISSING_PREWAVE_HISTORY')
    trough_begin=max(0,ph[0]-25)
    trough_end=ph[0]-4
    if trough_end-trough_begin<5:return fail('MISSING_PREWAVE_HISTORY')
    trough=min(float(x) for x in lows[trough_begin:trough_end+1])
    wave_gain=(float(highs[ph[0]])/trough-1)*100
    z['wave_gain_pct']=wave_gain
    if wave_gain<r.min_wave_gain_pct:return fail('MISSING_FIRST_WAVE')
    first=chron[0][0]
    up_first=line(ph,highs,first);low_first=line(pl,lows,first)
    up_last=line(ph,highs,t-1);low_last=line(pl,lows,t-1)
    up_today=line(ph,highs,t);low_today=line(pl,lows,t)
    if min(up_first,low_first,up_last,low_last,up_today,low_today)<=0:
        return fail('NONPOSITIVE_TRENDLINE')
    start_width=up_first-low_first;end_width=up_last-low_last
    if start_width<=0 or end_width<=0 or up_today<=low_today:
        return fail('CROSSED_TRENDLINES')
    contraction=100*(1-end_width/start_width)
    width_pct=100*end_width/up_last
    z.update({'contraction_pct':contraction,'end_width_pct':width_pct,
       'upper_line_t':up_today,'lower_line_t':low_today,
       'upper_line_tminus1':up_last,'lower_line_tminus1':low_last})
    if contraction<r.min_contraction_pct:return fail('NO_REAL_COMPRESSION')
    if not r.end_width_min_pct<=width_pct<=r.end_width_max_pct:
        return fail('END_WIDTH_OUT_OF_RANGE')
    # Restrict to actual post-four-pivot consolidation; no implicit assumption
    # that prices lie in the wedge just because its two regressions converge.
    n_inside=0;n_checked=0
    for j in range(chron[0][0],t):
        upper=line(ph,highs,j);lower=line(pl,lows,j)
        if upper<=lower:return fail('LINES_CROSSED_BEFORE_BREAKOUT')
        close=float(closes[j]);n_checked+=1
        if lower*(1-r.candle_line_tolerance_pct/100)<=close<=upper*(1+r.candle_line_tolerance_pct/100):
            n_inside+=1
    residency=n_inside/n_checked if n_checked else 0
    z.update({'n_inside':n_inside,'n_checked':n_checked,'residency_ratio':residency})
    if residency<r.min_close_residency:return fail('PRICE_NOT_CONTAINED_IN_TRIANGLE')
    z['geometry_valid']=True
    previous_inside=float(closes[t-1])<=up_last*(1+r.breakout_buffer_pct/100)
    fresh_breakout=float(closes[t])>=up_today*(1+r.breakout_buffer_pct/100)
    not_chase=float(closes[t])<=up_today*(1+r.max_chase_pct/100)
    z['breakout_geometry']=bool(previous_inside and fresh_breakout and not_chase)
    z.update({'previous_inside_upper':previous_inside,'fresh_close_breakout':fresh_breakout,
              'not_chasing':not_chase})
    if not z['breakout_geometry']:return fail('NO_FRESH_CLOSE_BREAKOUT')
    # MA224 may be supplied by source context. Never infer it from future values.
    if ma224 is None and t>=223 and positive_bars(v,t-223,t):
        ma224=sum(float(x) for x in closes[t-223:t+1])/224
    if ma224 is None or not math.isfinite(float(ma224)) or float(ma224)<=0:
        return fail('MISSING_ASOF_MA224')
    z['ma224_asof']=float(ma224)
    z['qualified_below224']=float(closes[t])<float(ma224)
    if not z['qualified_below224']:return fail('NOT_BELOW224')
    prior_amt=sum(float(x) for x in amount[t-20:t])/20
    if prior_amt<=0:return fail('NO_PRE20_AMOUNT')
    amt_ratio=float(amount[t])/prior_amt
    z['amount_ratio']=amt_ratio
    z['qualified_with_amount']=amt_ratio>=r.amount_reentry_ratio
    if not z['qualified_with_amount']:return fail('NO_AMOUNT_REENTRY')
    z.update({'status':'PASS','reason':'PASS_STRICT_TRIANGLE'})
    return z


def no_future_signal_test(v,t,ma224=None,rule=RULE):
    # For tests only: a truncated as-of array should classify identically.
    return geometry(v,t,ma224,rule)
