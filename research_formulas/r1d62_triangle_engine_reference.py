#!/usr/bin/env python3
"""R1-D62: deterministic TWO-SWING triangle study. RESEARCH_ONLY.

Signal t is computed with history through t CLOSE. Both fitted trendlines are
frozen from fully confirmed pivots using bars at most t-1 (pivot <=t-3).
Followup decisions only use close at t+1 / t+3; fills NEXT day open.
Not the same definition as R1-D56 tri40_triangle_breakout.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict
import numpy as np

@dataclass(frozen=True)
class Rule:
    window:int=40
    pivot_radius:int=2
    min_pivot_gap:int=4
    pivot_local_prominence_min_pct:float=2.0
    wave_gain_min_pct:float=12.0
    high_drop_min_pct:float=0.5
    low_fall_tolerance_pct:float=0.5
    contraction_min_pct:float=25.0
    end_width_min_pct:float=0.5
    end_width_max_pct:float=12.0
    breakout_buffer_pct:float=0.3
    chase_ceiling_pct:float=6.0
    amount_reentry_min_ratio:float=1.3
    retest_penetration_pct:float=1.5
    retest_proximity_pct:float=1.0
    max_lower_break_pct:float=2.0

R=Rule()
DATA_KEYS=('open','high','low','close','amount')

def _bars(v,t,lookback):
    n=len(v['close'])
    if t<lookback or t>=n:return False
    s=slice(t-lookback,t+1)
    cols=[np.asarray(v[k][s],float) for k in DATA_KEYS]
    o,h,l,c,a=cols
    return bool(all(np.isfinite(z).all() for z in cols) and
       (o>0).all() and (h>0).all() and (l>0).all() and (c>0).all() and
       (h>=np.maximum(o,c)).all() and (l<=np.minimum(o,c)).all() and
       (h>=l).all() and (a>=0).all())

def _pivots(values, begin, end, radius=2, top=True, prominence_pct=2.0):
    """Candidate indices in [begin,end]; right confirmation must be <=t-1.
    Use strict uniqueness to exclude flat ambiguous plateaus.
    """
    a=np.asarray(values,float);out=[]
    for j in range(begin,end+1):
        z=a[j-radius:j+radius+1]
        if len(z)!=2*radius+1:continue
        neighbors=np.concatenate([z[:radius],z[radius+1:]])
        if (top and a[j]==max(z) and (z==a[j]).sum()==1 and
                a[j]>=max(neighbors)*(1+prominence_pct/100)):
            out.append(j)
        if (not top and a[j]==min(z) and (z==a[j]).sum()==1 and
                a[j]<=min(neighbors)/(1+prominence_pct/100)):
            out.append(j)
    return out

def _latest_pair(piv, min_gap):
    if len(piv)<2:return None
    b=piv[-1]
    older=[j for j in piv[:-1] if b-j>=min_gap]
    if not older:return None
    return older[-1],b

def line_at(pair, vals, x):
    a,b=pair
    return float(vals[a]+(vals[b]-vals[a])*(x-a)/(b-a))

def signal(v,t,rule=R):
    """No mutation; output never contains future outcomes or future bars."""
    if rule.window not in (20,40,60):raise ValueError('WINDOW_NOT_PREDECLARED')
    need=max(224,rule.window+32,80)
    if not _bars(v,t,need):return {'status':'INVALID_ASOF_HISTORY','ready':False,'breakout':False}
    o,h,l,c,a=(np.asarray(v[k],float) for k in DATA_KEYS)
    ma224=float(c[t-223:t+1].mean())
    prev_amt=float(a[t-20:t].mean())
    below224=bool(c[t]<ma224)
    if prev_amt<=0:return {'status':'INVALID_PREV_AMOUNT','ready':False,'breakout':False}
    # Lock both upper/lower lines BEFORE seeing bar t: pivots at most t-3,
    # fully confirmed by t-1.
    start=t-rule.window
    end=t-rule.pivot_radius-1 # for radius2: t-3, right end = t-1
    hp=_pivots(h,start,end,rule.pivot_radius,True,rule.pivot_local_prominence_min_pct)
    lp=_pivots(l,start,end,rule.pivot_radius,False,rule.pivot_local_prominence_min_pct)
    ph=_latest_pair(hp,rule.min_pivot_gap)
    pl=_latest_pair(lp,rule.min_pivot_gap)
    base={'status':'OK','ready':False,'breakout':False,'below224':below224,
          'ma224_signal':ma224,'signal_close':float(c[t]),
          'amount_ratio':float(a[t]/prev_amt),'upper_pivot_indices':ph,
          'lower_pivot_indices':pl,'window':rule.window}
    if not (ph and pl):return base
    h0,h1=ph;l0,l1=pl
    # The first high is anchored to the earlier impulse. An earlier low in the
    # 5..25 sessions preceding this high supplies the causal wave's base.
    prior_from=max(t-80,h0-25,1);prior_to=h0-4
    if prior_to<=prior_from:return base
    trough=float(np.min(l[prior_from:prior_to+1]))
    wave_gain=float((h[h0]/trough-1)*100)
    u0=line_at(ph,h,min(h0,l0));d0=line_at(pl,l,min(h0,l0))
    u1=line_at(ph,h,t-1);d1=line_at(pl,l,t-1)
    ut=line_at(ph,h,t);dt=line_at(pl,l,t)
    wstart=u0-d0;wend=u1-d1
    if wstart<=0 or wend<=0 or min(ut,dt)<=0:return base
    contraction=100*(1-wend/wstart)
    end_width_pct=100*wend/u1
    upper_drop_pct=100*(1-h[h1]/h[h0])
    low_change_pct=100*(l[l1]/l[l0]-1)
    ready=bool(below224 and wave_gain>=rule.wave_gain_min_pct and
        upper_drop_pct>=rule.high_drop_min_pct and
        low_change_pct>=-rule.low_fall_tolerance_pct and
        contraction>=rule.contraction_min_pct and
        rule.end_width_min_pct<=end_width_pct<=rule.end_width_max_pct and
        ut>dt)
    base.update({'ready':ready,'wave_gain_pct':wave_gain,
        'upper_drop_pct':upper_drop_pct,'lower_change_pct':low_change_pct,
        'contraction_pct':contraction,'end_width_pct':end_width_pct,
        'upper_t':ut,'lower_t':dt,'upper_slope':float((h[h1]-h[h0])/(h1-h0)),
        'lower_slope':float((l[l1]-l[l0])/(l1-l0)),
        'upper_t_minus1':u1})
    crossed=bool(c[t-1]<=u1*(1+rule.breakout_buffer_pct/100) and
        c[t]>=ut*(1+rule.breakout_buffer_pct/100))
    not_chasing=bool(c[t]<=ut*(1+rule.chase_ceiling_pct/100))
    base.update({'fresh_cross':crossed,'not_chasing':not_chasing,
        'amount_reentry':bool(base['amount_ratio']>=rule.amount_reentry_min_ratio),
        # Separate geometry vs amount for preregistered ablation
        'breakout_geometry':bool(ready and crossed and not_chasing),
        'breakout':bool(ready and crossed and not_chasing and
                        base['amount_ratio']>=rule.amount_reentry_min_ratio)})
    return base

def _project(f,day,which='upper'):
    return f[f'{which}_t']+f[f'{which}_slope']*day

def confirm(v,t,f,mode,rule=R):
    """Return confirmation using only t+N close; NO fill/outcome fields.
    Immediate knows signal t; retest D1 knows D1; retest D3 and fake recapture
    know only D1-D3. D3 is the *fixed* confirmation day, not first success date.
    """
    if mode not in ('IMMEDIATE_T1','RETEST_D1_T2','RETEST_D3_T4','FAKE_RECAPTURE_D3_T4'):
        raise ValueError('BAD_MODE')
    n={'IMMEDIATE_T1':0,'RETEST_D1_T2':1,'RETEST_D3_T4':3,'FAKE_RECAPTURE_D3_T4':3}[mode]
    if not f.get('breakout'):return {'status':'NOT_SIGNAL','confirmed':False,'confirm_offset':n}
    if t+n>=len(v['close']):return {'status':'HORIZON_MISSING','confirmed':False,'confirm_offset':n}
    for j in range(t+1,t+n+1):
        if not _bars(v,j,0):return {'status':'INVALID_CONFIRM_BAR','confirmed':False,'confirm_offset':n}
    c,l,a=(np.asarray(v[k],float) for k in ('close','low','amount'))
    if n==0:return {'status':'OK','confirmed':True,'confirm_offset':0}
    def hold(j):return bool(c[t+j]>=_project(f,j)*1.003 and c[t+j]>=_project(f,j,'lower')*(1-rule.max_lower_break_pct/100))
    def retest(j):
        u=_project(f,j)
        return bool(u*(1-rule.retest_penetration_pct/100)<=l[t+j]<=u*(1+rule.retest_proximity_pct/100))
    if mode=='RETEST_D1_T2':
        ok=bool(retest(1) and hold(1))
    elif mode=='RETEST_D3_T4':
        ok=bool(any(retest(j) for j in (1,2,3)) and hold(3) and
             c[t+3]>=c[t+2] and all(c[t+j]>=_project(f,j,'lower')*.98 for j in (1,2,3)))
    else:
        failed=any(c[t+j]<_project(f,j)*.997 for j in (1,2))
        ok=bool(failed and hold(3) and c[t+3]>c[t+2] and
             a[t+3]>=max(a[t+1],a[t+2])*1.15 and
             all(c[t+j]>=_project(f,j,'lower')*.98 for j in (1,2,3)))
    return {'status':'OK','confirmed':ok,'confirm_offset':n}

def simulate(v,t,confirmation,hold_days=20):
    """Next-open fill. TP=+10%, SL=-8%, 20 sessions, cost 0.2pp.
    Gap at open first, SL-first for double intrabar touch. Missing data FAIL-CLOSED.
    """
    if not confirmation.get('confirmed'):
        return {'status':'CASH_SKIP','traded':False}
    entry_idx=t+int(confirmation['confirm_offset'])+1
    if entry_idx+hold_days>len(v['close']):return {'status':'HORIZON_MISSING','traded':False}
    for j in range(entry_idx,entry_idx+hold_days):
        if not _bars(v,j,0):return {'status':'INVALID_OUTCOME_BAR','traded':False}
    o,h,l,c=(np.asarray(v[k],float) for k in ('open','high','low','close'))
    entry=float(o[entry_idx]);up=entry*1.10;down=entry*.92
    for j in range(entry_idx,entry_idx+hold_days):
        op,hi,lo=float(o[j]),float(h[j]),float(l[j])
        if op<=down:exit_price=op;side='SL'
        elif op>=up:exit_price=op;side='TP'
        elif lo<=down:exit_price=down;side='SL'
        elif hi>=up:exit_price=up;side='TP'
        else:continue
        return {'status':'COMPLETE','traded':True,'entry_idx':entry_idx,
                'entry_open':entry,'exit_price':exit_price,'exit_reason':side,
                'net_pct':100*(exit_price/entry-1)-.2,'holding_sessions':j-entry_idx+1}
    exit_price=float(c[entry_idx+hold_days-1])
    return {'status':'COMPLETE','traded':True,'entry_idx':entry_idx,
            'entry_open':entry,'exit_price':exit_price,'exit_reason':'TIME',
            'net_pct':100*(exit_price/entry-1)-.2,'holding_sessions':hold_days}
