"""R1-D109: point-in-time pivot and active trendline change diagnostics.

Read-only companion to the frozen R1-D106/R1-D108 data. These are observed
structural changes, not predictions, trading rules or profitability scores.
"""
from __future__ import annotations
from collections import defaultdict
import math
import numpy as np
import pandas as pd

PIVOT_RIGHT=2
FLAT_PCT=0.3  # Tick/near-equal descriptive band, not outcome fitted.


def confirmed_pivots(days: pd.DataFrame, right: int=PIVOT_RIGHT, flat_pct: float=FLAT_PCT):
    """As-of confirmation is always j+right. Never place signal on pivot date j."""
    d=days.sort_values('idx').reset_index(drop=True)
    if right < 1: raise ValueError('right must be >=1')
    if d[['idx','date']].duplicated().any() or not d.idx.is_monotonic_increasing:
        raise ValueError('duplicate/unsorted input')
    # Global trading indices must be contiguous over the observed window.
    if not np.array_equal(d.idx.to_numpy(int), np.arange(int(d.idx.iloc[0]),int(d.idx.iloc[0])+len(d))):
        raise ValueError('non-contiguous trading indices')
    n=len(d)
    events=[];state=[]
    last={'HIGH':None,'LOW':None};relation={'HIGH':None,'LOW':None}
    prev_structure='INSUFFICIENT_PIVOTS'
    highs=d.high.to_numpy(float);lows=d.low.to_numpy(float)
    for t in range(n):
        fresh=[];j=t-right
        if j>=right:
            for kind,vals in (('HIGH',highs),('LOW',lows)):
                v=float(vals[j]);window=vals[j-right:j+right+1]
                yes=(v==float(np.max(window)) if kind=='HIGH' else v==float(np.min(window)))
                yes=yes and int(np.count_nonzero(window==v))==1
                if not yes:continue
                old=last[kind];dist=100*(v/old-1) if old and old>0 else None
                tag='FIRST' if dist is None else ('EQUAL' if abs(dist)<=flat_pct else
                      ('HIGHER' if dist>0 else 'LOWER'))
                relation[kind]=tag if tag!='FIRST' else None
                rec={'code':str(d.iloc[t].code),'name':str(d.iloc[t]['name']),
                     'pivot_type':kind,'pivot_date':str(d.iloc[j].date),
                     'pivot_idx':int(d.iloc[j].idx),'confirmed_date':str(d.iloc[t].date),
                     'confirmed_idx':int(d.iloc[t].idx),'pivot_price':v,
                     'previous_same_kind_price':old,'change_pct':round(dist,4) if dist is not None else None,
                     'pivot_relation':tag,'right_confirmation_bars':right,
                     'when_usable':'CONFIRMED_AT_CLOSE_NOT_ON_PIVOT_DATE'}
                events.append(rec);fresh.append(rec);last[kind]=v
        hi,lo=relation['HIGH'],relation['LOW']
        if hi is None or lo is None:structure='INSUFFICIENT_PIVOTS'
        elif hi=='HIGHER' and lo=='HIGHER':structure='HIGHER_HIGH_HIGHER_LOW'
        elif hi=='LOWER' and lo=='LOWER':structure='LOWER_HIGH_LOWER_LOW'
        elif hi=='LOWER' and lo=='HIGHER':structure='CONTRACTING_HIGH_LOW'
        elif hi=='HIGHER' and lo=='LOWER':structure='EXPANDING_HIGH_LOW'
        else:structure='MIXED_OR_NEAR_EQUAL_PIVOTS'
        changed=bool(structure!=prev_structure and structure!='INSUFFICIENT_PIVOTS')
        state.append({'idx':int(d.iloc[t].idx),'code':str(d.iloc[t].code),'date':str(d.iloc[t].date),
                      'confirmed_high_today':any(e['pivot_type']=='HIGH' for e in fresh),
                      'confirmed_low_today':any(e['pivot_type']=='LOW' for e in fresh),
                      'latest_high_price_PIT':last['HIGH'],'latest_low_price_PIT':last['LOW'],
                      'high_pivot_relation':hi or 'NOT_ENOUGH','low_pivot_relation':lo or 'NOT_ENOUGH',
                      'pivot_structure_PIT':structure,'pivot_structure_changed_today':changed,
                      'previous_pivot_structure':prev_structure,
                      'PIT_note':'PIVOT_USES_RIGHT_TWO_COMPLETED_BARS'})
        prev_structure=structure
    return pd.DataFrame(events),pd.DataFrame(state)


def slope_sign(x:float, eps:float=1e-10):
    if x is None or not math.isfinite(float(x)):return 'NA'
    return 'POSITIVE' if float(x)>eps else 'NEGATIVE' if float(x)<-eps else 'FLAT'


def line_transition_events(states:pd.DataFrame):
    """Compare previous active line to newly known line only at a real re-anchor.

    Slope sign changes are geometric annotations, not proof price trend reversed.
    New-after-break lines are not compared as if continuously active.
    """
    d=states.sort_values(['code','lane','idx']).reset_index(drop=True)
    out=[]
    for (code,lane),g in d.groupby(['code','lane'],sort=True):
        prev=None
        for r in g.to_dict('records'):
            state=r['state']
            if state not in ('UNCHANGED','NO_ACTIVE_LINE','WAIT_NEW_CONFIRMED_ANCHOR'):
                oldid=(prev or {}).get('active_line_id') or ''
                old_slope=(prev or {}).get('active_line_slope_pct')
                new_slope=r.get('active_line_slope_pct')
                is_reanchor=state=='REANCHORED_NEW_PIVOT_OR_CANDLE'
                # A true slope comparison must have a continuous old active line.
                valid=is_reanchor and bool(oldid) and pd.notna(old_slope) and pd.notna(new_slope)
                old_sign=slope_sign(old_slope) if valid else 'NA'
                new_sign=slope_sign(new_slope) if valid else 'NA'
                sign_flip=bool(valid and old_sign!=new_sign and old_sign!='FLAT' and new_sign!='FLAT')
                if valid and sign_flip:kind='SLOPE_SIGN_FLIPPED_AT_REANCHOR'
                elif valid and abs(float(new_slope))<abs(float(old_slope)):kind='SLOPE_MAGNITUDE_DECREASED_AT_REANCHOR'
                elif valid and abs(float(new_slope))>abs(float(old_slope)):kind='SLOPE_MAGNITUDE_INCREASED_AT_REANCHOR'
                elif valid:kind='SLOPE_UNCHANGED_AT_REANCHOR'
                elif state=='RESISTANCE_BROKEN_UP':kind='ACTIVE_RESISTANCE_BROKEN'
                elif state=='SUPPORT_BROKEN_DOWN':kind='ACTIVE_SUPPORT_BROKEN'
                elif state=='NEW_LINE_AFTER_BREAK':kind='NEW_LINE_AFTER_PRIOR_BREAK'
                elif state=='NEW_ACTIVE_LINE':kind='INITIAL_ACTIVE_LINE'
                else:kind='LINE_TRANSITION_OTHER'
                old_fam=(prev or {}).get('active_family') if valid else None
                new_fam=r.get('active_family') if valid else None
                out.append({'code':code,'name':r['name'],'idx':int(r['idx']),'date':r['date'],
                    'lane':lane,'line_state':state,'transition_kind_PIT':kind,
                    'old_active_line_id':oldid if valid else '',
                    'new_active_line_id':r.get('active_line_id') or '',
                    'retired_line_id':r.get('retired_line_id') or '',
                    'old_family':old_fam,'new_family':new_fam,
                    'same_family_reanchor':bool(valid and old_fam==new_fam),
                    'old_slope_pct_per_bar':round(float(old_slope),5) if valid else None,
                    'new_slope_pct_per_bar':round(float(new_slope),5) if valid else None,
                    'slope_delta_pp_per_bar':round(float(new_slope-old_slope),5) if valid else None,
                    'slope_sign_flip':sign_flip,
                    'can_claim_price_trend_reversal':False,
                    'note':'GEOMETRIC_NEW_LINE_NOT_PROOF_OF_MARKET_REVERSAL'})
            prev=r
    return pd.DataFrame(out)


def role_retest_events(days:pd.DataFrame,states:pd.DataFrame,lines:pd.DataFrame,lookahead_days:int=10):
    """Revisit archived broken line from each successive day, never future at break time.

    Event emitted only on actual retest or invalidation observation.
    """
    if lookahead_days<1:raise ValueError('lookahead_days')
    d=days.set_index('idx');events=[]
    byid={x['line_id']:x for x in lines.to_dict('records')}
    for r in states.to_dict('records'):
        state=r['state']
        if state not in ('RESISTANCE_BROKEN_UP','SUPPORT_BROKEN_DOWN'):continue
        lid=str(r.get('retired_line_id') or '')
        if not lid or lid not in byid:raise AssertionError('Broken active line not in frozen archive '+lid)
        ln=byid[lid];start=int(r['idx'])
        for t in range(start+1,start+lookahead_days+1):
            if t not in d.index:break
            q=d.loc[t];level=float(ln['v2'])+float(ln['slope_krw_per_bar'])*(t-int(ln['i2']))
            if level<=0:continue
            low,high,close=float(q.low),float(q.high),float(q.close)
            touched=bool(low<=level*1.005 and high>=level*.995)
            if state=='RESISTANCE_BROKEN_UP':
                failed=bool(close<level*.996)
                held=bool(touched and close>=level*.996)
                role='POTENTIAL_SUPPORT_RETEST' if held else 'SUPPORT_RETEST_FAILED' if touched and failed else 'FORMER_RESISTANCE_RELOST' if failed else 'NO_TEST'
            else:
                failed=bool(close>level*1.004)
                held=bool(touched and close<=level*1.004)
                role='POTENTIAL_RESISTANCE_RETEST' if held else 'RESISTANCE_RETEST_FAILED' if touched and failed else 'FORMER_SUPPORT_RECLAIMED' if failed else 'NO_TEST'
            if role=='NO_TEST':continue
            events.append({'code':str(q.code),'name':str(q['name']),
              'idx':t,'date':str(q.date),'break_idx':start,'break_date':str(r['date']),
              'broken_line_id':lid,'broken_lane':r['lane'],'original_break':state,
              'days_since_break':t-start,'frozen_line_price':round(level,3),
              'close':close,'low':low,'high':high,'retest_touched':touched,
              'role_observation_PIT':role,
              'note':'PER_BAR_ASOF_EVIDENCE_NOT_CONFIRMED_ROLE_FLIP_OR_TRADE_SIGNAL'})
    return pd.DataFrame(events)
