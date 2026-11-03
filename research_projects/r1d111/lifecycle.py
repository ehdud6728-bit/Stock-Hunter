"""Point-in-time dynamic active trendline management.

Exactly one ACTIVE upper and one ACTIVE lower per algorithm. Retired lines
never return; replacement needs a newer confirmed anchor. Archive is immutable.
"""
from __future__ import annotations
from collections import defaultdict
from math import isfinite
import numpy as np
import pandas as pd
from algorithms import value,quality,ALGORITHMS,PARAMETERS


def _eligible(d,l,t,kind,limit=8.0):
    if l['kind']!=kind or l['known_idx']>t or l['known_idx']<t-35:return False
    if l['i2']<t-55:return False
    v=value(l,t);px=float(d.close.iloc[t])
    if v<=0 or abs(100*(px/v-1))>limit:return False
    # Resistance not >1.2% below price; support not >1.2% above.
    if kind=='UPPER' and px>v*1.012:return False
    if kind=='LOWER' and px<v*.988:return False
    return True


def monitor(d,algorithm,lines):
    """Return PIT daily states, transitions, and first selected per line.

    No event at t can depend on bar t+1. The two-close break rule is causal.
    New lines may only replace old at/after their known_idx and later endpoint.
    """
    if algorithm not in ALGORITHMS:raise ValueError(algorithm)
    by_known=defaultdict(list)
    for l in lines:by_known[int(l['known_idx'])].append(l)
    state={k:{'active':None,'selected_idx':None,'retired':set(),'wait_reason':'WAIT_TWO_ANCHORS'} for k in ('UPPER','LOWER')}
    recent={'UPPER':[],'LOWER':[]};daily=[];events=[]
    maxlife=PARAMETERS[algorithm]['max_life']
    date=lambda t:str(d.iloc[t].date.date())
    for t in range(len(d)):
        for l in by_known.get(t,[]):recent[l['kind']].append(l)
        px=float(d.close.iloc[t]);prev=float(d.close.iloc[t-1]) if t else None
        for kind in ('UPPER','LOWER'):
            s=state[kind];old=s['active'];event='NONE';reason='';former=old
            # Retire on price-confirmed opposite side (two consecutive closes).
            if old is not None and t>=2:
                l0=value(old,t-1);l1=value(old,t)
                if l0>0 and l1>0:
                    broken=(prev>l0*1.004 and px>l1*1.004) if kind=='UPPER' else (prev<l0*.996 and px<l1*.996)
                else:broken=False
                if broken:
                    event='BREAK_CONFIRMED_TWO_CLOSES';reason='TWO_OPPOSITE_SIDE_CLOSES'
                    s['retired'].add(old['line_id']);s['active']=None;s['selected_idx']=None
                    s['wait_reason']='WAIT_NEW_CONFIRMED_ANCHOR';old=None
            if old is not None and (t-int(old['known_idx'])>maxlife):
                event='RETIRED_STALE';reason='AGE_LIMIT_EXPLANATORY_NOT_TREND_END'
                s['retired'].add(old['line_id']);s['active']=None;s['selected_idx']=None
                s['wait_reason']='WAIT_NEW_CONFIRMED_ANCHOR';old=None
            if event=='NONE':
                # Keep a limited trailing pool; archived never revived.
                new=[l for l in recent[kind] if _eligible(d,l,t,kind) and l['line_id'] not in s['retired']]
                # When updating an ACTIVE line, new endpoint must be newer,
                # AND new line must have become known after the old selection.
                if old is not None:
                    new=[l for l in new if (l['i2']>old['i2'] and l['known_idx']>s['selected_idx'])]
                pool=sorted(new,key=lambda l:(quality(d,l,t)[0],l['i2'],l['line_id']),reverse=True)
                if old is None and pool:
                    best=pool[0];s['active']=best;s['selected_idx']=t
                    event='CREATED_ACTIVE';reason='CAUSALLY_AVAILABLE_AND_NEAR_PRICE';s['wait_reason']=''
                elif old is not None and pool and t>=int(s['selected_idx'])+2:
                    best=pool[0];bscore=quality(d,best,t)[0];oscore=quality(d,old,t)[0]
                    if bscore>=oscore+4.0:
                        s['retired'].add(old['line_id']);s['active']=best;s['selected_idx']=t
                        event='REANCHORED_NEW_CONFIRMED_PIVOT_OR_CANDLE';reason='NEWER_ENDPOINT_AND_QUALITY_HYSTERESIS'
            current=s['active'];current_id=current['line_id'] if current else ''
            active_level=value(current,t) if current else np.nan
            active_gap=100*(px/active_level-1) if current and active_level>0 else np.nan
            day={'algorithm':algorithm,'kind':kind,'idx':t,'date':date(t),
                'close':px,'state':'ACTIVE' if current else s['wait_reason'],
                'active_line_id':current_id,'active_family':current['family'] if current else '',
                'active_anchor1':current['a1'] if current else '',
                'active_anchor2':current['a2'] if current else '',
                'active_first_known_date':current['known_date'] if current else '',
                'active_level':round(active_level,4) if isfinite(active_level) else np.nan,
                'active_price_gap_pct':round(active_gap,4) if isfinite(active_gap) else np.nan,
                'active_slope_krw_per_bar':round(current['m'],6) if current else np.nan,
                'active_anchor_age':t-current['i2'] if current else np.nan,
                'event':event,'transition_reason':reason}
            daily.append(day)
            if event!='NONE':
                prevl=former or {}
                ev={**day,'old_line_id':prevl.get('line_id',''),'old_anchor2':prevl.get('a2',''),
                    'old_slope':prevl.get('m',np.nan),'new_slope':current['m'] if current else np.nan,
                    'new_pivot_confirmed_on_event':bool(current and current['known_idx']==t)}
                # Compare slope of like-for-like family separately from family changes.
                ev['slope_sign_flip']=bool(former and current and float(former['m'])*float(current['m'])<0)
                ev['same_family_reanchor']=bool(former and current and former['family']==current['family'])
                events.append(ev)
        # Bound memory; drop only old candidate display pool, never immutable catalog.
        for k in recent:
            if t%15==0:recent[k]=[l for l in recent[k] if int(l['known_idx'])>=t-60]
    return pd.DataFrame(daily),pd.DataFrame(events)


def assess_manual(d,algorithm,lines,states,refs):
    """Manual reference pairs used for EVALUATION ONLY, never line selection."""
    rows=[]
    for r in refs:
        date1,date2,family,kind,label=r
        found=[l for l in lines if l['a1']==date1 and l['a2']==date2 and l['family']==family and l['kind']==kind]
        if len(found)>1:raise AssertionError('nonunique reference detection')
        if not found:
            rows.append({'algorithm':algorithm,'reference':label,'kind':kind,'a1':date1,'a2':date2,'family':family,
                'candidate_detected':False,'first_available_date':None,'first_active_date':None,
                'reason':'NOT_IN_INDEPENDENT_CANDIDATE_SET'});continue
        l=found[0];match=states[(states.active_line_id==l['line_id']) & (states.kind==kind)]
        idx=int(l['known_idx']);score,info=quality(d,l,idx)
        # Rank among same-kind candidates made available on reference line known date.
        competitors=[x for x in lines if x['kind']==kind and x['known_idx']<=idx and x['known_idx']>=idx-35 and x['i2']>=idx-55]
        cand_rank=1+sum(quality(d,x,idx)[0]>score for x in competitors if x['line_id']!=l['line_id'])
        rows.append({'algorithm':algorithm,'reference':label,'kind':kind,'a1':date1,'a2':date2,'family':family,
            'candidate_detected':True,'first_available_date':l['known_date'],
            'first_active_date':str(match.iloc[0].date) if len(match) else None,
            'active_ever':bool(len(match)),'rank_in_all_recent_candidates_at_first_known':cand_rank,
            'available_competitor_count':len(competitors),
            'quality_raw_at_creation':round(score,4),'span':l['span'],
            'interior_body_penetrations_ATR_0p3':l['body_pen'],
            'interior_wick_penetrations_ATR_0p3':l['wick_pen'],
            'reason':'AUTO_GENERATED_NOT_MANUALLY_INJECTED'})
    return pd.DataFrame(rows)


def archived_line_retests(d,algorithm,lines,transitions,*,max_follow=15):
    """Prospective role-reversal attempts after a CONFIRMED break.

    Confirmation can happen only on a subsequent bar, never on the breakout
    bar itself. The old line stays archived; no automatic relabel into a new
    active support/resistance until a distinct confirmed retest is recorded.
    """
    cols=['algorithm','broken_line_id','old_kind','new_role_candidate','break_date',
          'retest_asof','days_after_break','contact','correct_side_close','status']
    if transitions.empty:return pd.DataFrame(columns=cols)
    broken=transitions[transitions.event.eq('BREAK_CONFIRMED_TWO_CLOSES')]
    if broken.empty:return pd.DataFrame(columns=cols)
    byid={l['line_id']:l for l in lines};rows=[]
    for r in broken.to_dict('records'):
        oldid=str(r['old_line_id'])
        if oldid not in byid:raise AssertionError('broken line not in immutable catalog')
        l=byid[oldid];t0=int(r['idx']);kind=l['kind']
        role='POTENTIAL_SUPPORT_AFTER_RESISTANCE_BREAK' if kind=='UPPER' else 'POTENTIAL_RESISTANCE_AFTER_SUPPORT_BREAK'
        seen_valid=False
        for t in range(t0+1,min(len(d),t0+max_follow+1)):
            q=d.iloc[t];lvl=value(l,t)
            if lvl<=0:break
            atr=float(q.atr14);tol=max(.0025*q.close,.30*atr) if isfinite(atr) else .005*q.close
            contact=(float(q.low)<=lvl+tol and float(q.high)>=lvl-tol)
            good=(float(q.close)>=lvl*1.004) if kind=='UPPER' else (float(q.close)<=lvl*.996)
            # An explicit role flip requires the price revisit AND close on the
            # NEW side. Subsequent daily observations remain causal.
            if contact:
                status='ROLE_RETEST_CONFIRMED_PIT' if good else 'RETEST_FAILED_OR_NO_ROLE_CONFIRMATION'
                seen_valid=seen_valid or good
                rows.append({'algorithm':algorithm,'broken_line_id':oldid,'old_kind':kind,
                    'new_role_candidate':role,'break_date':str(r['date']),
                    'retest_asof':str(q.date.date()),'days_after_break':t-t0,
                    'contact':True,'correct_side_close':good,'status':status})
                if good:break
    return pd.DataFrame(rows,columns=cols)
