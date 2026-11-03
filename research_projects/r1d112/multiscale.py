"""R1-D112 point-in-time, scale-separated active-line and broken-line role state.

Research only; does not execute trades or change prior R1-D111 selections.
All candidate lines are created independently in R1-D111. No human anchors are
used in discovery, scoring, activation, replacement, or role confirmation.
"""
from __future__ import annotations
from collections import defaultdict
from math import isfinite
import pandas as pd
import numpy as np

SCALES = ('SHORT','MEDIUM','LONG')
# Deliberately explanatory fixed windows, not fitted to historical outcome labels.
CONFIG = {
 'SHORT': {'span_lo':3,'span_hi':12,'known_age':30,'end_age':38,'gap_pct':8.,'hysteresis':3.0,'max_life':50,'min_endpoint_change':3,'min_hold':3},
 'MEDIUM':{'span_lo':13,'span_hi':35,'known_age':55,'end_age':65,'gap_pct':12.,'hysteresis':3.5,'max_life':85,'min_endpoint_change':5,'min_hold':4},
 'LONG':  {'span_lo':36,'span_hi':150,'known_age':125,'end_age':140,'gap_pct':18.,'hysteresis':4.0,'max_life':160,'min_endpoint_change':10,'min_hold':5},
}
# A 25/25 pivot line is structurally long-term even with shorter anchor distance.

def scale_of(l):
    if 'PIVOT25' in str(l['family']):return 'LONG'
    s=int(l['span'])
    if s<13:return 'SHORT'
    if s<36:return 'MEDIUM'
    return 'LONG'


def level(l,t):return float(l['v2'])+float(l['m'])*(int(t)-int(l['i2']))


def _atr(d,t):
    x=float(d.atr14.iloc[t]);return x if isfinite(x) and x>0 else float(d.close.iloc[t])*.02


def _candidate_ok(d,l,t,scale,kind):
    cfg=CONFIG[scale]
    if l['kind']!=kind or scale_of(l)!=scale or int(l['known_idx'])>t:return False
    if t-int(l['known_idx'])>cfg['known_age'] or t-int(l['i2'])>cfg['end_age']:return False
    v=level(l,t);px=float(d.close.iloc[t])
    if v<=0 or abs(100*(px/v-1))>cfg['gap_pct']:return False
    # A resistance substantially below close or support substantially above
    # close is not treated as the same directional role while it is broken.
    # 0.5 ATR tolerance keeps a just-crossed line observable until verified.
    margin=max(px*.004, .5*_atr(d,t))
    if kind=='UPPER' and v<px-margin:return False
    if kind=='LOWER' and v>px+margin:return False
    return True


def _score(d,l,t):
    """Point-in-time structural/reaction score; no future labels.

    Static geometry comes from R1-D111's R1-D111 candidate catalog; the
    dynamic part uses only bars <=t, no look-ahead or manual references.
    """
    if int(l['known_idx'])>t:raise ValueError('candidate used before first known')
    px=float(d.close.iloc[t]);v=level(l,t)
    if v<=0:return -1e10
    gap=abs(100*(px/v-1));age=t-int(l['known_idx'])
    span=int(l['span']);atr=_atr(d,t)
    base=(9.*float(l['anchor_strength'])+min(9.,span*.23)
          -3.0*int(l['body_pen'])-1.0*int(l['wick_pen'])
          +1.3*min(4.,int(l['interior_wick_touches']))
          +1.2*min(3.,int(l['interior_body_touches']))
          -1.5*float(l['mean_body_error_atr']))
    # A new line competes within its own temporal tier, never against a
    # different tier. Current relevance reduces the stale-line problem.
    score=base+max(0.,5.-age*.08)-gap*.65
    k=l['kind'];beg=max(int(l['known_idx'])+1,t-24);hits=holds=wrong=0
    if beg<=t:
        ix=np.arange(beg,t+1,dtype=int)
        lv=float(l['v2'])+float(l['m'])*(ix-int(l['i2']))
        good=lv>0
        if good.any():
            ix=ix[good];lv=lv[good]
            closes=d.close.to_numpy(float)[ix]
            high=d.high.to_numpy(float)[ix]
            low=d.low.to_numpy(float)[ix]
            atrs=d.atr14.to_numpy(float)[ix]
            atrs=np.where(np.isfinite(atrs)&(atrs>0),atrs,.02*closes)
            tol=np.maximum(closes*.003,.3*atrs)
            near=(high>=lv-tol)&(low<=lv+tol)
            hits=int(near.sum())
            if k=='UPPER':
                holds=int((near&(closes<=lv)).sum());wrong=int((closes>lv+tol).sum())
            else:
                holds=int((near&(closes>=lv)).sum());wrong=int((closes<lv-tol).sum())
    return round(score+3.*min(3,holds)-3.*min(5,wrong)+.4*min(5,hits),5)


def _events_cols():
    return ['algorithm','scale','kind','idx','date','event','old_line_id','new_line_id','reason','old_slope','new_slope','same_family','trend_sign_changed']


def monitor_multiscale(d,algorithm,lines,*,max_role_age=15):
    """Return (daily states, transitions, broken-line role events).

    As-of day t draws strictly from candidate lines whose known_idx <=t.
    A closed line is never reactivated in the same scale/kind lane.
    """
    by_known=defaultdict(list)
    for l in lines:
        if l['algorithm']!=algorithm:raise ValueError('wrong algorithm line')
        if int(l['known_idx'])<0 or int(l['known_idx'])>=len(d):raise ValueError('invalid candidate time')
        by_known[int(l['known_idx'])].append(l)
    lanes={(scale,kind):{'active':None,'selected':-1,'retired':set(),'wait':'WAIT_TWO_KNOWN_ANCHORS','cooldown':-1}
           for scale in SCALES for kind in ('UPPER','LOWER')}
    available=defaultdict(list);daily=[];transitions=[];roles=[];pending=[]
    dates=d.date.dt.strftime('%Y-%m-%d').tolist()
    for t in range(len(d)):
        for l in by_known.get(t,[]):available[(scale_of(l),l['kind'])].append(l)
        px=float(d.close.iloc[t]);atr=_atr(d,t)
        for scale in SCALES:
            cfg=CONFIG[scale]
            for kind in ('UPPER','LOWER'):
                key=(scale,kind);s=lanes[key];old=s['active'];event='NONE';reason='';former=old
                if old is not None and t>s['selected'] and t>0:
                    p=level(old,t-1);v=level(old,t)
                    tol0=max(float(d.close.iloc[t-1])*.004,.3*_atr(d,t-1))
                    tol1=max(px*.004,.3*atr)
                    if kind=='UPPER':broken=(float(d.close.iloc[t-1])>p+tol0 and px>v+tol1)
                    else:broken=(float(d.close.iloc[t-1])<p-tol0 and px<v-tol1)
                    if broken:
                        event='TWO_CLOSE_BREAK_ARCHIVE';reason='TWO_CONSECUTIVE_OPPOSITE_CLOSES_ATR_OR_0P4PCT'
                        s['retired'].add(old['line_id']);s['active']=None;s['selected']=-1;s['cooldown']=t
                        s['wait']='WAIT_NEW_CONFIRMED_ANCHOR';old=None
                        pending.append({'algorithm':algorithm,'scale':scale,'kind':kind,'line':former,'broken_idx':t,
                                        'touched':0,'first_respected_idx':None,'done':False})
                if old is not None and t-int(old['known_idx'])>cfg['max_life']:
                    event='ARCHIVE_STALE_DISPLAY';reason='DISPLAY_AGE_NOT_PRICE_TREND_END'
                    s['retired'].add(old['line_id']);s['active']=None;s['selected']=-1;s['cooldown']=t
                    s['wait']='WAIT_NEW_CONFIRMED_ANCHOR';old=None
                # Prevent reactivating a line just retired, and no same-bar
                # new line after break: the next candle supplies confirmation.
                if event=='NONE' and t>s['cooldown']:
                    new=[l for l in available[key]
                         if l['line_id'] not in s['retired'] and _candidate_ok(d,l,t,scale,kind)]
                    if old is not None:
                        new=[l for l in new if (int(l['i2'])>=int(old['i2'])+cfg['min_endpoint_change']
                               and int(l['known_idx'])>s['selected'])]
                    if new:
                        scored=sorted(((_score(d,l,t),l) for l in new),key=lambda p:(p[0],int(p[1]['i2']),p[1]['line_id']),reverse=True)
                        top_score,best=scored[0]
                        if old is None:
                            s['active']=best;s['selected']=t;s['wait']=''
                            event='CREATE_ACTIVE';reason='BEST_CURRENT_TIER_SCORE'
                        elif t-s['selected']>=cfg['min_hold'] and top_score>=_score(d,old,t)+cfg['hysteresis']:
                            s['retired'].add(old['line_id']);s['active']=best;s['selected']=t
                            event='REANCHOR_NEW_CONFIRMED';reason='NEW_ENDPOINT_AND_SCORE_HYSTERESIS'
                cur=s['active'];v=level(cur,t) if cur else float('nan')
                rec={'algorithm':algorithm,'scale':scale,'kind':kind,'idx':t,'date':dates[t],
                     'close':px,'state':'ACTIVE' if cur else s['wait'],
                     'active_line_id':cur['line_id'] if cur else '',
                     'active_family':cur['family'] if cur else '',
                     'anchor1':cur['a1'] if cur else '', 'anchor2':cur['a2'] if cur else '',
                     'active_known_date':cur['known_date'] if cur else '',
                     'active_level':round(v,4) if cur else float('nan'),
                     'active_gap_pct':round(100*(px/v-1),4) if cur and v>0 else float('nan'),
                     'active_slope_pct_per_bar':round(100*float(cur['m'])/float(cur['v1']),6) if cur else float('nan'),
                     'active_age_bars':t-int(cur['known_idx']) if cur else float('nan'),
                     'event':event,'reason':reason}
                daily.append(rec)
                if event!='NONE':
                    o=former or {};n=cur or {}
                    transitions.append({**rec,'old_line_id':o.get('line_id',''),'new_line_id':n.get('line_id',''),
                        'old_slope':o.get('m',float('nan')),'new_slope':n.get('m',float('nan')),
                        'same_family':bool(o and n and o['family']==n['family']),
                        'trend_sign_changed':bool(o and n and float(o['m'])*float(n['m'])<0)})
        for r in pending:
            if r['done'] or t<=r['broken_idx']:continue
            dt=t-r['broken_idx']
            if dt>max_role_age:r['done']=True;continue
            l=r['line'];v=level(l,t)
            if v<=0:r['done']=True;continue
            q=d.iloc[t];tol=max(.003*px,.3*atr)
            hit=float(q.low)<=v+tol and float(q.high)>=v-tol
            if not hit:continue
            new_side=(px>=v+tol*.2 if r['kind']=='UPPER' else px<=v-tol*.2)
            r['touched']+=1
            if new_side:
                if r['first_respected_idx'] is None:r['first_respected_idx']=t
                status='ROLE_RETEST_FIRST_RESPECTED' if r['touched']==1 else 'ROLE_RETEST_CONFIRMED_REPEATED'
            else:status='ROLE_RETEST_FAILED_OLD_SIDE_CLOSE'
            roles.append({'algorithm':algorithm,'scale':r['scale'],'broken_line_id':l['line_id'],
                  'old_kind':r['kind'],'new_role_candidate':'SUPPORT' if r['kind']=='UPPER' else 'RESISTANCE',
                  'break_idx':r['broken_idx'],'break_date':dates[r['broken_idx']],
                  'retest_idx':t,'retest_date':dates[t],'days_after_break':dt,
                  'retest_count':r['touched'],'correct_new_side_close':bool(new_side),
                  'status':status,'line_level':round(v,4),'close':px})
            # Invalidation and repeated confirmation terminate independent
            # role tracking; original archived trendline remains untouched.
            if not new_side or r['touched']>=2:r['done']=True
        pending=[r for r in pending if not r['done'] and t-r['broken_idx']<=max_role_age]
        if t%15==0:
            # Purge display candidates only; R111 immutable file is untouched.
            for key,a in available.items():
                available[key]=[l for l in a if int(l['known_idx'])>=t-CONFIG[key[0]]['known_age']-1]
    return pd.DataFrame(daily),pd.DataFrame(transitions),pd.DataFrame(roles)


def manual_audit(d,algorithm,lines,states,references,previous):
    """Manual references are NEVER inputs to monitor_multiscale."""
    byid={l['line_id']:l for l in lines};rows=[]
    for a1,a2,family,kind,label in references:
        matching=[l for l in lines if l['a1']==a1 and l['a2']==a2 and l['family']==family and l['kind']==kind]
        if len(matching)>1:raise AssertionError('ambiguous human pair')
        prev=previous[(previous.algorithm.eq(algorithm))&(previous.reference.eq(label))]
        old_first=prev.iloc[0]['first_active_date'] if len(prev) else ''
        if not matching:
            rows.append({'algorithm':algorithm,'reference':label,'kind':kind,'discovered':False,
                         'scale':'','first_known_date':'','r111_first_active_date':old_first,
                         'r112_first_active_date':'','r112_active_ever':False,
                         'reason':'NO_R111_CANDIDATE_AND_NO_MANUAL_INJECTION'});continue
        l=matching[0];scale=scale_of(l)
        hit=states[(states.scale.eq(scale))&(states.kind.eq(kind))&(states.active_line_id.eq(l['line_id']))]
        seen=str(hit.iloc[0]['date']) if len(hit) else ''
        rows.append({'algorithm':algorithm,'reference':label,'kind':kind,'discovered':True,
           'scale':scale,'first_known_date':l['known_date'],'r111_first_active_date':old_first,
           'r112_first_active_date':seen,'r112_active_ever':bool(len(hit)),
           'reason':'AUTO_SELECTED_FROM_FULL_COMPETITION' if seen else 'CANDIDATE_ONLY_NOT_SELECTED'})
    return pd.DataFrame(rows)


def structure_asof(daily):
    """Descriptive geometry of the SIX causal active-line lanes; no future bars.

    A slope-sign change alone is not an established price-trend reversal.
    Use separate upstream confirmed-pivot structure to adjudicate reversals.
    """
    rows=[]
    if daily.empty:return pd.DataFrame()
    for (algorithm,date,scale),g in daily.groupby(['algorithm','date','scale'],sort=False):
        u=g[g.kind.eq('UPPER')];l=g[g.kind.eq('LOWER')]
        if len(u)!=1 or len(l)!=1:raise AssertionError('nonunique upper/lower tier state')
        u=u.iloc[0];l=l.iloc[0]
        def slope(r):return float(r.active_slope_pct_per_bar) if pd.notna(r.active_slope_pct_per_bar) else np.nan
        su,sl=slope(u),slope(l)
        if not (isfinite(su) and isfinite(sl)):shape='INSUFFICIENT_ACTIVE_LINES'
        elif su<0<sl:shape='CONVERGING_UP_LOW_DOWN_HIGH'
        elif su>0 and sl>0:shape='BOTH_LINES_RISING'
        elif su<0 and sl<0:shape='BOTH_LINES_FALLING'
        elif su>0>sl:shape='DIVERGING_LINES'
        else:shape='MIXED_FLAT_OR_ZERO'
        rows.append({'algorithm':algorithm,'date':date,'idx':int(u.idx),'scale':scale,
          'upper_active_id':str(u.active_line_id),'lower_active_id':str(l.active_line_id),
          'upper_slope_pct_per_bar':su,'lower_slope_pct_per_bar':sl,
          'line_geometry_state_PIT':shape,
          'warning':'LINE_GEOMETRY_NOT_CONFIRMED_PRICE_TREND_REVERSAL'})
    return pd.DataFrame(rows)
