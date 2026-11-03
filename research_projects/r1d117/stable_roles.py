"""R1-D117 independent post-close state machine for existing R111 immutable lines.

This DOES NOT improve or replace candidate detection, modify R112/R116, or learn from returns.
Each day is processed in chronological order. All decisions are available after t close.
Retired lines remain archived, not silently recycled. Confirmed breaks create separate
role-watch records; a role reversal must be observed on later candles, never assumed.
"""
from __future__ import annotations
import math
from collections import defaultdict
import pandas as pd

# Reviewed, fixed research hypotheses. NO fit to manual cases or future returns.
POLICY={
 'SHORT':{'max_known_age':48,'max_endpoint_age':45,'max_gap':13.0,'min_hold':8,'endpoint_advance':4,'score_margin':8.0},
 'MEDIUM':{'max_known_age':85,'max_endpoint_age':85,'max_gap':18.0,'min_hold':12,'endpoint_advance':6,'score_margin':10.0},
 'LONG':{'max_known_age':170,'max_endpoint_age':160,'max_gap':29.0,'min_hold':20,'endpoint_advance':12,'score_margin':12.0},
}
BAND=0.004
RETEST_DAYS=12

def level(q,t):return float(q['v1'])+float(q['m'])*(int(t)-int(q['i1']))

def verify(q):
    if not 0<=int(q['i1'])<int(q['i2'])<=int(q['known_idx']):
        raise AssertionError('lookahead/reversed anchors '+str(q['line_id']))
    if not math.isclose(level(q,int(q['i2'])),float(q['v2']),abs_tol=.02):
        raise AssertionError('invalid original anchor geometry '+str(q['line_id']))

def admissible(q,t,close,scale,kind,retired=frozenset()):
    if str(q['line_id']) in retired or str(q['kind'])!=kind:return False
    if int(q['known_idx'])>=t:return False # NOT available before second anchor confirmation
    policy=POLICY[scale]
    if t-int(q['known_idx'])>policy['max_known_age'] or t-int(q['i2'])>policy['max_endpoint_age']:return False
    lv=level(q,t)
    if lv<=0:return False
    gap=100*(lv/close-1.)
    if abs(gap)>policy['max_gap']:return False
    if kind=='UPPER' and gap < -100*BAND:return False
    if kind=='LOWER' and gap > 100*BAND:return False
    return True

def score(q,t,close):
    # Frozen baseline explanatory geometry, not forecast probability.
    gap=abs(100*(level(q,t)/close-1.))
    return (8*float(q['anchor_strength'])+
            1.2*min(4,int(q['interior_body_touches']))+
            min(4,int(q['interior_wick_touches']))-
            1.5*float(q['mean_body_error_atr'])-
            3*int(q['body_pen'])-1.5*int(q['wick_pen'])-
            .15*(t-int(q['known_idx']))-.4*(t-int(q['i2']))-.45*gap)

def beyond(close,lv,kind):
    return close>lv*(1+BAND) if kind=='UPPER' else close<lv*(1-BAND)

def _near_candle(q,t,bar):
    lv=level(q,t);tol=BAND*float(bar['close'])
    return float(bar['low'])<=lv+tol and float(bar['high'])>=lv-tol

def replay_one(stock,algorithm,base_rows,top_rows,catalog,raw=None):
    """Chronological 6-lane replay. Requires exact R116 daily rows, top3 and R111 catalog.

    raw is optional only for local no-OHLCV smoke; then role-watches remain UNOBSERVABLE.
    No outcomes from D+1 or future are used in selector state.
    """
    marks=base_rows.sort_values(['idx','scale','kind']).copy()
    if marks[['idx','scale','kind']].duplicated().any():raise AssertionError('duplicate lane')
    marks['line_id']=marks.line_id.fillna('')
    idxs=sorted(marks.idx.unique());assert idxs==list(range(int(idxs[0]),int(idxs[-1])+1))
    for t,g in marks.groupby('idx'):
        if len(g)!=6:raise AssertionError('incomplete 6-lane day '+str(t))
    qmap={str(x['line_id']):x for x in catalog}
    for q in qmap.values():verify(q)
    shortlist=defaultdict(list)
    for r in top_rows.to_dict('records'):
        shortlist[(int(r['idx']),str(r['scale']),str(r['kind']))].append(str(r['line_id']))
    proposals={(int(r.idx),str(r.scale),str(r.kind)):r for r in marks.itertuples(index=False)}
    pxmap={int(g.idx.iloc[0]):float(g.close.iloc[0]) for _,g in marks.groupby('idx')}
    datemap={int(g.idx.iloc[0]):str(g.date.iloc[0]) for _,g in marks.groupby('idx')}
    rawmap={int(r['idx']):r for r in raw.to_dict('records')} if raw is not None else {}
    state={k:{'active':None,'since':None,'breaches':0,'retired':set(),'lockout':-1,
              'break_idx':-10**6,'watches':[]} for k in [(s,k) for s in POLICY for k in ['UPPER','LOWER']]}
    daily=[];changes=[];roles=[]
    for t in idxs:
        close=pxmap[t]
        for scale in POLICY:
            for kind in ['UPPER','LOWER']:
                lane=(scale,kind);ss=state[lane];prev=ss['active'];event='NONE';reason='';old_id=prev['line_id'] if prev else ''
                # Mature watches are graded using only this bar, never future bars.
                for watch in ss['watches']:
                    if t<=watch['break_idx'] or watch['state'] in ('CONFIRMED','FAILED','EXPIRED'):continue
                    if t-watch['break_idx']>RETEST_DAYS:
                        watch['state']='EXPIRED';continue
                    q=watch['line'];lv=level(q,t)
                    bar=rawmap.get(t)
                    if bar is None:
                        watch['state']='UNOBSERVABLE_OHLCV';continue
                    if not _near_candle(q,t,bar):continue
                    held=close>=lv if kind=='UPPER' else close<=lv
                    if not held:
                        watch['state']='FAILED'
                        roles.append({'code':stock,'algorithm':algorithm,'scale':scale,'original_kind':kind,
                           'break_date':watch['break_date'],'event_date':datemap[t], 'idx':t,
                           'line_id':q['line_id'],'role_event':'RETEST_FAILED','line_level':round(lv,4),
                           'close':close,'watch_status':'FAILED','when_known':'AT_EVENT_DAY_CLOSE'})
                    else:
                        if not watch['touches'] or t-watch['touches'][-1]>=2:watch['touches'].append(t)
                        watch['state']='CONFIRMED' if len(watch['touches'])>=2 else 'ONE_HELD'
                        roles.append({'code':stock,'algorithm':algorithm,'scale':scale,'original_kind':kind,
                           'break_date':watch['break_date'],'event_date':datemap[t], 'idx':t,
                           'line_id':q['line_id'],'role_event':'SECOND_RETEST_CONFIRMED' if watch['state']=='CONFIRMED' else 'FIRST_RETEST_HELD',
                           'line_level':round(lv,4),'close':close,'watch_status':watch['state'],
                           'when_known':'AT_EVENT_DAY_CLOSE'})
                # Yesterday's active line is the only existing line which can be broken.
                if prev:
                    lv=level(prev,t)
                    ss['breaches']=ss['breaches']+1 if beyond(close,lv,kind) else 0
                    expired=(t-int(prev['known_idx'])>POLICY[scale]['max_known_age'] or
                             t-int(prev['i2'])>POLICY[scale]['max_endpoint_age'] or
                             abs(100*(lv/close-1))>POLICY[scale]['max_gap'])
                    if ss['breaches']>=2:
                        event='TWO_CLOSE_BREAK_ARCHIVE';reason='TWO_CONSECUTIVE_CLOSES_OUTSIDE_EXACT_LINE'
                        ss['watches'].append({'line':prev,'break_idx':t,'break_date':datemap[t],
                           'touches':[],'state':'AWAIT_RETEST'})
                        ss['retired'].add(str(prev['line_id']));ss['active']=None;ss['breaches']=0
                        ss['break_idx']=t;ss['lockout']=t+1
                    elif expired and ss['breaches']==0:
                        event='EXPIRE_STALE_OR_FAR';reason='AGE_OR_DISTANCE_EXCEEDED'
                        ss['retired'].add(str(prev['line_id']));ss['active']=None
                        ss['lockout']=t+1
                    elif ss['breaches']:
                        event='FIRST_CLOSE_OUTSIDE_PENDING';reason='NOT_CONFIRMED_WAIT_SECOND_CLOSE'
                # Frozen R116 offers only post-close daily possibilities, not teacher labels.
                proposal=proposals[(t,scale,kind)];pool=[];ids=[]
                if isinstance(proposal.line_id,str) and proposal.line_id:ids.append(proposal.line_id)
                ids.extend(shortlist.get((t,scale,kind),[]))
                for lid in dict.fromkeys(ids):
                    q=qmap.get(lid)
                    if q is None:raise AssertionError('R116 proposes a line not in R111 catalog: '+lid)
                    if ss['break_idx']>-10**6 and int(q['known_idx'])<=ss['break_idx']:continue
                    if not admissible(q,t,close,scale,kind,ss['retired']):continue
                    pool.append((score(q,t,close),int(q['i2']),str(q['line_id']),q))
                pool.sort(key=lambda x:(x[0],x[1],x[2]),reverse=True)
                chosen=ss['active'];selected_score=None
                if chosen is None and t>ss['lockout'] and pool:
                    chosen=pool[0][3];ss['active']=chosen;ss['since']=t;ss['breaches']=0
                    if event=='NONE':event='ACTIVATE_NEW_CONFIRMED_LINE';reason='NO_PREVIOUS_ACTIVE_LINE'
                    selected_score=pool[0][0]
                elif chosen is not None and ss['breaches']==0 and pool and t-int(ss['since'])>=POLICY[scale]['min_hold']:
                    best=pool[0];oldscore=score(chosen,t,close)
                    if (best[2]!=chosen['line_id'] and
                        best[1]>=int(chosen['i2'])+POLICY[scale]['endpoint_advance'] and
                        best[0]>=oldscore+POLICY[scale]['score_margin']):
                        ss['retired'].add(str(chosen['line_id']))
                        ss['active']=best[3];chosen=best[3];ss['since']=t
                        event='REANCHOR_MAJOR_NEW_ENDPOINT';reason='ENDPOINT_ADVANCE_HOLD_AND_MARGIN'
                        selected_score=best[0]
                if chosen and selected_score is None:selected_score=score(chosen,t,close)
                if chosen:
                    verify(chosen)
                line_id=str(chosen['line_id']) if chosen else ''
                # Keep the strongest newly confirmed distinct line VISIBLE as
                # a WATCH, never override a stable active line merely to match
                # an illustrative example. A watch is not an executable signal.
                fresh_watch=next((x[3] for x in pool if x[2]!=line_id and
                    (not chosen or int(x[3]['i2'])>int(chosen['i2']))),None)
                rec={'code':stock,'algorithm':algorithm,'scale':scale,'kind':kind,
                     'idx':int(t),'date':datemap[t],'close':float(close),
                     'status':'PENDING_BREAK' if chosen and ss['breaches'] else ('ACTIVE' if chosen else 'WAIT_NEW_CONFIRMED_LINE'),
                     'event':event,'reason':reason,'line_id':line_id,'previous_line_id':old_id,
                     'selected_on_idx':ss['since'] if chosen else None,
                     'known_idx':int(chosen['known_idx']) if chosen else None,
                     'known_date':str(chosen['known_date']) if chosen else '',
                     'anchor1':str(chosen['a1']) if chosen else '',
                     'anchor2':str(chosen['a2']) if chosen else '',
                     'family':str(chosen['family']) if chosen else '',
                     'exact_level':round(level(chosen,t),4) if chosen else None,
                     'gap_pct':round(100*(level(chosen,t)/close-1),4) if chosen else None,
                     'selector_score':round(selected_score,5) if chosen else None,
                     'candidate_pool_size':len(pool),'breach_streak':ss['breaches'],
                     'fresh_watch_line_id':str(fresh_watch['line_id']) if fresh_watch else '',
                     'fresh_watch_anchor1':str(fresh_watch['a1']) if fresh_watch else '',
                     'fresh_watch_anchor2':str(fresh_watch['a2']) if fresh_watch else '',
                     'fresh_watch_level':round(level(fresh_watch,t),4) if fresh_watch else None,
                     'fresh_watch_status':'WATCH_ONLY_NOT_ACTIVE' if fresh_watch else 'NONE',
                     'r116_proposed_id':proposal.line_id if isinstance(proposal.line_id,str) else '',
                     'archived_unique_ids':len(ss['retired']),
                     'open_role_watches':sum(w['state'] not in ('FAILED','EXPIRED','CONFIRMED') for w in ss['watches']),
                     'when_known':'AFTER_T_CLOSE_ONLY','research_only':True}
                daily.append(rec)
                if event!='NONE':changes.append(rec.copy())
    return pd.DataFrame(daily),pd.DataFrame(changes),pd.DataFrame(roles)
