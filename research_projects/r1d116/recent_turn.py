"""R1-D116 independent, causal recent-turn shortlist (RESEARCH_ONLY).

Consumes immutable R1-D111 candidate geometry and R1-D112 as-of-close marks;
it DOES NOT alter R1-D112 or turn human illustrations into training labels.
All decisions use preexisting candidate known_idx <= day idx and day close.
The selected line at t is known only AFTER the close of t.  A new candidate
at t may not be advertised as a preexisting t-day breakout trigger.
"""
from __future__ import annotations
from collections import defaultdict
import math
import pandas as pd

SCALES=('SHORT','MEDIUM','LONG')
SIDES=('UPPER','LOWER')
# Fixed geometry-only study parameters; NOT optimized against return labels.
RULES={
 'SHORT': dict(span_lo=3,span_hi=12,known_age=20,endpoint_age=14,gap=8.,min_hold=3,move_anchor=2,hysteresis=2.5),
 'MEDIUM':dict(span_lo=13,span_hi=35,known_age=35,endpoint_age=30,gap=11.,min_hold=4,move_anchor=4,hysteresis=3.0),
 'LONG':dict(span_lo=36,span_hi=150,known_age=90,endpoint_age=85,gap=17.,min_hold=6,move_anchor=8,hysteresis=4.0),
}


def line_level(q,t):
    return float(q['v1'])+float(q['m'])*(int(t)-int(q['i1']))


def scale_of(q):
    if 'PIVOT25' in str(q['family']):return 'LONG'
    span=int(q['span'])
    if span<13:return 'SHORT'
    if span<36:return 'MEDIUM'
    return 'LONG'


def validate_line(q):
    a,b=int(q['i1']),int(q['i2']);known=int(q['known_idx'])
    if not (0<=a<b<=known):raise AssertionError('future or reversed anchors '+str(q['line_id']))
    if not math.isclose(line_level(q,b),float(q['v2']),abs_tol=.02):
        raise AssertionError('line does not pass original two anchors '+str(q['line_id']))


def score_line(q,t,close):
    """Explanatory score, not win probability; no returns, no handpicked labels."""
    age=int(t)-int(q['known_idx']);endage=int(t)-int(q['i2'])
    v=line_level(q,t);gap=100*(v/close-1.)
    structural=(8.*float(q['anchor_strength'])
       +1.2*min(4,int(q['interior_body_touches']))
       +1.0*min(4,int(q['interior_wick_touches']))
       -1.5*float(q['mean_body_error_atr'])
       -3.*int(q['body_pen'])-1.5*int(q['wick_pen']))
    freshness=-.15*age-.4*endage
    distance=-.45*abs(gap)
    return round(structural+freshness+distance,6),round(structural,6),round(freshness,6),round(distance,6),round(gap,6),v


def acceptable(q,t,close,scale,kind):
    # Minimum one full bar after candidate confirmation before it can become
    # an ACTIVE decision. On its known date it is a post-close WATCH candidate.
    if str(q['kind'])!=kind or scale_of(q)!=scale or int(q['known_idx'])>=t:return False
    cfg=RULES[scale]
    if t-int(q['known_idx'])>cfg['known_age'] or t-int(q['i2'])>cfg['endpoint_age']:return False
    v=line_level(q,t)
    if v<=0:return False
    gap=100*(v/close-1.)
    if abs(gap)>cfg['gap']:return False
    # Known resistance below price is no longer a live resistance; instead it
    # can be tracked as a separate historical-break candidate in the audit.
    if kind=='UPPER' and gap<-.4:return False
    if kind=='LOWER' and gap>.4:return False
    return True


def replay_one(stock,algorithm,immutable_lines,source_marks,top_n=3):
    """Replay one stock/algorithm over actual full-history close dates.

    Returns (daily lanes, transition log, ranked shortlist).  No run-time human
    reference is passed to discovery or scoring.  Previous immutable data is
    never mutated.  DAY t is as-of-CLOSE, not usable before the closing auction.
    """
    cols=['idx','date','close']
    if source_marks[cols[:2]].duplicated().any():raise AssertionError('duplicated source idx/date')
    marks=source_marks[cols].sort_values('idx').reset_index(drop=True)
    if not marks.idx.is_monotonic_increasing or marks.idx.diff().dropna().ne(1).any():
        raise AssertionError('R112 source marks have missing days')
    start=int(marks.idx.iloc[0]);end=int(marks.idx.iloc[-1])
    by_known=defaultdict(list)
    for q in immutable_lines:
        if q['algorithm']!=algorithm:raise AssertionError('algorithm mismatch')
        validate_line(q)
        ti=int(q['known_idx'])
        if ti<=end:by_known[ti].append(q)
    by_lane=defaultdict(list)
    daily=[];events=[];shortlists=[];prior={}
    retired=defaultdict(set);cooldown_until=defaultdict(lambda:-1)
    # Initialize eligibility even if marks begin mid-history.
    for t in range(start,end+1):
        for q in by_known.get(t,[]):by_lane[(scale_of(q),str(q['kind']))].append(q)
        row=marks.iloc[t-start];px=float(row.close)
        if px<=0:raise AssertionError('nonpositive close')
        for scale in SCALES:
            for kind in SIDES:
                k=(scale,kind);valid=[];cfg=RULES[scale]
                # Prune candidates as their known-date eligibility is expired.
                by_lane[k]=[q for q in by_lane[k] if t-int(q['known_idx'])<=cfg['known_age']]
                for q in by_lane[k]:
                    if str(q['line_id']) in retired[k] or not acceptable(q,t,px,scale,kind):continue
                    total,structure,fresh,dist,gap,v=score_line(q,t,px)
                    valid.append((total,int(q['i2']),str(q['line_id']),q,structure,fresh,dist,gap,v))
                valid.sort(key=lambda u:(u[0],u[1],u[2]),reverse=True)
                best=valid[0] if valid else None
                prev=prior.get(k)
                was_active=bool(prev)
                old_id=prev['line_id'] if prev else None
                # Separation of discovery and activation: avoid selecting a
                # new line every day just because a fresh body endpoint is at
                # today's close.  Hold while the old line is still relevant.
                old_acceptable=bool(prev and acceptable(prev['original'],t,px,scale,kind))
                if prev and not old_acceptable:
                    # Once a line stops representing the current side, keep
                    # its ID archived; do not silently reactivate the same
                    # old anchor pair after a price oscillation.
                    retired[k].add(str(prev['line_id']))
                    cooldown_until[k]=t+1
                if t<=cooldown_until[k]:
                    best=None
                decision=''
                if old_acceptable:
                    qold=prev['original']
                    old_score=score_line(qold,t,px)[0]
                    can_rotate=bool(best and best[2]!=old_id and
                        t-int(prev['selected'])>=cfg['min_hold'] and
                        int(best[3]['i2'])>=int(qold['i2'])+cfg['move_anchor'] and
                        best[0]>=old_score+cfg['hysteresis'])
                    if can_rotate:
                        selected=best;selected_since=t
                        state='REANCHOR_RECENT_CONFIRMED'
                        decision='NEW_ENDPOINT_PLUS_SCORE_HYSTERESIS'
                    else:
                        # Preserve the original line without modifying its
                        # source geometry or inventing a different price.
                        ov=score_line(qold,t,px)
                        selected=(ov[0],int(qold['i2']),old_id,qold,ov[1],ov[2],ov[3],ov[4],ov[5])
                        selected_since=int(prev['selected'])
                        state='RECENT_LINE_HELD';decision='HYSTERESIS_OR_NO_NEW_ENDPOINT'
                elif best:
                    selected=best;selected_since=t
                    state='RECENT_LINE_REPLACED_AFTER_EXIT' if was_active else 'NEW_RECENT_LINE'
                    decision='PRIOR_NO_LONGER_ELIGIBLE' if was_active else 'NO_PRIOR_ACTIVE_LINE'
                else:
                    selected=None;selected_since=None
                    state='WAIT_NEW_RELEVANT_LINE'
                    decision='WAIT_AFTER_ARCHIVE' if t<=cooldown_until[k] else 'NO_ELIGIBLE_RECENT_CANDIDATE'
                best=selected
                new_id=best[2] if best else None
                # A previous day's selection is the only eligible previous-day
                # reference for a confirmed close-crossing event.
                crossing='NONE';prior_level=None
                if prev and t>start:
                    prior_line=prev['original']
                    previous_close=float(marks.iloc[t-start-1].close)
                    prior_level=line_level(prior_line,t)
                    previous_level=line_level(prior_line,t-1)
                    tol0=.004*previous_close;tol1=.004*px
                    if kind=='UPPER' and previous_close<=previous_level+tol0 and px>prior_level+tol1:
                        crossing='CROSSED_ABOVE_PREVIOUS_ACTIVE_RESISTANCE'
                    elif kind=='LOWER' and previous_close>=previous_level-tol0 and px<prior_level-tol1:
                        crossing='CROSSED_BELOW_PREVIOUS_ACTIVE_SUPPORT'
                rec={'code':stock,'algorithm':algorithm,'idx':t,'date':str(row.date),'close':px,
                     'scale':scale,'kind':kind,'status':state,'decision_reason':decision,
                     'line_id':new_id or '',
                     'anchor1':str(best[3]['a1']) if best else '',
                     'anchor2':str(best[3]['a2']) if best else '',
                     'family':str(best[3]['family']) if best else '',
                     'known_idx':int(best[3]['known_idx']) if best else None,
                     'known_date':str(best[3]['known_date']) if best else '',
                     'level':round(best[8],4) if best else None,
                     'gap_pct':round(best[7],4) if best else None,
                     'selector_score':best[0] if best else None,
                     'candidate_count':len(valid),'prior_line_id':old_id or '',
                     'prior_line_level_on_day':round(prior_level,4) if prior_level is not None else None,
                     'prior_line_cross':crossing,
                     'when_actionable':'AFTER_T_CLOSE_ONLY',
                     'limitations':'STUDY_SHORTLIST_NOT_PREDICTED_RETURN_OR_ORDER'}
                daily.append(rec)
                if state not in ('RECENT_LINE_HELD','WAIT_NEW_RELEVANT_LINE') or crossing!='NONE' or (prev and not best):
                    events.append(rec.copy())
                for rank,x in enumerate(valid[:top_n],start=1):
                    _,__,lid,q,structure,fresh,dist,gap,v=x
                    shortlists.append({'code':stock,'algorithm':algorithm,'idx':t,'date':str(row.date),
                     'scale':scale,'kind':kind,'rank':rank,'line_id':lid,'family':q['family'],
                     'anchor1':q['a1'],'anchor2':q['a2'],'known_date':q['known_date'],
                     'known_idx':int(q['known_idx']),'i1':int(q['i1']),'i2':int(q['i2']),
                     'v1':float(q['v1']),'v2':float(q['v2']),'m':float(q['m']),
                     'level':round(v,4),'gap_pct':round(gap,4),'selector_score':x[0],
                     'structure_component':structure,'freshness_component':fresh,
                     'price_distance_component':dist,
                     'not_actionable_before_close':True})
                prior[k]={'line_id':new_id,'original':best[3],'selected':selected_since} if best else None
    return pd.DataFrame(daily),pd.DataFrame(events),pd.DataFrame(shortlists)
