"""R1-D98 point-in-time trendline state classifier (research only).

No capital allocation, profit estimation, or order placement. Every state is
computed only using rows through asof. R1-D96 candidate geometry is NOT changed.
"""
from __future__ import annotations

from typing import Dict, List, Optional

# These thresholds are hypotheses for human/sensitivity review, not tuned returns.
CROSS_EPS = 0.003   # 0.3% close crossing deadband, matching R1-D96 convention
TOUCH_EPS = 0.01   # 1% permitted touch window, NOT support confirmation
CRASH_PCT = -12.0  # one-day inter-anchor collapse -> structural quarantine
WICK_PCT = 8.0     # second swing-high large upper wick -> quarantine
RETEST_MAX_AGE = 20  # days elapsed after breakout to search for touch


def line_at(r: dict, k: int) -> float:
    i, j = int(r['h1_idx']), int(r['h2_idx'])
    if j == i:
        raise ValueError('coincident anchors')
    return float(r['h1_value']) + (float(r['h2_value']) - float(r['h1_value'])) * (k - i) / (j - i)


def structural_flags(r: dict) -> List[str]:
    if r['kind'] != 'RESISTANCE':
        return ['STEEP_SUPPORT_CAUTION'] if r.get('steep_postshock_support', False) else []
    out = []
    if float(r.get('inter_anchor_largest_one_day_drop_pct', 0)) <= CRASH_PCT:
        out.append('CRASH_BRIDGE_REVIEW')
    if float(r.get('h2_upper_wick_pct', 0)) >= WICK_PCT:
        out.append('SECOND_ANCHOR_WICK_REVIEW')
    return out


def _asof_index(d, asof: str) -> int:
    # Avoid reliance on date library details in this pure state classifier.
    dates = d['date'].dt.strftime('%Y-%m-%d')
    loc = [int(k) for k, v in enumerate(dates) if v == str(asof)]
    if len(loc) != 1:
        raise ValueError(f'asof must occur exactly once: {asof}')
    return loc[0]


def evaluate(r: dict, d, asof: str, *, require_confirmed: bool = True) -> dict:
    """Classify a frozen R1-D96 candidate as of ONE observation date.

    Reject lines whose pivot H2 was not confirmed by the requested timestamp.
    First crossings are immutable line-specific events; old lines can never
    generate a second 'fresh' signal. Historical crossing is not recomputed
    using any candles after asof.
    """
    t = _asof_index(d, asof)
    conf = int(r['h2_confirm_idx'])
    if t < conf:
        if require_confirmed:
            raise ValueError('H2 pivot not yet confirmed as of requested day')
        return dict(status='NOT_YET_CONFIRMED',bucket='ARCHIVE',first_event_date='',last_event_date='',
                    structure_flags='',fresh_break_today=False,retest_touch_count=0,
                    eligible_primary=False,eligible_watch=False)
    if conf < 1:
        raise ValueError('H2 confirmation precedes index 1')
    if not (0 <= int(r['h1_idx']) < int(r['h2_idx']) < conf < len(d)):
        raise ValueError('invalid confirmed pivot indices')
    if t >= len(d):
        raise ValueError('date outside dataframe')
    flags = structural_flags(r)
    severe = 'CRASH_BRIDGE_REVIEW' in flags or 'SECOND_ANCHOR_WICK_REVIEW' in flags
    kind = str(r['kind'])
    if kind not in {'RESISTANCE', 'SUPPORT'}:
        raise ValueError(f'unknown kind {kind}')
    # If the line is already on the wrong side when H2 becomes confirmed,
    # it must never be presented as a pristine active line.
    confirmed_close = float(d.iloc[conf].close)
    confirmed_line = line_at(r,conf)
    prebroken = (confirmed_close > confirmed_line*(1+CROSS_EPS) if kind=='RESISTANCE'
                 else confirmed_close < confirmed_line*(1-CROSS_EPS))
    first_k: Optional[int] = conf if prebroken else None
    last_k: Optional[int] = conf if prebroken else None
    for k in range(conf+1, t+1):
        p, c = float(d.iloc[k-1].close), float(d.iloc[k].close)
        lp, lc = line_at(r, k-1), line_at(r, k)
        if kind == 'RESISTANCE':
            crossed = p <= lp*(1+CROSS_EPS) and c > lc*(1+CROSS_EPS)
        else:
            crossed = p >= lp*(1-CROSS_EPS) and c < lc*(1-CROSS_EPS)
        if crossed:
            if first_k is None:
                first_k = k
            last_k = k
    close = float(d.iloc[t].close)
    now = line_at(r, t)
    if now <= 0:
        raise ValueError('nonpositive extrapolated line')
    first_date = str(d.iloc[first_k].date.date()) if first_k is not None else ''
    last_date = str(d.iloc[last_k].date.date()) if last_k is not None else ''
    # A line cannot generate a fresh tradeable breakout on the pivot-confirm day.
    fresh = (first_k == t and first_k != conf)
    retests = 0
    last_retest_k: Optional[int] = None
    # A touch is only a watch marker, NOT proof of support conversion.
    if kind == 'RESISTANCE' and first_k is not None:
        for k in range(first_k+1, min(t,first_k+RETEST_MAX_AGE)+1):
            lv = line_at(r, k)
            q = d.iloc[k]
            touching = float(q.low) <= lv*(1+TOUCH_EPS) and float(q.high) >= lv*(1-TOUCH_EPS)
            if touching and float(q.close) >= lv*(1-CROSS_EPS):
                retests += 1
                last_retest_k = k
    if kind == 'RESISTANCE':
        if severe:
            state, bucket = 'STRUCTURE_QUARANTINE', 'ARCHIVE'
        elif first_k is None:
            state, bucket = ('ACTIVE_RESISTANCE', 'PRIMARY') if close<=now*(1+CROSS_EPS) else ('ABOVE_RESISTANCE_UNRESOLVED','WATCH')
        elif prebroken and first_k==conf and t==conf:
            state,bucket = 'ABOVE_AT_CONFIRMATION_REFERENCE','ARCHIVE'
        elif fresh:
            state,bucket = 'FIRST_BREAK_TODAY','WATCH'
        elif close < now*(1-CROSS_EPS):
            state,bucket = 'HISTORIC_RESISTANCE_BACK_BELOW','ARCHIVE'
        elif last_retest_k is not None and close>=now*(1-CROSS_EPS):
            state,bucket = 'BREAKOUT_RETEST_SUPPORT_FLIP_WATCH','WATCH'
        else:
            state,bucket = 'OLD_BREAK_ABOVE_NO_CONFIRMED_FLIP','WATCH'
    else:
        if prebroken and first_k==conf and t==conf:
            state,bucket = 'SUPPORT_BROKEN_AT_CONFIRMATION','ARCHIVE'
        elif first_k is None:
            state,bucket = ('ACTIVE_SUPPORT','PRIMARY') if close>=now*(1-CROSS_EPS) else ('SUPPORT_UNRESOLVED','WATCH')
        elif fresh:
            state,bucket = 'FIRST_SUPPORT_BREAK_TODAY','ARCHIVE'
        elif close<now*(1-CROSS_EPS):
            state,bucket = 'BROKEN_SUPPORT_BELOW','ARCHIVE'
        else:
            state,bucket = 'BROKEN_SUPPORT_RECLAIM_WATCH','WATCH'
    return {
        'status':state,'bucket':bucket,'first_event_date':first_date,'last_event_date':last_date,
        'structure_flags':'|'.join(flags),'fresh_break_today':bool(fresh and kind=='RESISTANCE' and not severe),
        'retest_touch_count':retests,
        'eligible_primary':bucket=='PRIMARY','eligible_watch':bucket=='WATCH',
        'line_now_unrounded':round(now,6),
        'support_flip_proven':False,
        'ca_policy':'RAW_OHLCV_CA_NOT_CERTIFIED',
    }


def enrich(candidates: List[dict], d, asof: str) -> List[dict]:
    return [dict(r, **evaluate(r,d,asof)) for r in candidates]


def pick(annotated: List[dict], *, per_group: int = 2, min_score: float = 16.0):
    """Separate primary and WATCH. Structural quarantine never enters either.

    Preserve original score and within-class order, expose not silently drop
    potential failures in the full annotated audit. Primary = not a buy signal.
    """
    out = {'PRIMARY':[],'WATCH':[]}
    for bucket in ('PRIMARY','WATCH'):
        for kind in ('RESISTANCE','SUPPORT'):
            for tier in ('SMALL','MIDDLE'):
                found = [r for r in annotated if r['bucket']==bucket and r['kind']==kind
                         and r['tier']==tier and float(r['rank_score'])>=min_score]
                if bucket=='WATCH':
                    priority={'FIRST_BREAK_TODAY':0,'BREAKOUT_RETEST_SUPPORT_FLIP_WATCH':1,
                              'BROKEN_SUPPORT_RECLAIM_WATCH':2,'OLD_BREAK_ABOVE_NO_CONFIRMED_FLIP':3}
                    found.sort(key=lambda x:(priority.get(x['status'],4),-float(x['rank_score']),str(x['line_id'])))
                else:
                    found.sort(key=lambda x:(-float(x['rank_score']),str(x['line_id'])))
                out[bucket].extend(found[:per_group])
    return out
