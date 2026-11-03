"""Causal active-line role transitions. Immutable R106 history remains untouched.

This is a descriptive, deliberately provisional four-lane state model, not a
trading rule, prediction, or a retroactively perfect ZigZag.
"""
from __future__ import annotations
from collections import defaultdict
import numpy as np
import pandas as pd

LANES=('UPPER_SHORT','UPPER_MEDIUM','LOWER_SHORT','LOWER_MEDIUM')

def _line_at(l,t):
    return float(l['v2'])+float(l['slope_krw_per_bar'])*(t-int(l['i2']))

def _lane(l):
    return f"{l['kind']}_{'SHORT' if int(l['i2'])-int(l['i1'])<=12 else 'MEDIUM'}"

def _candidate(l,close,t,max_gap_pct=9.0):
    level=_line_at(l,t)
    if level<=0 or abs(100*(close/level-1))>max_gap_pct:return False
    # A still-active upper line is resistance above price, lower line is support below price.
    # A line already broken on its creation date must not be treated as still-active.
    if l['kind']=='UPPER' and close>level*1.004:return False
    if l['kind']=='LOWER' and close<level*.996:return False
    if int(l['penetrations_1p2pct'])>2:return False
    # A very short line connecting adjacent same-direction candles is usually noise.
    if int(l['i2'])-int(l['i1'])<4:return False
    return True

def _priority(l,px,t):
    level=_line_at(l,t)
    age=t-int(l['known_idx']);gap=int(l['i2'])-int(l['i1'])
    # fixed geometric diagnostics, intentionally not fit against forward returns
    return (min(12,gap)*.8 + min(1.,float(l['anchor_strength']))*8
            - min(3, int(l['penetrations_1p2pct']))*6
            - min(30,max(0,age))*.27
            - min(15,abs(100*(px/level-1)))*.42)

def track_lines(fr:pd.DataFrame,immutable_lines:pd.DataFrame,*,start_idx=0,end_idx=None):
    """Emit one record for each stock-date/lane using ONLY current/prior data.

    A close beyond the current line retires its *active role*, not the archived line.
    Following that event a new line needs an independently confirmed new anchor.
    New line confirmation uses the original R106 known_idx and never future pivots.
    """
    d=fr.reset_index(drop=True)
    if end_idx is None:end_idx=len(d)-1
    lines=immutable_lines.to_dict('records')
    by_known=defaultdict(list)
    for l in lines:
        k=int(l['known_idx'])
        if k<len(d):by_known[k].append(l)
    recent={lane:[] for lane in LANES}
    active={lane:None for lane in LANES}
    last_break={lane:-1 for lane in LANES}
    rows=[]
    for t in range(0,end_idx+1):
        close=float(d.iloc[t].close)
        for l in by_known.get(t,[]):
            recent[_lane(l)].append(l)
        for lane in LANES:
            prev=active[lane]
            switch='UNCHANGED' if prev is not None else 'NO_ACTIVE_LINE'
            retired_id=None
            if prev is not None:
                line_level=_line_at(prev,t)
                prior_level=_line_at(prev,t-1) if t>0 else None
                prev_close=float(d.iloc[t-1].close) if t>0 else None
                # A crossing must happen after the line was known the prior day.
                upper=prev['kind']=='UPPER'
                if t>int(prev['known_idx']) and prior_level is not None:
                    broken=(prev_close<=prior_level*1.004 and close>=line_level*1.004) if upper else (
                        prev_close>=prior_level*.996 and close<=line_level*.996)
                else:broken=False
                if broken:
                    switch='RESISTANCE_BROKEN_UP' if upper else 'SUPPORT_BROKEN_DOWN'
                    retired_id=prev['line_id']
                    last_break[lane]=t
                    active[lane]=None
            # Candidate maintenance: preserve all originals but retain only 30-day view.
            recent[lane]=[l for l in recent[lane] if 0<=t-int(l['known_idx'])<=30]
            newly=[l for l in recent[lane] if int(l['known_idx'])==t and _candidate(l,close,t)]
            viable=[l for l in recent[lane] if _candidate(l,close,t)]
            if active[lane] is None:
                if last_break[lane]<0:
                    can=viable
                else:
                    # Must be newly known after a break; do not immediately resurrect an old line.
                    can=[l for l in viable if int(l['known_idx'])>last_break[lane]]
                if can and (switch not in ('RESISTANCE_BROKEN_UP','SUPPORT_BROKEN_DOWN')):
                    active[lane]=max(can,key=lambda l:(_priority(l,close,t),int(l['i2']),l['line_id']))
                    switch='NEW_ACTIVE_LINE' if last_break[lane]<0 else 'NEW_LINE_AFTER_BREAK'
                elif last_break[lane]>=0 and switch=='NO_ACTIVE_LINE':switch='WAIT_NEW_CONFIRMED_ANCHOR'
            elif newly:
                # Only a new second anchor can re-anchor, not an old line re-ranked on a later price.
                current=active[lane]
                improving=[l for l in newly if int(l['i2'])>int(current['i2']) and (
                    _priority(l,close,t)>=_priority(current,close,t)-2)]
                if improving:
                    new=max(improving,key=lambda l:(_priority(l,close,t),int(l['i2']),l['line_id']))
                    if new['line_id']!=current['line_id']:
                        active[lane]=new;retired_id=current['line_id'];switch='REANCHORED_NEW_PIVOT_OR_CANDLE'
            line=active[lane]
            if t<start_idx:continue
            level=_line_at(line,t) if line is not None else np.nan
            rows.append({'idx':t,'date':str(d.iloc[t].date.date()),'lane':lane,'state':switch,
                         'active_line_id':line['line_id'] if line else '',
                         'active_family':line['family'] if line else '',
                         'known_date':line['known_date'] if line else '',
                         'anchor1':line['a1'] if line else '',
                         'anchor2':line['a2'] if line else '',
                         'active_line_level':level,'active_line_slope_pct':float(line['slope_pct_per_bar']) if line else np.nan,
                         'close_gap_to_active_pct':100*(close/level-1) if np.isfinite(level) and level>0 else np.nan,
                         'retired_line_id':retired_id or '',
                         'prior_break_idx':last_break[lane] if last_break[lane]>=0 else np.nan,
                         'warning':'DESCRIPTIVE_PIT_NOT_TRADE_SIGNAL'})
    return pd.DataFrame(rows)
