"""R1-D114: strict full-window morphology + PIT structure lifecycle overlay.

No revisions to R112 trendline selections or R113 original geometries/events.
All classifications refer only to geometry generated at the historical asof.
No future returns or human-selected references can enter selection.
"""
from __future__ import annotations
import math
from collections import defaultdict
import numpy as np
import pandas as pd

PATTERNS=('HORIZONTAL_BOX','DESCENDING_CHANNEL','ASCENDING_CHANNEL',
          'SYMMETRICAL_TRIANGLE','FALLING_WEDGE','RISING_WEDGE')


def _f(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else float('nan')
    except (TypeError,ValueError):
        return float('nan')


def classify_strict(rec):
    """Classify a *past-only* R113 geometry by whole-window boundary movement.

    Research constants are predeclared, geometry-only, never return-optimized.
    A flat band must remain stable throughout the window, not only per bar.
    """
    t=int(rec['asof_idx']);st=int(rec['start_idx']);span=t-st
    up=_f(rec['upper_m']);down=_f(rec['lower_m'])
    up_end=up*t+_f(rec['upper_b']);dn_end=down*t+_f(rec['lower_b'])
    up_start=up*st+_f(rec['upper_b']);dn_start=down*st+_f(rec['lower_b'])
    w0=up_start-dn_start;w1=up_end-dn_end
    px=(up_end+dn_end)/2
    result={'strict_pattern':'UNCLASSIFIED','strict_eligible':False,'strict_reject_reason':'',
            'span_bars':span,'upper_drift_pct':None,'lower_drift_pct':None,
            'parallelism_width_frac':None,'contraction_frac':None,
            'end_height_pct':None,'apex_bars_from_asof':None,
            'strict_rule_version':'R1D114_GEOMETRY_FIXED_THRESHOLDS_V1'}
    if span<8 or not all(map(math.isfinite,(up,down,up_end,dn_end,up_start,dn_start))):
        result['strict_reject_reason']='SHORT_OR_NONFINITE';return result
    if min(w0,w1,px,dn_end)<=0:
        result['strict_reject_reason']='NONPOSITIVE_OR_CROSSED_BOUNDARIES';return result
    drift_u=100*up*span/px;drift_l=100*down*span/px
    parallel=abs((up-down)*span)/max(w0,w1)
    contraction=1-w1/w0
    height=100*w1/px
    result.update(upper_drift_pct=round(drift_u,5),lower_drift_pct=round(drift_l,5),
                  parallelism_width_frac=round(parallel,5),contraction_frac=round(contraction,5),
                  end_height_pct=round(height,5))
    apex=(w1/(down-up) if down>up else float('nan'))
    result['apex_bars_from_asof']=round(apex,3) if math.isfinite(apex) else None
    if not (.7<=height<=30):
        result['strict_reject_reason']='HEIGHT_OUTSIDE_SCOPE';return result
    if int(rec.get('upper_touch_count',0))<2 or int(rec.get('lower_touch_count',0))<2:
        result['strict_reject_reason']='TOO_FEW_INDEPENDENT_TOUCHES';return result
    if _f(rec.get('body_inside_fraction',0))<.62:
        result['strict_reject_reason']='POOR_BODY_CONTAINMENT';return result
    # Horizontal: boundary movement <= 2% of price, <= 30% of height.
    # This rejects tilted bands which look like channels, not flat ranges.
    horizontal=(max(abs(drift_u),abs(drift_l))<=2.0 and
                max(abs(up*span),abs(down*span))<=.30*max(w0,w1) and
                parallel<=.30)
    # Channels: both move materially in SAME direction; separation stable.
    descending=(drift_u<=-2.0 and drift_l<=-2.0 and parallel<=.36 and
                abs(contraction)<=.35)
    ascending=(drift_u>=2.0 and drift_l>=2.0 and parallel<=.36 and
               abs(contraction)<=.35)
    # Converging structures must have narrowed in actual history, and not
    # passed their geometry apex on the observation date.
    converged=(contraction>=.20 and down>up and apex>=2)
    tri=converged and drift_u<=-1.0 and drift_l>=1.0
    falling=converged and drift_u<=-2.0 and drift_l<=-1.0 and up<down
    rising=converged and drift_u>=1.0 and drift_l>=2.0 and down>up
    strict='UNCLASSIFIED'
    if horizontal:strict='HORIZONTAL_BOX'
    elif descending:strict='DESCENDING_CHANNEL'
    elif ascending:strict='ASCENDING_CHANNEL'
    elif tri:strict='SYMMETRICAL_TRIANGLE'
    elif falling:strict='FALLING_WEDGE'
    elif rising:strict='RISING_WEDGE'
    result['strict_pattern']=strict
    result['strict_eligible']=strict!='UNCLASSIFIED'
    result['strict_reject_reason']=('' if result['strict_eligible'] else 'NO_STRICT_SHAPE_MATCH')
    return result


def classify_all(snapshots):
    if snapshots.empty:return snapshots.copy()
    cols=['code','asof_idx','basis','pattern']
    for x in cols:
        if x not in snapshots:raise ValueError('missing R113 column '+x)
    records=[]
    for r in snapshots.to_dict('records'):
        rec=r.copy();rec.update(classify_strict(r));records.append(rec)
    return pd.DataFrame(records)


def replay_lifecycle(strict, events=None, *, hold_bars=5):
    """PIT state: retain a frozen structure across short candidate gaps.

    A confirmed boundary break (from a formation known on t-1) terminates
    the active regime. A missing daily R113 candidate alone does not mean a
    structural trend ended. Unrefreshed regimes expire after hold_bars.
    """
    if strict.empty:return pd.DataFrame()
    if hold_bars<1:raise ValueError('hold_bars must be positive')
    rows=[];count=defaultdict(int)
    breaks={}
    if events is not None and not events.empty:
        # Events must already be matched to their *original* prior formation.
        needed={'code','basis','idx','strict_pattern','strict_eligible','event'}
        if not needed.issubset(events.columns):raise ValueError('ungated event input')
        valid=events[events.strict_eligible & events.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN'])]
        for r in valid.to_dict('records'):
            breaks.setdefault((r['code'],r['basis'],int(r['idx'])),set()).add(r['strict_pattern'])
    for (code,basis),g in strict.sort_values(['asof_idx','strict_pattern','window']).groupby(['code','basis'],sort=True):
        active={};last_t=int(g.asof_idx.max());first_t=int(g.asof_idx.min())
        by_t={int(t):z for t,z in g.groupby('asof_idx',sort=True)}
        date_for={int(t):str(z.iloc[0]['asof']) for t,z in g.groupby('asof_idx')}
        for t in range(first_t,last_t+1):
            for typ,old in list(active.items()):
                end_reason=('ENDED_CONFIRMED_BOUNDARY_BREAK'
                    if typ in breaks.get((code,basis,t),set()) else
                    'ENDED_STALE_GEOMETRY' if t-int(old['last_seen'])>hold_bars else '')
                if end_reason:
                    rows.append({'code':code,'basis':basis,'asof_idx':t,
                        'date':date_for.get(t),'strict_pattern':typ,
                        'regime_id':old['id'],'lifecycle_event':end_reason,
                        'source_snapshot_asof':old['asof'],'prior_regime_id':None})
                    del active[typ]
            subset=by_t.get(t)
            if subset is None:continue
            for r in subset[subset.strict_eligible].sort_values(['geometry_score','window'],ascending=[False,False]).to_dict('records'):
                typ=r['strict_pattern'];current=active.get(typ)
                if current is not None and current.get('observed_t')==t:continue
                reason='UPDATED_SAME_STRUCTURE'
                if current is not None:
                    old=current['row']
                    up_old=_f(old['upper_m'])*t+_f(old['upper_b'])
                    low_old=_f(old['lower_m'])*t+_f(old['lower_b'])
                    height=max(1.,_f(r['upper_price'])-_f(r['lower_price']))
                    relative=max(abs(up_old-_f(r['upper_price'])),abs(low_old-_f(r['lower_price'])))/height
                    if relative>.35:
                        rows.append({'code':code,'basis':basis,'asof_idx':t,'date':r['asof'],
                            'strict_pattern':typ,'regime_id':current['id'],
                            'lifecycle_event':'ENDED_MAJOR_REANCHOR',
                            'source_snapshot_asof':old['asof'],'prior_regime_id':None})
                        current=None;reason='NEW_AFTER_MAJOR_REANCHOR'
                if current is None:
                    count[(code,basis,typ)]+=1
                    rid=f'{code}|{basis}|{typ}|{count[(code,basis,typ)]}'
                    if reason!='NEW_AFTER_MAJOR_REANCHOR':reason='NEW_CONFIRMED_STRUCTURE'
                    prior=None
                else:
                    rid=current['id'];prior=current['id']
                active[typ]={'id':rid,'row':r,'asof':r['asof'],
                             'observed_t':t,'last_seen':t}
                rows.append({'code':code,'basis':basis,'asof_idx':t,'date':r['asof'],
                             'strict_pattern':typ,'regime_id':rid,'lifecycle_event':reason,
                             'source_snapshot_asof':r['asof'],'prior_regime_id':prior})
    return pd.DataFrame(rows)


def event_strict_gate(events,strict):
    """Join each historical event ONLY to its own R113 formation snapshot.

    Keep rejected events (negative evidence). Never re-label historical
    formations with their later as-of geometries.
    """
    if events.empty:return events.copy()
    a=strict[['code','basis','pattern','asof_idx','strict_pattern','strict_eligible',
              'strict_reject_reason','upper_drift_pct','lower_drift_pct',
              'parallelism_width_frac','contraction_frac','end_height_pct']].copy()
    if a.duplicated(['code','basis','pattern','asof_idx']).any():
        raise ValueError('R113 duplicate pattern snapshots')
    out=events.merge(a,left_on=['code','basis','pattern','formed_idx'],
                     right_on=['code','basis','pattern','asof_idx'],how='left',validate='many_to_one')
    out['formation_matched']=out['asof_idx'].notna()
    out['strict_eligible']=out['strict_eligible'].fillna(False).astype(bool)
    out['strict_signal_status']=np.where(~out.formation_matched,'NO_MATCHING_PRIOR_FORMATION',
        np.where(out.strict_eligible,'STRICT_FORMATION_KNOWN_BEFORE_EVENT','MORPHOLOGY_REJECTED'))
    if (out.loc[out.formation_matched,'formed_idx']>=out.loc[out.formation_matched,'idx']).any():
        raise AssertionError('event uses same-day or future geometry')
    return out.drop(columns=['asof_idx'])
