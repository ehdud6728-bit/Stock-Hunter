"""R1-D105: PIT-first, MA-independent multi-indicator event catalog.

An event is detected using the same/current and previous trading day only.
No future outcomes are accessed by detection, clustering or sequence ordering.
Selected event days are not selected on later returns. All raw events retained.
"""
from __future__ import annotations
import pandas as pd
import numpy as np

MA_MAIN=(5,20,60,112,224)
BBS=(20,30,40)
ENVS=(20,40)
CLOUDS=('ma_cloud_fast','ma_cloud_mid','ma_cloud_long','ma_cloud_all','ichimoku_current')
LINE_PREFIXES=('short_high_trend','short_low_trend','medium_high_trend','medium_low_trend')
SNAP_OFFSETS=(-20,-10,-5,-1,0)
OUTCOME_FIELDS=('forward20_close_pct_EXPOST','forward20_peak_pct_EXPOST','outcome_EXPOST',
                'is_sustained_surge','is_nonrise_control')


def _valid(v):
    return v is not None and pd.notna(v) and not (isinstance(v,str) and v in ('','NA','nan'))


def _pos(a,b):
    if not _valid(a) or not _valid(b):return None
    return 'ABOVE' if float(a)>float(b) else 'BELOW' if float(a)<float(b) else 'EQUAL'


def _append(events, row, family, signal, component, detail=''):
    events.append({'code':str(row['code']).zfill(6),'name':row['name'],
                   'idx':int(row['idx']),'date':str(row['date']),
                   'family':family,'signal':signal,'component':component,
                   'detail':detail,'event_id':f"{str(row['code']).zfill(6)}|{row['date']}|{family}|{component}|{signal}"})


def detect_events(fr:pd.DataFrame)->pd.DataFrame:
    """Catalog every event without any outcome access; all stock-date events kept."""
    need={'code','name','idx','date','close'}
    if not need.issubset(fr):raise ValueError('missing stock-date columns')
    if fr[['code','date']].duplicated().any():raise ValueError('duplicate stock-date')
    events=[]
    for code,stock in fr.groupby('code',sort=False):
        stock=stock.sort_values('idx').reset_index(drop=True)
        for i in range(1,len(stock)):
            cur=stock.iloc[i].to_dict();old=stock.iloc[i-1].to_dict()
            # Only contiguous observations can be compared as one-day changes.
            if int(cur['idx'])-int(old['idx'])!=1:continue
            for n in MA_MAIN:
                col=f'ma{n}_value'
                a,b=_pos(old['close'],old.get(col)),_pos(cur['close'],cur.get(col))
                if a=='BELOW' and b=='ABOVE':_append(events,cur,'MA','RECLAIM_UP',f'MA{n}')
                if a=='ABOVE' and b=='BELOW':_append(events,cur,'MA','LOSE_DOWN',f'MA{n}')
                slope=f'ma{n}_slope20_pct'
                a,b=_pos(old.get(slope),0),_pos(cur.get(slope),0)
                if a=='BELOW' and b=='ABOVE':_append(events,cur,'MA_SLOPE','TURN_POSITIVE',f'MA{n}')
                if a=='ABOVE' and b=='BELOW':_append(events,cur,'MA_SLOPE','TURN_NEGATIVE',f'MA{n}')
            for prefix in LINE_PREFIXES:
                key=prefix+'_validated_cross';flag=cur.get(key)
                # The frozen R104 detector emits CROSS only for a line known the prior bar.
                if flag=='CROSS_UP':_append(events,cur,'TRENDLINE','CROSS_UP',prefix,cur.get(prefix+'_line_id',''))
                if flag=='CROSS_DOWN':_append(events,cur,'TRENDLINE','CROSS_DOWN',prefix,cur.get(prefix+'_line_id',''))
                # New confirmed pivot must not be misread as a breakout.
                if cur.get(prefix+'_reanchored_today') is True or cur.get(prefix+'_reanchored_today')==True:
                    if _valid(cur.get(prefix+'_line_id')):
                        _append(events,cur,'TRENDLINE','REANCHOR',prefix,cur.get(prefix+'_line_id',''))
            for n in BBS:
                prefix=f'bb{n}'; key=prefix+'_position';a,b=old.get(key),cur.get(key)
                if a!=b and _valid(a) and _valid(b):
                    transitions={('INSIDE','ABOVE_UPPER'):'BREAK_UPPER',
                     ('BELOW_LOWER','INSIDE'):'REENTER_FROM_LOWER',
                     ('INSIDE','BELOW_LOWER'):'BREAK_LOWER',
                     ('ABOVE_UPPER','INSIDE'):'REENTER_FROM_UPPER',
                     ('BELOW_LOWER','ABOVE_UPPER'):'GAP_ACROSS_UP',
                     ('ABOVE_UPPER','BELOW_LOWER'):'GAP_ACROSS_DOWN'}
                    _append(events,cur,'BB',transitions.get((a,b),'POSITION_CHANGE'),f'BB{n}',f'{a}->{b}')
                v=f'{prefix}_width_delta5_pp';a,b=_pos(old.get(v),0),_pos(cur.get(v),0)
                if a=='BELOW' and b=='ABOVE':_append(events,cur,'BB_WIDTH','CONTRACT_TO_EXPAND',f'BB{n}')
                if a=='ABOVE' and b=='BELOW':_append(events,cur,'BB_WIDTH','EXPAND_TO_CONTRACT',f'BB{n}')
            for n in ENVS:
                prefix=f'env{n}_10';key=prefix+'_position';a,b=old.get(key),cur.get(key)
                if a!=b and _valid(a) and _valid(b):
                    transitions={('INSIDE','ABOVE_UPPER'):'BREAK_UPPER',
                     ('BELOW_LOWER','INSIDE'):'REENTER_FROM_LOWER',
                     ('INSIDE','BELOW_LOWER'):'BREAK_LOWER',
                     ('ABOVE_UPPER','INSIDE'):'REENTER_FROM_UPPER',
                     ('BELOW_LOWER','ABOVE_UPPER'):'GAP_ACROSS_UP',
                     ('ABOVE_UPPER','BELOW_LOWER'):'GAP_ACROSS_DOWN'}
                    _append(events,cur,'ENV',transitions.get((a,b),'POSITION_CHANGE'),f'ENV{n}',f'{a}->{b}')
                key=prefix+'_width_change5_pct';a,b=_pos(old.get(key),0),_pos(cur.get(key),0)
                if a=='BELOW' and b=='ABOVE':_append(events,cur,'ENV_WIDTH','SHRINK_TO_GROW_ABSOLUTE',f'ENV{n}')
                if a=='ABOVE' and b=='BELOW':_append(events,cur,'ENV_WIDTH','GROW_TO_SHRINK_ABSOLUTE',f'ENV{n}')
            for cloud in CLOUDS:
                key=cloud+'_position';a,b=old.get(key),cur.get(key)
                if _valid(a) and _valid(b) and a!=b:
                    _append(events,cur,'CLOUD','POSITION_CHANGE',cloud,f'{a}->{b}')
                key=cloud+'_thickness_change5_pp';a,b=_pos(old.get(key),0),_pos(cur.get(key),0)
                if a=='BELOW' and b=='ABOVE':_append(events,cur,'CLOUD_WIDTH','CONTRACT_TO_EXPAND',cloud)
                if a=='ABOVE' and b=='BELOW':_append(events,cur,'CLOUD_WIDTH','EXPAND_TO_CONTRACT',cloud)
                key=cloud+'_height_change5_pp';a,b=_pos(old.get(key),0),_pos(cur.get(key),0)
                if a=='BELOW' and b=='ABOVE':_append(events,cur,'CLOUD_HEIGHT','FALL_TO_RISE',cloud)
                if a=='ABOVE' and b=='BELOW':_append(events,cur,'CLOUD_HEIGHT','RISE_TO_FALL',cloud)
            a,b=old.get('structure'),cur.get('structure')
            if _valid(a) and _valid(b) and a!=b:
                _append(events,cur,'STRUCTURE','CHANGE','DUAL_PIVOT',f'{a}->{b}')
            a,b=_pos(old.get('amount_vs_prev20'),1),_pos(cur.get('amount_vs_prev20'),1)
            if a=='BELOW' and b=='ABOVE':_append(events,cur,'AMOUNT','ABOVE_PRIOR_AVG','AMOUNT20')
            if a=='ABOVE' and b=='BELOW':_append(events,cur,'AMOUNT','BELOW_PRIOR_AVG','AMOUNT20')
    cols=['code','name','idx','date','family','signal','component','detail','event_id']
    out=pd.DataFrame(events,columns=cols)
    if out.event_id.duplicated().any():raise AssertionError('duplicate event_id')
    return out.sort_values(['code','idx','family','component','signal']).reset_index(drop=True)


def unique_event_dates(catalog:pd.DataFrame,context:pd.DataFrame,refs=None):
    """Union of ALL families (not conditioned on moving-average events)."""
    cols=['code','date','idx']
    rows=[]
    if len(catalog):
        rows.append(catalog[cols].drop_duplicates().assign(anchor_type='DETECTED_EVENT'))
    if refs:
        for code,date in refs:
            found=context[context.code.astype(str).str.zfill(6).eq(code) & context.date.eq(date)]
            if len(found)!=1:raise ValueError('reference not eligible '+code+' '+date)
            rows.append(found[cols].assign(anchor_type='FIXED_REFERENCE'))
    if not rows:return pd.DataFrame(columns=cols+['anchor_type'])
    anchors=pd.concat(rows,ignore_index=True).sort_values(['code','idx','anchor_type'])
    anchors=anchors.drop_duplicates(cols,keep='last').reset_index(drop=True)  # refs take precedence
    return anchors


def collect_snapshots(context:pd.DataFrame,anchors:pd.DataFrame,offsets=SNAP_OFFSETS):
    # Context MUST contain historical lookback >=20 trading days for each anchor.
    snapshots=[]
    for code,g in context.groupby('code'):
        lookup={int(row['idx']):row for row in g.to_dict('records')}
        selected=anchors[anchors.code.eq(code)]
        for anchor in selected.to_dict('records'):
            for lag in offsets:
                row=lookup.get(int(anchor['idx'])+lag)
                if row is None:continue
                item={'code':code,'name':anchor.get('name',row.get('name')),
                      'anchor_date':anchor['date'],'anchor_type':anchor['anchor_type'],
                      'relative_bar':lag,'observation_date':row['date'],
                      'idx':int(row['idx'])}
                for col in context.columns:
                    if col not in ('code','name','idx','date') and not col.endswith('_EXPOST') and col not in OUTCOME_FIELDS:
                        item[col]=row[col]
                snapshots.append(item)
    return pd.DataFrame(snapshots)


def episode_anchors(catalog, *,bar_separation=20):
    """Chronological 20-trading-day anchor cooldown, never outcome-dependent."""
    if bar_separation<1:raise ValueError('bar_separation')
    episodes=[]
    for code,g in catalog.groupby('code'):
        daily=g.groupby('idx',sort=True)
        chosen_last=-10**8
        for idx,sub in daily:
            if int(idx)-chosen_last<bar_separation:continue
            chosen_last=int(idx)
            row=sub.iloc[0]
            episodes.append({'code':code,'name':row['name'],'idx':int(idx),'date':row['date'],
                             'event_count_today':len(sub),
                             'families_today':'|'.join(sorted(sub.family.unique())),
                             'bar_separation':bar_separation})
    return pd.DataFrame(episodes)


def attach_later_outcomes(anchors,context):
    """Use labels ONLY after anchor inventory is frozen."""
    labels=context[['code','date','forward20_close_pct_EXPOST','forward20_peak_pct_EXPOST','outcome_EXPOST']]
    if labels[['code','date']].duplicated().any():raise ValueError('duplicate labels')
    return anchors.merge(labels,on=['code','date'],validate='one_to_one',how='left')


def episode_sequence(catalog,episode_anchors_frame,*,before=20,after=20):
    """D-minus→D-plus sequence with POST marked as EXPOST, never a trigger."""
    entries=[]
    for rec in episode_anchors_frame.to_dict('records'):
        code=rec['code'];anchor_idx=int(rec['idx'])
        nearby=catalog[(catalog.code.eq(code)) & (catalog.idx.between(anchor_idx-before,anchor_idx+after))]
        for e in nearby.to_dict('records'):
            relative=int(e['idx'])-anchor_idx
            entries.append({**e,'episode_anchor_date':rec['date'],
                'relative_bar':relative,'known_at_anchor':relative<=0,
                'time_side':'PIT_HISTORY' if relative<=0 else 'POST_EXPOST_ONLY'})
    return pd.DataFrame(entries).sort_values(['code','episode_anchor_date','relative_bar','family','component']).reset_index(drop=True) if entries else pd.DataFrame()
