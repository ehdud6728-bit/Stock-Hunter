#!/usr/bin/env python3
"""R1.8 causal pre-entry waveform / outcome anatomy / entry-policy forensic, RESEARCH ONLY.

Never changes LIVE scanner. Original source: R1.7.1 selected top5, 2019~2026 KRX
historical day-cross-sectional immutable artifacts. Tested only within prepared 907-code
universe. Aftermarket, PIT universe, fills remain unresolved. Not a trade engine.
"""
from __future__ import annotations
import argparse
import hashlib
import io
import json
from pathlib import Path
import zipfile

import numpy as np
import pandas as pd

BAR_FIELDS = ['open','high','low','close','volume','amount']
INPUT_NAME = 'r17_krx/native/v49_76_global_selected_top5.csv'


def as_date(x):
    return pd.Timestamp(x).strftime('%Y-%m-%d')


def load_selected(path):
    path=Path(path)
    if path.suffix=='.zip':
        with zipfile.ZipFile(path) as z:
            choices=[n for n in z.namelist() if n.endswith('v49_76_global_selected_top5.csv')]
            if not choices:raise RuntimeError('NATIVE_SELECTED_NOT_IN_ARTIFACT')
            with z.open(INPUT_NAME if INPUT_NAME in choices else choices[0]) as f:
                a=pd.read_csv(f,dtype={'code':str},low_memory=False)
    else:a=pd.read_csv(path,dtype={'code':str},low_memory=False)
    needed={'signal_date','code','mode','entry_price','_open','_high','_low','_close','_vol'}
    if not needed.issubset(a.columns):raise RuntimeError(f'NATIVE_MISSING:{needed-set(a.columns)}')
    a['code']=a.code.str.zfill(6)
    if a[['signal_date','code','mode']].duplicated().any():raise RuntimeError('NATIVE_DUPLICATE_MODE_SIGNAL')
    for x in ['entry_price','_open','_high','_low','_close','_vol']:
        a[x]=pd.to_numeric(a[x],errors='raise')
    g=a.groupby(['signal_date','code'])[['entry_price','_open','_high','_low','_close','_vol']].nunique()
    if (g>1).any().any():raise RuntimeError('CONFLICTING_SIGNAL_BAR_BY_MODE')
    modes=a.groupby(['signal_date','code'])['mode'].apply(lambda x:'|'.join(sorted(set(x)))).rename('all_modes')
    s=a.sort_values(['signal_date','code','mode']).drop_duplicates(['signal_date','code']).copy()
    s=s.merge(modes.reset_index(),on=['signal_date','code'],validate='one_to_one')
    return a,s


def _list_artifacts(root):
    p=Path(root)
    if p.is_file() and p.suffix=='.zip':return [p]
    zips=sorted(p.glob('github-actions-artifact-119*.zip'))+sorted(p.glob('r171-history-*.zip'))
    zips=list(dict.fromkeys(zips))
    zips=[z for z in zips if any('krx_history_manifest_' in n for n in zipfile.ZipFile(z).namelist())]
    if zips:return zips
    if list(p.rglob('*.csv.gz')):return [p]
    raise RuntimeError(f'KRX_HISTORY_ARTIFACTS_MISSING:{p}')


def load_history(root, codes, earliest, asof):
    archives=_list_artifacts(root)
    print(f'R18_HISTORY_ARCHIVES {len(archives)}',flush=True)
    # Manifests bind sessions and sha256 (no self-asserted unverified CSVs)
    all_specs={}; inputs=[]; calendar=set(); official_calendar_sha=set(); asofs=set(); formats=set()
    for arch in archives:
        if arch.suffix=='.zip':
            with zipfile.ZipFile(arch) as z:
                manifests=[n for n in z.namelist() if n.endswith('.json') and 'krx_history_manifest' in n]
                if len(manifests)!=1:raise RuntimeError(f'HISTORY_MANIFEST_INVALID:{arch}:{manifests}')
                meta=json.loads(z.read(manifests[0]))
        else:
            manifests=list(arch.rglob('krx_history_manifest_*.json'))
            if len(manifests)!=4:raise RuntimeError('EXPECTED_FOUR_HISTORY_MANIFESTS')
            # A single directory with 4 manifests: traverse only once
            meta=None
            for mf in manifests:
                m=json.loads(mf.read_text())
                official_calendar_sha.add(m['official_calendar_sha256']);asofs.add(m['asof']);formats.add(m['format'])
                for r in m['files']:
                    if r['day'] in all_specs:raise RuntimeError('HISTORY_DAY_DUPLICATED')
                    all_specs[r['day']] = (arch,r['file'],r['sha256'])
                    calendar.add(r['day'])
            inputs.append(str(arch));break
        official_calendar_sha.add(meta['official_calendar_sha256']);asofs.add(meta['asof']);formats.add(meta['format'])
        inputs.append(str(arch))
        for r in meta['files']:
            if r['day'] in all_specs:raise RuntimeError('HISTORY_DAY_DUPLICATED')
            all_specs[r['day']] = (arch,r['file'],r['sha256'])
            calendar.add(r['day'])
    if len(official_calendar_sha)!=1 or len(asofs)!=1 or formats!={'R171_DATED_KRX_DAILY_V1'}:
        raise RuntimeError('HISTORY_MANIFEST_SOURCE_INCONSISTENT')
    if min(asofs)!=asof:raise RuntimeError(f'HISTORY_ASOF_MISMATCH:{asofs}:{asof}')
    if len(calendar)<1800:raise RuntimeError(f'HISTORY_CALENDAR_INCOMPLETE:{len(calendar)}')
    days=sorted(d for d in calendar if earliest<=d<=asof)
    if not days:raise RuntimeError('NO_KRX_HISTORY_DAYS')
    from collections import defaultdict
    per_code=defaultdict(list);loaded_n=0
    pools={p:zipfile.ZipFile(p) for p in archives if p.suffix=='.zip'}
    try:
        for i,day in enumerate(days,1):
            if day not in all_specs:raise RuntimeError('MISSING_DAY_IN_MANIFEST')
            arch,filename,sha=all_specs[day]
            if arch.suffix=='.zip': buf=pools[arch].read(filename)
            else:
                matches=list(arch.rglob(filename))
                if len(matches)!=1:raise RuntimeError(f'GZIP_FILE_MISSING:{filename}')
                buf=matches[0].read_bytes()
            if hashlib.sha256(buf).hexdigest()!=sha:raise RuntimeError(f'KRX_SNAPSHOT_SHA_INVALID:{day}')
            t=pd.read_csv(io.BytesIO(buf),compression='gzip',dtype={'code':str},usecols=['code','date',*BAR_FIELDS])
            if not t.empty:
                if t.code.duplicated().any() or not t.date.eq(day).all():raise RuntimeError(f'HISTORY_DAY_KEYS_INVALID:{day}')
                t=t[t.code.isin(codes)]
                for x in t.itertuples(index=False):per_code[x.code].append((day,x.open,x.high,x.low,x.close,x.volume,x.amount))
                loaded_n+=len(t)
            if i%150==0:print(f'R18_HISTORY_CHECKED {i}/{len(days)} linked={loaded_n}',flush=True)
    finally:
        for z in pools.values():z.close()
    hist={}
    for code,arr in per_code.items():
        d=pd.DataFrame(arr,columns=['date',*BAR_FIELDS]);d['date']=pd.to_datetime(d['date']);hist[code]=d.set_index('date').sort_index()
    if not hist:raise RuntimeError('KRX_HISTORY_EMPTY')
    return hist,pd.DatetimeIndex(pd.to_datetime(sorted(calendar))),{'history_artifacts':inputs,'historical_sessions_total':len(calendar),'history_loaded_sessions':len(days),'rows_linked':loaded_n,'unique_codes_loaded':len(hist),'official_calendar_sha256':next(iter(official_calendar_sha))}


def close_224(s):
    if len(s)<224:return np.nan
    return float(s.iloc[-224:].mean())


def value(x):
    return float(x) if np.isfinite(x) else np.nan


def make_prewave(d, pos):
    """Only bars <= signal day pos may influence output. Pivot window fixed ex ante.

    A candidate peak in [t-24,t-3], preceding trough [peak-35,peak-3].
    Pullback after peak through t-1. This is a historical set-up description,
    NOT first-wave truth or a causally known eventual outcome label.
    """
    y={'ma224_signal':np.nan,'ma224_at_trough':np.nan,'prewave_trough_below224':False,
       'prewave_impulse_pct':np.nan,'prewave_pullback_pct':np.nan,'prewave_retrace_fraction':np.nan,
       'prewave_impulse_vol_ratio':np.nan,'prewave_pullback_vol_ratio':np.nan,
       'prewave_prevolume_accum_ratio':np.nan,'wave_peak_days_ago':np.nan,
       'wave_trough_days_ago':np.nan,'wave_exists':False,'wave_setup_stage':0,
       'wave_stage_label':'INSUFFICIENT_BARS','ma224_dist_signal_pct':np.nan,'volume_dryup_10_30':np.nan,
       'candle_close_location':np.nan,'ma20_signal_dist_pct':np.nan,'prior_224_jump_above35pct':False}
    if pos<300:return y
    close=d.close;low=d.low;high=d.high;vol=d.volume
    ma224=close.iloc[pos-223:pos+1].mean();vclose=float(close.iloc[pos]);y['ma224_signal']=ma224
    y['ma224_dist_signal_pct']=(vclose/ma224-1)*100 if ma224>0 else np.nan
    y['ma20_signal_dist_pct']=(vclose/close.iloc[pos-19:pos+1].mean()-1)*100
    y['volume_dryup_10_30']=vol.iloc[pos-10:pos].mean()/vol.iloc[pos-30:pos].mean() if vol.iloc[pos-30:pos].mean()>0 else np.nan
    day=d.iloc[pos]; y['candle_close_location']=(vclose-day.low)/(day.high-day.low) if day.high>day.low else np.nan
    y['prior_224_jump_above35pct']=bool((close.pct_change().iloc[pos-223:pos+1].abs()>0.35).any())
    pstart=max(36,pos-24); pend=pos-2
    if pend<=pstart:return y
    peak_pos=int(np.nanargmax(high.iloc[pstart:pend].to_numpy()))+pstart
    if peak_pos<36:return y
    trough_end=peak_pos-2;trough_start=max(224,peak_pos-35)
    if trough_end<=trough_start:return y
    low_pos=int(np.nanargmin(low.iloc[trough_start:trough_end].to_numpy()))+trough_start
    if low_pos<223 or peak_pos>=pos-1:return y
    tl=float(low.iloc[low_pos]); ph=float(high.iloc[peak_pos])
    if tl<=0 or ph<=tl:return y
    m_at_low=float(close.iloc[low_pos-223:low_pos+1].mean())
    y['ma224_at_trough']=m_at_low
    y['prewave_trough_below224']=bool(close.iloc[low_pos]<m_at_low)
    y['wave_trough_days_ago']=int(pos-low_pos)
    y['wave_peak_days_ago']=int(pos-peak_pos)
    y['prewave_impulse_pct']=(ph/tl-1)*100
    pb_start=peak_pos+1;pb_end=pos
    if pb_end<=pb_start:return y
    pb_low=float(low.iloc[pb_start:pb_end].min())
    y['prewave_pullback_pct']=(pb_low/ph-1)*100
    y['prewave_retrace_fraction']=(ph-pb_low)/(ph-tl)
    wave_vol=float(vol.iloc[max(low_pos,peak_pos-4):peak_pos+1].mean())
    base_vol=float(vol.iloc[max(0,low_pos-25):low_pos-5].mean())
    pull_vol=float(vol.iloc[pb_start:pb_end].mean())
    y['prewave_impulse_vol_ratio']=wave_vol/base_vol if base_vol>0 else np.nan
    y['prewave_pullback_vol_ratio']=pull_vol/wave_vol if wave_vol>0 else np.nan
    early_vol=float(vol.iloc[low_pos-10:low_pos].mean());older_vol=float(vol.iloc[low_pos-35:low_pos-10].mean())
    y['prewave_prevolume_accum_ratio']=early_vol/older_vol if older_vol>0 else np.nan
    y['wave_exists']=True
    y['wave_stage_label']='PIVOTS_ONLY'
    if y['prewave_trough_below224']:
        y['wave_setup_stage']=1;y['wave_stage_label']='BELOW224_PREWAVE'
        if y['prewave_impulse_pct']>=8 and y['prewave_impulse_vol_ratio']>=1.2:
            y['wave_setup_stage']=2;y['wave_stage_label']='FIRST_WAVE_VOLUME'
            if 0.20<=y['prewave_retrace_fraction']<=0.80 and 3<=pos-peak_pos<=16:
                y['wave_setup_stage']=3;y['wave_stage_label']='PULLBACK_GEOMETRY'
                if y['prewave_pullback_vol_ratio']<=0.70:
                    y['wave_setup_stage']=4;y['wave_stage_label']='PULLBACK_VOLUME_DRY'
    return y


def get_market_forward(d,calendar,signal_day,horizon=20):
    """Calendar-aligned future KRX market sessions; no skipping suspension dates."""
    j=calendar.get_indexer([signal_day])[0]
    if j<0:raise RuntimeError('SIGNAL_NOT_IN_CALENDAR')
    return d.iloc[j+1:j+1+horizon]


def find_outcome(fr,entry,horizon):
    q=fr.iloc[:horizon]
    if len(q)!=horizon or q[BAR_FIELDS].isna().any().any():return {'complete':False}
    h=q.high.max();l=q.low.min();end=q.close.iloc[-1]
    def first(column,pred):
        z=np.flatnonzero(pred(q[column].to_numpy(dtype=float)))
        return int(z[0]+1) if len(z) else None
    return {'complete':True,'close_pct':(end/entry-1)*100,'mfe_pct':(h/entry-1)*100,
            'mae_pct':(l/entry-1)*100,'first5':first('high',lambda x:x>=entry*1.05),
            'first10':first('high',lambda x:x>=entry*1.10)}


def entry_trigger(d, calendar, day, entry_mode):
    j=calendar.get_indexer([day])[0];history=d
    if j<0:return None,None,'CALENDAR_MISSING'
    if entry_mode=='NEXT_OPEN':return j+1,'D1_OPEN','TRIGGER'
    # Decision at the *close* of day k; earliest execution NEXT market open.
    for k in range(1,4):
        if j+k>=len(calendar)-1:return None,None,'EVAL_CENSORED'
        now=history.iloc[j+k];prev=history.iloc[j+k-1];start=history.iloc[j]
        if now[BAR_FIELDS].isna().any() or prev[BAR_FIELDS].isna().any():return None,None,'BARS_MISSING'
        if now.volume<=0:continue
        if entry_mode=='BREAKOUT_CONFIRMED':
            cond=now.close>start.high and now.close>now.open
        elif entry_mode=='PULLBACK_RECLAIM':
            # Only confirmed at close, after a dip and bullish recovery.
            sub=history.iloc[j+1:j+k+1]
            cond=sub.low.min()<=start.close*0.985 and now.close>now.open and now.close>prev.close
        elif entry_mode=='MA20_RECOVERY':
            h=history.iloc[j+k-19:j+k+1]
            if len(h)<20 or h.close.isna().any():continue
            cond=now.close>h.close.mean() and now.close>now.open
        else:raise ValueError('ENTRY_MODE_UNKNOWN')
        if cond:return j+k+1,'D'+str(k+1)+'_OPEN','TRIGGER'
    return None,None,'NO_SIGNAL'


def simulate_five_sessions(d,calendar,signal_day,entry_mode,tp=.05,sl=.05,cost=.005):
    eidx,label,status=entry_trigger(d,calendar,signal_day,entry_mode)
    base={'entry_policy':entry_mode,'filled':0,'reason':status,'entry_date':'','entry_label':label or '',
          'entry_price':np.nan,'net_pct':np.nan,'win':np.nan,'duration':np.nan,'signal_level_net_pct':0.,'censored':0}
    if eidx is None:return base
    if eidx>=len(calendar):base['reason']='EVAL_CENSORED';base['censored']=1;return base
    if eidx+5>len(calendar):base['reason']='EVAL_CENSORED';base['censored']=1;return base
    q=d.iloc[eidx:eidx+5]
    if q[BAR_FIELDS].isna().any().any():base['reason']='BARS_MISSING';return base
    if (q.volume<=0).any() or (q.open<=0).any():base['reason']='UNTRADEABLE_ZERO_VOLUME_OR_OPEN';return base
    entry=float(q.open.iloc[0]);exitp=None;reason='TIME_EXIT';duration=5
    for i,r in enumerate(q.itertuples(index=False)):
        # New session open gap pricing takes precedence; intraday both-touch stop first.
        if r.open<=entry*(1-sl) and i>0:
            exitp=float(r.open);reason='GAP_STOP';duration=i+1;break
        if r.open>=entry*(1+tp) and i>0:
            exitp=float(r.open);reason='GAP_TP';duration=i+1;break
        if r.low<=entry*(1-sl):
            exitp=entry*(1-sl);reason='STOP_FIRST_OR_ONLY';duration=i+1;break
        if r.high>=entry*(1+tp):
            exitp=entry*(1+tp);reason='TP_FIRST_OR_ONLY';duration=i+1;break
    if exitp is None:exitp=float(q.close.iloc[-1])
    net=(exitp/entry-1-cost)*100
    return dict(base,filled=1,reason=reason,entry_date=str(calendar[eidx].date()),entry_price=entry,
                net_pct=net,win=int(net>0),duration=duration,signal_level_net_pct=net)


def tabulate(df, group, metrics):
    out=[]
    for label,g in df.groupby(group,dropna=False):
        row={group:str(label),'n':len(g)}
        for x in metrics:
            col=g[x]
            if x.startswith('is_') or x.endswith('_flag'):
                row[x+'_pct']=float(col.mean()*100)
            else:
                row[x+'_median']=float(col.median()) if col.notna().any() else None
        out.append(row)
    return pd.DataFrame(out)


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--selected',required=True,help='R1.7.1 selected CSV or published result artifact ZIP')
    p.add_argument('--history-artifacts',required=True,help='Folder containing four r171-history ZIPs or four extracted manifests')
    p.add_argument('--asof',default='2026-10-12')
    p.add_argument('--history-analysis-start',default='2025-01-02')
    p.add_argument('--out',default='reports/r18')
    p.add_argument('--dev-limit',type=int,default=0)
    args=p.parse_args();out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    events,unique=load_selected(args.selected)
    if unique.signal_date.max()>args.asof:raise RuntimeError('FUTURE_SIGNAL_DATE')
    hist,calendar,evidence=load_history(args.history_artifacts,set(unique.code),args.history_analysis_start,args.asof)
    if calendar.max()>pd.Timestamp(args.asof):raise RuntimeError('FUTURE_HISTORY_CALENDAR')
    calendar=calendar[(calendar>=pd.Timestamp(args.history_analysis_start)) & (calendar<=pd.Timestamp(args.asof))]
    hist={c:d.reindex(calendar) for c,d in hist.items()}
    # Main study is complete D20 cases + recently matured D5 OOS only.
    # Censored intermediate cases remain preserved in upstream R171 artifact.
    if not args.dev_limit:
        last20=calendar[-21];last5=calendar[-6]
        dates=pd.to_datetime(unique.signal_date)
        unique=unique[(dates<=last20) | ((dates>=pd.Timestamp('2026-09-21'))&(dates<=last5))].copy()
    else:unique=unique.head(args.dev_limit)
    rows=[];trades=[];mismatched=0
    for i,(_,r) in enumerate(unique.iterrows(),1):
        sig=pd.Timestamp(r.signal_date);d=hist.get(r.code)
        if d is None or sig not in d.index:raise RuntimeError(f'NO_KRX_SIGNAL_DAY:{sig.date()}:{r.code}')
        pos=d.index.get_loc(sig)
        sb=d.iloc[pos]
        for a,b in [('open','_open'),('high','_high'),('low','_low'),('close','_close'),('volume','_vol'),('close','entry_price')]:
            if float(sb[a])!=float(r[b]):mismatched+=1;break
        if mismatched:raise RuntimeError(f'SIGNAL_BAR_KRX_MISMATCH:{sig.date()}:{r.code}')
        item={'signal_date':r.signal_date,'code':r.code,'name':str(r['name']),'all_modes':r.all_modes,
              'source_entry_price':float(r.entry_price),'primary_mode_for_display':r['mode']}
        item.update(make_prewave(d,pos))
        f=get_market_forward(d,calendar,sig,20)
        for k in [1,3,5,10,15,20]:
            o=find_outcome(f,float(r.entry_price),k)
            item[f'D{k}_complete']=int(o['complete'])
            item[f'D{k}_close_pct']=o.get('close_pct',np.nan)
            item[f'D{k}_max_high_pct']=o.get('mfe_pct',np.nan)
            item[f'D{k}_min_low_pct']=o.get('mae_pct',np.nan)
            item[f'D{k}_first_plus5']=o.get('first5')
        item['outcome_class']='CENSORED_D20'
        if item['D20_complete']:
            if item['D20_max_high_pct']>=10 and item['D20_close_pct']>=5:item['outcome_class']='STRONG_PERSISTENT'
            elif item['D20_close_pct']>0:item['outcome_class']='OTHER_PROFIT'
            elif item['D20_max_high_pct']>=5:item['outcome_class']='GIVEBACK'
            else:item['outcome_class']='NO_5PCT_RISE'
        item['is_D5_close_win']=int(item['D5_close_pct']>0) if item['D5_complete'] else np.nan
        item['is_D20_close_win']=int(item['D20_close_pct']>0) if item['D20_complete'] else np.nan
        item['is_D5_high5']=int(item['D5_max_high_pct']>=5) if item['D5_complete'] else np.nan
        item['is_D20_high5']=int(item['D20_max_high_pct']>=5) if item['D20_complete'] else np.nan
        item['is_wave_stage3']=int(item['wave_setup_stage']>=3)
        item['is_wave_stage4']=int(item['wave_setup_stage']==4)
        item['is_prevolume_accum']=int(pd.notna(item['prewave_prevolume_accum_ratio']) and item['prewave_prevolume_accum_ratio']>=1.1)
        item['is_dryup_10_30']=int(pd.notna(item['volume_dryup_10_30']) and item['volume_dryup_10_30']<.7)
        item['is_ma20_overextended']=int(pd.notna(item['ma20_signal_dist_pct']) and item['ma20_signal_dist_pct']>=5)
        rows.append(item)
        if item['D20_complete'] or (sig>=pd.Timestamp('2026-09-21') and item['D5_complete']):
            for policy in ('NEXT_OPEN','BREAKOUT_CONFIRMED','PULLBACK_RECLAIM','MA20_RECOVERY'):
                q=simulate_five_sessions(d,calendar,sig,policy)
                trades.append({'signal_date':r.signal_date,'code':r.code,'name':str(r['name']),'all_modes':r.all_modes,
                               'outcome_class':item['outcome_class'],'recent_oos':int(sig>=pd.Timestamp('2026-09-21')),
                               'D20_complete':item['D20_complete'],**q})
        if i%150==0:print(f'R18_SIGNAL_ANALYZED {i}/{len(unique)}',flush=True)
    results=pd.DataFrame(rows).sort_values(['signal_date','code'])
    if results[['signal_date','code']].duplicated().any():raise RuntimeError('INTERNAL_DUPLICATE')
    results.to_csv(out/'r18_all_unique_signal_features.csv',index=False,encoding='utf-8-sig')
    mature=results[results.D20_complete.eq(1)].copy()
    mature.to_csv(out/'r18_mature_outcome_classes.csv',index=False,encoding='utf-8-sig')
    cols=['prewave_impulse_pct','prewave_pullback_pct','prewave_retrace_fraction','prewave_impulse_vol_ratio',
          'prewave_pullback_vol_ratio','prewave_prevolume_accum_ratio','ma224_dist_signal_pct','ma20_signal_dist_pct',
          'volume_dryup_10_30','D20_close_pct','D20_max_high_pct','D20_min_low_pct','is_wave_stage3',
          'is_wave_stage4','is_prevolume_accum','is_dryup_10_30','is_ma20_overextended']
    tabulate(mature,'outcome_class',cols).to_csv(out/'r18_class_feature_comparison.csv',index=False,encoding='utf-8-sig')
    # Each candidate is predefined before examining outcomes; the table is hypothesis-generating.
    flags=['is_wave_stage3','is_wave_stage4','is_prevolume_accum','is_dryup_10_30','is_ma20_overextended']
    rec=[]
    for cohort,q in [('D20_MATURE',mature),('EARLY_D5_SAME_COHORT',mature[mature.D5_complete.eq(1)]),
                     ('RECENT_D5_20260921',results[(results.signal_date>='2026-09-21') & (results.D5_complete.eq(1))])]:
        h=20 if cohort=='D20_MATURE' else 5
        for flag in flags:
            for yes in (1,0):
                v=q[q[flag].eq(yes)]
                if len(v)==0:continue
                rec.append({'cohort':cohort,'feature':flag,'flag':yes,'n':len(v),
                            'unique_codes':v.code.nunique(),'unique_days':v.signal_date.nunique(),
                            'win_close_pct':v[f'is_D{h}_close_win'].mean()*100,
                            'high5_touch_pct':v[f'is_D{h}_high5'].mean()*100,
                            'mean_close_pct':v[f'D{h}_close_pct'].mean(),
                            'median_close_pct':v[f'D{h}_close_pct'].median()})
    pd.DataFrame(rec).to_csv(out/'r18_predeclared_feature_cohorts.csv',index=False,encoding='utf-8-sig')
    tr=pd.DataFrame(trades)
    tr.to_csv(out/'r18_entry_policy_trade_level.csv',index=False,encoding='utf-8-sig')
    erows=[]
    for cohort,k in [('D20_MATURE',tr.D20_complete.eq(1)),('RECENT_D5',tr.recent_oos.eq(1))]:
        for policy,g in tr[k].groupby('entry_policy'):
            eligible=g[g.censored.eq(0)];fill=eligible[eligible.filled.eq(1)];
            erows.append({'cohort':cohort,'policy':policy,'signals':len(g),'eligible_signals':len(eligible),
                         'trigger_and_filled':len(fill),
                         'coverage_pct':100*len(fill)/len(g) if len(g) else None,
                         'eligible_fill_pct':100*len(fill)/len(eligible) if len(eligible) else None,
                         'censored':int(g.censored.sum()),'no_fill':int((g.filled.eq(0)&g.censored.eq(0)).sum()),
                         'filled_win_pct':100*fill.win.mean() if len(fill) else None,
                         'filled_net_mean_pct':fill.net_pct.mean() if len(fill) else None,
                         'per_eligible_signal_net_mean_pct':eligible.signal_level_net_pct.mean() if len(eligible) else None,
                         'filled_net_median_pct':fill.net_pct.median() if len(fill) else None,
                         'typical_days_held':fill.duration.median() if len(fill) else None})
    pd.DataFrame(erows).to_csv(out/'r18_entry_policy_diagnostics.csv',index=False,encoding='utf-8-sig')
    # From causal pre-signal state to class: descriptive transitions, NOT predictive validation.
    pd.crosstab(mature.wave_setup_stage,mature.outcome_class,margins=True).to_csv(out/'r18_wave_stage_x_outcome_counts.csv',encoding='utf-8-sig')
    ds=results.assign(cohort=np.where(results.D20_complete.eq(1),'D20_MATURE','RECENT_D5'))
    ds.groupby(['cohort','signal_date']).agg(unique_signals=('code','size'),unique_codes=('code','nunique'),
        D5_close_mean=('D5_close_pct','mean'),D5_high_mean=('D5_max_high_pct','mean'),
        D5_positive_pct=('is_D5_close_win',lambda x:100*x.mean())).reset_index().to_csv(
        out/'r18_date_cohort_diagnostics.csv',index=False,encoding='utf-8-sig')
    # Deterministic review sample to visualize mechanisms, not top-N winner selection for live.
    samples=[]
    candidate_cols=['signal_date','code','name','all_modes','outcome_class','source_entry_price',
         'wave_setup_stage','prewave_impulse_pct','prewave_pullback_pct',
         'prewave_pullback_vol_ratio','ma224_dist_signal_pct','ma20_signal_dist_pct',
         'D20_close_pct','D20_max_high_pct','D20_min_low_pct']
    cases=[('STRONG_PERSISTENT','D20_max_high_pct',False),('STRONG_PERSISTENT','D20_min_low_pct',True),
           ('OTHER_PROFIT','D20_close_pct',False),('GIVEBACK','D20_max_high_pct',False),
           ('GIVEBACK','D20_close_pct',True),('NO_5PCT_RISE','D20_close_pct',True)]
    for label,score,ascending in cases:
        g=mature[mature.outcome_class.eq(label)].sort_values([score,'signal_date','code'],ascending=[ascending,True,True])
        g=g.drop_duplicates('code').head(8)[candidate_cols].copy()
        g.insert(0,'casebook_selection',f'{label}|{score}|{("smallest" if ascending else "largest")}')
        samples.append(g)
    pd.concat(samples,ignore_index=True).to_csv(out/'r18_manual_casebook_48.csv',index=False,encoding='utf-8-sig')
    # Small externally checked master-name spot check; do not overwrite the labels.
    # Exact code/price consistency can be 100% despite wrong descriptive metadata.
    reference_names={'000720':'현대건설','000880':'한화'}
    label_warnings=[]
    for stock_code,expected_name in reference_names.items():
        actual_names=sorted(set(events.loc[events.code.eq(stock_code),'name'].astype(str)))
        for actual_name in actual_names:
            if actual_name!=expected_name:
                label_warnings.append({'code':stock_code,'source_display_name':actual_name,
                   'externally_verified_name_20261015':expected_name,'source':'R171_SELECTED_NAME_META',
                   'status':'NAME_METADATA_CONFLICT_NOT_PRICE_BAR_CONFLICT'})
    pd.DataFrame(label_warnings,columns=['code','source_display_name','externally_verified_name_20261015','source','status']).to_csv(
        out/'r18_name_code_spotcheck_warnings.csv',index=False,encoding='utf-8-sig')
    summary={'status':'RESEARCH_ONLY_NOT_TRADE_APPROVED','source_run_id':'38664095610',
             'input_strategy_rows':int(len(events)),'source_all_unique_signal_date_codes':int(len(events.drop_duplicates(['signal_date','code']))),
             'unique_signal_date_codes':int(len(unique)),
             'D20_mature_unique':int(len(mature)),'D5_recent_unique':int(((results.signal_date>='2026-09-21')&results.D5_complete.eq(1)).sum()),
             'outcome_distribution':mature.outcome_class.value_counts().to_dict(),
             'KRX_exact_selected_bar_match':True,'KRX_signal_count_validated':int(len(unique)),
             'name_metadata_conflicts_spotcheck':label_warnings,
             'wave_stage_counts':results.wave_setup_stage.value_counts().sort_index().to_dict(),
             'risk_flags':{'historical_universe_PIT':'NOT_PROVEN','independent_index_membership_PIT':'NOT_PROVEN',
                           'KRX_1530_session_exclusive':'NOT_PROVEN','execution_fills':'SIMULATED_NOT_VERIFIED',
                           'selection_bias':'TOP5_SELECTED_ONLY','signal_clustering':'SAME_STOCK_MANY_DATES',
                           'corporate_actions':'UNADJUSTED_PRICES_NEED_SPLIT_AUDIT',
                           'OOS_D20':'ZERO_RECENT_MATURE'},
             'entry_policy_assumptions':'entry after close decision at following market open, 5 market sessions, target +5% stop -5%, stop-first when both, 0.5% turnover cost, no slippage/queue, full 5-day evidence required',
             'caution':'Only discovery. No deployment, no strategies deleted or tuned. Even the provider 100% check is same KRX path, not separate regular-session proof.',
             'history_provenance':evidence}
    (out/'r18_research_manifest.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R18_COMPLETE',json.dumps({k:summary[k] for k in ('input_strategy_rows','unique_signal_date_codes','D20_mature_unique','D5_recent_unique','outcome_distribution','wave_stage_counts')},ensure_ascii=False),flush=True)

if __name__=='__main__':main()
