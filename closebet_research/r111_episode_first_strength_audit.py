"""R1.11 pinned KRX signal-episode / early-strength audit (RESEARCH_ONLY).

All episode keys and ex-ante feature flags use data available at the signal close.
Future prices label outcomes or provide strictly later confirmation, not retroactive entries.
NO orders, LIVE jobs, rank/search changes, or auto-exit activation.
"""
from __future__ import annotations
import argparse
import hashlib
import io
import json
import zipfile
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd

R171_RAW_SHA='4c28007a550007ad8a9c64fdaffab74c90faeb786194e291f9f7d9284e104280'
R110_FEATURE_SHA='1a03f7dd9a702dcbe2223d891f87b272123bf1dbec87d785bb4a64010af9dd76'
CALENDAR_SHA='2d5a868e68f969072b0a4d6a9ee097e954f46e7b7eb528c6c5c6a0af95e1a795'
CHECK_COLS={'_open':'open','_high':'high','_low':'low','_close':'close','_vol':'volume'}
BAR_COLS=['open','high','low','close','volume']
ASOF='2026-10-12'

def require(condition, reason):
    if not bool(condition): raise RuntimeError('R111_FAIL_CLOSED:'+str(reason))

def sha(b): return hashlib.sha256(b).hexdigest()

def src_verify(r171,r110):
    r17=Path(r171)/'r17_krx/native/v49_76_global_selected_top5.csv'
    r10=Path(r110)/'r110_signal_flags_and_outcomes.csv'
    require(r17.is_file() and r10.is_file(),'SOURCE_MISSING')
    require(sha(r17.read_bytes())==R171_RAW_SHA,'R171_SELECTED_SHA_MISMATCH')
    require(sha(r10.read_bytes())==R110_FEATURE_SHA,'R110_FEATURE_SHA_MISMATCH')
    m=json.loads((Path(r110)/'r110_integrity_manifest.json').read_text())
    require(m['status']=='PASS_DISCOVERY_ONLY_NOT_TRADE_APPROVED','R110_STATUS')
    require(m['source_runs']=={'r171':'38664095610','r18':'38693918216','r19':'38729925104'},'R110_PROVENANCE')
    require(m['r18_unique_analyzed']==754 and m['older_complete_D20']==570 and m['recent_complete_D5_only']==184,'R110_COHORT')
    require(m['output_sha256']['r110_signal_flags_and_outcomes.csv']==R110_FEATURE_SHA,'R110_MANIFEST_HASH')
    raw=pd.read_csv(r17,dtype={'code':str},low_memory=False)
    feats=pd.read_csv(r10,dtype={'code':str},low_memory=False)
    for f in (raw,feats):
        require(f.code.str.fullmatch(r'[0-9]{6}').all(),'CODE_FORMAT')
        require(pd.to_datetime(f.signal_date,format='%Y-%m-%d',errors='coerce').notna().all(),'DATE_FORMAT')
    require(len(raw)==1133,'R171_ROW_COUNT')
    require(len(raw[['signal_date','code']].drop_duplicates())==1059,'R171_UNIQUE_COUNT')
    require(len(feats)==754 and not feats[['signal_date','code']].duplicated().any(),'R110_UNIQUE_COUNT')
    require((feats.D5_complete==1).all() and int(feats.D20_complete.sum())==570,'R110_MATURITY')
    require(feats.loc[feats.D20_complete.eq(0),'D20_close_pct'].isna().all(),'D20_FUTURE_LEAK')
    raw['signal_date']=pd.to_datetime(raw.signal_date)
    feats['signal_date']=pd.to_datetime(feats.signal_date)
    return raw,feats,m

def build_archives(archive_dir):
    """Index fixed 4 KRX shard archives and assert every date and manifest content hash.
    Data is read from each archived dated csv.gz with embedded source hash checks.
    Only 2026-08-03 onward is materialized for this evaluation to reduce runtime.
    """
    zfiles=sorted(Path(archive_dir).glob('*.zip'))
    require(len(zfiles)==4,'FOUR_HISTORY_SHARDS_REQUIRED')
    zips=[zipfile.ZipFile(p) for p in zfiles]
    dates={}; mats=[]
    for z in zips:
        mf=[x for x in z.namelist() if x.startswith('krx_history_manifest_') and x.endswith('.json')]
        require(len(mf)==1,'MANIFEST_MISSING_OR_DUPLICATE')
        m=json.loads(z.read(mf[0])); mats.append(m)
        require(m['source']=='PYKRX_KRX_DAILY_CROSS_SECTION_ALL','KRX_SOURCE_NOT_EXPECTED')
        require(m['official_calendar_sha256']==CALENDAR_SHA,'CALENDAR_HASH_CHANGED')
        require(m['asof']==ASOF,'ASOF_CHANGED')
        for rec in m['files']:
            day=rec['day']
            require(day not in dates,'DUPLICATE_CALENDAR_DAY:'+day)
            require(rec['file'] in z.namelist(),'DATA_FILE_MISSING:'+day)
            dates[day]=(z,rec)
    require({m['shard_index'] for m in mats}==set(range(4)),'HISTORY_SHARD_INDICES')
    require(len(dates)==1907,'CALENDAR_SESSION_COUNT')
    cal=sorted(dates)
    require(cal[-1]==ASOF,'LAST_SESSION_ASOF')
    return zips,dates,cal

def read_recent(dates,cal):
    start='2026-08-03'
    useful=[d for d in cal if d>=start]
    require(len(useful)==46,'EXPECTED_46_SEARCH_AND_EVAL_SESSIONS')
    # 1-day cross-sections: rows are filtered to the frozen 907-stock universe.
    bars={}
    for day in useful:
        z,rec=dates[day]
        buf=z.read(rec['file'])
        require(sha(buf)==rec['sha256'],'KRX_DAILY_SHA_MISMATCH:'+day)
        v=pd.read_csv(io.BytesIO(buf),compression='gzip',dtype={'code':str})
        require(len(v)==int(rec['rows']),'KRX_DAILY_ROW_COUNT:'+day)
        require(v.code.str.fullmatch(r'[0-9]{6}').all(),'KRX_DAILY_CODE:'+day)
        require(not v.code.duplicated().any(),'KRX_DAILY_DUPLICATE_CODE:'+day)
        require((v.date==day).all(),'KRX_DAILY_DATE:'+day)
        require(set(BAR_COLS).issubset(v.columns),'KRX_DAILY_BAR_SCHEMA')
        bars[day]=v.set_index('code')[BAR_COLS]
    return bars,useful

def unique_raw(raw):
    for k,v in CHECK_COLS.items():
        require(k in raw.columns,'RAW_SELECTED_MISSING:'+k)
    # Conflicts among different strategy-mode rows on same date+code would be a data fault.
    for (d,c),grp in raw.groupby(['signal_date','code']):
        require(grp[list(CHECK_COLS)].nunique(dropna=False).max()==1,'SAME_SIGNAL_BAR_CONFLICT:'+str(c)+':'+str(d))
    out=raw.drop_duplicates(['signal_date','code']).copy()
    return out.sort_values(['signal_date','code']).reset_index(drop=True)

def verify_krx(raw_unique,bars):
    for _,r in raw_unique.iterrows():
        dt=r['signal_date'].strftime('%Y-%m-%d'); code=r['code']
        require(dt in bars and code in bars[dt].index,'KRX_SIGNAL_BAR_MISSING:'+code+':'+dt)
        bar=bars[dt].loc[code]
        for input_col,krx_col in CHECK_COLS.items():
            require(float(r[input_col])==float(bar[krx_col]),'KRX_SIGNAL_PRICE_MISMATCH:'+code+':'+dt+':'+krx_col)
    return len(raw_unique)

def assign_episodes(signals,cal,cooldown=10):
    """Episode spans same-code selected signals <=cooldown KRX trading sessions apart.
    Does not assert the first observed signal is the first true breakout/pullback.
    Uses NO future prices, only signal dates available up through this signal.
    """
    ix={d:i for i,d in enumerate(cal)}
    e=signals[['signal_date','code']].drop_duplicates().sort_values(['signal_date','code']).copy()
    e['date_str']=e.signal_date.dt.strftime('%Y-%m-%d')
    require(e.date_str.isin(ix).all(),'SIGNAL_OUTSIDE_KRX_CALENDAR')
    rec=[];prev={};counts=defaultdict(int);first={};epno=defaultdict(int)
    for r in e.itertuples(index=False):
        idx=ix[r.date_str];p=prev.get(r.code)
        gap=idx-p if p is not None else None
        if gap is None or gap>cooldown:
            epno[r.code]+=1
            first[r.code]=idx
            counts[r.code]=0
        counts[r.code]+=1
        rec.append({'signal_date':r.signal_date,'code':r.code,'episode_id':r.code+'-E'+str(epno[r.code]).zfill(3),
                    'stock_episode_number':epno[r.code],
                    'first_observed_for_code':int(p is None),
                    'first_in_episode':int(counts[r.code]==1),
                    'episode_signal_number':counts[r.code],
                    'episode_age_sessions':idx-first[r.code],
                    'prev_signal_gap_sessions':gap if gap is not None else np.nan,
                    'episode_cooldown_sessions':cooldown})
        prev[r.code]=idx
    return pd.DataFrame(rec)

def forward_data(feats,bars,cal):
    index={d:i for i,d in enumerate(cal)}
    rows=[];path=[]
    for r in feats.itertuples(index=False):
        d=r.signal_date.strftime('%Y-%m-%d'); i=index[d]; c=r.code
        entry=float(r.source_entry_price)
        require(float(bars[d].loc[c,'close'])==entry,'R110_SIGNAL_ENTRY_MISMATCH:'+c+':'+d)
        prev=[]
        for h in range(1,21):
            if i+h>=len(cal):break
            today=cal[i+h]
            bar=bars[today].loc[c] if c in bars[today].index else None
            if bar is None or bar.isna().any() or bar['volume']<=0 or bar['open']<=0:
                # Fail-closed for horizon-eligible outcomes, leave censored beyond horizon.
                if (h<=5 and r.D5_complete==1) or (h<=20 and r.D20_complete==1):
                    raise RuntimeError('R111_MATURE_BAR_MISSING_OR_UNTRADEABLE:'+c+':'+today)
                break
            v={k:float(bar[k]) for k in BAR_COLS}
            prev.append(v)
            path.append({'signal_date':d,'code':c,'horizon_day':h,'date':today,
                         'close_pct':(v['close']/entry-1)*100,
                         'high_pct':(v['high']/entry-1)*100,
                         'low_pct':(v['low']/entry-1)*100,
                         'volume':v['volume']})
        if r.D5_complete==1:
            require(len(prev)>=5,'MATURE_D5_CENSORED:'+c)
            d5=(prev[4]['close']/entry-1)*100
            h5=(max(x['high'] for x in prev[:5])/entry-1)*100
            l5=(min(x['low'] for x in prev[:5])/entry-1)*100
            for a,b in [(d5,r.D5_close_pct),(h5,r.D5_max_high_pct),(l5,r.D5_min_low_pct)]:
                require(abs(a-float(b))<1e-7,'D5_RECALC_MISMATCH:'+c+':'+d)
        if r.D20_complete==1:
            require(len(prev)>=20,'MATURE_D20_CENSORED:'+c)
            d20=(prev[19]['close']/entry-1)*100
            h20=(max(x['high'] for x in prev[:20])/entry-1)*100
            l20=(min(x['low'] for x in prev[:20])/entry-1)*100
            for a,b in [(d20,r.D20_close_pct),(h20,r.D20_max_high_pct),(l20,r.D20_min_low_pct)]:
                require(abs(a-float(b))<1e-7,'D20_RECALC_MISMATCH:'+c+':'+d)
        outcome_horizon=20 if r.D20_complete==1 else 5
        plus5=next((j+1 for j,v in enumerate(prev[:outcome_horizon]) if v['high']>=entry*1.05),None)
        d3= (prev[2]['close']/entry-1)*100 if len(prev)>=3 else np.nan
        # Observed only after D+3 close; earliest possible entry D+4 OPEN.
        confirm=bool(np.isfinite(d3) and d3>=2)
        d4_open=float(prev[3]['open']) if len(prev)>=4 else np.nan
        after5=(prev[4]['close']/d4_open-1)*100 if len(prev)>=5 and confirm else np.nan
        after10=(prev[9]['close']/d4_open-1)*100 if len(prev)>=10 and confirm and r.D20_complete==1 else np.nan
        rows.append({'signal_date':d,'code':c,'n_future_bars':len(prev),
                     'first_plus5_high_day':plus5,
                     'D1_close_pct_recalc':(prev[0]['close']/entry-1)*100 if len(prev)>=1 else np.nan,
                     'D3_close_pct_recalc':d3,
                     'D5_close_pct_recalc':(prev[4]['close']/entry-1)*100 if len(prev)>=5 else np.nan,
                     'D20_close_pct_recalc':(prev[19]['close']/entry-1)*100 if len(prev)>=20 else np.nan,
                     'D3_confirm_close_gte2pct':int(confirm),
                     'D4_open_after_D3_confirmation':d4_open if confirm else np.nan,
                     'D4open_to_D5close_pct':after5,
                     'D4open_to_D10close_pct':after10})
    fw=pd.DataFrame(rows); pp=pd.DataFrame(path)
    fw["signal_date"]=pd.to_datetime(fw["signal_date"])
    pp["signal_date"]=pd.to_datetime(pp["signal_date"])
    return fw,pp

def compare_cohorts(covered):
    rows=[]
    for period,m in [('OLD_D20',covered.D20_complete.eq(1)),('RECENT_D5',covered.D20_complete.eq(0))]:
        grp=covered[m]
        outcomes=['D5_close_pct','D5_max_high_pct','D5_min_low_pct']+(['D20_close_pct','D20_max_high_pct','D20_min_low_pct'] if period=='OLD_D20' else [])
        for gap in (5,10,20):
            sub=grp.copy()
            f='episode_first_c'+str(gap)
            for tag,v in [('FIRST_OBSERVED',sub.first_observed_for_code.eq(1)),('EPISODE_FIRST',sub[f].eq(1)),('EPISODE_REPEAT',sub[f].eq(0))]:
                p=sub[v]
                if len(p)==0:continue
                row={'period':period,'gap_threshold_sessions':gap,'group':tag,'n_signals':len(p),
                     'n_stocks':p.code.nunique(),'n_signal_days':p.signal_date.nunique(),
                     'd5_close_win_pct':100*(p.D5_close_pct>0).mean(),
                     'd5_high5_touch_pct':100*(p.D5_max_high_pct>=5).mean()}
                row.update({f'{x}_mean':p[x].mean() for x in outcomes})
                row['d20_close_win_pct']=100*(p.D20_close_pct>0).mean() if period=='OLD_D20' else np.nan
                rows.append(row)
    return pd.DataFrame(rows)

def matched_first_repeat(covered,gap=10):
    """Within each stock episode, use at most one first + the earliest repeat.
    Intended to limit date/stock repetition; NOT independent OOS evidence.
    """
    x=covered[(covered.D20_complete==1)].copy()
    out=[]
    for eid,g in x.groupby('episode_id_c'+str(gap)):
        g=g.sort_values('signal_date')
        f=g[g['episode_first_c'+str(gap)].eq(1)]
        if f.empty: continue
        repeat=g[g['episode_first_c'+str(gap)].eq(0)]
        if repeat.empty:continue
        a=f.iloc[0]; b=repeat.iloc[0]
        out.append({'episode_id':eid,'code':a.code,'first_date':a.signal_date.strftime('%Y-%m-%d'),
                    'repeat_date':b.signal_date.strftime('%Y-%m-%d'),
                    'first_D5_pct':a.D5_close_pct,'repeat_D5_pct':b.D5_close_pct,
                    'first_D20_pct':a.D20_close_pct,'repeat_D20_pct':b.D20_close_pct,
                    'first_minus_repeat_D5_pct':a.D5_close_pct-b.D5_close_pct,
                    'first_minus_repeat_D20_pct':a.D20_close_pct-b.D20_close_pct})
    return pd.DataFrame(out,columns=['episode_id','code','first_date','repeat_date','first_D5_pct','repeat_D5_pct','first_D20_pct','repeat_D20_pct','first_minus_repeat_D5_pct','first_minus_repeat_D20_pct'])

def produce(raw,feats,dates,cal,bars,out,base_m):
    raw_unique=unique_raw(raw)
    ncheck=verify_krx(raw_unique,bars)
    # Build chronology on ALL selected event ids, never from the 754 mature/selected subset.
    episodes={k:assign_episodes(raw_unique,cal,k) for k in (5,10,20)}
    all_epi=episodes[10].copy()
    for k in (5,20):
        t=episodes[k][['signal_date','code','episode_id','first_in_episode','episode_signal_number']].rename(columns={
            'episode_id':'episode_id_c'+str(k),'first_in_episode':'episode_first_c'+str(k),'episode_signal_number':'episode_n_c'+str(k)})
        all_epi=all_epi.merge(t,on=['signal_date','code'],validate='one_to_one')
    all_epi=all_epi.rename(columns={'episode_id':'episode_id_c10','first_in_episode':'episode_first_c10','episode_signal_number':'episode_n_c10'})
    covered=feats.merge(all_epi,on=['signal_date','code'],validate='one_to_one')
    require(len(covered)==754,'EPISODE_COHORT_JOIN')
    fw,path=forward_data(covered,bars,cal)
    covered=covered.merge(fw,on=['signal_date','code'],validate='one_to_one')
    covered['corp_action_suspect']=covered.prior_224_jump_above35pct.astype(bool).astype(int)
    # No re-ranking, no tuned thresholds, no future input into episode flags.
    # Outcome bins used ONLY in descriptive conditional tables.
    raw_counts=covered.groupby('cohort',dropna=False).size().to_dict() if 'cohort' in covered.columns else {}
    cohorts=compare_cohorts(covered)
    without_corp=compare_cohorts(covered[covered.corp_action_suspect.eq(0)])
    without_corp.insert(0,'exclude_corp_action_flag',1)
    cohorts.insert(0,'exclude_corp_action_flag',0)
    cohorts=pd.concat([cohorts,without_corp],ignore_index=True)
    pair=matched_first_repeat(covered)
    confirm=[]
    for period,g in [('OLD_D20',covered[covered.D20_complete.eq(1)]),('RECENT_D5',covered[covered.D20_complete.eq(0)])]:
        for flag in (0,1):
            q=g[g.D3_confirm_close_gte2pct.eq(flag)]
            confirm.append({'period':period,'D3_close_ge2pct':flag,'n_signals':len(q),
                            'n_stocks':q.code.nunique(),
                            'D5_close_win_pct':(q.D5_close_pct>0).mean()*100 if len(q) else np.nan,
                            'D5_close_mean_pct':q.D5_close_pct.mean(),
                            'D20_close_mean_pct':q.D20_close_pct.mean() if period=='OLD_D20' else np.nan,
                            'D4_OPEN_ENTRY_TO_D5_CLOSE_mean_pct':q.D4open_to_D5close_pct.mean(),
                            'D4_OPEN_ENTRY_TO_D10_CLOSE_mean_pct':q.D4open_to_D10close_pct.mean() if period=='OLD_D20' else np.nan,
                            'D4_open_entry_eligible_n':q.D4open_to_D5close_pct.notna().sum()})
    confirm=pd.DataFrame(confirm)
    # Session-level paired contrasts: compute each day's FIRST - REPEAT mean only if both
    # groups exist on that historical session; never count 10 signals as 10 independent days.
    day_rows=[]
    for period,g in [('OLD_D20',covered[covered.D20_complete.eq(1)]),('RECENT_D5',covered[covered.D20_complete.eq(0)])]:
        for dt,h in g.groupby('signal_date'):
            a=h[h.episode_first_c10.eq(1)];b=h[h.episode_first_c10.eq(0)]
            if len(a)==0 or len(b)==0: continue
            day_rows.append({'period':period,'signal_date':dt.strftime('%Y-%m-%d'),
                             'first_n':len(a),'repeat_n':len(b),
                             'first_mean_D5_pct':a.D5_close_pct.mean(),
                             'repeat_mean_D5_pct':b.D5_close_pct.mean(),
                             'same_day_first_minus_repeat_D5_pct':a.D5_close_pct.mean()-b.D5_close_pct.mean(),
                             'same_day_first_minus_repeat_D20_pct':a.D20_close_pct.mean()-b.D20_close_pct.mean() if period=='OLD_D20' else np.nan})
    same_day=pd.DataFrame(day_rows)
    daily=path.merge(covered[['signal_date','code','outcome_class','D20_complete','episode_first_c10','episode_n_c10']],
                     left_on=['signal_date','code'],right_on=['signal_date','code'],validate='many_to_one')
    daily=daily[daily.D20_complete.eq(1)]
    price_path=daily.groupby(['outcome_class','horizon_day'],dropna=False).agg(
        n=('code','size'),median_close_pct=('close_pct','median'),mean_close_pct=('close_pct','mean'),
        mean_high_pct=('high_pct','mean'),mean_low_pct=('low_pct','mean')).reset_index()
    risk={'historical_universe_PIT':'NOT_PROVEN','KRX_1530_exclusive':'NOT_PROVEN',
          'corporate_action_adjustment':'NOT_PROVEN','independent_OOS':'NO',
          'first_observed_is_true_first_wave':'NO_LEFT_CENSORED',
          'selection_scope':'TOP5_SELECTED_ONLY','same_stock_dependence':'YES',
          'D3_confirmation_entry':'D4_OPEN_ONLY_REQUIRES_REAL_EXECUTION_STUDY'}
    output_files={'r111_all_selected_episode_1059.csv':all_epi.assign(signal_date=lambda q:q.signal_date.dt.strftime('%Y-%m-%d')),
                  'r111_mature_recent_episode_outcomes_754.csv':covered.assign(signal_date=lambda q:q.signal_date.dt.strftime('%Y-%m-%d')),
                  'r111_episode_first_repeat_comparison.csv':cohorts,
                  'r111_within_episode_first_repeat_pairs.csv':pair,
                  'r111_same_day_first_repeat_contrasts.csv':same_day,
                  'r111_day1_to20_mature_price_paths.csv':price_path,
                  'r111_day3_strength_confirmation_and_delayed_entry.csv':confirm}
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    filehash={}
    for name,df in output_files.items():
        df.to_csv(out/name,index=False,encoding='utf-8-sig')
        filehash[name]=sha((out/name).read_bytes())
    summ={
      'status':'PASS_DESCRIPTIVE_ONLY_NOT_TRADE_APPROVED',
      'upstream_runs':{'r171':38664095610,'r110':38745541969},
      'r171_strategy_rows':len(raw),'all_selected_unique_signals':len(raw_unique),
      'krx_selected_price_bars_exact_verified':ncheck,'r110_reused_unique_signals':len(covered),
      'mature_D20':int(covered.D20_complete.sum()),
      'recent_D5_only':int(covered.D20_complete.eq(0).sum()),
      'fully_mature_D5_recalculated':int(covered.D5_complete.sum()),
      'fully_mature_D20_recalculated':int(covered.D20_complete.sum()),
      'corp_action_suspect_signals_in_mature':int(covered.loc[covered.D20_complete.eq(1),'corp_action_suspect'].sum()),
      'episode_first_10_mature':int(covered.loc[covered.D20_complete.eq(1),'episode_first_c10'].sum()),
      'episode_repeat_10_mature':int(covered.loc[covered.D20_complete.eq(1),'episode_first_c10'].eq(0).sum()),
      'within_episode_first_repeat_pair_count':len(pair),
      'distinct_stocks_in_pairs':int(pair.code.nunique()),
      'same_day_paired_D20_days':int(same_day.period.eq('OLD_D20').sum()),
      'same_day_first_minus_repeat_D5_daily_mean_pct':float(same_day.loc[same_day.period.eq('OLD_D20'),'same_day_first_minus_repeat_D5_pct'].mean()),
      'first_minus_repeat_D5_pair_mean_pct':float(pair.first_minus_repeat_D5_pct.mean()) if len(pair) else None,
      'first_minus_repeat_D20_pair_mean_pct':float(pair.first_minus_repeat_D20_pct.mean()) if len(pair) else None,
      'first_minus_repeat_D5_pair_median_pct':float(pair.first_minus_repeat_D5_pct.median()) if len(pair) else None,
      'first_minus_repeat_D20_pair_median_pct':float(pair.first_minus_repeat_D20_pct.median()) if len(pair) else None,
      'first_observed_left_censored_from':'2026-08-03','episode_gap_sensitivity_sessions':[5,10,20],
      'source_sha256':{'r171_raw_selected':R171_RAW_SHA,'r110_signal_flags':R110_FEATURE_SHA},
      'calendar_sha256':CALENDAR_SHA,'output_sha256':filehash,'limitations':risk}
    (out/'r111_integrity_manifest.json').write_text(json.dumps(summ,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R111_RESEARCH_PASS',json.dumps({k:summ[k] for k in ('all_selected_unique_signals','krx_selected_price_bars_exact_verified','mature_D20','recent_D5_only','episode_first_10_mature','episode_repeat_10_mature','within_episode_first_repeat_pair_count')},ensure_ascii=False),flush=True)
    return summ

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--r171-dir',required=True);ap.add_argument('--r110-dir',required=True)
    ap.add_argument('--history-artifacts',required=True);ap.add_argument('--out',required=True)
    a=ap.parse_args();raw,feats,m=src_verify(a.r171_dir,a.r110_dir)
    z,dates,cal=build_archives(a.history_artifacts)
    try:bars,_=read_recent(dates,cal);produce(raw,feats,dates,cal,bars,a.out,m)
    finally:
        for x in z:x.close()
if __name__=='__main__':main()
