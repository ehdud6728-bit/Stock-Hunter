#!/usr/bin/env python3
"""Research-only, as-of technical-support/reaction feature audit.
Uses pinned R1D3 2026 episodes and immutable KRX price shards already downloaded.
No trading/no LIVE score modifications. Outcomes only joined *after* ASOF features freeze.
This is descriptive hypothesis generation, NOT independent OOS validation.
"""
import sys, argparse, zipfile, gzip, io, json, hashlib
from collections import defaultdict, Counter
from pathlib import Path
import numpy as np
import pandas as pd

BANDS=('MA5','MA10','MA20','MA40','MA60','MA112','MA224','BB20_L','BB40_L','ENV20_L_10','ENV40_L_10','WAVE_LOW','FIB50','FIB618')
MIN_ROWS=225

def _roll_indicators(df):
    c=df['close'].astype(float)
    for n in (5,10,20,40,60,112,224):
        df[f'MA{n}']=c.rolling(n,min_periods=n).mean()
    # scanner/indicators.py uses ddof=0, default 2sigma.
    for n in (20,40):
        df[f'BB{n}_L']=df[f'MA{n}']-2*c.rolling(n,min_periods=n).std(ddof=0)
        # Closing_bet_backtest.py legacy 20/10 and 40/10 reference envelope.
        df[f'ENV{n}_L_10']=df[f'MA{n}']*.9
    return df

def _calc_behavior(ph, vals, peak_index, cur_index):
    """Observed after the fixed first-wave peak, inclusive of signal, no future."""
    out={}; pct=100
    if not np.isfinite(vals[peak_index]) or vals[peak_index]<=0:
        return {f'{ph}_{k}':np.nan for k in ('peak_above','touch_from_above','reclaim','hold','fail','signal_above','last_touch_lag','touch_low_pct','touch_type')}
    price_close=ph_df_close=DF_CTX['close']
    price_low=DF_CTX['low']
    price_high=DF_CTX['high']
    peak_above=bool(price_close[peak_index]>=vals[peak_index]*.99)
    touch=[]; break_days=[]; bottom_dist=[]
    for j in range(peak_index+1,cur_index+1):
        lev=vals[j]
        if not np.isfinite(lev) or lev<=0: continue
        # Only classify as support *from above* if the wave peak was over it.
        # A touch is low <= 1% above contemporaneous level; it might penetrate deeply.
        if peak_above and price_low[j]<=lev*1.01:
            touch.append(j)
            bottom_dist.append(100*(price_low[j]/lev-1))
        if peak_above and price_close[j]<lev*.99:
            break_days.append(j)
    cur=vals[cur_index]
    signal_above=bool(np.isfinite(cur) and price_close[cur_index]>=cur)
    touched=bool(touch)
    # bounce/restoration is measured ONLY as of the selection (signal) day.
    reclaimed=bool(touched and break_days and signal_above and max(break_days)<cur_index)
    held=bool(touched and not break_days and signal_above)
    failed=bool(touched and not signal_above)
    out[f'{ph}_peak_above']=int(peak_above)
    out[f'{ph}_touch_from_above']=int(touched)
    out[f'{ph}_reclaim']=int(reclaimed)
    out[f'{ph}_hold']=int(held)
    out[f'{ph}_fail']=int(failed)
    out[f'{ph}_signal_above']=int(signal_above)
    out[f'{ph}_last_touch_lag']=int(cur_index-touch[-1]) if touched else np.nan
    out[f'{ph}_touch_low_pct']=min(bottom_dist) if touched else np.nan
    out[f'{ph}_touch_type']= ('RECLAIM_AFTER_CLOSE_BREAK' if reclaimed else
           'HOLD_NO_CLOSE_BREAK' if held else 'STILL_BELOW_AFTER_TOUCH' if failed else
           'TOUCH_OTHER' if touched else 'PEAK_BELOW_LEVEL' if not peak_above else 'NO_TOUCH')
    return out

# Context is local to event to avoid recomputing per-band
DF_CTX={}

def one_episode(df, e):
    signal=e['date'];peak=e['wave_peak_date'];trough=e['wave_trough_date']
    if signal not in df.index or peak not in df.index or trough not in df.index:
        return {'date':signal,'code':e['code'],'status_feature':'MISSING_EVENT_PRICE_DAY'}
    s=int(df.index.get_loc(signal)); p=int(df.index.get_loc(peak)); t=int(df.index.get_loc(trough))
    if not (t<p<s):return {'date':signal,'code':e['code'],'status_feature':'BAD_WAVE_ORDER'}
    if s<MIN_ROWS or p+1>s:return {'date':signal,'code':e['code'],'status_feature':'INSUFFICIENT_ASOF_HISTORY'}
    rows=df.iloc[:s+1]
    o=rows.open.to_numpy(float);h=rows.high.to_numpy(float)
    l=rows.low.to_numpy(float);c=rows.close.to_numpy(float);a=rows.amount.to_numpy(float)
    global DF_CTX
    DF_CTX={'close':c,'low':l,'high':h}
    pp=float(h[p]);pl=float(l[t]);
    if pp<=pl or pl<=0:return {'date':signal,'code':e['code'],'status_feature':'BAD_WAVE_LEVELS'}
    first=rows.iloc[-1];prior=rows.iloc[-2]
    aftlow=float(np.min(l[p+1:s+1])); aftmin_i=p+1+int(np.argmin(l[p+1:s+1]))
    wick=(min(c[s],o[s])-l[s])/(h[s]-l[s]) if h[s]>l[s] else np.nan
    result={'date':signal,'code':e['code'],'status_feature':'ASOF_OK',
            'signal_date':signal,'wave_peak_date':peak,'wave_trough_date':trough,
            'peak_to_signal_sessions':s-p,
            'wave_peak_high':pp,'wave_trough_low':pl,'pullback_min_low':aftlow,
            'pullback_min_date':rows.index[aftmin_i],
            'pullback_intraday_max_retrace_pct':100*(pp-aftlow)/(pp-pl),
            'pullback_close_retrace_pct':100*(pp-c[s])/(pp-pl),
            'low_above_wave_low_pct':100*(aftlow/pl-1),
            'pullback_min_above_wave_low':int(aftlow>pl),
            'signal_bullish':int(c[s]>o[s]),
            'signal_close_above_prev_close':int(c[s]>c[s-1]),
            'signal_close_above_prev_high':int(c[s]>h[s-1]),
            'signal_close_above_previous3high':int(c[s]>np.max(h[max(0,s-3):s])),
            'signal_lower_wick_fraction':wick,
            'signal_wick_rejection_gt30pct':int(np.isfinite(wick) and wick>=.3),
            'signal_amount_vs_prior5median':float(a[s]/np.median(a[s-5:s])) if np.median(a[s-5:s])>0 else np.nan,
            'amount_3d_vs_prior10d':float(np.median(a[s-2:s+1])/np.median(a[s-12:s-2])) if np.median(a[s-12:s-2])>0 else np.nan,
            'signal_5MA_reclaim_today':int(c[s]>=first.MA5 and c[s-1]<prior.MA5),
            'signal_20MA_reclaim_today':int(c[s]>=first.MA20 and c[s-1]<prior.MA20),
            'signal_5MA_above':int(c[s]>=first.MA5),
            'signal_20MA_above':int(c[s]>=first.MA20),
            'prior3_upday_count':int(np.sum(np.diff(c[s-3:s+1])>0)),
           }
    # Compute static retracement levels known by signal date.
    static={'WAVE_LOW':np.full(s+1,pl),
       'FIB50':np.full(s+1,pp-.50*(pp-pl)),
       'FIB618':np.full(s+1,pp-.618*(pp-pl))}
    for band in BANDS:
        lev=static.get(band,rows[band].to_numpy(float) if band not in static else None)
        result.update(_calc_behavior(band,lev,p,s))
        val=lev[s]
        result[f'{band}_signal_close_distance_pct']=100*(c[s]/val-1) if np.isfinite(val) and val>0 else np.nan
    # Descriptive EXCLUSIVE nearest support at the LOWEST post-wave day known by signal.
    # A single historical low can be near multiple dynamic bands; record both nearest and overlap.
    near=[]
    for band in BANDS:
        lev=static.get(band, rows[band].to_numpy(float) if band not in static else None)
        if np.isfinite(lev[aftmin_i]) and lev[aftmin_i]>0:
            abs_pct=100*abs(float(l[aftmin_i])/float(lev[aftmin_i])-1)
            if abs_pct<=2.0:
                near.append((abs_pct,band))
    near.sort()
    result['postpeak_low_nearest_support_within2pct']=near[0][1] if near else 'NO_LINE_WITHIN_2PCT'
    result['postpeak_low_support_confluence_count_within2pct']=len(near)
    result['postpeak_low_nearest_support_distance_pct']=near[0][0] if near else np.nan
    result['postpeak_low_near_lines_joined']='|'.join(x[1] for x in near)
    # Breakdown and recovery of exact post-peak minimum by signal date;
    # this cannot be a future local minimum — current signal may be the low.
    lowdist=c[s]/aftlow-1 if aftlow>0 else np.nan
    result['bounce_from_postpeak_min_to_signal_close_pct']=100*lowdist
    result['min_low_on_signal_day']=int(aftmin_i==s)
    result['days_from_postpeak_min_to_signal']=s-aftmin_i
    return result

def read_source_csv(source, rel):
    source=Path(source)
    if source.is_dir():
        p=source/rel
        if not p.is_file():raise FileNotFoundError(p)
        return pd.read_csv(p,dtype={'code':str,'date':str,'wave_peak_date':str,'wave_trough_date':str,'signal_date':str})
    with zipfile.ZipFile(source) as zipfile_obj:
        return pd.read_csv(io.BytesIO(zipfile_obj.read(rel)),dtype={'code':str,'date':str,'wave_peak_date':str,'wave_trough_date':str,'signal_date':str})


def prepare_data(d,source):
    codes=set(d.code); min_date='2025-01-02'; max_date=max(d.date)
    dates=[];bars=defaultdict(list)
    # GitHub artifact extracts to a directory with shard_00/... day files.
    # Local reproducibility also accepts the original 8 ZIP shard bundles.
    source=Path(source)
    archive_paths=sorted(source.glob('KRX_FULL39295295197_SHARD[0-7].zip'))
    if len(archive_paths)==8:
        iterator=[]
        for path in archive_paths:
            with zipfile.ZipFile(path) as z:
                for name in z.namelist():
                    if name.endswith('.csv.gz'):
                        day=Path(name).name[:8];day=f'{day[:4]}-{day[4:6]}-{day[6:8]}'
                        if min_date<=day<=max_date:
                            iterator.append((day,gzip.decompress(z.read(name))))
    else:
        assert not archive_paths, 'PARTIAL_ORIGINAL_KRX_BUNDLE'
        files=sorted(source.glob('shard_*/*.csv.gz'))
        if not files:
            files=sorted(source.rglob('shard_*/*.csv.gz'))
        assert files, 'MISSING_GITHUB_EXTRACTED_KRX_DATA'
        iterator=[]
        for p in files:
            day=p.name[:8];day=f'{day[:4]}-{day[4:6]}-{day[6:8]}'
            if min_date<=day<=max_date:
                iterator.append((day,gzip.decompress(p.read_bytes())))
    for day,raw in iterator:
        df=pd.read_csv(io.BytesIO(raw),dtype={'code':str,'date':str},usecols=['code','date','open','high','low','close','volume','amount'])
        df=df[df.code.isin(codes)]
        for row in df.itertuples(index=False):
            bars[row.code].append((row.date,row.open,row.high,row.low,row.close,row.volume,row.amount))
        dates.append(day)

    frames={}
    for code,data in bars.items():
        df=pd.DataFrame(data,columns=['date','open','high','low','close','volume','amount'])
        df=df.sort_values('date').drop_duplicates('date').set_index('date')
        frames[code]=_roll_indicators(df)
    return frames,sorted(set(dates))

def summary_table(full,flags,by=None):
    v=full[full['outcome_ok']==1].copy()
    if by:
        cols=by
    else:
        cols=[]
    out=[]
    for col in flags:
        for k in (0,1):
            df=v[v[col]==k]
            if not len(df):continue
            out.append({'factor':col,'flag':k,'n_complete':len(df),
                        'unique_signal_dates':df.date.nunique(),'unique_codes':df.code.nunique(),
                        'strong_n':(df.net_20bps_pct>=15).sum(),
                        'failure_n':(df.group=='FAILURE_LE_MINUS5').sum(),
                        'strong_rate_pct':100*(df.net_20bps_pct>=15).mean(),
                        'failure_rate_pct':100*(df.group=='FAILURE_LE_MINUS5').mean(),
                        'median_d20_pct':df.net_20bps_pct.median(),
                        'avg_d20_pct':df.net_20bps_pct.mean(),
                        'rate_any_positive_pct':100*(df.net_20bps_pct>0).mean()})
    return pd.DataFrame(out)

def run(args):
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    d=read_source_csv(args.r1d3,'after_outcome_unblinded/event_group_outcome_and_prior_features.csv')
    d=d[d.horizon==20].reset_index(drop=True)
    assert len(d)==1249, f'wrong R1D3 episode count {len(d)}'
    frames,dates=prepare_data(d,args.shards)
    results=[]
    for i,row in enumerate(d.to_dict('records')):
        code=row['code'];df=frames.get(code)
        if df is None:ft={'date':row['date'],'code':code,'status_feature':'MISSING_TICKER_KRX'}
        else:ft=one_episode(df,row)
        results.append(ft)
    asof=pd.DataFrame(results)
    assert asof[['date','code']].duplicated().sum()==0
    assert len(asof)==1249
    assert asof.status_feature.eq('ASOF_OK').sum()==1249,asof.status_feature.value_counts().to_dict()
    # These descriptive reversal flags are also ASOF-only. Freeze before outcome join.
    for feature,fn in {'signal_amount_above_prev5':lambda a: a.signal_amount_vs_prior5median>=1.,
                       'amount3d_above_prior10':lambda a: a.amount_3d_vs_prior10d>=1.,
                       'signal_low_wick_gt30':lambda a: a.signal_lower_wick_fraction>=.30,
                       'postpeak_min_rebound_gt2':lambda a: a.bounce_from_postpeak_min_to_signal_close_pct>=2.,
                       'postpeak_min_rebound_gt5':lambda a: a.bounce_from_postpeak_min_to_signal_close_pct>=5.}.items():
        asof[feature]=fn(asof).astype(int)
    # NEVER mix forward returns into the as-of saved file.
    asof.to_csv(out/'ASOF_ONLY_1249_SUPPORT_REACTION_FEATURES.csv.gz',index=False,compression='gzip',encoding='utf-8-sig')
    labels=d[['date','code','net_20bps_pct','group','status_out','joint_index_20d_regime','wave_gain_pct','wave_retrace_pct']].copy()
    full=asof.merge(labels,on=['date','code'],validate='one_to_one',how='left')
    full['outcome_ok']=full.status_out.eq('PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED').astype(int)
    assert full.outcome_ok.sum()==939
    full.to_csv(out/'REVEALED_ONLY_GROUP_AND_SUPPORT_FEATURES.csv.gz',index=False,compression='gzip',encoding='utf-8-sig')
    flags=['pullback_min_above_wave_low','min_low_on_signal_day','signal_bullish','signal_close_above_prev_close','signal_close_above_prev_high','signal_close_above_previous3high','signal_wick_rejection_gt30pct','signal_5MA_reclaim_today','signal_20MA_reclaim_today','signal_5MA_above','signal_20MA_above']
    for band in BANDS:
        flags.extend([f'{band}_touch_from_above',f'{band}_reclaim',f'{band}_hold',f'{band}_fail',f'{band}_signal_above'])
    s=summary_table(full,flags)
    s.to_csv(out/'OUTCOME_BY_SUPPORT_AND_REACTION_BINARY_DESCRIPTIVE.csv',index=False,encoding='utf-8-sig')
    # Where was the most recent *touched from above* level? Not exclusive; supports overlap.
    detail=[]
    complete=full[full.outcome_ok==1]
    for band in BANDS:
        for state in ('HOLD_NO_CLOSE_BREAK','RECLAIM_AFTER_CLOSE_BREAK','STILL_BELOW_AFTER_TOUCH','NO_TOUCH','PEAK_BELOW_LEVEL'):
            x=complete[complete[f'{band}_touch_type']==state]
            if len(x)<1:continue
            detail.append({'level':band,'signal_state':state,'n':len(x),'date_count':x.date.nunique(),
                           'strong_n':int(x.net_20bps_pct.ge(15).sum()),'fail_n':int(x.group.eq('FAILURE_LE_MINUS5').sum()),
                           'strong_rate_pct':100*x.net_20bps_pct.ge(15).mean(),
                           'fail_rate_pct':100*x.group.eq('FAILURE_LE_MINUS5').mean(),
                           'median_d20_pct':x.net_20bps_pct.median(),
                           'positive_d20_pct':100*(x.net_20bps_pct>0).mean()})
    statuses=pd.DataFrame(detail)
    statuses.to_csv(out/'LEVEL_SUPPORT_TOUCH_RECLAIM_FAIL_COMPARISON.csv',index=False,encoding='utf-8-sig')
    # Exclusive closest-level bucket for a user-friendly "where was the pullback?" report.
    grp=[]
    for level, q in complete.groupby('postpeak_low_nearest_support_within2pct'):
        grp.append({'nearest_support':level,'n_complete':len(q),'n_dates':q.date.nunique(),
                    'strong_n':int(q.net_20bps_pct.ge(15).sum()),
                    'failure_n':int(q.net_20bps_pct.le(-5).sum()),
                    'strong_rate_pct':100*q.net_20bps_pct.ge(15).mean(),
                    'failure_rate_pct':100*q.net_20bps_pct.le(-5).mean(),
                    'median_d20_pct':q.net_20bps_pct.median()})
    pd.DataFrame(grp).sort_values('n_complete',ascending=False).to_csv(
        out/'EXCLUSIVE_NEAREST_LOW_SUPPORT_WITHIN_2PCT.csv',index=False,encoding='utf-8-sig')
    # same-day pairs: match existing frozen 31 pairs, no new pairing optimized to target factors.
    pairs=read_source_csv(args.sealed,'result/sealed_after_reveal/REVEAL_OUTCOME_PAIRS.csv')
    pp=pairs.rename(columns={'signal_date':'date'}).merge(full,on=['date','code'],how='left',validate='one_to_one')
    assert len(pp)==62 and pp.status_feature.eq('ASOF_OK').all()
    pair_rows=[]
    for pair, x in pp.groupby('pair_id'):
        a=x[x.outcome_label=='STRONG_GE15']; b=x[x.outcome_label=='FAILURE_LE_MINUS5']
        assert len(a)==len(b)==1
        a=a.iloc[0];b=b.iloc[0];row={'pair_id':pair,'date':a.date,'winner_code':a.code,'failure_code':b.code,
           'balanced_distance':a.distance_baseline}
        for k in flags:
            row[f'{k}_winner_minus_fail']=int(a[k])-int(b[k])
        pair_rows.append(row)
    paired=pd.DataFrame(pair_rows)
    paired.to_csv(out/'SEALED_SAME_DATE_31_PAIR_INDICATOR_COMPARISON.csv',index=False,encoding='utf-8-sig')
    ps=[]
    for k in flags:
        ds=paired[f'{k}_winner_minus_fail']
        n_dis=int((ds!=0).sum()); fav=int((ds==1).sum());opp=int((ds==-1).sum())
        ps.append({'factor':k,'n_pairs':len(paired),'n_discordant':n_dis,'winner_yes_failure_no':fav,
                'winner_no_failure_yes':opp,'discordant_winner_share_pct':(100*fav/n_dis if n_dis else np.nan),
                'pair_difference_mean_pp':100*ds.mean(),
                'support_metric_exploratory_not_validated':True})
    pd.DataFrame(ps).to_csv(out/'SEALED_PAIRED_31_SUPPORT_REACTION_DIRECTIONAL.csv',index=False,encoding='utf-8-sig')
    # Signal entry changes: cross hold of MA and liquidity re-entry with fixed descriptive gate 1.0x.
    labels2=['signal_amount_above_prev5','amount3d_above_prior10','signal_low_wick_gt30','postpeak_min_rebound_gt2','postpeak_min_rebound_gt5']
    summary_table(full,labels2).to_csv(out/'BOUNCE_PRICE_AND_AMOUNT_EVENT_COMPARISON.csv',index=False,encoding='utf-8-sig')
    # Pre-specified interaction comparisons: support state + near-term buying/reclaim, not a fitted score.
    interaction=[]
    for band in ('MA20','MA40','BB20_L','BB40_L','ENV20_L_10','ENV40_L_10','FIB50','FIB618'):
        for react in ('signal_close_above_prev_close','signal_close_above_prev_high','signal_amount_above_prev5','signal_5MA_above'):
            t=complete.copy()
            cond=t[f'{band}_touch_from_above'].eq(1)&t[f'{band}_signal_above'].eq(1)&full.loc[t.index,react].eq(1)
            for is_good in [False,True]:
                x=t[cond==is_good]
                interaction.append({'support':band,'reaction':react,'support_plus_reaction':int(is_good),'n':len(x),
                    'strong_n':int(x.net_20bps_pct.ge(15).sum()),'failure_n':int(x.group.eq('FAILURE_LE_MINUS5').sum()),
                    'strong_rate_pct':100*x.net_20bps_pct.ge(15).mean() if len(x) else np.nan,
                    'failure_rate_pct':100*x.group.eq('FAILURE_LE_MINUS5').mean() if len(x) else np.nan,
                    'median_d20_pct':x.net_20bps_pct.median() if len(x) else np.nan})
    pd.DataFrame(interaction).to_csv(out/'SUPPORT_PLUS_REACTION_INTERACTION_EXPLORATORY.csv',index=False,encoding='utf-8-sig')
    manifest={'study':'R1D5_EXPLORATORY_SUPPORT_REACTION_ASOF_SEPARATED_FROM_D20_OUTCOMES',
       'source_r1d3':'StockHunter_R1D3_FULL_COMPARE_39884755058.zip',
       'source_r1d4_sealed':'StockHunter_R1D4_SEALED_GITHUB_39896686387.zip',
       'source_KRX_shards':8,'source_window':['2026-06-15','2026-10-12'],
       'price_data_start_loaded':dates[0], 'price_data_end_loaded':dates[-1],
       'price_bars_for_each_event':'ONLY_DATE_LE_SIGNAL_DATE',
       'n_episode':len(asof),'n_complete':int(full.outcome_ok.sum()),
       'n_strong':int(full.net_20bps_pct.ge(15).sum()),
       'n_fail':int(full.group.eq('FAILURE_LE_MINUS5').sum()),
       'n_pairs':len(paired),'strategy_ready':False,'order_count':0,
       'envelope_reference':'MA20 and MA40, 10% lower; Closing_bet_backtest.py HTS reference',
       'exclusive_nearest_support_rule':'post-peak lowest intraday low compared with all dynamic/static levels at that day, absolute distance<=2%, assign closest one, keep overlap count; exploratory',
       'bollinger_reference':'MA20 and MA40 minus 2 sigma, ddof0, scanner/indicators.py',
       'definitions':{'touch':'After first wave peak, if peak close >= support*0.99 and intraday low <= current support*1.01',
         'hold':'Touch followed by no close below 0.99*support, signal close at/above support',
         'reclaim':'Touch and one or more closes below 0.99*support before signal, signal close at/above support',
         'failed':'Touch and signal close below support',
         'bounce_2pct':'From lowest postwave low to close at search date >=2%',
         'strong':'D+20 net20bp >=15%', 'failure':'D+20 net20bp <=-5%',
         'long_short':'No real executions or stop-first paths; candidates selected from future labels'} }
    (out/'MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print('RESULT',json.dumps({k:manifest[k] for k in ('n_episode','n_complete','n_strong','n_fail','n_pairs')},ensure_ascii=False))
    # useful console focused excerpts for report authoring
    for band in ('MA20','MA40','MA224','BB20_L','BB40_L','ENV20_L_10','ENV40_L_10','WAVE_LOW','FIB50','FIB618'):
        d=statuses[(statuses.level==band)&statuses.signal_state.isin(('HOLD_NO_CLOSE_BREAK','RECLAIM_AFTER_CLOSE_BREAK','STILL_BELOW_AFTER_TOUCH'))]
        print('BAND',band,d[['signal_state','n','strong_rate_pct','fail_rate_pct','median_d20_pct']].round(2).to_dict('records'))
    return out

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--r1d3',required=True);parser.add_argument('--sealed',required=True);parser.add_argument('--shards',required=True);parser.add_argument('--out',required=True);run(parser.parse_args())
