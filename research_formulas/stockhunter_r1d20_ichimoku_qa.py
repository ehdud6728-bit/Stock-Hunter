#!/usr/bin/env python3
"""R1-D20: price-time-causal Ichimoku + BB20/30/40; frozen historical cohorts.

RESEARCH_ONLY. D9 2021-23 date-disjoint retrospective comparison vs D19 2024-25
exploration, NOT genuinely unseen prospective OOS or native 66 parity.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
import re
from collections import defaultdict, deque
from pathlib import Path
import numpy as np
import pandas as pd

VERSION = 'STOCKHUNTER_R1D20_ICHIMOKU_CAUSAL_QA_RESEARCH_V1'
STATUS = 'PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
HORIZONS = (5, 10, 15, 20)
OUTCOME_FORBIDDEN = ('net_20bps_pct', 'gross_pct', 'entry_open', 'exit_close', 'status_out', 'mae_intraday_pct', 'mfe_intraday_pct')

def fail(msg): raise RuntimeError('R1D20_FAIL_CLOSED_' + str(msg))
def digest(b): return hashlib.sha256(b).hexdigest()
def load_protocol(path):
    p = json.loads(Path(path).read_text('utf-8'))
    if p.get('research_id') != 'STOCKHUNTER_R1D20_ICHIMOKU_CAUSAL_QA_AND_BB_INDEPENDENT_PERIOD' or not p.get('research_only'):
        fail('PROTOCOL_ID_DRIFT')
    if not all(p.get('guards', {}).values()) or not p['historical_period_not_new_prospective_oos']:
        fail('RESEARCH_GUARDS_DRIFT')
    if p['ichimoku_contract']['plot_forward_sessions'] != 26 or p['bollinger_contract']['ddof'] != 0:
        fail('FIXED_TECHNICAL_DEFINITIONS_DRIFT')
    if p['frozen_population'] != {'exploratory_events': 5247, 'retrospective_date_disjoint_events':7096, 'cooldown_trading_days':20}:
        fail('FROZEN_POPULATION_DRIFT')
    if p['price_contract']['horizons'] != [5,10,15,20] or p['price_contract']['fixed_roundtrip_cost_bps'] != 20:
        fail('PRICE_HORIZON_DRIFT')
    if len(p['hypotheses_locked_before_R1D20_period_labels']) != len(set(p['hypotheses_locked_before_R1D20_period_labels'])):
        fail('DUPLICATE_HYPOTHESIS')
    return p

def save_json(obj, file):
    f=Path(file);f.parent.mkdir(parents=True,exist_ok=True)
    f.write_text(json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2,default=str)+'\n',encoding='utf-8')

def save_csv(df, file):
    f=Path(file);f.parent.mkdir(parents=True,exist_ok=True)
    df.to_csv(f,index=False,encoding='utf-8-sig',compression='gzip' if f.suffix=='.gz' else None)

def read_csv_fixed(file, expected_sha, n, date_bounds, no_outcomes=False):
    f=Path(file)
    if not f.is_file() or digest(f.read_bytes())!=expected_sha:fail('FROZEN_SOURCE_FILE_SHA_DRIFT_'+f.name)
    x=pd.read_csv(f,compression='gzip' if f.suffix=='.gz' else None,dtype={'code':str,'date':str},low_memory=False)
    if len(x)!=n or x[['date','code']].duplicated().any():fail('FROZEN_ASOF_KEYS_'+f.name)
    if not x.code.str.fullmatch('[0-9A-Z]{6}').all():fail('SECURITY_IDENTITY_BAD_'+f.name)
    if not x.date.between(*date_bounds).all():fail('SOURCE_DATE_DRIFT_'+f.name)
    if 'reference_data_last_bar_date' in x and not x.date.eq(x.reference_data_last_bar_date).all():
        fail('SOURCE_TIMESTAMP_DRIFT_'+f.name)
    if 'status_feature' in x and not x.status_feature.eq('ASOF_OK').all():fail('SOURCE_ASOF_STATUS')
    if no_outcomes and any(k in x.columns for k in OUTCOME_FORBIDDEN):fail('SOURCE_ASOF_HAS_FUTURE')
    return x

def _middle(high,low,n):
    h=pd.Series(high,dtype=float).rolling(n,min_periods=n).max()
    l=pd.Series(low,dtype=float).rolling(n,min_periods=n).min()
    return (h+l)/2

def make_asof(bars, signal_ix):
    """Exactly 224 contiguous signal-or-earlier KRX bars, causal cloud shift=26.

    Use bounded NumPy slices to avoid O(224) rolling indicator computation per
    event; same values as pandas rolling(9/26/52).shift(26) and BB ddof=0.
    """
    if len(bars)<224:return {'asof_status':'BLOCK_NO_224_BARS'}
    x=list(bars)[-224:]
    if [int(y[0]) for y in x]!=list(range(signal_ix-223,signal_ix+1)):
        return {'asof_status':'BLOCK_NONCONTIGUOUS_224'}
    a=np.asarray([y[1:] for y in x],dtype=float)
    if a.shape!=(224,6) or not np.isfinite(a).all():return {'asof_status':'BLOCK_NONFINITE_OHLCVA'}
    o,h,l,c,vol,amt=[a[:,k] for k in range(6)]
    if (np.minimum.reduce([o,h,l,c])<=0).any() or (vol<0).any() or (amt<0).any():
        return {'asof_status':'BLOCK_NONPOSITIVE_PRICE_OR_VOLUME_AMOUNT'}
    if (h+1e-8<np.maximum.reduce([o,c,l])).any() or (l-1e-8>np.minimum.reduce([o,c,h])).any():
        return {'asof_status':'BLOCK_INCONSISTENT_OHLC'}
    if (abs(c[1:]/c[:-1]-1)>0.40).any() or (abs(o[1:]/c[:-1]-1)>0.40).any():
        return {'asof_status':'BLOCK_UNADJUSTED_CORPORATE_ACTION_GAP_PROXY'}
    def mid(n,off=0):
        # off=26 means the historical source calculation ending at t-26,
        # which is the cloud plotted at the current candle coordinate t.
        hh=h[-n-off:-off or None];ll=l[-n-off:-off or None]
        return (hh.max()+ll.min())/2.
    ten=mid(9);kij=mid(26);old_ten=mid(9,1);old_kij=mid(26,1)
    origin_a=(ten+kij)/2.;origin_b=mid(52)
    displayed_a=(mid(9,26)+mid(26,26))/2.;displayed_b=mid(52,26)
    prev_a=(mid(9,27)+mid(26,27))/2.;prev_b=mid(52,27)
    top=max(displayed_a,displayed_b);bot=min(displayed_a,displayed_b)
    prevtop=max(prev_a,prev_b);prevbot=min(prev_a,prev_b)
    above=c[-1]>top;below=c[-1]<bot;inside=not above and not below
    out={'asof_status':'ASOF_OK','signal_close':float(c[-1]),
         'ma224_below':int(c[-1]<c.mean()),
         'cloud_a_displayed_today':float(displayed_a),'cloud_b_displayed_today':float(displayed_b),
         'cloud_origin_session_offset':-26,
         'cloud_a_projected_t_plus26_known_today':float(origin_a),
         'cloud_b_projected_t_plus26_known_today':float(origin_b),
         'cloud_top_today':float(top),'cloud_bottom_today':float(bot),
         'cloud_thickness_pct':float(100*(top-bot)/c[-1]),
         'cloud_distance_top_pct':float(100*(c[-1]/top-1)),
         'cloud_above':int(above),'cloud_inside':int(inside),'cloud_below':int(below),
         'cloud_bullish_displayed_today':int(displayed_a>displayed_b),
         'cloud_bullish_projected_known_today':int(origin_a>origin_b),
         'cloud_breakout_up_today':int(c[-2]<=prevtop and c[-1]>top),
         'cloud_full_reclaim_today':int(c[-2]<prevbot and c[-1]>top),
         'cloud_top_support_today':int(c[-2]>=prevtop and l[-1]<=1.01*top and c[-1]>=top),
         'tenkan_today':float(ten),'kijun_today':float(kij),
         'tenkan_above_kijun':int(ten>kij),
         'tenkan_cross_up_today':int(old_ten<=old_kij and ten>kij),
         'kijun_reclaim_today':int(c[-2]<old_kij and c[-1]>=kij)}
    for n in (20,30,40):
        # Exactly six consecutive contemporaneous lower bands t-5..t,
        # so five possible close-under -> close-over crosses are detected.
        w=np.lib.stride_tricks.sliding_window_view(c[-(n+5):],n)
        low_band=w.mean(axis=1)-2*w.std(axis=1,ddof=0)
        closes=c[-6:]
        cross=(closes[:-1]<low_band[:-1])&(closes[1:]>=low_band[1:])
        out[f'bb{n}_reclaim_today']=int(cross[-1]);out[f'bb{n}_reclaim_recent5']=int(cross.any())
        out[f'bb{n}_lower_today']=float(low_band[-1])
    out['bb_all3_recent5']=int(all(out[f'bb{n}_reclaim_recent5'] for n in (20,30,40)))
    return out

def frozen_inputs(root,p):
    r=Path(root);src=p['source']
    a=read_csv_fixed(r/'r1d9asof/R1D9_ASOF_ONLY_2021_23_NO_OUTCOME.csv.gz',src['r1d9_asof_csv_sha256'],7096,('2021-01-04','2023-11-29'),True)
    b=read_csv_fixed(r/'r1d19/reports/r1d19/result/historical/R1D19_ASOF_ONLY_BB_RECLAIM_NO_OUTCOME.csv.gz',src['r1d19_asof_csv_sha256'],5247,('2024-01-02','2025-09-30'),True)
    if not a.status_feature.eq('ASOF_OK').all() or not b.asof_status.eq('ASOF_OK').all():fail('INVALID_FROZEN_ASOF_FEATURES')
    d6=read_csv_fixed(r/'r1d6asof/R1D6_ASOF_ONLY_FROZEN_FEATURES_NO_OUTCOME.csv.gz',src['r1d6_asof_csv_sha256'],5247,('2024-01-02','2025-09-30'),True)
    if set(zip(d6.date,d6.code))!=set(zip(b.date,b.code)):fail('ORIGINAL_R1D6_VS_R1D19_KEYS')
    b=b.merge(d6[['date','code','signal_5MA_reclaim_today','signal_20MA_reclaim_today','amount3d_above_prior10','wave_gain_pct','pullback_intraday_max_retrace_pct','low_above_wave_low_pct','signal_amount_vs_prior5median','amount_3d_vs_prior10d']],on=['date','code'],validate='one_to_one')
    if a.date.nunique()!=689 or b.date.nunique()!=425:fail('FROZEN_SIGNAL_DATE_SET')
    if set(zip(a.date,a.code))&set(zip(b.date,b.code)):fail('CROSS_PERIOD_SAME_SIGNAL_KEY')
    return a,b

def collect_asof(krx, source_asofs, out):
    """Verify full raw SHA first, then collect all original events without altering selection."""
    from research_formulas import formula66_r1d6_oos_holdout as d6
    src=d6.source_manifest(krx)
    calendar,idx=d6.index_asof(krx)
    if set(src)!=set(calendar) or d6.verify_all_daily_raw_sha(src)!=1907:fail('1907_KRX_SOURCE_SHA_FAIL')
    if calendar[0]!='2019-01-02' or calendar[-1]!='2026-10-12':fail('KRX_SESSION_CALENDAR_DRIFT')
    targets=defaultdict(set);all_codes=set()
    for a in source_asofs.values():
        for date,code in a.index:
            targets[date].add(code);all_codes.add(code)
    end=max(targets)
    history=defaultdict(lambda:deque(maxlen=224))
    result={key:[] for key in source_asofs}
    seen=set();invalid_counter=defaultdict(int)
    for si,day in enumerate(calendar[:calendar.index(end)+1]):
        df=d6.priceday(src[day][0],day)
        found=set()
        # Track only tickers present in the immutable union of both cohorts.
        for item in df.itertuples(index=False):
            code=str(item.code)
            if code not in all_codes:continue
            tup=(si,float(item.open),float(item.high),float(item.low),float(item.close),float(item.volume),float(item.amount))
            history[code].append(tup)
            if code not in targets.get(day,set()):continue
            if (day,code) in seen:fail('DUPLICATE_SOURCE_SIGNAL')
            seen.add((day,code));found.add(code)
            feats=make_asof(history[code],si)
            cohort='holdout_2021_23' if day<'2024-01-02' else 'exploratory_2024_25'
            old=source_asofs[cohort].loc[(day,code)]
            if feats['asof_status']=='ASOF_OK':
                expected=(float(old.wave_peak_high)-float(old.pullback_close_retrace_pct)/100*(float(old.wave_peak_high)-float(old.wave_trough_low))) if cohort=='holdout_2021_23' else float(old.signal_close)
                if not np.isclose(feats['signal_close'],expected,rtol=0,atol=1e-5):
                    fail('SOURCE_SIGNAL_CLOSE_PRICE_MISMATCH_'+cohort+'_'+day+'_'+code)
                if cohort=='exploratory_2024_25':
                    for n in (20,30,40):
                        for suff in ('reclaim_today','reclaim_recent5'):
                            k=f'bb{n}_{suff}'
                            if feats[k]!=int(old[k]):fail('R1D19_BB_ASOF_PARITY_'+k+'_'+day+'_'+code)
                    if feats['ma224_below']!=int(old.ma224_below):fail('R1D19_MA224_PARITY')
            else: invalid_counter[feats['asof_status']]+=1
            result[cohort].append({'date':day,'code':code,**feats})
        if targets.get(day,set())-found:fail('FROZEN_SECURITY_MISSING_ON_DAY_'+day)
    union=set().union(*[set(zip(a.index.get_level_values(0),a.index.get_level_values(1))) for a in source_asofs.values()])
    if union!=seen or len(result['holdout_2021_23'])!=7096 or len(result['exploratory_2024_25'])!=5247:
        fail('FROZEN_SIGNAL_POPULATION_MISMATCH')
    output={}
    for cohort,rows in result.items():
        df=pd.DataFrame(rows).sort_values(['date','code']).reset_index(drop=True)
        if any(x in df.columns for x in OUTCOME_FORBIDDEN):fail('ASOF_RESULT_LEAKAGE')
        path=Path(out)/(cohort+'_ASOF_ONLY_NO_OUTCOME.csv.gz')
        save_csv(df,path)
        output[cohort]=(df,digest(path.read_bytes()))
    return output,{'krx_sha256_daily_validated':1907,'frozen_raw_ticker_date_keys':len(seen),'qa_block_counts':dict(invalid_counter),'historical_raw_unadjusted':True,'corporate_actions_verified':False,'pit_security_type_verified':False,'future_candle_read':False,'outcomes_loaded_at_capture':False}

def read_sealed_outcome(file,expected,complete20):
    x=pd.read_csv(file,compression='gzip',dtype={'code':str,'date':str},low_memory=False)
    if len(x)!=expected*4 or x[['date','code','horizon']].duplicated().any() or set(x.horizon)!=set(HORIZONS):
        fail('FROZEN_OUTCOME_PATH_SCHEMA')
    if len(x[x.horizon.eq(20)&x.status_out.eq(STATUS)])!=complete20:fail('ORIGINAL_D20_COUNT_DRIFT')
    return x

def hypothesis_masks(df):
    c={}
    for n in (20,30,40):
        q=df[f'bb{n}_reclaim_recent5'].eq(1)
        c[f'bb{n}_reclaim_recent5']=q
        c[f'bb{n}_reclaim_recent5_AND_cloud_breakout_up_today']=q & df.cloud_breakout_up_today.eq(1)
        c[f'bb{n}_reclaim_recent5_AND_cloud_above']=q & df.cloud_above.eq(1)
    c['bb20_reclaim_recent5_AND_signal_5MA_reclaim_today']=df.bb20_reclaim_recent5.eq(1)&df.signal_5MA_reclaim_today.eq(1)
    c['bb30_reclaim_recent5_AND_signal_20MA_reclaim_today']=df.bb30_reclaim_recent5.eq(1)&df.signal_20MA_reclaim_today.eq(1)
    c['bb40_reclaim_recent5_AND_amount3d_above_prior10']=df.bb40_reclaim_recent5.eq(1)&df.amount3d_above_prior10.eq(1)
    for name in ('cloud_breakout_up_today','cloud_above','cloud_inside','cloud_below','tenkan_above_kijun','tenkan_cross_up_today','kijun_reclaim_today'):
        c[name]=df[name].eq(1)
    return c

def _score(data,name,val,cohort,segment='ALL'):
    q=data[data[name].eq(val)] if name!='ALL' else data
    ok=q[q.status_out.eq(STATUS)]
    ret=ok.net_20bps_pct
    n=len(ok)
    return {'period':cohort,'segment':segment,'factor':name,'value':val,'horizon':int(data.horizon.iloc[0]) if len(data) else None,
            'events':len(q),'complete':n,'blocked_outcomes':len(q)-n,
            'unique_dates':ok.date.nunique(),'unique_codes':ok.code.nunique(),
            'win_rate_pct':100*ret.gt(0).mean() if n else np.nan,
            'median_net_pct':ret.median() if n else np.nan,'mean_net_pct':ret.mean() if n else np.nan,
            'loss_le_minus5_pct':100*ret.le(-5).mean() if n else np.nan,
            'gain_ge_plus5_pct':100*ret.ge(5).mean() if n else np.nan,
            'median_mae_pct':ok.mae_intraday_pct.median() if n else np.nan,
            'median_mfe_pct':ok.mfe_intraday_pct.median() if n else np.nan,
            'median_giveback_pp':(ok.mfe_intraday_pct-(ret+0.2)).median() if n else np.nan,
            'warn_small_sample':int(n<30 or ok.date.nunique()<8),
            'not_prospective_oos':1}

def analyze(cohorts,source_asofs,outcomes,protocol,out):
    summaries=[];paired=[];winners=[];quality=[];overlaps=[];merged_out={}
    for cohort,(feats,seal) in cohorts.items():
        src=source_asofs[cohort].reset_index()
        j=feats.merge(src[['date','code','signal_5MA_reclaim_today','signal_20MA_reclaim_today','amount3d_above_prior10','wave_gain_pct','pullback_intraday_max_retrace_pct','low_above_wave_low_pct','signal_amount_vs_prior5median','amount_3d_vs_prior10d']],on=['date','code'],validate='one_to_one')
        masks=hypothesis_masks(j)
        if set(masks)!=set(protocol['hypotheses_locked_before_R1D20_period_labels']):fail('HYPOTHESIS_LIST_DRIFT')
        for k,v in masks.items():j[k+'_flag']=v.astype(int)
        paths=outcomes[cohort]
        if set(zip(paths.date,paths.code))!=set(zip(feats.date,feats.code)):fail('OUTCOME_KEYS_NOT_FROZEN')
        joined=paths.merge(j,on=['date','code'],validate='many_to_one')
        if len(joined)!=len(j)*4:fail('OUTCOME_JOIN_ROW_MULTIPLICATION')
        joined['first_code']=(joined.groupby('code').date.transform('min')==joined.date).astype(int)
        merged_out[cohort]=joined
        valid=joined[joined.asof_status.eq('ASOF_OK')].copy()
        flags=list(masks)
        for h,g in valid.groupby('horizon'):
            summaries.append(_score(g,'ALL','ALL',cohort))
            for factor in flags:
                for v in (0,1):
                    summaries.append(_score(g,factor+'_flag',v,cohort))
                # Preaggregate by (date, flag) once, rather than thousands of
                # nested per-date dataframe scans. Days with both flags exist
                # contribute one equally weighted paired contrast each.
                done=g[g.status_out.eq(STATUS)].copy()
                done['_win']=done.net_20bps_pct.gt(0).astype(int)
                done['_fail5']=done.net_20bps_pct.le(-5).astype(int)
                d=done.groupby(['date',factor+'_flag'],sort=True)[['_win','_fail5']].mean().unstack(factor+'_flag')
                if 0 in d['_win'] and 1 in d['_win']:
                    matched=d.dropna(subset=[('_win',0),('_win',1)])
                    nmatched=len(matched)
                    w=(matched[('_win',1)]-matched[('_win',0)]).mean()*100 if nmatched else np.nan
                    risk=(matched[('_fail5',1)]-matched[('_fail5',0)]).mean()*100 if nmatched else np.nan
                else:
                    nmatched=0;w=np.nan;risk=np.nan
                paired.append({'period':cohort,'factor':factor,'horizon':h,'matched_dates':nmatched,
                               'equal_day_win_diff_pp':w,'equal_day_minus5risk_diff_pp':risk,
                               'weak_support':int(nmatched<8),'not_causal':True})
        # Predeclared robustness: same signal-day MA224 phase, first-ever
        # ticker appearance within the independent cohort, and calendar year.
        selected=('bb20_reclaim_recent5','bb30_reclaim_recent5','bb40_reclaim_recent5',
                  'bb20_reclaim_recent5_AND_signal_5MA_reclaim_today',
                  'bb30_reclaim_recent5_AND_signal_20MA_reclaim_today',
                  'bb40_reclaim_recent5_AND_amount3d_above_prior10',
                  'cloud_above','cloud_below','cloud_breakout_up_today')
        for h,g in valid.groupby('horizon'):
            groups=[('MA224_BELOW',g[g.ma224_below.eq(1)]),
                    ('MA224_ABOVE_OR_EQUAL',g[g.ma224_below.eq(0)]),
                    ('FIRST_SIGNAL_PER_TICKER',g[g.first_code.eq(1)])]
            groups.extend([('YEAR_'+str(year),part) for year,part in g.groupby(g.date.str[:4])])
            for label,sub in groups:
                if not len(sub):continue
                summaries.append(_score(sub,'ALL','ALL',cohort,segment=label))
                for name in selected:
                    summaries.append(_score(sub,name+'_flag',1,cohort,segment=label))
        five=valid[(valid.horizon==5)&valid.status_out.eq(STATUS)]
        for n in (20,30,40):
            hits=five[five[f'bb{n}_reclaim_recent5_flag']==1]
            for typ,q in [('WIN',hits[hits.net_20bps_pct>0]),('LOSS',hits[hits.net_20bps_pct<=0])]:
                vals={'period':cohort,'band':n,'outcome':typ,'n':len(q),'unique_dates':q.date.nunique()}
                for col in ('wave_gain_pct','pullback_intraday_max_retrace_pct','low_above_wave_low_pct','amount_3d_vs_prior10d','signal_amount_vs_prior5median','cloud_thickness_pct','cloud_distance_top_pct'):
                    vals[col+'_median']=q[col].median() if len(q) else np.nan
                winners.append(vals)
        # Outcome cannot be joined for rejected ASOF features; preserve rejection explicitly.
        qa=feats[['date','code','asof_status']].copy();qa['period']=cohort
        quality.append(qa)
        active=feats[feats.asof_status.eq('ASOF_OK')]
        for n in (20,30,40):
            for m in (20,30,40):
                if n>=m:continue
                overlaps.append({'period':cohort,'pair':f'BB{n}_BB{m}',
                                 'both_recent5':int((active[f'bb{n}_reclaim_recent5'].eq(1)&active[f'bb{m}_reclaim_recent5'].eq(1)).sum()),
                                 'band_left':int(active[f'bb{n}_reclaim_recent5'].eq(1).sum()),
                                 'band_right':int(active[f'bb{m}_reclaim_recent5'].eq(1).sum())})
    out=Path(out)
    save_csv(pd.DataFrame(summaries),out/'R1D20_FIXED_HYPOTHESES_D5_D10_D15_D20_SCORECARDS.csv')
    save_csv(pd.DataFrame(paired),out/'R1D20_SAME_DATE_CLUSTER_COMPARISON.csv')
    save_csv(pd.DataFrame(winners),out/'R1D20_WIN_LOSS_ASOF_FEATURES.csv')
    save_csv(pd.DataFrame(overlaps),out/'R1D20_BB_OVERLAP_COUNTS.csv')
    save_csv(pd.concat(quality,ignore_index=True),out/'R1D20_ALL_EVENT_QUALITY.csv.gz')
    return {'score_rows':len(summaries),'date_comparison_rows':len(paired),
            'original_2021_23_D20_complete':int(merged_out['holdout_2021_23'].query('horizon==20').status_out.eq(STATUS).sum()),
            'original_2024_25_D20_complete':int(merged_out['exploratory_2024_25'].query('horizon==20').status_out.eq(STATUS).sum())}

def run(args):
    p=load_protocol(args.protocol);out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    if args.mode=='lock':
        save_json({'revision':VERSION,'hypotheses_locked':p['hypotheses_locked_before_R1D20_period_labels'],
                   'signal_periods':p['comparison_periods'],'outcome_labels_read':False,
                   'orders_placed':0,'prospective_oos':False},out/'R1D20_LOCK_MANIFEST.json')
        print('R1D20_PROTOCOL_LOCKED_NO_OUTCOMES');return
    if args.mode!='validate':fail('UNEXPECTED_MODE')
    a,b=frozen_inputs(args.inputs,p)
    original={'holdout_2021_23':a.set_index(['date','code'],verify_integrity=True),
              'exploratory_2024_25':b.set_index(['date','code'],verify_integrity=True)}
    sealed,qa=collect_asof(args.krx_root,original,out)
    # Verify seal hashes and existence BEFORE reading sealed D+5/10/15/20 outcomes.
    for cohort,(_,sha) in sealed.items():
        file=out/(cohort+'_ASOF_ONLY_NO_OUTCOME.csv.gz')
        if digest(file.read_bytes())!=sha:fail('ASOF_FEATURE_SEAL_DRIFT')
    root=Path(args.inputs)
    z={
      'holdout_2021_23':read_sealed_outcome(root/'r1d9out/reports/r1d9/result/R1D9_SEALED_2021_23_D5_D10_D15_D20.csv.gz',7096,7047),
      'exploratory_2024_25':read_sealed_outcome(root/'r1d7/result/R1D7_D5_D10_D15_D20_FROZEN_FORWARD_PRICE_PATHS.csv.gz',5247,5214)}
    metrics=analyze(sealed,original,z,p,out)
    manifest={'version':VERSION,'research_only':True,'fixed_hypotheses':len(p['hypotheses_locked_before_R1D20_period_labels']),
              'original_2021_23_signals':len(a),'original_2024_25_signals':len(b),
              'original_2021_23_path_rows':len(z['holdout_2021_23']),
              'original_2024_25_path_rows':len(z['exploratory_2024_25']),
              'asof_seals_sha256':{k:v[1] for k,v in sealed.items()},
              'asof_valid_counts':{k:int(v[0].asof_status.eq('ASOF_OK').sum()) for k,v in sealed.items()},
              'asof_block_counts':{k:v[0].loc[v[0].asof_status.ne('ASOF_OK'),'asof_status'].value_counts().to_dict() for k,v in sealed.items()},
              **qa,**metrics,'lookahead_certified_within_technical_feature_window':True,
              'market_membership_pit_certified':False,'corporate_actions_pit_certified':False,
              'future_independent_oos':False,'not_native_66_searches':True,
              'no_live_changes':True,'orders_placed':0,'strategy_ready':False}
    save_json(manifest,out/'R1D20_MANIFEST.json')
    print('R1D20_HISTORICAL_DATE_DISJOINT_QA_COMPLETE',json.dumps(manifest,ensure_ascii=False),flush=True)

def main():
    a=argparse.ArgumentParser()
    a.add_argument('--mode',choices=('lock','validate'),required=True)
    a.add_argument('--protocol',default='R1D20_PREDECLARED_PROTOCOL.json')
    a.add_argument('--inputs',default='reports/r1d20/input')
    a.add_argument('--krx-root',default='reports/r1d20/input/krx')
    a.add_argument('--out',default='reports/r1d20/result')
    run(a.parse_args())

if __name__=='__main__':main()
