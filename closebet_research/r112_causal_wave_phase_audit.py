"""R1.12: causal 224-session origin, wave-1 impulse, pullback and recovery research.
Research-only; dated KRX unadjusted OHLCV, no changes to strategy/LIVE/order authority.
All wave states use bars available THROUGH the observed signal session only.
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
from closebet_research.r111_episode_first_strength_audit import (
    build_archives, src_verify, unique_raw, verify_krx, sha, require,
)

R111_EPISODES_SHA='dd1c71077e829779be15546fb705ae9a5fa6103d6f7d6d24629d404be7015c4a'
OUTPUT_STATUS='PASS_EXPLORATORY_NOT_TRADE_APPROVED'
# These coarse hypotheses are frozen BEFORE performance comparison.
THRESHOLDS={
 'origin_lookback':40,'minimum_history':270,'wave_min_gain_pct':16,
 'impulse_volume_5_to_previous_20':1.25,'minimum_pullback_from_peak_pct':8,
 'pullback_retrace_min_fraction':0.20,'pullback_retrace_max_fraction':0.80,
 'pullback_volume_5_to_impulse_5_max':0.75,
 'pullback_invalid_retrace_fraction':0.85,'max_wave_age_sessions':45,
 'rebound_close_above_previous_high':True,'rebound_volume_to_prior5':1.20,
 'rebound_marker_sessions':7, 'corp_action_abs_close_change_limit':0.35,
}
FIELDS=['open','high','low','close','volume']


def load_historical_krx(daily_index, calendar, stock_codes, start='2025-01-02'):
    """Checks the SHA of EVERY compressed daily blob; materializes 2025+ only for selected stocks."""
    blocks=defaultdict(list)
    used=0
    for day in calendar:
        z,r=daily_index[day]
        raw=z.read(r['file'])
        require(sha(raw)==r['sha256'],'HISTORICAL_KRX_ARCHIVE_CONTENT:'+day)
        used+=1
        if day<start:continue
        df=pd.read_csv(io.BytesIO(raw),compression='gzip',dtype={'code':str})
        require(len(df)==int(r['rows']),'DATE_ROW_COUNT:'+day)
        require(df.code.str.fullmatch('[0-9]{6}').all(),'DATE_CODE_FORMAT:'+day)
        require(not df.code.duplicated().any(),'DATE_DUPLICATE_CODE:'+day)
        require(df.date.eq(day).all(),'DATE_METADATA:'+day)
        require(set(FIELDS).issubset(df.columns),'DATE_MISSING_OHLCV:'+day)
        df=df[df.code.isin(stock_codes)]
        for r0 in df.itertuples(index=False):
            blocks[r0.code].append((day, float(r0.open),float(r0.high),float(r0.low),float(r0.close),float(r0.volume)))
    data={}
    for code,records in blocks.items():
        d=pd.DataFrame(records,columns=['date']+FIELDS)
        require(not d.date.duplicated().any(),'DUPLICATED_DAILY_STOCK:'+code)
        nontrade=(d[['open','high','low','volume']]==0).all(axis=1) & d.close.gt(0)
        traded=d.volume.gt(0) & d.open.gt(0) & d.low.gt(0) & d.high.gt(0) & d.close.gt(0)
        require((nontrade|traded).all(),'UNEXPLAINED_INVALID_BAR:'+code)
        require((d.loc[traded,'high']>=d.loc[traded,['open','close','low']].max(axis=1)).all(),'HIGH_INCONSISTENT:'+code)
        require((d.loc[traded,'low']<=d.loc[traded,['open','close','high']].min(axis=1)).all(),'LOW_INCONSISTENT:'+code)
        # Report, but do NOT fill/repair suspended records (zero open/high/low/vol).
        data[code]=d.set_index('date')
    require(len(data)==len(stock_codes),'HISTORY_MISSING_SOME_SELECTED_STOCKS')
    return data,used


def _reset():
    return {'state':'NO_CONFIRMED_WAVE', 'origin_ix':None,'origin_low':None,
            'impulse_ix':None,'peak_ix':None,'peak_high':None,'impulse_vol':None,
            'pullback_low':None,'pullback_ix':None,'rebound_ix':None}


def causal_daily_states(d):
    """Chronological state machine. No future highs/lows, pivot hindsight or target labels.

    The causal 'first wave' is a PROXY; not an independently hand-labelled true first wave.
    """
    q=d.reset_index().copy()
    n=len(q)
    vclose=q.close.to_numpy(dtype='float64'); vlow=q.low.to_numpy(dtype='float64')
    vhigh=q.high.to_numpy(dtype='float64');vol=q.volume.to_numpy(dtype='float64')
    ma224=q.close.rolling(224,min_periods=224).mean().to_numpy()
    invalid_day=(q[['open','high','low','volume']]<=0).any(axis=1).to_numpy()
    states=[]; st=_reset();T=THRESHOLDS
    for i in range(n):
        dt=str(q.date.iloc[i]); state=st['state']
        # A 35% close-to-close discontinuity in the previous 224 sessions is quarantined.
        p0=max(1,i-223)
        jumps=np.abs(vclose[p0:i+1]/vclose[p0-1:i]-1) if i>=1 else np.array([])
        split_suspect=bool((jumps>T['corp_action_abs_close_change_limit']).any())
        suspended_recent=bool(invalid_day[max(0,i-223):i+1].any())
        valid=i>=T['minimum_history'] and np.isfinite(ma224[i]) and all(
            np.isfinite(x) and x>0 for x in (vclose[i],vlow[i],vhigh[i])) and vol[i]>0
        # State transitions are only evaluated from the information available by this close.
        if not valid or split_suspect or suspended_recent:
            st=_reset()
            stage=('CORP_ACTION_QUARANTINE' if split_suspect else
                   'SUSPENSION_224_QUARANTINE' if suspended_recent else 'INSUFFICIENT_HISTORY')
        else:
            # End an old wave before considering new origins.
            if st['impulse_ix'] is not None and i-st['impulse_ix']>T['max_wave_age_sessions']:
                st=_reset()
            if st['state']=='REBOUND_CONFIRMED' and i-st['rebound_ix']>T['rebound_marker_sessions']:
                st=_reset()
            if st['state']=='NO_CONFIRMED_WAVE':
                lb=max(224,i-T['origin_lookback']); window=vlow[lb:i]
                if len(window) >= 5:
                    anchor=lb+int(np.argmin(window)); floor=float(vlow[anchor]);base=float(ma224[anchor])
                    denom=np.mean(vol[i-25:i-5]) if i>=25 else 0
                    vr=np.mean(vol[i-5:i]) / denom if denom>0 else 0
                    current_gain=(vclose[i]/floor-1)*100
                    if (floor<base and current_gain>=T['wave_min_gain_pct']
                        and vr>=T['impulse_volume_5_to_previous_20']
                        and vclose[i]>=np.max(vclose[i-5:i])):
                        st={'state':'FIRST_WAVE_ACTIVE','origin_ix':anchor,'origin_low':floor,
                            'impulse_ix':i,'peak_ix':i,'peak_high':float(vhigh[i]),
                            'impulse_vol':float(np.mean(vol[i-4:i+1])),
                            'pullback_low':None,'pullback_ix':None,'rebound_ix':None}
            elif st['state']=='FIRST_WAVE_ACTIVE':
                if vhigh[i]>=st['peak_high']:
                    st['peak_ix']=i;st['peak_high']=float(vhigh[i])
                    st['impulse_vol']=max(st['impulse_vol'],float(np.mean(vol[max(0,i-4):i+1])))
                # Do not classify a decline as a pullback until a prior peak exists.
                elif i>st['peak_ix'] and vclose[i]<=st['peak_high']*(1-T['minimum_pullback_from_peak_pct']/100):
                    st['state']='PULLBACK_UNCONFIRMED'
                    st['pullback_low']=float(vlow[i]);st['pullback_ix']=i
            elif st['state'] in ('PULLBACK_UNCONFIRMED','PULLBACK_QUALITY'):
                if vlow[i]<st['pullback_low']:
                    st['pullback_low']=float(vlow[i]);st['pullback_ix']=i
                wave_height=st['peak_high']-st['origin_low']
                retrace=(st['peak_high']-st['pullback_low'])/wave_height if wave_height>0 else np.inf
                if (retrace>=T['pullback_invalid_retrace_fraction'] or vclose[i]<st['origin_low']):
                    st=_reset()
                else:
                    dvol=float(np.mean(vol[max(0,i-4):i+1]))
                    dry=(dvol/st['impulse_vol']) if st['impulse_vol']>0 else np.inf
                    valid_depth=T['pullback_retrace_min_fraction']<=retrace<=T['pullback_retrace_max_fraction']
                    if dry<=T['pullback_volume_5_to_impulse_5_max'] and valid_depth:
                        st['state']='PULLBACK_QUALITY'
                    else:
                        st['state']='PULLBACK_UNCONFIRMED'
                    # A rebound marker is a confirmed CLOSE; next open is earliest actionable entry.
                    prior5=np.mean(vol[i-5:i]) if i>=5 else np.inf
                    if (st['state']=='PULLBACK_QUALITY' and i>st['pullback_ix']
                        and vclose[i]>vhigh[i-1] and vol[i]>=T['rebound_volume_to_prior5']*prior5):
                        st['state']='REBOUND_CONFIRMED';st['rebound_ix']=i
            stage=st['state']
        x={'date':dt,'stage':stage,'ma224':float(ma224[i]) if np.isfinite(ma224[i]) else np.nan,
           'prior_224_corporate_action_suspect':split_suspect,
           'prior_224_suspended_session':suspended_recent,
           'day_suspended':bool(invalid_day[i]),
           'origin_date':str(q.date.iloc[st['origin_ix']]) if st['origin_ix'] is not None else '',
           'impulse_date':str(q.date.iloc[st['impulse_ix']]) if st['impulse_ix'] is not None else '',
           'peak_date_asof':str(q.date.iloc[st['peak_ix']]) if st['peak_ix'] is not None else '',
           'origin_low':st['origin_low'] if st['origin_low'] is not None else np.nan,
           'peak_high_asof':st['peak_high'] if st['peak_high'] is not None else np.nan,
           'age_from_impulse':i-st['impulse_ix'] if st['impulse_ix'] is not None else np.nan,
           'rebound_date':str(q.date.iloc[st['rebound_ix']]) if st['rebound_ix'] is not None else ''}
        if st['origin_low'] is not None and st['peak_high']>st['origin_low']:
            x['origin_to_peak_pct']=100*(st['peak_high']/st['origin_low']-1)
            x['current_retrace_fraction']=(st['peak_high']-vclose[i])/(st['peak_high']-st['origin_low'])
            x['pullback_low_retrace_fraction']=(st['peak_high']-st['pullback_low'])/(st['peak_high']-st['origin_low']) if st['pullback_low'] is not None else np.nan
            x['last5_to_impulse_volume']=np.mean(vol[i-4:i+1])/st['impulse_vol'] if st['impulse_vol']>0 else np.nan
        else:
            x.update({'origin_to_peak_pct':np.nan,'current_retrace_fraction':np.nan,'pullback_low_retrace_fraction':np.nan,'last5_to_impulse_volume':np.nan})
        states.append(x)
    return pd.DataFrame(states)


def run(r171,r110,r111,history,out):
    raw,feats,source=src_verify(r171,r110)
    p=Path(r111)/'r111_all_selected_episode_1059.csv'
    mf=Path(r111)/'r111_integrity_manifest.json'
    require(p.is_file() and mf.is_file(),'R111_SOURCE_MISSING')
    require(sha(p.read_bytes())==R111_EPISODES_SHA,'R111_EPISODES_SHA_MISMATCH')
    m=json.loads(mf.read_text());require(m['status']=='PASS_DESCRIPTIVE_ONLY_NOT_TRADE_APPROVED','R111_STATUS')
    require(m['all_selected_unique_signals']==1059 and m['mature_D20']==570,'R111_PROVENANCE')
    episodes=pd.read_csv(p,dtype={'code':str});require(len(episodes)==1059,'R111_EPISODES_COUNT')
    zips,arch,cal=build_archives(history)
    allraw=unique_raw(raw)
    tickers=set(allraw.code)
    history_data,sha_dates=load_historical_krx(arch,cal,tickers)
    # KRX can encode suspended symbols as O/H/L/Vol=0, last close retained.
    # Preserve these rows as exceptions, NEVER synthesize tradeable prices.
    suspended_rows=[]
    for code,td in history_data.items():
        mask=(td[['open','high','low','volume']]==0).all(axis=1) & td.close.gt(0)
        for day,q in td.loc[mask].iterrows():
            suspended_rows.append({'code':code,'date':day,'open':q.open,'high':q.high,
                                   'low':q.low,'close':q.close,'volume':q.volume,
                                   'status':'NONTRADEABLE_RETAINED_CLOSE_NO_FILL'})
    # Signal OHLCV checking of ALL 1059 native selected bars.
    features=[];bar_verified=0;corp_flags=0
    grps={c:g for c,g in allraw.groupby('code')}
    for code,g in sorted(grps.items()):
        df=history_data[code]
        state=causal_daily_states(df)
        stage_by_day=state.set_index('date')
        for _,r in g.iterrows():
            day=r.signal_date.strftime('%Y-%m-%d')
            require(day in df.index,'SIGNAL_MISSING_DAY:'+code+':'+day)
            actual=df.loc[day]
            for rk,k in {'_open':'open','_high':'high','_low':'low','_close':'close','_vol':'volume'}.items():
                require(float(r[rk])==float(actual[k]),'SIGNAL_BAR_DISAGREEMENT:'+code+':'+day+':'+k)
            bar_verified+=1
            require(day in stage_by_day.index,'SIGNAL_MISSING_STATE:'+code+':'+day)
            rec=stage_by_day.loc[day].to_dict()
            rec.update({'signal_date':day,'code':code,'entry_close':float(actual.close),
                        'historical_rows_for_code':len(df)})
            if rec['prior_224_corporate_action_suspect']: corp_flags+=1
            features.append(rec)
    for z in zips:z.close()
    feat=pd.DataFrame(features).sort_values(['signal_date','code']).drop(columns='date',errors='ignore')
    feat['signal_date']=pd.to_datetime(feat['signal_date'],format='%Y-%m-%d')
    prior=episodes[['signal_date','code','episode_id_c10','episode_first_c10','episode_n_c10']].copy()
    prior['signal_date']=pd.to_datetime(prior['signal_date'],format='%Y-%m-%d')
    require(not prior[['signal_date','code']].duplicated().any(),'R111_DUPLICATED_KEYS')
    y=feat.merge(prior,on=['signal_date','code'],how='left',validate='one_to_one',indicator=True)
    require(y._merge.eq('both').all(),'R111_MATCH_MISSING');y=y.drop(columns='_merge')
    covered=y.merge(feats[['signal_date','code','D5_complete','D20_complete','D5_close_pct','D5_max_high_pct',
                           'D5_min_low_pct','D20_close_pct','D20_max_high_pct','D20_min_low_pct',
                           'outcome_class','prior_224_jump_above35pct','primary_mode_for_display']],
                    on=['signal_date','code'],how='inner',validate='one_to_one')
    require(len(y)==1059 and len(covered)==754 and int(covered.D20_complete.sum())==570,'COUNT_INVARIANT')
    assert set(covered.D20_complete.unique()).issubset({0,1})
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(suspended_rows,columns=['code','date']+FIELDS+['status']).sort_values(
        ['code','date']).to_csv(out/'r112_krx_nontradeable_zero_ohlcv_audit.csv',index=False,encoding='utf-8-sig')
    y.to_csv(out/'r112_all_selected_1059_pit_wave_stage.csv',index=False,encoding='utf-8-sig')
    covered.to_csv(out/'r112_mature_recent_754_wave_x_outcome.csv',index=False,encoding='utf-8-sig')
    groups=[]
    for cohort,subset,horizon in [('OLDER_D20',covered[covered.D20_complete==1],20),
                                   ('OLDER_D5',covered[covered.D20_complete==1],5),
                                   ('RECENT_D5',covered[covered.D20_complete==0],5)]:
        pct=f'D{horizon}_close_pct'; high=f'D{horizon}_max_high_pct'; low=f'D{horizon}_min_low_pct'
        for (phase,first),g in subset.groupby(['stage','episode_first_c10'],dropna=False):
            groups.append({'cohort':cohort,'stage':phase,'first_in_10d_episode':int(first),
                           'n':len(g),'unique_codes':g.code.nunique(),'unique_days':g.signal_date.nunique(),
                           'close_win_pct':100*(g[pct]>0).mean(),'close_mean_pct':g[pct].mean(),
                           'close_median_pct':g[pct].median(),'high5_touch_pct':100*(g[high]>=5).mean(),
                           'mean_max_high_pct':g[high].mean(),'mean_min_low_pct':g[low].mean(),
                           'corp_action_suspect_n':int(g.prior_224_corporate_action_suspect.sum()),
                           'recent_suspension_n':int(g.prior_224_suspended_session.sum())})
    # One executable-timing proxy: signal stage known at D0 close -> D+1 OPEN,
    # exit D+5 CLOSE, no stop/TP, assumed 0.5% round-trip, no slippage.
    # A suspended or absent bar is never implicitly filled with a fabricated price.
    cal_ix={dt:i for i,dt in enumerate(cal)}
    entry_rows=[]
    for r in covered.itertuples(index=False):
        dt=r.signal_date.strftime('%Y-%m-%d');i=cal_ix[dt];code=r.code
        next_days=cal[i+1:i+6]
        q=history_data[code]
        complete=(len(next_days)==5 and all(x in q.index for x in next_days))
        if complete:
            test=q.loc[next_days]
            complete=bool(test[FIELDS].notna().all().all() and
                (test[['open','high','low','close','volume']]>0).all().all())
        if complete:
            p0=float(test.open.iloc[0]);p1=float(test.close.iloc[4])
            original=(p1/float(r.entry_close)-1)*100
            require(abs(original-float(r.D5_close_pct))<1e-7,'D5_FORWARDBAR_VALIDATION:'+code+':'+dt)
            gross=(p1/p0-1)*100
            net=gross-0.5
        else:
            p0=np.nan;p1=np.nan;gross=np.nan;net=np.nan
        entry_rows.append({'signal_date':dt,'code':code,'stage_at_D0_close':r.stage,
                           'cohort':'OLDER_D20' if r.D20_complete==1 else 'RECENT_D5',
                           'episode_first_c10':int(r.episode_first_c10),
                           'corp_action_or_suspension_suspect':bool(r.prior_224_corporate_action_suspect or r.prior_224_suspended_session),
                           'D1_open':p0,'D5_close':p1,'entry_fill_possible_on_daily_bars':int(complete),
                           'gross_D1open_D5close_pct':gross,'illustrative_net_05pct_cost':net})
    entry=pd.DataFrame(entry_rows)
    require(len(entry)==754,'ENTRY_ROWS_LOST')
    entry.to_csv(out/'r112_d1open_to_d5close_entry_policy.csv',index=False,encoding='utf-8-sig')
    ep=[]
    for (cohort,stage),sub in entry.groupby(['cohort','stage_at_D0_close']):
        valid=sub[(sub.entry_fill_possible_on_daily_bars==1)&(~sub.corp_action_or_suspension_suspect)]
        ep.append({'cohort':cohort,'stage':stage,'original_signals':len(sub),'valid_study_signals':len(valid),
                   'n_codes':valid.code.nunique(),'n_signal_days':valid.signal_date.nunique(),
                   'mean_gross_pct':valid.gross_D1open_D5close_pct.mean(),
                   'mean_illustrative_net_pct':valid.illustrative_net_05pct_cost.mean(),
                   'positive_net_pct':100*(valid.illustrative_net_05pct_cost>0).mean() if len(valid) else np.nan,
                   'fill_valid_ratio_pct':100*len(valid)/len(sub) if len(sub) else np.nan})
    pd.DataFrame(ep).to_csv(out/'r112_d1open_entry_stage_summary.csv',index=False,encoding='utf-8-sig')

    cohorts=pd.DataFrame(groups).sort_values(['cohort','stage','first_in_10d_episode'])
    cohorts.to_csv(out/'r112_stage_episode_cohort_audit.csv',index=False,encoding='utf-8-sig')
    phaseg=[]
    for cohort,subset,horizon in [('OLDER_D20',covered[covered.D20_complete==1],20),('RECENT_D5',covered[covered.D20_complete==0],5)]:
        for stage,g in subset.groupby('stage'):
            g0=g[~g.prior_224_corporate_action_suspect & ~g.prior_224_suspended_session]
            phaseg.append({'cohort':cohort,'stage':stage,'all_signals':len(g),'quarantined_corp_or_suspension':len(g)-len(g0),
                           'clean_signals':len(g0),'clean_first_episode_n':int(g0.episode_first_c10.sum()),
                           'clean_close_win_pct':100*(g0[f'D{horizon}_close_pct']>0).mean() if len(g0) else np.nan,
                           'clean_close_mean_pct':g0[f'D{horizon}_close_pct'].mean() if len(g0) else np.nan,
                           'clean_high5_touch_pct':100*(g0[f'D{horizon}_max_high_pct']>=5).mean() if len(g0) else np.nan})
    pd.DataFrame(phaseg).to_csv(out/'r112_phase_outcomes_without_corp_suspects.csv',index=False,encoding='utf-8-sig')
    # Export manual contrasting casebook only. No hindsight-optimized threshold search.
    cb=[]
    old=covered[covered.D20_complete.eq(1) & ~covered.prior_224_corporate_action_suspect & ~covered.prior_224_suspended_session]
    for stage,g in old.groupby('stage'):
        sorted_g=g.sort_values(['D20_close_pct','signal_date','code'])
        take=pd.concat([sorted_g.head(3),sorted_g.tail(3)])
        cb.append(take)
    if cb:
        pd.concat(cb).drop_duplicates(['signal_date','code']).sort_values(['stage','D20_close_pct'])[
         ['signal_date','code','primary_mode_for_display','stage','origin_date','impulse_date','peak_date_asof',
          'origin_to_peak_pct','current_retrace_fraction','last5_to_impulse_volume','episode_first_c10',
          'D5_close_pct','D5_max_high_pct','D5_min_low_pct','D20_close_pct','D20_max_high_pct','D20_min_low_pct']
        ].to_csv(out/'r112_manual_casebook_no_future_features.csv',index=False,encoding='utf-8-sig')
    outputs=[p.name for p in out.glob('*.csv')]
    manifest={'status':OUTPUT_STATUS,'pinned_source_runs':{'r171':38664095610,'r110':38745541969,'r111':38760251901},
              'method':'Point-in-time state machine, NOT ground-truth or globally optimal wave detector',
              'wave_thresholds':THRESHOLDS,'all_krx_daily_sha_checked':sha_dates,
              'native_selected_exact_bar_match':bar_verified,'all_selected_unique_signals':len(y),
              'evaluated_signals':len(covered),'D20_evaluable':int(covered.D20_complete.sum()),
              'D5_recent_evaluable':int((covered.D20_complete==0).sum()),
              'stage_distribution_all':y.stage.value_counts().to_dict(),
              'corp_action_suspect_selected':corp_flags,
              'observed_nontradeable_daily_rows_in_selected_stocks':len(suspended_rows),
              'stock_codes_with_nontradeable_daily_rows':len(set(x['code'] for x in suspended_rows)),
              'D1open_to_D5close_valid_study_rows':int(entry.entry_fill_possible_on_daily_bars.sum()),
              'suspended_session_quarantine_selected':int(y.prior_224_suspended_session.sum()),
              'source_sha256':{'r171_native_selected':sha(Path(r171,'r17_krx/native/v49_76_global_selected_top5.csv').read_bytes()),
                               'r110_feature':sha(Path(r110,'r110_signal_flags_and_outcomes.csv').read_bytes()),
                               'r111_episode':sha(p.read_bytes())},
              'output_sha256':{f:sha((out/f).read_bytes()) for f in sorted(outputs)},
              'limitations':{'PIT_universe':'NOT_PROVEN','index_membership':'NOT_PROVEN',
                             'KRX_1530_session_exclusive':'NOT_PROVEN','corporate_action_adjustment':'NOT_PROVEN',
                             'wave_ground_truth':'NO','true_first_wave':'NO',
                             'top5_selection_bias':'YES','fresh_OOS':'NO','actual_fills':'NOT_PROVEN',
                             'signal_date_execution':'NOT_ASSUMED'}}
    (out/'r112_integrity_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R112_RESEARCH_PASS',json.dumps({k:manifest[k] for k in ('all_krx_daily_sha_checked','native_selected_exact_bar_match','evaluated_signals','D20_evaluable','D5_recent_evaluable','stage_distribution_all')},ensure_ascii=False),flush=True)
    return manifest


def main():
    a=argparse.ArgumentParser();a.add_argument('--r171-dir',required=True);a.add_argument('--r110-dir',required=True)
    a.add_argument('--r111-dir',required=True);a.add_argument('--history-artifacts',required=True);a.add_argument('--out',required=True)
    z=a.parse_args();run(z.r171_dir,z.r110_dir,z.r111_dir,z.history_artifacts,z.out)
if __name__=='__main__':main()
