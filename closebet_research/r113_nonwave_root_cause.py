"""R1.13 research-only NO_CONFIRMED_WAVE forensic diagnostics.

Pinned R1.12 results plus unchanged KRX history; NO mutation of the state machine.
Tests why a signal is undetected using causal, prior/current day price/volume gates.
These diagnostic gates are NOT labeled ground truth and do NOT tune thresholds.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
import numpy as np
import pandas as pd
from closebet_research.r111_episode_first_strength_audit import build_archives, require, sha, unique_raw, R171_RAW_SHA, CHECK_COLS
from closebet_research.r112_causal_wave_phase_audit import THRESHOLDS, load_historical_krx, causal_daily_states

R112_RUN=38823870672
R171_RUN=38664095610
EXPECTED_SOURCE_HASH='3e2b8404522aa29df47c1ac68e2ab4c17c6333c0c91e6efa8b50b594e6eb2060'
EXPECTED_SOURCE_MANIFEST_STATUS='PASS_EXPLORATORY_NOT_TRADE_APPROVED'
EXPECTED_STAGES={
    'NO_CONFIRMED_WAVE':659, 'FIRST_WAVE_ACTIVE':100,
    'PULLBACK_UNCONFIRMED':96, 'REBOUND_CONFIRMED':82,
    'PULLBACK_QUALITY':79, 'CORP_ACTION_QUARANTINE':34,
    'INSUFFICIENT_HISTORY':5, 'SUSPENSION_224_QUARANTINE':4,
}
TRADING_STAGES={'FIRST_WAVE_ACTIVE','PULLBACK_QUALITY','PULLBACK_UNCONFIRMED','REBOUND_CONFIRMED'}
GATE_ORDER=('origin_below_ma224','gain_ge_16pct','prior5_vol_ge_1_25','close_5bar_high')
CAUSES=['RECENT_DETECTED_WAVE_RESET_OR_EXPIRED','NO_BELOW224_ANCHOR',
        'NO_16PCT_IMPULSE','NO_1_25_PRIOR_VOLUME','NO_FIVE_CLOSE_BREAKOUT',
        'GATES_ELIGIBLE_BUT_STATE_NONE_INSPECT']


def load_pinned_source(r112):
    root=Path(r112)
    path=root/'r112_all_selected_1059_pit_wave_stage.csv'
    mf=root/'r112_integrity_manifest.json'
    require(path.is_file() and mf.is_file(),'R112_REQUIRED_FILES_MISSING')
    m=json.loads(mf.read_text(encoding='utf-8'))
    require(m['status']==EXPECTED_SOURCE_MANIFEST_STATUS,'R112_WRONG_STATUS')
    require(m['pinned_source_runs']=={'r171':R171_RUN,'r110':38745541969,'r111':38760251901},'R112_WRONG_PROVENANCE')
    require(sha(path.read_bytes())==EXPECTED_SOURCE_HASH,'R112_SELECTED_STAGE_SOURCE_SHA_MISMATCH')
    require(m['output_sha256'][path.name]==EXPECTED_SOURCE_HASH,'R112_MANIFEST_OUTCOME_HASH_MISMATCH')
    raw=pd.read_csv(path,dtype={'code':str},encoding='utf-8-sig')
    require(len(raw)==1059,'R112_SIGNAL_COUNT')
    require(raw.code.str.fullmatch(r'[0-9]{6}').all(),'INVALID_TICKER')
    require(not raw[['signal_date','code']].duplicated().any(),'DUPLICATED_EVENT')
    require(raw.stage.value_counts().to_dict()==EXPECTED_STAGES,'R112_STAGE_DISTRIBUTION')
    require(m['all_krx_daily_sha_checked']==1907 and m['native_selected_exact_bar_match']==1059,'R112_STAGE_NOT_VERIFIED')
    return raw,m


def gates_at(d, dates, close, low, volume, ma224, i):
    """Mirror R1.12 NO state gate, showing its exclusions. Returns None if too few bars."""
    if i<270 or i<224+5:return None
    lb=max(224,i-THRESHOLDS['origin_lookback'])
    if i-lb<5:return None
    segment=low[lb:i]
    if not np.isfinite(segment).all():return None
    anchor=int(np.nanargmin(segment))+lb
    floor=float(low[anchor]);base=float(ma224[anchor]);c=float(close[i])
    prev20=float(np.mean(volume[i-25:i-5]))
    previous5=float(np.mean(volume[i-5:i])) # baseline R1.12 EXCLUDES signal day
    inclusive5=float(np.mean(volume[i-4:i+1])) # diagnostic ONLY
    preceding20_inc=float(np.mean(volume[i-24:i-4]))
    prev5close_max=float(np.max(close[i-5:i]))
    if floor<=0 or prev20<=0 or preceding20_inc<=0 or base<=0:return None
    gain=(c/floor-1)*100
    ratio=previous5/prev20
    incl_ratio=inclusive5/preceding20_inc
    g1=bool(floor<base);g2=bool(gain>=THRESHOLDS['wave_min_gain_pct']);g3=bool(ratio>=THRESHOLDS['impulse_volume_5_to_previous_20']);g4=bool(c>=prev5close_max)
    return {'date':str(dates[i]),'origin_day':str(dates[anchor]),'anchor_low':floor,'anchor_ma224':base,
            'gain_pct':gain,'previous5_to_older20_volume':ratio,
            'including_signal_day_volume_ratio':incl_ratio,
            'close_over_prior5_max':c>=prev5close_max,'signal_day_volume':float(volume[i]),
            'origin_below_ma224':g1,'gain_ge_16pct':g2,'prior5_vol_ge_1_25':g3,'close_5bar_high':g4,
            'gates_all':g1 and g2 and g3 and g4,
            'gates_all_if_signal_vol_included':g1 and g2 and bool(incl_ratio>=1.25) and g4}


def diagnose_one(day,code,d,computed_state,history_window=45):
    times=d.index.astype(str).tolist();idx={v:i for i,v in enumerate(times)}
    require(day in idx,'HISTORY_SELECTED_DAY_ABSENT:'+code+':'+day)
    i=idx[day]; state_at=str(computed_state.iloc[i].stage)
    require(state_at=='NO_CONFIRMED_WAVE','NOT_NO_WAVE_STUDY_CASE:'+code+':'+day)
    c=d.close.to_numpy(dtype='float64');lo=d.low.to_numpy(dtype='float64');vol=d.volume.to_numpy(dtype='float64')
    ma224=d.close.rolling(224,min_periods=224).mean().to_numpy()
    r0=gates_at(d,times,c,lo,vol,ma224,i)
    past_start=max(270,i-history_window+1)
    history_state=computed_state.iloc[past_start:i]
    prior_detected=history_state[history_state.stage.isin(TRADING_STAGES)]
    recent_quarantine=history_state[history_state.stage.isin({'SUSPENSION_224_QUARANTINE','CORP_ACTION_QUARANTINE','INSUFFICIENT_HISTORY'})]
    # Explain the transition to NO, without pretending this identifies a true market-wave failure.
    reset_event='NO_PRIOR_DETECTED_WAVE'
    if len(prior_detected):
        last_ix=int(prior_detected.index[-1]); last=computed_state.iloc[last_ix]
        following=computed_state.iloc[last_ix+1]
        if following.stage!='NO_CONFIRMED_WAVE':
            reset_event='STATE_RESET_OR_HISTORY_QUARANTINE_INSPECT'
        elif float(last.age_from_impulse)+1>THRESHOLDS['max_wave_age_sessions']:
            reset_event='WAVE_AGE_EXCEEDED_45_SESSIONS'
        elif last.stage=='REBOUND_CONFIRMED' and str(last.rebound_date) in idx:
            since_rebound=last_ix+1-idx[str(last.rebound_date)]
            if since_rebound>THRESHOLDS['rebound_marker_sessions']:
                reset_event='REBOUND_MARKER_EXPIRED_AFTER_7_SESSIONS'
            else:reset_event='UNEXPLAINED_REBOUND_RESET'
        elif last.stage in ('PULLBACK_UNCONFIRMED','PULLBACK_QUALITY'):
            peak=float(last.peak_high_asof);origin=float(last.origin_low)
            old_r=float(last.pullback_low_retrace_fraction)
            old_low=peak-(peak-origin)*old_r
            next_low=min(old_low,float(d.low.iloc[last_ix+1]))
            retrace=(peak-next_low)/(peak-origin)
            if retrace>=THRESHOLDS['pullback_invalid_retrace_fraction'] or float(d.close.iloc[last_ix+1])<origin:
                reset_event='PULLBACK_INVALIDATED_BY_LOW_OR_ORIGIN_BREAK'
            else:reset_event='UNEXPLAINED_PULLBACK_RESET'
        else:reset_event='OTHER_RESET_TRANSITION_INSPECT'
    eligible=[]
    for j in range(past_start,i+1):
        if j!=i and computed_state.iloc[j].stage!='NO_CONFIRMED_WAVE':continue
        row=gates_at(d,times,c,lo,vol,ma224,j)
        if row is not None:eligible.append(row)
    ever_gate={g:0 for g in GATE_ORDER};gate_counts={g:0 for g in GATE_ORDER}
    for a in eligible:
        for k,g in enumerate(GATE_ORDER):
            if all(a[h] for h in GATE_ORDER[:k+1]):gate_counts[g]+=1
            if a[g]:ever_gate[g]+=1
    maxfull=max([sum(all(a[g] for g in GATE_ORDER[:k+1]) for k in range(1,5)) for a in eligible] or [0])
    if len(prior_detected):cause=CAUSES[0]
    elif gate_counts[GATE_ORDER[0]]==0:cause=CAUSES[1]
    elif gate_counts[GATE_ORDER[1]]==0:cause=CAUSES[2]
    elif gate_counts[GATE_ORDER[2]]==0:cause=CAUSES[3]
    elif gate_counts[GATE_ORDER[3]]==0:cause=CAUSES[4]
    else:cause=CAUSES[5]
    best=None
    if eligible:
        best=max(eligible,key=lambda x:(sum(x[g] for g in GATE_ORDER), x['date']))
    out={'signal_date':day,'code':code,'r112_state':state_at,'primary_explanation':cause,
         'last45_prior_detected_wave_count':len(prior_detected),
         'last45_prior_detected_wave_last_date':str(prior_detected.date.iloc[-1]) if len(prior_detected) else '',
         'last_detected_wave_stage':str(prior_detected.stage.iloc[-1]) if len(prior_detected) else '',
         'inferred_reset_event':reset_event,
         'trading_sessions_since_last_detected_wave':(i-int(prior_detected.index[-1])) if len(prior_detected) else np.nan,
         'last45_prior_quarantine_days':len(recent_quarantine),
         'candidate_no_state_days_checked':len(eligible),
         'any_candidate_all_four_gates':int(any(q['gates_all'] for q in eligible)),
         'any_candidate_if_signal_volume_included':int(any(q['gates_all_if_signal_vol_included'] for q in eligible)),
         'candidate_day_exclusive_gate_full_count':gate_counts[GATE_ORDER[-1]],
         'candidate_day_exclusive_gate_partial_max':maxfull,
         'signal_day_gate_available':int(r0 is not None),
         'signal_day_passed_gates':sum(int(r0[g]) for g in GATE_ORDER) if r0 else 0,
         'r112_quarantine_on_signal':False,
         'interpretation_level':'CAUSAL_ALGORITHM_GATE_DIAGNOSTIC_NOT_TRUE_WAVE_LABEL'}
    for g in GATE_ORDER:
        out['candidate_prefix_count_'+g]=gate_counts[g]
        out['signal_'+g]=int(r0[g]) if r0 else -1
    if r0:
        for f in ['origin_day','anchor_low','anchor_ma224','gain_pct','previous5_to_older20_volume','including_signal_day_volume_ratio','signal_day_volume']:
            out['signal_'+f]=r0[f]
    if best:
        for f in ['date','origin_day','gain_pct','previous5_to_older20_volume','including_signal_day_volume_ratio']:
            out['best_candidate_'+f]=best[f]
    return out


def run(r112,r171,history,out):
    source,manifest=load_pinned_source(r112)
    raw_file=Path(r171)/'r17_krx/native/v49_76_global_selected_top5.csv'
    require(raw_file.is_file(),'R171_RAW_SOURCE_MISSING')
    require(sha(raw_file.read_bytes())==R171_RAW_SHA,'R171_RAW_SOURCE_SHA_MISMATCH')
    raw=pd.read_csv(raw_file,dtype={'code':str},low_memory=False)
    require(len(raw)==1133,'R171_RAW_STRATEGY_ROW_COUNT')
    raw['signal_date']=pd.to_datetime(raw.signal_date)
    native=unique_raw(raw)
    require(len(native)==1059,'R171_RAW_UNIQUE_DATE_CODE_COUNT')
    require(set(zip(native.signal_date.dt.strftime('%Y-%m-%d'),native.code))==set(zip(source.signal_date,source.code)),'R171_TO_R112_KEYS_MISMATCH')
    native_index=native.set_index([native.signal_date.dt.strftime('%Y-%m-%d'),'code'])
    modes=(raw.groupby([raw.signal_date.dt.strftime('%Y-%m-%d'),'code'])['mode']
           .agg(lambda x:'|'.join(sorted(set(str(v) for v in x)))))
    source['source_modes']=source.set_index(['signal_date','code']).index.map(modes)
    require(source.source_modes.notna().all(),'R171_MODE_IDENTITY_LOST')
    zips,archive,cal=build_archives(history)
    codes=set(source.code)
    h,sha_count=load_historical_krx(archive,cal,codes)
    require(sha_count==1907,'KRX_DATE_HASH_COUNT')
    expected=source.set_index(['signal_date','code'])
    rows=[];full_ohlcv_verified=0
    for code,g in source.groupby('code',sort=True):
        d=h[code]
        stages=causal_daily_states(d)
        dates=d.index.tolist()
        state=stages.set_index('date')
        for r in g.itertuples(index=False):
            day=r.signal_date
            require(day in state.index,'WAVE_MISSING_SIGNAL_DATE:'+code+':'+day)
            calc=state.loc[day]
            require(calc.stage==r.stage,'R112_STATE_RECOMPUTE_DISAGREEMENT:'+code+':'+day+':'+str(calc.stage)+'!='+str(r.stage))
            require(float(r.entry_close)==float(d.loc[day,'close']),'R112_SIGNAL_CLOSE_DIFFERENCE:'+code+':'+day)
            original=native_index.loc[(day,code)]
            for ncol, barcol in CHECK_COLS.items():
                require(float(original[ncol])==float(d.loc[day,barcol]),
                    'R171_VS_KRX_OHLCV_DISAGREEMENT:'+code+':'+day+':'+barcol)
            full_ohlcv_verified+=1
            require(bool(calc.prior_224_corporate_action_suspect)==bool(r.prior_224_corporate_action_suspect),'R112_CORP_FLAG_DIFFERENCE:'+code+':'+day)
            require(bool(calc.prior_224_suspended_session)==bool(r.prior_224_suspended_session),'R112_SUSPENDED_FLAG_DIFFERENCE:'+code+':'+day)
            if r.stage=='NO_CONFIRMED_WAVE':
                diagnosed=diagnose_one(day,code,d,stages)
                diagnosed['source_modes']=r.source_modes
                rows.append(diagnosed)
    for z in zips:z.close()
    df=pd.DataFrame(rows).sort_values(['signal_date','code'])
    require(len(df)==659 and df[['signal_date','code']].drop_duplicates().shape[0]==659,'NO_WAVE_EXPLANATION_COUNT')
    require(df.primary_explanation.isin(CAUSES).all(),'UNKNOWN_EXPLANATION')
    c=df.groupby('primary_explanation',sort=False).agg(n=('code','size'),codes=('code','nunique'),days=('signal_date','nunique')).reset_index()
    c['pct_of_659']=100*c.n/659
    c=c.set_index('primary_explanation').reindex(CAUSES).fillna(0).reset_index()
    mode_audit=source.groupby('source_modes').agg(total_signals=('code','size'),
        excluded_by_r112_wave_proxy=('stage',lambda q:int((q=='NO_CONFIRMED_WAVE').sum())),
        distinct_tickers=('code','nunique')).reset_index()
    mode_audit['proxy_exclusion_pct']=100*mode_audit.excluded_by_r112_wave_proxy/mode_audit.total_signals
    mode_audit=mode_audit.sort_values(['total_signals','source_modes'],ascending=[False,True])
    # The diagnostic alternative is NOT a proposed new entry strategy.
    v=[]
    for fld in ['signal_origin_below_ma224','signal_gain_ge_16pct','signal_prior5_vol_ge_1_25','signal_close_5bar_high']:
        v.append({'gate':fld,'signal_day_true_count':int(df[fld].eq(1).sum()),'signal_day_false_count':int(df[fld].eq(0).sum()),'signal_day_missing_count':int(df[fld].eq(-1).sum())})
    reset_detail=df[df.primary_explanation=='RECENT_DETECTED_WAVE_RESET_OR_EXPIRED'].groupby('inferred_reset_event').agg(signals=('code','size'),distinct_codes=('code','nunique')).reset_index()
    alt=pd.DataFrame([{'metric':'any_candidate_all_four_original_gates','n':int(df.any_candidate_all_four_gates.sum())},
                      {'metric':'any_candidate_all_if_volume_includes_signal_day','n':int(df.any_candidate_if_signal_volume_included.sum())},
                      {'metric':'prior_detected_wave_last45','n':int((df.last45_prior_detected_wave_count>0).sum())},
                      {'metric':'past_quarantine_days_in_last45','n':int((df.last45_prior_quarantine_days>0).sum())}])
    # Cases selected without looking at forward returns; anonymous codes for blind review.
    samples=[]
    for category,g in df.groupby('primary_explanation'):
        chosen=g.sort_values(['signal_date','code']).iloc[np.linspace(0,len(g)-1,min(8,len(g)),dtype=int)] if len(g) else g
        samples.append(chosen)
    cases=pd.concat(samples,ignore_index=True)
    cases.insert(0,'blind_case_id',['R113-'+str(i+1).zfill(3) for i in range(len(cases))])
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    outputs={'r113_nonwave_659_root_causes.csv':df,'r113_cause_summary.csv':c,
             'r113_signal_day_gate_audit.csv':pd.DataFrame(v),'r113_algorithm_diagnostic_alternatives.csv':alt,
             'r113_blind_review_casebook.csv':cases,
             'r113_source_mode_vs_wave_proxy.csv':mode_audit,
             'r113_recent_wave_reset_detail.csv':reset_detail}
    for file,table in outputs.items():table.to_csv(out/file,index=False,encoding='utf-8-sig')
    dest={'status':'PASS_FORENSIC_ONLY_NOT_VALIDATED_TRADING_RULES',
          'r112_run_id':R112_RUN,'krx_source_run_id':R171_RUN,
          'source_stage_sha256':EXPECTED_SOURCE_HASH,'r112_recomputed_signal_stage_count':1059,'native_all_ohlcv_bars_verified':full_ohlcv_verified,
          'krx_all_compressed_daily_sha_checked':sha_count,'missing_wave_explained':len(df),
          'mutually_exclusive_cause_sum':int(c.n.sum()),'cause_distribution':dict(zip(c.primary_explanation,c.n.astype(int))),
          'gate_diagnostics':['original R1.12 excludes same-day volume from its 5-day/20-day impulse ratio','all alternative gates are diagnostic only and not a trained rule'],
          'limitations':{'external_independent_krx_ohlcv':'NOT_CHECKED','corporate_action': 'NOT_RESOLVED',
            'historical_PIT_universe':'NOT_CHECKED','index_member_PIT':'NOT_CHECKED','state_machine_ground_truth':'NOT_CHECKED',
            'selection_bias':'TOP5_ONLY','true_first_wave':'NOT_PROVEN','no_live_orders':'YES'},
          'output_sha256':{k:sha((out/k).read_bytes()) for k in outputs}}
    (out/'r113_integrity_manifest.json').write_text(json.dumps(dest,indent=2,ensure_ascii=False),encoding='utf-8')
    print('R113_PASS',json.dumps({'explained':len(df),'causes':dest['cause_distribution'],'all_dates_sha':sha_count},ensure_ascii=False),flush=True)
    return dest


def main():
    a=argparse.ArgumentParser()
    a.add_argument('--r112-dir',required=True)
    a.add_argument('--r171-dir',required=True)
    a.add_argument('--history-artifacts',required=True)
    a.add_argument('--out',required=True)
    q=a.parse_args();run(q.r112_dir,q.r171_dir,q.history_artifacts,q.out)
if __name__=='__main__':main()
