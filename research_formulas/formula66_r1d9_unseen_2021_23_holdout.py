#!/usr/bin/env python3
"""StockHunter R1-D9: predeclared, disjoint 2021-2023 historical 224-wave proxy validation.

RESEARCH_ONLY. Does not rescreen native 66 formulas. Uses pinned, unchanged R1D1
selector, R1D5 support features, R1D6 pricing contracts and R1D7 path labels.
R1D8 outcomes are loaded ONLY after out-of-period signal-time feature lock.
No tuning, no live score/rank/orders. Retrospective price paths, not trade fills.
"""
from __future__ import annotations
import argparse, gzip, hashlib, io, json, math
from collections import Counter, defaultdict, deque
from pathlib import Path
import numpy as np
import pandas as pd
from research_formulas import formula66_r1d6_oos_holdout as d6
from research_formulas import formula66_r1d7_path_risk_audit as d7
from research_formulas import formula66_r1d8_structure_confounds as d8

VERSION='STOCKHUNTER_R1D9_PREDECLARED_2021_23_DISJOINT_HOLDOUT_V1'
RESEARCH_ONLY=True
WARMUP_START='2019-01-02'
SIGNAL_START='2021-01-04'
SIGNAL_END='2023-11-29'  # D+20 at 2023-12-28, BEFORE D6's first 2024 signal
EXPECTED_PREVIOUS='2020-12-30'
EXPECTED_D20_END='2023-12-28'
HORIZONS=(5,10,15,20)
D6_BLOB='00c78a26117670768b70772f578edcb5995e48a5'
D7_BLOB='4efbb3edd5d5e50786cc90420292b7854eb8e203'
D8_BLOB='c8a245ce53e866880d932fca125afc5e0bdf4e90'
D8_RUN=39954423000
D8_SHA='3b8321a2e48c98a49f93ada68ba64e92e072d6ed'
D8_ARTIFACT_ID=12502370149
D8_ARTIFACT_DIGEST='be933a599010f5ecb37a50574a25c54e25434c424889f41ab307c1c7161edf2a'
SOURCE_KRX_RUN=d6.SOURCE_RUN
DONE='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
# PRIMARY contrast is explicitly selected from R1D8 exploration, BEFORE new 2021-23 outcomes.
PRIMARY_FLAG='BB20_L_hold'
# Fixed secondary checks. No thresholds learned from new signal/outcome rows.
CHECK_FLAGS=('BB20_L_hold','MA20_hold','BB40_L_hold',
             'FIB50_reclaim','amount3d_above_prior10','signal_close_above_prev_high')
# R1D8's predeclared date+wave comparisons, with historical exploration directions.
REFERENCE_R1D8_MATCHED_FAIL_DELTA={
 'BB20_L_hold':-7.06,'MA20_hold':3.27,'BB40_L_hold':-3.09,
 'FIB50_reclaim':5.31,'amount3d_above_prior10':7.21,
 'signal_close_above_prev_high':4.66}


def fail(s):raise RuntimeError('R1D9_FAIL_CLOSED_'+str(s))
def sha(b):return hashlib.sha256(b).hexdigest()
def save_csv(df,p,gzip_file=False):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 df.to_csv(p,index=False,compression='gzip' if gzip_file else None,encoding='utf-8-sig')
def save_json(d,p):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 p.write_text(json.dumps(d,ensure_ascii=False,sort_keys=True,indent=2,default=str)+'\n',encoding='utf-8')


def source_lock(repo_root):
 root=Path(repo_root)
 for name,expected in [('formula66_r1d6_oos_holdout.py',D6_BLOB),
                       ('formula66_r1d7_path_risk_audit.py',D7_BLOB),
                       ('formula66_r1d8_structure_confounds.py',D8_BLOB)]:
  p=root/'research_formulas'/name
  if not p.is_file() or d7.git_blob(p.read_bytes())!=expected:fail('FROZEN_SOURCE_DRIFT_'+name)
 # R1D1/R1D5 ORIGINAL Git blobs, as verified by R1D6 on GitHub.
 original_d1,original_d5=d6.verify_source_frozen(root)
 if (d8.GAIN_LABELS!=('12_TO_LT20','20_TO_LT35','GE35')
     or d8.RETRACE_LABELS!=('15_TO_LT30','30_TO_LT50','50_TO_618')):
  fail('PREDECLARED_WAVE_BIN_DRIFT')
 if set(CHECK_FLAGS)-set(d8.COMPARE_FLAGS):fail('D8_FROZEN_FLAG_MISSING')
 protocol=root/'R1D9_PREDECLARED_PROTOCOL.json'
 if not protocol.is_file():fail('MISSING_PREDECLARED_PROTOCOL')
 q=json.loads(protocol.read_text('utf-8'))
 if (q.get('version')!=VERSION or q.get('signal_start')!=SIGNAL_START
     or q.get('signal_end')!=SIGNAL_END or q.get('D20_last_exit')!=EXPECTED_D20_END
     or q.get('primary_flag_from_R1D8')!=PRIMARY_FLAG
     or q.get('exits_D_plus_sessions')!=list(HORIZONS)
     or q.get('secondary_frozen_flags')!=[x for x in CHECK_FLAGS if x!=PRIMARY_FLAG]
     or q.get('KRX_daily_raw_sha_required')!=1907):fail('PREDECLARED_PROTOCOL_DRIFT')
 return original_d1,original_d5


def verify_calendar(calendar):
 if len(calendar)!=1907 or calendar[0]!=WARMUP_START or calendar[-1]!=d6.SOURCE_END:
  fail('IMMUTABLE_KRX_CALENDAR_DRIFT')
 if any(d not in calendar for d in (SIGNAL_START,SIGNAL_END,WARMUP_START,EXPECTED_D20_END)):
  fail('HOLDOUT_DAY_ABSENT')
 i=calendar.index(SIGNAL_START);j=calendar.index(SIGNAL_END)
 if calendar[i-1]!=EXPECTED_PREVIOUS or calendar[j+20]!=EXPECTED_D20_END:
  fail('HOLDOUT_PERIOD_BOUNDARY_DRIFT')
 if not SIGNAL_END<'2024-01-02' or not EXPECTED_D20_END<'2024-01-02':
  fail('PERIOD_OVERLAP_WITH_R1D6')
 if i-calendar.index(WARMUP_START)<350:fail('INSUFFICIENT_350_DAY_ASOF_WARMUP')
 return i,j


def forward_price_packs(ep,calendar):
 bars={d:tuple(v) for d,*v in ep['forward_bars']}
 out=[]
 for h in HORIZONS:
  x=d7.build_horizon(ep['date'],ep['code'],calendar,ep['signal_ix'],bars,h,ep['signal_close'])
  out.append(x)
 # Exact parity against R1D6 pricing for fixed D+20, including its invalid paths.
 old=d6.forward_outcome(ep,calendar,{},horizon=20)
 new=out[-1]
 if old['status_out']!=new['status_out']:fail('ORIGINAL_D20_STATUS_DRIFT')
 for o,n in (('net_20bps_pct','net_20bps_pct'),('gross_pct','gross_pct'),
             ('max_intraday_adverse_pct','mae_intraday_pct'),
             ('max_intraday_favorable_pct','mfe_intraday_pct')):
  a,b=old[o],new[n]
  if pd.isna(a) and pd.isna(b):continue
  if not np.isclose(a,b,atol=1e-10,rtol=0):fail('ORIGINAL_D20_NUMERIC_DRIFT_'+o)
 return out


def discover(krx_root,out,orig_d1,orig_d5,repo_root='.'): 
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 src=d6.source_manifest(krx_root)
 calendar,indices=d6.index_asof(krx_root)
 if set(src)!=set(calendar):fail('KOREAN_RAW_AND_INDEX_DAYS_DIFFER')
 first,last=verify_calendar(calendar)
 verified=d6.verify_all_daily_raw_sha(src)
 period=set(calendar[first:last+1]);
 histories=defaultdict(lambda:deque(maxlen=orig_d1.PROXY['lookback_history_bars']))
 previous_signal={};pending=defaultdict(list); episodes=[];asofs=[];audits=[]
 strict=0; t=0; idx={date:n for n,date in enumerate(calendar)}
 for day in calendar[:last+21]:
  raw=d6.priceday(src[day][0],day)
  new=0;hits=0;signal_day=day in period
  for rr in raw.itertuples(index=False):
   code=str(rr.code)
   vals=(float(rr.open),float(rr.high),float(rr.low),float(rr.close),float(rr.volume),float(rr.amount))
   if signal_day or day<SIGNAL_START:histories[code].append((t,day,*vals))
   for e in pending.get(code,()):
    if day>e['date'] and len(e['forward_bars'])<20:
     e['forward_bars'].append((day,*vals))
   if not signal_day:continue
   feature=orig_d1.asof_features(histories[code],t,day)
   if not feature.strict_proxy:continue
   strict+=1;hits+=1
   if code in previous_signal and t-previous_signal[code]<20:continue
   previous_signal[code]=t
   signal=orig_d1.feature_dict(feature)
   e={'date':day,'code':code,'wave_trough_date':signal['wave_trough_date'],
      'wave_peak_date':signal['wave_peak_date'],'wave_gain_pct':signal['wave_gain_pct'],
      'wave_retrace_pct':signal['wave_retrace_pct'],'signal_ix':idx[day],
      'signal_close':vals[3],'forward_bars':[],
      'joint_index_20d_regime':indices[day]['joint_index_20d_regime'],
      'KOSPI_ret20_pct':indices[day]['KOSPI_ret20_pct'],
      'KOSDAQ_ret20_pct':indices[day]['KOSDAQ_ret20_pct']}
   f=d6._one_episode_from_history(histories[code],e,orig_d5)
   f.update({k:e[k] for k in ('wave_gain_pct','wave_retrace_pct','joint_index_20d_regime','KOSPI_ret20_pct','KOSDAQ_ret20_pct')})
   f['reference_data_last_bar_date']=day
   f['source_is_original_KRX_unadjusted']=1
   if f['date']!=day or f['code']!=code:fail('D5_FEATURE_NOT_SAME_SIGNAL_DAY')
   episodes.append(e);asofs.append(f);pending[code].append(e);new+=1
  if signal_day:
   audits.append({'date':day,'strict_proxy_hits':hits,'new_20session_episodes':new,'fullmarket_rows':len(raw),
                  'market_regime':indices[day]['joint_index_20d_regime']})
   if len(audits)%75==0:
    print('R1D9_OOS_PROGRESS',day,'dates',len(audits),'new_signals',len(episodes),flush=True)
  t+=1
 if not episodes:fail('NO_NEW_SIGNALS')
 frame=pd.DataFrame(asofs)
 if frame.duplicated(['date','code']).any():fail('DUPLICATE_SIGNAL_DATE_CODE')
 if not frame.date.between(SIGNAL_START,SIGNAL_END).all():fail('SIGNAL_DATE_OUTSIDE_LOCK')
 if not frame.reference_data_last_bar_date.eq(frame.date).all():fail('ASOF_FUTURE_BAR')
 if not frame.status_feature.eq('ASOF_OK').all():fail('FEATURE_NOT_READY')
 if any(c.startswith(('net_','gross_','exit_','fwd_','future_','status_out')) for c in frame.columns):
  fail('OUTCOME_LEAKED_IN_SIGNAL_TIME_FEATURES')
 # Freeze ASOF rows and D8's signal-time wave bins BEFORE calculating forward returns.
 freeze=d8.shape_bins(frame)
 freeze=freeze.sort_values(['date','code']).reset_index(drop=True)
 feature_path=out/'R1D9_ASOF_ONLY_2021_23_NO_OUTCOME.csv.gz'
 save_csv(freeze,feature_path,True)
 frozen_digest=sha(feature_path.read_bytes())
 save_csv(pd.DataFrame(audits),out/'R1D9_DAILY_SIGNAL_COUNTS.csv')
 results=[]
 for e in episodes:results.extend(forward_price_packs(e,calendar))
 paths=pd.DataFrame(results)
 if len(paths)!=len(freeze)*4 or paths.duplicated(['date','code','horizon']).any():fail('PRICE_PATH_COUNT_OR_KEY_DRIFT')
 save_csv(paths,out/'R1D9_SEALED_2021_23_D5_D10_D15_D20.csv.gz',True)
 manifest={'version':VERSION,'signal_window':[SIGNAL_START,SIGNAL_END],
           'latest_D20_exit':EXPECTED_D20_END,'n_signal_days':len(audits),
           'n_signal_event_raw_strict':strict,'n_after_20_session_cooldown':len(episodes),
           'n_unique_codes':int(freeze.code.nunique()),
           'n_dates_with_signal':int(freeze.date.nunique()),
           'source_KRX_run':SOURCE_KRX_RUN,'source_verified_daily_raw_count':verified,
           'asof_locked_before_forward_paths_sha256':frozen_digest,
           'non_overlap_with_R1D6_signal_and_D20_paths':True,
           'new_unseen_signal_period_relative_to_R1D1_D8_research':True,
           'native_original_66_formulas':False,'RESEARCH_ONLY':True,
           'strategy_ready':False,'orders_placed':0,'tradeability_certified_PIT':False,
           'historical_unadjusted_KRX_original':True,
           'predeclared_protocol_sha256':sha((Path(repo_root)/'R1D9_PREDECLARED_PROTOCOL.json').read_bytes())}
 return freeze,paths,manifest


def summarize_segment(a,label):
 good=a[a.status_out.eq(DONE)]
 n=len(good)
 return {'segment':label,'n_episodes':len(a),'n_complete':n,
         'n_codes':int(good.code.nunique()),'n_dates':int(good.date.nunique()),
         'n_strong15':int(good.net_20bps_pct.ge(15).sum()),
         'n_fail5':int(good.net_20bps_pct.le(-5).sum()),
         'strong15_pct':100*good.net_20bps_pct.ge(15).mean() if n else np.nan,
         'fail5_pct':100*good.net_20bps_pct.le(-5).mean() if n else np.nan,
         'net_median_pct':good.net_20bps_pct.median() if n else np.nan,
         'MAE_median_pct':good.mae_intraday_pct.median() if n else np.nan,
         'MFE_median_pct':good.mfe_intraday_pct.median() if n else np.nan}


def date_matched_contrast(done,flag,matched,bootstrap_n=1200):
 columns=['date','wave_gain_bin','wave_retrace_bin'] if matched else ['date']
 cells=[]
 for key,g in done.groupby(columns,sort=True):
  yes=g[g[flag].eq(1)];no=g[g[flag].eq(0)]
  if yes.empty or no.empty:continue
  date=key[0] if isinstance(key,tuple) else key
  cells.append({'date':date,'delta_fail_pp':100*(yes.failure5.mean()-no.failure5.mean()),
                'delta_strong_pp':100*(yes.strong15.mean()-no.strong15.mean()),
                'delta_MAE_pp':yes.mae_intraday_pct.mean()-no.mae_intraday_pct.mean()})
 result={'condition':flag,'comparison':'DATE_WAVE_RETRACE_MATCHED' if matched else 'SAME_DATE',
         'n_eligible_dates':0,'n_strata':0,'delta_fail_pp':np.nan,
         'delta_strong_pp':np.nan,'delta_MAE_pp':np.nan,
         'date_bootstrap_fail_lo':np.nan,'date_bootstrap_fail_hi':np.nan}
 if not cells:return result
 x=pd.DataFrame(cells).groupby('date')[['delta_fail_pp','delta_strong_pp','delta_MAE_pp']].mean()
 result.update(n_eligible_dates=len(x),n_strata=len(cells),
               delta_fail_pp=x.delta_fail_pp.mean(),delta_strong_pp=x.delta_strong_pp.mean(),
               delta_MAE_pp=x.delta_MAE_pp.mean())
 lo,hi=d8._date_bootstrap_ci(x.delta_fail_pp.values,iterations=bootstrap_n,
                             seed=20261023+len(flag)+int(matched)*100)
 result.update(date_bootstrap_fail_lo=lo,date_bootstrap_fail_hi=hi)
 return result


def old_d8_reference(ref_dir):
 p=Path(ref_dir)/'result'/'R1D8_DATE_CLUSTERED_CONTRASTS_WITH_CI.csv'
 if not p.is_file():fail('MISSING_FROZEN_R1D8_COMPARISON')
 df=pd.read_csv(p,encoding='utf-8-sig')
 ref={}
 for flag in CHECK_FLAGS:
  q=df[(df.condition==flag)&(df.comparison=='DATE_WAVE_RETRACE_MATCHED')&(df.outcome=='failure')]
  if len(q)!=1:fail('R1D8_REFERENCE_FLAG_MISSING_'+flag)
  val=float(q.iloc[0].mean_flag1_minus_flag0)
  if abs(val-REFERENCE_R1D8_MATCHED_FAIL_DELTA[flag])>.011:fail('FROZEN_R1D8_REPORTED_RESULT_CHANGED_'+flag)
  ref[flag]=val
 return ref


def analyze(asof,paths,out,reference_dir):
 out=Path(out)
 p20=paths[paths.horizon.eq(20)].copy()
 j=asof.merge(p20,on=['date','code'],validate='one_to_one')
 if len(j)!=len(asof):fail('UNEXPECTED_JOIN_SIZE')
 j['failure5']=j.status_out.eq(DONE)&j.net_20bps_pct.le(-5)
 j['strong15']=j.status_out.eq(DONE)&j.net_20bps_pct.ge(15)
 # Compute the first occurrence BEFORE copying the complete D20 subset;
 # otherwise done lacks the first_code field in repeated-code sensitivity.
 first_dates=asof.sort_values(['date','code']).drop_duplicates('code')[['date','code']]
 first=set(map(tuple,first_dates.itertuples(index=False,name=None)))
 j['first_code']=[(a,b) in first for a,b in zip(j.date,j.code)]
 done=j[j.status_out.eq(DONE)].copy()
 if done.empty:fail('NO_COMPLETE_D20_IN_NEW_PERIOD')
 if any(done.date.ge('2024-01-02')):fail('OLD_TRAIN_PERIOD_LEAK')
 if not done.status_out.eq(DONE).all():fail('INCOMPLETE_IN_STATISTICS')
 score=[]
 frozen_D20_complete=set(zip(p20.loc[p20.status_out.eq(DONE),'date'],p20.loc[p20.status_out.eq(DONE),'code']))
 for h,group in paths.groupby('horizon'):
  score.append(summarize_segment(group,'DPLUS_'+str(h)))
  same=group[[ (a,b) in frozen_D20_complete for a,b in zip(group.date,group.code) ]]
  score.append(summarize_segment(same,'SAME_D20_ELIGIBLE_DPLUS_'+str(h)))
 save_csv(pd.DataFrame(score),out/'R1D9_D5_D10_D15_D20_SCORECARD.csv')
 for h in HORIZONS:
  k=paths[paths.horizon==h]
  if k.status_out.eq(DONE).sum()>len(asof):fail('UNEXPECTED_HORIZON_COUNTS')
 stat=[]
 stat.append(summarize_segment(j,'ALL_PREDECLARED_NEW_PERIOD'))
 stat.append(summarize_segment(j[j.first_code],'FIRST_CODE_IN_NEW_PERIOD'))
 for year,g in j.groupby(j.date.str[:4]):stat.append(summarize_segment(g,'YEAR_'+year))
 for reg,g in j.groupby('market_regime'):stat.append(summarize_segment(g,'REGIME_'+str(reg)))
 for flag in CHECK_FLAGS:
  for v in (0,1):stat.append(summarize_segment(j[j[flag]==v],'FLAG_'+flag+'_'+str(v)))
 save_csv(pd.DataFrame(stat),out/'R1D9_NEW_HOLDOUT_GROUP_SCORECARDS.csv')
 reference=old_d8_reference(reference_dir)
 contrasts=[]
 for flag in CHECK_FLAGS:
  for matched in (False,True):
   row=date_matched_contrast(done,flag,matched)
   row['historical_D8_matched_fail_pp']=reference[flag]
   row['direction_replicated']=(bool(np.sign(row['delta_fail_pp'])==np.sign(reference[flag]))
                                if pd.notna(row['delta_fail_pp']) else None)
   row['new_independent_period']=True
   row['not_causal']=True
   row['exploratory_multiple_factors']=True
   contrasts.append(row)
 save_csv(pd.DataFrame(contrasts),out/'R1D9_NEW_PERIOD_VS_D8_PREDECLARED_CONTRASTS.csv')
 # Frozen gross vs net risk and repeated signal sensitivity only; not ranking/gate.
 risk=[]
 for flag in CHECK_FLAGS:
  for grp,sub in [('ALL',done),('FIRST_CODE',done[done.first_code])]:
   for v in (0,1):
    m=sub[sub[flag].eq(v)]
    risk.append({'flag':flag,'cohort':grp,'value':v,**summarize_segment(m,'FROZEN')})
 save_csv(pd.DataFrame(risk),out/'R1D9_REPEATED_CODE_SENSITIVITY.csv')
 save_csv(j,out/'R1D9_SEALED_AUDIT_JOINED_NO_LIVE.csv.gz',True)
 manifest={'n_D20_complete':len(done),'n_D20_strong15':int(done.strong15.sum()),
           'n_D20_failure5':int(done.failure5.sum()),
           'primary_flag':PRIMARY_FLAG,
           'primary_same_date_wave_risk_delta_pp':next((r['delta_fail_pp'] for r in contrasts if r['condition']==PRIMARY_FLAG and r['comparison']=='DATE_WAVE_RETRACE_MATCHED'),None),
           'no_posthoc_parameter_tuning':True,'outcomes_are_retrospective_prices_only':True,
           'D8_hypotheses_chosen_after_old_period_exploration':True,
           'not_causal_or_investment_approval':True,
           'barriers_same_day_order_unknown':True,'multiple_factors_exploratory':True,
           'source_KRX_adjusted_for_corporate_actions':False,
           'independent_66_native_search_validated':False}
 return manifest


def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--krx-root',required=True)
 p.add_argument('--d8-reference',required=True)
 p.add_argument('--repo-root',default='.')
 p.add_argument('--out',default='reports/r1d9/result')
 a=p.parse_args(argv)
 orig_d1,orig_d5=source_lock(a.repo_root)
 asof,paths,manifest=discover(a.krx_root,a.out,orig_d1,orig_d5,repo_root=a.repo_root)
 # Read OLD R1D8 outcomes ONLY AFTER 2021-23 as-of and forward paths frozen.
 manifest.update(analyze(asof,paths,a.out,a.d8_reference))
 save_json(manifest,Path(a.out)/'R1D9_MANIFEST.json')
 print('R1D9_NEW_PERIOD_OOS_RESEARCH_ONLY_COMPLETE',json.dumps({k:manifest[k] for k in ('n_after_20_session_cooldown','n_D20_complete','n_D20_strong15','n_D20_failure5','primary_same_date_wave_risk_delta_pp','orders_placed')},ensure_ascii=False),flush=True)
 return 0

if __name__=='__main__':raise SystemExit(main())
