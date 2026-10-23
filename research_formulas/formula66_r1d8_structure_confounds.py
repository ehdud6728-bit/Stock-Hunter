#!/usr/bin/env python3
"""R1-D8: frozen Stock-Hunter R1-D6/D7 retrospective confounding/structure audit.

No scanner, no new OOS, no trades, no new optimized thresholds. The R1-D6
signal-time feature snapshot is kept immutable and joined AFTER loading to
R1-D7 historical outcomes. Stratification is descriptive and CIs are
clustered sensitivity intervals, not a basis for parameter search or a strategy.
"""
from __future__ import annotations
import argparse, gzip, hashlib, io, json
from pathlib import Path
import numpy as np
import pandas as pd

RESEARCH_ONLY=True
VERSION='STOCKHUNTER_R1D8_FROZEN_D6D7_CONFOUNDS_STRUCTURE_V1'
D6_RUN=39941707036
D6_SHA='019956f9aa85e419c4e8e4ca6fcc6f5dc21a9686'
D6_ASOF_ID=12496827313
D6_ASOF_DIGEST='7185b582e94131b7ee09daf55024c80eeac881f5a2cbc8cee7bbd418a106a884'
D6_SEALED_ID=12497006782
D6_SEALED_DIGEST='f01d83ab64650b8a39459c17381d320e89d8135f5e488180aed28bbe91c00030'
D7_RUN=39948595864
D7_SHA='ae74ffb655aea226ecbfa0e315b7c46077b3c458'
D7_ARTIFACT_ID=12499352019
D7_ARTIFACT_DIGEST='1ee50cef93a8e3adc0cf2e239d1088ebe160528d5f4336f9f5b2e3fe20ec80ed'
D7_SOURCE_BLOB='4efbb3edd5d5e50786cc90420292b7854eb8e203'
D6_ASOF_BYTE_SHA='5062180dbb9d1299773d61074c687e8f7bf7f7ec2fe06b8622b9ed9cc4de1d29'
DONE='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
LEVEL_FLAGS=('MA20_hold','BB20_L_hold','BB40_L_hold','ENV20_L_10_hold',
             'ENV40_L_10_hold','FIB50_reclaim','FIB618_reclaim','signal_close_above_prev_high')
AMOUNT_FLAG='amount3d_above_prior10'
COMPARE_FLAGS=LEVEL_FLAGS+(AMOUNT_FLAG,)
INTERACTION_FLAGS=('MA20_hold','BB20_L_hold','BB40_L_hold','FIB50_reclaim','ENV20_L_10_hold')
# Pre-declared R1-D6 group definitions. Boundaries not fit to R1-D7 outcomes.
GAIN_LABELS=('12_TO_LT20','20_TO_LT35','GE35')
RETRACE_LABELS=('15_TO_LT30','30_TO_LT50','50_TO_618')
N_BOOT=1200
SEED=20261023


def fail(msg):raise RuntimeError('R1D8_FAIL_CLOSED_'+msg)
def sha256(raw):return hashlib.sha256(raw).hexdigest()
def git_blob(raw):return hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\x00'+raw).hexdigest()
def read_raw(root,rel):
 p=Path(root)/rel
 if not p.is_file():fail('MISSING_SOURCE_'+rel)
 return p.read_bytes()
def read_csv_bytes(raw,**kwargs):return pd.read_csv(io.BytesIO(gzip.decompress(raw)),dtype={'date':str,'code':str},low_memory=False,**kwargs)
def save_csv(df,path):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 df.to_csv(p,index=False,encoding='utf-8-sig')
def save_json(x,path):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 p.write_text(json.dumps(x,ensure_ascii=False,indent=2,sort_keys=True,default=str)+'\n',encoding='utf-8')


def verify_sources(repo_root,d6asof,d6sealed,d7artifact):
 module=Path(repo_root)/'research_formulas/formula66_r1d7_path_risk_audit.py'
 if not module.is_file() or git_blob(module.read_bytes())!=D7_SOURCE_BLOB:fail('R1D7_SOURCE_DRIFT')
 a=read_raw(d6asof,'R1D6_ASOF_ONLY_FROZEN_FEATURES_NO_OUTCOME.csv.gz')
 if sha256(a)!=D6_ASOF_BYTE_SHA:fail('R1D6_ASOF_BYTES_DRIFT')
 d6m=json.loads(read_raw(d6sealed,'result/R1D6_MANIFEST.json'))
 d7m=json.loads(read_raw(d7artifact,'result/R1D7_MANIFEST.json'))
 if d6m.get('window')!=['2024-01-02','2025-09-30'] or d6m.get('cooldown_20session_episodes')!=5247 or d6m.get('outcome_D20_complete')!=5214:
  fail('D6_MANIFEST_COUNT_OR_WINDOW_DRIFT')
 if d7m.get('source_run_D6')!=D6_RUN or d7m.get('frozen_signal_population')!=5247 or d7m.get('source_KRX_daily_hashes_verified')!=1907:
  fail('D7_ORIGINAL_KRX_AND_SIGNAL_PINS_DRIFT')
 if not d7m.get('D20_reproduced_exactly',{}).get('passed') or d7m.get('new_independent_OOS') or d7m.get('orders_placed')!=0:
  fail('D7_SOURCE_REPRO_OR_READ_ONLY_DRIFT')
 asof=read_csv_bytes(a)
 frozen=read_csv_bytes(read_raw(d6sealed,'result/R1D6_SEALED_ONLY_FORWARD_PRICE_PATHS.csv.gz'))
 paths=read_csv_bytes(read_raw(d7artifact,'result/R1D7_D5_D10_D15_D20_FROZEN_FORWARD_PRICE_PATHS.csv.gz'))
 if len(asof)!=5247 or len(frozen)!=5247 or len(paths)!=5247*4:fail('ROW_COUNT_DRIFT')
 if asof[['date','code']].duplicated().any() or frozen[['date','code']].duplicated().any() or paths[['date','code','horizon']].duplicated().any():fail('DUPLICATE_KEYS')
 if set(zip(asof.date,asof.code))!=set(zip(frozen.date,frozen.code)) or set(zip(paths.date,paths.code))!=set(zip(asof.date,asof.code)):fail('POPULATION_DRIFT')
 if set(paths.horizon)!={5,10,15,20} or paths.groupby(['date','code']).horizon.nunique().ne(4).any():fail('HORIZON_DRIFT')
 if any(c in asof for c in ('net_20bps_pct','gross_pct','entry_open','exit_close','status_out','horizon','mfe_intraday_pct')):fail('FUTURE_COLUMN_LEAK_IN_ASOF')
 if not (asof.date==asof.reference_data_last_bar_date).all():fail('ASOF_DATE_AFTER_SIGNAL')
 if any(c not in asof for c in COMPARE_FLAGS+('wave_gain_pct','wave_retrace_pct','joint_index_20d_regime')):fail('MISSING_FROZEN_FEATURE')
 if int((paths.horizon.eq(20)&paths.status_out.eq(DONE)).sum())!=5214:fail('D20_COMPLETE_MISMATCH')
 from research_formulas.formula66_r1d7_path_risk_audit import compare_d20
 if not compare_d20(paths,frozen)['passed']:fail('D20_REPRO_DRIFT')
 return asof,paths,{'D6':d6m,'D7':d7m}


def shape_bins(df):
 x=df.copy()
 gain=pd.to_numeric(x.wave_gain_pct,errors='coerce')
 ret=pd.to_numeric(x.wave_retrace_pct,errors='coerce')
 x['wave_gain_bin']=np.select([gain.lt(20),gain.ge(20)&gain.lt(35),gain.ge(35)],GAIN_LABELS,default='UNKNOWN')
 x['wave_retrace_bin']=np.select([ret.ge(15)&ret.lt(30),ret.ge(30)&ret.lt(50),ret.ge(50)&ret.le(61.8)],RETRACE_LABELS,default='UNKNOWN')
 x['amount_state']=pd.to_numeric(x[AMOUNT_FLAG],errors='coerce').astype('Int64')
 x['signal_year']=x.date.str.slice(0,4)
 x['market_regime']=np.where(x.joint_index_20d_regime.str.contains('BEAR20',na=False),'BEAR_ANY20',
      np.where(x.joint_index_20d_regime.str.contains('BULL20',na=False),'BULL_BOTH20','MIXED_NONEXTREME20'))
 first=set(map(tuple,x[['date','code']].sort_values(['date','code']).drop_duplicates('code')[['date','code']].itertuples(index=False,name=None)))
 x['code_first_in_cohort']=[(d,c) in first for d,c in zip(x.date,x.code)]
 return x


def _describe(df,group,metric,label):
 good=df[df.status_out.eq(DONE)]
 n=len(good)
 def med(col):return float(good[col].median()) if n else np.nan
 return {'group':group,'metric':metric,'label':str(label),'n_episodes':len(df),'n_complete':n,
  'n_codes':int(good.code.nunique()),'n_dates':int(good.date.nunique()),
  'failure5_n':int(good.fail5.sum()),'strong15_n':int(good.strong15.sum()),
  'failure5_pct':float(100*good.fail5.mean()) if n else np.nan,
  'strong15_pct':float(100*good.strong15.mean()) if n else np.nan,
  'median_return_pct':med('net_20bps_pct'),'median_MAE_pct':med('mae_intraday_pct'),
  'median_MFE_pct':med('mfe_intraday_pct'),
  'early_D5_fail5_pct':float(100*good.early_fail5.mean()) if n else np.nan,
  'up_first_pct':float(100*(good.price_barrier_order=='UP_FIRST').mean()) if n else np.nan,
  'down_first_pct':float(100*(good.price_barrier_order=='DOWN_FIRST').mean()) if n else np.nan}


def _date_bootstrap_ci(dat,iterations=N_BOOT,seed=SEED):
 """Uniform-date bootstrap on day-level flag1-minus-flag0 differences.

 Do not treat within-day events as i.i.d.; bootstrap eligible *days* (not bars).
 Intervals are descriptive sensitivity to date-cluster dependence.
 """
 vals=np.asarray(dat,dtype=float)
 if len(vals)<2:return np.nan,np.nan
 r=np.random.default_rng(seed)
 idx=r.integers(0,len(vals),size=(iterations,len(vals)))
 m=vals[idx].mean(axis=1)
 return float(np.quantile(m,.025)),float(np.quantile(m,.975))


def date_contrasts(done,flags=COMPARE_FLAGS,matched=False):
 out=[]
 group_cols=['date','wave_gain_bin','wave_retrace_bin'] if matched else ['date']
 for flag in flags:
  dates=[];all_contrasts=[];cells=0;pos_n=neg_n=0
  for idx,g in done.groupby(group_cols,sort=True):
   yes=g[g[flag].eq(1)];no=g[g[flag].eq(0)]
   if yes.empty or no.empty:continue
   day=idx[0] if isinstance(idx,tuple) else idx
   pos_n+=len(yes);neg_n+=len(no)
   cells+=1
   all_contrasts.append({'date':day,'failure':100*(yes.fail5.mean()-no.fail5.mean()),
                         'strong':100*(yes.strong15.mean()-no.strong15.mean()),
                         'mae':float(yes.mae_intraday_pct.mean()-no.mae_intraday_pct.mean())})
  if all_contrasts:
   by_day=pd.DataFrame(all_contrasts).groupby('date')[['failure','strong','mae']].mean()
   dates=by_day.index.tolist()
   for key in ('failure','strong','mae'):
    lo,hi=_date_bootstrap_ci(by_day[key].values,seed=SEED+len(flag)+int(matched)*100+(0 if key=='failure' else 1 if key=='strong' else 2))
    out.append({'condition':flag,'comparison':'DATE_WAVE_RETRACE_MATCHED' if matched else 'SAME_DATE_UNMATCHED',
                'outcome':key,'n_eligible_dates':len(dates),'n_matched_strata':cells,
                'n_flag1_observations_in_cells':pos_n,'n_flag0_observations_in_cells':neg_n,
                'mean_flag1_minus_flag0':float(by_day[key].mean()),'bootstrap_date_cluster_95lo':lo,
                'bootstrap_date_cluster_95hi':hi,
                'not_independent_study':1,'multiple_contrasts_exploratory':1})
  else:
   for key in ('failure','strong','mae'):
    out.append({'condition':flag,'comparison':'DATE_WAVE_RETRACE_MATCHED' if matched else 'SAME_DATE_UNMATCHED',
                'outcome':key,'n_eligible_dates':0,'n_matched_strata':0,
                'n_flag1_observations_in_cells':0,'n_flag0_observations_in_cells':0,
                'mean_flag1_minus_flag0':np.nan,'bootstrap_date_cluster_95lo':np.nan,
                'bootstrap_date_cluster_95hi':np.nan,'not_independent_study':1,
                'multiple_contrasts_exploratory':1})
 return pd.DataFrame(out)


def analyze(asof,paths,out):
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 x=shape_bins(asof)
 # Outcomes can enter ONLY after all as-of category/flag features have been frozen.
 asof_freeze_hash=sha256(x.sort_values(['date','code']).to_csv(index=False).encode('utf-8'))
 p5=paths[paths.horizon==5][['date','code','status_out','net_20bps_pct']].rename(columns={'status_out':'D5_status','net_20bps_pct':'D5_net20bps_pct'})
 p20=paths[paths.horizon==20]
 p20=p20.merge(p5,on=['date','code'],validate='one_to_one')
 joined=x.merge(p20,on=['date','code'],validate='one_to_one')
 if len(joined)!=5247:fail('JOIN_CARDINALITY')
 joined['fail5']=joined.status_out.eq(DONE)&joined.net_20bps_pct.le(-5)
 joined['strong15']=joined.status_out.eq(DONE)&joined.net_20bps_pct.ge(15)
 joined['early_fail5']=joined.D5_status.eq(DONE)&joined.D5_net20bps_pct.le(-5)
 # Fail-closed early horizon only for D20-complete cohort: no censored forward leakage.
 done=joined[joined.status_out.eq(DONE)].copy()
 if len(done)!=5214 or done.fail5.sum()!=1902 or done.strong15.sum()!=541:fail('D20_FROZEN_OUTCOME_DRIFT')
 if done.date.ge('2025-10-01').any():fail('DEVELOPMENT_DATE_LEAK')
 # Pool outcomes descriptive, with year/regime/first-only view.
 summary=[]
 summary.append(_describe(joined,'ALL_D6_D7','TOTAL','ALL'))
 summary.append(_describe(joined[joined.code_first_in_cohort],'FIRST_IN_COHORT','TOTAL','ALL'))
 for y,g in joined.groupby('signal_year'):summary.append(_describe(g,'YEAR','YEAR',y))
 for reg,g in joined.groupby('market_regime'):summary.append(_describe(g,'REGIME','REGIME',reg))
 for (y,reg),g in joined.groupby(['signal_year','market_regime']):summary.append(_describe(g,'YEAR_REGIME','YEAR_REGIME',str(y)+'_'+str(reg)))
 for (w,r),g in joined.groupby(['wave_gain_bin','wave_retrace_bin']):summary.append(_describe(g,'WAVE_X_RETRACE','GAIN_RETRACE',str(w)+'__'+str(r)))
 for flag in COMPARE_FLAGS:
  for v in (0,1):summary.append(_describe(joined[joined[flag].eq(v)],'FROZEN_FEATURE',flag,v))
 save_csv(pd.DataFrame(summary),out/'R1D8_YEAR_REGIME_FIRST_CODE_AND_WAVE_STRUCTURE.csv')
 cells=[]
 for flag in INTERACTION_FLAGS:
  for a in (0,1):
   for b in (0,1):
    sub=joined[joined[flag].eq(a)&joined[AMOUNT_FLAG].eq(b)]
    cells.append({**_describe(sub,'SUPPORT_X_AMOUNT',flag,f'{a}__{b}'),
                  'support_flag':flag,'support_value':a,'amount_value':b,'min_30_completed_warning':int(sub.status_out.eq(DONE).sum()<30)})
 save_csv(pd.DataFrame(cells),out/'R1D8_SUPPORT_X_AMOUNT_FROZEN_CELLS.csv')
 # Intraday first barrier touches: 1 day ambiguity never sorted using high/low chronology.
 barr=[]
 for label,sub in [('ALL',done),('FIRST_CODE_ONLY',done[done.code_first_in_cohort])]:
  for k,g in sub.groupby('price_barrier_order'):
   barr.append({'cohort':label,'barrier_order':k,'n':len(g),'pct':100*len(g)/len(sub)})
 save_csv(pd.DataFrame(barr),out/'R1D8_FIRST_BARRIER_ORDER_DESCRIPTIVE.csv')
 # Conditional risk vs matched same-day and matched same-day coarse wave structure.
 all_ct=pd.concat([date_contrasts(done),date_contrasts(done,matched=True)],ignore_index=True)
 save_csv(all_ct,out/'R1D8_DATE_CLUSTERED_CONTRASTS_WITH_CI.csv')
 # First-code only repeats contrast excluding repeated observations, sometimes underpowered.
 first_ct=pd.concat([date_contrasts(done[done.code_first_in_cohort],matched=False),
                     date_contrasts(done[done.code_first_in_cohort],matched=True)],ignore_index=True)
 save_csv(first_ct,out/'R1D8_FIRST_CODE_DATE_CONTRAST_SENSITIVITY.csv')
 # Risk by year and support/amount: check directional instability, no winner picking.
 yr=[]
 for flag in COMPARE_FLAGS:
  for year,g in joined.groupby('signal_year'):
   a=g[g[flag].eq(1)];b=g[g[flag].eq(0)]
   for value,z in ((1,a),(0,b)):
    yr.append({**_describe(z,'YEAR_FLAG',flag,f'{year}__{value}'),'year':year,'flag':flag,'value':value})
 save_csv(pd.DataFrame(yr),out/'R1D8_YEAR_X_FROZEN_FLAG.csv')
 # Retrospective diagnostic ONLY (outcome columns available) -- labels cannot be written back to as-of.
 sliced=done[['date','code','signal_year','market_regime','wave_gain_bin','wave_retrace_bin','code_first_in_cohort',
              'wave_gain_pct','wave_retrace_pct',*COMPARE_FLAGS,'net_20bps_pct','mae_intraday_pct','mfe_intraday_pct',
              'D5_net20bps_pct','fail5','strong15','price_barrier_order']].copy()
 sliced.to_csv(out/'R1D8_SEALED_JOINED_DESCRIPTIVE_AUDIT.csv.gz',index=False,compression='gzip',encoding='utf-8-sig')
 manifest={'version':VERSION,'source_D6_run':D6_RUN,'source_D7_run':D7_RUN,'source_D6_asof_SHA256':D6_ASOF_BYTE_SHA,
   'asof_frozen_derived_features_SHA256':asof_freeze_hash,'source_D7_KRX_1907_daily_hashes_verified_upstream':True,
   'source_D7_D20_exact_reproduction_upstream':True,
   'upstream_source_sha_verified_in_this_run':False,
   'cohort':'FROZEN_R1D6_2024_2025_RETROSPECTIVE',
   'n_original_signals':len(asof),'n_unique_codes':asof.code.nunique(),'n_dates':asof.date.nunique(),
   'n_d20_complete':len(done),'n_d20_fail5':int(done.fail5.sum()),'n_d20_strong15':int(done.strong15.sum()),
   'n_first_in_cohort_complete':int(done.code_first_in_cohort.sum()),
   'n_repeated_signal_codes':int(asof.code.value_counts().gt(1).sum()),
   'bootstrap_resamples':N_BOOT,'bootstrap_seed':SEED,'bootstrap_unit':'SIGNAL_DATE',
   'within_date_matching':['date','date+predeclared_wave_gain_bin+predeclared_close_retrace_bin'],
   'no_new_independent_OOS':True,'no_new_search_or_optimized_cutoffs':True,
   'not_native_original_66_formula_rescreen':True,'not_causal_identification':True,
   'sample_is_not_iid':True,'multiple_testing_not_strategy_selection':True,
   'original_KRX_unadjusted_and_PIT_tradeability_not_certified':True,
   'new_live_score_rank_or_orders':False,'orders_placed':0,'strategy_ready':False,'RESEARCH_ONLY':True,
   'cautions':['same-day stratification does not remove sector/security confounding',
    'bootstrap percentile bands are exploratory clustered sensitivity ranges, not confirmatory P-values',
    'the first signal within sampled 2024-25 period is not necessarily first in stock lifetime',
    'same-day upper/lower barrier order cannot be identified from daily OHLCV',
    'the R1D1 proxy excludes many full wave-low break outcomes by construction']}
 save_json(manifest,out/'R1D8_MANIFEST.json')
 print('R1D8_FROZEN_CONFOUNDS_RESEARCH_ONLY_COMPLETE',json.dumps({k:manifest[k] for k in ['n_original_signals','n_d20_complete','n_d20_fail5','n_d20_strong15','n_first_in_cohort_complete','n_repeated_signal_codes','no_new_independent_OOS','orders_placed']},ensure_ascii=False),flush=True)
 return manifest


def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--d6-asof',required=True);p.add_argument('--d6-sealed',required=True)
 p.add_argument('--d7',required=True);p.add_argument('--repo-root',default='.')
 p.add_argument('--out',default='reports/r1d8/result')
 a=p.parse_args(argv)
 asof,paths,_=verify_sources(a.repo_root,a.d6_asof,a.d6_sealed,a.d7)
 analyze(asof,paths,a.out)
 return 0
if __name__=='__main__':raise SystemExit(main())
