#!/usr/bin/env python3
"""Stock-Hunter R1-D6: independently timed holdout of FROZEN R1-D1/R1-D5 definitions.

Signals: existing R1-D1 KRX 224-wave proxy, as-of only; NOT native 66 formulas.
Features: existing R1-D5 implementation, frozen. Never imports LIVE trading code.
Retrospective KRX original unadjusted bars; price-only hypothetical paths, not orders.

Primary PRE-DECLARED holdout: 2024-01-02..2025-09-30 (nonoverlapping R1D5 2026 window,
and does not re-use R1D1's 2025 Oct-Dec 13 capture days).
"""
from __future__ import annotations
import argparse, csv, gzip, hashlib, json, math, statistics
from collections import defaultdict, deque, Counter
from pathlib import Path
import numpy as np
import pandas as pd

VERSION='STOCKHUNTER_R1D6_INDEPENDENT_PERIOD_FROZEN_R1D5_V1'
REPO='ehdud6728-bit/Stock-Hunter'
SIGNAL_START='2024-01-02'
SIGNAL_END='2025-09-30'
WARMUP_START='2022-01-03'
SOURCE_END='2026-10-12'
OUTCOME_H=20
SOURCE_RUN=39295295197
D5_RUN=39901194216
D5_REFERENCE_PATH='result/REVEALED_ONLY_GROUP_AND_SUPPORT_FEATURES.csv.gz'
D5_SOURCE_BLOB='453a2d5276e7067d40470d782e6be9265d237a00'
D1_SOURCE_BLOB='1df1820bff078446f44335216b7ee7910d1d9942'
D5_ARTIFACT_SHA='23120099814f65601088143a8bd6191cf304f52c5de64a44216aa4ecd4171ce5'
D5_ARTIFACT_ID=12478153934
EXPECTED_SHARD_DIGESTS={
 0:'84dd5aa62f0c9538cd9c2d0ed0540c8f74063b21be49d9ecfc81e4b1583368b5',
 1:'3d372eebf915c870badf103356c3700d5fa914a32d79ce8057ef1f4c4f89295b',
 2:'5c49df64ad10bafbc09dd7ea78b9377804735fda6ad861018750b396ab2a27cb',
 3:'7a359f5959e2374622c40d068920adf9d695142b4a61b540d0752a8cb9bb4dd8',
 4:'88b35ea4c9a8a6a0ed4b61250a8018d49ee7ba4489c7345f04012915bb60f997',
 5:'eb503f7cabcc40991037092c6463437c8a8b71d43fb26fe646ea4f56885b3814',
 6:'b3cc055d919c7ab92d7f27b1b3ce6d3e56df787b107af961eaa43dedb885d6cd',
 7:'9563747eb8b27928ba0a0d2b7d3cf02e787acd2e9e4a656d410c286139321478'
}

# Exactly frozen R1D5 binary factors, preselected BEFORE independent holdout read.
HYPOTHESES={
 'H01_MA20_HOLD':'MA20_hold',
 'H02_FIB50_RECLAIM':'FIB50_reclaim',
 'H03_PREV_HIGH_BREAK':'signal_close_above_prev_high',
 'H04_WAVE_LOW_INTRADAY_INTACT':'pullback_min_above_wave_low',
 'H04B_WAVE_LOW_RECLAIM':'WAVE_LOW_reclaim',
 'H04C_WAVE_LOW_STILL_BELOW':'WAVE_LOW_fail',
 'H05_BB20_HOLD':'BB20_L_hold',
 'H05_BB20_RECLAIM':'BB20_L_reclaim',
 'H05_BB40_HOLD':'BB40_L_hold',
 'H05_BB40_RECLAIM':'BB40_L_reclaim',
 'H05_ENV20_HOLD':'ENV20_L_10_hold',
 'H05_ENV20_RECLAIM':'ENV20_L_10_reclaim',
 'H05_ENV40_HOLD':'ENV40_L_10_hold',
 'H05_ENV40_RECLAIM':'ENV40_L_10_reclaim',
 'H06_AMOUNT_SIGNAL_VS_5':'signal_amount_above_prev5',
 'H06_AMOUNT3_VS_10':'amount3d_above_prior10',
}
# Interactions with the already-frozen >=1 amount thresholds (R1-D5).
SUPPORT_STATES=['MA20_hold','FIB50_reclaim','BB20_L_hold','BB20_L_reclaim',
                'BB40_L_hold','BB40_L_reclaim','ENV20_L_10_hold','ENV20_L_10_reclaim',
                'ENV40_L_10_hold','ENV40_L_10_reclaim','WAVE_LOW_fail']


def fail(m):raise RuntimeError('R1D6_FAIL_CLOSED_'+str(m))

def git_blob(path):
 b=Path(path).read_bytes()
 return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()

def verify_source_frozen(repo_root):
 r=Path(repo_root)
 for name,expected in [('research_formulas/formula66_r1d1_native_context.py',D1_SOURCE_BLOB),
                       ('research_formulas/formula66_r1d5_support_reaction.py',D5_SOURCE_BLOB)]:
  got=git_blob(r/name)
  if got!=expected:fail(f'IMMUTABLE_FUNCTION_SOURCE_DRIFT:{name}:wanted={expected}:got={got}')
 from research_formulas import formula66_r1d1_native_context as d1
 from research_formulas import formula66_r1d5_support_reaction as d5
 if d1.PROXY['ma_period']!=224 or d1.PROXY['below224_lookback']!=60 or d1.PROXY['lookback_history_bars']!=350 or d1.MIN_HISTORY!=284:
  fail('D1_PROXY_PARAMETER_DRIFT')
 if d5.MIN_ROWS!=225 or 'FIB50' not in d5.BANDS or 'ENV40_L_10' not in d5.BANDS:
  fail('D5_FEATURE_CONTRACT_DRIFT')
 return d1,d5

def source_manifest(root):
 root=Path(root);daily={}
 for p in sorted(root.glob('shard_*/manifest_shard_*.json')):
  m=json.loads(p.read_text('utf-8'))
  for row in m['files']:
   day=row['date'];path=p.parent/row['file']
   if day in daily:fail('DUPLICATE_MANIFEST_DAY_'+day)
   daily[day]=(path,row['sha256'])
 if len(daily)!=1907 or min(daily)!='2019-01-02' or max(daily)!=SOURCE_END:
  fail('FULLMARKET_DATE_COVERAGE_OR_SOURCE_DRIFT')
 return daily

def verify_all_daily_raw_sha(src):
 """Fail closed on all 1,907 daily KRX price files, not only the OOS scan slice."""
 checked=0
 for day,(path,sha) in sorted(src.items()):
  if not path.is_file():fail('FULLMARKET_SHA_SOURCE_DAY_MISSING_'+day)
  if hashlib.sha256(path.read_bytes()).hexdigest()!=sha:
   fail('FULLMARKET_DAILY_RAW_SHA_DRIFT_'+day)
  checked+=1
 if checked!=1907:fail('EXPECTED_ALL_1907_SHA_CHECKS')
 return checked

def index_asof(root):
 p=Path(root)/'shard_00'/'official_indices.csv'
 x=pd.read_csv(p,dtype={'date':str,'index':str})
 if x.duplicated(['date','index']).any():fail('DUPLICATE_INDEX_DATES')
 a=x.pivot(index='date',columns='index',values='close').sort_index()
 if len(a)!=1907 or set(a.columns)!={'KOSPI','KOSDAQ'} or a.isna().any().any():
  fail('INDEX_DATE_OR_MARKET_COVERAGE')
 a=a.astype(float)
 out={}
 for day, r in a.iterrows():
  ix=int(a.index.get_loc(day));
  kr=100*(float(r['KOSPI'])/float(a.iloc[ix-20]['KOSPI'])-1) if ix>=20 else float('nan')
  qr=100*(float(r['KOSDAQ'])/float(a.iloc[ix-20]['KOSDAQ'])-1) if ix>=20 else float('nan')
  if not all(math.isfinite(v) for v in (kr,qr)): reg='UNKNOWN'
  elif kr<=-5 and qr<=-5:reg='BOTH_INDEX_BEAR20_LE_MINUS5'
  elif kr<=-5:reg='KOSPI_ONLY_BEAR20_LE_MINUS5'
  elif qr<=-5:reg='KOSDAQ_ONLY_BEAR20_LE_MINUS5'
  elif kr>=5 and qr>=5:reg='BOTH_INDEX_BULL20_GE_PLUS5'
  else:reg='MIXED_OR_NONEXTREME_20D'
  out[day]={'KOSPI_ret20_pct':kr,'KOSDAQ_ret20_pct':qr,
            'joint_index_20d_regime':reg,'KOSPI_close':float(r['KOSPI']),
            'KOSDAQ_close':float(r['KOSDAQ'])}
 return list(a.index),out

def write_csv(df,path):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 df.to_csv(p,index=False,encoding='utf-8-sig',compression='gzip' if p.suffix=='.gz' else None)

def write_json(data,path):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 p.write_text(json.dumps(data,indent=2,sort_keys=True,ensure_ascii=False,default=str)+'\n',encoding='utf-8')


def asof_flags(feat):
 # These definitions are frozen R1D5 post-asof processing, not learned in R1D6.
 v=feat.copy()
 def safe(name):
  try:return float(v[name])
  except (ValueError,KeyError,TypeError):return float('nan')
 v['signal_amount_above_prev5']=int(safe('signal_amount_vs_prior5median')>=1.)
 v['amount3d_above_prior10']=int(safe('amount_3d_vs_prior10d')>=1.)
 v['signal_low_wick_gt30']=int(safe('signal_lower_wick_fraction')>=.30)
 v['postpeak_min_rebound_gt2']=int(safe('bounce_from_postpeak_min_to_signal_close_pct')>=2.)
 v['postpeak_min_rebound_gt5']=int(safe('bounce_from_postpeak_min_to_signal_close_pct')>=5.)
 return v


def priceday(raw,day):
 df=pd.read_csv(raw,compression='gzip',dtype={'code':str,'date':str},
                usecols=['code','date','open','high','low','close','volume','amount'])
 if df.empty or df.date.nunique()!=1 or df.date.iloc[0]!=day or df.code.duplicated().any():
  fail('PRICE_DAY_SCHEMA_OR_DUPLICATE_'+day)
 return df


def forward_outcome(ep, calendar, index, horizon=OUTCOME_H):
 """D2-compatible next session OPEN to D+20 CLOSE; no actual fill or stop-first."""
 entry_ix=ep['signal_ix']+1;last_ix=ep['signal_ix']+horizon
 arr=ep['forward_bars'];d=ep['date'];code=ep['code']
 base={'date':d,'code':code,'signal_ix':ep['signal_ix'],
       'entry_day':calendar[entry_ix] if entry_ix<len(calendar) else '',
       'exit_day':calendar[last_ix] if last_ix<len(calendar) else '',
       'horizon':horizon,'entry_rule':'NEXT_SESSION_OPEN_HYPOTHETICAL_NOT_FILL_CERTIFIED',
       'exit_rule':'SIGNAL_PLUS_20_TH_TRADING_SESSION_CLOSE',
       'status_out':'','net_20bps_pct':np.nan,'gross_pct':np.nan,
       'max_intraday_adverse_pct':np.nan,'max_intraday_favorable_pct':np.nan,
       'gap_risk_flag':0,'fwd_count':len(arr)}
 if last_ix>=len(calendar):base['status_out']='RIGHT_CENSORED_SOURCE_END_NO_FUTURE_PRICE';return base
 if len(arr)!=horizon or [x[0] for x in arr]!=calendar[entry_ix:last_ix+1]:
  base['status_out']='INTERMEDIATE_SUSPENSION_OR_MISSING_PATH';return base
 vals=np.array([x[1:] for x in arr],dtype=float)
 if vals.shape!=(horizon,6) or not np.isfinite(vals).all() or (vals<=0).any():
  base['status_out']='MISSING_OR_SUSPENDED_ENTRY_OR_EXIT';return base
 priorclose=float(ep['signal_close'])
 for price in vals:
  if abs(price[0]/priorclose-1)>0.40:
   base['status_out']='UNADJUSTED_FUTURE_CORPORATE_ACTION_OR_EXTREME_GAP_BLOCKED'
   base['gap_risk_flag']=1;return base
  priorclose=float(price[3])
 opening=float(vals[0,0]);gross=100*(float(vals[-1,3])/opening-1)
 base.update(status_out='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED',entry_open=opening,
             exit_close=float(vals[-1,3]),gross_pct=gross,net_20bps_pct=gross-.20,
             max_intraday_adverse_pct=100*(np.min(vals[:,2])/opening-1),
             max_intraday_favorable_pct=100*(np.max(vals[:,1])/opening-1))
 return base


def _one_episode_from_history(history, e, d5):
 """From signal-day-only deque, invoke ORIGINAL R1D5 feature code unchanged."""
 frame=pd.DataFrame([x[1:] for x in history],columns=['date','open','high','low','close','volume','amount'])
 if frame.date.duplicated().any():fail('DUPLICATE_HISTORY_'+e['code'])
 frame=frame.set_index('date')
 frame=d5._roll_indicators(frame)
 output=d5.one_episode(frame, e)
 if output.get('status_feature')!='ASOF_OK':fail('D5_ASOF_FEATURE_INCOMPLETE_'+str(output))
 return asof_flags(output)


def census(krx_root,out,d1,d5,source_sha_verify=True):
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 src=source_manifest(krx_root);calendar,indices=index_asof(krx_root)
 if set(src)!=set(calendar):fail('RAW_AND_INDEX_CALENDAR_MISMATCH')
 fully_verified=verify_all_daily_raw_sha(src) if source_sha_verify else 0
 if SIGNAL_START not in calendar or SIGNAL_END not in calendar or WARMUP_START not in calendar:
  fail('HOLDOUT_DATE_NOT_EXACT_KRX_SESSION')
 first=calendar.index(SIGNAL_START);last=calendar.index(SIGNAL_END)
 if last+OUTCOME_H>=len(calendar):fail('NO_FULL_D20_FORWARD_COVERAGE')
 if calendar[first-1]!='2023-12-28':
  # 2023-12-28 was last 2023 KRX session; enforce exact boundary for this revision.
  fail('UNEXPECTED_2024_BEGINS_SESSION')
 today_set=set(calendar[first:last+1]);cal_idx={d:i for i,d in enumerate(calendar)}
 hist=defaultdict(lambda:deque(maxlen=d1.PROXY['lookback_history_bars']))
 last_epi={};pending=defaultdict(list); all_eps=[];all_asof=[];audit=[]
 t=0;ready=0;strict=0;blocked=Counter();selected_days=0
 for day in calendar[cal_idx[WARMUP_START]:last+OUTCOME_H+1]:
  file,sha=src[day]
  if not file.is_file():fail('DAILY_GZIP_MISSING_'+day)
  raw=file.read_bytes()
  if source_sha_verify and hashlib.sha256(raw).hexdigest()!=sha:fail('DAILY_RAW_SHA_DRIFT_'+day)
  df=priceday(file,day)
  is_signal=day in today_set
  day_count=0;day_new=0;date_t=len(audit)
  # Each iteration only uses that day's bars. Forward path recording is not queried by selector.
  for rr in df.itertuples(index=False):
   code=str(rr.code)
   o,h,l,c,v,a=(float(rr.open),float(rr.high),float(rr.low),float(rr.close),float(rr.volume),float(rr.amount))
   if is_signal or day<SIGNAL_START:
    hist[code].append((t,day,o,h,l,c,v,a))
   if code in pending:
    for ep in pending[code]:
     if day>ep['date'] and len(ep['forward_bars'])<OUTCOME_H:
      ep['forward_bars'].append((day,o,h,l,c,v,a))
   if not is_signal:continue
   ft=d1.asof_features(hist[code],t,day)
   blocked[str(ft.status)]+=1
   if str(ft.status).startswith('KRX_PRICE_CONTEXT_READY'):ready+=1
   if not ft.strict_proxy:continue
   strict+=1;day_count+=1
   if code in last_epi and (t-last_epi[code])<20:continue
   last_epi[code]=t;day_new+=1
   dat=d1.feature_dict(ft)
   e={'date':day,'code':code,'wave_trough_date':dat['wave_trough_date'],
      'wave_peak_date':dat['wave_peak_date'],'wave_gain_pct':dat['wave_gain_pct'],
      'wave_retrace_pct':dat['wave_retrace_pct'],'signal_ix':cal_idx[day],
      'signal_close':c,'forward_bars':[],
      'joint_index_20d_regime':indices[day]['joint_index_20d_regime'],
      'KOSPI_ret20_pct':indices[day]['KOSPI_ret20_pct'],
      'KOSDAQ_ret20_pct':indices[day]['KOSDAQ_ret20_pct']}
   feat=_one_episode_from_history(hist[code],e,d5)
   feat.update({k:e[k] for k in ('joint_index_20d_regime','KOSPI_ret20_pct','KOSDAQ_ret20_pct',
                                'wave_gain_pct','wave_retrace_pct')})
   feat['reference_data_last_bar_date']=day
   feat['source_is_original_KRX_unadjusted']=1
   all_asof.append(feat);all_eps.append(e);pending[code].append(e)
  if is_signal:
   selected_days+=1
   audit.append({'date':day,'fullmarket_original_rows':len(df),'strict_proxy_hits':day_count,
                 'new_20session_episodes':day_new,'index_regime':indices[day]['joint_index_20d_regime']})
   if selected_days%25==0:
    print('R1D6_OOS_PROGRESS',day,'sessions',selected_days,'new_episodes',len(all_eps),flush=True)
  t+=1
 asof=pd.DataFrame(all_asof)
 if asof.empty:fail('NO_HOLDOUT_PROXY_SIGNALS')
 if len(all_eps)!=len(asof) or asof[['date','code']].duplicated().any():fail('DUPLICATE_ASOF_SIGNAL_KEYS')
 if not asof.status_feature.eq('ASOF_OK').all() or not asof.reference_data_last_bar_date.eq(asof.date).all():
  fail('ASOF_EXTRACT_OR_DATE_LEAK')
 if asof.date.min()<SIGNAL_START or asof.date.max()>SIGNAL_END:fail('SIGNAL_OUTSIDE_HOLDOUT')
 if any(c.startswith(('net_','gross_', 'exit_','fwd_','future_','status_out')) for c in asof.columns):
  fail('FORWARD_LABEL_COLUMN_CONTAMINATION_OF_ASOF')
 # Immutable ASOF artifact must exist BEFORE the path returns are built.
 asof_file=out/'R1D6_ASOF_ONLY_FROZEN_FEATURES_NO_OUTCOME.csv.gz'
 write_csv(asof,asof_file)
 asof_digest=hashlib.sha256(asof_file.read_bytes()).hexdigest()
 outcomes=pd.DataFrame([forward_outcome(x,calendar,indices) for x in all_eps])
 if outcomes[['date','code']].duplicated().any() or len(outcomes)!=len(asof):fail('OUTCOME_CARDINALITY')
 write_csv(outcomes,out/'R1D6_SEALED_ONLY_FORWARD_PRICE_PATHS.csv.gz')
 merged=asof.merge(outcomes.drop(columns=['signal_ix']),on=['date','code'],validate='one_to_one')
 merged['outcome_ok']=merged.status_out.eq('PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED').astype(int)
 merged['strong15']=((merged.outcome_ok==1)&merged.net_20bps_pct.ge(15)).astype(int)
 merged['failure5']=((merged.outcome_ok==1)&merged.net_20bps_pct.le(-5)).astype(int)
 merged['signal_year']=merged.date.str[:4]
 merged['signal_month']=merged.date.str[:7]
 merged['market_bucket']=merged.joint_index_20d_regime.map(lambda v: 'BEAR_ANY20' if 'BEAR20' in v else 'BULL_BOTH20' if 'BULL20' in v else 'MIXED_NONEXTREME20')
 write_csv(merged,out/'R1D6_SEALED_ASOF_X_OUTCOME_FOR_AUDIT.csv.gz')
 write_csv(pd.DataFrame(audit),out/'R1D6_DAILY_FULLMARKET_COVERAGE.csv')
 return merged,{'version':VERSION,'window':[SIGNAL_START,SIGNAL_END],
                'independent_from_r1d5_window':True,'source_KRX_run':SOURCE_RUN,
                'allmarket_days_scanned':selected_days,
                'strict_proxy_hits_not_deduped':strict,'cooldown_20session_episodes':len(asof),
                'unique_codes':int(asof.code.nunique()),'unique_signal_dates':int(asof.date.nunique()),
                'outcome_D20_complete':int(merged.outcome_ok.sum()),
                'outcome_blocked':int((merged.outcome_ok==0).sum()),
                'block_reasons':merged.loc[merged.outcome_ok==0,'status_out'].value_counts().to_dict(),
                'asof_features_sha256':asof_digest,
                'source_gzip_SHA_checked':bool(source_sha_verify),
                'source_all_1907_daily_SHA_checked':int(fully_verified),
                'signals_not_original_66_formulas':True,
                'live_scoring_or_orders_changed':False,'orders_placed':0,'strategy_ready':False,
                'pit_tradability_certified':False,'corporate_actions_adjusted':False,
                'security_type_pit_certified':False,'market_membership_pit_certified':False}


def wilson(k,n,z=1.96):
 if n<=0:return (np.nan,np.nan)
 p=k/n;d=1+z*z/n;mid=(p+z*z/(2*n))/d
 half=z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/d
 return (100*(mid-half),100*(mid+half))


def score_rows(rows,segment,category,group,has_flag=None):
 total=len(rows);d=rows[rows.outcome_ok==1]
 n=len(d);strong=int(d.strong15.sum());fails=int(d.failure5.sum());
 sr=wilson(strong,n);fr=wilson(fails,n)
 return {'segment':segment,'category':category,'group':str(group),'flag_value':has_flag if has_flag is not None else '',
         'n_episode':total,'n_complete':n,'n_blocked':total-n,
         'n_codes':int(d.code.nunique()),'n_dates':int(d.date.nunique()),
         'n_strong15':strong,'n_failure5':fails,
         'strong15_rate_pct':100*strong/n if n else np.nan,
         'fail5_rate_pct':100*fails/n if n else np.nan,
         'strong15_wilson_low_pct':sr[0],'strong15_wilson_high_pct':sr[1],
         'fail5_wilson_low_pct':fr[0],'fail5_wilson_high_pct':fr[1],
         'median_net20bps_pct':d.net_20bps_pct.median() if n else np.nan,
         'positive_rate_pct':100*d.net_20bps_pct.gt(0).mean() if n else np.nan,
         'median_path_MAE_pct':d.max_intraday_adverse_pct.median() if n and d.max_intraday_adverse_pct.notna().any() else np.nan,
         'pct_hit_intraday_minus10_pct':100*d.max_intraday_adverse_pct.le(-10).mean() if n and d.max_intraday_adverse_pct.notna().any() else np.nan,
         'small_sample_warning':int(n<30 or d.date.nunique()<8)}


def _binary_rows(df,segment,feature,regime='ALL'):
 out=[]
 if feature not in df.columns:fail('FROZEN_FEATURE_MISSING_'+feature)
 for val in (0,1):
  sub=df.loc[df[feature]==val]
  out.append(score_rows(sub,segment,feature,regime,val))
 return out


def effects_table(df,segment):
 results=[]
 for title,col in HYPOTHESES.items():
  rows=_binary_rows(df,segment,col)
  one=next(x for x in rows if x['flag_value']==1)
  zero=next(x for x in rows if x['flag_value']==0)
  results.extend(rows)
 return pd.DataFrame(results)


def d5_reference_extract(path):
 p=Path(path)
 if p.suffix.lower()=='.zip':
  import io,zipfile
  with zipfile.ZipFile(p) as z:
   names=z.namelist();matches=[n for n in names if n.endswith(D5_REFERENCE_PATH)]
   if len(matches)!=1:fail('R1D5_SEALED_INPUT_MEMBER_MISSING')
   d=pd.read_csv(io.BytesIO(z.read(matches[0])),compression='gzip',dtype={'code':str,'date':str})
 elif p.is_dir():
  path2=p/D5_REFERENCE_PATH
  if not path2.is_file():fail('R1D5_ARTIFACT_DIRECTORY_MISSING')
  d=pd.read_csv(path2,dtype={'code':str,'date':str})
 else:fail('R1D5_REFERENCE_INPUT_FORMAT_INVALID')
 if len(d)!=1249 or d.outcome_ok.sum()!=939 or d.date.min()<'2026-06-15' or d.date.max()>'2026-10-12':
  fail('R1D5_REFERENCE_COUNTS_AND_PERIOD_DRIFT')
 if int(d.net_20bps_pct.ge(15).sum())!=91 or int(d.net_20bps_pct.le(-5).sum())!=286:
  fail('R1D5_REFERENCE_OUTCOMES_DRIFT')
 d['strong15']=((d.outcome_ok==1)&d.net_20bps_pct.ge(15)).astype(int)
 d['failure5']=((d.outcome_ok==1)&d.net_20bps_pct.le(-5)).astype(int)
 # D5 did not extract intraday forward adverse/favorable returns.
 # It is NOT valid to impute these from the development D20-close return.
 d['max_intraday_adverse_pct']=np.nan
 return d


def summary(holdout,reference,out):
 out=Path(out)
 h=effects_table(holdout,'HOLDOUT_2024_TO_2025'); r=effects_table(reference,'DEVELOPMENT_2026_R1D5')
 write_csv(h,out/'R1D6_HOLDOUT_HYPOTHESES_BY_FLAG.csv')
 write_csv(r,out/'R1D6_FROZEN_R1D5_REFERENCE_BY_FLAG.csv')
 h1=h[h.flag_value==1].copy();r1=r[r.flag_value==1].copy()
 aligned=h1.merge(r1,on='category',suffixes=('_oos','_dev'),validate='one_to_one')
 aligned['strong15_effect_pp_oos_vs_dev']=aligned.strong15_rate_pct_oos-aligned.strong15_rate_pct_dev
 aligned['fail5_effect_pp_oos_vs_dev']=aligned.fail5_rate_pct_oos-aligned.fail5_rate_pct_dev
 aligned['risk_direction_reproduced']=np.where((aligned.n_complete_oos>=30)&(aligned.n_dates_oos>=8),
  np.where(aligned.fail5_rate_pct_oos < aligned.fail5_rate_pct_dev, 'OOS_FLAG_GROUP_LOWER_RAW_FAIL_THAN_DEV_NOT_CAUSAL',
    'OOS_FLAG_GROUP_NOT_LOWER_RAW_FAIL_THAN_DEV'), 'UNDETERMINED_LOW_OOS_SUPPORT')
 write_csv(aligned,out/'R1D6_FROZEN_FACTOR_CROSS_PERIOD_COMPARISON.csv')
 # Comparisons with within-period absent-flag reference are more important than raw OOS-vs-dev level.
 within=[]
 for t in (h,r):
  for col,g in t.groupby('category',sort=False):
   if set(g.flag_value)!={0,1}:continue
   yes=g[g.flag_value==1].iloc[0];no=g[g.flag_value==0].iloc[0]
   within.append({'period':str(yes.segment),'factor':col,
       'n_flag1':int(yes.n_complete),'n_flag0':int(no.n_complete),
       'flag1_dates':int(yes.n_dates),'flag0_dates':int(no.n_dates),
       'strong15_yes_minus_no_pp':yes.strong15_rate_pct-no.strong15_rate_pct,
       'fail5_yes_minus_no_pp':yes.fail5_rate_pct-no.fail5_rate_pct,
       'median_d20_yes_minus_no_pp':yes.median_net20bps_pct-no.median_net20bps_pct})
 w=pd.DataFrame(within)
 write_csv(w,out/'R1D6_WITHIN_PERIOD_FLAG_1_VS_0_EFFECTS.csv')
 cross=w.pivot(index='factor',columns='period',values=['strong15_yes_minus_no_pp','fail5_yes_minus_no_pp'])
 cross.columns=['__'.join(c) for c in cross.columns]
 cross=cross.reset_index()
 write_csv(cross,out/'R1D6_DEVELOPMENT_VS_HOLDOUT_DIRECTION_CHECK.csv')
 # Compare flag-positive vs flag-negative signals *on the same exact KRX session*.
 # This controls for the common date regime descriptively, not for stock selection.
 datepairs=[];c=holdout[holdout.outcome_ok==1]
 for label,fld in HYPOTHESES.items():
  daily=[]
  for day,q in c.groupby('date'):
   a=q[q[fld]==1];b=q[q[fld]==0]
   if a.empty or b.empty:continue
   daily.append({'date':day,'n_yes':len(a),'n_no':len(b),
                 'strong_diff_pp':100*(a.strong15.mean()-b.strong15.mean()),
                 'fail_diff_pp':100*(a.failure5.mean()-b.failure5.mean())})
  if daily:
   dd=pd.DataFrame(daily)
   datepairs.append({'factor':fld,'shared_signal_dates':len(dd),
                     'n_yes_in_shared_dates':int(dd.n_yes.sum()),
                     'n_no_in_shared_dates':int(dd.n_no.sum()),
                     'mean_date_weighted_strong_yes_minus_no_pp':dd.strong_diff_pp.mean(),
                     'mean_date_weighted_fail_yes_minus_no_pp':dd.fail_diff_pp.mean(),
                     'dates_with_yes_fail_rate_lower':int(dd.fail_diff_pp.lt(0).sum()),
                     'small_shared_date_warning':int(len(dd)<8)})
 write_csv(pd.DataFrame(datepairs),out/'R1D6_SAME_DATE_WITHIN_MARKET_FACTOR_CONTRAST.csv')
 # Year and frozen joint KOSPI/KOSDAQ 20-session market regime.
 region=[]
 for name,groups in [('YEAR',holdout.groupby('signal_year')),
                     ('MONTH',holdout.groupby('signal_month')),
                     ('JOINT_MARKET',holdout.groupby('joint_index_20d_regime')),
                     ('MARKET_BUCKET',holdout.groupby('market_bucket'))]:
  for value,sub in groups:
   region.append(score_rows(sub,'HOLDOUT',name,value))
   for factor in ('MA20_hold','FIB50_reclaim','signal_close_above_prev_high',
                  'pullback_min_above_wave_low','signal_amount_above_prev5'):
    region+=_binary_rows(sub,'HOLDOUT_'+name+'_'+str(value),factor,str(value))
 write_csv(pd.DataFrame(region),out/'R1D6_YEAR_MONTH_AND_ASOF_MARKET_REGIME.csv')
 # Support x 2 frozen amount indicators, no new data-dependent threshold.
 inter=[]
 for support in SUPPORT_STATES:
  for amount in ('signal_amount_above_prev5','amount3d_above_prior10'):
   for s in (0,1):
    for a in (0,1):
     sub=holdout[(holdout[support]==s)&(holdout[amount]==a)]
     inter.append(score_rows(sub,'OOS_INTERACTION',support+'__X__'+amount,f'{s}__{a}'))
 write_csv(pd.DataFrame(inter),out/'R1D6_SUPPORT_X_AMOUNT_INTERACTION.csv')
 # Frozen D5 has BOTH overlapping support flags and exclusive nearest support.
 # Exclusive assignment is distance to the postpeak intraday low <=2%,
 # NEVER fitted to outcomes and never construed as an optimal buy level.
 near=[]
 for label,sub in holdout.groupby('postpeak_low_nearest_support_within2pct',dropna=False):
  near.append(score_rows(sub,'OOS_EXCLUSIVE_NEAREST_D5_2PCT','nearest_support',label))
 write_csv(pd.DataFrame(near),out/'R1D6_EXCLUSIVE_NEAREST_SUPPORT.csv')
 states=[]
 for band in ['MA20','MA40','MA60','MA112','MA224','FIB50','FIB618','WAVE_LOW',
              'BB20_L','BB40_L','ENV20_L_10','ENV40_L_10']:
  fld=band+'_touch_type'
  for label,sub in holdout.groupby(fld,dropna=False):
   states.append(score_rows(sub,'OOS_D5_FROZEN_SUPPORT_BEHAVIOR',band,label))
 write_csv(pd.DataFrame(states),out/'R1D6_SUPPORT_HOLD_RECLAIM_BREAKDOWN_BY_LEVEL.csv')
 # These coarse structural bins are exploratory descriptive tabulations only,
 # not an additional detector or candidate-selection rule.
 bins=[('FIRST_WAVE_GAIN_LT20',holdout.wave_gain_pct.lt(20)),
       ('FIRST_WAVE_GAIN_20_TO35',holdout.wave_gain_pct.ge(20)&holdout.wave_gain_pct.lt(35)),
       ('FIRST_WAVE_GAIN_GE35',holdout.wave_gain_pct.ge(35)),
       ('CLOSE_RETRACE_15_TO30',holdout.wave_retrace_pct.ge(15)&holdout.wave_retrace_pct.lt(30)),
       ('CLOSE_RETRACE_30_TO50',holdout.wave_retrace_pct.ge(30)&holdout.wave_retrace_pct.lt(50)),
       ('CLOSE_RETRACE_50_TO618',holdout.wave_retrace_pct.ge(50)&holdout.wave_retrace_pct.le(61.8)),
       ('INTRADAY_RETRACE_OVER618',holdout.pullback_intraday_max_retrace_pct.gt(61.8)),
       ('SIGNAL_ABOVE_224',holdout.MA224_signal_above.eq(1)),
       ('SIGNAL_BELOW_224',holdout.MA224_signal_above.eq(0))]
 wave=[score_rows(holdout.loc[flag],'OOS_DESCRIPTIVE_WAVE_STRUCTURE_NOT_A_NEW_GATE',
                  'wave_or_retrace',name) for name,flag in bins]
 write_csv(pd.DataFrame(wave),out/'R1D6_WAVE_GAIN_RETRACE_AND_224_STRUCTURE.csv')
 yearreg=[score_rows(q,'OOS_YEAR_X_MARKET_BUCKET','year_x_regime',str(y)+'__'+str(m))
          for (y,m),q in holdout.groupby(['signal_year','market_bucket'])]
 write_csv(pd.DataFrame(yearreg),out/'R1D6_YEAR_X_MARKET_BUCKET.csv')
 # Same ticker may reappear after 20 session cooldown: sensitivity only first per security.
 first=holdout.sort_values(['date','code']).drop_duplicates('code',keep='first')
 firstrows=[score_rows(first,'SENSITIVITY_FIRST_CODE_ONLY','ALL','FIRST_CODE_ONLY')]
 for k in HYPOTHESES.values():firstrows+=_binary_rows(first,'FIRST_CODE_ONLY',k)
 write_csv(pd.DataFrame(firstrows),out/'R1D6_FIRST_CODE_ONLY_SENSITIVITY.csv')
 # Aggregate event rates with same-date weighting to avoid one unusually high-signal day dominating.
 complete=holdout[holdout.outcome_ok==1]
 eq=complete.groupby('date')[['strong15','failure5']].mean()
 write_csv(pd.DataFrame([{'n_complete_dates':len(eq),'equal_signal_day_weight_strong15_pct':100*eq.strong15.mean(),
                          'equal_signal_day_weight_fail5_pct':100*eq.failure5.mean(),
                          'episode_weight_strong15_pct':100*complete.strong15.mean(),
                          'episode_weight_fail5_pct':100*complete.failure5.mean()}]),
           out/'R1D6_DATE_CLUSTER_WEIGHTING_SENSITIVITY.csv')
 return {'hypotheses':len(HYPOTHESES),'oos_complete':len(complete),
         'oos_strong15_count':int(complete.strong15.sum()),
         'oos_failure5_count':int(complete.failure5.sum()),
         'factor_direction_table':'R1D6_DEVELOPMENT_VS_HOLDOUT_DIRECTION_CHECK.csv',
         'causal_claims':False,'multiple_hypothesis_tests':len(HYPOTHESES),
         'p_values_or_strategy_optimization':False}


def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--krx-root',required=True)
 p.add_argument('--reference-d5',required=True)
 p.add_argument('--repo-root',default='.')
 p.add_argument('--out',default='reports/r1d6/result')
 p.add_argument('--no-gzip-sha-check',action='store_true',help='local synthetic fixtures only, forbidden on GitHub')
 a=p.parse_args(argv)
 if a.no_gzip_sha_check and Path('.github').is_dir():fail('FORBIDDEN_SOURCE_VERIFY_BYPASS_IN_REPO')
 d1,d5=verify_source_frozen(a.repo_root)
 holdout,manifest=census(a.krx_root,a.out,d1,d5,source_sha_verify=not a.no_gzip_sha_check)
 reference=d5_reference_extract(a.reference_d5)
 manifest.update(summary(holdout,reference,a.out))
 write_json(manifest,Path(a.out)/'R1D6_MANIFEST.json')
 print('R1D6_COMPLETE_RESEARCH_ONLY',json.dumps(manifest,ensure_ascii=False),flush=True)
 return 0

if __name__=='__main__':raise SystemExit(main())
