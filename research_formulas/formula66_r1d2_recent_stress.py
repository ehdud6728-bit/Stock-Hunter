#!/usr/bin/env python3
"""Read-only recent 3/4-month KRX downside stress cohort using *frozen* R1D1 as-of 224 proxy.

NOT original 66 formulas, NOT PIT tradeability certified, NOT OOS, NOT LIVE.
Only KRX original unadjusted OHLCV + actual Amount, no ticker-selection by legacy bot.
Candidate conditions are evaluated only using timestamps on/before signal day.
Price-only forward outcomes are written to a separate file, never passed to candidate selector.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict, deque
from datetime import date as Date
from pathlib import Path

import numpy as np
import pandas as pd

WINDOWS={'recent_4m':('2026-06-15','2026-10-12'),
         'recent_3m':('2026-07-01','2026-10-12')}
SOURCE_START='2024-01-02'
SOURCE_END='2026-10-12'
HORIZONS=(5,10,15,20)
COST_BPS=(20,50,100)
EPISODE_COOLDOWN=20
R1D1_SOURCE_GIT_BLOB='1df1820bff078446f44335216b7ee7910d1d9942'
VERSION='FORMULA66_R1D2_RECENT_224_FROZEN_PROXY_DOWNSIDE_STRESS_V1'


def fail(msg):raise RuntimeError('R1D2_FAIL_CLOSED_'+str(msg))
def blob_sha(path):
 raw=Path(path).read_bytes()
 return hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
def write_json(path, obj):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 p.write_text(json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True,default=str)+'\n',encoding='utf-8')
def write_csv(path,rows,fields=None):
 rows=list(rows);fields=fields or list(dict.fromkeys(k for row in rows for k in row)) or ['status']
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w',newline='',encoding='utf-8-sig') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)

def only_frozen_r1d1(root,with_source_pins=True):
 root=Path(root)
 path=root/'research_formulas/formula66_r1d1_native_context.py'
 if not path.is_file() or blob_sha(path)!=R1D1_SOURCE_GIT_BLOB:
  fail('FROZEN_R1D1_224_PROXY_SOURCE_DRIFT')
 if with_source_pins:
  from research_formulas.formula66_krx_r1c_pinned_reprice import check_source_pins
  check_source_pins(root)
 from research_formulas import formula66_r1d1_native_context as old
 if old.PROXY['ma_period']!=224 or old.PROXY['below224_lookback']!=60 or old.MIN_HISTORY!=284:
  fail('FROZEN_R1D1_224_PROXY_PARAMETERS_DRIFT')
 return old

def load_index(root):
 p=Path(root)/'shard_00'/'official_indices.csv'
 if not p.is_file():fail('PINNED_KRX_OFFICIAL_INDICES_MISSING')
 d=pd.read_csv(p,dtype={'date':str,'index':str,'index_code':str},low_memory=False)
 if not {'date','index','close','index_code'}.issubset(d.columns):fail('INDEX_BAD_SCHEMA')
 if d.duplicated(['date','index']).any():fail('INDEX_DUPLICATE_DATE_MARKET')
 if set(d['index'])!={'KOSPI','KOSDAQ'}:fail('INDEX_MARKET_SET_INVALID')
 d['close']=pd.to_numeric(d['close'],errors='coerce')
 if d['close'].isna().any() or d['close'].le(0).any():fail('INDEX_BAD_CLOSE')
 wide=d.pivot(index='date',columns='index',values='close').sort_index()
 if len(wide)!=1907 or wide.isna().any().any() or wide.index[0]!='2019-01-02' or wide.index[-1]!=SOURCE_END:
  fail('INDEX_DATE_COVERAGE_INVALID')
 market={}
 for market_name in ('KOSPI','KOSDAQ'):
  wide[market_name+'_RET20_PCT']=100*(wide[market_name]/wide[market_name].shift(20)-1)
  wide[market_name+'_RET60_PCT']=100*(wide[market_name]/wide[market_name].shift(60)-1)
 for day,row in wide.iterrows():
  k=float(row['KOSPI_RET20_PCT']);q=float(row['KOSDAQ_RET20_PCT'])
  if not math.isfinite(k) or not math.isfinite(q):status='UNKNOWN'
  elif k<=-5 and q<=-5:status='BOTH_INDEX_BEAR20_LE_MINUS5'
  elif k<=-5:status='KOSPI_ONLY_BEAR20_LE_MINUS5'
  elif q<=-5:status='KOSDAQ_ONLY_BEAR20_LE_MINUS5'
  elif k>=5 and q>=5:status='BOTH_INDEX_BULL20_GE_PLUS5'
  else:status='MIXED_OR_NONEXTREME_20D'
  market[day]={ 'date':day,'KOSPI_close':float(row['KOSPI']),'KOSDAQ_close':float(row['KOSDAQ']),
   'KOSPI_ret20_pct':k,'KOSDAQ_ret20_pct':q,
   'KOSPI_ret60_pct':float(row['KOSPI_RET60_PCT']),
   'KOSDAQ_ret60_pct':float(row['KOSDAQ_RET60_PCT']),
   'joint_index_20d_regime':status,
   'per_ticker_historical_exchange_membership':'NOT_VERIFIED'}
 return list(wide.index),market

def choose_dates(calendar,window,cadence):
 start,end=WINDOWS[window]
 days=[d for d in calendar if start<=d<=end]
 if not days or days[0]!=start or days[-1]!=end:fail('RECENT_SCOPE_OR_CALENDAR_DRIFT')
 if cadence=='daily':return days
 if cadence!='weekly':fail('INVALID_CADENCE')
 buckets={}
 for d in days:
  iso=Date.fromisoformat(d).isocalendar()
  buckets[(iso.year,iso.week)]=d  # last KRX session in each ISO week
 return sorted(buckets.values())

def manifest_sources(root):
 root=Path(root)
 out={}
 for p in root.glob('shard_*/manifest_shard_*.json'):
  obj=json.loads(p.read_text('utf-8'))
  for r in obj['files']:
   if r['date'] in out:fail('DUPLICATE_DAILY_KRX_SOURCE')
   out[r['date']]=(p.parent/r['file'],r['sha256'])
 if len(out)!=1907:fail('KRX_SOURCE_DATE_SET_NOT_1907')
 return out

def _valid_bar(b,entry=False):
 if b is None:return False
 # original unadjusted KRX snapshot includes halted days with positive close and zero amount
 o,h,l,c,v,a=b
 return all(math.isfinite(float(x)) and float(x)>0 for x in (o,h,l,c,v,a))

def outcome_rows(episode,calendar,prices,markets):
 day=episode['date'];code=episode['code'];i=calendar.index(day)
 out=[]
 for h in HORIZONS:
  record={'code':code,'signal_date':day,'entry_rule':'NEXT_SESSION_OPEN_HYPOTHETICAL_NOT_FILL_CERTIFIED',
     'exit_rule':f'SIGNAL_PLUS_{h}_TH_TRADING_SESSION_CLOSE',
     'horizon':h,'price_source':'KRX_ORIGINAL_UNADJUSTED',
     'source_ends':SOURCE_END,'PIT_tradeability':'NOT_CERTIFIED',
     'security_type':'NOT_CERTIFIED', 'signals_from':'FROZEN_R1D1_INDEPENDENT_224_PROXY_NOT_66_FORMULAS',
     'reference_joint20_regime':episode['joint_index_20d_regime'],
     'signal_day_below_ma224':episode['below_ma224']}
  if i+h>=len(calendar):
   record['status']='RIGHT_CENSORED_SOURCE_END_NO_FUTURE_PRICE';out.append(record);continue
  entryday=calendar[i+1];exitday=calendar[i+h]
  ent=prices.get(entryday,{}).get(code)
  ex=prices.get(exitday,{}).get(code)
  record.update({'entry_day':entryday,'exit_day':exitday})
  if not _valid_bar(ent) or not _valid_bar(ex):
   record['status']='MISSING_OR_SUSPENDED_ENTRY_OR_EXIT';out.append(record);continue
  bars=[prices.get(calendar[j],{}).get(code) for j in range(i+1,i+h+1)]
  if any(not _valid_bar(b) for b in bars):
   record['status']='INTERMEDIATE_SUSPENSION_OR_MISSING_PATH';out.append(record);continue
  # Raw, unadjusted: no claim to adjusted price returns if any candidate split/rights anomaly.
  reference=prices.get(day,{}).get(code)
  priorclose=float(reference[3]) if reference else float('nan')
  broken=False
  for b in bars:
   if not math.isfinite(priorclose) or priorclose<=0 or abs(float(b[0])/priorclose-1)>0.40:
    broken=True;break
   priorclose=float(b[3])
  if broken:
   record['status']='UNADJUSTED_FUTURE_CORPORATE_ACTION_OR_EXTREME_GAP_BLOCKED';out.append(record);continue
  gross=100*(float(ex[3])/float(ent[0])-1)
  record.update({'status':'PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED',
     'entry_open':float(ent[0]),'exit_close':float(ex[3]),'gross_pct':gross})
  for bp in COST_BPS:record[f'net_{bp}bps_pct']=gross-bp/100
  for ix in ('KOSPI','KOSDAQ'):
   start=float(markets[day][ix+'_close']);end=float(markets[exitday][ix+'_close'])
   record[f'{ix}_signal_close_to_exit_close_pct']=100*(end/start-1)
  out.append(record)
 return out

def produce(root,reference,out,repo_root,window='recent_4m',cadence='daily',verify_source=True):
 """No legacy PRE capture required: current period scanned across ALL KRX tickers."""
 old=only_frozen_r1d1(repo_root,with_source_pins=verify_source)
 from research_pit.krx_full_market_pit import verify as krx_verify
 root=Path(root);out=Path(out);out.mkdir(parents=True,exist_ok=True)
 if verify_source:
  verified=krx_verify(root,reference,out/'krx_original_1907_sha_rows_verify.json')
  if verified['status']!='PASS_UNFILTERED_PRICE_COLLECTION_ONLY' or verified['sessions']!=1907:
   fail('FULL_KRX_ORIGINAL_INTEGRITY_NOT_VERIFIED')
 else:
  verified={'status':'LOCAL_TEST_ONLY_NOT_VERIFIED','sessions':0}
 calendar,markets=load_index(root)
 source=manifest_sources(root)
 if set(calendar)!=set(source):fail('SOURCE_CALENDAR_SET_MISMATCH')
 selected=choose_dates(calendar,window,cadence)
 selected_set=set(selected)
 trackdays=[d for d in calendar if SOURCE_START<=d<=SOURCE_END]
 date_ix={day:i for i,day in enumerate(calendar)}
 hist=defaultdict(lambda:deque(maxlen=old.PROXY['lookback_history_bars']))
 dates_to_print=set(selected)
 all_rows=0;eligible=0;candidates=[];episodes=[];coverage=[];cache={};last_code_episode={};recent_prices={}
 stage_status=Counter();selected_status=Counter();market_rows=[]
 corpus_path=out/'recent_fullmarket_asof_context.csv.gz'
 context_fields=['date','code','KRX_market_type_unverified','status','close','ma224','below_ma224',
  'below_60_ratio','recent5_vs_prior20_actual_amount','base_now','wave_found',
  'wave_gain_pct','wave_retrace_pct','prewave_accum_amount_ratio','pullback_dry_amount_ratio',
  'wave_trough_date','wave_peak_date','strict_proxy','candidate_stage','joint_index_20d_regime']
 with gzip.open(corpus_path,'wt',encoding='utf-8-sig',newline='') as gz:
  writer=csv.DictWriter(gz,fieldnames=context_fields);writer.writeheader()
  # Work from 2024 to warm up 224MA and 60-session below criteria before June 2026.
  # For every signal, future bars are NOT present in `hist` at evaluation time.
  for t,day in enumerate(trackdays):
   path,expected=source[day]
   if not path.exists():fail('KRX_DATE_RAW_MISSING:'+day)
   # krx_verify checks every daily SHA and record count; recheck for fail-closed provenance.
   if verify_source and hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
    fail('KRX_DATE_SHA_DRIFT:'+day)
   d=old._read_day_krx(path,day)
   on_day=day in selected_set
   if day>=selected[0]:
    recent_prices[day]={}
   ct=Counter();rawcount=len(d);candidatecount=0;episodecount=0;readycount=0
   # Date-specific entire source ticker population; no list selected using present-day code.
   for row in d.itertuples(index=False):
    code=str(row.code)
    vals=(float(row.open),float(row.high),float(row.low),float(row.close),float(row.volume),float(row.amount))
    hist[code].append((t,day,*vals))
    if day>=selected[0]:recent_prices[day][code]=vals
    if not on_day:continue
    f=old.asof_features(hist[code],t,day)
    dat=old.feature_dict(f)
    status=str(f.status)
    ct[status]+=1;selected_status[status]+=1
    all_rows+=1
    if status.startswith('KRX_PRICE_CONTEXT_READY'):readycount+=1;eligible+=1
    payload={key:dat.get(key,'') for key in context_fields if key not in ('date','code','KRX_market_type_unverified','joint_index_20d_regime')}
    writer.writerow({'date':day,'code':code,'KRX_market_type_unverified':'TRUE',
      'joint_index_20d_regime':markets[day]['joint_index_20d_regime'],**payload})
    if not int(f.strict_proxy):continue
    candidatecount+=1
    epi=(t-last_code_episode[code])>=EPISODE_COOLDOWN if code in last_code_episode else True
    item={'date':day,'code':code,'episode_first_in_20session_cooldown':int(epi),
      'index_session_i':date_ix[day], 'source_scope':window, 'scan_cadence':cadence,
      'joint_index_20d_regime':markets[day]['joint_index_20d_regime'],
      'KOSPI_ret20_pct':markets[day]['KOSPI_ret20_pct'],
      'KOSDAQ_ret20_pct':markets[day]['KOSDAQ_ret20_pct'],**dat}
    candidates.append(item)
    if epi:
     last_code_episode[code]=t;episodecount+=1;episodes.append(item)
   if on_day:
    coverage.append({'date':day,'raw_allmarket_ticker_rows':rawcount,
      'asof_284_session_history_eligible':readycount,'raw_insufficient_or_blocked':rawcount-readycount,
      'strict_proxy_hits_not_deduped':candidatecount,'new_episode_20session_cooldown':episodecount,
      'joint_index_20d_regime':markets[day]['joint_index_20d_regime'],
      'KOSPI_ret20_pct':markets[day]['KOSPI_ret20_pct'],
      'KOSDAQ_ret20_pct':markets[day]['KOSDAQ_ret20_pct'],
      'is_KRX_exchange_member_verified':False})
    market_rows.append(markets[day]);stage_status[markets[day]['joint_index_20d_regime']]+=1
    print('R1D2_DATE',day,'raw',rawcount,'eligible',readycount,'strict',candidatecount,'episodes',episodecount,'index20',markets[day]['joint_index_20d_regime'],flush=True)
 if len(coverage)!=len(selected) or all_rows<=2000:fail('DID_NOT_PROCESS_ALL_KRX_TICKERS')
 if len({(r['code'],r['date']) for r in candidates})!=len(candidates):fail('DUPLICATE_CANDIDATE_ID')
 if len({(r['code'],r['date']) for r in episodes})!=len(episodes):fail('DUPLICATE_EPISODE_ID')
 write_csv(out/'market_index_asof_regime_each_scanned_day.csv',market_rows)
 write_csv(out/'recent_3to4m_date_coverage.csv',coverage)
 write_csv(out/'recent_proxy_asof_all_hits.csv',candidates)
 write_csv(out/'recent_proxy_asof_20session_deduplicated_episodes.csv',episodes)
 # Only AFTER candidate selection and frozen as-of files are written, calculate forward outcomes.
 outcomes=[]
 for e in episodes:outcomes.extend(outcome_rows(e,calendar,recent_prices,markets))
 if len(outcomes)!=len(episodes)*len(HORIZONS):fail('OUTCOME_CARDINALITY_DRIFT')
 write_csv(out/'FORWARD_PRICE_OUTCOMES_ONLY_NOT_TO_BLIND_REVIEW.csv',outcomes)
 groups=defaultdict(list)
 for r in outcomes:
  signalday=str(r['signal_date']);mon=signalday[:7];h=int(r['horizon']);reg=r['reference_joint20_regime']
  groups[('ALL',h,'ALL')].append(r)
  groups[('MONTH_'+mon,h,'ALL')].append(r)
  groups[('INDEXREGIME_'+reg,h,'ALL')].append(r)
  groups[('BELOW224_'+str(r['signal_day_below_ma224']),h,'ALL')].append(r)
  groups[('MONTH_'+mon,h,'INDEXREGIME_'+reg)].append(r)
 score=[]
 for (segment,h,detail),rows in sorted(groups.items()):
  done=[float(r['net_20bps_pct']) for r in rows if r['status']=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED']
  score.append({'segment':segment,'market_subsegment':detail,'horizon_Dplus':h,
   'candidate_episodes':len(rows),'price_paths_complete':len(done),
   'blocked_or_right_censored':len(rows)-len(done),
   'distinct_signal_dates':len({r['signal_date'] for r in rows}),
   'unique_codes':len({r['code'] for r in rows}),
   'mean_net20bps_pct':statistics.mean(done) if done else '',
   'median_net20bps_pct':statistics.median(done) if done else '',
   'win_net20bps_pct':100*sum(v>0 for v in done)/len(done) if done else '',
   'status':'EXPLORATORY_MARKET_REGIME_STRESS_NOT_OUT_OF_SAMPLE' if done else 'NO_MATURE_COMPLETE_PRICE_PATH',
   'price_source':'KRX_UNADJUSTED_PROVENANCE_ONLY','production_authority':False})
 write_csv(out/'month_and_market_regime_price_only_scorecard.csv',score)
 # Sealed blind selection: round-robin by signal month + current side of SMA224.
 # Selection NEVER sees outcome rows/return values: month+224-side+pre-signal identity hash only.
 chart_groups=defaultdict(list)
 for e in episodes:
  chart_groups[(e['date'][:7],int(e.get('below_ma224',0)))].append(e)
 for group in chart_groups.values():
  group.sort(key=lambda e:hashlib.sha256((e['date']+'|'+e['code']).encode()).hexdigest())
 selected_blind=[]
 while len(selected_blind)<48 and any(chart_groups.values()):
  for key in sorted(chart_groups):
   if chart_groups[key] and len(selected_blind)<48:
    selected_blind.append(chart_groups[key].pop(0))
 charts=[]
 blindfile=out/'BLIND_CHART_PRE_SIGNAL_ONLY.csv.gz'
 with gzip.open(blindfile,'wt',encoding='utf-8-sig',newline='') as gz:
  w=csv.DictWriter(gz,fieldnames=['review_id','date','open','high','low','close','volume','actual_amount_KRW','pre_or_on_signal_day'])
  w.writeheader()
  for r in selected_blind:
   code=str(r['code']);day=r['date']; rid=hashlib.sha256(f'{day}/{code}'.encode()).hexdigest()[:16]
   # max history available in `hist` through the end; seal to <= event date.
   hrows=[v for v in hist.get(code,()) if v[1]<=day][-260:]
   if not hrows:continue
   charts.append({'review_id':rid,'code':code,'signal_date':day,'blind_chart_label':'NOT_REVIEWED',
      'chart_bars':len(hrows),'future_outcome_hidden':True,'index_regime_at_signal':r['joint_index_20d_regime']})
   for v in hrows:
    w.writerow({'review_id':rid,'date':v[1],'open':v[2],'high':v[3],'low':v[4],
      'close':v[5],'volume':v[6],'actual_amount_KRW':v[7],'pre_or_on_signal_day':1})
 write_csv(out/'BLIND_SEALED_CHART_REVIEW_KEYS.csv',charts)
 info={'version':VERSION,'status':'RESEARCH_ONLY_RECENT_DOWNSIDE_224_PROXY_CENSUS_COMPLETE',
  'source_verified':verify_source,'krx_source_run':39295295197,'total_historical_KRX_sessions':len(calendar),
  'source_ends':SOURCE_END,'window':window,'start':WINDOWS[window][0],'end':WINDOWS[window][1],
  'cadence':cadence,'KRX_sessions_scanned':len(selected),
  'allmarket_original_rows_scanned':all_rows,'eligible_284_history_rows':eligible,
  'strict_proxy_hits':len(candidates),'unique_proxy_hit_codes':len({r['code'] for r in candidates}),
  'cooldown_20session_episodes':len(episodes),'episode_forward_price_only_paths':len(outcomes),
  'price_outcomes_complete_by_horizon':{str(h):sum(r['status']=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED' and r['horizon']==h for r in outcomes) for h in HORIZONS},
  'right_censored_by_horizon':{str(h):sum(r['status']=='RIGHT_CENSORED_SOURCE_END_NO_FUTURE_PRICE' and r['horizon']==h for r in outcomes) for h in HORIZONS},
  'official_market_index_regime_dates':dict(stage_status),'status_by_original_KRX_asof_price_readiness':dict(selected_status),
  'frozen_same_R1D1_PROXIES':old.PROXY,'base_regime_definition':'Both index prior 20-session close-to-close <= -5% at signal date, separately not individual stock market',
  'first_appearance_episode_cooldown_sessions':EPISODE_COOLDOWN,
  'not_independent_66_formula_rescreen':True,'no_PIT_security_type_or_tradeability_certification':True,
  'no_adjusted_price_corporate_action_full_verification':True,'not_independent_OOS':True,
  'no_future_used_for_asof_signal':True,'BLIND_CHART_FORWARD_RETURN_LEAK':False,
  'research_only':True,'strategy_ready':False,'LIVE_CHANGED':False,'ORDERS_PLACED':0,
  'manual_prospective_overlay_from_REAL_FULL':'NOT_AUTO_JOINED_OR_RETRO_EDITED',
  'warning':'Index comparisons are KRX officially recorded series; stock exchange membership not PIT verified. Price-only paths are hypothetical, not executable. Market regime is chosen retrospectively. Do not tune thresholds on this period.'}
 write_json(out/'recent_3to4month_research_manifest.json',info)
 print('R1D2_RECENT_DONE',json.dumps({k:info[k] for k in ('status','window','KRX_sessions_scanned','allmarket_original_rows_scanned','strict_proxy_hits','cooldown_20session_episodes','price_outcomes_complete_by_horizon','source_verified')},ensure_ascii=False),flush=True)
 return info

def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--mode',choices=['contract','run'],default='contract')
 p.add_argument('--window',choices=list(WINDOWS),default='recent_4m')
 p.add_argument('--cadence',choices=['daily','weekly'],default='daily')
 p.add_argument('--krx-root',default='')
 p.add_argument('--reference',default='research_pit/historical_market_counts_20190102_20261012.csv')
 p.add_argument('--repo-root',default='.')
 p.add_argument('--out',default='reports/formula66_r1d2_recent')
 a=p.parse_args(argv)
 old=only_frozen_r1d1(a.repo_root)
 out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
 if a.mode=='contract':
  result={'status':'RECENT_R1D2_FROZEN_PROXY_SOURCE_CONTRACT_PASS','window_defs':WINDOWS,
      'frozen_R1D1_params':old.PROXY,'R1D1_blob':R1D1_SOURCE_GIT_BLOB,
      'strategy_ready':False,'66_exact_KRX_rescreen':False,'ORDERS_PLACED':0}
  write_json(out/'recent_contract.json',result)
  print('R1D2_RECENT_CONTRACT_PASS',flush=True)
 else:
  if not a.krx_root:fail('MISSING_KRX_ROOT')
  produce(a.krx_root,a.reference,out,a.repo_root,a.window,a.cadence)
 return 0

if __name__=='__main__':raise SystemExit(main())
