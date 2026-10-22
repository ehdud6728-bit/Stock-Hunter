#!/usr/bin/env python3
"""R1-D1: KRX-only as-of 224 / real-Amount shape census + 66-legacy chart auditing.

IMPORTANT: This does *not* claim native parity with any of the 66 legacy
combination formulas. The 66 formulas consume 52 proprietary derived signal
variables which must each be validated as-of before exact native rescreen.

Uses pinned 1907-day complete KRX original prices and the frozen R1-B PRE-only
capture. A KRX independent hypothesis is scored on every historical ticker
visible on each of the 13 legacy signal dates, even if absent from legacy
capture. Outputs are exploratory, no profit metrics, no live execution.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import re
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from statistics import median

import numpy as np
import pandas as pd

VERSION='STOCKHUNTER_FORMULA66_R1D1_KRX_NATIVE_CONTEXT_BLIND_CHART_V1'
RESEARCH_ONLY=True
# Research-proxy thresholds set BEFORE evaluation. NOT claimed to be exact main7 semantics.
PROXY={
  'ma_period':224, 'below224_lookback':60, 'below224_min_ratio':0.70,
  'accum_recent':5, 'accum_prior':20, 'accum_min_ratio':1.03, 'accum_max_ratio':2.50,
  'wave_min_gain':0.12, 'wave_peak_days_ago_min':3, 'wave_peak_days_ago_max':15,
  'trough_before_peak_min_days':3, 'trough_before_peak_max_days':20,
  'healthy_retrace_min':0.15, 'healthy_retrace_max':0.618,
  'dry_amount_max_ratio':0.85,
  'corporate_action_gap_proxy_abs_max':0.40,
  'lookback_history_bars':350,
}
MIN_HISTORY=284  # 224 + 60 - 1 = 283; require one extra for conservative check
HIST_START='2024-01-02'
HIST_END='2026-10-12'
MANDATORY_SOURCE_FILES=(
 'formula66_capture_manifest.json',
 'formula66_capture_event_keys.csv',
 'formula66_capture_truth_per_event_formula.csv.gz',
 'formula66_capture_formula_summary.csv',
 'formula66_blind_chart_review_queue.csv',
)


def fail(msg):raise RuntimeError('FORMULA66_R1D1_FAIL_CLOSED_'+str(msg))


def write_json(path,data):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 p.write_text(json.dumps(data,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')


def write_csv(path,rows,fields):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w',newline='',encoding='utf-8-sig') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)


def read_csv(path):
 with open(path,newline='',encoding='utf-8-sig') as f:return list(csv.DictReader(f))


def ensure_inputs(capture, registry,main7):
 """All source metadata is already pinned by R1C; assert PRE-only content locally."""
 from research_formulas.formula66_truth_krx_r1 import get_registry
 from research_formulas.formula66_krx_r1c_pinned_reprice import read_pinned_capture
 folder=Path(capture)
 for nm in MANDATORY_SOURCE_FILES:
  if not (folder/nm).is_file():fail('CAPTURE_INPUT_MISSING:'+nm)
 reg=get_registry(registry,main7,verify_pins=True)
 if len(reg['combos'])!=66 or len(reg['aux_selectors'])!=6:fail('66_6_CONTRACT_DRIFT')
 events,names,source_manifest=read_pinned_capture(folder)  # immutable 3884 PRE TRUE, no POST promotion
 if len(names)!=66 or source_manifest.get('captured_pre_true_total')!=3884:fail('PINNED_R1B_RETURN_SCHEMA_DRIFT')
 manifest=json.loads((folder/'formula66_capture_manifest.json').read_text('utf-8'))
 if manifest.get('pre_mismatch')!=0 or manifest.get('pre_captured_error_count')!=0:
  fail('PRE_CAPTURE_INVALID')
 keys=read_csv(folder/'formula66_capture_event_keys.csv')
 if not keys or len(keys)!=int(manifest['event_keys']):fail('CAPTURE_EVENT_KEYS_MISMATCH')
 if len(keys)!=1979 or int(manifest['captured_pre_true_total'])!=3884:fail('FROZEN_1979_3884_CAPTURE_DRIFT')
 if not all(re.fullmatch('[0-9][0-9A-Z]{5}',r['code']) for r in keys):fail('INVALID_EVENT_CODE')
 if len({(r['code'],r['signal_date']) for r in keys})!=len(keys):fail('DUPLICATE_EVENT_CODE_DATE')
 dates=sorted({r['signal_date'] for r in keys})
 if len(dates)!=13:fail('EXPECTED_13_REPLAY_DATES')
 if min(dates)<HIST_START or max(dates)>HIST_END:fail('DATES_OUT_OF_OFFICIAL_KRX_SCOPE')
 summary=read_csv(folder/'formula66_capture_formula_summary.csv')
 if len(summary)!=66:fail('BAD_66_SUMMARY')
 q=read_csv(folder/'formula66_blind_chart_review_queue.csv')
 if not q or not all(int(r['chart_outcomes_hidden'])==1 for r in q):fail('BLIND_CAPTURE_MISSING')
 # protect against a silent change to the R1C reader's actual count
 if isinstance(events,list) and len(events)!=3884:fail('READ_CAPTURE_TRUE_COUNT_DRIFT')
 return reg,keys,q,summary,dates,events


def ratio(numer,denom):
 if not denom or not math.isfinite(float(denom)) or float(denom)<=0:return None
 v=float(numer)/float(denom)
 return float(v) if math.isfinite(v) else None


def _number(v):return float(v) if v is not None and math.isfinite(float(v)) else ''


@dataclass(frozen=True)
class Feature:
 status:str
 close:float|None=None
 ma224:float|None=None
 below_ma224:int|None=None
 below_60_ratio:float|None=None
 recent5_vs_prior20_actual_amount:float|None=None
 base_now:int=0
 wave_found:int=0
 wave_trough_date:str=''
 wave_peak_date:str=''
 wave_gain_pct:float|None=None
 wave_retrace_pct:float|None=None
 prewave_accum_amount_ratio:float|None=None
 pullback_dry_amount_ratio:float|None=None
 prewave_base:int=0
 strict_proxy:int=0
 candidate_stage:str='NO_PROXY_PATTERN'
 model_kind:str='KRX_INDEPENDENT_224_WAVE1_PB_HYPOTHESIS_NOT_MAIN7_FORMULA'


def asof_features(bars,day_index,session_date):
 """ONLY timestamp <= signal date. Rows (session_idx,date,o,h,l,c,v,real Amount)."""
 if not bars:return Feature('KRX_NO_HISTORY')
 seq=list(bars)
 last=seq[-1]
 if last[0]!=day_index or last[1]!=session_date:fail('LOOKAHEAD_OR_NONMATCHING_ASOF')
 if len(seq)<MIN_HISTORY:return Feature('INSUFFICIENT_284_SESSION_HISTORY')
 tail=seq[-MIN_HISTORY:]
 if [x[0] for x in tail]!=list(range(day_index-MIN_HISTORY+1,day_index+1)):
  return Feature('NOT_CONTIGUOUS_284_KRX_SESSION_HISTORY')
 if last[2]<=0 or last[3]<=0 or last[4]<=0 or last[5]<=0 or last[6]<=0 or last[7]<=0:
  return Feature('SIGNAL_DAY_NOT_TRADEABLE_PRICE_PROXY')
 arr=np.asarray([x[2:8] for x in seq],dtype=np.float64)
 if not np.isfinite(arr).all():fail('NONFINITE_KRX_BAR')
 close=arr[:,3];high=arr[:,1];low=arr[:,2];amount=arr[:,5]
 n=len(arr)
 # Strict price-history uncertainty: unusual raw >40% open jump indicates
 # likely split / corporate action; never declare a true tradeable strategy.
 past_open=arr[-MIN_HISTORY+1:,0]
 past_prev_close=arr[-MIN_HISTORY:-1,3]
 valid_gap=past_prev_close>0
 if not valid_gap.all():return Feature('RAW_HISTORY_ZERO_CLOSE_CORPORATE_ACTION_UNCERTAIN')
 if (np.abs(past_open/past_prev_close-1.0)>PROXY['corporate_action_gap_proxy_abs_max']).any():
  return Feature('UNADJUSTED_PRICE_GAP_GT_40PCT_CONTEXT_BLOCKED')
 cum=np.concatenate(([0.],np.cumsum(close)))
 ma=np.full(n,np.nan)
 width=PROXY['ma_period']
 ma[width-1:]=(cum[width:]-cum[:-width])/width
 this_ma=float(ma[-1]);below=int(close[-1]<this_ma)
 below60=float(np.mean(close[-60:]<ma[-60:]))
 base_now=int(below and below60>=PROXY['below224_min_ratio'])
 cur_amount_ratio=ratio(median(amount[-5:]),median(amount[-25:-5]))
 status='KRX_PRICE_CONTEXT_READY_NO_PIT_TRADEABILITY_CERT'
 best=None
 # Fixed first-match ordering; NOT an optimization of past outcomes.
 for peak_ago in range(PROXY['wave_peak_days_ago_min'],PROXY['wave_peak_days_ago_max']+1):
  peak_i=n-1-peak_ago
  if high[peak_i]<=0:continue
  for trough_gap in range(PROXY['trough_before_peak_min_days'],PROXY['trough_before_peak_max_days']+1):
   trough_i=peak_i-trough_gap
   if trough_i<284:continue  # full historical MA+60 context at trough
   wave_high=float(high[peak_i]);wave_low=float(low[trough_i])
   if wave_low<=0:continue
   if high[peak_i]+1e-6<max(high[trough_i:peak_i+1]):continue
   if low[trough_i]-1e-6>min(low[trough_i:peak_i+1]):continue
   gain=ratio(wave_high-wave_low,wave_low)
   if gain is None or gain<PROXY['wave_min_gain']:continue
   retrace=ratio(wave_high-float(close[-1]),wave_high-wave_low)
   if retrace is None or not PROXY['healthy_retrace_min']<=retrace<=PROXY['healthy_retrace_max']:continue
   wave_amt=amount[trough_i:peak_i+1]
   a_pre=ratio(median(amount[trough_i-5:trough_i]),median(amount[trough_i-25:trough_i-5]))
   a_dry=ratio(median(amount[-3:]),median(wave_amt))
   pre_base=int(np.isfinite(ma[trough_i]) and close[trough_i]<ma[trough_i] and
            float(np.mean(close[trough_i-59:trough_i+1]<ma[trough_i-59:trough_i+1]))>=PROXY['below224_min_ratio'])
   strict=int(pre_base and a_pre is not None and PROXY['accum_min_ratio']<=a_pre<=PROXY['accum_max_ratio'] and
              a_dry is not None and a_dry<=PROXY['dry_amount_max_ratio'])
   best=(trough_i,peak_i,gain,retrace,a_pre,a_dry,pre_base,strict)
   break
  if best is not None:break
 if best:
  tr,pk,gain,retrace,a_pre,a_dry,pre_base,strict=best
  return Feature(status,float(close[-1]),this_ma,below,below60,cur_amount_ratio,base_now,1,
                 seq[tr][1],seq[pk][1],gain*100,retrace*100,a_pre,a_dry,pre_base,strict,
                 'BASE_ACCUM_WAVE1_PULLBACK_DRY' if strict else 'WAVE1_PULLBACK_CONTEXT_ONLY')
 stage='BASE_AND_CURRENT_AMOUNT_UP' if base_now and cur_amount_ratio is not None and PROXY['accum_min_ratio']<=cur_amount_ratio<=PROXY['accum_max_ratio'] else 'BASE_ONLY' if base_now else 'NO_PROXY_PATTERN'
 return Feature(status,float(close[-1]),this_ma,below,below60,cur_amount_ratio,base_now,candidate_stage=stage)


def feature_dict(f):
 d={k:getattr(f,k) for k in Feature.__dataclass_fields__}
 for k,v in list(d.items()):
  if v is None:d[k]=''
 return d


def row_fields(rows):
 return list(dict.fromkeys(k for d in rows for k in d)) if rows else ['status']


def _read_day_krx(path, expected_date):
 df=pd.read_csv(path,compression='gzip',dtype={'code':str,'date':str},usecols=['code','date','open','high','low','close','volume','amount'])
 if df.empty or df['date'].nunique()!=1 or df['date'].iloc[0]!=expected_date:fail('KRX_DATE_MISMATCH')
 if df['code'].duplicated().any():fail('KRX_DUPLICATE_CODE_ON_DAY')
 return df


def run_census(root,reference,capture,out_dir,registry,main7,skip_full_verify=False):
 from research_pit.krx_full_market_pit import verify
 from research_formulas.formula66_truth_krx_r1 import get_registry
 from research_formulas.formula66_krx_r1c_pinned_reprice import check_source_pins
 root=Path(root);out=Path(out_dir);out.mkdir(parents=True,exist_ok=True)
 check_source_pins(Path(main7).parent) # pinned original scripts, registry and reference
 reg,keys,queue,summary,dates,capture_true=ensure_inputs(capture,registry,main7)
 if not skip_full_verify:
  official=verify(root,reference,out/'whole_market_price_data_verification.json')
  if official.get('status')!='PASS_UNFILTERED_PRICE_COLLECTION_ONLY' or official['sessions']!=1907:fail('KRX_ALL_MARKET_UNVERIFIED')
 else:
  # only invoked from synthetic unit-test fixture, never via CLI
  official={'sessions':0,'status':'TEST_FIXTURE_NOT_SOURCE_VERIFIED'}
 day_to_entries=defaultdict(list)
 for r in keys:day_to_entries[r['signal_date']].append(r)
 blind_by_day=defaultdict(list)
 for r in queue:blind_by_day[r['signal_date']].append(r)
 by_day_events=defaultdict(list)
 for r in capture_true:by_day_events[r['signal_date']].append(r)
 original_eval_keys={(r['code'],r['signal_date']) for r in keys}
 original_true_keys={(r['code'],r['signal_date']) for r in capture_true}
 files={}
 for p in root.glob('shard_*/manifest_shard_*.json'):
  meta=json.loads(p.read_text('utf-8'))
  for row in meta['files']:
   if row['date'] in files:fail('DUPLICATE_KRX_DAY_IN_MANIFESTS')
   files[row['date']]=(p.parent/row['file'],row['sha256'])
 # From 2024-01 through last captured as-of. No future price or outcome read.
 timeline=sorted(d for d in files if HIST_START<=d<=max(dates))
 if not timeline or timeline[-1]!=max(dates):fail('MISSING_ASOF_DATE_IN_KRX_CALENDAR')
 if set(dates)-set(timeline):fail('MISSING_CAPTURE_ASOF_DATES')
 hist=defaultdict(lambda:deque(maxlen=PROXY['lookback_history_bars']))
 candidate_rows=[];hit_rows=[];fullmarket_rows=[];review_key=[];review_count=0;reviews_with_raw_bars=0
 blind_filename=out/'formula66_krx_blind_bars_to_signal_day_only.csv.gz'
 with gzip.open(blind_filename,'wt',newline='',encoding='utf-8-sig') as gz:
  barwriter=csv.DictWriter(gz,fieldnames=['review_id','code','signal_date','date','open','high','low','close','volume','actual_amount_KRW','known_at_signal'])
  barwriter.writeheader()
  for t,day in enumerate(timeline):
   p,expected_sha=files[day]
   if not p.is_file():fail('KRX_DAY_FILE_MISSING:'+day)
   # verify() has independently checked SHA+source all 1907 sessions; only active if verified.
   df=_read_day_krx(p,day)
   for rr in df.itertuples(index=False,name=None):
    code,d,o,h,l,c,v,a=rr
    hist[code].append((t,day,float(o),float(h),float(l),float(c),float(v),float(a)))
   if day not in day_to_entries:continue
   day_feat={}; eligible=0;strict_count=0;partial_count=0;blocked=Counter()
   for rr in df.itertuples(index=False,name=None):
    code=rr[0]; ft=asof_features(hist[code],t,day);day_feat[code]=ft
    if not ft.status.startswith('KRX_PRICE_CONTEXT_READY'):blocked[ft.status]+=1
    else:eligible+=1
    strict_count+=int(ft.strict_proxy)
    partial_count+=int(ft.base_now)
    flag=('LEGACY_CAPTURE_PRE_TRUE' if (code,day) in original_true_keys else
          'LEGACY_COMBO_EVALUATED_NO_PRE_TRUE' if (code,day) in original_eval_keys else
          'NOT_EVALUATED_IN_LEGACY_CAPTURE')
    # Census never selects universe by legacy signals, so tally all original KRX tickers.
    fullmarket_rows.append({'date':day,'code':code,'legacy_capture_relationship':flag,**feature_dict(ft)})
    if ft.strict_proxy or (ft.base_now and ft.recent5_vs_prior20_actual_amount is not None):
     candidate_rows.append({'date':day,'code':code,'legacy_capture_relationship':flag,**feature_dict(ft)})
   # Source has no daily KRX row for some historical PRE codes -> UNKNOWN, never false.
   for e in by_day_events[day]:
    f=day_feat.get(e['code'],Feature('NO_ASOF_KRX_ROW_FOR_CAPTURE_CODE'))
    hit_rows.append({**{k:e[k] for k in ['code','signal_date','formula_index','formula']},'legacy_pre_true':'T',**feature_dict(f)})
   for q in blind_by_day[day]:
    review_count+=1
    code=q['code'];sid=q['signal_date']; idx=int(q['formula_index'])
    rid=hashlib.sha256(f'{idx}|{code}|{sid}|{q["review_role"]}'.encode()).hexdigest()[:20]
    history=hist.get(code,deque())
    has_history=bool(history and history[-1][1]==day)
    reviews_with_raw_bars+=int(has_history)
    review_key.append({'review_id':rid,'formula_index':idx,'formula':q['formula'],
                       'code':code,'signal_date':sid,'role_from_legacy_capture':q['review_role'],
                       'chart_future_hidden':1,'signal_formula_hidden_from_blind_bar_file':1,
                       'KRX_asof_chart_bars_available':int(has_history),
                       'review_label':'NOT_REVIEWED',
                       'review_scope':'LEGACY_CAPTURE_ONLY_NOT_RANDOM_FULL_MARKET'})
    if has_history:
     for a in list(history)[-260:]:
      barwriter.writerow({'review_id':rid,'code':code,'signal_date':sid,'date':a[1],
                          'open':a[2],'high':a[3],'low':a[4],'close':a[5],
                          'volume':a[6],'actual_amount_KRW':a[7],'known_at_signal':1})
   write_json(out/f'local_date_proof_{day}.json',{
       'status':'CENSUS_OF_ALL_ORIGINAL_KRX_DAY_TICKERS_NO_CODE_FILTER','day':day,'raw_rows':len(df),
       'eligible_284_history':eligible,'base_now':partial_count,'strict_proxy':strict_count,
       'blocked':dict(blocked),'original_capture_pre_true_unique_date':sum((c,day) in original_true_keys for c in day_feat)})
   print('R1D1_KRX_DATE',day,'raw',len(df),'elig',eligible,'base',partial_count,'strict_proxy',strict_count,flush=True)
 # Files and proof summary
 if len(hit_rows)!=3884:fail('3884_LEGACY_TRUE_CONTEXT_NOT_COMPLETE')
 if len(review_key)!=len(queue):fail('BLIND_REVIEW_QUEUE_COVERAGE_DRIFT')
 if len({x['review_id'] for x in review_key})!=len(review_key):fail('BLIND_ID_COLLISION')
 if len({r['date'] for r in fullmarket_rows})!=13:fail('NOT_13_DATES')
 write_csv(out/'formula66_all_market_native_proxy_asof_census.csv',fullmarket_rows,row_fields(fullmarket_rows))
 write_csv(out/'formula66_krx_native_proxy_candidates.csv',candidate_rows,row_fields(candidate_rows))
 write_csv(out/'formula66_original_PRE_hits_asof_KRX_context.csv',hit_rows,row_fields(hit_rows))
 write_csv(out/'formula66_blind_review_sealed_keys.csv',review_key,row_fields(review_key))
 formula_hit=defaultdict(list)
 for r in hit_rows:formula_hit[int(r['formula_index'])].append(r)
 audit66=[]
 for f in reg['combos']:
  i=int(f['index']);rows=formula_hit[i]
  ready=[r for r in rows if r['status'].startswith('KRX_PRICE_CONTEXT_READY')]
  audit66.append({'formula_index':i,'formula':f['combination'],
    'producer_signal_keys':'|'.join(f.get('referenced_keys',[])),
    'legacy_PRE_true_hits':len(rows),'krx_asof_context_ready':len(ready),
    'ready_asof_below224_hits':sum(int(r['below_ma224']) for r in ready),
    'ready_asof_base_60of224_hits':sum(int(r['base_now']) for r in ready),
    'ready_asof_independent_proxy_pattern_hits':sum(int(r['strict_proxy']) for r in ready),
    'producer_parity_to_main7':'NOT_VALIDATED_52_DERIVED_INPUTS_REQUIRE_SEPARATE_PRODUCER_PROOFS',
    'name_vs_chart':'BLIND_REVIEW_PENDING',
    'performance':'NOT_A_LIVE_STRATEGY_OR_ACCURACY_ESTIMATE'})
 write_csv(out/'formula66_all66_asof_context_audit.csv',audit66,row_fields(audit66))
 by_day=defaultdict(list)
 for r in fullmarket_rows:by_day[r['date']].append(r)
 coverage=[]
 for day,rs in sorted(by_day.items()):
  known=[r for r in rs if r['legacy_capture_relationship']=='LEGACY_CAPTURE_PRE_TRUE']
  unknown=[r for r in rs if r['legacy_capture_relationship']=='NOT_EVALUATED_IN_LEGACY_CAPTURE']
  complete=[r for r in rs if r['status'].startswith('KRX_PRICE_CONTEXT_READY')]
  proxy=[r for r in rs if r['strict_proxy']]
  coverage.append({'date':day,'KRX_all_original_rows':len(rs),'KRX_asof_284_history_eligible':len(complete),
       'KRX_no_284_history_or_price_blocked':len(rs)-len(complete),
       'original_PRE_positive_unique':len(known),
       'KRX_native_strict_PROXY_candidates':len(proxy),
       'proxy_in_original_PRE_positive':sum(r['legacy_capture_relationship']=='LEGACY_CAPTURE_PRE_TRUE' for r in proxy),
       'proxy_in_combo_evaluated_PRE_negative':sum(r['legacy_capture_relationship']=='LEGACY_COMBO_EVALUATED_NO_PRE_TRUE' for r in proxy),
       'proxy_not_evaluated_in_legacy':sum(r['legacy_capture_relationship']=='NOT_EVALUATED_IN_LEGACY_CAPTURE' for r in proxy),
       'captured_universe_incomplete':'TRUE_PREVENT_FALSE_NEGATIVE_CONCLUSION'})
 write_csv(out/'formula66_KRX_census_13days_coverage.csv',coverage,row_fields(coverage))
 sigs=sorted({x for f in reg['combos'] for x in f.get('referenced_keys',[])})
 static=[]
 source_text=Path(main7).read_text('utf-8',errors='replace')
 src_lines=source_text.splitlines()
 for k in sigs:
  re_key=re.compile(r'[\'\"]'+re.escape(k)+r'[\'\"]')
  hits=[str(i+1) for i,line in enumerate(src_lines) if re_key.search(line)]
  static.append({'producer_signal_key':k,'referenced_by_n_formulas':sum(k in c['referenced_keys'] for c in reg['combos']),
                 'main7_source_text_line_refs_sample':'|'.join(hits[:15]),
                 'potential_source_literal_occurrences':len(hits),
                 'asof_value_producer_verified':'NO',
                 'KRX_only_identical_legacy_formula_parity':'NOT_PROVEN',
                 'reason':'Static location is not proof the signal is computed from price alone at the correct time.'})
 write_csv(out/'formula66_52_producer_dependency_blockers.csv',static,row_fields(static))
 manifest={
   'status':'R1D1_COMPLETED_KRX_NATIVE_224_PATTERN_CONTEXT_ONLY_66_FORMULA_RESCREEN_NOT_DONE',
   'source':'KRX all-market OHLCV and ACTUAL Amount via verified source 39295295197',
   'capture':'R1-B 39738384453 / PRE-only 3884 formula hits / 1979 captured evaluation rows',
   'source_KRX_sessions_verified':official.get('sessions'),
   'source_verified':not skip_full_verify,
   'original_KRX_ticker_date_rows_on_13_dates':len(fullmarket_rows),
   'legacy_PRE_formula_hits_covered':len(hit_rows),
   'legacy_blind_chart_cases_total':len(review_key),
   'legacy_blind_chart_cases_with_signal_day_or_earlier_data':reviews_with_raw_bars,
   'legacy_blind_chart_cases_missing_KRX_asof_row':len(review_key)-reviews_with_raw_bars,
   'KRX_native_proxy_strict_events':sum(r['strict_proxy'] for r in fullmarket_rows),
   'proxy_is_not_exact_main7_66':'TRUE',
   'actual_allmarket_formula_66_native_recalculation':'NOT_PERFORMED_NO_PRODUCER_PARITY',
   'legacy_capture_universe_not_fullmarket':'TRUE',
   'legacy_non_capture_not_automatically_missing_result':'TRUE',
   'no_future_prices_in_context_or_blind_casebook':'TRUE',
   'no_forward_outcomes_or_strategy_ranking':'TRUE',
   'PIT_tradeability_corporate_actions_not_certified':'TRUE',
   'LIVE_CHANGE':False,'ORDER_COUNT':0,'strategy_ready':False,
   'next_step':'Producer-by-producer historical KRX parity for 52 derived keys, full 66 formula re-screen, blinded chart review, then OOS.',
   'frozen_independent_proxy_definitions':PROXY,
 }
 write_json(out/'formula66_r1d1_manifest.json',manifest)
 print('FORMULA66_R1D1_DONE',json.dumps({k:manifest[k] for k in ('source_verified','original_KRX_ticker_date_rows_on_13_dates','legacy_PRE_formula_hits_covered','KRX_native_proxy_strict_events','status')}),flush=True)
 return manifest


def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--mode',choices=['contract','run'],default='contract')
 p.add_argument('--repo-root',default='.')
 p.add_argument('--krx-root',default='')
 p.add_argument('--reference',default='research_pit/historical_market_counts_20190102_20261012.csv')
 p.add_argument('--capture',default='')
 p.add_argument('--registry',default='search_formula_contract_registry.json')
 p.add_argument('--main7',default='main7_bugfix_2.py')
 p.add_argument('--out',default='reports/formula66_r1d1')
 a=p.parse_args(argv)
 from research_formulas.formula66_krx_r1c_pinned_reprice import check_source_pins
 check_source_pins(a.repo_root)
 out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
 if a.mode=='contract':
  from research_formulas.formula66_truth_krx_r1 import get_registry
  reg=get_registry(a.registry,a.main7,verify_pins=True)
  write_json(out/'r1d1_contract.json',{'status':'STATIC_PINS_AND_66_COMBO_SOURCE_OK_NOT_NATIVE_RESCREEN',
             'source_main7_sha':reg['_audit']['main7_git_blob_sha'],
             'source_registry_sha':reg['_audit']['registry_git_blob_sha'],
             'formulas':len(reg['combos']),'aux':len(reg['aux_selectors']),
             'unique_legacy_producer_keys':len({x for c in reg['combos'] for x in c['referenced_keys']}),
             'strategy_ready':False,'LIVE_CHANGE':False})
  print('FORMULA66_R1D1_CONTRACT_PASS',flush=True)
 else:
  if not a.capture or not a.krx_root:fail('MISSING_CAPTURE_OR_KRX_ROOT')
  run_census(a.krx_root,a.reference,a.capture,a.out,a.registry,a.main7)
 return 0


if __name__=='__main__':raise SystemExit(main())
