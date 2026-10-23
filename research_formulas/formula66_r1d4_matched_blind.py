#!/usr/bin/env python3
"""StockHunter R1-D4 blinded matched-date 224-wave candidate review (RESEARCH ONLY).

RETROSPECTIVE, outcome-stratified case-control chart review. This is not a
prospective validation, nor a recalculation of the 66 proprietary formulas.

Stage 1 (--mode select): verified R1-D3 asof snapshots + D20 outcomes; choose
exact-signal-day pairs without using future values in feature distance; seal
outcome labels, codes and dates in restricted research outputs.
Stage 2 (--mode render): ONLY the stage-1 as-of-only chart identifiers and
original KRX daily OHLCV/Amount; no access to forward outcomes/return labels.
Output blinded prices truncated at signal date, 224MA and ACTUAL Amount.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import json
import math
from collections import defaultdict, Counter
from pathlib import Path

import numpy as np
import pandas as pd

VERSION='STOCKHUNTER_R1D4_OUTCOME_STRATIFIED_MATCHED_DATE_BLIND_REVIEW_V1'
RESEARCH_ONLY=True
EXPECTED_R1D3={'n_episode':1249,'n_complete':939,'n_not_complete':310,'strong_n':91,'failure_n':286}
STOCK_FEATURES=['wave_gain_pct','wave_retrace_pct','prewave_accum_amount_ratio',
                'pullback_dry_amount_ratio','recent5_vs_prior20_actual_amount',
                'stock_ret20_pct','stock_ret60_pct','krx_avg20_actual_amount_100mKRW',
                'krx_recent20_return_sd_pct','krx_actual_amount_5vs20_ratio',
                'pct_above224_among_unfiltered_eligible','above224_breadth_change_5day_pp']
MATCH_BASELINE=['stock_ret20_pct','stock_ret60_pct','krx_avg20_actual_amount_100mKRW',
                'krx_recent20_return_sd_pct']
DIAGNOSTIC_LIMIT=3.0   # report only; NEVER drop cases based on post-hoc tuning
LOOKBACK_BARS=260
CANDLE_WARMUP_BARS=223
BLIND_KEY_SEED='FROZEN_R1D4_20261023_v1_NO_OUTCOMES_IN_CHARTS'


def fail(s):raise RuntimeError('R1D4_FAIL_CLOSED_'+str(s))

def read_csv(path):
 p=Path(path)
 if not p.is_file():fail('SOURCE_MISSING:'+str(path))
 return pd.read_csv(p,dtype={'code':str,'date':str,'signal_date':str},low_memory=False,encoding='utf-8-sig')

def write_csv(path,df):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 df.to_csv(p,index=False,encoding='utf-8-sig',float_format='%.8f')

def write_json(path,d):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 p.write_text(json.dumps(d,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')

def sha256_bytes(b):return hashlib.sha256(b).hexdigest()

def code_key(pair_id,code,date):
 token=sha256_bytes(f'{BLIND_KEY_SEED}|{pair_id}|{code}|{date}'.encode())
 return 'C'+token[:15]

def asof_check(path):
 """Only asof features in blinded dataset. Drop all unknown columns if mismatch."""
 allowed={'date','code','episode_first_in_20session_cooldown','index_session_i',
          'source_scope','scan_cadence','joint_index_20d_regime','KOSPI_ret20_pct',
          'KOSDAQ_ret20_pct','status','close','ma224','below_ma224',
          'below_60_ratio','recent5_vs_prior20_actual_amount','base_now',
          'wave_found','wave_trough_date','wave_peak_date','wave_gain_pct',
          'wave_retrace_pct','prewave_accum_amount_ratio','pullback_dry_amount_ratio',
          'prewave_base','strict_proxy','candidate_stage','model_kind','KOSPI_ret60_pct',
          'KOSDAQ_ret60_pct','ready_n','pct_above224_among_unfiltered_eligible',
          'pct_base_now_among_unfiltered_eligible','pct_strict_proxy_among_unfiltered_eligible',
          'above224_breadth_change_5day_pp','above224_breadth_change_10day_pp',
          'all_source_market_amount_today_vs_prev20_ratio','unfiltered_market_actual_amount_sum',
          'stock_ret20_pct','stock_ret60_pct','krx_avg20_actual_amount_100mKRW',
          'krx_median20_actual_amount_KRW','krx_amount_today_100mKRW',
          'krx_actual_amount_5vs20_ratio','krx_recent20_return_sd_pct',
          'krx_20day_above_close_momentum_pct'}
 d=read_csv(path)
 unexpected=set(d.columns)-allowed
 if unexpected:fail('ASOF_LEAK_OR_SCHEMA_DRIFT:'+','.join(sorted(unexpected)))
 if not set(MATCH_BASELINE+['date','code']).issubset(d.columns):fail('ASOF_FIELDS_MISSING')
 return d

def select_pairs(r1d3,out):
 root=Path(r1d3);out=Path(out)
 man=json.loads((root/'r1d3_manifest.json').read_text('utf-8'))
 if man.get('source_run')!=39873801594 or man.get('observed_event_counts') is None:
  fail('R1D3_SOURCE_PIN_OR_COUNTS_MISSING')
 for k,v in EXPECTED_R1D3.items():
  if man['observed_event_counts'].get(k)!=v:fail('R1D3_COUNT_DRIFT:'+k)
 if man.get('strategy_ready') is not False or man.get('orders_placed')!=0:
  fail('R1D3_NOT_RESEARCH_ONLY')
 a=asof_check(root/'asof_only/frozen_episode_asof_features_NO_FUTURE.csv')
 o=read_csv(root/'after_outcome_unblinded/event_group_outcome_and_prior_features.csv')
 if len(a)!=1249 or len(o)!=1249:fail('R1D3_EVENT_ROW_COUNT_DRIFT')
 keys=['date','code']
 if a.duplicated(keys).any() or o.duplicated(keys).any():fail('DUPLICATE_SOURCE_EVENT')
 joined=a.merge(o[keys+['group','status_out','net_20bps_pct','exploratory_reference_excess_pct']],on=keys,validate='one_to_one',indicator=True)
 if len(joined)!=1249 or not (joined['_merge']=='both').all():fail('SOURCE_ASOF_OUTCOME_KEY_MISMATCH')
 if (joined['group']=='NO_D20_COMPLETE').sum()!=310 or (joined['group']=='FAILURE_LE_MINUS5').sum()!=286:
  fail('OUTCOME_LABEL_DRIFT')
 good=joined[joined.group.isin(['STRONG_15_TO_30','EXPLOSIVE_GE_30'])].copy()
 bad=joined[joined.group=='FAILURE_LE_MINUS5'].copy()
 if len(good)!=91 or len(bad)!=286:fail('SUCCESS_FAILURE_LABEL_DRIFT')
 if not (good.net_20bps_pct>=15-1e-8).all() or not (bad.net_20bps_pct<=-5+1e-8).all():
  fail('OUTCOME_BOUNDARY_DRIFT')
 if not ((good.status_out=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED').all() and
         (bad.status_out=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED').all()):fail('FORWARD_STATUS_DRIFT')

 # Outcome labels choose retrospective cases; only these 4 at-signal features
 # enter the score. Crucially do not match on wave_gain/retrace/amount dryness.
 # All 31 overlap dates available; choose one pair per day and distinct ticker
 # identities globally where possible, to avoid repeated examples.
 def baseline_distance(x,y):
  vals=[]
  for k,div in [('stock_ret20_pct',15),('stock_ret60_pct',30),('krx_recent20_return_sd_pct',5)]:
   u,v=x.get(k),y.get(k)
   if not (pd.notna(u) and pd.notna(v) and np.isfinite(float(u)) and np.isfinite(float(v))):return None
   vals.append(((float(u)-float(v))/div)**2)
  xx,yy=float(x['krx_avg20_actual_amount_100mKRW']),float(y['krx_avg20_actual_amount_100mKRW'])
  if not all(np.isfinite(i) and i>=0 for i in (xx,yy)):return None
  vals.append((math.log1p(xx)-math.log1p(yy))**2)
  return sum(vals)

 paired=[];seen_codes=set(); no_pair_dates=[]
 shared_dates=sorted(set(good.date)&set(bad.date))
 if len(shared_dates)!=31:fail('OVERLAP_DATE_DRIFT_EXPECTED_31')
 for day in shared_dates:
  candidates=[]
  for _,x in good.loc[good.date==day].iterrows():
   for _,y in bad.loc[bad.date==day].iterrows():
    if x.code==y.code:continue
    dist=baseline_distance(x,y)
    if dist is None:continue
    candidates.append((dist,str(x.code),str(y.code),x,y))
  candidates.sort(key=lambda r:(r[0],r[1],r[2]))
  chosen=next((p for p in candidates if p[1] not in seen_codes and p[2] not in seen_codes),None)
  if chosen is None:
   no_pair_dates.append(day);continue
  dist,sc,fc,x,y=chosen
  seen_codes.update((sc,fc))
  paired.append((day,dist,x,y))
 if len(paired)<25:fail('TOO_FEW_UNIQUE_DAILY_MATCHES')

 # Human reviewer does not see date, security code, group, return, or future bar.
 # Assignment to A/B is deterministic, independently opaque to future result.
 public=[];private=[];pair_summary=[];paldiffs=[]
 for i,(day,dist,x,y) in enumerate(paired,1):
  pairid=f'PAIR_{i:03d}'
  cases=[('STRONG_GE15',x),('FAILURE_LE_MINUS5',y)]
  # Blind sides are permuted with hash (not fixed success always A).
  flip=int(sha256_bytes((BLIND_KEY_SEED+'|'+day).encode()),16)%2
  if flip:cases.reverse()
  pairout={'pair_id':pairid,'signal_date_sealed':day,'distance_baseline':dist,
           'poor_baseline_balance_flag':int(dist>DIAGNOSTIC_LIMIT)}
  frow={'pair_id':pairid,'matched_day_sealed':day,'distance_baseline':dist}
  for n,(grp,row) in enumerate(cases):
   side='AB'[n];token=code_key(pairid,row.code,day)
   public.append({'pair_id':pairid,'side':side,'blind_card_id':token,
     'chart_name':f'{pairid}_{side}_{token}.png','review_status':'NOT_REVIEWED'})
   private.append({'pair_id':pairid,'side':side,'blind_card_id':token,'code':row.code,
     'signal_date':day,'outcome_label':grp,'D20_net20bps_pct':float(row.net_20bps_pct),
     'D20_exploratory_reference_excess_pct':float(row.exploratory_reference_excess_pct)
      if pd.notna(row.exploratory_reference_excess_pct) else None,
     'distance_baseline':dist})
   pairout[side+'_outcome_label']=grp
   pairout[side+'_code']=str(row.code)
   pairout[side+'_D20_return_pct']=float(row.net_20bps_pct)
  win=x;lose=y
  for key in STOCK_FEATURES:
   v=float(win[key]) if pd.notna(win[key]) else np.nan
   w=float(lose[key]) if pd.notna(lose[key]) else np.nan
   frow[key+'_winner_minus_failure']=v-w
  paldiffs.append(frow);pair_summary.append(pairout)

 out.mkdir(parents=True,exist_ok=True)
 write_csv(out/'blind_review/BLIND_PAIRS_REVIEWER_INDEX.csv',pd.DataFrame(public))
 rubric=pd.DataFrame([{'blind_card_id':r['blind_card_id'],'pair_id':r['pair_id'],'side':r['side'],
  'wave_appearance_clean_or_spike':'','prewave_amount_gradual_or_one_day':'',
  'pullback_higher_low_or_breakdown':'','pullback_amount_dry_or_persistent':'',
  '224MA_relation_or_recovery':'','suspicious_raw_gap_or_action':'',
  'overall_shape_quality_0to3':'','reviewer_comment':''} for r in public])
 write_csv(out/'blind_review/EMPTY_HUMAN_BLIND_REVIEW_FORM.csv',rubric)
 write_csv(out/'internal_only/ASOF_CHART_IDENTIFIERS_DO_NOT_UPLOAD.csv',
           pd.DataFrame([{'pair_id':p['pair_id'],'side':p['side'],
                          'blind_card_id':p['blind_card_id'],'code':p['code'],
                          'signal_date':p['signal_date']} for p in private]))
 write_csv(out/'sealed_after_reveal/REVEAL_OUTCOME_PAIRS.csv',pd.DataFrame(private))
 write_csv(out/'sealed_after_reveal/PAIR_BALANCE_AND_OUTCOME_CONTROL.csv',pd.DataFrame(pair_summary))
 write_csv(out/'sealed_after_reveal/WINNER_MINUS_FAILURE_ASOF_PAIRED_FEATURES.csv',pd.DataFrame(paldiffs))
 stats=[]
 for key in STOCK_FEATURES:
  vals=np.asarray([r[key+'_winner_minus_failure'] for r in paldiffs],float)
  valid=vals[np.isfinite(vals)]
  stats.append({'asof_feature':key,'paired_days_available':len(valid),
    'median_winner_minus_failure':float(np.median(valid)) if len(valid) else None,
    'pct_dates_winner_higher':float(100*(valid>0).mean()) if len(valid) else None,
    'n_equal':int((valid==0).sum()),'interpretation':'RETROSPECTIVE_CASE_CONTROL_NOT_PREDICTIVE'})
 write_csv(out/'sealed_after_reveal/PAIRED_FEATURE_SUMMARY.csv',pd.DataFrame(stats))
 manifest={'version':VERSION,'source_r1d3_run':39884755058,'source_r1d3_artifact':12471499751,
  'n_all_original_episodes':1249,'n_D20_complete':939,
  'n_strong_original':91,'n_failed_original':286,
  'n_shared_strong_failure_signal_days':len(shared_dates),
  'n_match_date_pairs':len(paired),'n_blind_cards':len(public),
  'n_distinct_ticker_codes':len(seen_codes),
  'pairing_group_labels_used_for_retro_case_selection':True,
  'pairing_distance_asof_only':MATCH_BASELINE,
  'pairs_poor_balance_distance_gt3':sum(int(p[1]>DIAGNOSTIC_LIMIT) for p in paired),
  'no_pair_dates_due_to_global_ticker_independence':no_pair_dates,
  'no_future_in_blind_chart_render_source':True,
  'blind_case_selection_is_outcome_stratified_NOT_an_unbiased_predictive_sample':True,
  'external_issuer_sector_supply_fx_disclosures_joined':False,
  'market_index_and_market_breadth_asof_only':True,
  'original_66_native_formulas_rerun':False,'strategy_ready':False,'orders_placed':0,
  'caution':'Retrospective cases; groups chosen using D20 outcome; single 4-month period, overlapping market dates and censoring. No alpha / live / independent OOS claims.'}
 write_json(out/'sealed_after_reveal/R1D4_MANIFEST.json',manifest)
 (out/'blind_review/START_HERE_BLIND_REVIEW_KO.md').write_text('''# R1-D4 블라인드 차트 검토 가이드

이 폴더에는 **과거 신호 당일까지의 가격·224일 이동평균선·실제 KRX 거래대금**만 있습니다. 실제 종목명, 신호 날짜, 이후 수익률은 표시하지 않습니다.

같은 `PAIR_###`에서 `A`와 `B` 차트를 비교하며 먼저 어떤 유형인지 평가하세요. 하지만 A가 성공, B가 실패인 식으로 고정돼 있지 않습니다.

`EMPTY_HUMAN_BLIND_REVIEW_FORM.csv`의 `overall_shape_quality_0to3`는 0(불명확/불량)~3(명확한 건강한 눌림)으로 적습니다. `wave_appearance_clean_or_spike`, `prewave_amount_gradual_or_one_day`, `pullback_higher_low_or_breakdown`, `pullback_amount_dry_or_persistent`, `224MA_relation_or_recovery`, `suspicious_raw_gap_or_action`도 **미래 결과를 보지 않고** 기록하세요.

**주의:** 연구자는 이후 D+20 수익률을 기준으로 사례 자체를 고른 상태입니다. 차트에서 정답을 가려 놓은 눈가림 연구일 뿐, 미래를 모르는 무작위 미래 검증은 아닙니다. 차트 검토 후에만 별도 SEALED 아티팩트의 정답표를 열어보세요.
''',encoding='utf-8')
 (out/'sealed_after_reveal/RESULTS_INTERPRETATION_KO.md').write_text(
  '# R1-D4 동일 검색일 성공·실패 비교 (연구용)\n\n'
  f"같은 날 성공군(+15% 이상)과 실패군(-5% 이하)이 함께 있었던 {len(shared_dates)}개 검색일에서 종목 중복 없는 {len(paired)}쌍({len(public)}개 차트)을 선택했습니다.\n\n"
  f"기초 지표 매칭 거리 > {DIAGNOSTIC_LIMIT:g}로 균형이 불충분한 쌍은 {sum(int(p[1]>DIAGNOSTIC_LIMIT) for p in paired)}쌍입니다.\n\n"
  '성공·실패 종목을 미래 결과로 의도적으로 선별한 후행적 사례대조군 연구입니다. 독립 OOS 또는 수익 예측력을 증명하지 않습니다.\n\n'
  '개별 종목 업종·테마·외국인/기관 순매수·기업공시·FX·금리·수정주가/PIT 적격성은 아직 결합되지 않았습니다.\n\n'
  '## 검색 당시 특징별 두 종목 차이 (성공 - 실패)\n\n'
  '| 검색 당시 지표 | 31쌍 중앙 차이 | 성공 종목 값이 큰 날짜 비율 |\n|---|---:|---:|\n'
  +''.join(f"| {d['asof_feature']} | {d['median_winner_minus_failure']:+.3f} | {d['pct_dates_winner_higher']:.1f}% |\n" for d in stats if d['paired_days_available']==31)
  +'\n위 표는 미래 결과를 기준으로 두 집단을 선택한 **기술통계**로서 매매 기준으로 사용할 수 없습니다.\n',encoding='utf-8')
 print('R1D4_SELECT_PASS',json.dumps({k:manifest[k] for k in ('n_match_date_pairs','n_blind_cards','n_distinct_ticker_codes','pairs_poor_balance_distance_gt3')},ensure_ascii=False),flush=True)
 return manifest


def day_path_map(root):
 root=Path(root)
 fs={}
 for p in root.rglob('*.csv.gz'):
  stem=p.name[:8]
  if len(stem)==8 and stem.isdigit():
   date=f'{stem[:4]}-{stem[4:6]}-{stem[6:8]}'
   if date in fs:fail('DUPLICATE_KRX_DAY:'+date)
   fs[date]=p
 if len(fs)!=1907:fail('ORIGINAL_KRX_SESSION_SET_NOT_1907:'+str(len(fs)))
 if min(fs)!='2019-01-02' or max(fs)!='2026-10-12':fail('KRX_DAY_RANGE_DRIFT')
 return fs


def historical_bars(root,chart_keys):
 fs=day_path_map(root);days=sorted(fs)
 targets={(str(row.code),str(row.signal_date)) for row in chart_keys.itertuples(index=False)}
 codes={code for code,date in targets}
 first=min(date for code,date in targets)
 last=max(date for code,date in targets)
 first_idx=days.index(first)
 start_idx=max(0,first_idx-(LOOKBACK_BARS+CANDLE_WARMUP_BARS+16))
 use_days=days[start_idx:days.index(last)+1]
 hist=defaultdict(list)
 for day in use_days:
  p=fs[day]
  # KRX original original AMOUNT=KRW, not volume or derived turnover.
  with gzip.open(p,'rt',encoding='utf-8') as f:
   rdr=csv.DictReader(f)
   if not {'code','date','open','high','low','close','volume','amount'}.issubset(rdr.fieldnames or []):
    fail('KRX_OHLCA_HEADER_DRIFT:'+day)
   for row in rdr:
    code=row['code']
    if code in codes:
     if row['date']!=day:fail('INCONSISTENT_KRX_DAY:'+day)
     data={'date':day,'open':float(row['open']),'high':float(row['high']),
       'low':float(row['low']),'close':float(row['close']),
       'volume':float(row['volume']),'amount':float(row['amount'])}
     hist[code].append(data)
 return hist,days


def extract_asof_window(hist,all_days,code,signal_date):
 pos=all_days.index(signal_date)
 desired=all_days[max(0,pos-LOOKBACK_BARS+1):pos+1]
 warm=all_days[max(0,pos-LOOKBACK_BARS-CANDLE_WARMUP_BARS+1):pos+1]
 hm={r['date']:r for r in hist.get(code,[]) if r['date']<=signal_date}
 df=pd.DataFrame([{'date':day,**hm.get(day,{})} for day in warm])
 if len(df)<224:fail('INSUFFICIENT_KRX_SESSION_CALENDAR')
 for key in ('open','high','low','close','volume','amount'):
  if key not in df.columns:df[key]=np.nan
  df[key]=pd.to_numeric(df[key],errors='coerce')
 valid=(df[['open','high','low','close']]>0).all(axis=1)&(df['high']>=df[['open','close','low']].max(axis=1))&(df['low']<=df[['open','close']].min(axis=1))&(df.amount>=0)&(df.volume>=0)
 for k in ('open','high','low','close','amount'):
  df.loc[~valid,k]=np.nan
 df['MA224']=df.close.rolling(224,min_periods=224).mean()
 take=df[df.date.isin(desired)].copy().reset_index(drop=True)
 if len(take)!=len(desired):fail('ASOF_CALENDAR_MISMATCH')
 if take.date.max()!=signal_date:fail('FUTURE_BAR_CHART_LEAK')
 if not np.isfinite(take.close.iloc[-1]) or take.close.iloc[-1]<=0:fail('SIGNAL_CLOSE_INVALID:'+code)
 return take


def render_one(df,pair_id,side,card,path):
 # Completely blind plot. Signal-day OHLCV+Amount ONLY. No code/date/outcome labels.
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 from matplotlib.ticker import MaxNLocator
 x=np.arange(len(df));scale=100/df.close.iloc[-1]
 fig=plt.figure(figsize=(13.0,7.2),facecolor='white')
 grid=fig.add_gridspec(4,1,hspace=.09)
 ax=fig.add_subplot(grid[:3,0]);av=fig.add_subplot(grid[3,0],sharex=ax)
 o=df.open.to_numpy(dtype=float)*scale
 h=df.high.to_numpy(dtype=float)*scale
 l=df.low.to_numpy(dtype=float)*scale
 c=df.close.to_numpy(dtype=float)*scale
 ma=df.MA224.to_numpy(dtype=float)*scale
 v=(c>=o)&np.isfinite(c)&np.isfinite(o)
 col=np.where(v,'#c44843','#2e6b9c')
 valid=np.isfinite(o)&np.isfinite(h)&np.isfinite(l)&np.isfinite(c)
 ax.vlines(x[valid],l[valid],h[valid],color=col[valid],lw=.9)
 ax.vlines(x[valid],np.minimum(o[valid],c[valid]),
           np.maximum(o[valid],c[valid]),color=col[valid],lw=3.2)
 ax.plot(x,ma,lw=1.7,color='#b48b39',label='224-session MA (raw close; no future)')
 ax.axhline(100,lw=.75,ls='--',color='#999999',alpha=.5)
 ax.grid(alpha=.2)
 ax.set_ylabel('Raw price indexed to signal close = 100')
 ax.set_title(f'{pair_id} · BLINDED CARD {side} / {card}\n'
  'KRX raw OHLCV + actual traded Amount, ending ON signal day',fontsize=13,loc='left')
 ax.legend(loc='upper right',frameon=False,fontsize=9)
 val=df.amount.to_numpy(dtype=float)/1e8
 av.bar(x,val,width=.85,color='#90999f',alpha=.8)
 av.grid(axis='y',alpha=.2)
 av.set_ylabel('Amount (KRW 100m)')
 av.set_xlabel('Trading sessions before signal (only pre-signal bars; latest at right)')
 av.xaxis.set_major_locator(MaxNLocator(nbins=8,integer=True))
 labels=av.get_xticks()
 av.set_xticks([q for q in labels if 0<=q<len(df)])
 av.set_xticklabels([str(int(q)-len(df)+1) for q in labels if 0<=q<len(df)])
 ax.tick_params(labelbottom=False)
 fig.text(.01,.006,'Retrospective case-control sample: outcome group is concealed; reviewer must classify shape before reveal.',fontsize=8,color='#555555')
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 fig.savefig(path,dpi=128,bbox_inches='tight',metadata={'Software':'StockHunter Research R1D4 (signal-only, no outcome)','Title':f'Blind chart {pair_id} {side}'})
 plt.close(fig)


def render_blind(selected,out,krx_root,max_pairs=31):
 root=Path(out);selected=Path(selected)
 k=read_csv(selected)
 exp={'pair_id','side','blind_card_id','code','signal_date'}
 if set(k.columns)!=exp:fail('CHART_IDENTITY_SCHEMA_DRIFT')
 if len(k)!=62 or k[['pair_id','side']].duplicated().any():fail('BLIND_PAIR_KEYS_DRIFT')
 if max_pairs<1 or max_pairs>31:fail('CHART_PAIR_LIMIT_INVALID')
 k=k.loc[k.pair_id.isin(sorted(k.pair_id.unique())[:max_pairs])]
 hist,days=historical_bars(krx_root,k)
 report=[]
 for row in k.itertuples(index=False):
  df=extract_asof_window(hist,days,str(row.code),str(row.signal_date))
  target=root/'blind_review/charts'/f'{row.pair_id}_{row.side}_{row.blind_card_id}.png'
  render_one(df,row.pair_id,row.side,row.blind_card_id,target)
  report.append({'pair_id':row.pair_id,'side':row.side,'blind_card_id':row.blind_card_id,
    'bar_count':len(df),'valid_price_bars':int(df.close.notna().sum()),
    '224MA_available_at_signal':int(np.isfinite(df.MA224.iloc[-1])),
    'source':'REAL_KRX_OHLCV_ACTUAL_AMOUNT_ONLY',
    'latest_bar_is_signal_day':True,'future_outcome_values_in_chart':False})
 write_csv(root/'blind_review/BLIND_CHART_COVERAGE_NO_IDENTITIES.csv',pd.DataFrame(report))
 print('R1D4_RENDER_PASS',len(report),'BLIND_NO_FUTURE',flush=True)
 return report


def main(argv=None):
 p=argparse.ArgumentParser()
 p.add_argument('--mode',required=True,choices=['select','render'])
 p.add_argument('--r1d3')
 p.add_argument('--out',required=True)
 p.add_argument('--selected')
 p.add_argument('--krx-root')
 p.add_argument('--max-pairs',type=int,default=31)
 args=p.parse_args(argv)
 if args.mode=='select':
  if not args.r1d3:fail('REQUIRES_R1D3_PATH')
  select_pairs(args.r1d3,args.out)
 else:
  if not (args.selected and args.krx_root):fail('REQUIRES_BLIND_CARD_KEYS_AND_KRX_ROOT')
  render_blind(args.selected,args.out,args.krx_root,args.max_pairs)

if __name__=='__main__':main()
