#!/usr/bin/env python3
"""R1D3: read-only winner/loser, extreme-winner, market-context audit of frozen R1D2.

Does NOT revise the R1D1/R1D2 proxy, generate trading signals or claim OOS.
Use historical prices as-of the signal date; forward outcomes joined only after
feature extraction. PIT corporate actions, share types and execution are unknown.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

VERSION='STOCKHUNTER_R1D3_FROZEN_R1D2_OUTCOME_EXPLANATION_RESEARCH_ONLY_V1'
FROZEN_SOURCE_RUN=39873801594
FROZEN_R1D2_ARTIFACT_ID=12468095269
FROZEN_R1D2_ARTIFACT_SHA256='a3384e292dfc971f6716861de81f5a03e13f89476e4ddad3fdd3b30e34be84a6'
SUCCESS_5=5.0
STRONG_15=15.0
EXPLOSIVE_30=30.0
FAILURE_MINUS_5=-5.0
OUTCOME_STATUS='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
EPISODES='recent_proxy_asof_20session_deduplicated_episodes.csv'
OUTCOMES='FORWARD_PRICE_OUTCOMES_ONLY_NOT_TO_BLIND_REVIEW.csv'
INDEX='market_index_asof_regime_each_scanned_day.csv'
CONTEXT='recent_fullmarket_asof_context.csv.gz'
MANIFEST='recent_3to4month_research_manifest.json'
EXTERNAL_UNKNOWN=[
 'signal_date_exchange_membership_and_security_type',
 'PIT_sector_industry_theme',
 'foreign_institutional_net_buy_by_stock',
 'USD_KRW_and_bond_yield_asof',
 'short_sell_balance_and_margin_liquidation',
 'corporate_disclosures_known_before_signal',
 'corporate_action_adjusted_price_and_true_tradeability',
 'historically_confirmed_market_cap_and_shares_outstanding',
]


def fail(msg):
 raise RuntimeError('R1D3_FAIL_CLOSED_'+str(msg))


def as_float(s):
 return pd.to_numeric(s,errors='coerce')


def encode_code(col):
 col=col.astype(str).str.strip().str.upper()
 if not col.str.fullmatch(r'[0-9][0-9A-Z]{5}').all():
  # pandas converted 6-digit numeric codes to integer? The file contract is strings.
  if col.str.fullmatch(r'\d{1,6}').all():return col.str.zfill(6)
  fail('INVALID_TICKER_CODE')
 return col


def read_inputs(root:Path):
 root=Path(root)
 for f in [EPISODES,OUTCOMES,INDEX,CONTEXT,MANIFEST]:
  if not (root/f).is_file():fail('MISSING_R1D2_INPUT:'+f)
 m=json.loads((root/MANIFEST).read_text('utf-8'))
 checks=(m.get('source_verified') is True and m.get('status')=='RESEARCH_ONLY_RECENT_DOWNSIDE_224_PROXY_CENSUS_COMPLETE'
  and m.get('window')=='recent_4m' and m.get('cadence')=='daily'
  and m.get('start')=='2026-06-15' and m.get('end')=='2026-10-12'
  and m.get('strict_proxy_hits')==6634 and m.get('cooldown_20session_episodes')==1249
  and m.get('KRX_sessions_scanned')==80 and m.get('not_independent_66_formula_rescreen') is True
  and m.get('strategy_ready') is False and m.get('ORDERS_PLACED')==0)
 if not checks:fail('R1D2_FROZEN_MANIFEST_DRIFT')
 epis=pd.read_csv(root/EPISODES,dtype={'code':str,'date':str},low_memory=False)
 out=pd.read_csv(root/OUTCOMES,dtype={'code':str,'signal_date':str},low_memory=False)
 idx=pd.read_csv(root/INDEX,dtype={'date':str},low_memory=False)
 epis['code']=encode_code(epis.code);out['code']=encode_code(out.code)
 if len(epis)!=1249 or len(out)!=4996 or len(idx)!=80:fail('SOURCE_LENGTH_DRIFT')
 if epis.duplicated(['code','date']).any() or out.duplicated(['code','signal_date','horizon']).any():fail('DUPLICATE_SOURCE_EVENT')
 if not epis['strict_proxy'].eq(1).all() or not epis['episode_first_in_20session_cooldown'].eq(1).all():fail('NOT_FROZEN_STRICT_EPISODES')
 if set(out.horizon.unique())!={5,10,15,20}:fail('WRONG_HORIZONS')
 if out[['code','signal_date']].drop_duplicates().shape[0]!=1249:fail('NOT_EXACT_1249_OUTCOMES')
 if not out['signals_from'].str.contains('NOT_66_FORMULAS',regex=False).all():fail('CLAIMS_66_NATIVE')
 if idx.duplicated(['date']).any():fail('DUPLICATED_INDEX_DATE')
 # Deliberate exposure of availability and censoring: keep ALL 1249 episode keys.
 return epis,out,idx,m


def label_outcome(ret):
 if not np.isfinite(ret):return 'NOT_OBSERVED'
 if ret>=EXPLOSIVE_30:return 'EXPLOSIVE_GE_30'
 if ret>=STRONG_15:return 'STRONG_15_TO_30'
 if ret>=SUCCESS_5:return 'SUCCESS_5_TO_15'
 if ret<=FAILURE_MINUS_5:return 'FAILURE_LE_MINUS5'
 return 'MIDDLE_MINUS5_TO_PLUS5'


def market_breadth(root):
 ctx=pd.read_csv(Path(root)/CONTEXT,usecols=['date','status','below_ma224','base_now','strict_proxy'],dtype={'date':str},low_memory=False)
 ready=ctx[ctx.status.eq('KRX_PRICE_CONTEXT_READY_NO_PIT_TRADEABILITY_CERT')].copy()
 if len(ctx)!=229812 or len(ready)!=175212:fail('BREADTH_BASE_POPULATION_DRIFT')
 if not set(ready.below_ma224.dropna().unique()).issubset({0.0,1.0}):fail('BAD_MA224_FLAG')
 breadth=ready.groupby('date',as_index=False).agg(
  ready_n=('status','size'),
  pct_above224_among_unfiltered_eligible=('below_ma224',lambda v:100*(1-v.astype(float)).mean()),
  pct_base_now_among_unfiltered_eligible=('base_now',lambda v:100*(v.astype(float)).mean()),
  pct_strict_proxy_among_unfiltered_eligible=('strict_proxy',lambda v:100*(v.astype(float)).mean()),
 )
 breadth=breadth.sort_values('date')
 if len(breadth)!=80:fail('BREADTH_MISSING_80_DAYS')
 for n in [5,10]:
  breadth[f'above224_breadth_change_{n}day_pp']=breadth.pct_above224_among_unfiltered_eligible-breadth.pct_above224_among_unfiltered_eligible.shift(n)
 return breadth


def extract_krx_daily(root):
 """Only available source days; never infer rows for a halted/missing ticker."""
 root=Path(root)
 files=sorted(root.glob('shard_*/2026*.csv.gz'))
 if len(files)<130:fail('KRX_ASOF_DAILY_SOURCE_FILES_INCOMPLETE')
 required=['code','date','open','high','low','close','volume','amount']
 rows=[]
 for file in files:
  chunk=pd.read_csv(file,dtype={'code':str,'date':str},usecols=required,low_memory=False)
  if list(chunk.columns)!=required:fail('KRX_RAW_COLUMNS_DRIFT')
  if len(chunk)<500:fail('KRX_SMALL_DAY:'+file.name)
  if chunk.date.nunique()!=1 or chunk.date.iloc[0]!=file.stem[:8][:4]+'-'+file.stem[:8][4:6]+'-'+file.stem[:8][6:8]:fail('BAD_SOURCE_DATE:'+file.name)
  chunk.code=encode_code(chunk.code)
  rows.append(chunk)
 allx=pd.concat(rows,ignore_index=True)
 if allx.duplicated(['code','date']).any():fail('DUPLICATED_KRX_DATE_TICKER')
 return allx


def stock_and_market_asof(krx):
 dates=sorted(krx.date.unique().tolist())
 if dates[0]>'2026-02-01' or dates[-1]<'2026-10-12':fail('KRX_ASOF_HISTORY_OR_END_MISSING')
 price=krx.copy()
 price['code']=encode_code(price.code)
 # The daily calendar is bounded to the original KRX historical source, no later data.
 index={d:i for i,d in enumerate(dates)}
 price['session_i']=price.date.map(index)
 price.sort_values(['code','date'],inplace=True)
 g=price.groupby('code',sort=False)
 for days in [20,60]:
  lag=g.close.shift(days)
  lag_i=g.session_i.shift(days)
  price[f'stock_ret{days}_pct']=np.where((price.session_i-lag_i==days)&(lag>0)&(price.close>0),100*(price.close/lag-1),np.nan)
 # A rolling liquidity measure is valid only when all 20 original source sessions exist.
 prev19_i=g.session_i.shift(19)
 contiguous20=price.session_i-prev19_i==19
 price['krx_avg20_actual_amount_KRW']=g.amount.transform(lambda s:s.rolling(20,min_periods=20).mean()).where(contiguous20)
 price['krx_median20_actual_amount_KRW']=g.amount.transform(lambda s:s.rolling(20,min_periods=20).median()).where(contiguous20)
 price['krx_avg5_actual_amount_KRW']=g.amount.transform(lambda s:s.rolling(5,min_periods=5).mean()).where(price.session_i-g.session_i.shift(4)==4)
 price['krx_actual_amount_5vs20_ratio']=price.krx_avg5_actual_amount_KRW/price.krx_avg20_actual_amount_KRW.replace({0:np.nan})
 price['krx_avg20_actual_amount_100mKRW']=price.krx_avg20_actual_amount_KRW/1e8
 price['krx_amount_today_100mKRW']=price.amount/1e8
 r=price.close / g.close.shift(1) - 1
 price['krx_recent20_return_sd_pct']=r.groupby(price.code).transform(lambda s:s.rolling(20,min_periods=20).std())*100
 price['krx_20day_above_close_momentum_pct']=np.where((price.close>0)&(g.close.transform(lambda s:s.rolling(20,min_periods=20).mean())>0),100*(price.close/g.close.transform(lambda s:s.rolling(20,min_periods=20).mean())-1),np.nan)
 valid=price[(price.date>='2026-06-15')&(price.date<='2026-10-12')]
 cols=['date','code','stock_ret20_pct','stock_ret60_pct','krx_avg20_actual_amount_100mKRW','krx_median20_actual_amount_KRW','krx_amount_today_100mKRW','krx_actual_amount_5vs20_ratio','krx_recent20_return_sd_pct','krx_20day_above_close_momentum_pct']
 stock=valid[cols]
 # Total traded amount includes all original KRX code families; security types NOT verified.
 agg=krx.groupby('date',as_index=False).agg(unfiltered_market_actual_amount_sum=('amount','sum'),unfiltered_krx_codes=('code','size')).sort_values('date')
 agg['all_source_market_amount_today_vs_prev20_ratio']=agg.unfiltered_market_actual_amount_sum/agg.unfiltered_market_actual_amount_sum.shift(1).rolling(20,min_periods=20).mean().replace({0:np.nan})
 agg=agg.loc[(agg.date>='2026-06-15')&(agg.date<='2026-10-12')]
 return stock,agg


def add_outcomes_after_asof(asof,out,idx):
 d=out[out.horizon.eq(20)].copy()
 a=asof.merge(d,left_on=['code','date'],right_on=['code','signal_date'],how='left',validate='one_to_one',suffixes=('','_out'))
 if len(a)!=1249 or a.status_out.isna().any():fail('D20_OUTCOME_JOIN_COVERAGE_DRIFT')
 if int(a.status_out.eq(OUTCOME_STATUS).sum())!=939:fail('D20_COMPLETE_SET_DRIFT')
 a['group']=a.apply(lambda x:label_outcome(float(x.net_20bps_pct)) if x.status_out==OUTCOME_STATUS else 'NO_D20_COMPLETE',axis=1)
 # Full cost/market comparator only as an illustrative path benchmark, never proof of alpha.
 a['index_equal_weight_signal_close_to_exit_pct']=(a.KOSPI_signal_close_to_exit_close_pct+a.KOSDAQ_signal_close_to_exit_close_pct)/2
 a['exploratory_reference_excess_pct']=a.net_20bps_pct-a.index_equal_weight_signal_close_to_exit_pct
 return a


def serialize(df,path):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 df.to_csv(path,index=False,encoding='utf-8-sig')


def profile_groups(a,cols):
 o=[]
 for group,d in a.groupby('group',sort=True):
  row={'group':group,'n':len(d),'codes':d.code.nunique(),'signal_days':d.date.nunique(), 'future_D20_complete':int(d.status_out.eq(OUTCOME_STATUS).sum()),'median_D20_net20bps_pct':float(d.net_20bps_pct.median()) if d.net_20bps_pct.notna().any() else None}
  for k in cols:
   v=as_float(d[k]);row[k+'_median']=float(v.median()) if v.notna().any() else None
  o.append(row)
 return pd.DataFrame(o)


def match_dates(a,features):
 # Compare both groups only on identical signal dates; this removes mean market-day differences.
 s=a[a.net_20bps_pct>=15];f=a[a.net_20bps_pct<=-5]
 matches=[]
 for col in features:
  sm=s.groupby('date')[col].median();fm=f.groupby('date')[col].median()
  dif=sm.to_frame('strong').join(fm.to_frame('failed'),how='inner').dropna()
  n=len(dif)
  if n<2:continue
  deltas=dif.strong-dif.failed
  matches.append({'asof_feature':col,'paired_signal_dates':n,
   'median_strong_minus_failed_date_median':float(deltas.median()),
   'pct_dates_strong_higher':float(100*(deltas>0).mean()),
   'n_dates_equal':int((deltas==0).sum()),
   'comparison':'EXPLORATORY_WITHIN_DATE_MEDIANS_NOT_CAUSAL'})
 return pd.DataFrame(matches)


def quantile_group(a,cols):
 # Do not use trained thresholds. Cohort-internal quartiles are for audit-only profiling.
 rows=[]
 d=a[a.status_out.eq(OUTCOME_STATUS)].copy()
 for col in cols:
  if d[col].nunique(dropna=True)<6:continue
  tmp=d[d[col].notna()].copy()
  # Quantiles by variable values, not per-row ranking to avoid artificial binning of tied discrete vars.
  tmp['quantile']=pd.qcut(tmp[col],4,duplicates='drop')
  for j,(q,v) in enumerate(tmp.groupby('quantile',observed=True),start=1):
   ret=v.net_20bps_pct
   rows.append({'asof_feature':col,'quantile':f'Q{j}_interval_{q}','min_feature':float(v[col].min()),'max_feature':float(v[col].max()),'n':len(v),
                'D20_net20bps_median':float(ret.median()),'D20_plus5_pct':float(100*(ret>=5).mean()),
                'D20_plus15_pct':float(100*(ret>=15).mean()),'D20_minus5_pct':float(100*(ret<=-5).mean())})
 return pd.DataFrame(rows)


def summary_counts(a):
 d=a[a.status_out.eq(OUTCOME_STATUS)]
 m={'strong_n':int((d.net_20bps_pct>=15).sum()),'explosive_n':int((d.net_20bps_pct>=30).sum()),'failure_n':int((d.net_20bps_pct<=-5).sum()),'n_complete':len(d),'n_episode':len(a),'n_not_complete':int(a.status_out.ne(OUTCOME_STATUS).sum()),
    'strong_signal_dates':d.loc[d.net_20bps_pct>=15,'date'].nunique(),
    'failure_signal_dates':d.loc[d.net_20bps_pct<=-5,'date'].nunique()}
 m['D20_status_counts']={str(k):int(v) for k,v in a.status_out.value_counts().items()}
 m['by_month']=d.groupby(d.date.str.slice(0,7)).agg(complete=('code','size'),strong=('net_20bps_pct',lambda x:int((x>=15).sum())),failed=('net_20bps_pct',lambda x:int((x<=-5).sum()))).to_dict(orient='index')
 return m


def run(r1d2,krx_root,out):
 root=Path(r1d2);out=Path(out)
 ep,fw,idx,orig=read_inputs(root)
 breadth=market_breadth(root)
 daily=extract_krx_daily(krx_root)
 stock,market=stock_and_market_asof(daily)
 asof=ep.merge(idx[['date','KOSPI_ret60_pct','KOSDAQ_ret60_pct']],on='date',validate='many_to_one',how='left')
 asof=asof.merge(breadth,on='date',validate='many_to_one',how='left')
 asof=asof.merge(market[['date','all_source_market_amount_today_vs_prev20_ratio','unfiltered_market_actual_amount_sum']],on='date',validate='many_to_one',how='left')
 asof=asof.merge(stock,on=['date','code'],validate='one_to_one',how='left')
 if len(asof)!=1249 or asof['krx_avg20_actual_amount_100mKRW'].notna().sum()<1200:fail('ASOF_FEATURE_COVERAGE_INSUFFICIENT')
 serialize(asof,out/'asof_only'/'frozen_episode_asof_features_NO_FUTURE.csv')
 serialize(breadth,out/'asof_only'/'market_224_breadth_daily_NO_FUTURE.csv')
 serialize(market,out/'asof_only'/'unfiltered_all_market_amount_proxy_NO_FUTURE.csv')
 joined=add_outcomes_after_asof(asof,fw,idx)
 serialize(joined,out/'after_outcome_unblinded'/'event_group_outcome_and_prior_features.csv')
 pre=['wave_gain_pct','wave_retrace_pct','prewave_accum_amount_ratio','pullback_dry_amount_ratio','recent5_vs_prior20_actual_amount','below_60_ratio',
      'stock_ret20_pct','stock_ret60_pct','krx_avg20_actual_amount_100mKRW','krx_actual_amount_5vs20_ratio','krx_recent20_return_sd_pct','krx_20day_above_close_momentum_pct']
 external=['KOSPI_ret20_pct','KOSDAQ_ret20_pct','KOSPI_ret60_pct','KOSDAQ_ret60_pct','pct_above224_among_unfiltered_eligible','above224_breadth_change_5day_pp','all_source_market_amount_today_vs_prev20_ratio']
 serialize(profile_groups(joined,pre+external),out/'after_outcome_unblinded'/'group_profiles_asof_medians.csv')
 serialize(match_dates(joined,pre),out/'after_outcome_unblinded'/'same_day_strong_vs_failure_feature_differences.csv')
 serialize(quantile_group(joined,pre+external),out/'after_outcome_unblinded'/'quartile_sensitivity_descriptive_NO_TUNING.csv')
 complete=joined[joined.status_out.eq(OUTCOME_STATUS)]
 cols=['code','date','net_20bps_pct','net_100bps_pct','stock_ret20_pct','stock_ret60_pct','wave_gain_pct','wave_retrace_pct','prewave_accum_amount_ratio','pullback_dry_amount_ratio','krx_avg20_actual_amount_100mKRW','krx_recent20_return_sd_pct','KOSPI_ret20_pct','KOSDAQ_ret20_pct','pct_above224_among_unfiltered_eligible','exploratory_reference_excess_pct']
 serialize(complete.sort_values('net_20bps_pct',ascending=False).head(30)[cols],out/'after_outcome_unblinded'/'top30_STRONG_examples_not_recommendations.csv')
 serialize(complete.sort_values('net_20bps_pct').head(30)[cols],out/'after_outcome_unblinded'/'bottom30_FAILED_examples_not_recommendations.csv')
 counts=summary_counts(joined)
 checks={'version':VERSION,'source_run':FROZEN_SOURCE_RUN,'source_artifact_id':FROZEN_R1D2_ARTIFACT_ID,'status':'SUCCESS_EXPLORATORY_COHORT_DESCRIPTIVE_NOT_OOS',
  'R1D2_original_224_proxy_unchanged':True,'asof_features_only_before_outcomes_join':True,'no_research_to_live_promotion':True,
  'KRX_unadjusted_price_and_actual_amount':True,'KRX_SECURITY_TYPE_PIT_NOT_CERTIFIED':True,
  'sector_fundamentals_fx_rates_events_NOT_ATTACHED':EXTERNAL_UNKNOWN,
  'no_causal_predictive_or_out_of_sample_claim':True,'orders_placed':0,'strategy_ready':False,
  'observed_event_counts':counts,'asof_liquidity_available':int(asof.krx_avg20_actual_amount_100mKRW.notna().sum()),
  'asof_stock_ret60_available':int(asof.stock_ret60_pct.notna().sum()),
  'caution':'Censoring, unadjusted corporate actions, 20-day overlapping episodes, same-date/stock dependence, unverified security type and survivor universe. No threshold tuning.'}
 (out/'r1d3_manifest.json').write_text(json.dumps(checks,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
 return checks


def main(argv=None):
 p=argparse.ArgumentParser()
 p.add_argument('--r1d2',required=True)
 p.add_argument('--krx-root',required=True)
 p.add_argument('--out',required=True)
 a=p.parse_args(argv)
 m=run(a.r1d2,a.krx_root,a.out)
 print('R1D3_DESCRIPTIVE_COMPLETE',json.dumps(m['observed_event_counts'],ensure_ascii=False),flush=True)
 return 0

if __name__=='__main__':raise SystemExit(main())
