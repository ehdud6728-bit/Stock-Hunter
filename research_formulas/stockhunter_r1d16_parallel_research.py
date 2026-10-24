#!/usr/bin/env python3
"""R1-D16: strictly isolated research-only evaluation of frozen REAL_FULL and ODOLI lanes.

No network, no trades, no production imports, no row corrections. Never use
post-event ODOLI status/flow as a signal-time predictor.
"""
from __future__ import annotations
import argparse,hashlib,io,json,re,zipfile
from pathlib import Path
import numpy as np
import pandas as pd

VERSION='STOCKHUNTER_R1D16_PARALLEL_FROZEN_RESEARCH_V1'
H=(1,3,5,10)
ARCHIVES={
 'r143': ('real_full_r1c1_r143_seed_ledger.csv','real_full_r1c1_r143_outcome_ledger.csv'),
 'r1d13':('reports/r1d13/result/R1D13_EVENT_QUALITY_UNION.csv',),
 'odoli':('odoli_independent_r1/odoli_events.csv','odoli_independent_r1/odoli_current_status.csv'),
 'after':('odoli_after_session_history.csv',),
}
EXPECTED={'r143':{'seed':240,'outcome':705},'r1d13':{'trust':360},'odoli':{'events':1347,'status':1338},'after':{'rows':592}}

def fail(s):raise RuntimeError('R1D16_'+s)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def must(b,label):
 if not b:fail(label)
def protocol(path):
 must(Path(path).is_file(),'PROTOCOL_MISSING')
 x=json.loads(Path(path).read_text(encoding='utf-8'))
 must(x.get('version')==VERSION,'PROTOCOL_VERSION')
 must(x.get('research_only') is True and x.get('orders_placed')==0 and x.get('live_logic_changed') is False,'PROTOCOL_LIVE_GUARD')
 must(x.get('snapshot_asof')=='2026-10-23' and x.get('no_new_oos_claim') is True,'PROTOCOL_TIME_GUARD')
 must(set(x['pinned_sources'])==set(ARCHIVES),'PROTOCOL_PIN_SET')
 for key,pin in x['pinned_sources'].items():
  must(re.fullmatch(r'[0-9a-f]{64}',pin['zip_sha256']) is not None, 'INVALID_SHA_'+key)
  must(int(pin['run_id'])>0 and int(pin['artifact_id'])>0 and re.fullmatch(r'[0-9a-f]{40}',pin['head_sha']) is not None,'INVALID_PIN_'+key)
 return x

def table(src,member):
 p=Path(src)
 if p.is_dir():
  f=p/member
  must(f.is_file(),'SOURCE_FILE_MISSING_'+member.replace('/','_'))
  data=f.read_bytes()
 elif p.is_file() and zipfile.is_zipfile(p):
  with zipfile.ZipFile(p) as z:
   must(member in z.namelist(),'SOURCE_ZIP_MEMBER_MISSING_'+member.replace('/','_'))
   data=z.read(member)
 else:fail('INVALID_SOURCE_FORMAT')
 try:return pd.read_csv(io.BytesIO(data),dtype={'code':str},low_memory=False)
 except Exception as e:fail('BAD_SOURCE_CSV_'+str(e)[:70])

def exact(df,label):
 must(df.code.notna().all() and df.code.astype(str).str.fullmatch(r'[0-9A-Z]{6}').all(),'EXACT_ID_'+label)

def unique(df,keys,label):must(not df.duplicated(keys).any(),'DUPLICATE_'+label)
def num(s):return pd.to_numeric(s,errors='coerce')
def bools(s):return s.astype(str).str.strip().str.lower().isin(['true','1','yes'])
def safe_write(df,path):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 df.to_csv(path,index=False,float_format='%.10f',encoding='utf-8-sig')
def meditate(x):return float(x.median()) if len(x.dropna()) else None

def realfull(args,p,out):
 seeds,outcomes=map(lambda m:table(args.r143,m),ARCHIVES['r143'])
 qa=table(args.r1d13,ARCHIVES['r1d13'][0])
 must(len(seeds)==240 and len(outcomes)==705 and len(qa)==360,'FROZEN_REALFULL_ROWS')
 for d,l in [(seeds,'SEED'),(outcomes,'OUTCOME'),(qa,'TRUST_QA')]:exact(d,l)
 unique(seeds,['event_key'],'R143_SEEDS');unique(seeds,['signal_date','code'],'R143_SEED_IDENTITY')
 unique(outcomes,['event_key','horizon_td'],'R143_OUTCOME');unique(qa,['snapshot_date','code'],'TRUST_QA')
 must(set(outcomes.horizon_td)==set(H),'HORIZONS_DRIFT')
 must(set(outcomes.event_key).issubset(set(seeds.event_key)),'OUTCOME_KEY_NOT_SEEDED')
 must(seeds['signal_date'].astype(str).max()<='2026-10-23','LATE_SEED_DATE')
 must((qa['qa_any'].astype(str).str.lower().isin(['true','false'])).all(),'QA_BOOLEAN')
 # Every signal-side feature is frozen in seed ledger, never recomputed from future prices.
 sf=['event_key','signal_date','code','name','rank','entry_price','r1c1_shadow_eligible','r1c1_shadow_state','signal_close_vs_ma224_pct','ma224_status','observer_metric_status','pb_volume_vs_wave1','wave1_high_price','pullback_low_price','signal_amount_proxy','source_run_id','seed_hash']
 panel=seeds[sf].copy()
 panel['below_ma224_signal']=num(panel.signal_close_vs_ma224_pct).lt(0).astype('boolean')
 panel.loc[num(panel.signal_close_vs_ma224_pct).isna(),'below_ma224_signal']=pd.NA
 panel['signal_pb_volume_less_than_wave1']=num(panel.pb_volume_vs_wave1).lt(1).astype('boolean')
 panel.loc[num(panel.pb_volume_vs_wave1).isna(),'signal_pb_volume_less_than_wave1']=pd.NA
 qa_map=qa[['snapshot_date','code','qa_any','quality_status']].rename(columns={'snapshot_date':'signal_date','qa_any':'trust_qa_warning','quality_status':'trust_quality_status'})
 qa_map['trust_qa_warning']=bools(qa_map.trust_qa_warning)
 panel=panel.merge(qa_map,on=['signal_date','code'],how='left',validate='one_to_one',indicator=True)
 panel['qa_relation']=np.where(panel['_merge'].eq('left_only'),'UNMATCHED_TRUST_QA',np.where(panel.trust_qa_warning.eq(True),'TRUST_QA_WARNING','TRUST_QA_NO_FLAG_NOT_CERTIFIED'))
 panel=panel.drop(columns=['_merge'])
 outcomekeys=['event_key','horizon_td','horizon_date','close_ret_pct','mfe_pct','mae_pct','horizon_close','outcome_price_source']
 outcomes=outcomes[outcomekeys].copy()
 for h in H:
  piece=outcomes[outcomes.horizon_td.eq(h)].drop(columns='horizon_td').rename(columns={c:f'd{h}_{c}' for c in outcomekeys if c!='event_key'})
  panel=panel.merge(piece,on='event_key',how='left',validate='one_to_one')
 panel['d5_d10_both_ready']=num(panel.d5_close_ret_pct).notna() & num(panel.d10_close_ret_pct).notna()
 panel['d10_minus_d5_pp']=num(panel.d10_close_ret_pct)-num(panel.d5_close_ret_pct)
 panel['d5_mfe_at_least5_then_d10_close_below0']=(num(panel.d5_mfe_pct)>=5)&(num(panel.d10_close_ret_pct)<0)
 panel['historic_outcome_cache_sha_certified']=False
 # Group statistics are observational. QA-missing does not become QA-clean.
 dims=['ma224_signal_group','r1c1_shadow_state','qa_relation']
 panel['ma224_signal_group']=np.where(num(panel.signal_close_vs_ma224_pct).isna(),'UNAVAILABLE',np.where(num(panel.signal_close_vs_ma224_pct)<0,'BELOW_224','AT_OR_ABOVE_224'))
 def group_rows(col):
  records=[]
  for label,g in panel.groupby(col,dropna=False):
   rec={'dimension':col,'group':str(label),'n_signals':len(g),'n_distinct_days':g.signal_date.nunique(),'n_distinct_codes':g.code.nunique(),'n_trust_qa_warning':int(g.qa_relation.eq('TRUST_QA_WARNING').sum()),'n_without_matched_trust_qa':int(g.qa_relation.eq('UNMATCHED_TRUST_QA').sum())}
   for h in H:
    v=num(g[f'd{h}_close_ret_pct']).dropna()
    rec[f'd{h}_n']=len(v);rec[f'd{h}_median_pct']=meditate(v)
   paired=g[g.d5_d10_both_ready];rec['d5d10_paired_n']=len(paired);rec['median_paired_d10_minus_d5_pp']=meditate(num(paired.d10_minus_d5_pp))
   records.append(rec)
  return records
 rows=[]
 for dim in dims:rows.extend(group_rows(dim))
 safe_write(panel,out/'REALFULL_R143_SIGNAL_SIDE_224_WAVE_OUTCOME_PANEL.csv')
 safe_write(pd.DataFrame(rows),out/'REALFULL_SIGNAL_SIDE_GROUP_DESCRIPTIVE.csv')
 # QA sensitivity is diagnostic only; no filter for a production signal or rank.
 g=panel[panel.d5_d10_both_ready]
 levels=[('ALL_R143_PAIRED',g),('MATCHED_TRUST_QA_NO_FLAG_NOT_CERTIFIED',g[g.qa_relation.eq('TRUST_QA_NO_FLAG_NOT_CERTIFIED')]),('TRUST_QA_WARNINGS',g[g.qa_relation.eq('TRUST_QA_WARNING')]),('UNMATCHED_TRUST_QA',g[g.qa_relation.eq('UNMATCHED_TRUST_QA')])]
 sens=[{'cohort':label,'n':len(d),'d5_median':meditate(num(d.d5_close_ret_pct)),'d10_median':meditate(num(d.d10_close_ret_pct)),'d10_minus_d5_median_pp':meditate(num(d.d10_minus_d5_pp)),'n_d5_mfe5_then_d10_below0':int(d.d5_mfe_at_least5_then_d10_close_below0.sum())} for label,d in levels]
 safe_write(pd.DataFrame(sens),out/'REALFULL_R143_PAIRED_GIVEBACK_QA_SENSITIVITY.csv')
 cross=[]
 for (ma,st),grp in panel.groupby(['ma224_signal_group','r1c1_shadow_state']):
  r5=num(grp.d5_close_ret_pct).dropna();r10=num(grp.d10_close_ret_pct).dropna();pair=grp[grp.d5_d10_both_ready]
  cross.append({'ma224_signal_group':ma,'frozen_r1c1_shadow_state':st,'n_seeds':len(grp),'n_d5':len(r5),'n_d10':len(r10),'median_d5_pct':meditate(r5),'median_d10_pct':meditate(r10),'paired_d5d10_n':len(pair),'paired_delta_median_pp':meditate(num(pair.d10_minus_d5_pp)),'descriptive_only_no_new_gate':True})
 safe_write(pd.DataFrame(cross),out/'REALFULL_MA224_X_FROZEN_PULLBACK_CROSS.csv')
 # Compare below-vs-above only on dates where BOTH statuses have completed D10,
 # and never interpret the day-median differences as a causal trading edge.
 dd=panel[panel.d10_close_ret_pct.notna() & panel.ma224_signal_group.isin(['BELOW_224','AT_OR_ABOVE_224'])]
 dayrows=[]
 for day,grp in dd.groupby('signal_date'):
  a=num(grp[grp.ma224_signal_group.eq('BELOW_224')].d10_close_ret_pct).dropna()
  b=num(grp[grp.ma224_signal_group.eq('AT_OR_ABOVE_224')].d10_close_ret_pct).dropna()
  if len(a) and len(b):dayrows.append({'signal_date':day,'below_n':len(a),'above_n':len(b),'below_median_d10_pct':meditate(a),'above_median_d10_pct':meditate(b),'below_minus_above_median_pp':float(a.median()-b.median()),'same_day_small_sample_noncausal':True})
 safe_write(pd.DataFrame(dayrows),out/'REALFULL_SAME_DATE_BELOW_VS_ABOVE_DESCRIPTIVE.csv')
 return {'r143_seeds':len(seeds),'r143_outcome_rows':len(outcomes),'matched_trust_quality':int(panel.qa_relation.ne('UNMATCHED_TRUST_QA').sum()),'unmatched_trust_quality':int(panel.qa_relation.eq('UNMATCHED_TRUST_QA').sum()),'r143_paired_d5d10':int(panel.d5_d10_both_ready.sum()),'same_day_below_above_dates':len(dayrows),'asof_features_pre_signal_frozen':True,'shadow_state_not_a_production_gate':True}

def odoli(args,p,out):
 events,status=map(lambda m:table(args.odoli,m),ARCHIVES['odoli'])
 after=table(args.after,ARCHIVES['after'][0]);must(len(events)==1347 and len(status)==1338 and len(after)==592,'FROZEN_ODOLI_ROWS')
 for d,l in [(events,'EVENTS'),(status,'CURRENT_STATUS'),(after,'AFTER')]:exact(d,l)
 for d,l in [(events,'EVENTS'),(status,'CURRENT_STATUS'),(after,'AFTER')]:unique(d,['signal_date','code'],l)
 must(events.signal_date.nunique()==17,'FROZEN_ODOLI_DATES')
 must((events.definition=='STRICT_ODOLI_R1').all(),'SOURCE_DEFINITION_CHANGED')
 must(status.asof_date.astype(str).le('2026-10-23').all() and status.asof_date.astype(str).ge('2026-09-28').all(),'STATUS_ASOF_OUT_OF_FROZEN_PERIOD')
 must(set(zip(status.signal_date,status.code)).issubset(set(zip(events.signal_date,events.code))),'STATUS_NOT_IN_EVENTS')
 must(set(zip(after.signal_date,after.code)).issubset(set(zip(events.signal_date,events.code))),'AFTER_NOT_IN_EVENTS')
 # Status/flow is retrospective, not a signal-time classifier or a D+5-locked outcome for all rows.
 snap=events[['signal_date','code','name','market','close','signal_low','signal_amount_ratio20','signal_volume_ratio20','down_up_amount_ratio_10d','down_up_volume_ratio_10d']].merge(status[['signal_date','code','asof_date','day','flow','current_close_ret_pct','mfe_pct','mae_pct','d1_to_d2_higher_low','d2_above_ma5','d1_d3_all_above_ma5','signal_low_survived','C2','C3','C4','C2_day','C3_day','C4_day']],on=['signal_date','code'],how='left',validate='one_to_one',indicator=True)
 snap['status_coverage']=np.where(snap['_merge'].eq('both'),'STATUS_PRESENT_ASOF_RECORDED','STATUS_MISSING_DO_NOT_IMPUTE')
 snap=snap.drop(columns='_merge')
 snap['status_stale_before_snapshot']=snap.asof_date.notna() & snap.asof_date.astype(str).lt('2026-10-23')
 snap['qa_extreme_mfe_mae']=num(snap.mfe_pct).le(-99) | num(snap.mae_pct).le(-99)
 snap['flow_is_signal_time_feature']=False
 snap['snapshot_close_is_fixed_horizon_d5']=num(snap.day).eq(5)
 safe_write(snap,out/'ODOLI_CURRENT_ASOF_FLOW_NOT_PREDICTOR.csv')
 rows=[]
 for day,g in snap[snap.day.notna()].groupby('day'):
  for flow,grp in g.groupby('flow'):
   rows.append({'source_status_dates_include_earlier_than_20261023':True,'observed_day_since_signal':int(day),'snapshot_flow_label':flow,'n':len(grp),'n_signal_dates':grp.signal_date.nunique(),'snapshot_ret_median_pct':meditate(num(grp.current_close_ret_pct)),'snapshot_mfe_median_pct':meditate(num(grp.mfe_pct)),'snapshot_mae_median_pct':meditate(num(grp.mae_pct)),'n_stale_asof':int(grp.status_stale_before_snapshot.sum()),'n_extreme_mfe_mae':int(grp.qa_extreme_mfe_mae.sum()),'flow_is_forward_observation_not_signal_time':True,'same_age_not_same_signal_date_market_regime':True})
 safe_write(pd.DataFrame(rows),out/'ODOLI_CURRENT_FLOW_BY_ELAPSED_DAY_DESCRIPTIVE.csv')
 # after availability itself is the outcome; absent values never zero return.
 after['has_valid_after']=bools(after.after_has_data)&num(after.after_ret_from_krx_close_pct).notna()
 after['after_return_valid']=num(after.after_ret_from_krx_close_pct).where(after.has_valid_after)
 af=after[['signal_date','code','after_has_data','has_valid_after','after_return_valid','after_source','after_market_status','after_error']].copy()
 af=af.merge(status[['signal_date','code','day','flow','current_close_ret_pct','asof_date']],on=['signal_date','code'],how='left',validate='one_to_one',indicator=True)
 af['status_matched']=af['_merge'].eq('both');af=af.drop(columns='_merge')
 af['evaluation_is_snapshot_only']=True
 af['asof_d5_available_exactly_day5']=num(af.day).eq(5)&af.status_matched
 af['status_stale_before_snapshot']=af.asof_date.notna() & af.asof_date.astype(str).lt('2026-10-23')
 safe_write(af,out/'ODOLI_AFTER_OBSERVABILITY_AND_SNAPSHOT.csv')
 cov=[]
 for day,g in af.groupby('signal_date'):
  cov.append({'signal_date':day,'n':len(g),'valid_after_n':int(g.has_valid_after.sum()),'valid_after_pct':round(100*g.has_valid_after.mean(),5),'current_status_n':int(g.status_matched.sum()),'snapshot_day':int(g.day.dropna().iloc[0]) if len(g.day.dropna()) and g.day.dropna().nunique()==1 else None,'n_stale_status':int(g.status_stale_before_snapshot.sum()),'valid_after_missing_not_zero':True})
 safe_write(pd.DataFrame(cov),out/'ODOLI_AFTER_COVERAGE_AND_AGE.csv')
 # Exactly age D5 makes Oct16 a single-date descriptive comparison; not an AFTER alpha claim.
 same=af[af.asof_d5_available_exactly_day5]
 same_rows=[]
 for sd,g in same.groupby('signal_date'):
  for covered,gg in g.groupby('has_valid_after'):
   same_rows.append({'signal_date':sd,'group':'AFTER_COVERED' if covered else 'AFTER_MISSING','n':len(gg),'d5_snapshot_ret_median_pct':meditate(num(gg.current_close_ret_pct)),'after_ret_median_pct':meditate(num(gg.after_return_valid)),'single_date_noncausal':True})
 safe_write(pd.DataFrame(same_rows),out/'ODOLI_AFTER_SAME_DATE_EXACT_D5_DESCRIPTIVE.csv')
 return {'events':len(events),'events_distinct_days':events.signal_date.nunique(),'status_rows':len(status),'status_missing':len(events)-len(status),'status_stale':int(snap.status_stale_before_snapshot.sum()),'status_extreme_mfe_mae':int(snap.qa_extreme_mfe_mae.sum()),'after_rows':len(after),'after_valid_rows':int(after.has_valid_after.sum()),'fast_snapshot_n':int(snap.flow.eq('⚡ FAST 흐름 관찰').sum()),'slow_c2c3_snapshot_n':int(snap.flow.eq('🌱 SLOW C2/C3 확인').sum()),'slow_c4_snapshot_n':int(snap.flow.eq('🌱 SLOW C4 확인').sum()),'snapshot_flows_are_not_frozen_predictors':True}

def main(argv=None):
 a=argparse.ArgumentParser();a.add_argument('--lane',choices=['realfull','odoli'],required=True);a.add_argument('--protocol',required=True);a.add_argument('--r143');a.add_argument('--r1d13');a.add_argument('--odoli');a.add_argument('--after');a.add_argument('--out',required=True);args=a.parse_args(argv)
 p=protocol(args.protocol)
 sources=['r143','r1d13'] if args.lane=='realfull' else ['odoli','after']
 for key in sources:
  path=getattr(args,key);must(path is not None,'SOURCE_ARG_'+key)
  if Path(path).is_file():must(sha(path)==p['pinned_sources'][key]['zip_sha256'],'SOURCE_SHA_DRIFT_'+key)
  else:must(Path(path).is_dir(),'INPUT_NOT_FILE_OR_DIR_'+key)
 o=Path(args.out);must(not o.exists() or not any(o.iterdir()),'RESULT_ALREADY_EXISTS');o.mkdir(parents=True,exist_ok=True)
 result=realfull(args,p,o) if args.lane=='realfull' else odoli(args,p,o)
 m={'version':VERSION,'lane':args.lane,'asof':'2026-10-23','inputs':{k:{'run_id':p['pinned_sources'][k]['run_id'],'artifact_id':p['pinned_sources'][k]['artifact_id'],'zip_sha256':p['pinned_sources'][k]['zip_sha256']} for k in sources},**result,'RESEARCH_ONLY':True,'orders_placed':0,'live_logic_changed':False,'production_eligible':False,'new_oos':False,'historical_prices_unadjusted_and_not_certified':True,'no_cross_lane_population_merging':True,'no_retro_flow_used_as_signal_feature':True}
 (o/'R1D16_MANIFEST.json').write_text(json.dumps(m,ensure_ascii=False,sort_keys=True,indent=2),encoding='utf-8')
 print('R1D16_RESEARCH_ONLY_PASS',json.dumps(result,ensure_ascii=False))
 return m
if __name__=='__main__':main()
