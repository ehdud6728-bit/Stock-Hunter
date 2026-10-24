#!/usr/bin/env python3
"""R1-D10 accumulation / PROVENANCE AND OUTCOME QA ONLY.

Reads pre-existing, immutable REAL_FULL TRUST, R143 and ODOLI AFTER artifacts.
Never screens stocks or mutates source files. All retrospective diagnostics; no trade gates.
"""
from __future__ import annotations
import argparse, json, hashlib, io, math, re, zipfile
from pathlib import Path
import numpy as np
import pandas as pd

RESEARCH_ONLY = True
VERSION = 'STOCKHUNTER_R1D10_ACCUMULATED_PROVENANCE_QA_V1'
PIN = {
 'realfull': {'id':12504190178,'run':39948786391,'sha256':'3d2ee08727e50b248cdf7c41be7fb21744d2f9ee908747e693895c4d443ab34b'},
 'r143': {'id':12504990509,'run':39960213718,'sha256':'a1ba01eea0e1976e9edd59ec6b548d2d56f897e7638498dc4185b66b284edbbf'},
 'odoli': {'id':12503340512,'run':39956630843,'sha256':'bb7025612994c30f4d143f3460089316ed2d97a868506481ac2fc4f567dc9fc9'},
 'after': {'id':12514050084,'run':39980702510,'sha256':'04eaaf6d2a8309e51aaa32a20096dbb2f42125e93501707a40cd280ea4c9a6c3'},
}
SRC = {
 'trust': ('realfull','reports/real_full_trust_r1/real_full_trust_selection_ledger.csv'),
 'overlay': ('realfull','reports/real_full_research_overlay_r1/real_full_research_overlay.csv'),
 'r143': ('r143','real_full_r1c1_r143_outcome_ledger.csv'),
 'odoli': ('odoli','odoli_independent_r1/odoli_events.csv'),
 'odoli_status': ('odoli','odoli_independent_r1/odoli_current_status.csv'),
 'after': ('after','odoli_after_session_history.csv'),
}
HORIZONS = (1,3,5,10)
QA_ABS_RETURN_PCT = 40.0  # diagnostic flag only, never filtering signals or ranking
QA_CROSS_SOURCE_DIFF_PP = 10.0  # mismatch flag, not a trading parameter


def fail(msg): raise RuntimeError('R1D10_FAIL_CLOSED_'+str(msg))


def ticker(v):
 """Preserve the original 6-character Korean code including capital letters."""
 s=str(v).strip().upper()
 if not re.fullmatch(r'[0-9A-Z]{6}',s):fail('BAD_OR_LOSSY_CODE_'+s[:50])
 return s


def digest_file(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def read_csv(p):
 if not Path(p).is_file():fail('MISSING_CSV_'+str(p))
 return pd.read_csv(p,dtype=str,low_memory=False,encoding='utf-8-sig')


def load_sources(paths,root):
 """paths are four extracted GitHub artifact folders or exact pinned ZIP files."""
 frames={}
 for key,(stream,name) in SRC.items():
  p=Path(paths[stream]);
  if p.is_file() and zipfile.is_zipfile(p):
   if digest_file(p)!=PIN[stream]['sha256']:fail('ARTIFACT_DIGEST_DRIFT_'+stream)
   with zipfile.ZipFile(p) as z:
    try: raw=z.read(name)
    except KeyError:fail('FILE_MISSING_IN_ZIP_'+name)
   frames[key]=pd.read_csv(io.BytesIO(raw),dtype=str,low_memory=False,encoding='utf-8-sig')
  elif p.is_dir():frames[key]=read_csv(p/name)
  else:fail('INVALID_ARTIFACT_LOCATION_'+stream)
 for key,df in frames.items():
  if df.empty:fail('EMPTY_SOURCE_'+key)
 return frames


def numeric(x):return pd.to_numeric(x,errors='coerce')


def bool_complete(s):return s.astype(str).str.lower().isin(('1','true','yes'))


def ensure_key(df,keys,source,dup=False):
 for k in keys:
  if k not in df.columns:fail('MISSING_COLUMN_'+source+'_'+k)
 if df[keys].isna().any().any():fail('MISSING_KEY_'+source)
 if not dup and df.duplicated(keys).any():fail('DUPLICATE_KEY_'+source)


def safe_table(df,path):
 path.parent.mkdir(parents=True,exist_ok=True)
 df.to_csv(path,index=False,encoding='utf-8-sig')


def trust_horizon(trust):
 outcomes=[]
 for h in HORIZONS:
  ret=numeric(trust[f'd{h}_close_ret_pct']);mfe=numeric(trust[f'd{h}_mfe_pct']);mae=numeric(trust[f'd{h}_mae_pct']);valid=bool_complete(trust[f'd{h}_complete']) & ret.notna()
  vals=ret[valid]
  outcomes.append({'horizon':h,'n_events':len(trust),'n_complete':int(valid.sum()),
                   'median_close_pct':float(vals.median()),'mean_close_pct_raw':float(vals.mean()),
                   'pct_mfe_ge5':float(100*(mfe[valid]>=5).mean()),
                   'pct_mae_le_minus5':float(100*(mae[valid]<=-5).mean()),
                   'pct_abs_close_gt40':float(100*(vals.abs()>QA_ABS_RETURN_PCT).mean()),
                   'data_quality_notice':'UNADJUSTED_AND_IDENTITY_NOT_YET_CERTIFIED'})
 return pd.DataFrame(outcomes)


def provenance_timing(trust):
 a=trust[['snapshot_date','first_observed_at_kst','capture_slot']].drop_duplicates().copy()
 a['first_observed_parsed']=pd.to_datetime(a['first_observed_at_kst'],errors='coerce',utc=True)
 if a.first_observed_parsed.isna().any():fail('INVALID_FIRST_OBSERVED_TIME')
 # Asia/Seoul is the explicit provenance timezone; captured TIME is not signal-frozen TIME.
 a['local_time']=a.first_observed_parsed.dt.tz_convert('Asia/Seoul')
 a['observed_after_1530']=a.local_time.dt.hour*60+a.local_time.dt.minute>=(15*60+30)
 days=a.sort_values('local_time').groupby('snapshot_date').first().reset_index()
 return days[['snapshot_date','first_observed_at_kst','capture_slot','observed_after_1530']]


def analyze(trust,overlay,r143,odoli,odoli_status,after,out):
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 ensure_key(trust,['snapshot_date','code'],'TRUST')
 ensure_key(overlay,['code'],'OVERLAY')
 ensure_key(odoli,['signal_date','code'],'ODOLI')
 ensure_key(after,['signal_date','code'],'AFTER')
 ensure_key(odoli_status,['signal_date','code'],'ODOLI_STATUS')
 ensure_key(r143,['signal_date','code','horizon_td'],'R143')
 for name,d,column in [('TRUST',trust,'code'),('R143',r143,'code'),('ODOLI',odoli,'code'),('AFTER',after,'code'),('OVERLAY',overlay,'code')]:
  # Exact identity verification; no digits-only aliases anywhere in JOIN logic.
  try:d[column]=d[column].map(ticker)
  except RuntimeError:raise
  if not d[column].map(lambda s:len(s)==6).all():fail('CODE_LOSS_'+name)

 horizon=trust_horizon(trust);safe_table(horizon,out/'REALFULL_TRUST_HORIZONS.csv')
 dates=provenance_timing(trust);safe_table(dates,out/'REALFULL_CAPTURE_TIME_AUTHORITY.csv')

 # Keep source values unchanged; outliers are observations requiring QA, not automatic corrections.
 odd=[]
 for h in HORIZONS:
  r=numeric(trust[f'd{h}_close_ret_pct']);sub=trust.loc[bool_complete(trust[f'd{h}_complete'])&r.abs().gt(QA_ABS_RETURN_PCT),['snapshot_date','code','name','snapshot_price']].copy()
  sub['horizon']=h;sub['raw_close_ret_pct']=r.loc[sub.index];sub['qa_status']='REVIEW_SOURCE_IDENTITY_OR_CORPORATE_ACTION_NO_AUTO_FIX';odd.append(sub)
 odd=pd.concat(odd,ignore_index=True);safe_table(odd,out/'REALFULL_OUTLIER_QA_ONLY.csv')

 # Paired horizon event-level, preventing selection from changing as each horizon matures.
 p5=numeric(trust['d5_close_ret_pct']);p10=numeric(trust['d10_close_ret_pct']);mfe5=numeric(trust['d5_mfe_pct'])
 ready=bool_complete(trust['d5_complete']) & bool_complete(trust['d10_complete']) & p5.notna() & p10.notna()
 paired=trust.loc[ready,['snapshot_date','code','name','capture_slot']].copy();paired['d5_close_ret_pct']=p5[ready];paired['d10_close_ret_pct']=p10[ready];paired['d5_mfe_pct']=mfe5[ready]
 paired['d10_minus_d5_pp']=paired.d10_close_ret_pct-paired.d5_close_ret_pct
 paired['had_d5_mfe5_and_d10_close_below0']=paired.d5_mfe_pct.ge(5)&paired.d10_close_ret_pct.lt(0)
 paired['qa_abs_ret_gt40']=paired[['d5_close_ret_pct','d10_close_ret_pct']].abs().gt(QA_ABS_RETURN_PCT).any(axis=1)
 safe_table(paired,out/'REALFULL_PAIRED_D5_D10_GIVEBACK.csv')

 # R143 is an independent shadow definition. Same code, same date, same horizon ONLY.
 j=[]
 for h in HORIZONS:
  c=trust.loc[bool_complete(trust[f'd{h}_complete']),['snapshot_date','code','name',f'd{h}_close_ret_pct']].copy()
  c=c.rename(columns={'snapshot_date':'signal_date',f'd{h}_close_ret_pct':'trust_ret_pct'})
  r=r143[r143['horizon_td'].eq(str(h))][['signal_date','code','close_ret_pct']].rename(columns={'close_ret_pct':'r143_ret_pct'})
  merge=c.merge(r,on=['signal_date','code'],validate='one_to_one')
  merge['horizon']=h;merge['trust_ret_pct']=numeric(merge.trust_ret_pct);merge['r143_ret_pct']=numeric(merge.r143_ret_pct)
  merge['abs_diff_pp']=(merge.trust_ret_pct-merge.r143_ret_pct).abs()
  merge['qa_mismatch_gt10pp']=merge.abs_diff_pp.gt(QA_CROSS_SOURCE_DIFF_PP)
  j.append(merge)
 cross=pd.concat(j,ignore_index=True);safe_table(cross,out/'REALFULL_R143_SAME_ID_RETURNS_QA.csv')

 # Detect name-and-price agreement but different exact security identity; NEVER join these for performance.
 a=trust[['snapshot_date','code','name','snapshot_price']].rename(columns={'snapshot_date':'signal_date','code':'trust_code','snapshot_price':'trust_snapshot_price'})
 b=r143[['signal_date','code','name','entry_price']].drop_duplicates(['signal_date','code']).rename(columns={'code':'r143_code','entry_price':'r143_entry_price'})
 namejoin=a.merge(b,on=['signal_date','name'],how='inner',validate='many_to_many')
 namejoin=namejoin[namejoin.trust_code.ne(namejoin.r143_code)].copy()
 namejoin['price_same']=np.isclose(numeric(namejoin.trust_snapshot_price),numeric(namejoin.r143_entry_price),rtol=0,atol=0)
 namejoin['qa_status']='EXACT_TICKER_MISMATCH_DO_NOT_AUTO_ALIAS'
 safe_table(namejoin,out/'REALFULL_ALPHANUMERIC_IDENTITY_COLLISIONS.csv')

 # Repetition and the overlay's point-in-time limit.
 t=trust.sort_values(['snapshot_date','rank']).copy();t['first_signal_of_code']=~t.duplicated('code')
 t['d10_valid']=bool_complete(t.d10_complete)&numeric(t.d10_close_ret_pct).notna();t['d10_ret']=numeric(t.d10_close_ret_pct)
 reps=[]
 for x,q in [('FIRST',t[t.first_signal_of_code]),('REPEATED',t[~t.first_signal_of_code])]:
  done=q[q.d10_valid]
  reps.append({'group':x,'n_all':len(q),'n_d10_complete':len(done),'d10_median_pct':float(done.d10_ret.median()) if len(done) else None})
 safe_table(pd.DataFrame(reps),out/'REALFULL_FIRST_VS_REPEAT_DESCRIPTIVE.csv')

 # AFTER availability: denominators are the AFTER frozen cohort itself, never whole ODOLI historical population.
 aft=after.copy();aft['covered']=bool_complete(aft.after_has_data) & numeric(aft.after_ret_from_krx_close_pct).notna()
 cover=aft.groupby('signal_date',as_index=False).agg(n_cohort=('code','size'),n_covered=('covered','sum'))
 cover['coverage_pct']=100*cover.n_covered/cover.n_cohort
 safe_table(cover,out/'ODOLI_AFTER_COVERAGE_BY_DATE.csv')
 aft=aft.merge(odoli[['signal_date','code','close','market']].rename(columns={'close':'odoli_krx_close','market':'odoli_market'}),on=['signal_date','code'],how='left',validate='one_to_one',indicator=True)
 aft['reference_match']=aft._merge.eq('both');aft=aft.drop(columns='_merge')
 aft['after_source_krx_equal']=np.isclose(numeric(aft.krx_close),numeric(aft.odoli_krx_close),rtol=0,atol=1e-8)
 safe_table(aft[['signal_date','code','covered','after_source','after_error','reference_match','after_source_krx_equal']],out/'ODOLI_AFTER_SOURCE_COVERAGE_QA.csv')

 # After D+5 only for 2026-10-16, as-of 2026-10-23 and day==5; no fake prospective D5 10/23 values.
 status=odoli_status[['signal_date','code','day','current_close_ret_pct']].copy(); status['day_num']=numeric(status.day)
 dd=aft.merge(status,on=['signal_date','code'],how='left',validate='one_to_one')
 dd=dd[(dd.signal_date=='2026-10-16') & dd.day_num.eq(5) & numeric(dd.current_close_ret_pct).notna()].copy()
 dd['d5_ret_pct']=numeric(dd.current_close_ret_pct); dd['after_pct']=numeric(dd.after_ret_from_krx_close_pct)
 safe_table(dd[['signal_date','code','covered','after_pct','d5_ret_pct']],out/'ODOLI_AFTER_SINGLE_DATE_D5_AUDIT.csv')
 d5_summary=[]
 for value,g in dd.groupby('covered'):
  d5_summary.append({'signal_date':'2026-10-16','covered':bool(value),'n':len(g),'d5_median_pct':float(g.d5_ret_pct.median())})
 safe_table(pd.DataFrame(d5_summary),out/'ODOLI_AFTER_SINGLE_DATE_D5_GROUPS.csv')

 overlay_date=trust.snapshot_date.max()
 reasons=overlay.research_level.fillna('').value_counts().to_dict()
 meta={
  'version':VERSION,'RESEARCH_ONLY':True,'orders_placed':0,'live_logic_changed':False,
  'source_pins':PIN,'trust_locked_events':len(trust),'trust_dates':int(trust.snapshot_date.nunique()),
  'trust_unique_codes':int(trust.code.nunique()),'trust_first_observed_after1530_days':int(dates.observed_after_1530.sum()),
  'trust_d10_complete_paired_d5':len(paired),'trust_d5_median_paired':float(paired.d5_close_ret_pct.median()),
  'trust_d10_median_paired':float(paired.d10_close_ret_pct.median()),'trust_d10_minus_d5_median':float(paired.d10_minus_d5_pp.median()),
  'trust_d5_mfe5_to_d10_below0_n':int(paired.had_d5_mfe5_and_d10_close_below0.sum()),
  'trust_d5_mfe5_to_d10_below0_denominator':len(paired),
  'trust_abs_gt40_qa_rows':len(odd),'r143_matching_keys_outcome_rows':len(cross),
  'r143_return_abs_diff_gt10pp':int(cross.qa_mismatch_gt10pp.sum()),
  'name_same_ticker_different_pairs':int(len(namejoin)),
  'overlay_snapshot_only':True,'overlay_n':len(overlay),'overlay_asof_trust_date':overlay_date,
  'overlay_levels_snapshot_counts':reasons,'no_historical_overlay_level_success_rates':True,
  'odoli_n_events':len(odoli),'odoli_after_n_observations':len(aft),'odoli_after_n_covered':int(aft.covered.sum()),
  'odoli_after_reference_not_matched':int((~aft.reference_match).sum()),
  'odoli_after_krx_close_mismatch':int((aft.reference_match&~aft.after_source_krx_equal).sum()),
  'odoli_after_d5_same_date_n':len(dd),'odoli_after_d5_same_date_covered_n':int(dd.covered.sum()),
  'after_NOT_used_in_gate_score_rank_order':True,
  'caveats':['separate historical populations, no causal inference','raw/partly unadjusted corporate actions may affect returns',
             'same-day AFTER market observation missing not random','R1D9 2021-23 and R1D6 2024-25 are disjoint historical periods, NOT prospective'],
 }
 (out/'R1D10_MANIFEST.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
 return meta


def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__)
 for k in PIN:p.add_argument('--'+k,required=True)
 p.add_argument('--out',required=True)
 a=p.parse_args(argv)
 meta=analyze(**load_sources({k:getattr(a,k) for k in PIN},a.out),out=a.out)
 print('R1D10_ACCUMULATED_PROVENANCE_AUDIT_PASS',json.dumps({k:meta[k] for k in ('trust_locked_events','trust_dates','trust_d10_complete_paired_d5','r143_return_abs_diff_gt10pp','odoli_after_n_covered','orders_placed')},ensure_ascii=False),flush=True)
 return 0
if __name__=='__main__':raise SystemExit(main())
