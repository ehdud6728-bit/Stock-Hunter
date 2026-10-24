#!/usr/bin/env python3
"""R1-D11 RESEARCH_ONLY: frozen R1-D10 data QA plus date/code clustered Giveback sensitivity.

Historical observations ONLY. No search, scoring, rank, gate, orders, return corrections,
threshold tuning, condition promotion or LIVE interfaces. No current data acquisition.
"""
from __future__ import annotations
import argparse, hashlib, io, json, math, re, zipfile
from pathlib import Path
import numpy as np
import pandas as pd

VERSION='STOCKHUNTER_R1D11_FROZEN_SOURCE_QUALITY_GIVEBACK_V1'
RESEARCH_ONLY=True
PIN={
 'r1d10':{'run':40074113253,'head':'2d8aa4d1c0c0097d798d4ff028474ac8c0cde982','id':12551784748,'sha256':'dc01ee35eca74197e6d95511c659bce92c975746f413f6a483c9604f8243a1bd'},
 'trust':{'run':39948786391,'head':'ae74ffb655aea226ecbfa0e315b7c46077b3c458','id':12504190178,'sha256':'3d2ee08727e50b248cdf7c41be7fb21744d2f9ee908747e693895c4d443ab34b'},
}
ROOTS={'r1d10':'reports/r1d10/result/','trust':'reports/real_full_trust_r1/'}
CLEAN_QA_ABS40=40.0   # diagnostic sensitivity ONLY; NOT automatically incorrect returns
CROSS_MISMATCH10=10.0 # diagnostic sensitivity ONLY; NOT a trading threshold
HORIZONS=(5,10)
SEED=20261024
BOOTSTRAP=500


def fail(tag):raise RuntimeError('R1D11_FAIL_CLOSED_'+str(tag))
def sha_file(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def ticker(v):
 s=str(v)
 if not re.fullmatch(r'[0-9A-Z]{6}',s):fail('SECURITY_IDENTITY_NOT_EXACT_'+s[:50])
 return s

def as_bool(s):return s.astype(str).str.lower().isin(('true','1','yes'))
def num(s):return pd.to_numeric(s,errors='coerce')
def read_source(path,stream,name):
 p=Path(path)
 if p.is_file() and zipfile.is_zipfile(p):
  if sha_file(p)!=PIN[stream]['sha256']:fail('FROZEN_ZIP_DIGEST_CHANGED_'+stream)
  with zipfile.ZipFile(p) as z:
   try:blob=z.read(ROOTS[stream]+name)
   except KeyError:fail('REQUIRED_SOURCE_FILE_MISSING_'+stream+'_'+name)
  return pd.read_csv(io.BytesIO(blob),dtype=str,encoding='utf-8-sig',low_memory=False)
 if p.is_dir():
  f=p/ROOTS[stream]/name
  if not f.is_file():fail('REQUIRED_SOURCE_FILE_MISSING_'+stream+'_'+name)
  return pd.read_csv(f,dtype=str,encoding='utf-8-sig',low_memory=False)
 fail('INPUT_NOT_ZIP_OR_DIRECTORY_'+stream)


def load_meta(path,stream,name):
 p=Path(path)
 if p.is_file() and zipfile.is_zipfile(p):
  if sha_file(p)!=PIN[stream]['sha256']:fail('FROZEN_ZIP_DIGEST_CHANGED_'+stream)
  with zipfile.ZipFile(p) as z:
   try:return json.loads(z.read(ROOTS[stream]+name))
   except KeyError:fail('MANIFEST_NOT_FOUND_'+name)
 if p.is_dir():
  f=p/ROOTS[stream]/name
  if not f.is_file():fail('MANIFEST_NOT_FOUND_'+name)
  return json.loads(f.read_text(encoding='utf-8'))
 fail('MANIFEST_INPUT_MISSING_'+stream)


def unique(df,cols,label):
 if any(c not in df for c in cols):fail('MISSING_KEY_COLUMN_'+label)
 if df[cols].isna().any().any() or df.duplicated(cols).any():fail('INVALID_DUPLICATE_OR_NULL_KEY_'+label)

def protocol_guard(protocol):
 p=Path(protocol)
 if not p.is_file():fail('MISSING_PREDECLARED_PROTOCOL')
 x=json.loads(p.read_text('utf-8'))
 if x.get('version')!=VERSION or x.get('fixed_source_pins')!=PIN or x.get('raw_research_only') is not True or x.get('qa_abs_pct')!=40.0 or x.get('qa_diff_pp')!=10.0 or x.get('bootstrap')!=BOOTSTRAP or x.get('seed')!=SEED or x.get('horizons')!=list(HORIZONS):
  fail('PREDECLARED_PROTOCOL_MISMATCH')
 return x


def stat(v):
 a=pd.to_numeric(v,errors='coerce').dropna()
 return float(a.median()) if len(a) else None

def date_code_bootstrap(df,column,group=None,n=BOOTSTRAP,seed=SEED,cluster='snapshot_date'):
 """Fixed seeded descriptive resampling by entire DATE or CODE cluster; no inference of causality."""
 if df.empty:return (None,None)
 values=[]
 groups=[g for _,g in df.groupby(cluster,sort=True)]
 if len(groups)<2:return (None,None)
 rng=np.random.default_rng(seed)
 for i in range(n):
  picks=rng.integers(0,len(groups),size=len(groups))
  draw=pd.concat([groups[k] for k in picks],ignore_index=True)
  if group is None:target=num(draw[column]);v=target.median() if target.notna().any() else np.nan
  else:
   a=num(draw.loc[draw.first_of_code,column]);b=num(draw.loc[~draw.first_of_code,column])
   v=b.median()-a.median() if len(a) and len(b) else np.nan
  if np.isfinite(v):values.append(v)
 if len(values)<n//2:return(None,None)
 return tuple(round(float(x),6) for x in np.quantile(values,[.025,.975]))


def compute(paired,trust,cross,aliases,outlier,capture,after,manifest,protocol_path,out):
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 if manifest.get('trust_locked_events')!=360 or manifest.get('trust_d10_complete_paired_d5')!=199 or manifest.get('odoli_after_n_observations')!=592:fail('R1D10_SOURCE_POPULATION_CHANGED')
 if len(trust)!=360 or len(paired)!=199 or len(cross)!=462 or len(capture)!=24:fail('FROZEN_SOURCE_ROWS_CHANGED')
 for df,keys,label in [(trust,['snapshot_date','code'],'TRUST'),(paired,['snapshot_date','code'],'PAIRED'),(cross,['signal_date','code','horizon'],'CROSS'),(after,['signal_date','code'],'AFTER')]:
  unique(df,keys,label)
  for v in df.code:ticker(v)
 for df,col in [(aliases,'trust_code'),(aliases,'r143_code'),(outlier,'code')]:
  if col not in df:fail('BAD_SOURCE_SCHEMA_'+col)
  for v in df[col]:ticker(v)
 dates=trust[['snapshot_date','code','rank','d5_complete','d10_complete','d5_close_ret_pct','d10_close_ret_pct']].copy()
 dates['rank_num']=num(dates['rank'])
 if dates.rank_num.isna().any():fail('RANK_INVALID')
 dates=dates.sort_values(['snapshot_date','rank_num','code'])
 dates['first_of_code']=~dates.duplicated(['code'])
 p=paired.merge(dates[['snapshot_date','code','first_of_code','d5_complete','d10_complete','d5_close_ret_pct','d10_close_ret_pct']],on=['snapshot_date','code'],how='left',validate='one_to_one',suffixes=('','_raw'))
 if len(p)!=199 or p.first_of_code.isna().any():fail('PAIR_NOT_IN_IMMUTABLE_TRUST')
 if not as_bool(p.d5_complete).all() or not as_bool(p.d10_complete).all():fail('PAIRED_HORIZONS_NOT_COMPLETE')
 for field in ['d5_close_ret_pct','d10_close_ret_pct']:
  if not np.allclose(num(p[field]),num(p[field+'_raw']),atol=1e-9,rtol=0):fail('SOURCE_RETURN_CHANGED_'+field)
 p['qa_abs40']=p[['d5_close_ret_pct','d10_close_ret_pct']].apply(num).abs().gt(CLEAN_QA_ABS40).any(axis=1)
 if 'qa_abs_ret_gt40' not in p or not p.qa_abs40.eq(as_bool(p.qa_abs_ret_gt40)).all():fail('PREVIOUS_QA_FLAG_DRIFT')
 cm=cross[as_bool(cross.qa_mismatch_gt10pp)].copy()
 if len(cm)!=4 or any(num(cm.abs_diff_pp).le(CROSS_MISMATCH10)):fail('FROZEN_CROSS_SOURCE_QA_CHANGED')
 # Any cross-source discrepancy for the same DATE,CODE is propagated; not a ticker correction.
 bad_key=set(map(tuple,cm[['signal_date','code']].itertuples(index=False,name=None)))
 identity_key=set(map(tuple,aliases[['signal_date','trust_code']].itertuples(index=False,name=None)))
 p['qa_cross_mismatch_any_horizon']=[(d,c) in bad_key for d,c in zip(p.snapshot_date,p.code)]
 p['qa_original_ticker_collision']=[(d,c) in identity_key for d,c in zip(p.snapshot_date,p.code)]
 p['qa_flag_any']=p[['qa_abs40','qa_cross_mismatch_any_horizon','qa_original_ticker_collision']].any(axis=1)
 p['d10_minus_d5_pp']=num(p.d10_close_ret_pct)-num(p.d5_close_ret_pct)
 p['giveback_from_5mfe_to_d10_below0']=num(p.d5_mfe_pct).ge(5)&num(p.d10_close_ret_pct).lt(0)
 p['qa_explanation']='QA_FLAG_NOT_AUTOMATIC_BAD_FILL_OR_BAD_TRADE'
 # SOURCE ORIGINAL and QA FLAG CLEAR are *both* exported; second is only sensitivity.
 p.to_csv(out/'R1D11_EXACT_IDENTITY_PAIRED_PATH_FLAGS.csv',index=False,encoding='utf-8-sig')
 panel=[]
 cohorts=[('SOURCE_ALL_UNMODIFIED',p),('QA_FLAG_CLEAR_SENSITIVITY',p[~p.qa_flag_any])]
 for label,x in cohorts:
  for gl,sub in [('ALL',x),('FIRST_CODE',x[x.first_of_code]),('REPEATED_CODE',x[~x.first_of_code])]:
   n=len(sub);count=int(sub.giveback_from_5mfe_to_d10_below0.sum());mfe5=num(sub.d5_mfe_pct).ge(5)
   passed=int(mfe5.sum());negs=int(sub.loc[mfe5,'giveback_from_5mfe_to_d10_below0'].sum())
   ci=date_code_bootstrap(sub,'d10_minus_d5_pp',seed=SEED+len(gl)+(1 if label.startswith('QA') else 0),cluster='snapshot_date') if len(sub)>1 else (None,None)
   panel.append({'cohort':label,'subgroup':gl,'n_events':n,'n_dates':int(sub.snapshot_date.nunique()),'n_codes':int(sub.code.nunique()),'d5_ret_median_pct':stat(sub.d5_close_ret_pct),'d10_ret_median_pct':stat(sub.d10_close_ret_pct),'paired_d10_minus_d5_median_pp':stat(sub.d10_minus_d5_pp),'date_cluster_resample_median_delta_lo_pp':ci[0],'date_cluster_resample_median_delta_hi_pp':ci[1],'d5_mfe_ge5_n':passed,'d5_mfe_ge5_to_d10_negative_n':negs,'d5_mfe_ge5_to_d10_negative_pct':100*negs/passed if passed else None,'QA_NOT_STRATEGY_FILTER':True})
 summary=pd.DataFrame(panel);summary.to_csv(out/'R1D11_GIVEBACK_QA_SENSITIVITY_SCORECARD.csv',index=False,encoding='utf-8-sig')
 match=[]
 for lab,x in cohorts:
  a=x.loc[x.first_of_code];b=x.loc[~x.first_of_code]
  date_mixed=[]
  for day,g in x.groupby('snapshot_date',sort=True):
   first=g[g.first_of_code];repeat=g[~g.first_of_code]
   if len(first) and len(repeat):date_mixed.append({'snapshot_date':day,'first_n':len(first),'repeat_n':len(repeat),'repeated_minus_first_d10_median_pp':stat(repeat.d10_close_ret_pct)-stat(first.d10_close_ret_pct)})
  f=pd.DataFrame(date_mixed)
  ci_date=date_code_bootstrap(x,'d10_close_ret_pct',group='diff',cluster='snapshot_date',seed=SEED)
  ci_code=date_code_bootstrap(x,'d10_close_ret_pct',group='diff',cluster='code',seed=SEED+1)
  match.append({'cohort':lab,'first_n':len(a),'repeat_n':len(b),'repeat_minus_first_median_d10_pp':stat(b.d10_close_ret_pct)-stat(a.d10_close_ret_pct),'date_resample_lo_pp':ci_date[0],'date_resample_hi_pp':ci_date[1],'code_resample_lo_pp':ci_code[0],'code_resample_hi_pp':ci_code[1],'same_date_comparable_dates':len(f),'same_date_equal_weighted_repeat_minus_first_median_pp':stat(f.repeated_minus_first_d10_median_pp) if not f.empty else None,'OBSERVATIONAL_NOT_CAUSAL':True})
  f.to_csv(out/('R1D11_SAME_DATE_FIRST_REPEAT_'+lab+'.csv'),index=False,encoding='utf-8-sig')
 pd.DataFrame(match).to_csv(out/'R1D11_REPEAT_DATE_CODE_CLUSTER_SENSITIVITY.csv',index=False,encoding='utf-8-sig')
 qa=pd.DataFrame([
  {'check':'PAIRED_EVENTS','n':len(p),'description':'Original sealed D5/D10 pairs unchanged'},
  {'check':'ABS40_ANY_D5_D10','n':int(p.qa_abs40.sum()),'description':'Diagnostic extreme returns; MAY be legitimate'},
  {'check':'CROSS_SOURCE_GT10PP_ORIGINAL_ROWS','n':len(cm),'description':'Same security/date/horizon return disagreement'},
  {'check':'CROSS_SOURCE_GT10PP_UNIQUE_EVENT','n':len(bad_key),'description':'Propagate issue to all affected horizons'},
  {'check':'ALPHANUMERIC_IDENTITY_COLLISION_ORIGINAL_ROWS','n':len(aliases),'description':'No auto-alias of 0088M0 to 000880'},
  {'check':'QA_FLAGGED_PAIRED_EVENTS_UNION','n':int(p.qa_flag_any.sum()),'description':'Never remove from SOURCE_ALL_UNMODIFIED'},
  {'check':'QA_FLAG_CLEAR_PAIRED_EVENTS','n':int((~p.qa_flag_any).sum()),'description':'Sensitivity only; not proof of valid pricing'},
  {'check':'CAPTURE_DAYS_POST_1530','n':int(as_bool(capture.observed_after_1530).sum()),'description':'Capture time is not necessarily signal-generation time'},
  {'check':'AFTER_TOTAL','n':len(after),'description':'ODOLI AFTER separate population'},
  {'check':'AFTER_COVERED','n':int(as_bool(after.covered).sum()),'description':'Missing AFTER stays missing'},
 ])
 qa.to_csv(out/'R1D11_IMMUTABLE_SOURCE_QA_COUNTS.csv',index=False,encoding='utf-8-sig')
 manifest_out={'version':VERSION,'research_only':True,'n_paired':len(p),'n_qa_flagged_union':int(p.qa_flag_any.sum()),'n_qa_flag_clear':int((~p.qa_flag_any).sum()),'n_first':int(p.first_of_code.sum()),'n_repeat':int((~p.first_of_code).sum()),'source_pins':PIN,'protocol_sha256':sha_file(protocol_path),'old_r1d10_manifest_sha256':None,'labels_are_research_only':True,'qa_sensitivity_not_source_correction':True,'not_new_OOS':True,'no_224ma_gate_change':True,'no_live_code_change':True,'orders_placed':0,'after_coverage_not_predictive':True,'not_tradeability_certified':True}
 (out/'R1D11_MANIFEST.json').write_text(json.dumps(manifest_out,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')
 print('R1D11_READONLY_QA_FINISHED',json.dumps({k:manifest_out[k] for k in ('n_paired','n_qa_flagged_union','n_first','n_repeat','orders_placed')},ensure_ascii=False),flush=True)
 return manifest_out


def main(argv=None):
 ap=argparse.ArgumentParser(description=__doc__)
 ap.add_argument('--r1d10',required=True);ap.add_argument('--trust',required=True);ap.add_argument('--protocol',default='R1D11_PREDECLARED_PROTOCOL.json');ap.add_argument('--out',required=True)
 a=ap.parse_args(argv);protocol_guard(a.protocol)
 file=lambda n:read_source(a.r1d10,'r1d10',n)
 trust=read_source(a.trust,'trust','real_full_trust_selection_ledger.csv')
 compute(file('REALFULL_PAIRED_D5_D10_GIVEBACK.csv'),trust,file('REALFULL_R143_SAME_ID_RETURNS_QA.csv'),file('REALFULL_ALPHANUMERIC_IDENTITY_COLLISIONS.csv'),file('REALFULL_OUTLIER_QA_ONLY.csv'),file('REALFULL_CAPTURE_TIME_AUTHORITY.csv'),file('ODOLI_AFTER_SOURCE_COVERAGE_QA.csv'),load_meta(a.r1d10,'r1d10','R1D10_MANIFEST.json'),a.protocol,a.out)
 return 0
if __name__=='__main__':raise SystemExit(main())
