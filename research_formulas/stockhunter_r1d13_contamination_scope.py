#!/usr/bin/env python3
"""R1-D13 frozen source / price contamination SENSITIVITY audit, RESEARCH_ONLY.

This module reads only finished pinned artifacts, NEVER repairs old data or updates
LIVE/production inputs. Price-cache source files are NOT available in these artifacts;
any alleged price fix is strictly forbidden here.
"""
from __future__ import annotations
import argparse, hashlib, io, json, math, re, zipfile
from pathlib import Path
import numpy as np
import pandas as pd

VERSION='STOCKHUNTER_R1D13_FROZEN_PROVENANCE_CONTAMINATION_SCOPE_V1'
PIN={
 'trust':dict(run=39948786391,id=12504190178,name='stockhunter-v1107-4-5-73-3-6-6-24-causal-universe-full-denom-pattern-oos-exit-reports-1681',sha256='3d2ee08727e50b248cdf7c41be7fb21744d2f9ee908747e693895c4d443ab34b',head='ae74ffb655aea226ecbfa0e315b7c46077b3c458'),
 'r143':dict(run=39960213718,id=12504990509,name='real-full-r143-path-integrity-39960213718',sha256='a1ba01eea0e1976e9edd59ec6b548d2d56f897e7638498dc4185b66b284edbbf',head='3b8321a2e48c98a49f93ada68ba64e92e072d6ed'),
 'r1d12':dict(run=40084169858,id=12556616388,name='stockhunter-r1d12-FROZEN-IDENTITY-PRICE-PATH-ROOTCAUSE-NOT-OOS',sha256='21ec907817f145126681bcf7d10dcce381913779db6f63e26052dea854304fbe',head='044a3c04dc4786baffe8e3bb24862e4df1168075')
}
SRC_BLOBS={
 'real_full_capture_runner.py':'6167746fe363961919a70e5ec206861bfaec465c',
 'real_full_trust_audit.py':'b18d67a13a1d08076b6c0583272939c06ed508af',
 'real_full_r1c1_path_integrity_r143.py':'fe7fba1e26f2a952aeff54b07230c888547e03ef',
}
HORIZONS=(1,3,5,10)

def fail(msg):raise RuntimeError('R1D13_FAIL_CLOSED_'+msg)

def exact_ticker(value):
 s=str(value).strip().upper()
 if not re.fullmatch('[0-9A-Z]{6}',s):fail('SECURITY_ID_'+s[:40])
 return s

def lossy_legacy(value):
 s=re.sub(r'\D','',str(value))
 return s[-6:].zfill(6) if s else ''

def sha256(data):return hashlib.sha256(data).hexdigest()

def read_data(root,suffix):
 p=Path(root)
 if p.is_dir():
  candidates=[x for x in p.rglob('*') if x.is_file() and x.as_posix().endswith(suffix)]
  if len(candidates)!=1:fail('INPUT_NOT_UNIQUE_'+suffix)
  b=candidates[0].read_bytes()
 elif p.is_file() and zipfile.is_zipfile(p):
  with zipfile.ZipFile(p) as z:
   names=[n for n in z.namelist() if n.endswith(suffix)]
   if len(names)!=1:fail('ARCHIVE_INPUT_NOT_UNIQUE_'+suffix)
   b=z.read(names[0])
 else:fail('INPUT_ROOT_UNAVAILABLE_'+str(root))
 return pd.read_csv(io.BytesIO(b),dtype=str,keep_default_na=False,low_memory=False)

def git_blob_sha(data):
 return hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()

def check_source_blobs(root):
 out=[]
 for name,required in SRC_BLOBS.items():
  f=Path(root)/name
  if not f.is_file():fail('MISSING_STATIC_SOURCE_'+name)
  current=git_blob_sha(f.read_bytes())
  if current!=required:fail('FROZEN_GIT_BLOB_CHANGED_'+name)
  out.append(dict(source=name,source_git_blob=current,source_unmodified=True))
 return pd.DataFrame(out)

def protocol_guard(repo,protocol):
 b=Path(protocol)
 if not b.is_file():fail('PREDECLARED_PROTOCOL_MISSING')
 p=json.loads(b.read_text(encoding='utf-8'))
 if p.get('version')!=VERSION or p.get('pins')!=PIN or p.get('source_git_blobs')!=SRC_BLOBS:fail('PROTOCOL_LOCK_MISMATCH')
 if not p.get('research_only') or not p.get('no_live_or_source_changes') or not p.get('no_corrections'):
  fail('NO_TRADE_CONTRACT')
 check_source_blobs(repo)
 return p

def num(s):return pd.to_numeric(s,errors='coerce')

def yes(s):return s.astype(str).str.strip().str.lower().isin(['1','true','yes','y'])

def med(s):
 x=num(s).dropna();return float(x.median()) if len(x) else None

def metric(row, h):
 return num(row[f'd{h}_close_ret_pct'])

def run(trust_root,r143_root,r1d12_root,repo_root,protocol,out):
 p=protocol_guard(repo_root,protocol)
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 trust=read_data(trust_root,'real_full_trust_selection_ledger.csv')
 r143=read_data(r143_root,'real_full_r1c1_r143_outcome_ledger.csv')
 d12=read_data(r1d12_root,'R1D12_EXACT_TICKER_HORIZON_PRICE_RATIO_DISCREPANCIES.csv')
 collision=read_data(r1d12_root,'R1D12_ALPHANUMERIC_IDENTITY_PRESERVATION_PROOF.csv')
 if (len(trust),len(r143),len(d12),len(collision))!=(360,705,4,4):fail('FROZEN_POPULATION_CHANGED')
 for label,data,datekey in [('TRUST',trust,'snapshot_date'),('R143',r143,'signal_date')]:
  data['code']=data['code'].map(exact_ticker)
  if not data[datekey].str.fullmatch(r'\d{4}-\d{2}-\d{2}').all():fail(label+'_DATE_FORMAT')
 if trust.duplicated(['snapshot_date','code']).any() or r143.duplicated(['signal_date','code','horizon_td']).any():fail('DUPLICATE_SIGNAL_KEY')
 if sorted(num(r143.horizon_td).dropna().unique().tolist())!=list(HORIZONS):fail('HORIZON_DRIFT')
 # 1) Full frozen historical outcome panel. Every flag is descriptive, NOT a repaired price.
 horizons=[]
 for h in HORIZONS:
  q=trust[['snapshot_date','code','name','snapshot_price',f'd{h}_complete',f'd{h}_close_ret_pct']].copy()
  q=q.rename(columns={f'd{h}_complete':'complete',f'd{h}_close_ret_pct':'trust_ret_pct'})
  q['horizon']=h;q['complete']=yes(q.complete);q['trust_ret_pct']=num(q.trust_ret_pct)
  q['signal_price']=num(q.snapshot_price)
  q['trust_implied_horizon_close']=q.signal_price*(1+q.trust_ret_pct/100)
  q['qa_abs_return_gt40']=q.complete & q.trust_ret_pct.abs().gt(40.0)
  m=r143[r143.horizon_td.eq(str(h))][['signal_date','code','entry_price','horizon_close','close_ret_pct','horizon_date','outcome_price_source']].copy()
  m=m.rename(columns={'signal_date':'snapshot_date','entry_price':'r143_entry_price','horizon_close':'r143_horizon_close','close_ret_pct':'r143_ret_pct'})
  if m.duplicated(['snapshot_date','code']).any():fail('R143_DUP_HORIZON')
  q=q.merge(m,on=['snapshot_date','code'],how='left',validate='one_to_one',indicator=True)
  q['r143_available']=q['_merge'].eq('both');q=q.drop(columns=['_merge'])
  q['r143_ret_pct']=num(q.r143_ret_pct);q['r143_horizon_close']=num(q.r143_horizon_close)
  q['r143_entry_price']=num(q.r143_entry_price)
  q['abs_return_diff_pp']=(q.trust_ret_pct-q.r143_ret_pct).abs()
  q['qa_cross_diff_gt10pp']=q.complete & q.r143_available & q.abs_return_diff_pp.gt(10.0)
  q['qa_price_ratio_outside_half_to_two']=q.complete & q.r143_available & (q.trust_implied_horizon_close/q.r143_horizon_close).pipe(lambda x:x.lt(0.5)|x.gt(2.0))
  q['source_path_file_sha_available']=False
  q['horizon_calendar_alignment_certified']=False
  q['cash_action_adjustment_certified']=False
  horizons.append(q)
 allpath=pd.concat(horizons,ignore_index=True).sort_values(['snapshot_date','code','horizon']).reset_index(drop=True)
 # 2) No substitution based on same NAME is allowed. Identity loss is flagged as an incident.
 if not ((collision['r143_ticker_exact']=='0088M0') & yes(collision.normalization_collision_proven)).all():fail('COLLISION_EVIDENCE_CHANGED')
 if lossy_legacy('0088M0')!='000880':fail('LOSSY_REPRO_DRIFT')
 identity=trust.loc[(trust.snapshot_date=='2026-09-23') & (trust.code=='000880') & (trust.name=='메쥬')].copy()
 if len(identity)!=1:fail('COLLISION_EVENT_NOT_FOUND')
 q=allpath.copy()
 q['qa_identity_collision_suspect']=(q.snapshot_date=='2026-09-23')&(q.code=='000880')&(q.name=='메쥬')
 q['qa_any_horizon']=q.qa_abs_return_gt40 | q.qa_cross_diff_gt10pp | q.qa_price_ratio_outside_half_to_two | q.qa_identity_collision_suspect
 # Unmatched cross-sources are NOT clean/verified. NO ref is not equal to matched and verified.
 q['qa_review_required']=q.qa_any_horizon
 q['research_price_certified']=False
 # Event-level warnings propagate from ANY completed horizon; do not pretend D+10 is independently fine.
 event=q.groupby(['snapshot_date','code'],sort=False).agg(
  n_completed=('complete','sum'),n_r143_matched=('r143_available','sum'),
  n_abs40=('qa_abs_return_gt40','sum'),n_cross_mismatch=('qa_cross_diff_gt10pp','sum'),
  n_price_ratio=('qa_price_ratio_outside_half_to_two','sum'),
  qa_identity=('qa_identity_collision_suspect','max'),qa_any=('qa_any_horizon','max')
 ).reset_index()
 t=trust.merge(event,on=['snapshot_date','code'],validate='one_to_one')
 t=t.sort_values(['snapshot_date','code'])
 t['first_signal_of_code']=~t.duplicated('code')
 t['complete5']=yes(t.d5_complete);t['complete10']=yes(t.d10_complete)
 t['r5']=num(t.d5_close_ret_pct);t['r10']=num(t.d10_close_ret_pct);t['mfe5']=num(t.d5_mfe_pct)
 t['paired']=t.complete5 & t.complete10 & t.r5.notna() & t.r10.notna()
 t['giveback5to10_pp']=t.r10-t.r5
 t['ever_mfe5_then_d10_negative']=t.mfe5.ge(5)&t.r10.lt(0)
 t['quality_status']=np.where(t.qa_any,'QA_WARNING_NOT_REPAIRED','QA_UNFLAGGED_STILL_UNCERTIFIED')
 # Exact historic denominator; all metrics are descriptive, no train/test or optimization.
 def scoreframe(sub,lab):
  z=sub[sub.paired]
  return dict(group=lab,n_events=int(len(sub)),n_paired=int(len(z)),n_qa_warning=int(sub.qa_any.sum()),
   median_d5_pct=med(z.r5),median_d10_pct=med(z.r10),median_d10_minus_d5_pp=med(z.giveback5to10_pp),
   mean_d5_pct=float(z.r5.mean()) if len(z) else None,mean_d10_pct=float(z.r10.mean()) if len(z) else None,
   first_n=int(z.first_signal_of_code.sum()),repeated_n=int((~z.first_signal_of_code).sum()),
   mfe5_then_d10_negative_n=int(z.ever_mfe5_then_d10_negative.sum()),
   strong_research_uncertified=True)
 stats=[scoreframe(t,'FULL_UNMODIFIED'),scoreframe(t[~t.qa_any],'QA_UNFLAGGED_DESCRIPTIVE'),scoreframe(t[t.qa_any],'QA_WARNINGS_SEPARATE')]
 for kind,selector in [('FIRST',t.first_signal_of_code),('REPEAT',~t.first_signal_of_code)]:
  stats.append(scoreframe(t[selector],'FULL_'+kind))
  stats.append(scoreframe(t[selector & ~t.qa_any],'QA_UNFLAGGED_'+kind))
 stats=pd.DataFrame(stats)
 cross=q[q.qa_cross_diff_gt10pp].copy()
 matched=q[q.r143_available & q.complete].copy()
 qa=pd.DataFrame([
  dict(check='ALL_FROZEN_EVENTS',n=len(t)),
  dict(check='EVIDENCE_R143_EXACT_MATCHED_HORIZON_ROWS',n=int(len(matched))),
  dict(check='QA_ABS_GT40_HORIZON_ROWS',n=int(q.qa_abs_return_gt40.sum())),
  dict(check='QA_CROSS_RETURN_GT10PP_ROWS',n=int(q.qa_cross_diff_gt10pp.sum())),
  dict(check='QA_PRICE_RATIO_OUTSIDE_HALF_TO_TWO_ROWS',n=int(q.qa_price_ratio_outside_half_to_two.sum())),
  dict(check='QA_IDENTITY_COLLISION_EVENT',n=int(identity.shape[0])),
  dict(check='QA_EVENT_UNION',n=int(t.qa_any.sum())),
  dict(check='EVENT_PAIRED_D5_D10',n=int(t.paired.sum())),
  dict(check='MISSING_RAW_TRUST_PRICE_FILES_IN_FROZEN_ARCHIVE',n=1),
 ])
 # Source files: original TRUST archive lacks actual per-day price cache+hash. Never assert source-specific cause.
 prov=pd.DataFrame([dict(system='REAL_FULL_TRUST',historic_archive=PIN['trust']['name'],daily_price_cache_file_present=False,
                             sha256_of_daily_cache_present=False,original_entry_snapshot_available=True,
                             outcome_date_price_source_certified=False,correction_applied=False),
                    dict(system='R143',historic_archive=PIN['r143']['name'],daily_price_cache_file_present=False,
                             sha256_of_daily_cache_present=False,original_entry_snapshot_available=True,
                             outcome_date_price_source_certified=False,correction_applied=False)])
 files={
  'R1D13_SOURCE_OUTCOME_PATH_PANEL.csv':q,
  'R1D13_EVENT_QUALITY_UNION.csv':t[['snapshot_date','code','name','snapshot_price','first_signal_of_code','paired','qa_any','quality_status','n_completed','n_r143_matched','n_abs40','n_cross_mismatch','n_price_ratio','qa_identity','r5','r10','mfe5','giveback5to10_pp','ever_mfe5_then_d10_negative']],
  'R1D13_SENSITIVITY_NO_REPAIR.csv':stats,
  'R1D13_QA_COUNTS.csv':qa,
  'R1D13_CROSS_SOURCE_DISCREPANCIES.csv':cross,
  'R1D13_SOURCE_PROVENANCE_LIMITS.csv':prov,
  'R1D13_FROZEN_STATIC_CODE_HASH_AUDIT.csv':check_source_blobs(repo_root)
 }
 for name,df in files.items():df.to_csv(out/name,index=False,encoding='utf-8-sig',float_format='%.10g')
 manifest=dict(version=VERSION,study='PROVENANCE_SCOPE_SENSITIVITY_NOT_NEW_OOS',
  trust_events=int(len(t)),r143_horizon_rows=int(len(r143)),r1d12_discrepancy_rows=int(len(d12)),
  paired_events=int(t.paired.sum()),qa_event_union=int(t.qa_any.sum()),
  qa_abs40_rows=int(q.qa_abs_return_gt40.sum()),qa_exact_price_discrepancy_rows=int(q.qa_cross_diff_gt10pp.sum()),
  qa_alphanumeric_identity_events=int(len(identity)),
  source_price_cache_file_sha_available=False,underlying_price_file_cause_proven=False,
  all_reported_retrospective=True,new_independent_OOS=False,predictive_efficacy_certified=False,
  adjusted_tradeability_certified=False,live_search_score_rank_order_changed=False,
  input_source_data_mutated=False,correction_applied=False,orders_placed=0,
  pins=PIN,source_git_blobs=SRC_BLOBS,predeclared_protocol_sha256=sha256(Path(protocol).read_bytes()))
 (out/'R1D13_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
 print('R1D13_FROZEN_CONTAMINATION_SCOPE_QA_PASS',json.dumps({k:manifest[k] for k in ('trust_events','paired_events','qa_event_union','qa_abs40_rows','qa_exact_price_discrepancy_rows','orders_placed')}),flush=True)
 return manifest

def main(argv=None):
 a=argparse.ArgumentParser()
 a.add_argument('--trust',required=True);a.add_argument('--r143',required=True);a.add_argument('--r1d12',required=True)
 a.add_argument('--repo-root',default='.');a.add_argument('--protocol',default='R1D13_PREDECLARED_PROTOCOL.json')
 a.add_argument('--out',default='reports/r1d13/result')
 x=a.parse_args(argv);run(x.trust,x.r143,x.r1d12,x.repo_root,x.protocol,x.out)
if __name__=='__main__':main()
