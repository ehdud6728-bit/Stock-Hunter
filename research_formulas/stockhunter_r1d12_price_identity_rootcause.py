#!/usr/bin/env python3
"""R1-D12 frozen source identity and price-path root-cause audit.

RESEARCH_ONLY / NOT OOS / NO LIVE mutation. Reads frozen existing GitHub artifacts,
statistically descriptive, never corrects original tickers or future returns.
"""
from __future__ import annotations
import argparse, ast, hashlib, io, json, math, re, zipfile
from pathlib import Path
import pandas as pd

VERSION='STOCKHUNTER_R1D12_FROZEN_IDENTITY_PRICE_PATH_ROOTCAUSE_AUDIT_V1'
RESEARCH_ONLY=True
SRC={
 'trust':('real_full_trust_audit.py','b18d67a13a1d08076b6c0583272939c06ed508af','_code','LOSSY_DIGITS_ONLY'),
 'capture':('real_full_capture_runner.py','6167746fe363961919a70e5ec206861bfaec465c','_clean_code','LOSSY_DIGITS_ONLY'),
 'r143':('real_full_r1c1_path_integrity_r143.py','fe7fba1e26f2a952aeff54b07230c888547e03ef','norm_code','ALPHANUMERIC_RETAINED'),
}
PIN={
 'trust':{'run':39948786391,'id':12504190178,'sha256':'3d2ee08727e50b248cdf7c41be7fb21744d2f9ee908747e693895c4d443ab34b','name':'stockhunter-v1107-4-5-73-3-6-6-24-causal-universe-full-denom-pattern-oos-exit-reports-1681'},
 'r143':{'run':39960213718,'id':12504990509,'sha256':'a1ba01eea0e1976e9edd59ec6b548d2d56f897e7638498dc4185b66b284edbbf','name':'real-full-r143-path-integrity-39960213718'},
 'r1d10':{'run':40074113253,'id':12551784748,'sha256':'dc01ee35eca74197e6d95511c659bce92c975746f413f6a483c9604f8243a1bd','name':'stockhunter-r1d10-ACCUMULATED-PROVENANCE-QA-NOT-OOS'},
}
HORIZONS=(1,3,5,10)
EXPECTED_MISMATCH_ROWS=4
EXPECTED_NAME_ID_COLLISIONS=1

def fail(msg):raise RuntimeError('R1D12_FAIL_CLOSED_'+str(msg))
def sha(data):return hashlib.sha256(data).hexdigest()
def blob(data):return hashlib.sha1(f'blob {len(data)}\0'.encode()+data).hexdigest()

def ticker(v):
 s=str(v).strip().upper()
 if not re.fullmatch(r'[A-Z0-9]{6}',s):fail('INVALID_EXACT_TICKER:'+s[:20])
 return s

def legacy_digit_only(v):
 s=re.sub(r'\D','',str(v))
 return s[-6:].zfill(6) if s else ''

def source_audit(root):
 # Parse code without import/exec; only recorded source and Git blob identities are checked.
 rows=[]
 for k,(name,expected,func,contract) in SRC.items():
  p=Path(root)/name
  if not p.is_file():fail('SOURCE_FILE_MISSING_'+name)
  raw=p.read_bytes()
  if blob(raw)!=expected:fail('SOURCE_GIT_BLOB_DRIFT_'+name)
  tree=ast.parse(raw.decode('utf-8'),filename=name)
  f=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name==func]
  if len(f)!=1:fail('CODE_NORMALIZER_NOT_FOUND_'+name)
  segment=ast.get_source_segment(raw.decode('utf-8'),f[0]) or ''
  if contract=='LOSSY_DIGITS_ONLY' and not ('re.sub' in segment and r'\D' in segment and '.zfill(6)' in segment):fail('CODE_NORMALIZER_PATTERN_DRIFT_'+name)
  if contract=='ALPHANUMERIC_RETAINED' and not ('isalnum()' in segment and '.zfill(6)' in segment):fail('R143_NORMALIZER_PATTERN_DRIFT')
  rows.append({'source':name,'git_blob_sha':expected,'normalizer':func,'observed_contract':contract,'file_imported_or_executed':False,'live_file_modified':False})
 return pd.DataFrame(rows)

def read_artifact(root,relative):
 root=Path(root)
 if root.is_file() and root.suffix.lower()=='.zip':
  with zipfile.ZipFile(root) as z:
   matches=[x for x in z.namelist() if x.endswith(relative)]
   if len(matches)!=1:fail('ARTIFACT_CSV_NOT_UNIQUE_'+relative)
   data=z.read(matches[0])
 else:
  matches=[x for x in root.rglob('*') if x.is_file() and x.as_posix().endswith(relative)]
  if len(matches)!=1:fail('ARTIFACT_CSV_NOT_UNIQUE_'+relative)
  data=matches[0].read_bytes()
 return pd.read_csv(io.BytesIO(data),dtype=str,keep_default_na=True,low_memory=False)

def load_inputs(trust_root,r143_root,d10_root):
 t=read_artifact(trust_root,'real_full_trust_selection_ledger.csv')
 r=read_artifact(r143_root,'real_full_r1c1_r143_outcome_ledger.csv')
 cross=read_artifact(d10_root,'REALFULL_R143_SAME_ID_RETURNS_QA.csv')
 ident=read_artifact(d10_root,'REALFULL_ALPHANUMERIC_IDENTITY_COLLISIONS.csv')
 for d,key,label in [(t,['snapshot_date','code'],'TRUST'),(r,['signal_date','code','horizon_td'],'R143'),(cross,['signal_date','code','horizon'],'CROSS')]:
  if d.duplicated(key).any():fail('DUPLICATE_KEY_'+label)
 for a in [t,r,cross]:
  if not a.code.map(lambda x:bool(re.fullmatch(r'[A-Z0-9]{6}',str(x)))).all():fail('SOURCE_LOSSY_OR_BAD_CODE')
 if len(t)!=360 or len(cross)!=462 or len(ident)!=EXPECTED_NAME_ID_COLLISIONS:fail('PINNED_DATA_ROW_DRIFT')
 flags=cross['qa_mismatch_gt10pp'].astype(str).str.lower().isin(['true','1'])
 if flags.sum()!=EXPECTED_MISMATCH_ROWS:fail('CROSS_MISMATCH_COUNTER_DRIFT')
 return t,r,cross,ident

def write_csv(df,p):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True);df.to_csv(p,index=False,encoding='utf-8-sig',float_format='%.12g')
def write_json(x,p):Path(p).write_text(json.dumps(x,ensure_ascii=False,sort_keys=True,indent=2,default=str)+'\n',encoding='utf-8')

def analysis(t,r,cross,ident,out):
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 # 1. Show exact source row and invertible information loss. NEVER auto-link codes.
 collide=[]
 for x in ident.itertuples(index=False):
  native=t[(t.snapshot_date==x.signal_date)&(t.code==x.trust_code)&(t.name==x.name)]
  shadow=r[(r.signal_date==x.signal_date)&(r.code==x.r143_code)&(r.name==x.name)]
  if len(native)!=1 or shadow.empty:fail('IDENTITY_EVIDENCE_NOT_COMPLETE')
  if ticker(x.r143_code)==ticker(x.trust_code):fail('IDENTITY_SAME_INVALID')
  lossy=legacy_digit_only(x.r143_code)
  if lossy!=x.trust_code:fail('NORMALIZATION_PATH_UNPROVEN')
  ref=float(native.iloc[0].snapshot_price)
  if ref!=float(x.trust_snapshot_price):fail('TRUST_SNAPSHOT_PRICE_DRIFT')
  for h in HORIZONS:
   sr=shadow[shadow.horizon_td==str(h)]
   if len(sr)!=1:fail('R143_HORIZON_MISSING')
   val=float(native.iloc[0]['d'+str(h)+'_close_ret_pct'])
   inferred=ref*(1+val/100)
   sec=sr.iloc[0]
   collide.append({'signal_date':x.signal_date,'name':x.name,'trust_ticker_exact':x.trust_code,'r143_ticker_exact':x.r143_code,
    'digit_only_result':lossy,'normalization_collision_proven':True,'same_stock_identification_inferred_from_name_date_price_only':True,
    'horizon':h,'trust_baseline_price':ref,'trust_reported_return_pct':val,'trust_inferred_horizon_close':inferred,
    'r143_entry_price':float(sec.entry_price),'r143_horizon_close':float(sec.horizon_close),'r143_return_pct':float(sec.close_ret_pct),
    'price_ratio_trust_inferred_over_r143':inferred/float(sec.horizon_close),'do_not_alias_or_correct':True})
 collisions=pd.DataFrame(collide)
 write_csv(collisions,out/'R1D12_ALPHANUMERIC_IDENTITY_PRESERVATION_PROOF.csv')

 # 2. Same exact code/date/horizon: price disagreement evidence (not proven same provider/session or cause).
 bad=cross[cross.qa_mismatch_gt10pp.astype(str).str.lower().isin(['true','1'])].copy()
 details=[]
 for row in bad.itertuples(index=False):
  code=ticker(row.code);h=int(row.horizon)
  x=t[(t.snapshot_date==row.signal_date)&(t.code==code)]
  y=r[(r.signal_date==row.signal_date)&(r.code==code)&(r.horizon_td==str(h))]
  if len(x)!=1 or len(y)!=1:fail('CROSS_SOURCE_MISMATCH_EVIDENCE_MISSING')
  x=x.iloc[0];y=y.iloc[0]
  base=float(x.snapshot_price);tv=float(x['d'+str(h)+'_close_ret_pct']);rv=float(y.close_ret_pct)
  implied=base*(1+tv/100);other=float(y.horizon_close)
  if abs(tv-float(row.trust_ret_pct))>1e-7 or abs(rv-float(row.r143_ret_pct))>1e-7:fail('CROSS_SOURCE_STATS_DRIFT')
  details.append({'signal_date':row.signal_date,'code':code,'name':row.name,'horizon':h,
  'trust_signal_price':base,'trust_implied_horizon_close':implied,
  'r143_signal_price':float(y.entry_price),'r143_horizon_close':other,
  'r143_horizon_date':y.horizon_date,'trust_ret_pct':tv,'r143_ret_pct':rv,
  'abs_return_diff_pp':abs(tv-rv),'trust_over_r143_price_ratio':implied/other,
  'identical_horizon_calendar_proven':False,'same_price_adjustment_basis_proven':False,
  'cause_classification':'SUSPECT_SOURCE_PRICE_PATH_OR_ADJUSTMENT_NOT_PROVEN',
  'original_corrected':False})
 p=pd.DataFrame(details).sort_values(['signal_date','code','horizon']).reset_index(drop=True)
 write_csv(p,out/'R1D12_EXACT_TICKER_HORIZON_PRICE_RATIO_DISCREPANCIES.csv')

 # 3. Query all R143 same-code path & TRUST to demonstrate intermittent return issue,
 # including non-flagged same code/horizon rows (e.g. D+5 on 000150).
 keys=p[['signal_date','code']].drop_duplicates()
 rows=[]
 for ev in keys.itertuples(index=False):
  x=t[(t.snapshot_date==ev.signal_date)&(t.code==ev.code)]
  if len(x)!=1:fail('PATH_EVENT_MISSING')
  x=x.iloc[0];base=float(x.snapshot_price)
  for h in HORIZONS:
   sy=r[(r.signal_date==ev.signal_date)&(r.code==ev.code)&(r.horizon_td==str(h))]
   if sy.empty or not str(x.get('d'+str(h)+'_complete')).strip().lower() in ['true','1']:continue
   y=sy.iloc[0]
   val=float(x['d'+str(h)+'_close_ret_pct']);rv=float(y.close_ret_pct)
   rows.append({'signal_date':ev.signal_date,'code':ev.code,'name':x['name'],'horizon':h,
   'trust_inferred_close':base*(1+val/100),'r143_horizon_close':float(y.horizon_close),
   'trust_ret_pct':val,'r143_ret_pct':rv,'ret_difference_pp':val-rv,
   'potential_cache_or_price_adjustment_discontinuity':abs(val-rv)>10,
   'r143_outcome_price_source':y.outcome_price_source,
   'exact_cache_file_sha_available_in_source_artifact':False})
 paths=pd.DataFrame(rows).sort_values(['signal_date','code','horizon'])
 write_csv(paths,out/'R1D12_MISMATCH_EVENT_ALL_AVAILABLE_HORIZONS.csv')

 manifest={'version':VERSION,'RESEARCH_ONLY':True,'strategy_ready':False,
 'historical_only_not_new_OOS':True,'source_production_changed':False,'orders_placed':0,
 'trust_rows':len(t),'r143_rows':len(r),'r1d10_cross_rows':len(cross),
 'n_normalizer_collision_evidence_events':len(ident),'n_cross_return_discrepancy_rows':len(p),
 'n_cross_discrepancy_unique_events':len(keys),
 'normalization_root_cause_code_proven':True,
 'price_file_specific_root_cause_proven':False,
 'price_cache_file_sha_available':False,
 'source_ticker_or_return_automatically_fixed':False,
 'future_signal_outcomes_not_changed':True,
 'live_selection_score_rank_orders_changed':False,
 'caveats':['TRUST horizon underlying cache dates and file SHA are absent from this frozen artifact','R143 price provenance is research outcome-only FDR','corporate action, stale cache, or provider mismatch require raw cache and PIT adjustment metadata to distinguish']}
 write_json(manifest,out/'R1D12_MANIFEST.json')
 return manifest

def main(argv=None):
 ap=argparse.ArgumentParser()
 ap.add_argument('--trust',required=True);ap.add_argument('--r143',required=True);ap.add_argument('--r1d10',required=True)
 ap.add_argument('--repo-root',default='.');ap.add_argument('--protocol',default='R1D12_PREDECLARED_PROTOCOL.json');ap.add_argument('--out',required=True)
 a=ap.parse_args(argv)
 q=json.loads(Path(a.protocol).read_text('utf-8'))
 if q.get('version')!=VERSION or q.get('expected_cross_mismatch_rows')!=4 or q.get('expected_identity_collision_events')!=1 or q.get('source_pins')!=PIN:fail('PROTOCOL_DRIFT')
 if q.get('no_auto_corrections') is not True:fail('AUTO_CORRECTIONS_FORBIDDEN')
 s=source_audit(a.repo_root)
 t,r,c,i=load_inputs(a.trust,a.r143,a.r1d10)
 m=analysis(t,r,c,i,a.out)
 write_csv(s,Path(a.out)/'R1D12_CODE_PATH_STATIC_ATTESTATION.csv')
 m['source_git_blobs']={x:info[1] for x,info in SRC.items()};m['source_artifact_pins']=PIN
 m['predeclared_protocol_sha256']=sha(Path(a.protocol).read_bytes())
 write_json(m,Path(a.out)/'R1D12_MANIFEST.json')
 print('R1D12_IDENTITY_AND_PRICE_PATH_ROOTCAUSE_RESEARCH_ONLY_PASS',json.dumps({k:m[k] for k in ['n_normalizer_collision_evidence_events','n_cross_return_discrepancy_rows','n_cross_discrepancy_unique_events','orders_placed']}))
 return 0
if __name__=='__main__':raise SystemExit(main())
