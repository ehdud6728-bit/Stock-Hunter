#!/usr/bin/env python3
"""R1-D15: isolated, read-only security identity quarantine and price-cache provenance.

The original REAL_FULL and shared price-loader source code is NOT imported or changed.
The R1D14 price cache artifact is frozen; a restored cache here is NOT an historic as-used cache.
Security identity is exact uppercase six-character KRX code (digits or capital letters).
Never fallback from 0088M0 to 000880. Do not produce substitute signal performance.
"""
from __future__ import annotations
import argparse, hashlib, io, json, re, zipfile
from pathlib import Path
import pandas as pd
import numpy as np

REVISION='R1D15_V1_CACHE_ALIAS_QUARANTINE_RESEARCH_ONLY_20261024'
PIN={'run':40099215663,'head':'7fc9d0580f369491aa1b01a31ddfa9f088533723',
     'artifact_id':12562425614,'artifact_name':'stockhunter-r1d14-RESTORED-CACHE-EVIDENCE-NOT-HISTORIC-AS-USED',
     'sha256':'84ae76f87c17dc4699630afe4d54b09f512f5739ad188712099c954bfda9f3ff'}
STATIC_BLOBS={
 'real_full_trust_audit.py':'b18d67a13a1d08076b6c0583272939c06ed508af',
 'real_full_capture_runner.py':'6167746fe363961919a70e5ec206861bfaec465c',
 'triangle1pb_research.py':'1225fa7c90e7e7bab7937f96e7c08dec861e226d',
 'real_full_r1c1_path_integrity_r143.py':'fe7fba1e26f2a952aeff54b07230c888547e03ef'}
TARGETS=('000100','000150','000880','0088M0')
SIGNAL_DATE='2026-09-23'
SCHEMA='STOCKHUNTER_R1D15_PREDECLARED_RESEARCH_CONTRACT_V1'
FROZEN_BARS=3713
FROZEN_FILES=4
PRICE_BAR_NAME='R1D14_RESTORED_TARGET_CODE_PRICE_BARS.csv'
PRICE_FILE_NAME='R1D14_RESTORED_CACHE_FILE_EVIDENCE.csv'


def digest(b:bytes)->str:return hashlib.sha256(b).hexdigest()

def git_blob(path):
 b=Path(path).read_bytes()
 return hashlib.sha1(b'blob '+str(len(b)).encode('ascii')+b'\0'+b).hexdigest()

def exact_code(x):
 s=str(x if x is not None else '').strip().upper()
 if not re.fullmatch(r'[0-9A-Z]{6}',s):raise ValueError('R1D15_INVALID_EXACT_SECURITY_ID:'+s[:40])
 return s

def legacy_code(x):
 s=re.sub(r'\D','',str(x))
 return s[-6:].zfill(6) if s else ''

def filename_code(name):
 """Exact, conservative extraction, not a permissive substring search.

 Accepts existing Stock-Hunter PRICE basename (6 chars)_(window).pkl.gz only.
 Unknown filenames are quarantined without generating any guessed security ID.
 """
 m=re.fullmatch(r'([0-9A-Z]{6})_(\d{1,5})\.PKL\.GZ',Path(name).name.upper())
 return m.group(1) if m else ''

def protocol_guard(protocol_path,repo_root=None):
 p=json.loads(Path(protocol_path).read_text(encoding='utf-8'))
 check={'schema':SCHEMA,'pinned_r1d14':PIN,'static_git_blobs':STATIC_BLOBS,
        'target_exact_codes':list(TARGETS),'meju_signal_date':SIGNAL_DATE,
        'frozen_r1d14_bars':FROZEN_BARS,'frozen_r1d14_file_evidence':FROZEN_FILES,
        'restore_cache_historical_as_used':False,'apply_data_corrections':False,
        'change_production_scanner':False,'change_live_score_rank_orders':False,
        'place_orders':False,'retune_hypotheses':False,'new_independent_out_of_sample':False}
 for key,val in check.items():
  if p.get(key)!=val:raise ValueError('R1D15_PREDECLARED_PROTOCOL_MISMATCH:'+key)
 if repo_root is not None:
  for rel,expected in STATIC_BLOBS.items():
   fp=Path(repo_root)/rel
   if not fp.is_file() or git_blob(fp)!=expected:raise ValueError('R1D15_FROZEN_SOURCE_DRIFT:'+rel)
 return p

def frozen_csv(path,name):
 root=Path(path)
 if root.is_file():
  with zipfile.ZipFile(root,'r') as z:
   files=[k for k in z.namelist() if k==name or k.endswith('/'+name)]
   if len(files)!=1:raise ValueError('R1D15_FROZEN_CSV_MISSING_OR_DUP:'+name)
   raw=z.read(files[0])
 else:
  files=list(root.rglob(name))
  if len(files)!=1:raise ValueError('R1D15_FROZEN_CSV_MISSING_OR_DUP:'+name)
  raw=files[0].read_bytes()
 return pd.read_csv(io.BytesIO(raw),dtype={'code':str,'parsed_loader_code':str,'cache_relative_path':str,'file_sha256':str,'sha256':str},low_memory=False)

def safe_lookup(code, index, *, join_mode='exact'):
 """Only exact-code lookups permitted; legacy alias lookup fails closed.

 A valid exact file can be selected despite a collision in the old numeric namespace.
 It must never be silently substituted with the *other* security's cache.
 """
 if join_mode!='exact':raise ValueError('R1D15_LEGACY_ALIAS_JOIN_FORBIDDEN')
 original=exact_code(code)
 candidates=index.get(original,[])
 if len(candidates)!=1:raise ValueError('R1D15_NO_UNIQUE_EXACT_PRICE_CACHE:'+original)
 return candidates[0]

def validate_frozen(files,bars):
 if len(files)!=FROZEN_FILES or len(bars)!=FROZEN_BARS:raise ValueError('R1D15_FROZEN_R1D14_SOURCE_ROWCOUNT_DRIFT')
 for required in ('cache_relative_path','sha256','read_status','parsed_loader_code','first_date','last_date'):
  if required not in files:raise ValueError('R1D15_FROZEN_FILE_SCHEMA_DRIFT:'+required)
 for required in ('cache_relative_path','file_sha256','date','close','code'):
  if required not in bars:raise ValueError('R1D15_FROZEN_BAR_SCHEMA_DRIFT:'+required)
 if files['cache_relative_path'].duplicated().any():raise ValueError('R1D15_FROZEN_FILE_PATH_DUP')
 if bars[['cache_relative_path','date']].duplicated().any():raise ValueError('R1D15_FROZEN_BAR_DATE_DUP')
 if bars[['close']].isna().any().any() or (pd.to_numeric(bars.close,errors='coerce')<=0).any():
  raise ValueError('R1D15_FROZEN_BAD_PRICE')
 for x in files.itertuples(index=False):
  exact=filename_code(x.cache_relative_path)
  if not exact or exact not in TARGETS:raise ValueError('R1D15_UNRECOGNISED_PRICE_FILE_NAME')
  bs=bars[bars.cache_relative_path==x.cache_relative_path]
  if len(bs)!=int(x.parsed_date_rows) or len(bs)==0:raise ValueError('R1D15_FROZEN_FILE_ROWCOUNT_DRIFT')
  if not bs.file_sha256.eq(x.sha256).all():raise ValueError('R1D15_FROZEN_FILE_SHA_BINDING_DRIFT')
  if not bs.code.eq(x.parsed_loader_code).all():raise ValueError('R1D15_FROZEN_LOADER_CODE_DRIFT')
 return True

def file_evidence(files,cache_root):
 data=[];root=Path(cache_root)
 for f in files.itertuples(index=False):
  relative=str(f.cache_relative_path)
  exact=exact_code(filename_code(relative))
  legacy=legacy_code(exact)
  raw_path=root/relative
  exists=raw_path.is_file()
  actual_sha=digest(raw_path.read_bytes()) if exists else ''
  fixed_sha=str(f.sha256)
  status=('RESTORED_IDENTICAL_SHA_R1D14' if exists and actual_sha==fixed_sha else
          'RESTORED_CHANGED_SHA_REVIEW' if exists else 'NO_CURRENT_RESTORED_FILE')
  data.append({'relative_path':relative,'exact_file_code':exact,'old_loader_code':exact_code(f.parsed_loader_code),
               'legacy_numeric_alias':legacy,'legacy_erased_identity':exact!=exact_code(f.parsed_loader_code),
               'r1d14_sha256':fixed_sha,'r1d14_bytes':int(f.byte_size),
               'currently_restored_exists':exists,'currently_restored_sha256':actual_sha,
               'current_equals_r1d14_sha':bool(exists and actual_sha==fixed_sha),
               'cache_evidence_state':status,'r1d14_first_date':str(f.first_date),'r1d14_last_date':str(f.last_date),
               'historic_as_used_certified':False,'source_price_correction_applied':False})
 return pd.DataFrame(data).sort_values('exact_file_code').reset_index(drop=True)

def exact_bars(bars,files):
 evidence=files.set_index('relative_path')
 q=bars.copy()
 q['exact_file_code']=q.cache_relative_path.map(lambda x:exact_code(filename_code(x)))
 q['old_loader_code']=q.code.map(exact_code)
 q['date']=pd.to_datetime(q.date,errors='coerce').dt.strftime('%Y-%m-%d')
 if q.date.isna().any():raise ValueError('R1D15_BAD_FROZEN_DATE')
 q['legacy_numeric_alias']=q.exact_file_code.map(legacy_code)
 q['original_code_erased_by_old_loader']=q.exact_file_code!=q.old_loader_code
 q['cache_origin_sha256']=q.cache_relative_path.map(evidence.r1d14_sha256)
 if q[['exact_file_code','date']].duplicated().any():raise ValueError('R1D15_DUPLICATED_EXACT_CODE_DATE')
 if not q.cache_origin_sha256.eq(q.file_sha256).all():raise ValueError('R1D15_SHA_PROVENANCE_NOT_BOUND')
 return q.sort_values(['exact_file_code','date']).reset_index(drop=True)

def build_collision(files,prices):
 groups=[]
 for alias,g in files.groupby('legacy_numeric_alias',sort=True):
  ids=sorted(set(g.exact_file_code));collision=len(ids)>1
  if not collision:continue
  for exact in ids:
   x=prices[prices.exact_file_code==exact]
   groups.append({'legacy_alias':alias,'exact_file_code':exact,'distinct_exact_security_ids':len(ids),
                  'other_exact_ids':'|'.join(v for v in ids if v!=exact),
                  'r1d14_price_rows':len(x),'r1d14_first_date':min(x.date) if len(x) else '',
                  'r1d14_last_date':max(x.date) if len(x) else '',
                  'join_by_legacy_alias':'BLOCKED_AMBIGUOUS_IDENTITY',
                  'join_by_exact_security_code':'ONLY_EXACT_WITH_TIME_AUTHORITY_AND_FILE_SHA',
                  'historic_as_used_certified':False,'live_change':False})
 return pd.DataFrame(groups)

def signal_guard(prices,files):
 rows=[]
 for code in ('0088M0','000880'):
  frame=prices[prices.exact_file_code.eq(code)]
  after=frame[frame.date>SIGNAL_DATE]
  same=frame[frame.date==SIGNAL_DATE]
  existing=files[files.exact_file_code.eq(code)]
  rows.append({'signal_date':SIGNAL_DATE,'requested_exact_code':code,
               'r1d14_file_present':len(existing)==1,'r1d14_last_date':max(frame.date) if len(frame) else '',
               'r1d14_signal_date_close_available':not same.empty,
               'r1d14_post_signal_price_rows':len(after),
               'r1d14_post_signal_price_available':not after.empty,
               'alias_unsafe':True,
               'permitted_source_for_meju_0088M0':code=='0088M0',
               'impute_from_000880':False,
               'horizon_return_status':('BLOCK_MISSING_EXACT_POST_SIGNAL_SOURCE' if code=='0088M0' and len(after)==0 else 'DO_NOT_REPLACE_OTHER_SECURITY'),
               'historical_file_certified':False})
 return pd.DataFrame(rows)

def write_csv(df,p):df.to_csv(p,index=False,encoding='utf-8-sig',float_format='%.10g')

def analyze(r1d14,cache_root,protocol,out,cache_key='',repo_root=None):
 lock=protocol_guard(protocol,repo_root)
 files=frozen_csv(r1d14,PRICE_FILE_NAME);bars=frozen_csv(r1d14,PRICE_BAR_NAME)
 validate_frozen(files,bars)
 files_out=file_evidence(files,cache_root)
 prices=exact_bars(bars,files_out.rename(columns={'relative_path':'relative_path'}))
 collisions=build_collision(files_out,prices)
 signal=signal_guard(prices,files_out)
 if not set(collisions.exact_file_code)=={'0088M0','000880'}:raise ValueError('R1D15_EXPECTED_SECURITY_COLLISION_NOT_REPRODUCED')
 if not files_out.loc[files_out.exact_file_code=='0088M0','legacy_erased_identity'].all():raise ValueError('R1D15_MEJU_CODE_LOSS_NOT_REPRODUCED')
 if int(signal.loc[signal.requested_exact_code=='0088M0','r1d14_post_signal_price_rows'].iloc[0])!=0:
  raise ValueError('R1D15_R1D14_MEJU_SOURCE_TIME_BOUNDARY_CHANGED')
 a=prices[prices.exact_file_code=='0088M0'][['date','close']].rename(columns={'close':'meju_close'})
 b=prices[prices.exact_file_code=='000880'][['date','close']].rename(columns={'close':'code000880_close'})
 o=a.merge(b,on='date',how='inner',validate='one_to_one')
 o['same_price']=np.isclose(pd.to_numeric(o.meju_close),pd.to_numeric(o.code000880_close),rtol=0,atol=1e-8)
 if len(o)!=98 or o.same_price.any():raise ValueError('R1D15_FROZEN_COLLISION_OVERLAP_CHANGED')
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 for n,v in [
  ('R1D15_FILE_SHA_AND_EXACT_CODE_PROVENANCE.csv',files_out),
  ('R1D15_EXACT_ID_PRICE_BARS_NOT_RELABELED.csv',prices),
  ('R1D15_LEGACY_ALIAS_COLLISION_QUARANTINE.csv',collisions),
  ('R1D15_MEJU_SIGNAL_TIME_AUTHORITY_GUARD.csv',signal),
  ('R1D15_SHARED_DATES_DISTINCT_PRICES.csv',o)]:write_csv(v,out/n)
 manifest={'revision':REVISION,'schema':SCHEMA,'frozen_r1d14_source_run':PIN['run'],
   'frozen_artifact_sha256':PIN['sha256'],'protocol_sha256':digest(Path(protocol).read_bytes()),
   'cache_key_restored_now':cache_key,'restored_files_present':int(files_out.currently_restored_exists.sum()),
   'restored_files_sha_equal_r1d14':int(files_out.current_equals_r1d14_sha.sum()),
   'frozen_bars_reviewed':len(bars),'frozen_files_reviewed':len(files),
   'identity_collision_exact_count':len(collisions),'identity_collision_legacy_keys':int(collisions.legacy_alias.nunique()),
   'collision_shared_dates':len(o),'collision_shared_dates_equal_close':int(o.same_price.sum()),
   'meju_has_post_signal_exact_cache_in_frozen_r1d14':False,
   'all_legacy_alias_joins_blocked':True,'negative_missing_price_never_zero_filled':True,
   'cache_origin_certified_historic_as_used':False,
   'research_only':True,'live_66_search_score_rank_order_unchanged':True,
   'production_price_parser_mutated':False,'source_price_cache_mutated':False,
   'correction_applied':False,'orders_placed':0,'new_oos':False,
   'decision':'PROOF_OF_BUG_AND_RESEARCH_QUARANTINE_ONLY__PRODUCTION_REMEDIATION_PENDING'}
 (out/'R1D15_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')
 (out/'R1D15_REPORT_KR.md').write_text(
  '# R1-D15 — 종목 원형 식별자 격리 및 재발 방지 검증\n\n'
  f'- R1D14 고정 가격 {len(bars):,}행 / 파일 {len(files):,}개.\n'
  f'- 서로 다른 정확한 종목코드가 같은 숫자 전용 키로 묶인 위험 코드 {len(collisions):,}건, 공통날짜 {len(o):,}개, 종가 일치 {int(o.same_price.sum())}개.\n'
  '- `0088M0`와 `000880`은 영문 포함 코드를 삭제하지 않고 반드시 별개로 보존합니다.\n'
  '- 2026-09-23 메쥬 신호에 대한 0088M0 정확 코드의 사후 가격은 이 R1D14 복원 캐시 자료에 없으므로 성과 계산은 BLOCK.\n'
  f'- 이번 실행에서 현재 복원된 파일 {manifest["restored_files_present"]}개, R1D14와 SHA 동일 {manifest["restored_files_sha_equal_r1d14"]}개.\n'
  '- 캐시 동일성은 R1D14와의 비교일 뿐, 과거 실거래 시점에 실제 사용된 캐시를 의미하지 않습니다.\n'
  '- LIVE 66 검색식·점수·랭킹·주문 변경하지 않았으며, 기존 원본 수정이나 과거 지표 정정도 없습니다.\n',encoding='utf-8')
 print('R1D15_RESEARCH_IDENTITY_QUARANTINE_PASS',json.dumps({k:manifest[k] for k in ['frozen_files_reviewed','collision_shared_dates','identity_collision_legacy_keys','restored_files_present','orders_placed']}))
 return manifest

def main(argv=None):
 ap=argparse.ArgumentParser()
 ap.add_argument('--r1d14',required=True);ap.add_argument('--cache-root',required=True)
 ap.add_argument('--protocol',default='R1D15_PREDECLARED_PROTOCOL.json');ap.add_argument('--out',default='reports/r1d15/result')
 ap.add_argument('--cache-key',default='');ap.add_argument('--repo-root',default=None)
 a=ap.parse_args(argv)
 return analyze(a.r1d14,a.cache_root,a.protocol,a.out,a.cache_key,a.repo_root)
if __name__=='__main__':main()
