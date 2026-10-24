#!/usr/bin/env python3
"""R1-D14 research-only current-cache recovery evidence, never historic-cache certification.

No live scans, order actions, score/rank mutations, or source cache writes.
Historical TRUST event/source identity is pinned and frozen in R1-D13 output.
A restored cache is *not* assumed to be the cache in a prior GitHub run.
"""
from __future__ import annotations
import argparse,hashlib,io,json,re,zipfile
from pathlib import Path
import pandas as pd
import numpy as np

VERSION='R1D14_RESTORED_CACHE_ONLY_NOT_AS_USED_PRICE_PROVENANCE_V1'
PINS={
 'r1d13': {'run':40096795372,'head':'fd5e32262c3e63792b3e653cab4e79628832dbf6','id':12561441155,'name':'stockhunter-r1d13-FROZEN-FULLCOHORT-CONTAMINATION-SCOPE-NOT-OOS','sha256':'f15e203bf7528d6cbce637b2c7bc846e35a40b2acbc04bb8b6ee87148a08435d'},
}
FROZEN_BLOBS={
 'real_full_trust_audit.py':'b18d67a13a1d08076b6c0583272939c06ed508af',
 'real_full_capture_runner.py':'6167746fe363961919a70e5ec206861bfaec465c',
 'triangle1pb_research.py':'1225fa7c90e7e7bab7937f96e7c08dec861e226d',
 'real_full_r1c1_path_integrity_r143.py':'fe7fba1e26f2a952aeff54b07230c888547e03ef',
}
PROTOCOL_VERSION='R1D14_PRICE_CACHE_EVIDENCE_PREDECLARED_V1'
TARGETS=('000150','000100','000880','0088M0')
SIGNAL_DATES=('2026-09-17','2026-10-20','2026-09-23')
PANEL='R1D13_SOURCE_OUTCOME_PATH_PANEL.csv'
CACHE_STATUS_MISSING='CACHE_NOT_RESTORED_HISTORIC_SOURCE_UNKNOWN'
CACHE_STATUS_PRESENT='RESTORED_CACHE_SNAPSHOT_NOT_AS_USED'


def sha(b:bytes)->str:return hashlib.sha256(b).hexdigest()
def exact_code(v):
 s=str(v if v is not None else '').strip().upper()
 if not re.fullmatch('[A-Z0-9]{6}',s):raise ValueError('R1D14_INVALID_EXACT_CODE:'+s[:30])
 return s

def lossy_code(v):
 s=re.sub(r'\D','',str(v))
 return s[-6:].zfill(6) if s else ''

def gitblob(path):
 b=Path(path).read_bytes()
 import hashlib
 return hashlib.sha1(b'blob '+str(len(b)).encode('ascii')+b'\0'+b).hexdigest()

def freeze(p,repo_root):
 protocol=json.loads(Path(p).read_text('utf-8'))
 if protocol.get('version')!=PROTOCOL_VERSION or protocol.get('targets')!=list(TARGETS) or protocol.get('pinned_r1d13')!=PINS['r1d13'] or protocol.get('source_git_blobs')!=FROZEN_BLOBS or protocol.get('allowed_live_change') is not False or protocol.get('historic_cache_equivalence_certifiable') is not False:
  raise ValueError('R1D14_PREDECLARED_PROTOCOL_MISMATCH')
 for k,v in FROZEN_BLOBS.items():
  fp=Path(repo_root)/k
  if not fp.is_file() or gitblob(fp)!=v:raise ValueError('R1D14_FROZEN_SOURCE_DRIFT:'+k)
 return protocol

def get_csv(root,filename):
 root=Path(root)
 if root.is_file():
  with zipfile.ZipFile(root) as z:
   matches=[n for n in z.namelist() if n.endswith('/'+filename) or n==filename]
   if len(matches)!=1:raise ValueError('R1D14_PINNED_FILE_MISSING_OR_AMBIGUOUS:'+filename)
   return pd.read_csv(io.BytesIO(z.read(matches[0])),dtype={'code':str,'snapshot_date':str},low_memory=False)
 matches=list(root.rglob(filename))
 if len(matches)!=1:raise ValueError('R1D14_PINNED_FILE_MISSING_OR_AMBIGUOUS:'+filename)
 return pd.read_csv(matches[0],dtype={'code':str,'snapshot_date':str},low_memory=False)

def csv_out(frame,path):
 frame.to_csv(path,index=False,encoding='utf-8-sig',float_format='%.10g')

def candidates(cache_root,targets):
 root=Path(cache_root)
 if not root.exists():return [],0
 fs=sorted([p for p in root.rglob('*') if p.is_file()])
 # Cache adapter first uses six-digit code from basename; expose candidate locations only.
 chosen=[]
 for p in fs:
  n=p.name.upper()
  if any(re.search(r'(?<![0-9])'+re.escape(code)+r'(?![0-9])',n) for code in targets):chosen.append(p)
 return chosen,len(fs)

def extracted_frames(files, cache_root):
 from triangle1pb_research import _load_any,_iter_frame_candidates,_frame_code,_date_series_from_frame,_first_col,CLOSE_ALIASES,HIGH_ALIASES,LOW_ALIASES,OPEN_ALIASES
 manifest=[]; allbars=[]
 for p in files:
  rel=str(p.relative_to(cache_root))
  row={'cache_relative_path':rel,'sha256':sha(p.read_bytes()),'byte_size':p.stat().st_size,'file_mtime_ns':p.stat().st_mtime_ns,'read_status':'UNREAD','parsed_loader_code':'','parsed_date_rows':0,'first_date':'','last_date':'','note':'CURRENT_RESTORED_CACHE_ONLY_NOT_HISTORIC_AS_USED'}
  try:
   obj=_load_any(p)
   frames=list(_iter_frame_candidates(obj))
   found=0
   for raw,meta in frames:
    c=_frame_code(raw,p,meta)
    if c not in TARGETS:continue
    dates,how=_date_series_from_frame(raw)
    cc=_first_col(raw.columns,CLOSE_ALIASES)
    if dates is None or cc is None:continue
    oh=_first_col(raw.columns,OPEN_ALIASES);hh=_first_col(raw.columns,HIGH_ALIASES);ll=_first_col(raw.columns,LOW_ALIASES)
    z=pd.DataFrame({'date':pd.to_datetime(dates,errors='coerce').to_numpy(),'close':pd.to_numeric(raw[cc],errors='coerce').to_numpy()})
    if hh:z['high']=pd.to_numeric(raw[hh],errors='coerce').to_numpy()
    if ll:z['low']=pd.to_numeric(raw[ll],errors='coerce').to_numpy()
    if oh:z['open']=pd.to_numeric(raw[oh],errors='coerce').to_numpy()
    z=z.dropna(subset=['date','close']).copy()
    z=z[z.close.gt(0)].sort_values('date').drop_duplicates('date',keep='last')
    if z.empty:continue
    z['date']=z.date.dt.strftime('%Y-%m-%d')
    z['code']=c;z['cache_relative_path']=rel;z['file_sha256']=row['sha256'];z['date_source']=how
    row['parsed_loader_code']=c;row['parsed_date_rows']+=len(z)
    row['first_date']=min(row['first_date'] or z.date.min(),z.date.min())
    row['last_date']=max(row['last_date'] or z.date.max(),z.date.max())
    allbars.append(z);found+=1
   row['read_status']='PARSED_SELECTED_FRAME' if found else 'NO_TARGET_PRICE_FRAME'
  except Exception as e:
   row['read_status']='FILE_LOAD_FAILED';row['read_error']=type(e).__name__+':'+str(e)[:180]
  manifest.append(row)
 return pd.DataFrame(manifest), pd.concat(allbars,ignore_index=True) if allbars else pd.DataFrame(columns=['date','close','code','cache_relative_path','file_sha256','date_source'])

def audit(r1d13,cache_root,repo_root,protocol,out,cache_key=''):
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 lock=freeze(protocol,repo_root)
 panel=get_csv(r1d13,PANEL)
 if len(panel)!=1440 or panel.code.nunique()<1 or panel[['snapshot_date','code','horizon']].duplicated().any():raise ValueError('R1D14_PINNED_R1D13_PANEL_CHANGED')
 panel['code']=panel.code.map(exact_code)
 relevant=panel[panel.code.isin(TARGETS)&panel.snapshot_date.isin(SIGNAL_DATES)&pd.to_numeric(panel.horizon,errors='coerce').isin([1,3,5,10])].copy()
 if relevant.empty:raise ValueError('R1D14_FROZEN_DISCREPANCY_CASES_EMPTY')
 cache_root=Path(cache_root);selected,count=candidates(cache_root,TARGETS)
 if selected:
  files,bars=extracted_frames(selected,cache_root)
 else:
  files=pd.DataFrame(columns=['cache_relative_path','sha256','byte_size','read_status','parsed_loader_code','parsed_date_rows','first_date','last_date','note'])
  bars=pd.DataFrame(columns=['date','close','code','cache_relative_path','file_sha256','date_source'])
 csv_out(files,out/'R1D14_RESTORED_CACHE_FILE_EVIDENCE.csv')
 csv_out(bars,out/'R1D14_RESTORED_TARGET_CODE_PRICE_BARS.csv')
 # Compare ONLY same numeric code+date. Neither restore nor price equality certifies historical provenance.
 r=relevant[['snapshot_date','code','name','horizon','horizon_date','trust_implied_horizon_close','r143_horizon_close','abs_return_diff_pp','qa_review_required']].copy()
 r['horizon_date']=r.horizon_date.astype(str).replace({'nan':''})
 r['trust_implied_horizon_close']=pd.to_numeric(r.trust_implied_horizon_close,errors='coerce')
 r['r143_horizon_close']=pd.to_numeric(r.r143_horizon_close,errors='coerce')
 if not bars.empty:
  z=bars.drop_duplicates(['date','code'],keep='last').copy()
  z=z.rename(columns={'date':'horizon_date','close':'restored_cache_close','cache_relative_path':'restored_cache_file_path','file_sha256':'restored_cache_file_sha256'})
  cols=['horizon_date','code','restored_cache_close','restored_cache_file_path','restored_cache_file_sha256']
  r=r.merge(z[cols],on=['horizon_date','code'],how='left',validate='many_to_one')
 else:
  r['restored_cache_close']=np.nan;r['restored_cache_file_path']='';r['restored_cache_file_sha256']=''
 r['cache_matches_trust_implied']=(np.isclose(r.restored_cache_close,r.trust_implied_horizon_close,rtol=0,atol=1e-6)&r.restored_cache_close.notna())
 r['cache_matches_r143']=(np.isclose(r.restored_cache_close,r.r143_horizon_close,rtol=0,atol=1e-6)&r.restored_cache_close.notna())
 r['historical_actual_cache_verified']=False
 r['restored_snapshot_only']=True
 r['audit_status']=np.where(r.restored_cache_close.notna(),'LATEST_RESTORED_PRICE_EVIDENCE_NOT_HISTORIC','NO_TARGET_DATE_IN_RESTORED_CACHE')
 csv_out(r,out/'R1D14_FROZEN_CASE_PRICE_CACHE_COMPARISON.csv')
 collision=pd.DataFrame([{'original_exact_code':'0088M0','lossy_legacy_code':lossy_code('0088M0'),'same_identity':False,'normalization_loss_proven':True,'historic_cache_selection_proven':False,'source':'FROZEN_CODE_READ_ONLY'}]);csv_out(collision,out/'R1D14_LOSSY_SECURITY_IDENTITY_GUARD.csv')
 matched=int(r.restored_cache_close.notna().sum());num_c=int(r.cache_matches_trust_implied.sum());num_j=int(r.cache_matches_r143.sum())
 manifest={'revision':VERSION,'protocol_version':lock['version'],'protocol_sha256':sha(Path(protocol).read_bytes()),'frozen_r1d13_rows':len(panel),'frozen_target_case_rows':len(r),'target_codes':list(TARGETS),'price_cache_scanned_files':count,'price_cache_candidate_files':len(selected),'price_cache_parsed_files':int((files.read_status=='PARSED_SELECTED_FRAME').sum()) if len(files) else 0,'frozen_case_horizon_date_cache_matches':matched,'restored_cache_match_trust_rows':num_c,'restored_cache_match_r143_rows':num_j,'cache_recovery_key':cache_key,'cache_recovered_snapshot':bool(count),'price_file_sha256_evidence_count':int(files.sha256.notna().sum()) if len(files) else 0,'cache_state':CACHE_STATUS_PRESENT if count else CACHE_STATUS_MISSING,'historic_original_cache_as_used_proven':False,'current_cache_is_historical_cache':False,'frozen_original_outcomes_corrected':False,'trading_price_corporate_action_certified':False,'causal_daily_cache_time_verified':False,'research_only':True,'new_independent_oos':False,'live_logic_changed':False,'orders_placed':0,'upstream_original_data_mutated':False,'future_data_used_for_signal':False}
 (out/'R1D14_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')
 (out/'R1D14_REPORT_KR.md').write_text(f'''# StockHunter R1-D14 — 원본 가격 캐시 복원 가능성 조사\n\n- R1-D13 고정 평가행 {len(panel):,}개, 핵심 의심 사례 평가행 {len(r):,}개.\n- 복원 캐시 스캔 파일 {count:,}개 / 파일명 기준 후보 {len(selected):,}개 / 정상 가격프레임 {manifest['price_cache_parsed_files']:,}개.\n- 같은 종목코드 및 날짜로 대조 가능한 가격 {matched:,}건 (TRUST 역산가 일치 {num_c:,}, R143 종가 일치 {num_j:,}).\n- cache restore key: `{cache_key or 'NONE / offline local'}`\n- **유의:** 현재 복원한 캐시가 2026년 9~10월 역사적 GitHub run 당시의 캐시라는 증거는 없습니다. 당시 가격 파일·해시는 기존 아티팩트에 보존되지 않았습니다. 0건이면 미확보이지 주가가 0원이라는 뜻이 아닙니다.\n- `0088M0 → 000880` 손실 정규화는 코드에서 확인됐지만, 잘못된 가격 파일이 실제 선택되었다는 결정적 증거는 현재까지 별도로 필요합니다.\n- 기존 LIVE 검색/점수/순위/주문 변경 0, 연구 원본 수정 0.\n''',encoding='utf-8')
 print('R1D14_CACHE_EVIDENCE_READONLY_COMPLETE',json.dumps({k:manifest[k] for k in ['price_cache_scanned_files','price_cache_candidate_files','frozen_case_horizon_date_cache_matches','historic_original_cache_as_used_proven','orders_placed']}))
 return manifest

def main():
 ap=argparse.ArgumentParser()
 ap.add_argument('--r1d13',required=True);ap.add_argument('--cache-root',required=True);ap.add_argument('--repo-root',default='.')
 ap.add_argument('--protocol',default='R1D14_PREDECLARED_PROTOCOL.json');ap.add_argument('--out',default='reports/r1d14/result');ap.add_argument('--cache-key',default='')
 a=ap.parse_args();audit(a.r1d13,a.cache_root,a.repo_root,a.protocol,a.out,a.cache_key)
if __name__=='__main__':main()
