#!/usr/bin/env python3
"""Stage7-E official KIND document-date extraction and per-incident ledger.

Research-only. Sources are exclusively immutable Stage7-B/C/D GitHub artifacts.
No KRX refetch and NO daily trading eligibility approval. Document effective dates
are NOT fully PIT-validated until the complete correction/notice chain is audited.
"""
from __future__ import annotations
import argparse,csv,hashlib,io,json,pathlib,re,sys
from collections import Counter
from datetime import datetime,timezone
from bs4 import BeautifulSoup

SCHEMA='STOCKHUNTER_PIT_STAGE7E_DOCUMENT_EVENT_LEDGER_V1'
RUN7B=39436389287
RUN7C=39516839081
RUN7D=39523929718
PINNED7C_SHA='a88af6dbde14d4413f228fcd8b7c5e692fe48d7b'
PINNED7D_SOURCE7C_MANIFEST='d179016de7f5ec6181a0df27c6a49e1bcd374bd39e85969111130ee365c1ac57'
VALID_CODES={'377460','418250'}
POLICY={
 '20231026000748':('377460','HALT_START','Q377460_01'),
 '20231120000599':('377460','HALT_RELEASE','Q377460_01'),
 '20231127000588':('377460','HALT_START','Q377460_02'),
 '20240110000746':('377460','HALT_PERIOD_CHANGE','Q377460_02'),
 '20260918000770':('377460','HALT_PERIOD_CHANGE','Q377460_02'),
 '20240405000391':('418250','HALT_START','S418250_01'),
 '20240405000982':('418250','HALT_PERIOD_CHANGE','S418250_01'),
 '20250124000730':('418250','CONTINUATION_NOTICE','S418250_01'),
 '20250124000734':('418250','HALT_PERIOD_CHANGE','S418250_01'),
 '20260722000930':('418250','HALT_PERIOD_CHANGE','S418250_01'),
}
START_DATES={'Q377460_01':('2023-10-27',''), 'Q377460_02':('2023-11-28',''), 'S418250_01':('2024-04-05','14:11:00')}
DATE=re.compile(r'\d{4}-\d{2}-\d{2}\Z')
TIME=re.compile(r'(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d\Z')

def stop(message):raise RuntimeError('FAIL_CLOSED_STAGE7E_'+str(message))
def sha(b):return hashlib.sha256(b).hexdigest()
def load(path):return json.loads(pathlib.Path(path).read_text(encoding='utf8'))
def asbool(value):return value is True or value=='True'
def crows(path):
 with open(path,encoding='utf-8-sig',newline='') as fd:return list(csv.DictReader(fd))
def save(path,obj):
 p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 p.write_text(json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf8')
def save_csv(path,cols,rows):
 p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(rows)
def valid_date(s):
 if not DATE.fullmatch(str(s)):stop('UNRECOGNIZED_DATE:'+str(s))
 try: datetime.strptime(s,'%Y-%m-%d')
 except ValueError:stop('INVALID_DATE:'+s)
 return s

def source7d(root):
 root=pathlib.Path(root)
 s=load(root/'stage7d_kind_full_document_summary.json')
 if (s.get('status')!='KIND_FULL_DOCUMENT_CANARY_COLLECTED_CANDIDATE_EVIDENCE_ONLY' or
  s.get('requested_official_receipts')!=10 or s.get('full_document_resolved_count')!=10 or
  s.get('retrieved_raw_provider_files')!=30 or s.get('document_unresolved_count')!=0 or
  s.get('source_stage7c',{}).get('source_stage7c_run')!=RUN7C or
  s.get('source_stage7c',{}).get('source_stage7c_commit')!=PINNED7C_SHA or
  s.get('source_stage7c',{}).get('stage7c_manifest_sha256')!=PINNED7D_SOURCE7C_MANIFEST or
  s.get('strategy_ready') is not False or s.get('official_resume_effective_date_verified') is not False):
  stop('STAGE7D_PROVENANCE_OR_GATE_DRIFT')
 m=load(root/'raw_full_document_sha_manifest.json')
 if not isinstance(m.get('files'),list) or len(m['files'])!=30:stop('RAW_FILE_COUNT_DRIFT')
 observed={};rec_sha={}
 for f in m['files']:
  ac=f.get('receipt');typ=f.get('type');rel=f.get('path','')
  if (ac not in POLICY or typ not in ('viewer_shell','searchContents_00','full_body') or
      rel!=f'raw_kind_document/{ac}_{typ}.htm'.replace('_viewer_shell.htm','_viewer_shell.html').replace('_searchContents_00.htm','_searchContents_00.html') or
      (ac,typ) in observed):stop('RAW_PATH_OR_IDENTITY_INVALID:'+str(ac)+':'+str(typ))
  raw=(root/rel).read_bytes()
  if sha(raw)!=f.get('sha256') or len(raw)!=f.get('bytes'):stop('RAW_BYTES_SHA_MISMATCH:'+rel)
  observed[ac,typ]=raw
  rec_sha[ac,typ]=f['sha256']
 if set(a for a,_ in observed)!=set(POLICY) or len(observed)!=30:stop('INCOMPLETE_RECEIPT_TRIPLES')
 rows=crows(root/'stage7d_full_notice_document_audit.csv')
 if len(rows)!=10 or {r['accession'] for r in rows}!=set(POLICY):stop('DOCUMENT_AUDIT_COUNT_DRIFT')
 mapped={}
 for row in rows:
  ac=row['accession'];code,typ,event=POLICY[ac]
  if (row.get('code')!=code or row.get('raw_document_body_sha256')!=rec_sha[ac,'full_body'] or
      row.get('parse_state')!='DOCUMENT_CAPTURED_CANDIDATES_ONLY' or
      not asbool(row.get('body_corroborates_official_company_or_code')) or
      not asbool(row.get('document_mentions_market_action')) or
      asbool(row.get('official_resume_effective_date_verified')) or
      asbool(row.get('strategy_ready'))):stop('DOCUMENT_AUDIT_IDENTITY_DRIFT:'+ac)
  valid_date(row.get('notice_date',''))
  if row['notice_date']!=f'{ac[:4]}-{ac[4:6]}-{ac[6:8]}':stop('RECEIPT_DATE_DRIFT:'+ac)
  try:datetime.strptime(row['notice_time_display'],'%Y-%m-%d %H:%M')
  except (ValueError,KeyError):stop('DISPLAY_TIME_INVALID:'+ac)
  if not (row.get('resolved_content_url','').startswith('https://kind.krx.co.kr/external/'+ac[:4]+'/'+ac[4:6]+'/'+ac[6:8]+'/')):
   stop('UNTRUSTED_DOC_URL:'+ac)
  mapped[ac]=row
 return observed,mapped,rec_sha,s

def source7c(root):
 root=pathlib.Path(root);s=load(root/'stage7c_kind_resume_source_summary.json')
 if s.get('status')!='KIND_MARKET_DISCLOSURE_SOURCE_CANDIDATES_ONLY' or s.get('notice_candidate_count')!=57 or s.get('strategy_ready') is not False:stop('STAGE7C_SUMMARY_DRIFT')
 rows=crows(root/'all_kind_market_notice_candidates.csv')
 if len(rows)!=57:stop('STAGE7C_57_NOTICE_MISSING')
 by={}
 for row in rows:
  ac=row['accession']
  if ac in by:stop('STAGE7C_DUPLICATE_RECEIPT:'+ac)
  by[ac]=row
 if not set(POLICY)<=set(by):stop('STAGE7C_TARGET_MISSING')
 for ac,(code,_,_) in POLICY.items():
  r=by[ac]
  if r['code']!=code or r['title']=='' or r['submission_date_from_accession']!=f'{ac[:4]}-{ac[4:6]}-{ac[6:8]}':
   stop('STAGE7C_RECEIPT_CONFLICT:'+ac)
 return by

def source7b(root):
 root=pathlib.Path(root);s=load(root/'stage7b_halt_price_bridge_summary.json')
 if s.get('status')!='OFFICIAL_HALT_START_VS_IMMUTABLE_PRICE_OBSERVATION_MATCH_ONLY' or s.get('focus_price_observations')!=1809 or s.get('strategy_ready') is not False:stop('STAGE7B_STATUS_DRIFT')
 rows=crows(root/'stage7b_2_codes_all_observed_price_days.csv');seq={}
 if len(rows)!=1809:stop('STAGE7B_FOCUS_ROWS_CHANGED')
 for r in rows:
  code,day=r['code'],r['date']
  if code not in VALID_CODES:stop('UNEXPECTED_PRICE_CODE')
  valid_date(day)
  key=(code,day)
  if key in seq:stop('DUPLICATE_PRICE_DAY:'+str(key))
  try:volume=int(r['volume']);amount=int(r['amount'])
  except ValueError:stop('BAD_PRICE_NUMERIC')
  if volume<0 or amount<0 or r['official_daily_halt_status']!='UNKNOWN' or r['tradability_for_backtest']!='UNKNOWN' or asbool(r['strategy_ready']):stop('PRICE_OBSERVATION_MISREPRESENTED')
  seq[key]={'volume':volume,'amount':amount}
 if sum(c== '377460' for c,d in seq)!=1050 or sum(c=='418250' for c,d in seq)!=759:stop('CODE_COUNTS_DRIFT')
 if seq['377460','2023-11-21']['volume']!=9889897 or seq['418250','2024-04-05']['volume']!=51071:stop('PINNED_PRICE_ANCHORS_CHANGED')
 return seq

def table_cells(body:bytes):
 soup=BeautifulSoup(body,'html.parser')
 triples=[]
 for tr in soup.find_all('tr'):
  cells=tr.find_all(['th','td'],recursive=False)
  if cells:triples.append([re.sub(r'\s+',' ',v.get_text(' ',strip=True)).strip() for v in cells])
 return triples,re.sub(r'\s+',' ',soup.get_text(' ',strip=True))

def parse_one(ac,body,audit_row):
 code,action,event=POLICY[ac]
 rows,txt=table_cells(body)
 if not rows or len(txt)<55:stop('INVALID_BODY_TABLE:'+ac)
 if action=='HALT_START':key='가.정지일시';required='1.대상종목'
 elif action=='HALT_RELEASE':key='3.해제일시';required='1.대상종목'
 elif action=='HALT_PERIOD_CHANGE':key='나.변경후';required='1.대상종목'
 else:key=None;required=None
 if required and not any(required in ' '.join(r) for r in rows):stop('SUBJECT_ROW_MISSING:'+ac)
 labels=[c for r in rows for c in r]
 if required:
  subject=' '.join(r for row in rows for r in row if '대상종목' not in r)[:4000]
  if ('위니아에이드' not in subject and '큐에이드' not in subject) if code=='377460' else '시큐레터' not in subject:
   stop('WRONG_ISSUE_IDENTITY:'+ac)
 actiondate='';actiontime='';condition=''
 if action in ('HALT_START','HALT_RELEASE'):
  matches=[r for r in rows if r and any(c.startswith(key) for c in r)]
  if len(matches)!=1:stop('ACTION_KEY_CARDINALITY:'+ac)
  r=matches[0];valid=[(i,v) for i,v in enumerate(r) if DATE.fullmatch(v)]
  if len(valid)!=1:stop('EFFECTIVE_DATE_COUNT_MISMATCH:'+ac+':'+str(r))
  pos,actiondate=valid[0];valid_date(actiondate)
  times=[v for v in r[pos+1:] if TIME.fullmatch(v)]
  if len(times)>1:stop('MULTIPLE_TIMES:'+ac)
  actiontime=times[0] if times else ''
 elif action=='HALT_PERIOD_CHANGE':
  matches=[r for r in rows if r and any(c.startswith('나.변경후') for c in r)]
  if len(matches)!=1:stop('CHANGED_INTERVAL_NOT_UNIQUE:'+ac)
  r=matches[0];condition=' '.join(r[1:])
  # This is a repeated original start, NOT the effective date of a resume.
  # Different patterns: 2023년 11월 28일 / 2024년 04월 05일 14:11:00.
  compact=re.sub(r'\s+',' ',condition)
  dates=re.search(r'(20\d{2})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일',compact)
  if not dates:stop('ORIGINAL_START_NOT_IN_CHANGED_INTERVAL:'+ac)
  actiondate=f'{int(dates[1]):04d}-{int(dates[2]):02d}-{int(dates[3]):02d}'
  valid_date(actiondate)
  after=compact[dates.end():dates.end()+25]
  match_time=re.search(r'(?<!\d)(\d{2}:\d{2}:\d{2})(?!\d)',after)
  actiontime=match_time[1] if match_time else ''
 elif action=='CONTINUATION_NOTICE':
  if not ('매매거래정지 지속' in txt or '매매거래정지지속' in txt):stop('CONTINUATION_KEYWORDS_NOT_FOUND')
  condition=txt[:280]
 else:stop('UNRECOGNIZED_ACTION:'+ac)
 if action!='CONTINUATION_NOTICE':
  expectdate,expecttime=START_DATES[event]
  if action=='HALT_RELEASE':
   if actiondate!='2023-11-21' or actiontime:stop('RELEASE_DATE_DRIFT:'+ac)
  elif actiondate!=expectdate or actiontime!=expecttime:stop('START_OR_CHANGED_DATE_DRIFT:'+ac)
 if action=='HALT_START' and '주권매매거래정지' not in txt:stop('BAD_START_HEADLINE:'+ac)
 if action=='HALT_RELEASE' and '주권매매거래정지해제' not in txt:stop('BAD_RELEASE_HEADLINE:'+ac)
 if action=='HALT_PERIOD_CHANGE' and '기간변경' not in txt:stop('BAD_CHANGE_HEADLINE:'+ac)
 return {'code':code,'receipt':ac,'event_id':event,'action':action,'official_notice_date':audit_row['notice_date'],
         'notice_display_time':audit_row['notice_time_display'],'notice_timestamp_independent_verified':False,
         'document_declared_effective_start_or_repeat_date':actiondate,
         'document_declared_effective_start_or_repeat_time':actiontime,
         'document_declared_resume_effective_date':actiondate if action=='HALT_RELEASE' else '',
         'document_repeated_original_start_not_new_resume':action=='HALT_PERIOD_CHANGE',
         'condition_excerpt_for_manual_review':condition[:480],
         'official_full_body_sha256':audit_row['raw_document_body_sha256'],
         'kind_source_url':audit_row['resolved_content_url'],
         'document_text_identified':True,'full_correction_chain_verified':False,
         'daily_tradability_verified':False,'strategy_ready':False}

def build_ledger(parsed,prices):
 by={a['receipt']:a for a in parsed};evs=[]
 for event,ids in [('Q377460_01',['20231026000748','20231120000599']),
                   ('Q377460_02',['20231127000588','20240110000746','20260918000770']),
                   ('S418250_01',['20240405000391','20240405000982','20250124000730','20250124000734','20260722000930'])]:
  code='418250' if event.startswith('S') else '377460'
  linked=[by[ac] for ac in ids];start=linked[0]
  if start['action']!='HALT_START' or start['document_declared_effective_start_or_repeat_date']!=START_DATES[event][0]:stop('EVENT_START_CONFLICT:'+event)
  release=[x for x in linked if x['action']=='HALT_RELEASE']
  if len(release)>1:stop('AMBIGUOUS_RELEASE_EVENTS')
  effective=release[0]['document_declared_resume_effective_date'] if release else ''
  if effective and (effective<=start['document_declared_effective_start_or_repeat_date'] or effective not in [d for (c,d) in prices if c==code]):stop('BAD_RELEASE_EFFECTIVE_DATE:'+event)
  startdate=start['document_declared_effective_start_or_repeat_date']
  anchor=prices.get((code,startdate));resume_anchor=prices.get((code,effective)) if effective else None
  if not anchor:stop('MISSING_EVENT_PRICE_DAY:'+event)
  if event=='Q377460_01' and (anchor['volume']!=0 or resume_anchor['volume']!=9889897):stop('FIRST_EVENT_PRICE_ANCHORS_WRONG')
  if event=='Q377460_02' and anchor['volume']!=0:stop('REHALT_PRICE_ANCHOR_WRONG')
  if event=='S418250_01' and anchor['volume']!=51071:stop('INTRADAY_EVENT_PRICE_ANCHOR_WRONG')
  evs.append({'event_id':event,'code':code,'official_halt_start_date_from_body':startdate,
   'official_halt_start_time_from_body':start['document_declared_effective_start_or_repeat_time'],
   'official_release_effective_date_from_body':effective,
   'official_release_time_from_body':release[0]['document_declared_effective_start_or_repeat_time'] if release else '',
   'halt_start_receipt':start['receipt'], 'release_receipt':release[0]['receipt'] if release else '',
   'linked_receipts':ids,'related_notice_count':len(ids),
   'start_day_market_volume_observed':anchor['volume'],
   'release_day_market_volume_observed':resume_anchor['volume'] if resume_anchor else '',
   'official_body_date_extracted':True,'body_date_independent_correction_chain_verified':False,
   'date_interval_complete_certification':False,
   'day_level_official_halt_state':'UNKNOWN','execution_feasibility':'UNKNOWN',
   'event_status':'SOURCE_DOCUMENT_DATE_EXTRACTED_REQUIRES_CORRECTION_CHAIN_REVIEW',
   'research_only':True,'strategy_ready':False})
 return evs

def run(stage7b,stage7c,stage7d,out):
 out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
 bodies,ds,shas,summaryd=source7d(stage7d)
 cs=source7c(stage7c)
 prices=source7b(stage7b)
 parsed=[]
 for ac in POLICY:
  if cs[ac]['title']!=ds[ac]['notice_title'] or cs[ac]['listed_cell_time']!=ds[ac]['notice_time_display']:stop('SOURCE_CROSS_IDENTITY_CONFLICT:'+ac)
  row=parse_one(ac,bodies[ac,'full_body'],ds[ac]);parsed.append(row)
 ledger=build_ledger(parsed,prices)
 save_csv(out/'official_document_dated_actions_10.csv',list(parsed[0]),parsed)
 save_csv(out/'three_halt_incidents_document_dates.csv',list(ledger[0]),ledger)
 labels={('377460','2023-10-27'):'Q377460_01',('377460','2023-11-21'):'Q377460_01',('377460','2023-11-28'):'Q377460_02',('418250','2024-04-05'):'S418250_01'}
 anchors=[]
 for (code,day),eid in labels.items():
  anchor=prices.get((code,day))
  if not anchor:stop('REQUIRED_PRICE_ANCHOR_MISSING')
  anchors.append({'event_id':eid,'code':code,'date':day,'price_volume':anchor['volume'],'price_amount':anchor['amount'],
                  'official_daily_halt_state':'UNKNOWN','daily_tradability':'UNKNOWN','price_is_not_official_resume_proof':True})
 save_csv(out/'four_daily_observation_anchors_RESEARCH_ONLY.csv',list(anchors[0]),anchors)
 result={'schema':SCHEMA,'status':'OFFICIAL_FULL_DOCUMENT_EFFECTIVE_DATES_EXTRACTED_EVENT_CHAIN_NOT_APPROVED',
  'source_stage7b_run':RUN7B,'source_stage7c_run':RUN7C,'source_stage7d_run':RUN7D,
  'verified_stage7d_raw_source_sha_files':30,'verified_stage7c_receipt_rows':57,
  'verified_stage7b_price_observations':1809,'full_document_actions_extracted':len(parsed),
  'distinct_incidents':len(ledger),'halt_start_actions':sum(x['action']=='HALT_START' for x in parsed),
  'document_release_actions':sum(x['action']=='HALT_RELEASE' for x in parsed),
  'period_change_actions':sum(x['action']=='HALT_PERIOD_CHANGE' for x in parsed),
  'continuation_notice_actions':sum(x['action']=='CONTINUATION_NOTICE' for x in parsed),
  'first_incident_document_release_date':'2023-11-21','seculeter_intraday_start':'2024-04-05 14:11:00',
  'complete_event_correction_chain_verified':False,'official_daily_status_coverage_verified':False,
  'event_timestamp_independently_verified':False,'all_historical_security_types_verified':False,
  'lookahead_safe_daily_strategy_filter_approved':False,'strategy_ready':False,
  'research_only':True,'production_rank_search_order_unchanged':True,
  'note':'These are official document-stated effective dates, not certified complete trading intervals; all daily eligibility remains UNKNOWN. Later continuation/change notices were not silently treated as releases.',
  'generated_at_utc':datetime.now(timezone.utc).isoformat()}
 save(out/'stage7e_official_document_event_ledger_summary.json',result)
 save(out/'stage7e_receipt_sha_provenance.json',{'source_stage7d_raw_manifest_sha256':sha((pathlib.Path(stage7d)/'raw_full_document_sha_manifest.json').read_bytes()),
   'source_stage7c_candidate_csv_sha256':sha((pathlib.Path(stage7c)/'all_kind_market_notice_candidates.csv').read_bytes()),
   'source_stage7b_price_csv_sha256':sha((pathlib.Path(stage7b)/'stage7b_2_codes_all_observed_price_days.csv').read_bytes()),
   'receipt_full_body_sha256':{ac:shas[ac,'full_body'] for ac in POLICY}})
 print(json.dumps(result,ensure_ascii=False,sort_keys=True),flush=True)
 return result

def main():
 p=argparse.ArgumentParser()
 p.add_argument('--stage7b-dir',default='stage7b-source')
 p.add_argument('--stage7c-dir',default='stage7c-source')
 p.add_argument('--stage7d-dir',default='stage7d-source')
 p.add_argument('--out',default='reports/pit-stage7e')
 a=p.parse_args();run(a.stage7b_dir,a.stage7c_dir,a.stage7d_dir,a.out)
if __name__=='__main__':
 try:main()
 except Exception as e:
  print('FAIL_CLOSED_STAGE7E',type(e).__name__,str(e),file=sys.stderr,flush=True)
  sys.exit(2)
