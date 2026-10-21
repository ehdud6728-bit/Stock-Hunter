#!/usr/bin/env python3
"""Stage7-F: complement the 10 pinned KIND bodies with 11 previously unseen
trading-halt/period-change/continuation documents from the 57-source canary.

Research evidence ONLY. Direct notice coverage among the already observed 57
is not a historical all-notice/correction-chain completeness certificate.
No live/search/rank/order imports and no daily eligibility inference.
"""
from __future__ import annotations
import argparse,csv,hashlib,io,json,pathlib,re,time
from datetime import datetime,timezone
from bs4 import BeautifulSoup
import pit_stage7d_kind_full_document as d
import pit_stage7e_effective_event_ledger as e

SCHEMA='STOCKHUNTER_PIT_STAGE7F_DIRECT_HALT_NOTICE_CHAIN_CANARY_V1'
STAGE7C_RUN=39516839081
STAGE7D_RUN=39523929718
STAGE7E_RUN=39530243566
EXPECTED_NOTICES=57
EXPECTED_DIRECT=21
EXPECTED_OTHER=36
EXPECTED_OLD=10
EXPECTED_NEW=11
EXPECTED_RAW_NEW=33
SOURCE_RECEIPTS=tuple(e.POLICY)
SOURCE_CODES={'377460','418250'}


def fail(s):raise RuntimeError('FAIL_CLOSED_STAGE7F_'+str(s))
def sha(b):return hashlib.sha256(b).hexdigest()
def read(path):return json.loads(pathlib.Path(path).read_text(encoding='utf8'))
def rows(path):
 with pathlib.Path(path).open(encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))
def output(path,body):
 path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 if isinstance(body,str):body=body.encode('utf8')
 temp=path.with_name(path.name+'.part');temp.write_bytes(body);temp.replace(path)
def jwrite(p,x):output(p,json.dumps(x,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
def cwrite(p,x,fields):
 s=io.StringIO(newline='');w=csv.DictWriter(s,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(x);output(p,s.getvalue())


def direct_notice(title):
 """Exact title category, not loose occurrence of 'halt' in explanatory text."""
 return title.startswith('주권매매거래정지') or ('매매거래정지 지속' in title and title.startswith('기타시장안내('))


def classify(title):
 if title.startswith('주권매매거래정지해제'):return 'RELEASE_TITLE'
 if title.startswith('주권매매거래정지기간변경'):return 'PERIOD_CHANGE_TITLE'
 if title.startswith('주권매매거래정지('):return 'START_TITLE'
 if '매매거래정지 지속' in title:return 'CONTINUATION_TITLE'
 return 'OTHER_MARKET_NOTICE_UNREVIEWED'


def incident_link(code,day):
 if code=='377460':return 'Q377460_01' if day<'2023-11-27' else 'Q377460_02'
 if code=='418250':return 'S418250_01' if day>='2024-04-05' else 'UNKNOWN_BEFORE_KNOWN_START'
 fail('UNEXPECTED_CODE:'+code)


def audited_sources(stage7c,stage7d,stage7e):
 """Replay all 14 original list HTML and 30 original document SHA objects."""
 indexed,provenance=d.stage7c_preflight(stage7c)
 if len(indexed)!=EXPECTED_NOTICES:fail('LIST_COUNT_DRIFT')
 direct={ac:n for ac,n in indexed.items() if direct_notice(n['title'])}
 other={ac:n for ac,n in indexed.items() if ac not in direct}
 if len(direct)!=EXPECTED_DIRECT or len(other)!=EXPECTED_OTHER or not set(SOURCE_RECEIPTS)<=set(direct):
  fail('HALT_DOCUMENT_TITLE_COHORT_DRIFT')
 if len(direct)-len(SOURCE_RECEIPTS)!=EXPECTED_NEW:fail('NEW_DOCUMENT_COUNT_DRIFT')
 if sum(classify(n['title'])=='RELEASE_TITLE' for n in direct.values())!=1:fail('RELEASE_COUNT_DRIFT')
 if len({n['accession'] for n in indexed.values()})!=57:fail('DUPLICATE_RECEIPT')
 for ac,n in indexed.items():
  if (n['code'] not in SOURCE_CODES or len(ac)!=14 or not ac.isdigit() or
      n['submission_date_from_accession']!=f'{ac[:4]}-{ac[4:6]}-{ac[6:8]}'):
   fail('BAD_NOTICE_IDENTIFIERS:'+ac)
 original,docrows,rawshas,docsum=e.source7d(stage7d)
 if len(original)!=30 or len(docrows)!=EXPECTED_OLD:fail('OLD_FULL_DOC_SOURCE_COUNT_DRIFT')
 eroot=pathlib.Path(stage7e)
 es=read(eroot/'stage7e_official_document_event_ledger_summary.json')
 if (es.get('status')!='OFFICIAL_FULL_DOCUMENT_EFFECTIVE_DATES_EXTRACTED_EVENT_CHAIN_NOT_APPROVED'
     or es.get('source_stage7c_run')!=STAGE7C_RUN or es.get('source_stage7d_run')!=STAGE7D_RUN
     or es.get('distinct_incidents')!=3 or es.get('full_document_actions_extracted')!=10
     or es.get('complete_event_correction_chain_verified') is not False
     or es.get('strategy_ready') is not False):fail('STAGE7E_SOURCE_GATE_DRIFT')
 erows=rows(eroot/'official_document_dated_actions_10.csv')
 ledgers=rows(eroot/'three_halt_incidents_document_dates.csv')
 if len(erows)!=10 or len(ledgers)!=3 or set(x['receipt'] for x in erows)!=set(SOURCE_RECEIPTS):
  fail('STAGE7E_LEDGER_COUNT_DRIFT')
 for r in erows:
  ac=r['receipt']
  if r['official_full_body_sha256']!=rawshas[ac,'full_body'] or r['code']!=indexed[ac]['code'] or r['strategy_ready']!='False':
   fail('STAGE7E_VS_STAGE7D_SOURCE_SHA_DRIFT:'+ac)
 for r in ledgers:
  if r['event_id'] not in e.START_DATES or r['day_level_official_halt_state']!='UNKNOWN' or r['execution_feasibility']!='UNKNOWN' or r['date_interval_complete_certification']!='False':
   fail('STAGE7E_DAILY_GATE_DRIFT')
 return direct,other,docrows,rawshas,provenance,es


def inspect_body(raw,notice):
 """Inspect source, archive labeled verbatim rows; NO derived trading dates."""
 meta=d.interpret_document(raw,notice)
 if meta['parse_state']!='DOCUMENT_CAPTURED_CANDIDATES_ONLY':
  fail('OFFICIAL_BODY_IDENTITY_OR_ACTION_NOT_CORROBORATED:'+notice['accession'])
 soup=BeautifulSoup(raw.decode('utf8'),'html.parser')
 table=[]
 for tr in soup.find_all('tr'):
  cells=tr.find_all(['th','td'],recursive=False)
  if cells:
   x=[re.sub(r'\s+',' ',c.get_text(' ',strip=True))[:700] for c in cells]
   if any(re.search(r'정지|해제|기간|변경|개선|심사|거래|대상',v) for v in x):table.append(x)
 if not table:fail('NO_STRUCTURED_MARKET_ACTION_BODY:'+notice['accession'])
 return {'document_action_table_rows_for_manual_review':table[:45],
   'body_contains_release_string_not_evidence':('거래정지해제' in soup.get_text() or '거래재개' in soup.get_text()),
   'body_bytes':len(raw),'body_sha256':sha(raw),
   'body_identity_corroborated':True,'official_effective_date_certified':False,
   'document_text_candidate_preview':meta['body_text_preview_for_review'][:1900]}


def notice_output(indexed,docrows,rawshas,extras):
 result=[]
 for ac,n in sorted(indexed.items(),key=lambda p:(p[1]['submission_date_from_accession'],p[0])):
  row={k:n[k] for k in ('code','accession','title','submission_date_from_accession','listed_cell_time','notice_class','source_raw_sha256','source_raw_file')}
  row.update({'classification_from_title_only':classify(n['title']),
      'event_id_candidate_not_verified':incident_link(n['code'],n['submission_date_from_accession']),
      'full_document_origin':'STAGE7D_REPLAY' if ac in docrows else ('STAGE7F_NEW_FETCH' if ac in extras else 'NOT_REQUESTED_OTHER_MARKET'),
      'official_effective_datetime_verified':False,'official_daily_halt_status':'UNKNOWN',
      'correction_chain_complete':False,'strategy_ready':False})
  if ac in docrows:
   row['full_document_sha256']=rawshas[ac,'full_body']
   row['document_resolution_state']='SOURCE_DOCUMENT_CAPTURED_STAGED_PRIOR'
  else:
   record=extras.get(ac,{})
   row['full_document_sha256']=record.get('body_sha256','')
   row['document_resolution_state']=record.get('status','UNRESOLVED')
  result.append(row)
 return result


def run(stage7c,stage7d,stage7e,out,provider=None,pause=0.65):
 dest=pathlib.Path(out);dest.mkdir(parents=True,exist_ok=True)
 direct,other,docrows,rawshas,source,es=audited_sources(stage7c,stage7d,stage7e)
 provider=provider or d.KindProvider()
 wanted=[ac for ac in sorted(direct) if ac not in docrows]
 if len(wanted)!=EXPECTED_NEW:fail('TARGET_DIFFERENCE_DRIFT')
 traces={};rawmf=[];n_error=0
 for k,ac in enumerate(wanted,1):
  notice=direct[ac];entry={'code':notice['code'],'receipt':ac,'notice_title':notice['title'],
   'submission_date':notice['submission_date_from_accession'],
   'notice_time_display':notice['listed_cell_time'],'original_list_source_sha256':notice['source_raw_sha256'],
   'title_action_only':classify(notice['title']),'event_id_candidate':incident_link(notice['code'],notice['submission_date_from_accession']),
   'status':'UNRESOLVED','official_daily_halt_status':'UNKNOWN','resume_effective_date_verified':False,
   'complete_correction_chain_verified':False,'strategy_ready':False}
  def record(typ,buf,ext):
   rel=f'raw_kind_document/{ac}_{typ}.{ext}'
   d.write(dest/rel,buf)
   rawmf.append({'receipt':ac,'type':typ,'path':rel,'sha256':sha(buf),'bytes':len(buf)})
   jwrite(dest/'stage7f_new_raw_sha_manifest.json',{'schema':SCHEMA,'status':'PARTIAL','files':rawmf})
  try:
   shell=provider.shell(ac);record('viewer_shell',shell,'html')
   ids=d.resolve_shell(shell,ac)
   resolved=None
   for idx,cid in enumerate(ids):
    b=provider.resolve(cid);record(f'searchContents_{idx:02d}',b,'html')
    try:resolved=d.resolve_response(b,ac);break
    except RuntimeError:continue
   if not resolved:fail('NO_VALID_SOURCE_DOCUMENT_URL:'+ac)
   body=provider.content(resolved);record('full_body',body,'htm')
   entry.update(inspect_body(body,notice))
   entry['resolved_content_url']=resolved
   entry['status']='FULL_BODY_COLLECTED_REVIEW_ONLY'
  except Exception as exc:
   n_error+=1
   entry['diagnostic']=type(exc).__name__+': '+str(exc)[:330]
  traces[ac]=entry
  jwrite(dest/'stage7f_11_document_review_candidates.json',list(traces.values()))
  print('STAGE7F_DIRECT_DOCUMENT',k,len(wanted),ac,entry['status'],flush=True)
  if pause and k<len(wanted):time.sleep(pause)
 # Preserve complete cross-sectional notice chronology for review, even on failures.
 chronological=notice_output(direct,docrows,rawshas,traces)
 other_rows=notice_output(other,{}, {},{})
 cwrite(dest/'stage7f_21_direct_halt_notice_chronology.csv',chronological,list(chronological[0]))
 cwrite(dest/'stage7f_36_other_market_notices_UNREVIEWED.csv',other_rows,list(other_rows[0]))
 jwrite(dest/'stage7f_new_raw_sha_manifest.json',{'schema':SCHEMA,
      'status':'COMPLETE' if n_error==0 and len(rawmf)>=EXPECTED_RAW_NEW else 'PARTIAL',
      'files':rawmf})
 # Verdict deliberately independent of successful source retrieval. NOT a daily-halt approval.
 summary={'schema':SCHEMA,'status':('ALL_21_DIRECT_HALT_TITLES_BODIES_COLLECTED_CORRECTION_CHAIN_STILL_UNVERIFIED'
       if n_error==0 and len(rawmf)>=EXPECTED_RAW_NEW else 'DIRECT_HALT_DOCS_INCOMPLETE_FAIL_CLOSED'),
      'source_stage7c_run':STAGE7C_RUN,'source_stage7d_run':STAGE7D_RUN,
      'source_stage7e_run':STAGE7E_RUN,
      'replayed_stage7c_original_notice_html':source['verified_stage7c_raw_html'],
      'original_57_notice_receipts_sha_verified':source['verified_stage7c_notice_rows'],
      'replayed_stage7d_existing_raw_objects_sha_verified':len(rawshas),
      'replayed_stage7e_incidents':es['distinct_incidents'],
      'source_existing_full_bodies':len(docrows),'direct_halt_related_title_count':len(direct),
      'other_market_notices_still_not_full_body_reviewed':len(other),
      'new_full_documents_requested':len(wanted),
      'new_full_documents_captured':len(wanted)-n_error,'new_full_documents_unresolved':n_error,
      'new_source_raw_files':len(rawmf),
      'all_57_market_notice_list_exhaustive_independently_verified':False,
      'all_market_actions_types_in_kind_queried':False,
      'original_disclosure_versions_and_corrections_fully_verified':False,
      'publication_timestamps_independently_verified':False,
      'three_incidents_complete_chain_verified':False,
      'daily_tradability_verified':False,'official_day_halt_state':'UNKNOWN',
      'strategy_ready':False,'production_rank_search_order_unchanged':True,
      'research_only':True,'generated_at_utc':datetime.now(timezone.utc).isoformat(),
      'warning':'21 direct halt titles are from one 57-item, 2-category source cohort only. The remaining 36 market notices may alter context. Even 21/21 full documents do not prove complete correction chains or historical trade eligibility.'}
 jwrite(dest/'stage7f_coverage_and_fail_closed_summary.json',summary)
 print(json.dumps(summary,ensure_ascii=False,sort_keys=True),flush=True)
 return summary


def main():
 a=argparse.ArgumentParser();a.add_argument('--stage7c-dir',required=True);a.add_argument('--stage7d-dir',required=True)
 a.add_argument('--stage7e-dir',required=True);a.add_argument('--out',required=True);a.add_argument('--pause',type=float,default=.65)
 x=a.parse_args();s=run(x.stage7c_dir,x.stage7d_dir,x.stage7e_dir,x.out,pause=x.pause)
 return 0 if s['new_full_documents_unresolved']==0 and s['new_source_raw_files']>=EXPECTED_RAW_NEW else 2
if __name__=='__main__':raise SystemExit(main())
