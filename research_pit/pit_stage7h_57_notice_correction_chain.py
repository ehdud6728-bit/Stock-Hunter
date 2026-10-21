#!/usr/bin/env python3
"""StockHunter PIT Stage7-H: OFFLINE, SHA-strict, evidence-only 57-notice correction audit.

Never turns notice language, content versions or old price rows into historical
tradability; no KRX network and no LIVE/ranking/order integration. Two securities
and two KIND notice categories, NOT an exhaustive historic notice master.
"""
from __future__ import annotations
import argparse,csv,hashlib,io,json,pathlib,re,collections
from bs4 import BeautifulSoup
import pit_stage7d_kind_full_document as d
import pit_stage7f_full_halt_notice_chain as f
import pit_stage7g_other_market_notices as g

SCHEMA='STOCKHUNTER_PIT_STAGE7H_SHA_PINNED_57_NOTICE_CORRECTION_CANDIDATE_AUDIT_V1'
RUNS={'C':39516839081,'D':39523929718,'E':39530243566,'F':39541621602,'G':39570031470}
STATES={'UNKNOWN'}
COUNT={'c':57,'d':10,'f':11,'g':36,'raw_d':30,'raw_f':33,'raw_g':109}


def fail(detail):raise RuntimeError('FAIL_CLOSED_STAGE7H_'+str(detail))
def sha(b):return hashlib.sha256(b).hexdigest()
def readj(path):return json.loads(pathlib.Path(path).read_text(encoding='utf-8'))
def store(path,obj):
 p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 p.write_text(json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')
def csvwrite(path,rows,columns):
 p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w',newline='',encoding='utf-8-sig') as fp:
  w=csv.DictWriter(fp,fieldnames=columns,extrasaction='ignore');w.writeheader();w.writerows(rows)

def check_manifest(root,manifest_name,prefix,allowed_receipts,expected_files):
 root=pathlib.Path(root);manifest=readj(root/manifest_name)
 if manifest.get('status')!='COMPLETE' or len(manifest.get('files',[]))!=expected_files:
  fail('SOURCE_MANIFEST_NOT_COMPLETE_'+prefix)
 seen=set();by={};bodies={};shells={}
 for x in manifest['files']:
  ac=x['receipt'];typ=x['type'];rel=x['path'];n=x.get('bytes')
  if ac not in allowed_receipts or not re.fullmatch(r'\d{14}',ac):fail('UNKNOWN_RECEIPT_'+prefix+':'+str(ac))
  if typ not in {'full_body','viewer_shell'} and not re.fullmatch(r'searchContents_\d\d',typ):fail('RAW_TYPE_'+str(typ))
  ext='htm' if typ=='full_body' else 'html'
  expected_path=f'{prefix}/{ac}_{typ}.{ext}'
  if rel!=expected_path or rel in seen:fail('RAW_PATH_OR_DUPLICATE_'+str(rel))
  seen.add(rel);body=(root/rel).read_bytes()
  if sha(body)!=x['sha256'] or len(body)!=n or n<=0:fail('RAW_SHA_OR_BYTES_TAMPER_'+str(rel))
  by.setdefault(ac,set()).add(typ)
  if typ=='full_body':bodies[ac]=(body,x['sha256'])
  if typ=='viewer_shell':shells[ac]=(body,x['sha256'])
 if set(by)!=set(allowed_receipts) or set(bodies)!=set(allowed_receipts) or set(shells)!=set(allowed_receipts):
  fail('RAW_COHORT_NOT_EXACT_'+prefix)
 if any(not {'full_body','viewer_shell'}<=s for s in by.values()):fail('RECEIPT_RAW_INCOMPLETE_'+prefix)
 return bodies,shells,sha((root/manifest_name).read_bytes())

def text_from_body(body):
 try: raw=body.decode('utf-8')
 except UnicodeDecodeError:
  try:raw=body.decode('euc-kr')
  except UnicodeDecodeError:fail('UNKNOWN_BODY_CHARSET')
 soup=BeautifulSoup(raw,'html.parser')
 for item in soup(['script','style','noscript']):item.decompose()
 return re.sub(r'\s+',' ',soup.get_text(' ',strip=True))

def version_flags(shell):
 """Raw viewer version options are candidates; not a complete historical timeline."""
 text=shell.decode('euc-kr',errors='replace')
 options=re.findall(r'<option\b[^>]*\bvalue\s*=\s*[\'\"](\d{14})\|([YN])[\'\"]',text,re.I)
 unique={}
 for a,b in options:
  if a in unique and unique[a]!=b:fail('CONTRADICTORY_VERSION_FLAG_'+a)
  unique[a]=b
 return [{'content_id':cid,'current_viewer_flag':flag} for cid,flag in unique.items()]

def is_correction(text):
 normalized=re.sub(r'\s+','',text)
 return bool(('정정신고' in normalized or '정정일자' in normalized) and
             any(token in normalized for token in ('정정전','정정사항','정정관련공시서류','정정사유')))

def corrective_claim(text):
 t=re.sub(r'\s+','',text)
 return any(k in t for k in ('정정전','정정후','정정사유','정정관련공시서류'))

def normalized_title(title):
 # Exact title identity up to whitespace only; do not merge different actions.
 return re.sub(r'\s+','',title or '').strip()

def correction_excerpt(text):
 t=re.sub(r'\s+',' ',text)
 i=t.find('정정전')
 return t[max(0,i-65):i+320] if i>=0 else t[:190]

def candidate_links(documents):
 """Conservative original/correction candidates, NOT confirmed global version chain."""
 groups=collections.defaultdict(list)
 for item in documents:
  groups[(item['code'],item['notice_date'],normalized_title(item['title']))].append(item)
 edges=[]
 for (code,date,title),members in groups.items():
  if len(members)<2:continue
  members=sorted(members,key=lambda x:x['receipt'])
  for newer in members:
   if not newer['correction_in_body']:continue
   for older in members:
    if older['receipt']>=newer['receipt']:continue
    v0=set(older['viewer_content_ids']);v1=set(newer['viewer_content_ids'])
    evidence=['same_code_date_exact_title','later_receipt_explicit_correction_body']
    if v0 and v0==v1:evidence.append('same_viewer_content_option_set')
    elif v0.intersection(v1):evidence.append('partial_viewer_content_option_overlap')
    edges.append({'code':code,'date':date,'original_receipt_candidate':older['receipt'],
                  'correction_receipt_candidate':newer['receipt'],
                  'evidence':evidence,'original_body_sha256':older.get('body_sha256',''),
                  'correction_body_sha256':newer.get('body_sha256',''),
                  'correction_excerpt':correction_excerpt(newer.get('body_text','')),
                  'relationship_verified_complete':False,
                  'official_halt_state':'UNKNOWN','lookahead_safe_approved':False})
 return edges

def label(item):
 title=item['title'].replace(' ','')
 if '주권매매거래정지해제' in title:return 'DIRECT_RELEASE_NOTICE_TITLE_REQUIRES_FULL_CHAIN'
 if '주권매매거래정지기간변경' in title:return 'DIRECT_PERIOD_CHANGE_NOT_RELEASE'
 if '주권매매거래정지' in title:return 'DIRECT_HALT_NOTICE_TITLE'
 if item['correction_in_body']:return 'CONTEXT_EXPLICIT_CORRECTION_NOTICE'
 if item['release_mention_only']:return 'CONTEXT_RELEASE_WORD_ONLY_NOT_PROOF'
 if '개선기간' in item['body_text'] or '상장폐지' in item['body_text']:
  return 'CONTEXT_MARKET_REVIEW_NO_AUTO_RELEASE'
 return 'CONTEXT_OTHER_MARKET_NOTICE'

def audit(cdir,ddir,edir,fdir,gdir,out):
 target=pathlib.Path(out);target.mkdir(parents=True,exist_ok=True)
 other,direct,pins=g.source_preflight(cdir,ddir,edir,fdir)
 if len(other)!=COUNT['g'] or len(direct)!=21:fail('SOURCE_57_COHORT_DRIFT')
 all_notices={**direct,**other}
 if len(all_notices)!=COUNT['c']:fail('OVERLAPPING_COHORT_RECEIPTS')
 gd=readj(pathlib.Path(gdir)/'stage7g_fail_closed_summary.json')
 if not (gd.get('schema')==g.SCHEMA and
         gd.get('status')=='57_COHORT_NOTICE_BODIES_COVERED_BUT_HISTORICAL_CORRECTION_CHAIN_UNVERIFIED' and
         gd.get('selected_cohort_57_document_body_coverage')==57 and
         gd.get('other_market_notices_captured')==36 and
         gd.get('other_market_notices_unresolved')==0 and
         gd.get('new_source_raw_objects')==109 and
         gd.get('source_stage7f_run')==RUNS['F'] and
         all(gd.get('source_stage7'+k.lower()+'_run')==RUNS[k] for k in ('C','D','E')) and
         gd.get('strategy_ready') is False and
         gd.get('daily_tradability_verified') is False and
         gd.get('all_original_and_correction_versions_verified') is False):fail('STAGE7G_SUMMARY_NOT_PINNED')
 gbody,gshell,gsha=check_manifest(gdir,'stage7g_new_raw_sha_manifest.json',
                                  'raw_kind_other',set(other),COUNT['raw_g'])
 droot=pathlib.Path(ddir)
 dra=readj(droot/'raw_full_document_sha_manifest.json')
 if dra.get('status')!='COMPLETE':fail('D_SOURCE_RAW_INCOMPLETE')
 d_receipts={x['receipt'] for x in dra['files'] if x['type']=='full_body'}
 if len(d_receipts)!=COUNT['d'] or not d_receipts<=set(direct):fail('D_BODY_RECEIPT_COHORT')
 dbody,dshell,dsha=check_manifest(ddir,'raw_full_document_sha_manifest.json',
                                   'raw_kind_document',d_receipts,COUNT['raw_d'])
 fbody,fshell,fsha=check_manifest(fdir,'stage7f_new_raw_sha_manifest.json',
                                   'raw_kind_document',set(direct)-d_receipts,COUNT['raw_f'])
 docs=[]
 for ac in sorted(all_notices):
  src=all_notices[ac]
  if ac in dbody: b,hashb=dbody[ac]; sh,_=dshell[ac];lane='7D'
  elif ac in fbody:b,hashb=fbody[ac];sh,_=fshell[ac];lane='7F'
  else:b,hashb=gbody[ac];sh,_=gshell[ac];lane='7G'
  txt=text_from_body(b)
  if ac in other:
   result=g.review_body(b,src)
   if result['document_body_sha256']!=hashb or not result['company_identity_corroborated']:
    fail('G_DOC_BODY_IDENTITY_'+ac)
  else:
   # Reconfirm original direct document identity in previously pinned D/F audit.
   compact=re.sub(r'\s+','',txt)
   code=src['code']
   if code not in txt and not any(alias.replace(' ','') in compact for alias in d.prior.CODE_ALIASES[code]):
    fail('DIRECT_DOC_BODY_IDENTITY_'+ac)
  flags=version_flags(sh)
  docs.append({'code':src['code'],'receipt':ac,'notice_date':src['submission_date_from_accession'],
     'title':src['title'],'notice_time_display_not_independently_verified':src['listed_cell_time'],
     'source_lane':lane,'listed_html_sha256':src['source_raw_sha256'],
     'body_sha256':hashb,'viewer_sha256':sha(sh),
     'viewer_content_ids':[p['content_id'] for p in flags],
     'viewer_current_flags':flags,'multiple_viewer_content_versions_observed':len(flags)>1,
     'correction_in_body':is_correction(txt),
     'correction_fields_in_body':corrective_claim(txt),
     'release_mention_only':('거래정지해제' in txt.replace(' ','') or '거래재개' in txt.replace(' ','')),
     'event_id_candidate_not_verified':f.incident_link(src['code'],src['submission_date_from_accession']),
     'body_text':txt,'notice_classification':'',
     'source_cohort_exhaustive':False,'official_daily_halt_status':'UNKNOWN',
     'complete_correction_chain_verified':False,'execution_eligibility_approved':False,
     'strategy_ready':False})
 for v in docs:v['notice_classification']=label(v)
 correction_pairs=candidate_links(docs)
 cids={m['receipt'] for m in docs if m['correction_in_body']}
 flagged=[m for m in docs if m['multiple_viewer_content_versions_observed']]
 coverage=collections.Counter(m['notice_classification'] for m in docs)
 incident_ids=['Q377460_01','Q377460_02','S418250_01']
 incident_summary=[]
 for incident in incident_ids:
  arr=[m for m in docs if m['event_id_candidate_not_verified']==incident]
  incident_summary.append({'incident_id':incident,'document_candidate_count':len(arr),
          'chronological_receipts':[m['receipt'] for m in arr],
          'explicit_correction_notice_count':sum(m['correction_in_body'] for m in arr),
          'release_title_count':sum(m['notice_classification']=='DIRECT_RELEASE_NOTICE_TITLE_REQUIRES_FULL_CHAIN' for m in arr),
          'correction_chain_complete':False,'official_daily_halt_status':'UNKNOWN',
          'strategy_ready':False})
 for x in docs:
  if x['official_daily_halt_status'] not in STATES or x['execution_eligibility_approved'] or x['strategy_ready']:
   fail('UNAPPROVED_PROMOTION')
 redacted=[{k:v for k,v in doc.items() if k!='body_text'} for doc in docs]
 columns=['code','receipt','notice_date','title','source_lane','notice_time_display_not_independently_verified',
          'body_sha256','viewer_sha256','notice_classification','correction_in_body',
          'multiple_viewer_content_versions_observed','event_id_candidate_not_verified',
          'official_daily_halt_status','complete_correction_chain_verified','strategy_ready']
 csvwrite(target/'stage7h_57_notice_correction_classification_RESEARCH_ONLY.csv',redacted,columns)
 store(target/'stage7h_57_notices_full_version_context_RESEARCH_ONLY.json',redacted)
 store(target/'stage7h_correction_candidate_pairs_REQUIRE_REVIEW.json',correction_pairs)
 store(target/'stage7h_3_incident_unchanged_unknown.json',incident_summary)
 risk=[{'receipt':x['receipt'],'code':x['code'],'title':x['title'],
        'reason':'BODY_EXPLICIT_CORRECTION' if x['correction_in_body'] else 'VIEWER_HAS_MULTIPLE_VERSIONS',
        'new_body_sha256':x['body_sha256'],'release_not_approved':True}
       for x in docs if x['correction_in_body'] or x['multiple_viewer_content_versions_observed']]
 store(target/'stage7h_manual_version_correction_review_queue.json',risk)
 summary={'schema':SCHEMA,'status':'SHA_PINNED_57_NOTICE_CORRECTION_CANDIDATES_ONLY',
   'source_stage7c_run':RUNS['C'],'source_stage7d_run':RUNS['D'],'source_stage7e_run':RUNS['E'],
   'source_stage7f_run':RUNS['F'],'source_stage7g_run':RUNS['G'],
   'verified_notice_count':len(docs),'verified_d_original_full_bodies':len(dbody),
   'verified_f_original_full_bodies':len(fbody),'verified_g_original_full_bodies':len(gbody),
   'verified_sha_raw_objects':COUNT['raw_d']+COUNT['raw_f']+COUNT['raw_g'],
   'verified_g_raw_manifest_sha256':gsha,'verified_d_raw_manifest_sha256':dsha,
   'verified_f_raw_manifest_sha256':fsha,
   'viewer_multiple_version_receipt_count':len(flagged),
   'explicit_correction_body_receipt_count':len(cids),
   'correction_candidate_pair_count':len(correction_pairs),
   'notice_classification_counts':dict(coverage),'three_original_incident_count':3,
   'prior_pin_g_sha_check_passed':True,
   'all_relevant_market_action_categories_searched':False,
   'all_original_and_correction_versions_verified':False,
   'complete_three_incident_chain_verified':False,
   'publication_timestamps_independently_verified':False,
   'official_daily_halt_status':'UNKNOWN','daily_tradability_verified':False,
   'lookahead_safe_strategy_filter_approved':False,'research_only':True,
   'production_rank_search_order_unchanged':True,'strategy_ready':False,
   'warning':'Candidates ONLY. Current viewer Y/N flags are not historical as-of version proofs. Remaining KIND categories and all amended histories are unverified. No daily tradeability approval.'}
 store(target/'stage7h_fail_closed_summary.json',summary)
 print(json.dumps(summary,ensure_ascii=False,sort_keys=True),flush=True)
 return summary

if __name__=='__main__':
 p=argparse.ArgumentParser()
 for k in ('c','d','e','f','g'):p.add_argument('--stage7'+k+'-dir',required=True)
 p.add_argument('--out',required=True)
 a=p.parse_args()
 audit(a.stage7c_dir,a.stage7d_dir,a.stage7e_dir,a.stage7f_dir,a.stage7g_dir,a.out)
