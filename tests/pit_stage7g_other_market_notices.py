#!/usr/bin/env python3
"""Stage7-G: SHA-replay 7-C/D/E/F and fetch only 36 *other* KIND market notices.

Evidence discovery only; NEVER infer formal suspension/reopening or PIT execution.
The 57 items are a two-category research canary, NOT a full KIND universe.
No connection to LIVE search, ranks, scores or orders.
"""
from __future__ import annotations
import argparse,csv,hashlib,io,json,pathlib,re,time
from datetime import datetime,timezone
from bs4 import BeautifulSoup
import pit_stage7d_kind_full_document as d
import pit_stage7f_full_halt_notice_chain as f

SCHEMA='STOCKHUNTER_PIT_STAGE7G_OTHER_MARKET_NOTICE_SOURCE_CANARY_V1'
STAGE7C_RUN=39516839081
STAGE7D_RUN=39523929718
STAGE7E_RUN=39530243566
STAGE7F_RUN=39541621602
EXPECTED_ALL=57
EXPECTED_DIRECT=21
EXPECTED_OTHER=36
EXPECTED_ORIGINAL_D=10
EXPECTED_F=11
EXPECTED_F_RAW=33
SOURCE_CODES={'377460','418250'}
HIGH_PRIORITY_WORDS=('매매거래정지','거래정지해제','거래재개','정정','취소','상장폐지','개선기간','실질심사','기업심사위원회','코스닥시장위원회')

def fail(s):raise RuntimeError('FAIL_CLOSED_STAGE7G_'+str(s))
def sha(b):return hashlib.sha256(b).hexdigest()
def jload(p):return json.loads(pathlib.Path(p).read_text(encoding='utf8'))
def csvread(p):
 with pathlib.Path(p).open(encoding='utf-8-sig',newline='') as fp:return list(csv.DictReader(fp))
def write(p,x):
 p=pathlib.Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 if isinstance(x,str):x=x.encode('utf8')
 tmp=p.with_name(p.name+'.part');tmp.write_bytes(x);tmp.replace(p)
def jout(p,obj):write(p,json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
def cout(p,rows,keys):
 o=io.StringIO(newline='');writer=csv.DictWriter(o,fieldnames=keys,extrasaction='ignore')
 writer.writeheader();writer.writerows(rows);write(p,o.getvalue())


def source_preflight(cdir,ddir,edir,fdir):
    """C/D/E via previously exercised 7F preflight; F via exact SHA manifest replay."""
    direct,other,docrows,dshas,cprov,es=f.audited_sources(cdir,ddir,edir)
    if len(direct)!=EXPECTED_DIRECT or len(other)!=EXPECTED_OTHER or len(docrows)!=EXPECTED_ORIGINAL_D:
        fail('CDE_SOURCE_COUNT_DRIFT')
    root=pathlib.Path(fdir)
    sm=jload(root/'stage7f_coverage_and_fail_closed_summary.json')
    if not (sm.get('schema')==f.SCHEMA and
            sm.get('status')=='ALL_21_DIRECT_HALT_TITLES_BODIES_COLLECTED_CORRECTION_CHAIN_STILL_UNVERIFIED' and
            sm.get('source_stage7c_run')==STAGE7C_RUN and
            sm.get('source_stage7d_run')==STAGE7D_RUN and
            sm.get('source_stage7e_run')==STAGE7E_RUN and
            sm.get('new_full_documents_requested')==EXPECTED_F and
            sm.get('new_full_documents_captured')==EXPECTED_F and
            sm.get('new_full_documents_unresolved')==0 and
            sm.get('other_market_notices_still_not_full_body_reviewed')==EXPECTED_OTHER and
            sm.get('daily_tradability_verified') is False and
            sm.get('strategy_ready') is False):fail('STAGE7F_SUMMARY_DRIFT')
    m=jload(root/'stage7f_new_raw_sha_manifest.json')
    if m.get('schema')!=f.SCHEMA or m.get('status')!='COMPLETE' or len(m.get('files',[]))!=EXPECTED_F_RAW:
        fail('STAGE7F_SOURCE_MANIFEST_DRIFT')
    by={};paths=set();full=set()
    for entry in m['files']:
        ac=entry['receipt'];typ=entry['type'];rel=entry['path']
        if ac not in direct or ac in docrows or not re.fullmatch(r'\d{14}',ac):
            fail('STAGE7F_INVALID_RECEIPT_OR_OVERLAP:'+ac)
        if typ not in ('viewer_shell','full_body') and not re.fullmatch(r'searchContents_\d{2}',typ):
            fail('STAGE7F_UNEXPECTED_RAW_TYPE:'+typ)
        ext='htm' if typ=='full_body' else 'html'
        expected=f'raw_kind_document/{ac}_{typ}.{ext}'
        if rel!=expected or rel in paths:fail('STAGE7F_RAW_PATH_DRIFT')
        paths.add(rel);buf=(root/rel).read_bytes()
        if sha(buf)!=entry['sha256'] or len(buf)!=entry['bytes']:
            fail('STAGE7F_SHA_OR_LENGTH_MISMATCH:'+rel)
        by.setdefault(ac,set()).add(typ)
        if typ=='full_body':
            full.add(ac)
            candidate=f.inspect_body(buf,direct[ac])
            if candidate['body_sha256']!=entry['sha256'] or not candidate['body_identity_corroborated']:
                fail('STAGE7F_PRIOR_BODY_IDENTITY_DRIFT:'+ac)
    if len(by)!=EXPECTED_F or len(full)!=EXPECTED_F or any(not {'viewer_shell','full_body'}<=k for k in by.values()):
        fail('STAGE7F_RAW_RECEIPT_COVERAGE_DRIFT')
    review=jload(root/'stage7f_11_document_review_candidates.json')
    if len(review)!=EXPECTED_F or {x['receipt'] for x in review}!=full:
        fail('STAGE7F_REVIEW_SET_DRIFT')
    for item in review:
        ac=item['receipt']
        if item['status']!='FULL_BODY_COLLECTED_REVIEW_ONLY' or item['code']!=direct[ac]['code'] or item['body_sha256']!=sha((root/f'raw_kind_document/{ac}_full_body.htm').read_bytes()):
            fail('STAGE7F_REVIEW_SOURCE_MISMATCH:'+ac)
    prior_other=csvread(root/'stage7f_36_other_market_notices_UNREVIEWED.csv')
    if len(prior_other)!=EXPECTED_OTHER or {x['accession'] for x in prior_other}!=set(other):
        fail('STAGE7F_OTHER_36_COHORT_DRIFT')
    for row in prior_other:
        ac=row['accession']
        if (row['code']!=other[ac]['code'] or row['title']!=other[ac]['title'] or
            row['source_raw_sha256']!=other[ac]['source_raw_sha256'] or
            row['document_resolution_state']!='UNRESOLVED' or
            row['strategy_ready']!='False' or row['official_daily_halt_status']!='UNKNOWN'):
            fail('STAGE7F_OTHER_ROW_DRIFT:'+ac)
    return other, direct, {'verified_original_c_14_raw_html':cprov['verified_stage7c_raw_html'],
        'verified_c_57_receipts':cprov['verified_stage7c_notice_rows'],
        'verified_d_30_raw_objects':len(dshas),
        'verified_e_3_incidents':es['distinct_incidents'],
        'verified_f_33_raw_objects':len(paths),
        'verified_f_11_full_documents':len(full),
        'f_original_manifest_sha256':sha((root/'stage7f_new_raw_sha_manifest.json').read_bytes())}


def review_body(raw,notice):
    """Capture identity and verbatim evidence, not effective dates or execution facts."""
    if not isinstance(raw,bytes) or len(raw)<100:fail('BODY_TOO_SHORT')
    decoded=None
    for enc in ('utf-8','euc-kr'):
        try:decoded=raw.decode(enc);break
        except UnicodeDecodeError:pass
    if decoded is None:fail('UNKNOWN_BODY_ENCODING')
    soup=BeautifulSoup(decoded,'html.parser')
    for tag in soup(['script','style','noscript']):tag.decompose()
    txt=re.sub(r'\s+',' ',soup.get_text(' ',strip=True))
    compact=re.sub(r'\s+','',txt)
    if len(txt)<42 or 'blank.html' in txt[:80]:fail('NOT_ACTUAL_DOCUMENT')
    code=notice['code']
    identity=code in txt or any(alias.replace(' ','') in compact for alias in d.prior.CODE_ALIASES[code])
    if not identity:fail('DOCUMENT_IDENTITY_NOT_PROVEN:'+notice['accession'])
    # Do not require '거래정지' in all the 36 bodies; these are other market notices.
    keywords=[word for word in HIGH_PRIORITY_WORDS if word.replace(' ','') in compact]
    tables=[]
    for tr in soup.select('tr'):
        cells=tr.find_all(['th','td'],recursive=False)
        vals=[re.sub(r'\s+',' ',c.get_text(' ',strip=True))[:900] for c in cells]
        if vals and any(k in ''.join(vals) for k in ('대상','종목','심사','폐지','개선','정지','이의','기한','결정','의견')):
            tables.append(vals)
    return {'document_body_sha256':sha(raw),'body_bytes':len(raw),
        'company_identity_corroborated':True,'body_contains_market_relevance_terms_for_review':keywords,
        'contains_release_like_word_not_proof':any(k in compact for k in ('거래정지해제','거래재개','매매거래재개')),
        'contains_correction_like_word_not_proof':any(k in compact for k in ('정정공시','정정신고','취소공시')),
        'body_structured_rows_verbatim':tables[:55],
        'body_text_preview':txt[:2800],
        'official_effective_datetime_verified':False,
        'daily_tradability_verified':False,'complete_correction_chain_verified':False,
        'strategy_ready':False}


def run(cdir,ddir,edir,fdir,out,provider=None,pause=.7):
    dest=pathlib.Path(out);dest.mkdir(parents=True,exist_ok=True)
    other,direct,source=source_preflight(cdir,ddir,edir,fdir)
    provider=provider or d.KindProvider()
    targets=sorted(other, key=lambda ac:(other[ac]['submission_date_from_accession'],ac))
    if len(targets)!=EXPECTED_OTHER or set(targets)&set(direct):fail('TARGET_COHORT_NOT_EXACTLY_36_OTHERS')
    manifest=[];found=[];unresolved=0
    for idx,ac in enumerate(targets,1):
        n=other[ac]
        item={'code':n['code'],'receipt':ac,'title':n['title'],
              'notice_date':n['submission_date_from_accession'],'notice_time_display':n['listed_cell_time'],
              'listing_source_sha256':n['source_raw_sha256'],'source_list_html':n['source_raw_file'],
              'event_id_candidate_not_verified':f.incident_link(n['code'],n['submission_date_from_accession']),
              'status':'UNRESOLVED','official_daily_halt_status':'UNKNOWN',
              'official_resume_effective_date_verified':False,'correction_chain_complete':False,
              'strategy_ready':False}
        def store(typ,blob,ext):
            if not isinstance(blob,bytes):fail('NON_BYTES_PROVIDER_RESPONSE')
            rel=f'raw_kind_other/{ac}_{typ}.{ext}'
            write(dest/rel,blob)
            manifest.append({'receipt':ac,'type':typ,'path':rel,'sha256':sha(blob),'bytes':len(blob)})
            jout(dest/'stage7g_new_raw_sha_manifest.json',{'schema':SCHEMA,'status':'PARTIAL','files':manifest})
        try:
            shell=provider.shell(ac);store('viewer_shell',shell,'html')
            ids=d.resolve_shell(shell,ac)
            url=None
            for j,cid in enumerate(ids):
                obj=provider.resolve(cid);store(f'searchContents_{j:02d}',obj,'html')
                try:url=d.resolve_response(obj,ac);break
                except RuntimeError:continue
            if not url:fail('NO_DOCUMENT_URL:'+ac)
            d.document_target(url,ac)
            raw=provider.content(url);store('full_body',raw,'htm')
            item.update(review_body(raw,n));item['resolved_content_url']=url
            item['status']='FULL_BODY_COLLECTED_CONTEXT_REVIEW_ONLY'
        except Exception as exc:
            unresolved+=1
            item['failure_diagnostic']=type(exc).__name__+': '+str(exc)[:330]
        found.append(item)
        jout(dest/'stage7g_36_other_notice_body_review.json',found)
        print('STAGE7G_OTHER_MARKET',idx,len(targets),ac,item['status'],flush=True)
        if pause and idx<len(targets):time.sleep(pause)
    result_by={r['receipt']:r for r in found}
    output_rows=[]
    for ac,n in sorted({**other,**direct}.items(),key=lambda p:(p[1]['submission_date_from_accession'],p[0])):
        x=result_by.get(ac)
        output_rows.append({'code':n['code'],'receipt':ac,'date':n['submission_date_from_accession'],
          'title':n['title'],'notice_time_display':n['listed_cell_time'],
          'cohort':'STAGE7G_OTHER_36' if x else 'STAGE7D_F_DIRECT_21_REPLAY',
          'original_source_html_sha256':n['source_raw_sha256'],
          'document_status':x['status'] if x else 'PREVIOUSLY_CAPTURED_7D_OR_7F',
          'document_body_sha256':x.get('document_body_sha256','') if x else 'SEE_PINNED_STAGE7D_OR_7F',
          'event_id_candidate_not_verified':f.incident_link(n['code'],n['submission_date_from_accession']),
          'formal_halt_status':'UNKNOWN','daily_execution_approved':False,'complete_correction_chain_verified':False,
          'strategy_ready':False})
    cout(dest/'stage7g_all_57_market_notices_chronology_REVIEW_ONLY.csv',output_rows,list(output_rows[0]))
    successes=sum(r['status']=='FULL_BODY_COLLECTED_CONTEXT_REVIEW_ONLY' for r in found)
    mf_complete=(unresolved==0 and successes==EXPECTED_OTHER and len(manifest)>=3*EXPECTED_OTHER)
    jout(dest/'stage7g_new_raw_sha_manifest.json',{'schema':SCHEMA,'status':'COMPLETE' if mf_complete else 'PARTIAL','files':manifest})
    summary={'schema':SCHEMA,
      'status':'57_COHORT_NOTICE_BODIES_COVERED_BUT_HISTORICAL_CORRECTION_CHAIN_UNVERIFIED' if mf_complete else 'OTHER_MARKET_DOCUMENTS_INCOMPLETE_FAIL_CLOSED',
      'source_stage7c_run':STAGE7C_RUN,'source_stage7d_run':STAGE7D_RUN,
      'source_stage7e_run':STAGE7E_RUN,'source_stage7f_run':STAGE7F_RUN,
      **source,'selected_original_cohort_notice_rows':EXPECTED_ALL,
      'direct_halt_documents_replayed':EXPECTED_DIRECT,'other_market_notices_requested':EXPECTED_OTHER,
      'other_market_notices_captured':successes,'other_market_notices_unresolved':unresolved,
      'new_source_raw_objects':len(manifest),
      'selected_cohort_57_document_body_coverage':(EXPECTED_DIRECT+successes) if mf_complete else None,
      'all_relevant_market_action_categories_searched':False,
      'all_original_and_correction_versions_verified':False,
      'all_3341_codes_official_halt_verified':False,
      'all_three_incidents_complete_chain_verified':False,
      'publication_timestamps_independently_verified':False,
      'daily_tradability_verified':False,'official_daily_halt_status':'UNKNOWN',
      'lookahead_safe_strategy_filter_approved':False,'strategy_ready':False,
      'research_only':True,'production_rank_search_order_unchanged':True,
      'generated_at_utc':datetime.now(timezone.utc).isoformat(),
      'warning':'57 documents are ONLY the 7-C two-category, two-code sampled cohort. Full-body coverage is not full-market, cross-category, corrected-version, or day-level trading eligibility certification.'}
    jout(dest/'stage7g_fail_closed_summary.json',summary)
    print(json.dumps(summary,ensure_ascii=False,sort_keys=True),flush=True)
    return summary


def main():
    ap=argparse.ArgumentParser()
    for v in ('stage7c','stage7d','stage7e','stage7f'):
        ap.add_argument('--'+v+'-dir',required=True)
    ap.add_argument('--out',required=True)
    ap.add_argument('--pause',type=float,default=.7)
    a=ap.parse_args()
    s=run(a.stage7c_dir,a.stage7d_dir,a.stage7e_dir,a.stage7f_dir,a.out,pause=a.pause)
    return 0 if s['other_market_notices_unresolved']==0 and s['other_market_notices_captured']==EXPECTED_OTHER else 2
if __name__=='__main__':raise SystemExit(main())
