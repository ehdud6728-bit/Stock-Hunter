#!/usr/bin/env python3
"""PIT Stage7-I: SHA-pinned Stage7-H reuse + strictly bounded KIND scope-gap canary.

RESEARCH_ONLY. This finds *candidate* out-of-cohort notices; it does NOT verify
correction chains, effect times, daily tradeability or strategic eligibility.
No production import, orders, 224-day signal changes or full-market sweep.
"""
from __future__ import annotations

import argparse
import collections
import csv
from datetime import date, datetime, timezone
import hashlib
import io
import json
import pathlib
import re
import sys
import time
from typing import Callable

# Use the previously audited Stage7-C KIND form/parser (repository-relative import).
try:
    import pit_stage7c_kind_resume_source as source
except ModuleNotFoundError:
    source=None  # Local offline source audit works without repository checkout.

SCHEMA = 'STOCKHUNTER_PIT_STAGE7I_BOUNDED_OUTSIDE_57_CATEGORY_GAP_CANARY_V1'
SOURCE_RUN = 39578003404
SOURCE_COMMIT = '0fb4392da16320c11671f9fd64f5582a28579d00'
SOURCE_ARTIFACT_ID = 12335870064
SOURCE_ARTIFACT_SHA256 = '18bee4ef59c5ce7d9cc50483973b469dca2aabeee8c53b9bc9ba320e20bbed04'
SOURCE_FILES = {
    'stage7h_3_incident_unchanged_unknown.json': '0720463cb226b55ec11d522ed3c5f0f129c9ebbf76e01b1c091b5569cee18008',
    'stage7h_57_notice_correction_classification_RESEARCH_ONLY.csv': '1c59c913dceb7fcb25dec3d8c015c73ecece6ef11fc637670ecad50b25da429c',
    'stage7h_57_notices_full_version_context_RESEARCH_ONLY.json': '8db6f8ba94bcd975ed177900fc1967bb087201a67ef616f8cf2a9cad0ce281c0',
    'stage7h_correction_candidate_pairs_REQUIRE_REVIEW.json': 'f244473f590eb0ab2c5feebd9b78cf1e4ffccd9f0a6c45bc9f7d4cec26c311b0',
    'stage7h_fail_closed_summary.json': '1211d70b99aa8f88ef128e7a3f5655b41d0bac38f79b27ac5da6758d2cfc70c0',
    'stage7h_manual_version_correction_review_queue.json': '005173d73ceaf1a8629eea13699d34f641c6b14abab4b38c59f35f01e78f3ac7',
}
CODE_SET = {'377460','418250'}
INCIDENT_SET = {'Q377460_01','Q377460_02','S418250_01'}
COUNT_BY_INCIDENT = {'Q377460_01':2,'Q377460_02':33,'S418250_01':22}
MAX_PAGES_PER_WINDOW = 2
PAGE_SIZE = 100
MAX_REQUESTS = 16
# These are CANARY windows around known official receipts, NOT all-date completeness.
# The last field pins one historical positive-control receipt in each scoped window.
WINDOWS = (
    ('377460','2023-10-24','2023-11-22','20231026000748'),
    ('377460','2023-11-24','2023-11-30','20231127000588'),
    ('377460','2024-03-14','2024-03-27','20240318001339'),
    ('377460','2025-08-12','2025-08-19','20250814003855'),
    ('377460','2026-09-16','2026-09-22','20260918000770'),
    ('418250','2024-04-03','2024-04-17','20240405000391'),
    ('418250','2025-01-22','2025-01-28','20250124000730'),
    ('418250','2026-07-20','2026-07-26','20260722000930'),
)


def fail(reason):
    raise RuntimeError('FAIL_CLOSED_STAGE7I_'+str(reason))


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(pathlib.Path(path).read_text(encoding='utf-8'))


def atomic_write(path, data):
    p=pathlib.Path(path)
    p.parent.mkdir(parents=True,exist_ok=True)
    if isinstance(data,str):data=data.encode('utf-8')
    if not isinstance(data,bytes):fail('INVALID_WRITE_TYPE')
    tmp=p.with_name(p.name+'.part')
    tmp.write_bytes(data);tmp.replace(p)


def write_json(path, obj):
    atomic_write(path,json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2)+'\n')


def write_csv(path, rows, fields):
    out=io.StringIO(newline='')
    w=csv.DictWriter(out,fieldnames=fields,extrasaction='ignore')
    w.writeheader();w.writerows(rows)
    atomic_write(path,out.getvalue())


def check_evidence(root):
    """Reject any missing/altered Stage7-H artifact; counts alone cannot authenticate."""
    root=pathlib.Path(root)
    for filename,digest in SOURCE_FILES.items():
        path=root/filename
        if not path.is_file() or sha(path.read_bytes())!=digest:
            fail('PINNED_STAGE7H_FILE_MISSING_OR_CHANGED:'+filename)
    summary=read_json(root/'stage7h_fail_closed_summary.json')
    if not (summary.get('schema')=='STOCKHUNTER_PIT_STAGE7H_SHA_PINNED_57_NOTICE_CORRECTION_CANDIDATE_AUDIT_V1'
            and summary.get('status')=='SHA_PINNED_57_NOTICE_CORRECTION_CANDIDATES_ONLY'
            and summary.get('verified_notice_count')==57
            and summary.get('verified_sha_raw_objects')==172
            and summary.get('three_original_incident_count')==3
            and summary.get('correction_candidate_pair_count')==1
            and summary.get('viewer_multiple_version_receipt_count')==2
            and summary.get('strategy_ready') is False
            and summary.get('daily_tradability_verified') is False
            and summary.get('all_relevant_market_action_categories_searched') is False
            and summary.get('official_daily_halt_status')=='UNKNOWN'):
        fail('STAGE7H_SUMMARY_INVARIANT')
    docs=read_json(root/'stage7h_57_notices_full_version_context_RESEARCH_ONLY.json')
    inc=read_json(root/'stage7h_3_incident_unchanged_unknown.json')
    pairs=read_json(root/'stage7h_correction_candidate_pairs_REQUIRE_REVIEW.json')
    risk=read_json(root/'stage7h_manual_version_correction_review_queue.json')
    with (root/'stage7h_57_notice_correction_classification_RESEARCH_ONLY.csv').open(encoding='utf-8-sig',newline='') as handle:
        csvrows=list(csv.DictReader(handle))
    if not all(isinstance(x,list) for x in (docs,inc,pairs,risk)):
        fail('STAGE7H_ARRAY_TYPES')
    if len(docs)!=57 or len(csvrows)!=57 or len(inc)!=3 or len(pairs)!=1 or len(risk)!=2:
        fail('STAGE7H_ROW_COUNTS')
    receipts=[x.get('receipt') for x in docs]
    if len(set(receipts))!=57 or any(not isinstance(r,str) or not re.fullmatch(r'\d{14}',r) for r in receipts):
        fail('STAGE7H_DUPLICATE_OR_MALFORMED_RECEIPT')
    by={x['receipt']:x for x in docs}
    for x in docs:
        if x.get('code') not in CODE_SET or x.get('notice_date')!=date(int(x['receipt'][:4]),int(x['receipt'][4:6]),int(x['receipt'][6:8])).isoformat():
            fail('STAGE7H_COMPANY_OR_DATE_IDENTITY')
        if (x.get('strategy_ready') is not False or x.get('execution_eligibility_approved') is not False or
            x.get('source_cohort_exhaustive') is not False or x.get('complete_correction_chain_verified') is not False or
            x.get('official_daily_halt_status')!='UNKNOWN'):
            fail('STAGE7H_UNAPPROVED_PROMOTION')
        if not re.fullmatch(r'[0-9a-f]{64}',x.get('body_sha256','')):
            fail('STAGE7H_BAD_BODY_SHA')
    if set(x['receipt'] for x in csvrows)!=set(by):fail('STAGE7H_CSV_JSON_RECEIPT_MISMATCH')
    for x in csvrows:
        original=by[x['receipt']]
        for field in ('code','notice_date','title','body_sha256','viewer_sha256','notice_classification'):
            if x[field]!=original[field]:fail('STAGE7H_CSV_JSON_ROW_DRIFT:'+field)
    if {x.get('incident_id'):x.get('document_candidate_count') for x in inc}!=COUNT_BY_INCIDENT:
        fail('STAGE7H_INCIDENT_DRIFT')
    if any(x.get('official_daily_halt_status')!='UNKNOWN' or x.get('strategy_ready') is not False or x.get('correction_chain_complete') is not False for x in inc):
        fail('STAGE7H_INCIDENT_PROMOTION')
    for x in inc:
        if set(x['chronological_receipts'])!={r['receipt'] for r in docs if r.get('event_id_candidate_not_verified')==x['incident_id']}:
            fail('STAGE7H_INCIDENT_MEMBERSHIP_DRIFT')
    if (pairs[0].get('original_receipt_candidate')!='20240318000555' or
        pairs[0].get('correction_receipt_candidate')!='20240318001339' or
        pairs[0].get('relationship_verified_complete') is not False or
        pairs[0].get('lookahead_safe_approved') is not False):
        fail('STAGE7H_CORRECTION_PAIR_DRIFT')
    if any(x['receipt'] not in by or x.get('release_not_approved') is not True for x in risk):
        fail('STAGE7H_REVIEW_QUEUE_DRIFT')
    return {'documents':docs,'by_receipt':by,'incidents':inc,'correction_pair':pairs[0],
            'pin_count':len(SOURCE_FILES),'pinned_run':SOURCE_RUN,
            'verified_receipt_count':len(by),'verified_raw_sha_objects_from_prior_run':172}


def validate_windows(known):
    if len(WINDOWS)!=8 or len(WINDOWS)*MAX_PAGES_PER_WINDOW>MAX_REQUESTS:
        fail('CANARY_BUDGET_DRIFT')
    seen=set();per_code=collections.defaultdict(list)
    for code,start,end,anchor in WINDOWS:
        lo=date.fromisoformat(start);hi=date.fromisoformat(end)
        if code not in CODE_SET or not lo<=hi or (hi-lo).days>35:
            fail('INVALID_WINDOW_SCOPE')
        if not (start<=known[anchor]['notice_date']<=end and known[anchor]['code']==code):
            fail('MISSING_PINNED_WINDOW_POSITIVE_CONTROL:'+anchor)
        if (code,start,end) in seen:fail('DUPLICATED_SCOPE_WINDOW')
        seen.add((code,start,end));per_code[code].append((lo,hi))
    for code,dates in per_code.items():
        for (a,b),(c,d) in zip(sorted(dates),sorted(dates)[1:]):
            if c<=b:fail('WINDOW_OVERLAP:'+code)
    return True


def candidate_class(title):
    t=re.sub(r'\s+','',str(title))
    # Order matters: a period-change title is NEVER sufficient proof of resumption,
    # even when the reason happens to contain the word '해제'.
    if '정지기간변경' in t or ('거래정지' in t and '기간변경' in t):
        return 'HALT_PERIOD_CHANGE_CONTEXT_NOT_RELEASE'
    if ('거래정지해제' in t or '매매거래재개' in t or '거래재개' in t):
        return 'RELEASE_TITLE_CANDIDATE'
    if '거래정지' in t or '매매거래정지' in t:
        if '취소' in t or '철회' in t:return 'HALT_CANCELLATION_TITLE_CANDIDATE'
        return 'HALT_TITLE_CANDIDATE'
    if '정정' in t or '기재정정' in t:
        return 'CORRECTION_TITLE_CANDIDATE'
    if '취소' in t or '철회' in t:
        return 'CANCELLATION_TITLE_CANDIDATE'
    if any(v in t for v in ('상장적격성','상장폐지','개선기간','실질심사','기업심사위원회','코스닥시장위원회')):
        return 'MARKET_REVIEW_CONTEXT_CANDIDATE'
    return 'UNRELATED_TITLE_IN_BOUNDED_QUERY'


def expanded_form(code,start,end,page):
    """Start from exact Stage7-C official request form; clear ONLY category filters.

    This deliberately does NOT claim that KIND honored all-category scope.
    Positive-control receipts and human review remain necessary.
    """
    if source is None:fail('GITHUB_REPOSITORY_STAGE7C_KIND_SOURCE_REQUIRED_FOR_CANARY')
    d=source.kind_form(code,start,end,page)
    for k in list(d):
        if k.startswith(('disclosureType','pDisclosureType')) or k in ('disTypevalue','bfrDsclsType'):
            if k.startswith('disclosureTypeArr'):
                d.pop(k)
            else:
                d[k]=''
    assert d['repIsuSrtCd']=='A'+code
    assert d['lastReport']==''
    assert d['currentPageSize']==str(PAGE_SIZE)
    return d


def offline_scope(source_data):
    rows=source_data['documents']
    types=collections.Counter(x['notice_classification'] for x in rows)
    base={'schema':SCHEMA,'mode':'OFFLINE_SCOPE_GAPS','stage7h_run':SOURCE_RUN,
          'stage7h_commit':SOURCE_COMMIT,'pinned_57_replayed':True,
          'pinned_stage7h_receipts':57,'original_raw_objects_verified_in_stage7h':172,
          'focus_codes':sorted(CODE_SET),'existing_scope':{'from':'2023-10-01','to':'2026-10-12',
          'kind_market_action_category_codes':['0311','0305'],
          'two_code_selected_cohort_not_global_universe':True,
          'document_versions_and_categories_exhaustive':False},
          'notice_classification_counts':dict(types),
          'known_correction_pair_count':1,
          'known_multiple_viewer_version_documents':2,
          'known_incident_counts':COUNT_BY_INCIDENT,
          'audit_windows':[
              {'code':c,'start':a,'end':b,'positive_control_receipt':anchor}
              for c,a,b,anchor in WINDOWS],
          'in_scope_evidence_gaps':[
              'KIND category restriction 0311,0305 could omit correction/cancellation/release across other categories',
              'H original selected 57 receipts are NOT a complete KIND disclosure master',
              'KIND viewer current version flags are NOT historical as-of versions',
              'Correction/withdrawal notice may have a new receipt, changed title, or separate category',
              'Query boundary before 2023-10-01 and after 2026-10-12 remains unchecked',
              'Receipt/display time not independently verified for PIT publication',
              'Complete formal trading resumption/correction chain and daily tradability still UNKNOWN',
          ],
          'bounded_canary_can_certify_all_history':False,
          'all_relevant_market_action_categories_searched':False,
          'all_original_and_correction_versions_verified':False,
          'official_daily_halt_status':'UNKNOWN','daily_tradability_verified':False,
          'strategy_ready':False,'research_only':True,
          'production_rank_search_order_unchanged':True}
    validate_windows(source_data['by_receipt'])
    return base


def default_fetch(form):
    import requests
    session=default_fetch.session
    r=session.post(source.KIND_URL,data=form,timeout=(12,30),headers={
        'User-Agent':'Mozilla/5.0 (StockHunter PIT7I RESEARCH_ONLY limited canary)',
        'Referer':source.KIND_URL,'Accept':'text/html,application/xhtml+xml'},allow_redirects=False)
    r.raise_for_status()
    if r.is_redirect or r.url.split('?',1)[0]!=source.KIND_URL:
        fail('UNEXPECTED_KIND_ORIGIN_OR_REDIRECT')
    if not r.content or len(r.content)>5_000_000 or 'html' not in r.headers.get('Content-Type','text/html').lower():
        fail('INVALID_KIND_HTML_RESPONSE')
    return bytes(r.content)


def run(source_dir,out,mode='offline',provider:Callable|None=None,pause=1.25):
    if mode not in ('offline','canary'):fail('INVALID_MODE')
    source_data=check_evidence(source_dir)
    scope=offline_scope(source_data)
    dest=pathlib.Path(out)
    dest.mkdir(parents=True,exist_ok=True)
    write_json(dest/'stage7i_existing_57_scope_gaps_RESEARCH_ONLY.json',scope)
    if mode=='offline':
        summary={'schema':SCHEMA,'mode':'offline','status':'STAGE7H_PINNED_57_SCOPE_GAPS_IDENTIFIED',
                 'source_stage7h_run':SOURCE_RUN,'source_stage7h_commit':SOURCE_COMMIT,
                 'source_stage7h_verified_receipts':57,'targeted_requests_executed':0,
                 'potentially_missing_official_notices':'UNKNOWN',
                 'all_relevant_market_action_categories_searched':False,
                 'official_daily_halt_status':'UNKNOWN','daily_tradability_verified':False,
                 'strategy_ready':False,'research_only':True,'production_rank_search_order_unchanged':True}
        write_json(dest/'stage7i_fail_closed_summary.json',summary)
        return summary
    if source is None:fail('GITHUB_REPOSITORY_STAGE7C_KIND_SOURCE_REQUIRED_FOR_CANARY')
    if pause < 0 or pause > 20:fail('INVALID_PAUSE')
    write_json(dest/'stage7i_canary_request_policy.json',{
        'schema':SCHEMA,'mode':'CANARY','window_count':len(WINDOWS),
        'maximum_provider_calls':MAX_REQUESTS,'maximum_pages_per_window':MAX_PAGES_PER_WINDOW,
        'query_original_categories_only':False,'category_filter_honor_independently_verified':False,
        'raw_document_body_downloads_allowed':False,
        'original_receipts_already_sha_pinned':57,
        'warnings':['Bounded scan may identify only candidate gaps; not a complete history',
                    'No official document contents retrieved or independent effective-time checks performed']})
    if provider is None:
        import requests
        default_fetch.session=requests.Session()
        provider=default_fetch
    by=source_data['by_receipt']
    per_query=[];candidates=[];observed={};mf=[];errors=[];req_count=0
    for code,start,end,anchor in WINDOWS:
        window_key=code+'_'+start.replace('-','')+'_'+end.replace('-','')
        anchored=False
        for page in range(1,MAX_PAGES_PER_WINDOW+1):
            if req_count>=MAX_REQUESTS:fail('MAX_REQUEST_BUDGET_REACHED')
            form=expanded_form(code,start,end,page)
            req_count+=1
            name=f'raw_kind_canary/{window_key}_p{page:02d}.html'
            entry={'code':code,'from':start,'to':end,'page':page,'raw_file':name,
                   'positive_control':anchor,'requested_at_utc':datetime.now(timezone.utc).isoformat()}
            try:
                raw=provider(form)
                if isinstance(raw,str):raw=raw.encode('utf-8')
                if not isinstance(raw,bytes) or len(raw)<40 or len(raw)>5_000_000:fail('INVALID_PROVIDER_RESULT')
                atomic_write(dest/name,raw)
                entry.update(response_sha256=sha(raw),bytes=len(raw),state='RAW_CAPTURED_PENDING_PARSE')
                mf.append(entry)
                write_json(dest/'stage7i_raw_query_manifest_PARTIAL.json',mf)
                parsed=source.parse_kind_html(raw,code,start,end,page)
                entry['parsed_rows']=len(parsed)
                if any(r['accession']==anchor for r in parsed):anchored=True
                for r in parsed:
                    accession=r['accession']
                    prior=observed.get(accession)
                    if prior is not None and (prior['code']!=r['code'] or prior['title']!=r['title']):
                        fail('CROSS_PAGE_RECEIPT_CONFLICT:'+accession)
                    if prior:continue
                    observed[accession]=r
                    cls=candidate_class(r['title'])
                    state='ALREADY_COVERED' if accession in by else (
                        'TITLE_ONLY_UNRESOLVED' if cls=='UNRELATED_TITLE_IN_BOUNDED_QUERY'
                        else 'POSSIBLE_MISSING')
                    item={'code':code,'receipt':accession,'notice_date':r['submission_date_from_accession'],
                          'title':r['title'],'candidate_class':cls,'coverage_state':state,
                          'source_window':window_key,'source_page':page,
                          'listing_source_sha256':sha(raw),
                          'body_document_downloaded':False,
                          'original_or_correction_relationship_verified':False,
                          'independent_publication_time_verified':False,
                          'official_daily_halt_status':'UNKNOWN',
                          'strategy_ready':False}
                    candidates.append(item)
                entry['state']='RECORDED'
                per_query.append({'window':window_key,'page':page,'rows':len(parsed),'status':'RECORDED'})
                write_json(dest/'stage7i_raw_query_manifest_PARTIAL.json',mf)
                write_csv(dest/'stage7i_outside_57_notice_candidates_REVIEW_ONLY.csv',candidates,
                          list(candidates[0]) if candidates else ['code','receipt','title','coverage_state'])
                if len(parsed)<PAGE_SIZE:break
                if page==MAX_PAGES_PER_WINDOW:
                    fail('WINDOW_PAGINATION_SATURATED:'+window_key)
            except Exception as exc:
                error={'window':window_key,'page':page,'error_type':type(exc).__name__,
                       'detail':str(exc)[:300]}
                errors.append(error)
                per_query.append({'window':window_key,'page':page,'rows':None,'status':'UNRESOLVED'})
                # A partial raw response, if any, is retained; do not infer absence.
                break
            if pause:time.sleep(pause)
        if not anchored:
            errors.append({'window':window_key,'page':None,
                           'error_type':'POSITIVE_CONTROL_NOT_OBSERVED',
                           'detail':'Previously SHA-pinned receipt was not returned in the expanded query'})
        if pause:time.sleep(pause)
    # Verify the raw evidence that was actually saved, regardless of incomplete queries.
    for ent in mf:
        raw=(dest/ent['raw_file']).read_bytes()
        if sha(raw)!=ent['response_sha256'] or len(raw)!=ent['bytes']:
            fail('LOCAL_RAW_QUERY_SHA_TAMPER')
    if req_count>MAX_REQUESTS:fail('REQUEST_BUDGET_BREACH')
    counts=collections.Counter(x['coverage_state'] for x in candidates)
    complete=not errors and len(per_query)>=len(WINDOWS)
    summary={'schema':SCHEMA,'mode':'canary','status':(
        'BOUNDED_CANARY_FINISHED_NOT_COMPLETENESS_PROOF' if complete
        else 'BOUNDED_CANARY_INCOMPLETE_FAIL_CLOSED'),
        'source_stage7h_run':SOURCE_RUN,'source_stage7h_commit':SOURCE_COMMIT,
        'source_stage7h_verified_receipts':57,'source_stage7h_known_incidents':3,
        'source_stage7h_known_correction_pairs':1,
        'windows_planned':len(WINDOWS),'windows_positive_controls_expected':len(WINDOWS),
        'windows_positive_controls_missed':sum(x['error_type']=='POSITIVE_CONTROL_NOT_OBSERVED' for x in errors),
        'targeted_requests_executed':req_count,
        'raw_kind_search_responses_sha_saved':len(mf),
        'observed_unique_notice_receipts_in_windows':len(observed),
        'observed_existing_57_overlap':counts.get('ALREADY_COVERED',0),
        'potentially_missing_notice_candidates_needing_document_review':counts.get('POSSIBLE_MISSING',0),
        'unclassified_new_notice_receipts_not_excluded':counts.get('TITLE_ONLY_UNRESOLVED',0),
        'confirmed_missing_original_57_document_relationships':0,
        'canary_queries_resolved':complete,'errors':errors,
        'canary_does_not_prove_all_date_or_all_category_completeness':True,
        'all_relevant_market_action_categories_searched':False,
        'all_original_and_correction_versions_verified':False,
        'historical_publication_timestamps_verified':False,
        'complete_three_incident_chain_verified':False,
        'official_daily_halt_status':'UNKNOWN','daily_tradability_verified':False,
        'lookahead_safe_strategy_filter_approved':False,'strategy_ready':False,
        'research_only':True,'production_rank_search_order_unchanged':True}
    write_json(dest/'stage7i_fail_closed_summary.json',summary)
    write_json(dest/'stage7i_raw_query_manifest.json',{'schema':SCHEMA,
               'status':'COMPLETE_WITHIN_BOUNDED_WINDOWS' if complete else 'PARTIAL',
               'files':mf,'request_results':per_query})
    write_csv(dest/'stage7i_outside_57_notice_candidates_REVIEW_ONLY.csv',candidates,
              ['code','receipt','notice_date','title','candidate_class','coverage_state',
               'source_window','source_page','listing_source_sha256',
               'body_document_downloaded','original_or_correction_relationship_verified',
               'independent_publication_time_verified','official_daily_halt_status','strategy_ready'])
    write_json(dest/'stage7i_manual_gap_review_queue.json',
               [x for x in candidates if x['coverage_state'] in ('POSSIBLE_MISSING','TITLE_ONLY_UNRESOLVED')])
    print(json.dumps(summary,ensure_ascii=False,sort_keys=True),flush=True)
    return summary


def main(argv=None):
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--stage7h-dir',required=True)
    ap.add_argument('--out',required=True)
    ap.add_argument('--mode',choices=('offline','canary'),default='offline')
    ap.add_argument('--pause',type=float,default=1.25)
    a=ap.parse_args(argv)
    result=run(a.stage7h_dir,a.out,mode=a.mode,pause=a.pause)
    return 0 if result.get('canary_queries_resolved',True) else 2


if __name__=='__main__':
    try:sys.exit(main())
    except Exception as exc:
        print('FAIL_CLOSED_STAGE7I',type(exc).__name__,str(exc),file=sys.stderr,flush=True)
        sys.exit(2)
