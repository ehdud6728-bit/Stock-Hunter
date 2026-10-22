#!/usr/bin/env python3
"""StockHunter PIT 7-J recovery: reuse 7 already captured documents; fetch ONLY 2.

Old failed-run bytes (7 bodies, all original shells, one body pending company
corroboration) are SHA-locked. For 15-version viewers, inspect the exact
*selected* option; never mistake present-day Y for a historical as-of version.
A full body without company identity IN TEXT stays identity-unverified; a
source-linked official receipt is not independent historical tradability proof.
Research only: no order/strategy/rank/live imports or writes.
"""
from __future__ import annotations
import argparse, collections, copy, hashlib, json, pathlib, re, sys, time
from bs4 import BeautifulSoup
import pit_stage7j_selected_notice_fullbody as prior

SCHEMA = 'STOCKHUNTER_PIT_STAGE7J_3_RESIDUAL_PINNED_REPLAY_AND_TWO_RESOLVE_V1'
OLD_RUN = 39716439150
OLD_COMMIT = '5ed7756851bdc5968f67f8fa25dfcac4318c3ccb'
OLD_ARTIFACT = 12399431069
OLD_ARTIFACT_DIGEST = 'dd08df6a038317b26ee0d7ecb0722ee423fc20c37740db89b5923a976a688591'
OLD_FILE_SHA = {
 'stage7j_new_raw_sha_manifest.json': '114f6fa077bf84729c37d574e539e9c22698f2dfa4709c13483b629a80bf5605',
 'stage7j_fail_closed_summary.json': '306e823d9a302c28a002fac4c48d71a9ffdd6ff1301b39201a543c2d68759a4e',
 'stage7j_selected_ten_full_document_review_RESEARCH_ONLY.json': '6a5be6e52c83c3ac1fc2a0b5b5f83c3807de09d9a305bd62959502cb544c66dd',
 'stage7j_selected_ten_scope_plan_RESEARCH_ONLY.json': '1b6e208583d2088e4445cbf933bbbadb9aed5607c8803f47486c2784951ddc9c',
}
RECOVERY_RESOLVER_RECEIPTS = ('20231120000604', '20240320001600')
NO_NAME_BODY_RECEIPT = '20250813000836'
EXPECTED_ERRORS = {
 '20231120000604': 'FAIL_CLOSED_STAGE7J_INVALID_SHELL_CONTENT_OPTIONS:20231120000604',
 '20240320001600': 'FAIL_CLOSED_STAGE7J_INVALID_SHELL_CONTENT_OPTIONS:20240320001600',
 '20250813000836': 'FAIL_CLOSED_STAGE7J_DOCUMENT_COMPANY_IDENTITY_UNCORROBORATED:20250813000836',
}
MAX_NEW_HTTP_CALLS = 4
MAX_SHELL_OPTIONS = 24
VERSION_PATTERN = re.compile(r'^(\d{14})\|([YN])$')
RAW_PATH_PATTERN = re.compile(r'^raw_kind_selected/(\d{14})_(viewer_shell|full_body|searchContents_\d{2})\.(html|htm)$')


def fail(reason):
    raise RuntimeError('FAIL_CLOSED_STAGE7J_RECOVERY_'+str(reason))

def sha(b): return hashlib.sha256(b).hexdigest()
def getj(path): return json.loads(pathlib.Path(path).read_text(encoding='utf-8'))
def writej(path,obj): prior.writej(path,obj)


def inspect_prior_partial(root, source_plan=None):
    """Pin previous failed run exactly: failure is legitimate evidence, not a success.

    Only trust SHA-pinned original 27 raw objects + matching 10 rows. No network.
    """
    root=pathlib.Path(root)
    for rel,expected in OLD_FILE_SHA.items():
        file=root/rel
        if not file.is_file() or sha(file.read_bytes())!=expected:
            fail('OLD_PARTIAL_REPORT_SHA_DRIFT:'+rel)
    summary=getj(root/'stage7j_fail_closed_summary.json')
    expected={
        'status':'TEN_TARGET_BODY_COLLECTION_INCOMPLETE_FAIL_CLOSED',
        'mode':'collect','captured_new_document_count':7,
        'unresolved_new_document_count':3,'requested_new_document_count':10,
        'raw_objects_sha_saved':27,'actual_provider_calls':27,
        'source_stage7h_run':prior.SOURCE_7H_RUN,
        'source_stage7i_run':prior.SOURCE_7I_RUN,
        'official_daily_halt_status':'UNKNOWN','strategy_ready':False,
        'historical_daily_tradeability_verified':False,
        'full_market_pit_backtest_approved':False,
        'complete_target_collection':False,
    }
    for k,v in expected.items():
        if summary.get(k)!=v:fail('OLD_PARTIAL_SUMMARY_INVARIANT:'+k)
    records=getj(root/'stage7j_selected_ten_full_document_review_RESEARCH_ONLY.json')
    plan=getj(root/'stage7j_selected_ten_scope_plan_RESEARCH_ONLY.json')
    if source_plan is not None and plan!=source_plan:fail('OLD_PARTIAL_PLAN_NOT_EQUAL_TO_PINNED_7HI')
    if (len(records)!=10 or len(plan)!=10 or
        [r.get('receipt') for r in records]!=list(prior.TARGET_RECEIPTS) or
        [p.get('receipt') for p in plan]!=list(prior.TARGET_RECEIPTS)):
        fail('OLD_PARTIAL_TARGET_ORDER_OR_COHORT_DRIFT')
    raws=getj(root/'stage7j_new_raw_sha_manifest.json')
    if raws.get('status')!='PARTIAL_FAIL_CLOSED' or raws.get('new_raw_objects_verified')!=27 or len(raws.get('files',[]))!=27:
        fail('OLD_PARTIAL_RAW_COUNT_OR_STATUS_DRIFT')
    verified_raw={};type_by_receipt=collections.defaultdict(set)
    for ent in raws['files']:
        rel=ent.get('path','');ac=ent.get('receipt','');typ=ent.get('type','')
        match=RAW_PATH_PATTERN.fullmatch(rel)
        if (match is None or match.group(1)!=ac or match.group(2)!=typ or
            match.group(3)!=('htm' if typ=='full_body' else 'html') or
            ac not in prior.TARGET_RECEIPTS or rel in verified_raw):
            fail('OLD_PARTIAL_RAW_UNSAFE_PATH_OR_DUPLICATE')
        path=root/rel
        if not path.is_file():fail('OLD_PARTIAL_RAW_FILE_MISSING:'+rel)
        data=path.read_bytes()
        if not data or len(data)!=ent.get('bytes') or sha(data)!=ent.get('sha256'):
            fail('OLD_PARTIAL_RAW_SHA_OR_SIZE_MISMATCH:'+rel)
        verified_raw[rel]=ent
        type_by_receipt[ac].add(typ)
    required_missing=set(EXPECTED_ERRORS)
    for item,planned in zip(records,plan):
        ac=item['receipt']
        if any(item.get(k)!=planned.get(k) for k in ('receipt','code','title','notice_date','role')):
            fail('OLD_PARTIAL_ROW_PLAN_DRIFT:'+ac)
        if item.get('official_daily_halt_status')!='UNKNOWN' or item.get('strategy_ready') is not False:
            fail('OLD_PARTIAL_UNSAFE_STATE:'+ac)
        if ac in required_missing:
            diag=item.get('error',{})
            if item.get('status')!='UNRESOLVED_REQUIRE_REVIEW' or diag.get('detail')!=EXPECTED_ERRORS[ac]:
                fail('OLD_PARTIAL_EXPECTED_FAILURE_CHANGED:'+ac)
        else:
            if item.get('status')!='FULL_BODY_CAPTURED_IDENTITY_VERIFIED_REVIEW_ONLY':
                fail('OLD_PARTIAL_ORIGINAL_SEVEN_MISSING:'+ac)
            if not {'full_body','viewer_shell'}<=type_by_receipt[ac]:
                fail('OLD_PARTIAL_SEVEN_RAW_BODY_SHELL_MISSING:'+ac)
            data=(root/f'raw_kind_selected/{ac}_full_body.htm').read_bytes()
            if sha(data)!=item.get('body_sha256') or not item.get('company_identity_corroborated'):
                fail('OLD_PARTIAL_SEVEN_BODY_IDENTITY_DRIFT:'+ac)
            # Verify source path still ties the exact receipt to the KIND hostname.
            prior.full_body_url(item['resolved_official_url'],ac)
            rebuilt=prior.document_examination(data,planned,set())
            if rebuilt['body_sha256']!=item['body_sha256']:
                fail('OLD_PARTIAL_SEVEN_BODY_REPARSE_MISMATCH:'+ac)
    for ac in RECOVERY_RESOLVER_RECEIPTS:
        if type_by_receipt[ac]!={'viewer_shell'}:fail('OLD_RESOLVER_TARGET_MUST_HAVE_SHELL_ONLY:'+ac)
    if type_by_receipt[NO_NAME_BODY_RECEIPT]!={'viewer_shell','searchContents_00','full_body'}:
        fail('OLD_CONTEXT_RAW_NOT_COMPLETE')
    return {'summary':summary,'records':records,'plan':plan,'raw':raws['files'],
            'verified_raw':verified_raw,'type_by_receipt':type_by_receipt,
            'prior7_successfully_reused':7}


def selected_viewer_option(raw, receipt):
    """An exact selected option from the 15-entry viewer, NOT the current Y.

    Selection reflects present-day viewer routing for the requested receipt; it
    is NOT an independently verified historical document-publication timestamp.
    """
    if not isinstance(raw,bytes) or len(raw)>prior.MAX_RAW_RESPONSE_BYTES:fail('VIEWER_SIZE_UNTRUSTED')
    decoded=raw.decode('euc-kr',errors='replace')
    if receipt not in decoded:fail('VIEWER_RECEIPT_IDENTITY_NOT_VISIBLE')
    soup=BeautifulSoup(decoded,'html.parser')
    options=[];seen=set();selected=[];y_ids=[]
    for tag in soup.select('option'):
        raw_val=tag.get('value','')
        match=VERSION_PATTERN.fullmatch(raw_val)
        if match is None:continue
        cid,flag=match.groups()
        if cid in seen:fail('DUPLICATE_VIEWER_CONTENT_ID:'+cid)
        seen.add(cid)
        options.append({'content_id':cid,'viewer_current_flag_not_asof':flag,
                        'selected_for_receipt_not_asof':tag.has_attr('selected')})
        if tag.has_attr('selected'):selected.append(cid)
        if flag=='Y':y_ids.append(cid)
    if not 1<=len(options)<=MAX_SHELL_OPTIONS:fail('VIEWER_VERSION_COUNT_OUT_OF_BOUND')
    if len(selected)!=1 or len(y_ids)!=1:fail('VIEWER_SELECTED_OR_Y_AMBIGUOUS')
    return {'chosen_content_id':selected[0], 'selected_is_current_Y': selected[0] in y_ids,
            'selected_is_historical_asof_proof':False,
            'viewed_version_count':len(options), 'version_candidates':options}


def inspect_name_absent_body(root, previous, plan):
    """Replay captured full body WITHOUT upgrading its missing company-in-text.

    Prior official 7I list + same-receipt official KIND URL are evidence of a
    source link only; not independently proven body-company identity, event or
    as-of timing.
    """
    ac=NO_NAME_BODY_RECEIPT
    body=(pathlib.Path(root)/f'raw_kind_selected/{ac}_full_body.htm').read_bytes()
    resolver=(pathlib.Path(root)/f'raw_kind_selected/{ac}_searchContents_00.html').read_bytes()
    url=prior.resolve_document(resolver,ac)
    prior.full_body_url(url,ac)
    if not prior.sha(body)==previous['verified_raw'][f'raw_kind_selected/{ac}_full_body.htm']['sha256']:
        fail('PREVIOUS_RELATED_BODY_SHA_MISMATCH')
    soup=BeautifulSoup(body.decode('utf-8'),'html.parser')
    text=re.sub(r'\s+',' ',soup.get_text(' ',strip=True))
    compact=re.sub(r'\s+','',text)
    if len(compact)<42 or '회생절차종결결정' not in compact or '2025-08-13' not in compact:
        fail('NO_NAME_BODY_TITLE_OR_DATE_NOT_COHERENT')
    if plan.get('receipt')!=ac or plan.get('code')!='377460' or not '회생절차 종결결정' in plan['title']:
        fail('NO_NAME_BODY_PRIOR_LIST_MISMATCH')
    if '위니아에이드' in compact or '큐에이드' in compact or '377460' in text:
        fail('NO_NAME_BODY_SUDDEN_COMPANY_TEXT_PRESENT')
    case_numbers=sorted(set(re.findall(r'20\d{2}회합\d+',compact)))
    return {'receipt':ac,'code':plan['code'],'title':plan['title'],
            'body_sha256':sha(body),'official_url_receipt_path_correlated_only':url,
            'body_was_already_saved_in_failed_run':True,'new_network_requests':0,
            'body_has_company_name_or_stock_code':False,
            'company_identity_corroborated_in_body':False,
            'company_source_linked_by_pinned_official_list_and_receipt_url':True,
            'court_case_numbers_candidate_only':case_numbers,
            'classification':'SOURCE_LINKED_OFFICIAL_BODY_COMPANY_ABSENT_IN_TEXT_REQUIRES_REVIEW',
            'independent_company_identity_verified':False,
            'legal_case_lineage_verified':False,
            'historical_pit_tradeability_verified':False,
            'official_daily_halt_status':'UNKNOWN', 'strategy_ready':False}


def recover(hdir,idir,ddir,fdir,gdir,old_dir,out,provider=None,pause=1.0,dry_run=False):
    if not 0<=pause<=10:fail('INVALID_PAUSE')
    verified=prior.check_all_sources(hdir,idir,ddir,fdir,gdir)
    plan=prior.target_plan(verified)
    old=inspect_prior_partial(old_dir,source_plan=plan)
    dest=pathlib.Path(out);dest.mkdir(parents=True,exist_ok=True)
    writej(dest/'stage7j_recovery_prior_partial_sha_proof.json',{
        'old_run':OLD_RUN,'old_commit':OLD_COMMIT,'old_artifact_id':OLD_ARTIFACT,
        'old_artifact_digest':OLD_ARTIFACT_DIGEST,'exact_partial_four_file_sha256':OLD_FILE_SHA,
        'old_raw_count_replayed':len(old['raw']),'original_57_and_172_raw_reverified':True,
        'stage7i_8_raw_list_and_74_receipts_reverified':True,
        'old_seven_documents_not_requested_again':True,'old_three_failure_receipts':list(EXPECTED_ERRORS),
        'research_only':True,'strategy_ready':False})
    if dry_run:
        report={'schema':SCHEMA,'mode':'offline','status':'THREE_RESIDUAL_SCOPE_AND_PARTIAL_SHA_REPLAYED',
                'original_seven_reused_count':7,'recovery_network_requests':0,'old_raw_objects_sha_replayed':27,
                'original_172_raw_sha_replayed':172,'official_daily_halt_status':'UNKNOWN',
                'strategy_ready':False,'full_market_pit_backtest_approved':False}
        writej(dest/'stage7j_recovery_fail_closed_summary.json',report)
        print(json.dumps(report,ensure_ascii=False,sort_keys=True),flush=True)
        return report
    if provider is None:
        import pit_stage7d_kind_full_document as d
        provider=d.KindProvider()
    recovered=[];raw_added=[];errors=[];calls=0
    old_root=pathlib.Path(old_dir)
    for ac in RECOVERY_RESOLVER_RECEIPTS:
        row=next(x for x in plan if x['receipt']==ac)
        item={'receipt':ac,'code':row['code'],'title':row['title'],
              'status':'UNRESOLVED_REQUIRES_REVIEW','official_daily_halt_status':'UNKNOWN',
              'strategy_ready':False,'historical_version_publication_time_verified':False}
        try:
            shell=(old_root/f'raw_kind_selected/{ac}_viewer_shell.html').read_bytes()
            parsed=selected_viewer_option(shell,ac)
            item.update(parsed)
            if calls+2>MAX_NEW_HTTP_CALLS:fail('NETWORK_BUDGET_EXCEEDED')
            calls+=1
            resolver=provider.resolve(parsed['chosen_content_id'])
            prior._safe_response(resolver,'resolver')
            resolver_url=prior.resolve_document(resolver,ac)
            name=f'raw_kind_selected/{ac}_recovery_searchContents_00.html'
            prior.write_bytes(dest/name,resolver)
            raw_added.append({'receipt':ac,'type':'recovery_searchContents_00','path':name,
                              'sha256':sha(resolver),'bytes':len(resolver)})
            writej(dest/'stage7j_recovery_additional_raw_manifest.json',{'status':'PARTIAL','files':raw_added})
            calls+=1
            body=provider.content(resolver_url)
            prior._safe_response(body,'body')
            name=f'raw_kind_selected/{ac}_recovery_full_body.htm'
            prior.write_bytes(dest/name,body)
            raw_added.append({'receipt':ac,'type':'recovery_full_body','path':name,
                              'sha256':sha(body),'bytes':len(body)})
            writej(dest/'stage7j_recovery_additional_raw_manifest.json',{'status':'PARTIAL','files':raw_added})
            # The original identity check is still strict. Nothing is approved
            # on source listing/title alone if the body does not identify company.
            item['resolved_full_body_url_exact_receipt_path']=resolver_url
            try:
                parsed_body=prior.document_examination(body,row,{x['receipt'] for x in verified['h']['documents']})
                item.update(parsed_body)
                item['status']='NEW_BODY_CAPTURED_COMPANY_IN_TEXT_REVIEW_ONLY'
            except RuntimeError as exc:
                # A KIND full body can omit the issuer name (observed in the
                # prior 2025-08-13 official body). Do NOT falsify identity;
                # still preserve the exact URL and SHA-verified source object.
                if not str(exc).startswith('FAIL_CLOSED_STAGE7J_DOCUMENT_COMPANY_IDENTITY_UNCORROBORATED:'):
                    raise
                decoded=None
                for encoding in ('utf-8','euc-kr'):
                    try:
                        decoded=body.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        pass
                if decoded is None:fail('UNKNOWN_BODY_ENCODING_WITHOUT_COMPANY')
                soup=BeautifulSoup(decoded,'html.parser')
                text=re.sub(r'\s+',' ',soup.get_text(' ',strip=True))
                compact=re.sub(r'\s+','',text)
                if len(compact)<42 or '회생' not in compact:
                    fail('UNIDENTIFIED_BODY_HAS_NO_CASE_CONTEXT')
                other_code='418250' if row['code']=='377460' else '377460'
                if other_code in compact or any(alias in compact for alias in prior.COMPANY_ALIASES[other_code]):
                    fail('CONFLICTING_COMPANY_IN_BODY_NOT_SOURCE_LINKED')
                item.update({'body_sha256':sha(body),'body_bytes':len(body),
                    'company_identity_corroborated':False,
                    'official_source_url_and_receipt_path_correlated':True,
                    'body_title_context_candidate_only':'회생' in compact,
                    'full_company_identity_verified':False,
                    'official_daily_halt_status':'UNKNOWN','strategy_ready':False})
                item['status']='NEW_BODY_CAPTURED_SOURCE_LINKED_COMPANY_TEXT_UNVERIFIED'
        except Exception as exc:
            error={'receipt':ac,'error_type':type(exc).__name__,'detail':str(exc)[:350]}
            item['error']=error;errors.append(error)
        recovered.append(item)
        writej(dest/'stage7j_recovery_two_new_body_review.json',recovered)
        print('STAGE7J_RECOVERY_TARGET',ac,item['status'],flush=True)
        if pause and ac!=RECOVERY_RESOLVER_RECEIPTS[-1]:time.sleep(pause)
    for x in raw_added:
        if sha((dest/x['path']).read_bytes())!=x['sha256']:
            fail('NEW_RAW_SHA_REPLAY_MISMATCH')
    third=inspect_name_absent_body(old_root,old,plan[-1])
    writej(dest/'stage7j_recovery_no_company_body_require_manual_review.json',third)
    strict_good=sum(x['status']=='NEW_BODY_CAPTURED_COMPANY_IN_TEXT_REVIEW_ONLY' for x in recovered)
    raw_good=sum(x['status'] in ('NEW_BODY_CAPTURED_COMPANY_IN_TEXT_REVIEW_ONLY',
               'NEW_BODY_CAPTURED_SOURCE_LINKED_COMPANY_TEXT_UNVERIFIED') for x in recovered)
    unresolved_names=[x['receipt'] for x in recovered if
             x['status']=='NEW_BODY_CAPTURED_SOURCE_LINKED_COMPANY_TEXT_UNVERIFIED'] + [NO_NAME_BODY_RECEIPT]
    all_raw_available=raw_good==2 and third['body_was_already_saved_in_failed_run']
    # No automatic upgrade to verified identity or trading status: one of the
    # original 10 disclosures did NOT state its company name in its own body.
    report={'schema':SCHEMA,'mode':'recover','status':(
        'ALL_TEN_RAW_BODIES_CAPTURED_COMPANY_IDENTITY_REVIEW_REMAINS' if all_raw_available
        else 'RESIDUAL_BODY_RECOVERY_INCOMPLETE_FAIL_CLOSED'),
        'old_run':OLD_RUN,'source_stage7h_run':prior.SOURCE_7H_RUN,'source_stage7i_run':prior.SOURCE_7I_RUN,
        'previously_collected_7_unchanged':True,'old_raw_objects_sha_replayed':27,
        'original_172_raw_sha_replayed':172,'original_57_full_body_sha_replayed':57,
        'stage7i_eight_list_html_sha_replayed':8,
        'original_seven_reused_count':7,'new_company_in_body_verified_count':strict_good,
        'additional_target_already_has_raw_body_reused':1,
        'raw_documents_available_count':7+raw_good+1,
        'independent_company_identity_unverified_receipts':unresolved_names,
        'new_http_requests_made':calls,'new_sha_raw_objects_saved':len(raw_added),
        'max_new_http_requests':MAX_NEW_HTTP_CALLS,'viewer_15_options_present_day_not_asof':True,
        'historical_publication_timestamps_verified':False,
        'all_original_and_correction_versions_verified':False,
        'complete_three_incident_chain_verified':False,
        'historical_daily_tradeability_verified':False,
        'official_daily_halt_status':'UNKNOWN','strategy_ready':False,
        'full_market_pit_backtest_approved':False,'production_rank_search_order_unchanged':True,
        'research_only':True,'errors':errors}
    writej(dest/'stage7j_recovery_additional_raw_manifest.json',{
        'schema':SCHEMA,'status':'TWO_RECOVERY_BODIES_SHA_COMPLETE' if all_raw_available else 'PARTIAL_FAIL_CLOSED',
        'files':raw_added})
    writej(dest/'stage7j_recovery_fail_closed_summary.json',report)
    print(json.dumps(report,ensure_ascii=False,sort_keys=True),flush=True)
    return report


def main(argv=None):
    ap=argparse.ArgumentParser()
    for k in ('stage7h','stage7i','stage7d','stage7f','stage7g','old7j'):
        ap.add_argument('--'+k+'-dir',required=True)
    ap.add_argument('--out',required=True)
    ap.add_argument('--pause',type=float,default=1.0)
    ap.add_argument('--mode',choices=('offline','recover'),default='offline')
    a=ap.parse_args(argv)
    r=recover(a.stage7h_dir,a.stage7i_dir,a.stage7d_dir,a.stage7f_dir,a.stage7g_dir,
        a.old7j_dir,a.out,pause=a.pause,dry_run=(a.mode=='offline'))
    return 0 if a.mode=='offline' or r['status']=='ALL_TEN_RAW_BODIES_CAPTURED_COMPANY_IDENTITY_REVIEW_REMAINS' else 2

if __name__=='__main__':
    try: sys.exit(main())
    except Exception as exc:
        print('STAGE7J_RECOVERY_FAIL_CLOSED',type(exc).__name__,str(exc),file=sys.stderr,flush=True)
        sys.exit(2)
