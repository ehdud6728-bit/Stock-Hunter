#!/usr/bin/env python3
"""PIT Stage7-J: SHA-pinned 7H/7I + 172 previous KIND raw objects, 10 targeted bodies.

STRICT RESEARCH_ONLY, NEVER historical daily trading eligibility. Scoped to six
new market-review titles and four previously unreviewed court/reorganization
contextual disclosures. No all-market scan, no score/rank/trade or 224MA change.
"""
from __future__ import annotations

import argparse
import collections
import csv
from datetime import date, datetime, timezone
import hashlib
from html import unescape
import io
import json
import pathlib
import re
import sys
import time
from urllib.parse import urlparse, unquote

from bs4 import BeautifulSoup
import pit_stage7i_bounded_missing_notice_audit as prior

SCHEMA = 'STOCKHUNTER_PIT_STAGE7J_PINNED_TEN_SELECTED_KRX_KIND_BODY_CONTEXT_AUDIT_V1'
SOURCE_7I_RUN = 39708812035
SOURCE_7I_COMMIT = 'a3cd338b1306030039f117fa8392a9c9be359db7'
SOURCE_7H_RUN = prior.SOURCE_RUN
SOURCE_7H_COMMIT = prior.SOURCE_COMMIT
SOURCE_7I_ARTIFACT = 12396655109
SOURCE_7I_DIGEST = '275cdfc71fe2be4280bd21ecc400449f78246a0a88fd99fbeffafa553b9e3a48'
RAW_PRIOR_COUNT = {'7D': 30, '7F': 33, '7G': 109}
PRIOR_BODY_COUNTS = {'7D': 10, '7F': 11, '7G': 36}
ORIGINAL_LANES = {
    '7D': ('raw_full_document_sha_manifest.json', 'raw_kind_document'),
    '7F': ('stage7f_new_raw_sha_manifest.json', 'raw_kind_document'),
    '7G': ('stage7g_new_raw_sha_manifest.json', 'raw_kind_other'),
}
# Exact byte SHA-256 from successfully completed GitHub run 39708812035.
# Never take a silent 'latest' replacement or only trust a claimed CSV count.
PINNED_7I_SHA = {
 'stage7i_fail_closed_summary.json': '4bdc6a0006cb28dd058c35fd18ac62f24f218fcc5e893f0727e535146ed3dae1',
 'stage7i_existing_57_scope_gaps_RESEARCH_ONLY.json': 'c46e4633cbbb1f11b68a3d776a4c14e2f4ecf473f4656b4c6eb0210bd49fe6e1',
 'stage7i_canary_request_policy.json': '30fbe0f1c141828b30cca65f87b153978c50241a118b34e895cbd3c49d3405f3',
 'stage7i_outside_57_notice_candidates_REVIEW_ONLY.csv': 'b0811c66b600b956c06a14187f982da4ded4e38a47150de9fa09ed462468755c',
 'stage7i_manual_gap_review_queue.json': 'e421f3e387bf9b9bd886a210a519e4aeadfe6514d3bb9a365951627747b6a1ce',
 'stage7i_raw_query_manifest.json': 'b95de23fd237461109964b9140c55c2a779a81d01705ae4c6c2060442804d89f',
 'stage7i_raw_query_manifest_PARTIAL.json': 'd94ca294e507cd5bf6ef5fc8d5dc8d873949f652e212afee962395c21f47d18a',
 'raw_kind_canary/377460_20231024_20231122_p01.html': '4633daf6bbb329d81056770a88af104c327cb89154191bdf182aa89d498c5f82',
 'raw_kind_canary/377460_20240314_20240327_p01.html': 'e4b34330cf06636c411f9d6dc84729f0b1876f3f26b0f718c9d4fd8136917909',
 'raw_kind_canary/377460_20231124_20231130_p01.html': '80499d515037648a3207cbbf779c3ba53c1a4627c9bb7ddbfc2b6feacff22967',
 'raw_kind_canary/377460_20250812_20250819_p01.html': '8d9374d7fcbb320d494dc7c9a88d0d0d8242ffb81bda995dcbfb2eef4e142be1',
 'raw_kind_canary/377460_20260916_20260922_p01.html': '83badb8873cac720694e3d18a2d24d795db985d02ec0bd22e1986422bc044408',
 'raw_kind_canary/418250_20240403_20240417_p01.html': '2793fea932d264a2a1e4c2373bbeb68ebaeb6089eccdc1de1ab5c453d62ce0bc',
 'raw_kind_canary/418250_20250122_20250128_p01.html': 'efd054c75c574de3c3d3d4cab856aaff3bb65c98805d1d11aea1d655631a5e97',
 'raw_kind_canary/418250_20260720_20260726_p01.html': 'c84a016bfa7582fc8b6d7141f18fce8502baf5bedbe3b6e7269efac8a33998ac',
}
# Six prioritized outside-57 market actions + only four specifically selected
# court/reorganization predecessor-context items from remaining 39.
PRIORITY_RECEIPTS = (
 '20240320002747', '20250814003852', '20260918000768',
 '20240405000992', '20250124000732', '20260723000332',
)
CONTEXT_RECEIPTS = (
 '20231026000746', '20231120000604',
 '20240320001600', '20250813000836',
)
TARGET_RECEIPTS = PRIORITY_RECEIPTS + CONTEXT_RECEIPTS
COMPANY_ALIASES = {'377460': ('큐에이드', '위니아에이드'), '418250': ('시큐레터',)}
MAX_NEW_DOCUMENTS = 10
MAX_PROVIDER_CALLS = 50   # 10 shells + at most 30 resolvers + 10 bodies
MAX_RESOLVERS_PER_DOC = 3
MAX_RAW_RESPONSE_BYTES = 5_000_000
RECEIPT_PATTERN = re.compile(r'(?<!\d)\d{14}(?!\d)')
RAW_REL = re.compile(r'raw_kind_(?:document|other)/\d{14}_(?:viewer_shell|full_body|searchContents_\d\d)\.html?')
SHELL_OPTION = re.compile(r'<option\b[^>]*\bvalue\s*=\s*[\'\"](\d{14})\|([YN])[\'\"]', re.I)
FULL_HTML = re.compile(r'https?://[^\s\'"<>]+?\.htm(?:\?[^\s\'"<>]*)?', re.I)


def fail(message):
    raise RuntimeError('FAIL_CLOSED_STAGE7J_' + str(message))


def sha(data): return hashlib.sha256(data).hexdigest()


def readj(path): return json.loads(pathlib.Path(path).read_text(encoding='utf-8'))


def loadcsv(path):
    with pathlib.Path(path).open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def write_bytes(path, data):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str): data=data.encode('utf-8')
    if not isinstance(data, bytes): fail('OUTPUT_NOT_BYTES')
    tmp=path.with_name(path.name+'.part');tmp.write_bytes(data);tmp.replace(path)


def writej(path, data):
    write_bytes(path, json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2)+'\n')


def writecsv(path, items, fields):
    s=io.StringIO(newline='');writer=csv.DictWriter(s, fieldnames=fields, extrasaction='ignore')
    writer.writeheader();writer.writerows(items)
    write_bytes(path, s.getvalue())


def verify_original_172(documents, stage7d, stage7f, stage7g, source_summary):
    """Independently replay all original D/F/G raw bytes against 7H document/manifest SHA."""
    roots={'7D':pathlib.Path(stage7d),'7F':pathlib.Path(stage7f),'7G':pathlib.Path(stage7g)}
    verified=[];raw_count=0;lane_counts=collections.Counter();claimed_by={}
    for lane, (name, folder) in ORIGINAL_LANES.items():
        root=roots[lane]; mpath=root/name
        expected=source_summary.get('verified_'+ {'7D':'d','7F':'f','7G':'g'}[lane]+'_raw_manifest_sha256')
        if not mpath.is_file() or sha(mpath.read_bytes())!=expected:
            fail('PINNED_ORIGINAL_MANIFEST_TAMPER:'+lane)
        m=readj(mpath)
        if m.get('status')!='COMPLETE' or len(m.get('files',[]))!=RAW_PRIOR_COUNT[lane]:
            fail('INCOMPLETE_ORIGINAL_MANIFEST:'+lane)
        paths=set()
        for ent in m['files']:
            rel=ent.get('path',''); receipt=ent.get('receipt','')
            if (not RAW_REL.fullmatch(rel) or not rel.startswith(folder+'/') or
                rel in paths or not re.fullmatch(r'\d{14}',receipt) or
                not rel.startswith(folder+'/'+receipt+'_')):
                fail('INVALID_ORIGINAL_RAW_IDENTITY:'+lane)
            paths.add(rel)
            body=(root/rel).read_bytes()
            if sha(body)!=ent.get('sha256') or len(body)!=ent.get('bytes') or len(body)==0:
                fail('ORIGINAL_RAW_SHA_MISMATCH:'+lane+':'+rel)
            raw_count+=1
            claimed_by[(lane,rel)]=ent
    if raw_count!=172:fail('ORIGINAL_RAW_COUNT_DRIFT')
    for doc in documents:
        lane=doc['source_lane'];receipt=doc['receipt'];folder=ORIGINAL_LANES[lane][1];root=roots[lane]
        for typ,expected_field,ext in [('full_body','body_sha256','htm'),('viewer_shell','viewer_sha256','html')]:
            path=f'{folder}/{receipt}_{typ}.{ext}'
            if (lane,path) not in claimed_by or sha((root/path).read_bytes())!=doc[expected_field]:
                fail('H_DOCUMENT_ORIGINAL_BODY_VIEWER_SHA_MISMATCH:'+receipt+':'+typ)
        lane_counts[lane]+=1
        verified.append(receipt)
    if dict(lane_counts)!=PRIOR_BODY_COUNTS or len(set(verified))!=57:
        fail('ORIGINAL_57_BODY_COHORT_DRIFT')
    return {'previous_57_full_bodies_sha_verified':57,
            'previous_57_viewer_shells_sha_verified':57,
            'previous_172_raw_objects_sha_verified':raw_count,
            'prior_document_lane_counts':dict(lane_counts)}


def parse_saved_listing(raw,code,lo,hi,page):
    """Replay Stage7-I raw HTML, excluding any outside-company or malformed receipt."""
    try: text=raw.decode('utf-8')
    except UnicodeError:fail('CANARY_HTML_ENCODING')
    soup=BeautifulSoup(text,'html.parser')
    table=soup.select_one('table.list.type-00.mt10')
    if table is None or table.find('tbody') is None:fail('CANARY_EXPECTED_OFFICIAL_TABLE_MISSING')
    rows=[]
    for tr in table.select('tbody > tr'):
        cells=tr.find_all('td',recursive=False)
        if len(cells)<5:
            if len(cells)==1 and any(w in cells[0].get_text(' ',strip=True) for w in ('조회','없습니다','데이터')):continue
            if not cells:continue
            fail('CANARY_ROW_FORMAT_DRIFT')
        cmp=cells[2].get_text(' ',strip=True)
        if not any(alias in cmp for alias in COMPANY_ALIASES[code]):fail('CANARY_COMPANY_FILTER_LEAK')
        at=cells[3].find('a')
        if at is None:fail('CANARY_LINK_MISSING')
        m=re.search(r'openDisclsViewer\([\'\"](\d{14})[\'\"]',at.get('onclick',''))
        if not m:fail('CANARY_RECEIPT_LINK_MISSING')
        receipt=m.group(1);day=f'{receipt[:4]}-{receipt[4:6]}-{receipt[6:8]}'
        try:date.fromisoformat(day)
        except ValueError:fail('CANARY_DATE_MALFORMED')
        if not lo<=day<=hi:fail('CANARY_OUTSIDE_QUERY_WINDOW')
        title=unescape(at.get('title','').strip() or at.get_text(' ',strip=True))
        if not title:fail('CANARY_EMPTY_TITLE')
        rows.append({'receipt':receipt,'code':code,'notice_date':day,'title':title,
                     'source_window':f'{code}_{lo.replace("-","")}_{hi.replace("-","")}',
                     'source_page':str(page),'listing_source_sha256':sha(raw)})
    if len(rows)>100:fail('CANARY_PAGE_SATURATED')
    return rows


def verify_7i(stage7i, original):
    root=pathlib.Path(stage7i)
    for name,digest in PINNED_7I_SHA.items():
        p=root/name
        if not p.is_file() or sha(p.read_bytes())!=digest:
            fail('PINNED_STAGE7I_SOURCE_SHA_MISMATCH:'+name)
    summary=readj(root/'stage7i_fail_closed_summary.json')
    required={'schema':prior.SCHEMA,'status':'BOUNDED_CANARY_FINISHED_NOT_COMPLETENESS_PROOF',
              'source_stage7h_run':SOURCE_7H_RUN,'source_stage7h_commit':SOURCE_7H_COMMIT,
              'source_stage7h_verified_receipts':57,'source_stage7h_known_incidents':3,
              'source_stage7h_known_correction_pairs':1,'windows_planned':8,
              'windows_positive_controls_missed':0,'targeted_requests_executed':8,
              'raw_kind_search_responses_sha_saved':8,'observed_unique_notice_receipts_in_windows':74,
              'observed_existing_57_overlap':29,'potentially_missing_notice_candidates_needing_document_review':6,
              'unclassified_new_notice_receipts_not_excluded':39,'official_daily_halt_status':'UNKNOWN',
              'strategy_ready':False,'daily_tradability_verified':False,
              'all_original_and_correction_versions_verified':False,
              'canary_queries_resolved':True,'research_only':True,'errors':[]}
    if any(summary.get(k)!=v for k,v in required.items()):fail('STAGE7I_SUMMARY_INVARIANT')
    manifest=readj(root/'stage7i_raw_query_manifest.json')
    if manifest.get('status')!='COMPLETE_WITHIN_BOUNDED_WINDOWS' or len(manifest.get('files',[]))!=8:
        fail('STAGE7I_MANIFEST_INCOMPLETE')
    refs={};anchors=set();request_anchors={w[3] for w in prior.WINDOWS}
    seen_windows=set()
    for x in manifest['files']:
        window=(x['code'],x['from'],x['to'],x['positive_control']);key=window[:3]
        if window not in prior.WINDOWS or key in seen_windows or x.get('page')!=1 or x.get('state')!='RECORDED':
            fail('STAGE7I_MANIFEST_WINDOW_OR_STATE_DRIFT')
        seen_windows.add(key)
        expected_name=f'raw_kind_canary/{x["code"]}_{x["from"].replace("-","")}_{x["to"].replace("-","")}_p01.html'
        if x['raw_file']!=expected_name:fail('STAGE7I_RAW_PATH_CHANGED')
        body=(root/expected_name).read_bytes()
        if sha(body)!=x['response_sha256'] or len(body)!=x['bytes']:
            fail('STAGE7I_RAW_MANIFEST_SHA_MISMATCH')
        rows=parse_saved_listing(body,x['code'],x['from'],x['to'],1)
        if len(rows)!=x['parsed_rows']:
            fail('STAGE7I_RAW_PARSE_COUNT_DRIFT')
        if x['positive_control'] not in {r['receipt'] for r in rows}:
            fail('STAGE7I_POSITIVE_CONTROL_MISSING')
        anchors.add(x['positive_control'])
        for item in rows:
            receipt=item['receipt']
            if receipt in refs:fail('STAGE7I_DUPLICATE_RAW_RECEIPT')
            refs[receipt]=item
    if len(seen_windows)!=8 or anchors!=request_anchors or len(refs)!=74:
        fail('STAGE7I_COHORT_REPLAY_DRIFT')
    rows=loadcsv(root/'stage7i_outside_57_notice_candidates_REVIEW_ONLY.csv')
    if len(rows)!=74 or {r['receipt'] for r in rows}!=set(refs):fail('STAGE7I_CSV_RECEIPT_DRIFT')
    original_ids={x['receipt'] for x in original}
    for r in rows:
        item=refs[r['receipt']]
        for k in ('code','notice_date','title','source_window','source_page','listing_source_sha256'):
            if str(r.get(k))!=str(item[k]):fail('STAGE7I_CSV_VS_RAW_DRIFT:'+k)
        state=('ALREADY_COVERED' if r['receipt'] in original_ids else
               ('TITLE_ONLY_UNRESOLVED' if r['candidate_class']=='UNRELATED_TITLE_IN_BOUNDED_QUERY' else 'POSSIBLE_MISSING'))
        if r['coverage_state']!=state or r['official_daily_halt_status']!='UNKNOWN' or r['strategy_ready']!='False' or r['body_document_downloaded']!='False':
            fail('STAGE7I_ROW_STATE_PROMOTION')
        if r['receipt'] in original_ids:
            o=next(x for x in original if x['receipt']==r['receipt'])
            if r['title']!=o['title'] or r['code']!=o['code'] or r['notice_date']!=o['notice_date']:
                fail('STAGE7I_OVERLAP_SOURCE_CONFLICT')
    states=collections.Counter(x['coverage_state'] for x in rows)
    if states != {'ALREADY_COVERED':29,'POSSIBLE_MISSING':6,'TITLE_ONLY_UNRESOLVED':39}:
        fail('STAGE7I_EXPECTED_CLASS_COUNTS')
    queue=readj(root/'stage7i_manual_gap_review_queue.json')
    if len(queue)!=45 or {x['receipt'] for x in queue}!={r['receipt'] for r in rows if r['coverage_state']!='ALREADY_COVERED'}:
        fail('STAGE7I_MANUAL_REVIEW_QUEUE_DRIFT')
    if len(TARGET_RECEIPTS)!=MAX_NEW_DOCUMENTS or len(set(TARGET_RECEIPTS))!=MAX_NEW_DOCUMENTS:
        fail('STAGE7J_TARGET_SET_DRIFT')
    by={r['receipt']:r for r in rows}
    if {r['receipt'] for r in rows if r['coverage_state']=='POSSIBLE_MISSING'}!=set(PRIORITY_RECEIPTS):
        fail('STAGE7J_SIX_PRIORITY_RECEIPT_DRIFT')
    for receipt in CONTEXT_RECEIPTS:
        r=by.get(receipt)
        if not r or r['coverage_state']!='TITLE_ONLY_UNRESOLVED' or '회생절차' not in r['title']:
            fail('STAGE7J_CONTEXT_RECEIPT_NOT_SUPPORTED_BY_RAW')
    return {'rows':rows,'by_receipt':by,'summary':summary,
            'verified_7i_list_raw_files':8,'verified_7i_replayed_notices':74,
            'prior_overlaps':29,'priority_six':6,'contextual_four':4,
            'not_in_selected_target_unresolved_count':35}


def check_all_sources(hdir,idir,ddir,fdir,gdir):
    h=prior.check_evidence(hdir)
    previous=verify_original_172(h['documents'],ddir,fdir,gdir,readj(pathlib.Path(hdir)/'stage7h_fail_closed_summary.json'))
    i=verify_7i(idir,h['documents'])
    return {'h':h,'i':i,'previous':previous}


def target_plan(source_data):
    original=source_data['h']['documents'];by=source_data['i']['by_receipt']
    plans=[]
    for receipt in TARGET_RECEIPTS:
        item=by[receipt];code=item['code'];target_date=date.fromisoformat(item['notice_date'])
        near=[]
        for o in original:
            if o['code']!=code:continue
            gap=abs((date.fromisoformat(o['notice_date'])-target_date).days)
            if gap<=7:
                near.append({'receipt':o['receipt'],'title':o['title'],'gap_calendar_days':gap,
                             'incident_candidate_not_verified':o['event_id_candidate_not_verified'],
                             'body_sha256_verified_from_prior_source':o['body_sha256'],
                             'candidate_relation_only':True})
        near.sort(key=lambda x:(x['gap_calendar_days'],x['receipt']))
        plans.append({'code':code,'receipt':receipt,'notice_date':item['notice_date'],
                      'title':item['title'],'source_listing_sha256':item['listing_source_sha256'],
                      'role':'SIX_MARKET_ACTION_REVIEW' if receipt in PRIORITY_RECEIPTS else 'FOUR_REORGANIZATION_CONTEXT',
                      'related_existing_57_near_date_candidates':near[:12],
                      'related_existing_57_near_date_count':len(near),
                      'body_downloaded':False,
                      'independent_asof_publication_time_verified':False,
                      'complete_original_correction_chain_verified':False,
                      'management_watchlist_release_is_not_trading_resumption':True,
                      'official_daily_halt_status':'UNKNOWN','strategy_ready':False})
    return plans


def full_body_url(url,receipt):
    u=urlparse(url)
    if u.scheme!='https' or u.hostname!='kind.krx.co.kr' or u.port not in (None,443) or u.username or u.password:
        fail('OFF_DOMAIN_FULL_BODY_URL')
    path=unquote(u.path)
    if '\\' in path or '..' in path or len(path)>260:fail('BAD_FULL_BODY_PATH')
    want=f'/external/{receipt[:4]}/{receipt[4:6]}/{receipt[6:8]}/{receipt[8:]}/'
    if not path.startswith(want) or not re.fullmatch(r'/external/\d{4}/\d{2}/\d{2}/\d{6}/\d{14}/\d+\.htm',path):
        fail('FULL_BODY_URL_RECEIPT_IDENTITY_NOT_PROVEN')
    return url


def resolve_shell(shell,receipt):
    if not isinstance(shell,bytes):fail('NON_BYTES_SHELL')
    text=shell.decode('euc-kr',errors='replace')
    opts=SHELL_OPTION.findall(text)
    if not opts or len(opts)>12 or len({cid for cid,flag in opts})!=len(opts):
        fail('INVALID_SHELL_CONTENT_OPTIONS:'+receipt)
    ids=[cid for cid,flag in opts if flag=='Y']+[cid for cid,flag in reversed(opts) if flag!='Y']
    return ids, [{'content_id':cid,'viewer_current_flag_not_asof':flag} for cid,flag in opts]


def resolve_document(resolver,receipt):
    if not isinstance(resolver,bytes):fail('NON_BYTES_RESOLVER')
    text=unescape(resolver.decode('euc-kr',errors='replace'))
    for match in FULL_HTML.findall(text):
        try:return full_body_url(match,receipt)
        except RuntimeError:pass
    fail('NO_OFFICIAL_FULL_BODY_URL_IN_RESOLVER')


def document_examination(body,notice,previous_receipts):
    if not isinstance(body,bytes) or not 100<=len(body)<=MAX_RAW_RESPONSE_BYTES:
        fail('INVALID_FULL_BODY_LENGTH')
    decoded=None
    for enc in ('utf-8','euc-kr'):
        try:decoded=body.decode(enc);break
        except UnicodeDecodeError:pass
    if decoded is None:fail('UNKNOWN_DOCUMENT_CHARSET')
    soup=BeautifulSoup(decoded,'html.parser')
    for tag in soup(['script','style','noscript']):tag.decompose()
    text=re.sub(r'\s+',' ',soup.get_text(' ',strip=True))
    compact=re.sub(r'\s+','',text)
    code=notice['code']
    if len(compact)<42 or not (code in text or any(alias.replace(' ','') in compact for alias in COMPANY_ALIASES[code])):
        fail('DOCUMENT_COMPANY_IDENTITY_UNCORROBORATED:'+notice['receipt'])
    rows=[]
    for tr in soup.select('tr'):
        cells=tr.find_all(['th','td'],recursive=False)
        vals=[re.sub(r'\s+',' ',c.get_text(' ',strip=True))[:580] for c in cells]
        if vals and any(x in ''.join(vals) for x in ('정정','회생','상장','관리','거래','심사','개선','지정','취소','철회')):
            rows.append(vals)
    mentions=sorted(set(RECEIPT_PATTERN.findall(text)) & set(previous_receipts))
    return {'body_sha256':sha(body),'body_bytes':len(body),
            'company_identity_corroborated':True,
            'explicit_reference_to_existing_57_receipts':mentions,
            'contains_correction_words_candidate_only':any(x in compact for x in ('정정전','정정후','정정신고','정정관련공시서류')),
            'contains_cancellation_words_candidate_only':any(x in compact for x in ('취소','철회')),
            'contains_resume_words_candidate_only':any(x in compact for x in ('매매거래정지해제','매매거래재개','거래재개')),
            'market_review_status_context_only':any(x in compact for x in ('관리종목','상장폐지','개선기간','실질심사')),
            'structured_rows_for_human_review':rows[:70],
            'body_preview_for_human_review':text[:2100],
            'explicit_original_correction_link_verified_complete':False,
            'independent_publication_timestamp_verified':False,
            'actual_daily_trading_eligibility_verified':False,
            'official_daily_halt_status':'UNKNOWN',
            'strategy_ready':False}


def _safe_response(raw,label):
    if not isinstance(raw,bytes) or not 40<=len(raw)<=MAX_RAW_RESPONSE_BYTES:
        fail('INVALID_KIND_RESPONSE:'+label)
    return raw


def execute(hdir,idir,ddir,fdir,gdir,out,mode='offline',provider=None,pause=1.0):
    if mode not in ('offline','collect'):fail('INVALID_MODE')
    if pause<0 or pause>10:fail('INVALID_RATE_LIMIT')
    sources=check_all_sources(hdir,idir,ddir,fdir,gdir)
    targets=target_plan(sources)
    dest=pathlib.Path(out);dest.mkdir(parents=True,exist_ok=True)
    writej(dest/'stage7j_selected_ten_scope_plan_RESEARCH_ONLY.json',targets)
    common={'schema':SCHEMA,'source_stage7h_run':SOURCE_7H_RUN,'source_stage7h_commit':SOURCE_7H_COMMIT,
            'source_stage7i_run':SOURCE_7I_RUN,'source_stage7i_commit':SOURCE_7I_COMMIT,
            'source_stage7i_artifact_id':SOURCE_7I_ARTIFACT,
            'original_raw_sha_replayed':sources['previous']['previous_172_raw_objects_sha_verified'],
            'original_full_body_count_verified':sources['previous']['previous_57_full_bodies_sha_verified'],
            'original_viewer_count_verified':sources['previous']['previous_57_viewer_shells_sha_verified'],
            'stage7i_list_pages_sha_replayed':8,'stage7i_listed_unique_receipts_sha_replayed':74,
            'original_57_overlap':29,'new_outside_57_title_only_total':45,
            'priority_new_six':6,'prior_context_selected_four':4,'other_35_title_only_still_unreviewed':True,
            'verified_new_original_correction_chains':0,'verified_new_daily_tradeability_rows':0,
            'independent_publication_timestamps_verified':False,
            'historical_daily_tradeability_verified':False,
            'all_original_and_correction_versions_verified':False,
            'all_relevant_market_action_categories_searched':False,
            'complete_three_incident_chain_verified':False,
            'official_daily_halt_status':'UNKNOWN',
            'strategy_ready':False,'lookahead_safe_strategy_filter_approved':False,
            'research_only':True,'production_rank_search_order_unchanged':True,
            'full_market_pit_backtest_approved':False}
    if mode=='offline':
        result={**common,'mode':'offline','status':'TEN_TARGET_SCOPE_VERIFIED_NO_NEW_BODY_FETCH',
                'requested_new_document_count':0,'captured_new_document_count':0,
                'unresolved_new_document_count':10}
        writej(dest/'stage7j_fail_closed_summary.json',result)
        print(json.dumps(result,ensure_ascii=False,sort_keys=True),flush=True)
        return result
    if provider is None:
        # Re-use previously exercised KIND full-document provider from Stage7-D.
        import pit_stage7d_kind_full_document as previous_kind
        provider=previous_kind.KindProvider()
    records=[];raw=[];errors=[];requests=0
    previous_receipts={x['receipt'] for x in sources['h']['documents']}
    for idx,target in enumerate(targets,1):
        receipt=target['receipt'];entry={**target,'status':'UNRESOLVED_REQUIRE_REVIEW',
                 'independent_trading_resumption_verified':False,
                 'relationship_to_existing_event_verified':False}
        def call(stage, method, argument, ext):
            nonlocal requests
            # Count attempted HTTP operations BEFORE invoking the provider; even failed
            # calls belong to the network budget and must not silently disappear.
            if requests>=MAX_PROVIDER_CALLS:fail('MAX_PROVIDER_CALLS_EXCEEDED')
            requests+=1
            data=method(argument)
            body=_safe_response(data,stage)
            name=f'raw_kind_selected/{receipt}_{stage}.{ext}'
            write_bytes(dest/name,body)
            obj={'receipt':receipt,'type':stage,'path':name,'sha256':sha(body),'bytes':len(body)}
            raw.append(obj)
            writej(dest/'stage7j_new_raw_sha_manifest.json',{'schema':SCHEMA,'status':'PARTIAL','files':raw})
            return body
        try:
            shell=call('viewer_shell',provider.shell,receipt,'html')
            ids,flags=resolve_shell(shell,receipt)
            entry['viewer_version_options_not_historical_asof']=flags
            entry['multiple_viewer_versions_currently_observed']=len(flags)>1
            chosen_url=None;chosen_cid=None
            for index,cid in enumerate(ids[:MAX_RESOLVERS_PER_DOC]):
                resp=call(f'searchContents_{index:02d}',provider.resolve,cid,'html')
                try:
                    chosen_url=resolve_document(resp,receipt);chosen_cid=cid;break
                except RuntimeError:continue
            if not chosen_url:fail('NO_RESOLVED_OFFICIAL_BODY_WITHIN_BUDGET:'+receipt)
            body=call('full_body',provider.content,chosen_url,'htm')
            parsed=document_examination(body,entry,previous_receipts)
            entry.update(parsed)
            entry['resolved_official_url']=chosen_url
            entry['chosen_viewer_content_id_not_asof_verified']=chosen_cid
            entry['status']='FULL_BODY_CAPTURED_IDENTITY_VERIFIED_REVIEW_ONLY'
        except Exception as exc:
            diag={'receipt':receipt,'error_type':type(exc).__name__,'detail':str(exc)[:320]}
            errors.append(diag);entry['error']=diag
        records.append(entry)
        writej(dest/'stage7j_selected_ten_full_document_review_RESEARCH_ONLY.json',records)
        print('STAGE7J_KIND_BODY',idx,len(targets),receipt,entry['status'],flush=True)
        if pause and idx<len(targets):time.sleep(pause)
    for x in raw:
        if sha((dest/x['path']).read_bytes())!=x['sha256'] or (dest/x['path']).stat().st_size!=x['bytes']:
            fail('NEW_RAW_SHA_REPLAY_TAMPER')
    n_ok=sum(x['status']=='FULL_BODY_CAPTURED_IDENTITY_VERIFIED_REVIEW_ONLY' for x in records)
    all_ok=n_ok==10 and len(errors)==0 and len(records)==10
    writej(dest/'stage7j_new_raw_sha_manifest.json',{'schema':SCHEMA,
          'status':'COMPLETE_TEN_TARGET_BODIES_CANDIDATES_ONLY' if all_ok else 'PARTIAL_FAIL_CLOSED',
          'files':raw,'new_raw_objects_verified':len(raw)})
    q=[]
    for row in records:
        q.append({'receipt':row['receipt'],'code':row['code'],'role':row['role'],
                  'title':row['title'],'status':row['status'],
                  'body_sha256':row.get('body_sha256',''),
                  'near_date_7h_document_count':row['related_existing_57_near_date_count'],
                  'explicit_reference_receipts':';'.join(row.get('explicit_reference_to_existing_57_receipts',[])),
                  'correction_words_candidate_only':row.get('contains_correction_words_candidate_only',''),
                  'viewer_multiple_versions':row.get('multiple_viewer_versions_currently_observed',''),
                  'independent_publication_time_verified':False,
                  'relationship_verified_complete':False,'official_daily_halt_status':'UNKNOWN',
                  'strategy_ready':False})
    writecsv(dest/'stage7j_manual_relationship_review_queue.csv',q,list(q[0]) if q else ['receipt'])
    result={**common,'mode':'collect',
            'status':'TEN_SELECTED_BODIES_COLLECTED_CHAIN_UNVERIFIED' if all_ok else 'TEN_TARGET_BODY_COLLECTION_INCOMPLETE_FAIL_CLOSED',
            'requested_new_document_count':10,'captured_new_document_count':n_ok,
            'unresolved_new_document_count':len(records)-n_ok,
            'actual_provider_calls':requests,'raw_objects_sha_saved':len(raw),
            'errors':errors,'correction_word_receipts_candidate_only':[x['receipt'] for x in records if x.get('contains_correction_words_candidate_only')],
            'viewer_multiple_version_receipts_candidate_only':[x['receipt'] for x in records if x.get('multiple_viewer_versions_currently_observed')],
            'complete_target_collection':all_ok,
            'warning':'Original 57 and selected 10 document bodies are evidence, not complete amendment lineage, verified release timestamp, or permission to backtest/live.'}
    writej(dest/'stage7j_fail_closed_summary.json',result)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True),flush=True)
    return result


def main(argv=None):
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--stage7h-dir',required=True)
    ap.add_argument('--stage7i-dir',required=True)
    ap.add_argument('--stage7d-dir',required=True)
    ap.add_argument('--stage7f-dir',required=True)
    ap.add_argument('--stage7g-dir',required=True)
    ap.add_argument('--out',required=True)
    ap.add_argument('--mode',choices=['offline','collect'],default='offline')
    ap.add_argument('--pause',type=float,default=1.0)
    x=ap.parse_args(argv)
    result=execute(x.stage7h_dir,x.stage7i_dir,x.stage7d_dir,x.stage7f_dir,x.stage7g_dir,x.out,x.mode,pause=x.pause)
    return 0 if (x.mode=='offline' or result['complete_target_collection']) else 2


if __name__=='__main__':
    try:sys.exit(main())
    except Exception as exc:
        print('FAIL_CLOSED_STAGE7J',type(exc).__name__,str(exc),file=sys.stderr,flush=True)
        sys.exit(2)
