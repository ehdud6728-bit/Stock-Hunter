#!/usr/bin/env python3
"""PIT Stage7-D: SHA-replay Stage7-C official KIND disclosure candidates, fetch FULL documents.

SOURCE-DOCUMENT CANARY ONLY; document text and effective-date *candidates* do not
establish verified transaction-time halts, resumption, or historical execution.
No production import, strategy-score/rank change, order or LIVE functionality.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import io
import json
import pathlib
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import pit_stage7c_kind_resume_source as prior

SCHEMA='STOCKHUNTER_PIT_STAGE7D_KIND_FULL_NOTICE_DOCUMENT_CANARY_V1'
STAGE7C_RUN=39516839081
STAGE7C_SHA='a88af6dbde14d4413f228fcd8b7c5e692fe48d7b'
STAGE7B_RUN=39436389287
ALLOWED_HOST='kind.krx.co.kr'
VIEWER='https://kind.krx.co.kr/common/disclsviewer.do'
REQUIRED=(
    '20231026000748','20231120000599','20231127000588','20240110000746',
    '20240405000391','20240405000982','20250124000730','20250124000734',
    '20260722000930','20260918000770',
)
SHELL_OPTION=re.compile(r'<option\b[^>]*\bvalue=[\'"](\d{14})\|([YN])[\'"]',re.I)
FULL_HTML=re.compile(r'https?://[^\s\'"<>]+?\.htm(?:\?[^\s\'"<>]*)?',re.I)
DATE_PATTERN=re.compile(r'(?<!\d)(20\d{2})[.\-/년\s]+([01]?\d)[.\-/월\s]+([0-3]?\d)(?:일)?(?!\d)')
LABEL_PATTERN=re.compile(r'정지|해제|재개|기간|일시|변경|상장폐지|거래')


def fail(x):raise RuntimeError('FAIL_CLOSED_STAGE7D_'+str(x))
def sha(b):return hashlib.sha256(b).hexdigest()
def write(path,data):
    p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    b=data.encode('utf-8') if isinstance(data,str) else data
    temp=p.with_name(p.name+'.part');temp.write_bytes(b);temp.replace(p)
def dump(path,obj):write(path,json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True)+'\n')
def read_json(p):return json.loads(pathlib.Path(p).read_text(encoding='utf8'))
def read_csv(p):
    with pathlib.Path(p).open(encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))
def out_csv(p,fields,records):
    o=io.StringIO(newline='');w=csv.DictWriter(o,fieldnames=fields,extrasaction='ignore')
    w.writeheader();w.writerows(records);write(p,o.getvalue())


def stage7c_preflight(source):
    """Reject missing/mutated original KIND source and mismatched CSV receipt metadata."""
    p=pathlib.Path(source)
    s=read_json(p/'stage7c_kind_resume_source_summary.json')
    if not (s.get('schema')==prior.SCHEMA and
            s.get('status')=='KIND_MARKET_DISCLOSURE_SOURCE_CANDIDATES_ONLY' and
            s.get('request_count')==14 and s.get('notice_candidate_count')==57 and
            s.get('possible_release_title_count')==1 and
            s.get('matched_codes')==['377460','418250'] and
            s.get('strategy_ready') is False and
            s.get('official_resume_effective_date_verified') is False and
            s.get('source_stage7b',{}).get('stage7b_source_run')==STAGE7B_RUN):
        fail('SOURCE_STAGE7C_SUMMARY_OR_PINS_DRIFT')
    m=read_json(p/'source_manifest.json')
    if m.get('status')!='COMPLETE' or len(m.get('files',[]))!=14:
        fail('SOURCE_STAGE7C_RAW_MANIFEST_DRIFT')
    source_rows=[];seen=set()
    for rec in m['files']:
        code,start,end,page=rec['code'],rec['from'],rec['to'],rec['page']
        key=(code,start,end,page)
        if key in seen:fail('SOURCE_DUPLICATE_WINDOW:'+str(key))
        seen.add(key)
        rel=rec['raw_file']
        if not re.fullmatch(r'raw_kind/[0-9A-Z]{6}_[0-9]{8}_[0-9]{8}_p\d{2}\.html',rel):
            fail('RAW_PATH_MALFORMED')
        buf=(p/rel).read_bytes()
        if sha(buf)!=rec['response_sha256'] or len(buf)!=rec['bytes']:
            fail('SOURCE_STAGE7C_SHA_OR_SIZE_DRIFT:'+rel)
        parsed=prior.parse_kind_html(buf,code,start,end,page)
        if len(parsed)!=rec['parsed_rows']:fail('SOURCE_STAGE7C_REPLAY_ROWCOUNT_DRIFT:'+rel)
        for row in parsed:
            row['source_raw_sha256']=rec['response_sha256'];row['source_raw_file']=rel
            if row['accession'] in {r['accession'] for r in source_rows}:
                fail('UNEXPECTED_DUPLICATE_RECEIPT:'+row['accession'])
            source_rows.append(row)
    expected=read_csv(p/'all_kind_market_notice_candidates.csv')
    if len(source_rows)!=57 or len(expected)!=57:fail('KIND_57_NOTICE_COUNT_DRIFT')
    keys=('accession','code','title','submission_date_from_accession','source_raw_sha256','source_raw_file')
    a={x['accession']:tuple(str(x[k]) for k in keys) for x in expected}
    b={x['accession']:tuple(str(x[k]) for k in keys) for x in source_rows}
    if a!=b:fail('STAGE7C_CANDIDATE_CSV_VS_RAW_REPLAY_DRIFT')
    if not set(REQUIRED)<=set(a):fail('MISSING_TARGET_OFFICIAL_RECEIPTS:'+str(set(REQUIRED)-set(a)))
    if sum(x['notice_class']=='POSSIBLE_RELEASE_NOTICE_TITLE_ONLY' for x in expected)!=1:
        fail('UNEXPECTED_RELEASE_TITLE_COUNT')
    return {r['accession']:r for r in expected}, {
      'verified_stage7c_raw_html':len(seen),'verified_stage7c_notice_rows':len(a),
      'stage7c_manifest_sha256':sha((p/'source_manifest.json').read_bytes()),
      'source_stage7c_run':STAGE7C_RUN,'source_stage7c_commit':STAGE7C_SHA,
      'stage7c_raw_manifest_replayed':True,
    }


def document_target(raw_url, receipt):
    """Only same-origin KIND official HTML at path derived from the receipt."""
    from urllib.parse import unquote
    u=urlparse(raw_url)
    if u.scheme!='https' or u.hostname!=ALLOWED_HOST or u.port not in (None,443) or u.username or u.password:
        fail('CROSS_ORIGIN_DOCUMENT_URL')
    path=unquote(u.path)
    if '..' in path or '\\' in path:fail('INVALID_DOCUMENT_PATH')
    # Document path includes original receipt date and the six-digit accession serial.
    want=f"/external/{receipt[:4]}/{receipt[4:6]}/{receipt[6:8]}/{receipt[8:]}/"
    if not path.startswith(want) or not path.endswith('.htm') or len(path)>260:
        fail('UNEXPECTED_DOCUMENT_URL_FOR_RECEIPT:'+receipt+':'+path[:200])
    if not re.fullmatch(r'/external/\d{4}/\d{2}/\d{2}/\d{6}/\d{14}/\d+\.htm',path):
        fail('UNEXPECTED_DOCUMENT_PATH_FORMAT')
    return raw_url


def resolve_shell(shell_bytes,receipt):
    if not isinstance(shell_bytes,bytes):fail('INVALID_SHELL_BYTES')
    shell=shell_bytes.decode('euc-kr',errors='replace')
    ids=SHELL_OPTION.findall(shell)
    if not ids:fail('DOCUMENT_SHELL_NO_CONTENT_IDS:'+receipt)
    preferred=[cid for cid,flag in ids if flag=='Y']+[cid for cid,flag in reversed(ids) if flag!='Y']
    if len(preferred)>12 or len(set(preferred))!=len(preferred):fail('SHELL_DUPLICATE_OR_TOO_MANY_CONTENT_IDS')
    return preferred


def resolve_response(raw,receipt):
    if not isinstance(raw,bytes):fail('NON_BYTES_RESOLVER')
    # KIND shell/searchContents is EUC-KR and can also contain HTML entities.
    from html import unescape
    txt=unescape(raw.decode('euc-kr',errors='replace'))
    urls=FULL_HTML.findall(txt)
    for candidate in urls:
        try:return document_target(candidate,receipt)
        except RuntimeError:continue
    fail('NO_VALID_FULL_DOCUMENT_URL:'+receipt)


def interpret_document(raw,notice):
    """Extract verbatim labeled facts for human review, never an approved effective date."""
    from bs4 import BeautifulSoup
    if not isinstance(raw,bytes) or len(raw)<100:fail('DOCUMENT_TOO_SHORT_OR_UNREADABLE')
    decoded=None
    for enc in ('utf-8','euc-kr'):
        try:decoded=raw.decode(enc);break
        except UnicodeError:continue
    if decoded is None:fail('DOCUMENT_UNKNOWN_ENCODING')
    soup=BeautifulSoup(decoded,'html.parser')
    for tag in soup(['script','style','noscript']):tag.decompose()
    all_text=re.sub(r'\s+',' ',soup.get_text(' ',strip=True))
    if len(all_text)<45 or 'blank.html' in all_text[:80]:fail('DOCUMENT_MISSING_ACTUAL_TEXT')
    notice_titles=re.sub(r'\s+','',notice['title'])
    # Complete text may contain a condensed title; title match is diagnostic, not mandatory.
    compact=re.sub(r'\s+','',all_text)
    title_key='매매거래정지'
    mentions_market_action=('거래정지' in compact or '매매거래' in compact)
    aliases=prior.CODE_ALIASES[notice['code']]
    identity_text_corroborated=(notice['code'] in all_text or any(a in compact for a in aliases))
    matched_title=notice_titles in compact
    labels=[];seen=set()
    for tr in soup.select('tr'):
        heads=tr.find_all('th',recursive=False)
        vals=tr.find_all('td',recursive=False)
        if heads and vals:
            for i,h in enumerate(heads):
                label=re.sub(r'\s+',' ',h.get_text(' ',strip=True))[:130]
                val=re.sub(r'\s+',' ',vals[min(i,len(vals)-1)].get_text(' ',strip=True))[:800]
                if LABEL_PATTERN.search(label+' '+val) and (label,val) not in seen:
                    seen.add((label,val));labels.append({'label':label,'verbatim_value':val,
                       'date_text_candidates':';'.join(m.group(0) for m in DATE_PATTERN.finditer(val))[:280]})
    return {'document_body_text_chars':len(all_text), 'body_corroborates_official_company_or_code':identity_text_corroborated,
      'document_mentions_market_action':mentions_market_action, 'body_contains_exact_title':matched_title,
      'labeled_rows_for_manual_review':labels[:90],
      'body_text_preview_for_review':all_text[:3000],
      'possible_effective_date_texts_are_not_approved':True,
      'official_resume_effective_date_verified':False,
      'official_halt_end_date_verified':False,
      'daily_trade_eligibility_verified':False,
      'parse_state':'DOCUMENT_CAPTURED_CANDIDATES_ONLY' if (identity_text_corroborated and mentions_market_action)
          else 'DOCUMENT_CAPTURED_IDENTITY_OR_ACTION_UNCORROBORATED'}


class KindProvider:
    def __init__(self,session=None):
        import requests
        self.session=session or requests.Session()
        self.headers={'User-Agent':'Mozilla/5.0 (StockHunter PIT RESEARCH ONLY)',
                      'Referer':'https://kind.krx.co.kr/disclosure/details.do'}
    def fetch(self,url,method='get',body=None):
        import requests
        if method=='get':resp=self.session.get(url,headers=self.headers,timeout=(10,40))
        elif method=='post':resp=self.session.post(url,data=body,headers=self.headers,timeout=(10,40))
        else:fail('INVALID_METHOD')
        resp.raise_for_status()
        if not resp.content:fail('KIND_EMPTY_RESPONSE')
        final=urlparse(resp.url)
        if final.scheme!='https' or final.hostname!=ALLOWED_HOST:fail('KIND_REDIRECTED_OUTSIDE_DOMAIN')
        return bytes(resp.content)
    def shell(self,receipt):
        return self.fetch(VIEWER+'?method=search&acptno='+receipt+'&docno=&viewerhost=&viewerport=')
    def resolve(self,content_id):
        return self.fetch(VIEWER,method='post',body={'method':'searchContents','docNo':content_id})
    def content(self,url):
        return self.fetch(url)


def run(stage7c_dir,out,provider=None,pause=0.65):
    indexed,ref=stage7c_preflight(stage7c_dir)
    dest=pathlib.Path(out);dest.mkdir(parents=True,exist_ok=True)
    provider=provider or KindProvider()
    results=[];mf=[];full_ok=0
    for n,receipt in enumerate(REQUIRED,1):
        notice=indexed[receipt]
        raw_files=[]
        def record_raw(label,blob,ext):
            if not isinstance(blob,bytes):fail('NON_BYTES_SOURCE_RESPONSE')
            rel=f'raw_kind_document/{receipt}_{label}.{ext}'
            write(dest/rel,blob)
            item={'receipt':receipt,'type':label,'path':rel,'sha256':sha(blob),'bytes':len(blob)}
            raw_files.append(item);mf.append(item)
            dump(dest/'raw_full_document_sha_manifest.json',{'schema':SCHEMA,'status':'PARTIAL','files':mf})
        row={'code':notice['code'],'accession':receipt,'notice_title':notice['title'],
             'notice_date':notice['submission_date_from_accession'],
             'notice_time_display':notice['listed_cell_time'],
             'notice_class':notice['notice_class'],
             'official_resume_effective_date_verified':False,
             'official_halt_end_date_verified':False,
             'daily_trade_eligibility_verified':False,'strategy_ready':False,
             'source_kind':'KIND_OFFICIAL_DISCLOSURE_DOCUMENT',
             'event_receipt_provenance_verified':True}
        try:
            shell=provider.shell(receipt)
            record_raw('viewer_shell',shell,'html')
            ids=resolve_shell(shell,receipt)
            raw_url=None
            for i,cid in enumerate(ids):
                resolver=provider.resolve(cid)
                record_raw(f'searchContents_{i:02d}',resolver,'html')
                try:
                    raw_url=resolve_response(resolver,receipt)
                    break
                except RuntimeError:continue
            if raw_url is None:fail('NO_VALID_CONTENT_URL_FOR_RECEIPT:'+receipt)
            body=provider.content(raw_url)
            record_raw('full_body',body,'htm')
            row['resolved_content_url']=raw_url
            row['raw_document_body_sha256']=sha(body)
            row.update(interpret_document(body,notice))
            full_ok+=1
        except Exception as exc:
            # Archive raw even if a provider or document format is currently unsupported.
            row['parse_state']='UNRESOLVED_REQUIRES_MANUAL_REVIEW'
            row['diagnostic']=type(exc).__name__+': '+str(exc)[:330]
        results.append(row)
        dump(dest/'stage7d_partial_document_results.json',results)
        print('STAGE7D_FULL_DOCUMENT',n,len(REQUIRED),receipt,row['parse_state'],flush=True)
        if pause:time.sleep(pause)
    dump(dest/'raw_full_document_sha_manifest.json',{'schema':SCHEMA,'status':'COMPLETE','files':mf})
    verify=0
    for f in mf:
        if sha((dest/f['path']).read_bytes())!=f['sha256'] or (dest/f['path']).stat().st_size!=f['bytes']:
            fail('SHA_REPLAY_MISMATCH:'+f['path'])
        verify+=1
    cols=['code','accession','notice_title','notice_date','notice_time_display','notice_class','parse_state',
          'body_corroborates_official_company_or_code','document_mentions_market_action','body_contains_exact_title',
          'raw_document_body_sha256','resolved_content_url','official_resume_effective_date_verified',
          'official_halt_end_date_verified','daily_trade_eligibility_verified','strategy_ready','diagnostic']
    out_csv(dest/'stage7d_full_notice_document_audit.csv',cols,results)
    write(dest/'stage7d_labeled_table_candidates_for_manual_review.json',json.dumps([
      {'code':x['code'],'accession':x['accession'],'notice_title':x['notice_title'],
       'parse_state':x['parse_state'],'labeled_rows':x.get('labeled_rows_for_manual_review',[]),
       'body_text_preview':x.get('body_text_preview_for_review','')}
      for x in results],ensure_ascii=False,indent=2)+'\n')
    summary={'schema':SCHEMA,'status': 'KIND_FULL_DOCUMENT_CANARY_COLLECTED_CANDIDATE_EVIDENCE_ONLY' if full_ok else 'KIND_FULL_DOCUMENT_SOURCE_NOT_VALIDATED',
      'source_stage7c':ref, 'requested_official_receipts':len(REQUIRED),
      'full_document_resolved_count':full_ok,'document_unresolved_count':len(REQUIRED)-full_ok,
      'retrieved_raw_provider_files':verify,'raw_sha_replay_verified':True,
      'body_identity_and_action_corroborated_count':sum(r.get('parse_state')=='DOCUMENT_CAPTURED_CANDIDATES_ONLY' for r in results),
      'official_resume_effective_date_verified':False,'official_halt_end_date_verified':False,
      'daily_tradability_verified':False,'official_daily_halt_interval_master_verified':False,
      'all_3341_codes_halt_status_verified':False,
      'historical_security_type_verified':False,'strategy_ready':False,'research_only':True,
      'production_rank_search_order_unchanged':True,
      'warning':'Captured official document text and extracted table values are only research evidence. Do not infer any effective halt or resume date without independent full-body reading, correction-chain analysis, and publication-time verification.',
      'generated_at_utc':datetime.now(timezone.utc).isoformat()}
    dump(dest/'stage7d_kind_full_document_summary.json',summary)
    print(json.dumps(summary,ensure_ascii=False,sort_keys=True),flush=True)
    if full_ok==0:fail('ALL_TEN_FULL_DOCUMENT_FETCHES_UNRESOLVED')
    # Nonzero unresolved receipts require further study; do not promote a source result to coverage.
    return summary


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage7c-dir',default='stage7c-source')
    p.add_argument('--out',default='reports/pit-stage7d')
    p.add_argument('--pause',type=float,default=0.65)
    x=p.parse_args(argv)
    run(x.stage7c_dir,x.out,pause=x.pause)

if __name__=='__main__':
    try:main()
    except Exception as exc:
        print('FAIL_CLOSED_STAGE7D',type(exc).__name__,str(exc),file=sys.stderr,flush=True)
        sys.exit(2)
