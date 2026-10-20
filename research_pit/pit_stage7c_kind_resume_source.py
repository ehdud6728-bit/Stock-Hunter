#!/usr/bin/env python3
"""PIT Stage7-C: discover official KIND market-action disclosure *documents*, not halt-day truth.

Strict RESEARCH_ONLY. Disclosure titles/receipt timestamps DO NOT establish
trading-resumption effective dates, the absence of suspensions, or execution
feasibility. Never imports or changes production signals/orders.

The selected two codes have independent official KRX 20011 start evidence from
Stage7-A and SHA-audited Stage7-B original-price observations. KIND is a
separate KRX disclosure service, NOT a fully independent source authority.
"""
from __future__ import annotations
import argparse
import calendar
import csv
import hashlib
import io
import json
import pathlib
import re
import sys
import time
from datetime import date, datetime, timezone, timedelta
from html import unescape

SCHEMA = 'STOCKHUNTER_PIT_STAGE7C_KIND_DISCLOSURE_SOURCE_CANARY_V1'
PINNED_STAGE7B_RUN = 39436389287
PINNED_STAGE7B_SHA = '031713507387142b455476c57db3352688fccc50'
STAGE7A_RUN = 39430555533
STAGE1_RUN = 39295295197
PINNED_PRICE_SHA = '08e328c101f0774a2236c0a44cc1fdbbaa59e96e'
PINNED_CODE_EVENTS = {'377460':'2023-11-28','418250':'2024-04-05'}
CODE_ALIASES = {'377460':('큐에이드','위니아에이드'), '418250':('시큐레터',)}
START = '2023-10-01'
END = '2026-10-12'
KIND_URL = 'https://kind.krx.co.kr/disclosure/details.do'
KIND_MARKET_ACTION = ('0311','0305')  # halt/release/period-change + other market notices
RECEIPT = re.compile(r"openDisclsViewer\(['\"](\d{14})['\"]")
CORP_ID = re.compile(r"companysummary_open\(['\"]([A-Za-z0-9]+)['\"]")
DATEFMT = re.compile(r'\d{4}-\d{2}-\d{2}\Z')
PAGE_SIZE = 100
MAX_PAGES = 15


def fail(reason):
    raise RuntimeError('FAIL_CLOSED_STAGE7C_'+str(reason))
def sha256(data):return hashlib.sha256(data).hexdigest()
def output(path,data):
    p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    if isinstance(data,str):data=data.encode('utf-8')
    temp=p.with_name(p.name+'.part');temp.write_bytes(data);temp.replace(p)
def writejson(path,obj):output(path,json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
def writecsv(path,columns,rows):
    s=io.StringIO(newline='');w=csv.DictWriter(s,fieldnames=columns,extrasaction='ignore')
    w.writeheader();w.writerows(rows);output(path,s.getvalue())
def loadjson(path):return json.loads(pathlib.Path(path).read_text(encoding='utf-8'))
def checked_date(text):
    if not DATEFMT.fullmatch(str(text)):fail('BAD_DATE_SYNTAX:'+str(text))
    try:return date.fromisoformat(text)
    except ValueError:fail('BAD_DATE:'+str(text))
def begin_next_month(d):
    return date(d.year+1,1,1) if d.month==12 else date(d.year,d.month+1,1)
def add_months(d,n):
    month0=(d.year*12+d.month-1)+n
    y,m=divmod(month0,12);m+=1
    return date(y,m,min(d.day,calendar.monthrange(y,m)[1]))
def half_year_windows(start=START,end=END):
    lo,hi=checked_date(start),checked_date(end)
    if lo>hi:fail('WINDOW_DATE_ORDER')
    a=lo;segments=[]
    while a<=hi:
        # rolling 6-month ranges; never overlap or skip a date
        b=min(add_months(a,6)-timedelta(days=1),hi)
        segments.append((a.isoformat(),b.isoformat()))
        a=b+timedelta(days=1)
    if segments[0][0]!=start or segments[-1][1]!=end or any(
        checked_date(segments[i][1])+timedelta(days=1)!=checked_date(segments[i+1][0])
        for i in range(len(segments)-1)):
        fail('WINDOW_GAP_OR_OVERLAP')
    return segments


def preflight(stage7b_dir):
    root=pathlib.Path(stage7b_dir)
    j=loadjson(root/'stage7b_halt_price_bridge_summary.json')
    if (j.get('source_stage1_run')!=STAGE1_RUN or
        j.get('source_stage1_commit')!=PINNED_PRICE_SHA or
        j.get('source_stage7a_run')!=STAGE7A_RUN or
        j.get('source_stage7a_commit')!='d60639f9f9920290b51dbb9e502ead0a4cbe254f' or
        j.get('status')!='OFFICIAL_HALT_START_VS_IMMUTABLE_PRICE_OBSERVATION_MATCH_ONLY' or
        j.get('official_halt_start_event_rows')!=2 or
        j.get('official_resume_event_rows')!=0 or
        j.get('focus_price_observations')!=1809 or
        j.get('source_original_daily_price_sha_verified') is not True or
        j.get('strategy_ready') is not False or
        sorted(j.get('reviewed_codes',[]))!=sorted(PINNED_CODE_EVENTS)):
        fail('PREVIOUS_STAGE7B_ATTESTATION_DRIFT')
    events=j.get('focus_event_comparisons',[])
    if len(events)!=2 or {e.get('code'):e.get('official_halt_event_date') for e in events}!=PINNED_CODE_EVENTS:
        fail('STAGE7B_EVENT_START_DRIFT')
    for e in events:
        if e.get('actual_official_resume_date_confirmed') is not False or e.get('official_resume_date') is not None:
            fail('STAGE7B_RESUME_MISCLASSIFICATION')
    return {'stage7b_source_run':PINNED_STAGE7B_RUN,
            'stage7b_source_blob_sha256':sha256((root/'stage7b_halt_price_bridge_summary.json').read_bytes()),
            'stage7b_source_event_codes':list(sorted(PINNED_CODE_EVENTS))}


def kind_form(code,start,end,page):
    if code not in PINNED_CODE_EVENTS:fail('CODE_OUTSIDE_FOCUS:'+code)
    checked_date(start);checked_date(end)
    if start>end or page<1 or page>MAX_PAGES:fail('BAD_KIND_WINDOW_OR_PAGE')
    d={
        'method':'searchDetailsSub','forward':'details_sub',
        'currentPageSize':str(PAGE_SIZE),'pageIndex':str(page),'orderMode':'1','orderStat':'D',
        'searchCodeType':'','repIsuSrtCd':'A'+code,'allRepIsuSrtCd':'',
        'oldSearchCorpName':'','disclosureType':'','disTypevalue':'','reportNm':'',
        'reportCd':'','searchCorpName':'','business':'','marketType':'','kosdaqSegment':'',
        'settlementMonth':'','securities':'','submitOblgNm':'','enterprise':'',
        'reportNmTemp':'','reportNmPop':'','bfrDsclsType':'on',
        'fromDate':start,'toDate':end,
        'lastReport':'',  # preserve original + corrected versions; PIT uses publication date only
        'disclosureType02':'0311|0305|','pDisclosureType02':'0311|0305|',
        'disclosureTypeArr02':list(KIND_MARKET_ACTION),
    }
    for t in ('01','03','04','05','06','07','08','09','10','11','13','14','20'):
        d['disclosureType'+t]='';d['pDisclosureType'+t]=''
    return d


def parse_kind_html(raw, code, start, end, page):
    """Extract *candidate* notice metadata, never an actual halt/resume state.

    Fail closed on unknown HTML, cross-company results or wrong dates, including
    server responses that silently ignore a filter.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError as e:fail('BEAUTIFULSOUP_REQUIRED:'+str(e))
    if not isinstance(raw,(str,bytes)):fail('INVALID_HTML_INPUT')
    s=raw.decode('utf-8') if isinstance(raw,bytes) else raw
    soup=BeautifulSoup(s,'html.parser')
    table=soup.select_one('table.list.type-00.mt10')
    if table is None or table.find('tbody') is None:
        fail('KIND_DETAILS_EXPECTED_TABLE_MISSING:'+code+':'+str(page))
    out=[];data_rows=0
    for tr in table.select('tbody > tr'):
        td=tr.find_all('td',recursive=False)
        if len(td)<5:
            # no-results placeholder has 1 td; other structures are not trusted.
            if len(td)==1 and ('조회' in td[0].get_text(' ',strip=True) or
                               '없습니다' in td[0].get_text(' ',strip=True) or
                               '데이터' in td[0].get_text(' ',strip=True)):
                continue
            if not td:continue
            fail('KIND_ROW_UNRECOGNIZED_COLUMN_COUNT:'+str(len(td)))
        data_rows+=1
        cell=td[2];a=cell.find('a');titlecell=td[3];ta=titlecell.find('a')
        if a is None or ta is None:fail('KIND_ROW_EXPECTED_LINK_MISSING')
        name=a.get_text(' ',strip=True)
        if not any(alias in name for alias in CODE_ALIASES[code]):
            fail('KIND_COMPANY_FILTER_LEAK:'+code+':'+name[:100])
        cid=CORP_ID.search(a.get('onclick',''))
        receipt=RECEIPT.search(ta.get('onclick',''))
        if not cid or not receipt:fail('KIND_COMPANY_OR_RECEIPT_MISSING')
        accession=receipt.group(1)
        submission_day=accession[:4]+'-'+accession[4:6]+'-'+accession[6:8]
        checked_date(submission_day)
        if not start<=submission_day<=end:fail('KIND_RECEIPT_OUTSIDE_QUERY_WINDOW:'+accession)
        title=unescape(ta.get('title','').strip() or ta.get_text(' ',strip=True))
        if not title:fail('KIND_TITLE_EMPTY:'+accession)
        marketimg=cell.select_one('img.legend')
        market=marketimg.get('alt','') if marketimg else ''
        out.append({'code':code,'company_name':name,'kind_company_id':cid.group(1),
                    'submission_date_from_accession':submission_day,'accession':accession,
                    'title':title,'market_label':market,'listed_cell_time':td[1].get_text(' ',strip=True),
                    'notice_class':classify_title(title),
                    'query_from':start,'query_to':end,'source_page':page,
                    'notice_is_official_source_candidate':True,
                    'official_resume_effective_date_verified':False,
                    'daily_halt_state_verified':False})
    if data_rows>PAGE_SIZE:fail('KIND_PAGE_SIZE_EXCEEDED')
    return out


def classify_title(title):
    """Categories are *research queues*, not executable effective-date evidence."""
    t=re.sub(r'\s+','',str(title))
    if '거래정지' in t and ('해제' in t or '재개' in t):
        return 'POSSIBLE_RELEASE_NOTICE_TITLE_ONLY'
    if '정지기간변경' in t or ('거래정지' in t and '기간변경' in t):
        return 'HALT_PERIOD_CHANGE_TITLE_ONLY'
    if '거래정지' in t:return 'HALT_START_OR_NOTICE_TITLE_ONLY'
    return 'OTHER_MARKET_NOTICE_TITLE_REVIEW'


def fetch_html(session,params,retries=3,delay=1.1):
    err=None
    for attempt in range(retries):
        try:
            r=session.post(KIND_URL,data=params,timeout=(12,35),headers={
                'User-Agent':'Mozilla/5.0 (compatible; StockHunter-PIT-RESEARCH-ONLY/1.0)',
                'Referer':'https://kind.krx.co.kr/disclosure/details.do',
                'Accept':'text/html,application/xhtml+xml'})
            r.raise_for_status()
            if not r.content:fail('KIND_EMPTY_HTTP_BODY')
            if 'html' not in r.headers.get('Content-Type','text/html').lower():
                fail('KIND_UNEXPECTED_CONTENT_TYPE')
            return r.content
        except Exception as e:
            err=e
            if attempt+1<retries:time.sleep(delay*(attempt+1))
    fail('KIND_REQUEST_FAILED:'+str(err)[:180])



def verify_saved_raw(root, expected_requests=None):
    root=pathlib.Path(root)
    m=loadjson(root/'source_manifest.json')
    if m.get('status')!='COMPLETE' or not isinstance(m.get('files'),list):
        fail('KIND_RAW_MANIFEST_INCOMPLETE')
    records=m['files']
    if expected_requests is not None and len(records)!=expected_requests:
        fail('KIND_SAVED_REQUEST_COUNT_MISMATCH')
    seen=set();total_rows=0
    for x in records:
        key=(x['code'],x['from'],x['to'],x['page'])
        if key in seen:fail('KIND_DUPLICATED_QUERY_KEY')
        seen.add(key)
        rel=x['raw_file']
        if not re.fullmatch(r'raw_kind/[0-9A-Z]{6}_[0-9]{8}_[0-9]{8}_p[0-9]{2}\.html',rel):
            fail('KIND_INVALID_RAW_FILE_PATH')
        raw=(root/rel).read_bytes()
        if sha256(raw)!=x['response_sha256'] or len(raw)!=x['bytes']:
            fail('KIND_RAW_SHA_MISMATCH:'+rel)
        parsed=parse_kind_html(raw,x['code'],x['from'],x['to'],x['page'])
        if len(parsed)!=x['parsed_rows']:
            fail('KIND_REPLAY_PARSE_COUNT_MISMATCH:'+rel)
        total_rows+=len(parsed)
    return {'verified_raw_html_files':len(records),'parsed_notice_rows_before_receipt_dedup':total_rows,
            'raw_source_sha_verified':True}

def run(stage7b,out,provider=None,pause=.8):
    # Provider is an injected test function that returns raw HTML bytes.
    pinned=preflight(stage7b)
    root=pathlib.Path(out);root.mkdir(parents=True,exist_ok=True)
    w=half_year_windows()
    output(root/'scoped_request_policy.json',json.dumps({
        'schema':SCHEMA,'codes':sorted(PINNED_CODE_EVENTS), 'window_start':START,'window_end':END,
        'market_action_category_codes':KIND_MARKET_ACTION, 'half_year_windows':w,
        'all_versions_last_report':'','type_and_daily_status_remain_unknown':True},
        ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    if provider is None:
        import requests
        sess=requests.Session()
        provider=lambda form:fetch_html(sess,form)
    rows=[];raw_manifest=[];req_count=0;pages_with_records=0
    seen_accessions={}
    try:
        for code in sorted(PINNED_CODE_EVENTS):
            for a,b in w:
                for page in range(1,MAX_PAGES+1):
                    form=kind_form(code,a,b,page)
                    raw=provider(form)
                    if isinstance(raw,str):raw=raw.encode('utf-8')
                    if not isinstance(raw,bytes):fail('INVALID_RESPONSE_BYTES')
                    rel=f'raw_kind/{code}_{a.replace("-","")}_{b.replace("-","")}_p{page:02d}.html'
                    output(root/rel,raw)  # preserve provider response BEFORE trying to parse
                    mf={'code':code,'from':a,'to':b,'page':page,'raw_file':rel,
                        'response_sha256':sha256(raw),'bytes':len(raw)}
                    raw_manifest.append(mf)
                    writejson(root/'source_manifest.json',{'status':'PARTIAL','files':raw_manifest})
                    parsed=parse_kind_html(raw,code,a,b,page)
                    mf['parsed_rows']=len(parsed)
                    req_count+=1;pages_with_records+=bool(parsed)
                    for row in parsed:
                        k=row['accession'];old=seen_accessions.get(k)
                        if old is not None:
                            if (old['code']!=row['code'] or old['title']!=row['title'] or
                                old['submission_date_from_accession']!=row['submission_date_from_accession']):
                                fail('CROSS_PAGE_RECEIPT_CONFLICT:'+k)
                            continue
                        row['source_raw_sha256']=mf['response_sha256']
                        row['source_raw_file']=rel
                        rows.append(row);seen_accessions[k]=row
                    print('STAGE7C_KIND_WINDOW',code,a,b,'page',page,
                          'notice_candidates',len(parsed),flush=True)
                    if len(parsed)<PAGE_SIZE:break
                    if page==MAX_PAGES:fail('KIND_PAGINATION_EXHAUSTED:'+code+':'+a)
                    if pause:time.sleep(pause)
                if pause:time.sleep(pause)
    finally:
        # No extra uploads needed on error; raw source and manifest are retained.
        writejson(root/'source_manifest.json',{'status':'COMPLETE' if req_count and req_count==len(raw_manifest) else 'PARTIAL',
                                               'files':raw_manifest})
    replay=verify_saved_raw(root,expected_requests=req_count)
    rows.sort(key=lambda r:(r['submission_date_from_accession'],r['accession']))
    cols=['code','company_name','kind_company_id','submission_date_from_accession','accession',
          'title','market_label','listed_cell_time','notice_class','query_from','query_to',
          'source_page','source_raw_sha256','source_raw_file',
          'notice_is_official_source_candidate','official_resume_effective_date_verified','daily_halt_state_verified']
    writecsv(root/'all_kind_market_notice_candidates.csv',cols,rows)
    possible=[r for r in rows if r['notice_class']=='POSSIBLE_RELEASE_NOTICE_TITLE_ONLY']
    writecsv(root/'possible_resume_titles_REQUIRE_FULL_BODY_AND_EFFECTIVE_DATE.csv',cols,possible)
    matched_codes=sorted({x['code'] for x in rows})
    summary={'schema':SCHEMA,'status':'KIND_MARKET_DISCLOSURE_SOURCE_CANDIDATES_ONLY' if matched_codes else 'KIND_CANARY_NO_MATCHED_SOURCE',
        'source_stage7b':pinned,'raw_replay_evidence':replay,'request_count':req_count,'expected_min_request_count':len(w)*len(PINNED_CODE_EVENTS),
        'saved_raw_response_count':len(raw_manifest),'source_market':'KRX_KIND_DISCLOSURE',
        'source_disclosure_category_codes':list(KIND_MARKET_ACTION),
        'queried_codes':sorted(PINNED_CODE_EVENTS),'matched_codes':matched_codes,
        'notice_candidate_count':len(rows), 'possible_release_title_count':len(possible),
        'all_notice_receipts_are_document_candidates_not_effective_dates':True,
        'official_resume_effective_date_verified':False,'historical_daily_halt_status_verified':False,
        'historical_security_type_verified':False,'full_market_halt_register_verified':False,
        'execution_feasibility_verified':False,'strategy_ready':False,'research_only':True,
        'production_or_ranking_changed':False,
        'notice_ambiguity':'Title and receipt do not determine effective start/restart or interday/immediate tradability. Full KIND document with explicit effective date and corporate identity must be checked separately.',
        'generated_at_utc':datetime.now(timezone.utc).isoformat()}
    writejson(root/'stage7c_kind_resume_source_summary.json',summary)
    print(json.dumps({k:summary[k] for k in ('status','request_count','notice_candidate_count','matched_codes',
             'possible_release_title_count','strategy_ready')},ensure_ascii=False),flush=True)
    if not matched_codes:fail('NO_POSITIVE_KIND_SOURCE_CONTROL')
    return summary


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage7b-dir',default='stage7b-source')
    p.add_argument('--out',default='reports/pit-stage7c')
    p.add_argument('--pause',type=float,default=.8)
    a=p.parse_args()
    run(a.stage7b_dir,a.out,pause=a.pause)

if __name__=='__main__':
    try:main()
    except Exception as e:
        print('FAIL_CLOSED_STAGE7C',type(e).__name__,str(e),file=sys.stderr,flush=True)
        sys.exit(2)
