#!/usr/bin/env python3
"""Stage7 bounded KRX *official* 20011 individual halt-register live source canary.

RESEARCH_ONLY. This is NOT a dated all-issue suspension master. The historical
listing events provide ISINs only for a subset, and Stage6's zero-volume proxy
is NOT itself evidence of trading suspension. Every raw provider response is
preserved with SHA256 before schema parsing. Never activates any strategy.
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
from collections import Counter
from datetime import datetime,timezone

SOURCE_STAGE3_RUN=39364301572
SOURCE_STAGE6_RUN=39408210227
SOURCE_STAGE1_RUN=39295295197
SOURCE_STAGE1_SHA='08e328c101f0774a2236c0a44cc1fdbbaa59e96e'
EXPECTED_CAP_ROWS=5090428
EXPECTED_ZERO_VOLUME=229867
EXPECTED_CODES=3341
EXPECTED_UNRESOLVED_TYPES=2023
EXPECTED_NEWLIST=1015
CUTOFF='2026-10-12'
WINDOW=('20250101','20261012')   # 650 days, less than KRX 730-day restriction
# Stage7 39423727900 failed with 12/12 *empty* replies because the test queried
# 2025-26 despite selecting candidates by zero-volume counts from 2019-2026.
# Do not change endpoint or treat empty rows as a proven clear/active status.
# Cover all Stage1 observation years with disjoint KRX <=730-day windows.
HISTORICAL_WINDOWS=(('20190102','20201231'), ('20210101','20221231'),
                    ('20230101','20241230'), ('20241231','20261012'))
KRX_URL='https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
KRX_BLD='dbms/MDC/STAT/issue/MDCSTAT21301'   # KRX 20011 individual halt history
SHORT=re.compile(r'[0-9][0-9A-Z]{5}\Z')
ISIN=re.compile(r'[A-Z]{2}[A-Z0-9]{10}\Z')
DATE_KEYS=('TRD_HALT_DD','TRD_STOP_DD','HALT_DD','STOP_DD','SUSPEND_DD','TRD_STP_DD','정지일')
RESUME_KEYS=('TRD_RSM_DD','RESTART_DD','RSM_DD','TRD_RSMPT_DD','REOPEN_DD','재개일')
CODE_KEYS=('ISU_SRT_CD','ISU_CD2','isuCd2','종목코드')
SCHEMA='STOCKHUNTER_PIT_STAGE7_KRX_INDIVIDUAL_HALT_CANARY_V1'


def fail(s):raise RuntimeError('FAIL_CLOSED_STAGE7_'+str(s))
def sha(b):return hashlib.sha256(b).hexdigest()
def save(p,data):
    p=pathlib.Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    b=data.encode('utf8') if isinstance(data,str) else data
    tmp=p.with_name(p.name+'.part');tmp.write_bytes(b);tmp.replace(p)
def dump(p,x):save(p,json.dumps(x,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
def readjson(p):return json.loads(pathlib.Path(p).read_text(encoding='utf8'))
def readcsv(p):
    with pathlib.Path(p).open(encoding='utf-8-sig',newline='') as fd:return list(csv.DictReader(fd))
def outcsv(p,rows,fields):
    s=io.StringIO(newline='');w=csv.DictWriter(s,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows);save(p,s.getvalue())
def first(row,names):
    for n in names:
        if n in row and str(row[n]).strip():return str(row[n]).strip()
    return ''

def evidence(stage3,stage6):
    base3=pathlib.Path(stage3);base6=pathlib.Path(stage6)
    s3=readjson(base3/'stage3_event_crosscheck_summary.json')
    s6=readjson(base6/'stage6_historical_market_cap_audit_summary.json')
    if (s3.get('source_price_run_id')!=SOURCE_STAGE1_RUN or s3.get('source_price_head_sha')!=SOURCE_STAGE1_SHA or
        s3.get('official_new_listing_event_rows')!=EXPECTED_NEWLIST or
        s3.get('observed_codes')!=EXPECTED_CODES or s3.get('strategy_ready') is not False):fail('SOURCE_STAGE3_DRIFT')
    if (s6.get('source_stage1_run')!=SOURCE_STAGE1_RUN or s6.get('source_stage5_run')!=39383970402 or
        s6.get('source_stage4_run')!=39373355974 or s6.get('cap_rows')!=EXPECTED_CAP_ROWS or
        s6.get('zero_volume_proxy_rows')!=EXPECTED_ZERO_VOLUME or
        s6.get('distinct_codes')!=EXPECTED_CODES or s6.get('stage4_unresolved_type_codes')!=EXPECTED_UNRESOLVED_TYPES or
        s6.get('historical_market_cap_price_source_agrees') is not True or
        s6.get('strategy_ready') is not False):fail('SOURCE_STAGE6_DRIFT')
    profiles=readcsv(base6/'all_3341_market_cap_observation_profiles.csv')
    if len(profiles)!=EXPECTED_CODES:fail('CAP_PROFILES_WRONG_COUNT')
    by_code={}
    for row in profiles:
        code=row.get('code','')
        if not SHORT.fullmatch(code) or code in by_code:fail('CAP_DUPLICATE_OR_BAD_CODE')
        if row['original_historical_security_type_verified']!='False' or row['official_suspension_verified']!='False':
            fail('CAP_CLAIM_ESCALATION')
        v=int(row['zero_volume_proxy_sessions']);days=int(row['observed_cap_sessions'])
        if v<0 or v>days:fail('CAP_BAD_ZERO_PROXY')
        by_code[code]=row
    if sum(int(r['zero_volume_proxy_sessions']) for r in profiles)!=EXPECTED_ZERO_VOLUME:fail('ZERO_VOLUME_TOTAL_DRIFT')
    manifest=readjson(base3/'official_event_source_manifest.json')
    windows=manifest.get('windows',[])
    if len(windows)!=16:fail('STAGE3_WINDOWS_NOT_16')
    code_to_isins={};raw_rows=0;seen_wins=set()
    for entry in windows:
        key=(entry.get('endpoint'),entry.get('start'),entry.get('end'))
        if key in seen_wins:fail('DUPLICATE_EVENT_WINDOW')
        seen_wins.add(key)
        rel=entry['raw_file']
        if not rel.startswith('raw_krx/') or '..' in rel:fail('EVENT_PATH_TRAVERSAL')
        data=(base3/rel).read_bytes()
        if sha(data)!=entry['raw_sha256']:fail('STAGE3_RAW_SHA_MISMATCH:'+rel)
        j=json.loads(data);rows=j.get('output',j.get('OutBlock_1'))
        if not isinstance(rows,list) or len(rows)!=entry['rows']:fail('STAGE3_EVENT_COUNT_MISMATCH')
        if entry['endpoint']!='new_listing':continue
        raw_rows+=len(rows)
        for row in rows:
            code=str(row.get('ISU_CD','')).strip();full=str(row.get('ISU_CD_FULL','')).strip()
            if not (SHORT.fullmatch(code) and ISIN.fullmatch(full) and code in by_code):continue
            code_to_isins.setdefault(code,set()).add(full)
    if raw_rows!=EXPECTED_NEWLIST:fail('STAGE3_NEWLIST_DRIFT')
    usable={code:next(iter(ids)) for code,ids in code_to_isins.items() if len(ids)==1}
    ambiguous={code:sorted(ids) for code,ids in code_to_isins.items() if len(ids)>1}
    return by_code,usable,ambiguous


def choose(profiles,isins,n=12):
    """Non-random diagnostics only, NOT a population estimator."""
    if not 2<=n<=24:fail('BAD_CANARY_SIZE')
    eligible=[c for c in isins if c in profiles]
    if len(eligible)<n:fail('NOT_ENOUGH_PINNED_ISIN')
    z=lambda c:int(profiles[c]['zero_volume_proxy_sessions'])
    high=sorted(eligible,key=lambda c:(-z(c),c))
    low=sorted(eligible,key=lambda c:(z(c),c))
    selected=high[:n-min(3,n//2)]
    for c in low:
        if c not in selected:selected.append(c)
        if len(selected)==n:break
    assert len(selected)==n
    return [{'code':code,'official_listing_event_isin':isins[code],
             'zero_volume_proxy_sessions_all_history':z(code),
             'observed_cap_sessions_all_history':int(profiles[code]['observed_cap_sessions']),
             'selection':'NONRANDOM_ZERO_VOLUME_DIAGNOSTIC',
             'date_window_start':WINDOW[0],'date_window_end':WINDOW[1]} for code in selected]


def request_body(code,isin,start,end):
    if not SHORT.fullmatch(code) or not ISIN.fullmatch(isin):fail('INVALID_IDENTIFIERS')
    if not re.fullmatch(r'\d{8}',start) or not re.fullmatch(r'\d{8}',end):fail('INVALID_DATES')
    if not start<=end:fail('REVERSED_DATES')
    days=(datetime.strptime(end,'%Y%m%d')-datetime.strptime(start,'%Y%m%d')).days
    if days>729:fail('DATE_RANGE_OVER_730_DAYS')
    return {'bld':KRX_BLD,'locale':'ko_KR','isuCd':isin,'isuCd2':code,
            'strtDd':start,'endDd':end,'param1isuCd_finder_stkisu0_3':'ALL',
            'csvxls_isNo':'false'}


def get_source(kind,code,isin,start,end,auth):
    del kind
    r=auth.post(KRX_URL,data=request_body(code,isin,start,end),timeout=40)
    r.raise_for_status()
    if 'LOGOUT' in r.text[:2000].upper():fail('LOGIN_EXPIRED')
    try:return r.json()
    except ValueError:fail('KRX_NON_JSON')


def decode(raw):
    if not isinstance(raw,dict) or any(k in raw for k in ('_error_code','error')):fail('KRX_ERROR_RESPONSE')
    if 'output' in raw:rows=raw['output']
    elif 'OutBlock_1' in raw:rows=raw['OutBlock_1']
    else:fail('KRX_UNKNOWN_RESPONSE_KEYS')
    if not isinstance(rows,list) or not all(isinstance(r,dict) for r in rows):fail('KRX_NOT_ARRAY_OF_OBJECTS')
    return rows    # Empty is *not* evidence of zero halts; only indeterminate.


def normalize_date(s):
    txt=re.sub(r'[./\-\s]','',str(s))
    if not re.fullmatch(r'\d{8}',txt):return ''
    try:return datetime.strptime(txt,'%Y%m%d').strftime('%Y-%m-%d')
    except ValueError:return ''


def interpret_row(row,issue,start,end):
    """Never claim a halt unless date + issue ID can be recognized unequivocally."""
    date=normalize_date(first(row,DATE_KEYS))
    resume_raw=first(row,RESUME_KEYS)
    resume=normalize_date(resume_raw) if resume_raw else ''
    code=first(row,CODE_KEYS)
    # Some endpoint rows return short ticker in ISU_CD; full ISIN instead is permissible.
    if not code and SHORT.fullmatch(str(row.get('ISU_CD',''))):code=str(row['ISU_CD'])
    full=str(row.get('ISU_CD','')).strip()
    if code and code!=issue['code']:return {'status':'ROW_IDENTITY_CONFLICT','halt_date':date,'resume_date':resume}
    if full and ISIN.fullmatch(full) and full!=issue['official_listing_event_isin']:
        return {'status':'ROW_ISIN_CONFLICT','halt_date':date,'resume_date':resume}
    # Missing code means unable to tie provider record to an independently recorded short code.
    if not code or not date or not (start<=date.replace('-','')<=end):
        return {'status':'SCHEMA_OR_DATE_UNVERIFIED','halt_date':date,'resume_date':resume}
    if resume and resume<date:return {'status':'BAD_RESUME_BEFORE_START','halt_date':date,'resume_date':resume}
    return {'status':'SOURCE_EVENT_CODE_DATE_MATCH_ONLY','halt_date':date,'resume_date':resume}


def check_windows(windows):
    """Require bounded, non-overlapping, consecutive calendar coverage."""
    from datetime import timedelta
    if not isinstance(windows,(tuple,list)) or not windows:fail('WINDOWS_EMPTY')
    previous=None
    for start,end in windows:
        if not (re.fullmatch(r'[0-9]{8}',start) and re.fullmatch(r'[0-9]{8}',end)):
            fail('WINDOW_NOT_YYYYMMDD')
        a=datetime.strptime(start,'%Y%m%d').date()
        b=datetime.strptime(end,'%Y%m%d').date()
        if b<a or (b-a).days>729:fail('WINDOW_TOO_LONG_OR_REVERSED')
        if previous and a!=previous+timedelta(days=1):fail('WINDOW_GAP_OR_OVERLAP')
        previous=b
    return tuple(tuple(x) for x in windows)


def run(stage3,stage6,out,auth=None,n=12,pause=1.5,client=None,strict_source=True,windows=None):
    del strict_source
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    profiles,links,conflicts=evidence(stage3,stage6)
    sample=choose(profiles,links,n)
    outcsv(out/'all_isin_linkable_codes_from_listing_events.csv',
        [{'code':c,'listing_event_isin':full,'zero_volume_proxy_sessions':profiles[c]['zero_volume_proxy_sessions'],
          'linkage_is_retrospective':True,'all_dates_isin_verified':False} for c,full in sorted(links.items())],
        ['code','listing_event_isin','zero_volume_proxy_sessions','linkage_is_retrospective','all_dates_isin_verified'])
    dump(out/'conflicting_listing_event_isins.json',conflicts)
    outcsv(out/'selected_halt_source_canary.csv',sample,list(sample[0]))
    if client is None:
        if auth is None:
            from pykrx.website.comm.auth import get_auth_session
            auth=get_auth_session()
        if auth is None:fail('KRX_AUTH_UNAVAILABLE')
        client=lambda issue,start,end:get_source('trading_halt',issue['code'],issue['official_listing_event_isin'],start,end,auth)
    # Backward-compatible run() defaults for tests; production CLI uses full history.
    windows=check_windows((WINDOW,) if windows is None else windows)
    audit=[];raw_manifest=[];returned=0;verified=0;unmapped=0;empty=0
    codes_with_any_provider_rows=set()
    per_window=[]
    for i,issue in enumerate(sample):
        for w,(start,end) in enumerate(windows):
            obj=client(issue,start,end)
            filename='raw_krx/issue_'+issue['code']+'_'+start+'_'+end+'.json'
            raw=(json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
            save(out/filename,raw) # preserve source BEFORE decoding or inferring anything
            raw_manifest.append({'code':issue['code'],'source_listing_isin':issue['official_listing_event_isin'],
                                 'raw_file':filename,'sha256':sha(raw),'start':start,'end':end})
            dump(out/'official_halt_raw_manifest.json',{'status':'PARTIAL','files':raw_manifest})
            rows=decode(obj)
            returned+=len(rows);empty+=int(not rows)
            if rows:codes_with_any_provider_rows.add(issue['code'])
            per_window.append({'code':issue['code'],'start':start,'end':end,'rows':len(rows),
                               'raw_file':filename,'raw_sha256':sha(raw)})
            for j,row in enumerate(rows):
                details=interpret_row(row,issue,start,end)
                status=details['status']
                verified+=int(status=='SOURCE_EVENT_CODE_DATE_MATCH_ONLY')
                unmapped+=int(status!='SOURCE_EVENT_CODE_DATE_MATCH_ONLY')
                audit.append({'code':issue['code'],'official_listing_event_isin':issue['official_listing_event_isin'],
                              'row_number':j+1,'raw_file':filename,'field_names':';'.join(sorted(row)),**details})
            print('STAGE7_PROVIDER_WINDOW',i+1,len(sample),issue['code'],
                  start,end,'provider_rows',len(rows),flush=True)
            if pause:time.sleep(pause)
    outcsv(out/'halt_historical_window_diagnostics.csv',per_window,
           ['code','start','end','rows','raw_file','raw_sha256'])
    outcsv(out/'halt_provider_rows_schema_audit.csv',audit,
        ['code','official_listing_event_isin','row_number','raw_file','field_names','status','halt_date','resume_date'])
    report={'schema':SCHEMA,'status':'PROVIDER_HALT_CANARY_HAS_MAPPED_EVENTS' if verified else 'PROVIDER_HALT_CANARY_NOT_YET_VALIDATED',
            'research_only':True,'strategy_ready':False,'source_stage3_run':SOURCE_STAGE3_RUN,
            'source_stage6_run':SOURCE_STAGE6_RUN,'source_stage1_run':SOURCE_STAGE1_RUN,
            'observed_codes':EXPECTED_CODES,'historical_zero_volume_proxy_rows':EXPECTED_ZERO_VOLUME,
            'stage4_unresolved_security_type_codes':EXPECTED_UNRESOLVED_TYPES,
            'codes_with_retrospective_listing_event_isin':len(links),'conflicting_isin_codes':len(conflicts),
            'sampled_codes':len(sample),'sample_method':'NONRANDOM_DIAGNOSTIC_ONLY',
            'queried_window':list(WINDOW) if len(windows)==1 else None,
            'queried_windows':[list(x) for x in windows],
            'provider_code_window_queries':len(per_window),
            'provider_rows_returned':returned,'provider_empty_response_queries':empty,
            'provider_empty_response_codes':len(sample)-len(codes_with_any_provider_rows),
            'provider_unmapped_or_conflicting_rows':unmapped,'provider_mapped_halt_event_rows':verified,
            'historical_halt_all_codes_verified':False,'zero_volume_halt_day_join_verified':False,
            'date_scoped_security_type_verified':False,'all_dates_issue_isin_verified':False,
            'official_halt_coverage_claim':False,'generated_at_utc':datetime.now(timezone.utc).isoformat(),
            'critical_note':'20011 is individual-code halt event history, not full daily suspension status. No rows cannot prove unrestricted trading. Stop/resume edges require manual/independent schema verification. Snapshot values do not prove execution possibility.'}
    dump(out/'stage7_official_halt_canary_summary.json',report)
    dump(out/'official_halt_raw_manifest.json',{'status':'COMPLETE','files':raw_manifest})
    print(json.dumps(report,ensure_ascii=False,sort_keys=True),flush=True)
    if not verified:fail('NO_MAPPED_POSITIVE_HALT_CANARY_ROWS_SOURCE_UNVALIDATED')
    if unmapped:fail('UNMAPPED_OR_CONFLICTING_HALT_ROWS_REQUIRE_REVIEW')
    return report


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage3-dir',default='stage3-source');p.add_argument('--stage6-dir',default='stage6-source')
    p.add_argument('--out',default='reports/pit-stage7');p.add_argument('--sample-size',type=int,default=12)
    p.add_argument('--pause',type=float,default=1.5)
    p.add_argument('--window-mode',choices=['full_history','old_2025_2026'],
                   default='full_history',help='Default covers 2019-2026 in 4 disjoint <=730-day probes')
    a=p.parse_args(argv)
    windows=HISTORICAL_WINDOWS if a.window_mode=='full_history' else (WINDOW,)
    run(a.stage3_dir,a.stage6_dir,a.out,n=a.sample_size,pause=a.pause,windows=windows)

if __name__=='__main__':
    try:main()
    except Exception as exc:
        print('FAIL_CLOSED_STAGE7',type(exc).__name__,str(exc),file=sys.stderr,flush=True)
        sys.exit(2)
