#!/usr/bin/env python3
"""STOCKHUNTER research-only KRX official listing/delisting EVENT register crosscheck.

Not a full PIT security master. No source download, trading, ranking or LIVE changes.
Uses pinned Stage2 observed issue-code lifetimes; fetches KRX event registries, preserves
raw responses + SHA, and classifies *match evidence*, not confirmed corporate actions.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import os
import pathlib
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone

SCHEMA = 'STOCKHUNTER_KRX_OFFICIAL_EVENT_AUDIT_V1'
EXPECTED_STAGE2_RUN = 39343301692
EXPECTED_SOURCE_RUN = 39295295197
EXPECTED_SOURCE_SHA = '08e328c101f0774a2236c0a44cc1fdbbaa59e96e'
EXPECTED_CODES = 3341
EXPECTED_TERMINAL = 472
EXPECTED_PRICE_ROWS = 5090428
CUTOFF = '2026-10-12'
START = '2019-01-02'
OFFICIAL_URL = 'https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
BLD = {'delisted':'dbms/MDC/STAT/issue/MDCSTAT23801',
       'new_listing':'dbms/MDC/STAT/issue/MDCSTAT20001'}
CODE_PATTERN = re.compile(r'[0-9A-Z]{6}\Z')
DATE_FIELDS = {'delisted': ('DELIST_DD','DELIST_DATE','폐지일'),
               'new_listing': ('LIST_DD','LIST_DATE','상장일')}
CODE_FIELDS = ('ISU_SRT_CD','ISU_CD','isuSrtCd','종목코드')
ISSUER_FIELDS = ('ISUR_CD','isurCd')  # issuer identity is NOT a tradable issue-code
NAME_FIELDS = ('ISU_NM','ISU_ABBRV','ISUR_NM','종목명')
MARKET_FIELDS = ('MKT_NM','MKT_TP_NM','시장구분')
TYPE_FIELDS = ('SECU_GRP_NM','SECUGRP_NM','KIND_STKCERT_TP_NM','주식종류','증권구분')
REASON_FIELDS = ('DELIST_RSN_DSC','DELIST_RSN','폐지사유')


def fail(msg):
    raise RuntimeError('FAIL_CLOSED_'+str(msg))


def sha(obj: bytes):
    return hashlib.sha256(obj).hexdigest()


def put(path, data):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    payload = data if isinstance(data,bytes) else data.encode('utf-8')
    tmp=path.with_name(path.name+'.part')
    tmp.write_bytes(payload)
    tmp.replace(path)


def put_json(path, obj):
    put(path,json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True)+'\n')


def write_csv(path, rows, columns):
    import io
    out=io.StringIO(newline='')
    wr=csv.DictWriter(out,fieldnames=columns,extrasaction='ignore')
    wr.writeheader()
    for r in rows:wr.writerow(r)
    put(path,out.getvalue())


def first(row,keys):
    for k in keys:
        x=row.get(k,'')
        if x is not None and str(x).strip():return str(x).strip()
    return ''


def norm_date(s):
    digits=re.sub(r'[-./\s]','',str(s).strip())
    if not re.fullmatch(r'[0-9]{8}',digits):fail('BAD_EVENT_DATE:'+str(s)[:45])
    try:
        return datetime.strptime(digits,'%Y%m%d').date().isoformat()
    except ValueError:fail('BAD_CALENDAR_DATE:'+str(s)[:45])


def norm_code(s):
    v=str(s).strip()
    # An ISIN is NOT a short ticker and must never be shortened heuristically.
    if CODE_PATTERN.fullmatch(v):return v
    return ''


def check_stage2(stage2_dir):
    base=pathlib.Path(stage2_dir)
    summary=json.loads((base/'lifecycle_summary.json').read_text(encoding='utf-8'))
    price=json.loads((base/'source_fullmarket_price_verification.json').read_text(encoding='utf-8'))
    if (summary.get('source_run_id'),summary.get('source_head_sha'),summary.get('observed_codes'),
        summary.get('codes_last_observed_before_final'),summary.get('price_rows')) != (
        EXPECTED_SOURCE_RUN,EXPECTED_SOURCE_SHA,EXPECTED_CODES,EXPECTED_TERMINAL,EXPECTED_PRICE_ROWS):
        fail('UNPINNED_STAGE2_SUMMARY')
    if summary.get('status')!='PRICE_OBSERVATIONS_ONLY_SECURITY_MASTER_UNVERIFIED' or summary.get('strategy_ready') is not False:
        fail('BAD_STAGE2_STATUS')
    if (price.get('full_market_price_rows'),price.get('sessions'),price.get('whole_market_pit_strategy_ready')) != (EXPECTED_PRICE_ROWS,1907,False):
        fail('BAD_STAGE1_EVIDENCE')
    observed={}
    with (base/'observed_ticker_lifecycles.csv').open(encoding='utf-8-sig',newline='') as fd:
        for row in csv.DictReader(fd):
            code=row['code'].strip()
            if not CODE_PATTERN.fullmatch(code) or code in observed:fail('BAD_OBSERVED_CODE:'+code)
            if not (START<=row['first_seen']<=row['last_seen']<=CUTOFF):fail('BAD_OBSERVED_LIFETIME:'+code)
            observed[code]=row
    if len(observed)!=EXPECTED_CODES:fail('OBSERVED_CODE_COUNT:'+str(len(observed)))
    if sum(row['last_observation_before_end']=='True' for row in observed.values()) != EXPECTED_TERMINAL:
        fail('TERMINAL_COUNT_MISMATCH')
    return observed


def yearly_windows(start=START,end=CUTOFF):
    a=date.fromisoformat(start);b=date.fromisoformat(end)
    if b<a:fail('BAD_WINDOW')
    return [(max(a,date(y,1,1)).strftime('%Y%m%d'), min(b,date(y,12,31)).strftime('%Y%m%d')) for y in range(a.year,b.year+1)]


def decode_json(raw,kind,start,end):
    if not isinstance(raw,dict) or any(x in raw for x in ('_error_code','error')):
        fail('KRX_ERROR_RESPONSE:'+kind+':'+start)
    if raw.get('output') is not None:
        recs=raw['output']
    elif raw.get('OutBlock_1') is not None:
        recs=raw['OutBlock_1']
    else:fail('KRX_UNKNOWN_RESPONSE_KEYS:'+kind+':'+start)
    if not isinstance(recs,list) or not recs:
        fail('KRX_EMPTY_OR_NOT_LIST:'+kind+':'+start)  # fail closed; do not represent unavailable as no events
    if not all(isinstance(x,dict) for x in recs):fail('KRX_BAD_ROW:'+kind)
    return recs


def normalize_events(raw_rows,kind,start,end):
    output=[]
    for ix,row in enumerate(raw_rows):
        d=norm_date(first(row,DATE_FIELDS[kind]))
        if not start<=d.replace('-','')<=end:
            fail('EVENT_OUTSIDE_WINDOW:'+kind+':'+d)
        # Do not merge ISINs with 6-digit codes or perform ticker name matching.
        raw_code=first(row,CODE_FIELDS)
        raw_issuer=first(row,ISSUER_FIELDS)
        candidates={norm_code(row.get(field,'')) for field in CODE_FIELDS} - {''}
        if len(candidates)>1:fail('CONFLICTING_OFFICIAL_SHORT_CODES:'+kind+':'+d)
        code=next(iter(candidates),'')
        # Issuer identity is NEVER equated to security issue-code; unresolved is evidence.
        # If ISU_CD is an ISIN, preserve but do not truncate or guess.
        if not code and not raw_code and not raw_issuer:fail('NO_KRX_EVENT_IDENTITY:'+kind+':'+d)
        output.append({'kind':kind,'date':d,'code':code,'raw_issue_id':raw_code,
                       'raw_issuer_id':raw_issuer,
                       'name':first(row,NAME_FIELDS),'market':first(row,MARKET_FIELDS),
                       'security_class':first(row,TYPE_FIELDS),
                       'reason':first(row,REASON_FIELDS),
                       'identity_matchable':bool(code),'source_row_number':ix+1,
                       'response_window':start+'_'+end})
    return output


def build_krx_request(kind, start, end):
    """Screen-specific form data. Do not combine issuer and issue filters.

    Only the authenticated, already-pinned pykrx requests session is used;
    public endpoint schemas guide request construction, not auth libraries.
    """
    if kind not in BLD:
        fail('UNKNOWN_KRX_REGISTRY:'+str(kind))
    if not (re.fullmatch(r'[0-9]{8}',start) and re.fullmatch(r'[0-9]{8}',end)
            and start <= end):
        fail('INVALID_KRX_REGISTRY_WINDOW:'+str(kind))
    common={'bld':BLD[kind], 'locale':'ko_KR', 'mktId':'ALL',
            'strtDd':start,'endDd':end}
    if kind=='delisted':
        # Preserve the exact request that fetched 1,168 dated official rows.
        return {**common,'isuCd':'ALL','isuCd2':'ALL',
                'share':'1','csvxls_isNo':'true'}
    # This is the KRX 20001 JSON screen's issuer-name contract.
    # It must not silently fall back to the unrelated delisting form.
    return {**common,'isurCd':'ALL','isurCd2':'ALL',
            'listClssCd':'ALL','secugrpTp':'ALL','cntrIsoCd':'ALL'}


def fetch_krx(kind,start,end,auth=None):
    if auth is None:
        # Reuse already-vetted pykrx's authenticated KRX session (NO new dependency with credentials).
        from pykrx.website.comm.auth import get_auth_session
        auth=get_auth_session()
    if auth is None:fail('NO_AUTHENTICATED_KRX_SESSION')
    # KRX's issue screens do NOT share the same identity/search fields.
    # MDCSTAT23801 uses isuCd/isuCd2 (securities), while MDCSTAT20001
    # uses isurCd/isurCd2 (issuers) plus listing-class/security filters.
    # Sending isuCd=ALL to the new-listing JSON endpoint returned a valid
    # HTTP 200 with output=[] for 2019, which is missing evidence, NOT zero IPOs.
    body = build_krx_request(kind, start, end)
    r=auth.post(OFFICIAL_URL,data=body,timeout=40)
    r.raise_for_status()
    if 'LOGOUT' in r.text[:2000].upper():fail('KRX_LOGIN_EXPIRED:'+kind+':'+start)
    try:return r.json()
    except ValueError:fail('KRX_NON_JSON:'+kind+':'+start)


def fetch_all(out,client=fetch_krx,pause=1.5):
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    events=[]; manifests=[]
    for kind in ('new_listing','delisted'):
        for start,end in yearly_windows():
            obj=client(kind,start,end)
            # Write raw evidence BEFORE normalization: failures must remain inspectable.
            raw=(json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode('utf-8')
            rel='raw_krx/'+kind+'_'+start+'_'+end+'.json'
            put(out/rel,raw)
            records=decode_json(obj,kind,start,end)
            events.extend(normalize_events(records,kind,start,end))
            manifests.append({'endpoint':kind,'start':start,'end':end,'rows':len(records),
                              'raw_file':rel,'raw_sha256':sha(raw),'KRX_bld':BLD[kind]})
            put_json(out/'official_event_source_manifest.json',{'schema':SCHEMA,'status':'PARTIAL_UNTIL_ALL_WINDOWS_COMPLETE',
                         'collected_at_utc':datetime.now(timezone.utc).isoformat(),
                         'source':'KRX data.krx.co.kr getJsonData.cmd','windows':manifests})
            print('KRX_OFFICIAL_EVENT_WINDOW '+kind+' '+start+'_'+end+' rows='+str(len(records)),flush=True)
            if pause:time.sleep(pause)
    put_json(out/'official_event_source_manifest.json',{'schema':SCHEMA,'collected_at_utc':datetime.now(timezone.utc).isoformat(),
                                                        'source':'KRX data.krx.co.kr getJsonData.cmd','windows':manifests})
    return events,manifests


def compare(observed,events,out):
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    codes_seen={c for c,row in observed.items() if row['last_observation_before_end']=='True'}
    by_code={}
    bad_ids=[]
    for event in events:
        c=event['code']
        if c:by_code.setdefault(c,[]).append(event)
        else:bad_ids.append(event)
    rows=[]
    for code in sorted(observed):
        rec=observed[code]
        observed_last=rec['last_seen']
        delists=sorted((e for e in by_code.get(code,[]) if e['kind']=='delisted'),key=lambda e:e['date'])
        newlist=sorted((e for e in by_code.get(code,[]) if e['kind']=='new_listing'),key=lambda e:e['date'])
        valid_delists=[e for e in delists if observed_last<=e['date']<=CUTOFF]
        earlier_delists=[e for e in delists if e['date']<observed_last]
        if len(valid_delists)==1 and not earlier_delists:
            match='KRX_DELIST_EVENT_CODE_DATE_MATCH_ISIN_UNVERIFIED'
            ev=valid_delists[0]
        elif len(valid_delists)>1 or earlier_delists:
            match='CONFLICTING_MULTIPLE_OR_BEFORE_LAST_SEEN_EVENT'
            ev=valid_delists[0] if valid_delists else earlier_delists[-1]
        else:
            match='NO_MATCH_IN_2019_2026_KRX_DELIST_EVENT_REGISTER'
            ev=None
        rows.append({'code':code,'first_seen':rec['first_seen'],'last_seen':observed_last,
                     'terminal_before_cutoff':rec['last_observation_before_end'],
                     'event_match_status':match,'krx_delist_event_date':ev['date'] if ev else '',
                     'krx_delist_market':ev['market'] if ev else '',
                     'krx_delist_security_class':ev['security_class'] if ev else '',
                     'krx_delist_reason':ev['reason'] if ev else '',
                     'krx_list_event_count':len(newlist),
                     'krx_delist_event_count':len(delists),
                     'pit_ordinary_share_verified':False,
                     'pit_listing_status_verified':False})
    terminal=[r for r in rows if r['code'] in codes_seen]
    fields=list(rows[0])
    write_csv(out/'all_3341_issue_code_event_crosscheck.csv',rows,fields)
    write_csv(out/'priority_472_terminal_issue_codes.csv',terminal,fields)
    event_fields=list(events[0]) if events else []
    if not event_fields:fail('NO_OFFICIAL_EVENTS')
    write_csv(out/'all_official_event_rows_normalized.csv',events,event_fields)
    dup_event_keys={}
    for e in events:
        key=(e['kind'],e['date'],e['raw_issue_id'],e['market'],e['name'])
        dup_event_keys[key]=dup_event_keys.get(key,0)+1
    duplicates=sum(n-1 for n in dup_event_keys.values() if n>1)
    report={'status':'OFFICIAL_KRX_EVENT_CROSSCHECK_ONLY_NOT_FULL_PIT_MASTER',
            'research_only':True,'strategy_ready':False,'source_stage2_run_id':EXPECTED_STAGE2_RUN,
            'source_price_run_id':EXPECTED_SOURCE_RUN,'source_price_head_sha':EXPECTED_SOURCE_SHA,
            'cutoff':CUTOFF,'observed_codes':len(rows),'terminal_before_cutoff':len(terminal),
            'official_delisting_event_rows':sum(e['kind']=='delisted' for e in events),
            'official_new_listing_event_rows':sum(e['kind']=='new_listing' for e in events),
            'official_event_identity_unmatchable_rows':len(bad_ids),
            'exact_event_duplicate_records':duplicates,
            'terminal_code_date_event_matches':sum(r['event_match_status']=='KRX_DELIST_EVENT_CODE_DATE_MATCH_ISIN_UNVERIFIED' for r in terminal),
            'terminal_code_conflicts':sum(r['event_match_status']=='CONFLICTING_MULTIPLE_OR_BEFORE_LAST_SEEN_EVENT' for r in terminal),
            'terminal_without_matching_2019_2026_delist_event':sum(r['event_match_status']=='NO_MATCH_IN_2019_2026_KRX_DELIST_EVENT_REGISTER' for r in terminal),
            'date_scoped_security_master_verified':False,'full_daily_membership_verified':False,
            'official_halt_history_verified':False,'historical_security_type_verified':False,
            'isin_issue_identity_verified':False,
            'caveat':'Exchange event register is event evidence, NOT an all-date security master. Neither terminal absence nor unmatched event proves anything about delisting; short code match does not prove ISIN identity. Prior-to-2019 listing events are outside event range.',
            'generated_at_utc':datetime.now(timezone.utc).isoformat()}
    put_json(out/'stage3_event_crosscheck_summary.json',report)
    print(json.dumps(report,ensure_ascii=False,sort_keys=True),flush=True)
    return report


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage2-dir',default='stage2-source')
    p.add_argument('--out',default='reports/pit-stage3')
    p.add_argument('--pause',type=float,default=1.5)
    p.add_argument('--events-json',default=None,help='OFFLINE ONLY: fixture with delisted/new_listing yearly windows; no network')
    args=p.parse_args(argv)
    observed=check_stage2(args.stage2_dir)
    if args.events_json:
        data=json.loads(pathlib.Path(args.events_json).read_text(encoding='utf-8'))
        def offline(kind,start,end):
            return data[kind][start+'_'+end]
        events,_=fetch_all(args.out,client=offline,pause=0)
    else:events,_=fetch_all(args.out,pause=args.pause)
    compare(observed,events,args.out)

if __name__=='__main__':
    try:main()
    except Exception as e:
        print('FAIL_CLOSED_STAGE3',type(e).__name__,str(e),file=sys.stderr,flush=True)
        raise SystemExit(2)
