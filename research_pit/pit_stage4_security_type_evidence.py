#!/usr/bin/env python3
"""RESEARCH ONLY. Source-locked retrospective KRX event *security-type evidence* audit.

NEVER asserts a PIT daily security master, listing permission, SPAC legal status,
trading suspension, historical market cap, or strategy backtest readiness.
Never mutates production scanners, rankings or orders.
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
from collections import Counter, defaultdict
from datetime import datetime, timezone

SOURCE_RUN=39295295197
SOURCE_SHA='08e328c101f0774a2236c0a44cc1fdbbaa59e96e'
STAGE2_RUN=39343301692
STAGE3_RUN=39364301572
PRICE_ROWS=5090428
EXPECTED_CODES=3341
EXPECTED_TERMINAL=472
EXPECTED_DELIST=1168
EXPECTED_NEWLIST=1015
EXPECTED_UNMATCHABLE=695
START='2019-01-02'
END='2026-10-12'
CODE_RE=re.compile(r'[0-9A-Z]{6}\Z')
MANIFEST_FORMAT='STOCKHUNTER_KRX_OFFICIAL_EVENT_AUDIT_V1'


def stop(msg):
    raise RuntimeError('STAGE4_FAIL_CLOSED_'+str(msg))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def readjson(path):
    return json.loads(pathlib.Path(path).read_text(encoding='utf-8'))


def readcsv(path):
    with pathlib.Path(path).open(newline='', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def dumpjson(path,value):
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2,ensure_ascii=False,sort_keys=True)+'\n',encoding='utf-8')


def dumpcsv(path,rows,columns):
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=columns,extrasaction='ignore');w.writeheader();w.writerows(rows)


def is_six_code(value):
    value=str(value or '').strip()
    return value if CODE_RE.fullmatch(value) else ''


def key_date(raw):
    text=str(raw or '').strip().replace('/','-').replace('.','-')
    if re.fullmatch(r'\d{8}',text):text=f'{text[:4]}-{text[4:6]}-{text[6:]}'
    try: out=datetime.strptime(text,'%Y-%m-%d').date().isoformat()
    except ValueError:stop('BAD_EVENT_DATE_'+text[:40])
    return out


def preflight(s2,s3):
    s2=pathlib.Path(s2);s3=pathlib.Path(s3)
    a=readjson(s2/'lifecycle_summary.json')
    b=readjson(s2/'source_fullmarket_price_verification.json')
    c=readjson(s3/'stage3_event_crosscheck_summary.json')
    if any([a.get('source_run_id')!=SOURCE_RUN,a.get('source_head_sha')!=SOURCE_SHA,
            a.get('observed_codes')!=EXPECTED_CODES,a.get('codes_last_observed_before_final')!=EXPECTED_TERMINAL,
            a.get('price_rows')!=PRICE_ROWS,a.get('strategy_ready') is not False,
            b.get('full_market_price_rows')!=PRICE_ROWS,b.get('sessions')!=1907,
            b.get('whole_market_pit_strategy_ready') is not False]):
        stop('STAGE2_PIN_OR_COVERAGE_MISMATCH')
    if any([c.get('source_stage2_run_id')!=STAGE2_RUN,c.get('source_price_run_id')!=SOURCE_RUN,
            c.get('source_price_head_sha')!=SOURCE_SHA,c.get('strategy_ready') is not False,
            c.get('observed_codes')!=EXPECTED_CODES,c.get('terminal_before_cutoff')!=EXPECTED_TERMINAL,
            c.get('official_delisting_event_rows')!=EXPECTED_DELIST,
            c.get('official_new_listing_event_rows')!=EXPECTED_NEWLIST,
            c.get('official_event_identity_unmatchable_rows')!=EXPECTED_UNMATCHABLE,
            c.get('terminal_code_date_event_matches')!=EXPECTED_TERMINAL,
            c.get('terminal_without_matching_2019_2026_delist_event')!=0]):
        stop('STAGE3_PIN_OR_EVENT_SUMMARY_MISMATCH')
    observed=readcsv(s2/'observed_ticker_lifecycles.csv')
    codes={}
    for r in observed:
        code=r['code']
        if not is_six_code(code) or code in codes or not START<=r['first_seen']<=r['last_seen']<=END:
            stop('BAD_OBSERVATION_OR_DUPLICATE_'+code)
        codes[code]=r
    if len(codes)!=EXPECTED_CODES or sum(r['last_observation_before_end']=='True' for r in codes.values())!=EXPECTED_TERMINAL:
        stop('OBSERVATION_COUNTERS_CHANGED')
    comparisons=readcsv(s3/'all_3341_issue_code_event_crosscheck.csv')
    cp={r['code']:r for r in comparisons}
    if len(comparisons)!=EXPECTED_CODES or set(cp)!=set(codes) or any(cp[k]['first_seen']!=r['first_seen'] or cp[k]['last_seen']!=r['last_seen'] for k,r in codes.items()):
        stop('STAGE2_STAGE3_IDENTITY_DRIFT')
    terminal=readcsv(s3/'priority_472_terminal_issue_codes.csv')
    if len(terminal)!=EXPECTED_TERMINAL or {r['code'] for r in terminal}!={k for k,r in codes.items() if r['last_observation_before_end']=='True'}:
        stop('STAGE3_TERMINAL_SET_DRIFT')
    return codes,cp,c


def official_raw_events(s3,summary):
    s3=pathlib.Path(s3)
    manifest=readjson(s3/'official_event_source_manifest.json')
    windows=manifest.get('windows')
    if manifest.get('schema')!=MANIFEST_FORMAT or not isinstance(windows,list) or len(windows)!=16:
        stop('SOURCE_MANIFEST_SCHEMA_OR_COVERAGE')
    observed_windows=set();events=[]
    for e in windows:
        kind=e.get('endpoint');start=e.get('start');end=e.get('end')
        if kind not in ('new_listing','delisted') or not re.fullmatch(r'\d{8}',str(start)) or not re.fullmatch(r'\d{8}',str(end)):
            stop('BAD_SOURCE_MANIFEST_WINDOW')
        key=(kind,start,end)
        if key in observed_windows:stop('DUPLICATE_SOURCE_WINDOW')
        observed_windows.add(key)
        expected_path=f'raw_krx/{kind}_{start}_{end}.json'
        if e.get('raw_file')!=expected_path:stop('SOURCE_MANIFEST_FILE_DRIFT')
        file=s3/expected_path
        if not file.exists() or sha(file.read_bytes())!=e.get('raw_sha256'):stop('RAW_SHA_MISMATCH_'+expected_path)
        source=readjson(file)
        raw=source.get('output') if isinstance(source,dict) else None
        if not isinstance(raw,list) or not raw or e.get('rows')!=len(raw):
            stop('RAW_RESPONSE_MISSING_'+expected_path)
        for n,r in enumerate(raw,1):
            if not isinstance(r,dict):stop('RAW_NON_OBJECT_'+expected_path)
            dt=key_date(r.get('DELIST_DD' if kind=='delisted' else 'LIST_DD',''))
            if not start<=dt.replace('-','')<=end:stop('EVENT_OUTSIDE_PINNED_WINDOW')
            candidates={is_six_code(r.get(t,'')) for t in ('ISU_SRT_CD','ISU_CD','isuSrtCd')}-{''}
            if len(candidates)>1:stop('CONFLICTING_SHORT_CODES_IN_RAW')
            code=next(iter(candidates),'')
            events.append({'kind':kind,'code':code,'date':dt,'group':str(r.get('SECUGRP_NM') or r.get('SECU_GRP_NM') or '').strip(),
                           'share_kind':str(r.get('KIND_STKCERT_TP_NM') or '').strip(),
                           'name':str(r.get('ISU_NM') or '').strip(),
                           'market':str(r.get('MKT_NM') or '').strip(),
                           'listing_class':str(r.get('LIST_CLSS_NM') or '').strip(),
                           'full_id':str(r.get('ISU_CD_FULL') or '').strip(),
                           'source_window':f'{start}_{end}','source_row':n,
                           'official_source':'KRX_20001_NEW_LIST' if kind=='new_listing' else 'KRX_20037_DELIST'})
    per_kind=Counter(e['kind'] for e in events)
    if per_kind != {'new_listing':EXPECTED_NEWLIST,'delisted':EXPECTED_DELIST}:
        stop('TOTAL_EVENT_COUNT_DRIFT_'+str(dict(per_kind)))
    if sum(not e['code'] for e in events)!=EXPECTED_UNMATCHABLE:
        stop('UNMATCHABLE_TOTAL_CHANGED')
    historical=readcsv(s3/'all_official_event_rows_normalized.csv')
    if len(historical)!=len(events):stop('STAGE3_NORMALIZED_EVENTS_DRIFT')
    hist_by_key={}
    for row in historical:
        k=(row['kind'],row['response_window'],int(row['source_row_number']))
        if k in hist_by_key:stop('STAGE3_DUPLICATED_NORMALIZED_EVENT')
        hist_by_key[k]=row
    for ev in events:
        key=(ev['kind'],ev['source_window'],ev['source_row'])
        hist=hist_by_key.get(key)
        if not hist or hist['code']!=ev['code'] or hist['date']!=ev['date'] or hist['security_class']!=ev['group']:
            stop('NORMALIZED_VS_OFFICIAL_RAW_CONFLICT_'+str(key))
    return events


def class_of_event(e):
    """Evidence label only; not a date-scoped PIT tradability/ordinary-share decision."""
    group=e['group'];kind=e['share_kind'];name=e['name']
    if not group: return 'UNRESOLVED_MISSING_GROUP'
    if group!='주권': return 'OFFICIAL_EVENT_NON_STOCK_OR_SPECIAL_SECURITY'
    if kind=='보통주':
        if '스팩' in name or '기업인수목적' in name or re.search(r'\bSPAC\b',name,re.I):
            return 'SPAC_NAME_REVIEW_NOT_OFFICIAL_SPAC_CLASS'
        return 'RETROSPECTIVE_EVENT_COMMON_SHARE_CANDIDATE'
    if kind in ('구형우선주','신형우선주','우선주','종류주권'):
        return 'RETROSPECTIVE_EVENT_PREFERRED_OR_CLASS_SHARE'
    return 'UNRESOLVED_UNKNOWN_STOCK_KIND'


def audit(s2,s3,out):
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    observed,oldcross,s3summary=preflight(s2,s3)
    events=official_raw_events(s3,s3summary)
    per_code=defaultdict(list)
    for e in events:
        if e['code'] in observed:
            per_code[e['code']].append(e)
    rows=[];event_rows=[]
    for e in events:
        event_rows.append({**e,'event_evidence_type':class_of_event(e),'matches_source_observed_code':e['code'] in observed})
    for code,r in sorted(observed.items()):
        e=per_code[code]
        lab=[class_of_event(x) for x in e]
        determined={x for x in lab if x not in ('UNRESOLVED_MISSING_GROUP','UNRESOLVED_UNKNOWN_STOCK_KIND')}
        if not e:decision='UNRESOLVED_NO_2019_2026_EVENT'
        elif len(determined)>1:decision='CONFLICTING_EVENT_SECURITY_TYPES_REVIEW'
        elif not determined:decision='UNRESOLVED_EVENT_WITHOUT_USABLE_TYPE'
        else:decision=next(iter(determined))
        # Prevent a misleading universal equity pass from retrospective event labels.
        # Even an event with a common-share label cannot establish the type on each historic date.
        if decision=='RETROSPECTIVE_EVENT_COMMON_SHARE_CANDIDATE':
            gate='HISTORICAL_PIT_COMMON_SHARE_UNVERIFIED'
        elif decision.startswith('UNRESOLVED_') or decision.startswith('CONFLICTING_'):
            gate='REQUIRE_HISTORICAL_TYPE_EVIDENCE'
        else:gate='REQUIRES_FURTHER_REVIEW_OR_NON_TARGET'
        suspicious=[x for x in e if x['kind']=='new_listing' and x['date']>r['last_seen']]
        rows.append({'code':code,'first_seen':r['first_seen'],'last_seen':r['last_seen'],
                     'last_observation_before_end':r['last_observation_before_end'],
                     'observed_sessions':r.get('seen_sessions',''),
                     'zero_or_nontrade_proxy_sessions':r.get('zero_or_nontradeable_proxy_sessions',''),
                     'event_count':len(e),'newlisting_event_count':sum(x['kind']=='new_listing' for x in e),
                     'delisting_event_count':sum(x['kind']=='delisted' for x in e),
                     'type_evidence_label':decision,'historical_universe_gate':gate,
                     'event_type_details':' | '.join(sorted({x['group']+' / '+x['share_kind'] for x in e})),
                     'event_names':' | '.join(sorted({x['name'] for x in e if x['name']})),
                     'event_markets':' | '.join(sorted({x['market'] for x in e if x['market']})),
                     'listing_event_after_last_price_observation':bool(suspicious),
                     'event_group_classification_provenance':'KRX_20001_20037_EVENT_ONLY',
                     'historical_pit_security_type_verified':False,
                     'historical_tradability_verified':False,
                     'historical_market_cap_verified':False,
                     'strategy_ready':False})
    result={
      'status':'RETROSPECTIVE_EVENT_TYPE_EVIDENCE_ONLY_NO_PIT_APPROVAL',
      'research_only':True,'strategy_ready':False,'production_or_rank_changes':False,
      'date_scoped_security_master_verified':False,
      'official_daily_suspensions_verified':False,
      'historical_pit_security_type_verified':False,
      'historical_market_cap_verified':False,
      'isin_identity_linkage_verified':False,
      'spac_legal_status_verified':False,
      'source_price_run':SOURCE_RUN,'source_price_sha':SOURCE_SHA,
      'source_stage2_run':STAGE2_RUN,'source_stage3_run':STAGE3_RUN,
      'observed_codes':len(rows),
      'terminal_codes':sum(r['last_observation_before_end']=='True' for r in rows),
      'event_rows':len(events),
      'event_rows_without_short_code':sum(not e['code'] for e in events),
      'codes_without_usable_event_type':sum(r['type_evidence_label'].startswith('UNRESOLVED_') for r in rows),
      'codes_with_conflicting_event_types':sum(r['type_evidence_label'].startswith('CONFLICTING_') for r in rows),
      'type_evidence_label_counts':dict(sorted(Counter(r['type_evidence_label'] for r in rows).items())),
      'terminal_type_evidence_label_counts':dict(sorted(Counter(r['type_evidence_label'] for r in rows if r['last_observation_before_end']=='True').items())),
      'warning':'KRX listing/delisting EVENT records carry retrospective share-kind metadata, not complete date-specific master. Ordinary-share event evidence is not permission to enter a strategy backtest; SPAC name review is non-authoritative, and inactive price observations are not suspension evidence.',
      'generated_at_utc':datetime.now(timezone.utc).isoformat()
    }
    columns=list(rows[0]);dumpcsv(out/'all_3341_event_type_evidence.csv',rows,columns)
    dumpcsv(out/'priority_472_terminal_type_evidence.csv',[r for r in rows if r['last_observation_before_end']=='True'],columns)
    dumpcsv(out/'unresolved_or_conflicted_type_review_queue.csv',[r for r in rows if r['type_evidence_label'].startswith(('UNRESOLVED_','CONFLICTING_'))],columns)
    dumpcsv(out/'spac_name_review_queue.csv',[r for r in rows if r['type_evidence_label']=='SPAC_NAME_REVIEW_NOT_OFFICIAL_SPAC_CLASS'],columns)
    dumpcsv(out/'all_official_event_type_evidence.csv',event_rows,list(event_rows[0]))
    dumpjson(out/'stage4_event_type_audit_summary.json',result)
    print(json.dumps(result,sort_keys=True,ensure_ascii=False),flush=True)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage2-dir',required=True)
    p.add_argument('--stage3-dir',required=True)
    p.add_argument('--out',required=True)
    args=p.parse_args()
    audit(args.stage2_dir,args.stage3_dir,args.out)

if __name__=='__main__':
    try:main()
    except Exception as e:
        print('STAGE4_RESEARCH_FAIL_CLOSED',type(e).__name__,e,file=sys.stderr,flush=True)
        raise SystemExit(2)
