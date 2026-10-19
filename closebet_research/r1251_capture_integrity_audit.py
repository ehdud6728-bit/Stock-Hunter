#!/usr/bin/env python3
"""R1.25.1: independent-code audit of the future R1.25.0 same-day KRX provider snapshot.

Research only. Do not use this to claim independently certified KRX data,
224-session PIT, signal performance, any D+20 OOS, fills, or LIVE approval.
Does not import the producer. Read-only, no network or broker actions.
"""
from __future__ import annotations
import argparse
import csv
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal, InvalidOperation
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import sys

REVISION='R1251_PIT_CAPTURE_SOURCE_AUDIT_V1'
SCHEMA=['code','date','market','open','high','low','close','volume','amount']
MEMBER_SCHEMA=['code','market']
MARKETS={'KOSPI','KOSDAQ','KONEX'}
GITHUB_PRODUCER_BLOBS={
 'closebet_research/r125_prospective_capture.py':'41c1edd3ad70813853ec71765e1b1b12fbb8b56d',
 '.github/workflows/closing_bet_r1250_prospective_capture.yml':'8b262e03b3223aeafa0bf32ead460c514658468e',
}
EXPECTED_PRODUCER_REV='R1250_CONTEMPORANEOUS_FULL_MARKET_CAPTURE_PILOT_V1'
OUTPUT='r1251_audit_report_RESEARCH_ONLY.json'


def demand(ok,code):
    if not bool(ok):raise ValueError(code)


def digest(data):return hashlib.sha256(data).hexdigest()

def git_blob(data):return hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()


def code(s):
    demand(bool(re.fullmatch(r'[0-9]{6}',s)),'TICKER_FORMAT_INVALID')
    return s


def utc_stamp(s):
    try: dt=datetime.fromisoformat(s)
    except (ValueError,TypeError):raise ValueError('INVALID_TIMESTAMP')
    demand(dt.tzinfo is not None and dt.utcoffset() is not None,'TIMESTAMP_NO_OFFSET')
    return dt.astimezone(timezone.utc)


def iso_day(s):
    try:d=date.fromisoformat(s)
    except (ValueError,TypeError):raise ValueError('INVALID_MARKET_DATE')
    demand(d.isoformat()==s and d>=date(2026,10,19),'PRE_FREEZE_OR_NON_ISO_DATE')
    return d


def fixed_int(v,label):
    """Exact integral source values; never silently round prices or traded value."""
    try:n=Decimal(v)
    except (InvalidOperation,TypeError,ValueError):raise ValueError('INVALID_INTEGER_'+label)
    demand(n.is_finite() and n>=0 and n==n.to_integral_value(),'INVALID_INTEGER_'+label)
    return int(n)


def csv_rows(raw,cols,label):
    try:
        text=raw.decode('utf-8-sig')
        rd=csv.DictReader(io.StringIO(text,newline=''))
        demand(rd.fieldnames==cols,'UNEXPECTED_'+label+'_SCHEMA')
        rows=[]
        for row in rd:
            demand(None not in row and all(row[x] is not None for x in cols),'CORRUPTED_'+label+'_CSV')
            rows.append(row)
    except UnicodeDecodeError as e:raise ValueError('UNDECODABLE_'+label+'_CSV') from e
    demand(bool(rows),'EMPTY_'+label)
    return rows


def verified_file(root,name,expected):
    p=root/name
    demand(p.is_file(),'SNAPSHOT_MEMBER_MISSING:'+name)
    raw=p.read_bytes()
    demand(digest(raw)==expected,'SNAPSHOT_SHA256_BAD:'+name)
    return raw


def producer_baseline(repo):
    root=Path(repo)
    checks={}
    for path,expected in GITHUB_PRODUCER_BLOBS.items():
        file=root/path
        demand(file.is_file(),'PINNED_PRODUCER_ABSENT:'+path)
        actual=git_blob(file.read_bytes())
        demand(actual==expected,'PINNED_PRODUCER_CHANGED:'+path)
        checks[path]={'git_blob_sha1':actual,'sha256':digest(file.read_bytes())}
    return checks


def audit_capture(root,source_run,producer_git_sha=None):
    root=Path(root)
    demand(str(source_run).isdigit() and int(source_run)>0,'SOURCE_RUN_ID_INVALID')
    manifest_file=root/'r1250_capture_manifest.json'
    demand(manifest_file.is_file(),'NO_CAPTURE_MANIFEST_EXPECTED_PREFLIGHT_ARTIFACT')
    manifest_raw=manifest_file.read_bytes()
    obj=json.loads(manifest_raw.decode('utf-8'))
    demand(obj.get('revision')==EXPECTED_PRODUCER_REV,'PRODUCER_REVISION_MISMATCH')
    demand(obj.get('status') in ('CAPTURED_MEMBERSHIP_PARITY_CHECKED_HISTORY_PENDING','BLOCKED_MEMBERSHIP_INCOMPLETE_NOT_OOS'),'CAPTURE_STATUS_INVALID')
    demand(obj.get('github_run_id')==str(source_run),'PRODUCER_RUN_ID_MISMATCH')
    if producer_git_sha is not None:
        demand(obj.get('github_checkout_sha')==producer_git_sha,'PRODUCER_CHECKOUT_SHA_MISMATCH')
    demand(obj.get('research_only') is True,'RESEARCH_ONLY_FLAG_MISSING')
    for k in ('trade_approved','oos_complete','D20_complete','prospective_full_market_pit_complete',
              'historical_warmup_full_market_pit','official_source_independent_audit_complete',
              'historical_posthoc_universe_reused','LOW224_R1C1_and_R124_gates_replayed'):
        demand(obj.get(k) is False,'UNWARRANTED_AUTHORITY:'+k)
    for k in ('no_live_changed','no_shadow_changed','no_telegram_changed','no_order_changed'):
        demand(obj.get(k) is True,'UNKNOWN_LIVE_SCOPE:'+k)
    day=iso_day(obj.get('market_date'))
    recorded_utc=utc_stamp(obj.get('observed_at_utc'))
    recorded_kst=utc_stamp(obj.get('observed_at_kst'))
    demand(recorded_utc==recorded_kst,'CAPTURE_CLOCK_CONFLICT')
    loc=recorded_kst.astimezone(timezone(timedelta(hours=9)))
    demand(loc.date()==day and loc.hour>=16,'NON_CONTEMPORANEOUS_OR_PRE_EOD_CAPTURE')
    demand(obj.get('timestamp_provenance')=='RUNNER_UTC_CLOCK_NOT_INDEPENDENT_TRUSTED_TIMESTAMP','TIMESTAMP_PROVENANCE_MISMATCH')
    demand(obj.get('source')=='PYKRX_KRX_DAILY_CROSS_SECTION_ALL_UNVERIFIED_OFFICIAL_PARITY','PROVIDER_MISMATCH')
    demand(obj.get('provider_version')=='1.2.9','UNEXPECTED_KRX_PROVIDER_VERSION')
    files=obj.get('files_sha256',{})
    demand(set(files)=={'r1250_full_market_ohlcva.csv.gz','r1250_listed_membership.csv'},'MEMBER_LIST_CHANGED')
    bars_blob=verified_file(root,'r1250_full_market_ohlcva.csv.gz',files['r1250_full_market_ohlcva.csv.gz'])
    members_blob=verified_file(root,'r1250_listed_membership.csv',files['r1250_listed_membership.csv'])
    try:bars_rows=csv_rows(gzip.decompress(bars_blob),SCHEMA,'BAR')
    except (OSError,EOFError) as e:raise ValueError('INVALID_BAR_GZIP') from e
    members_rows=csv_rows(members_blob,MEMBER_SCHEMA,'MEMBERSHIP')
    bars={};members={};total_amount=0;total_volume=0;zero_bars=0
    counts={m:0 for m in sorted(MARKETS)}
    for row in members_rows:
        k=code(row['code']);market=row['market']
        demand(market in MARKETS,'UNKNOWN_MARKET')
        demand(k not in members,'DUPLICATE_LISTED_CODE')
        members[k]=market;counts[market]+=1
    for row in bars_rows:
        k=code(row['code'])
        demand(k not in bars,'DUPLICATE_CENSUS_CODE')
        demand(row['date']==day.isoformat(),'WRONG_DAY_BAR')
        demand(row['market'] in MARKETS,'BAR_MARKET_UNKNOWN')
        p={q:fixed_int(row[q],q.upper()) for q in SCHEMA[3:]}
        zero=(p['open']==0 or p['close']==0 or p['high']==0 or p['low']==0)
        if zero:
            # R1250 producer's only valid zero quote is a suspended name with
            # O/H/L=0, unchanged last close (possibly 0), volume=amount=0.
            demand(p['volume']==0 and p['amount']==0,'ZERO_BAR_HAS_TRADING')
            demand(p['open']==p['high']==p['low']==0,'BAD_SUSPENSION_SHAPE')
            zero_bars+=1
        else:
            demand(p['high']>=max(p['open'],p['close'],p['low']) and
                   p['low']<=min(p['open'],p['close'],p['high']),'INVALID_OHLC_BAR')
            demand((p['volume']>0)==(p['amount']>0),'PRICE_VOLUME_AMOUNT_CONFLICT')
        bars[k]=row['market'];total_amount+=p['amount'];total_volume+=p['volume']
    missing=set(members)-set(bars)
    extra=set(bars)-set(members)
    wrong_market={k for k in (set(members)&set(bars)) if members[k]!=bars[k]}
    coverage=obj['coverage']
    demand(len(members)==coverage['member_count'] and len(bars)==coverage['quote_count'],'MANIFEST_COUNT_CONFLICT')
    demand(len(missing)==coverage['missing_member_quotes_count'] and len(extra)==coverage['extra_unlisted_quotes_count'],'MANIFEST_COVERAGE_CONFLICT')
    demand(zero_bars==coverage['invalid_or_suspended_bars'],'ZERO_BAR_METRIC_CONFLICT')
    demand(counts==coverage['member_market_counts'],'MARKET_SPLIT_MANIFEST_MISMATCH')
    scope_pass=not (missing or extra or wrong_market) and len(members)>=500 and all(counts.values())
    demand(coverage['exact_market_membership_match']==(not(missing or extra)),'MANIFEST_MEMBERSHIP_FLAG_CONFLICT')
    demand(coverage['eod_snapshot_scope_pass']==(not(missing or extra) and len(members)>=500),'MANIFEST_SCOPE_FLAG_CONFLICT')
    status='PASS_CAPTURE_SOURCE_INTEGRITY_NOT_OOS_NOT_TRADE_APPROVED' if scope_pass and obj['status']=='CAPTURED_MEMBERSHIP_PARITY_CHECKED_HISTORY_PENDING' else 'BLOCKED_INCOMPLETE_CENSUS_NOT_OOS'
    return {
        'revision':REVISION,'status':status,'source_run':int(source_run),
        'source_market_date':day.isoformat(),'observed_at_utc':recorded_utc.isoformat(),
        'producer_manifest_sha256':digest(manifest_raw),
        'producer_git_sha':obj.get('github_checkout_sha'),
        'member_sha256':files,'listed_tickers':len(members),'quote_tickers':len(bars),
        'market_listed_counts':counts,'missing_quotes':len(missing),'extra_quotes':len(extra),
        'wrong_market_assignments':len(wrong_market),'zero_or_suspended_bars':zero_bars,
        'observed_traded_volume':total_volume,'observed_actual_traded_amount_krw':total_amount,
        'scope_pass':scope_pass,'permanent_archive':False,'independent_krx_certification':False,
        'full_market_pit_224_sessions':False,'oos_complete':False,'d20_complete':False,
        'live_changed':False,'shadow_changed':False,'telegram_changed':False,'orders_changed':False,
        'warnings':[
            'All price and membership facts originate from the SAME pykrx/KRX adapter. This is an independent-code integrity audit, not independent official-data certification.',
            'GitHub archive expires; a separate append-only durable store is necessary.',
            'One date is NOT an as-of 224-session universe/history or D+20 outcome.',
            'No strategy candidate was emitted. No TP/stop or realized entry/exit validated.',
            'Runner timestamp is not a trusted external timestamp attestation.',
        ]
    }


def report(root,source_run,output,repo=None,producer_git_sha=None):
    out=Path(output);out.mkdir(parents=True,exist_ok=True)
    info={'revision':REVISION,'status':'FAIL_CLOSED_NOT_OOS','source_run':str(source_run),
          'oos_complete':False,'trade_approved':False,'live_changed':False}
    try:
        if repo is not None: info['producer_blobs']=producer_baseline(repo)
        info.update(audit_capture(root,source_run,producer_git_sha=producer_git_sha))
    except (ValueError,KeyError,TypeError,UnicodeDecodeError,json.JSONDecodeError) as e:
        info['status']='FAIL_CLOSED_NOT_OOS'
        info['reason']=str(e)
    (out/OUTPUT).write_text(json.dumps(info,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    (out/'r1251_korean_readme.md').write_text(
       '# R1.25.1 신규 거래일 스냅샷 무결성 검사\n\n'
       f"- 최종: **{info['status']}**\n"
       f"- 원본 R1.25.0 실행: `{source_run}`\n"
       '- *직접 수집한 새로운 당일 거래일* 자료에 대한 사후 파일·시장목록 교차 검증만 수행합니다.\n'
       '- 독립 KRX 공식 인증, 전체시장 PIT 224일, 실전 체결, 미래 OOS, LIVE 적용 권한이 아닙니다.\n'
       '- GitHub 아티팩트는 영구 저장이 아니므로 별도 append-only 원본 보관이 필요합니다.\n',encoding='utf-8')
    return info


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--snapshot-root',required=True)
    p.add_argument('--source-run',required=True)
    p.add_argument('--output',required=True)
    p.add_argument('--repo',default='.')
    p.add_argument('--producer-git-sha')
    args=p.parse_args(argv)
    x=report(args.snapshot_root,args.source_run,args.output,args.repo,args.producer_git_sha)
    print('R1251_RESEARCH_ONLY '+json.dumps({'status':x['status'],'source_run':args.source_run},ensure_ascii=False))
    return 0 if x['status']=='PASS_CAPTURE_SOURCE_INTEGRITY_NOT_OOS_NOT_TRADE_APPROVED' else 2

if __name__=='__main__':sys.exit(main())
