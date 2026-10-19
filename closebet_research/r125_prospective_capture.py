#!/usr/bin/env python3
"""Stock-Hunter R1.25.0: prospective KRX EOD census intake, NOT a trade signal.

Explicitly protects the prior frozen R1.24 rules and LOW224 R1C1 detector.
Never backdates an observed historical price/membership as an as-of capture.
This first stage records ONE contemporaneously observed source snapshot.
No forward returns, LIVE search/rank, Telegram, orders, or shadow writes.
"""
from __future__ import annotations
import argparse
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sys
import importlib.metadata
import pandas as pd
import numpy as np

REVISION = 'R1250_CONTEMPORANEOUS_FULL_MARKET_CAPTURE_PILOT_V1'
FREEZE_DATE = '2026-10-16'
FIRST_ELIGIBLE_DATE = '2026-10-19'
KST = ZoneInfo('Asia/Seoul')
RESEARCH_ONLY = True
COLUMNS = ['code','date','market','open','high','low','close','volume','amount']
MARKETS = ('KOSPI','KOSDAQ','KONEX')
# Audited GitHub blob identities: do not modify discovery/frozen baselines in this lane.
PINS = {
    'low224_accum_wave1_pb_research.py':'eef5447b0ee24ee7f7ded3a6e7edc32ae1ada7eb',
    'closebet_research/r124_frozen_hypotheses.json':'6ceef2b3b2672bf90b227b83deb3be8ec3beb54f',
    'closebet_research/r122_candle_support_action_research.py':'bb78b49160d320cb6e78a80984cd4f4ad14bcd75'
}
PROTECTED = {
    '.github/workflows/v49.76.yml':'81a796c7e86e61458d4e4bfdec3f4c7252a4c562',
    '.github/workflows/closing_bet_live_shadow_board_r175.yml':'ec2ea4ee8e35199fff706029c29873eb157a0343'
}


def fail(ok, label):
    if not bool(ok):
        raise ValueError(label)


def hash_bytes(b):
    return hashlib.sha256(b).hexdigest()


def gitblob_bytes(b):
    return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()


def sha(path):
    return hash_bytes(Path(path).read_bytes())


def iso_date(s):
    try:
        d=date.fromisoformat(str(s))
        fail(d.isoformat()==str(s),'DATE_NOT_ISO')
        return d
    except (TypeError,ValueError) as e:
        raise ValueError('DATE_NOT_ISO') from e


def check_baselines(repo):
    """Check exact existing code version, rather than silently importing tuned logic."""
    repo=Path(repo)
    details={}
    for path,expected in {**PINS,**PROTECTED}.items():
        p=repo/path
        fail(p.is_file(),'PINNED_REPO_FILE_MISSING:'+path)
        val=gitblob_bytes(p.read_bytes())
        fail(val==expected,'BASELINE_GIT_BLOB_DRIFT:'+path)
        details[path]={'git_blob':val,'sha256':sha(p)}
    policy=json.loads((repo/'closebet_research/r124_frozen_hypotheses.json').read_text(encoding='utf-8'))
    fail(policy.get('research_revision')=='R124_FROZEN_D0_GATE_V1','R124_POLICY_BAD')
    fail(policy.get('candidate_gates')==['H1_PREV_LOW_HELD','H2_HELD_MA20_UP_ABOVE_20_60','H3_HELD_ACCUM20_DRY5'],'R124_GATES_CHANGED')
    fail(policy.get('future_oos_complete') is False,'UNEXPECTED_OOS_CLAIM')
    return details


def capture_clock(now=None):
    n=now if now is not None else datetime.now(timezone.utc)
    fail(n.tzinfo is not None and n.utcoffset() is not None,'NAIVE_CAPTURE_CLOCK_FORBIDDEN')
    n=n.astimezone(timezone.utc)
    return n,n.astimezone(KST)


def validate_capture_time(signal_day, now=None):
    d=iso_date(signal_day)
    utc,local=capture_clock(now)
    fail(d.isoformat()>=FIRST_ELIGIBLE_DATE,'DATE_NOT_AFTER_FROZEN_DISCOVERY')
    fail(local.date()==d,'NOT_CAPTURED_ON_TARGET_KST_DATE')
    fail(local.time().replace(tzinfo=None)>=time(16,0),'KRX_EOD_NOT_YET_FROZEN')
    return utc,local


def code_str(x):
    s=str(x).strip()
    fail(bool(re.fullmatch(r'\d{6}',s)),'INVALID_SIX_DIGIT_TICKER:'+s[:30])
    return s


def normalize_market_frame(raw, day):
    """No Close*Volume derived Amount; no code/date fallback; no duplicate repair."""
    fail(isinstance(raw,pd.DataFrame) and not raw.empty,'KRX_ALL_OHLCV_EMPTY')
    mapping={'시가':'open','고가':'high','저가':'low','종가':'close','거래량':'volume','거래대금':'amount'}
    fail(set(mapping).issubset(raw.columns),'KRX_ACTUAL_AMOUNT_OR_OHLCV_MISSING')
    keys=[code_str(x) for x in raw.index.tolist()]
    fail(len(set(keys))==len(keys),'KRX_DUPLICATE_CODE')
    d=raw[list(mapping)].rename(columns=mapping).reset_index(drop=True)
    d.insert(0,'code',keys)
    d.insert(1,'date',day)
    # No numeric coercion: provider unexpectedly introducing strings or NaN is rejected.
    for c in ('open','high','low','close','volume','amount'):
        d[c]=pd.to_numeric(d[c],errors='coerce')
    fail(np.isfinite(d[['open','high','low','close','volume','amount']].to_numpy(dtype=float)).all(),
         'KRX_NAN_INF_OR_BAD_NUMERIC')
    fail((d[['open','high','low','close','volume','amount']]>=0).all().all(),'KRX_NEGATIVE_VALUE')
    # KRX suspension records may contain last CLOSE while OPEN/HIGH/LOW are zero.
    # Retain non-tradable suspended members in the full-market census; they MUST
    # NOT be reclassified as a tradable candle or dropped from historical PIT.
    # Reject every other partial-zero OHLC combination rather than "repair" it.
    zero_flags=d[['open','high','low','close']].eq(0).any(axis=1)
    suspended_shape=(d['open'].eq(0)&d['high'].eq(0)&d['low'].eq(0)&
                     d['volume'].eq(0)&d['amount'].eq(0))
    fail((~zero_flags | suspended_shape).all(),'KRX_PARTIAL_ZERO_BAR_INVALID')
    traded=d.loc[~zero_flags]
    fail(((traded['high']>=traded[['open','close','low']].max(axis=1)) &
          (traded['low']<=traded[['open','close','high']].min(axis=1))).all(),
         'KRX_BAR_RANGE_INVALID')
    # Zero-volume bars are still retained, but may not carry positive traded value.
    fail((traded['volume'].gt(0)==traded['amount'].gt(0)).all(),
         'KRX_VOLUME_AMOUNT_CONFLICT')
    d['invalid_or_suspended_bar']=zero_flags
    return d


def normalize_membership(stock,day):
    member=[]
    for market in MARKETS:
        raw=stock.get_market_ticker_list(day.replace('-',''),market=market)
        fail(isinstance(raw,(list,tuple)) and len(raw)>0,'KRX_MEMBERSHIP_EMPTY:'+market)
        for val in raw:
            member.append((code_str(val),market))
    d=pd.DataFrame(member,columns=['code','market'])
    fail(not d.code.duplicated().any(),'KRX_MEMBERSHIP_DUPLICATE_ACROSS_MARKETS')
    return d.sort_values('code').reset_index(drop=True)


def official_session(stock,day):
    stamp=day.replace('-','')
    q=stock.get_index_ohlcv_by_date(stamp,stamp,'1001')
    fail(isinstance(q,pd.DataFrame) and not q.empty,'KRX_OFFICIAL_DAY_NOT_FOUND')
    days={pd.Timestamp(x).date().isoformat() for x in q.index}
    fail(days=={day},'KRX_OFFICIAL_SESSION_DATE_MISMATCH')


def create_snapshot(stock,signal_day,output,now=None,provider_version='UNKNOWN'):
    """Provider called only after the contemporaneous timing gate. Output never overwrites."""
    utc,local=validate_capture_time(signal_day,now)
    day=iso_date(signal_day).isoformat()
    out=Path(output)
    fail(not out.exists(),'APPEND_ONLY_DESTINATION_ALREADY_EXISTS')
    official_session(stock,day)
    members=normalize_membership(stock,day)
    # Alternative=False forbids silently switching a holiday to the previous session.
    quotes=stock.get_market_ohlcv_by_ticker(day.replace('-',''),market='ALL',alternative=False)
    bars=normalize_market_frame(quotes,day)
    codes=set(bars.code)
    expected=set(members.code)
    missing=sorted(expected-codes)
    extra=sorted(codes-expected)
    audit={'member_count':len(members),'quote_count':len(bars),
           'missing_member_quotes_count':len(missing),'extra_unlisted_quotes_count':len(extra),
           'missing_member_quotes_sample':missing[:20], 'extra_unlisted_quotes_sample':extra[:20],
           'invalid_or_suspended_bars':int(bars.invalid_or_suspended_bar.sum()),
           'exact_market_membership_match':not(missing or extra),
           'member_market_counts':members.market.value_counts().sort_index().to_dict()}
    # A large count alone is never evidence of complete exchange membership.
    good=audit['exact_market_membership_match'] and len(members)>=500
    audit['eod_snapshot_scope_pass']=bool(good)
    bars=bars.drop(columns=['invalid_or_suspended_bar']).merge(members,on='code',how='left',validate='one_to_one')
    bars=bars[COLUMNS].sort_values('code').reset_index(drop=True)
    fail(not bars[['code','date']].duplicated().any(),'SNAPSHOT_DUPLICATES')
    # Make deterministic gz data files. No future label or strategy gate is read/written.
    encoded=gzip.compress(bars.to_csv(index=False,lineterminator='\n').encode('utf-8'),mtime=0)
    listed=members.to_csv(index=False,lineterminator='\n').encode('utf-8')
    # Container time is an observed run time, not independent cryptographic time attestation.
    meta={'revision':REVISION, 'status':('CAPTURED_MEMBERSHIP_PARITY_CHECKED_HISTORY_PENDING' if good else 'BLOCKED_MEMBERSHIP_INCOMPLETE_NOT_OOS'),
          'research_only':True,'observed_at_utc':utc.isoformat(),'observed_at_kst':local.isoformat(),
          'market_date':day,'timestamp_provenance':'RUNNER_UTC_CLOCK_NOT_INDEPENDENT_TRUSTED_TIMESTAMP',
          'source':'PYKRX_KRX_DAILY_CROSS_SECTION_ALL_UNVERIFIED_OFFICIAL_PARITY',
          'provider_version':provider_version,
          'github_run_id':os.getenv('GITHUB_RUN_ID','LOCAL_UNATTESTED'),
          'github_checkout_sha':os.getenv('GITHUB_SHA','LOCAL_UNATTESTED'),
          'historical_posthoc_universe_reused':False,
          'historical_warmup_full_market_pit':False,'prospective_full_market_pit_complete':False,
          'official_source_independent_audit_complete':False, 'D20_complete':False,
          'trade_approved':False,'oos_complete':False,
          'LOW224_R1C1_and_R124_gates_replayed':False,
          'requires_unadjusted_224_session_history_and_corporate_action_audit':True,
          'no_live_changed':True,'no_shadow_changed':True,'no_telegram_changed':True,'no_order_changed':True,
          'coverage':audit,
          'files_sha256':{'r1250_full_market_ohlcva.csv.gz':hash_bytes(encoded),
                          'r1250_listed_membership.csv':hash_bytes(listed)}}
    out.mkdir(parents=True,exist_ok=False)
    (out/'r1250_full_market_ohlcva.csv.gz').write_bytes(encoded)
    (out/'r1250_listed_membership.csv').write_bytes(listed)
    (out/'r1250_capture_manifest.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return meta


def verify_snapshot(root):
    out=Path(root)
    meta=json.loads((out/'r1250_capture_manifest.json').read_text(encoding='utf-8'))
    fail(meta['revision']==REVISION and meta['research_only'],'MANIFEST_AUTHORITY_BAD')
    for fn,h in meta['files_sha256'].items():
        p=out/fn
        fail(p.is_file() and sha(p)==h,'SNAPSHOT_MEMBER_SHA_MISMATCH:'+fn)
    payload=gzip.decompress((out/'r1250_full_market_ohlcva.csv.gz').read_bytes())
    d=pd.read_csv(io.BytesIO(payload),dtype={'code':str,'date':str,'market':str})
    m=pd.read_csv(out/'r1250_listed_membership.csv',dtype={'code':str,'market':str})
    fail(list(d.columns)==COLUMNS,'SNAPSHOT_SCHEMA_DRIFT')
    fail(not d.code.duplicated().any() and not m.code.duplicated().any(),'SNAPSHOT_DUPLICATE_CODE')
    fail(d.date.eq(meta['market_date']).all(),'SNAPSHOT_FUTURE_OR_OTHER_DATE')
    fail(not any(c in d for c in ('future_return','tp5','stop5','outcome')),'FUTURE_COLUMN_FOUND')
    fail(len(d)==meta['coverage']['quote_count'] and len(m)==meta['coverage']['member_count'],'CENSUS_COUNT_MISMATCH')
    if meta['coverage']['exact_market_membership_match']:
        a=d[['code','market']].sort_values('code').reset_index(drop=True)
        b=m[['code','market']].sort_values('code').reset_index(drop=True)
        fail(a.equals(b),'MARKET_MEMBERSHIP_CHANGED_AFTER_CAPTURE')
    fail(meta['trade_approved'] is False and meta['oos_complete'] is False,'FALSE_TRADE_OR_OOS_AUTHORITY')
    return meta


def readiness(baselines, captured=None):
    m={'revision':REVISION,'phase':'R1250_DAY0_INTAKE_INFRASTRUCTURE',
       'status':'WAIT_NEW_REAL_PIT_DATA_AND_224_HISTORY',
       'historic_r1241_run':38904901778,'historic_r1241_mature':3077,
       'R124_rules_frozen':True,'LOW224_R1C1_baseline_frozen':True,
       'fresh_KRX_session_captured':bool(captured is not None),
       'latest_snapshot_scope_pass':bool(captured and captured['coverage']['eod_snapshot_scope_pass']),
       'separate_wave1_pullback_detector_executed':False,
       'independent_forward_OOS':False,'first_oos_signal_date_not_before':FIRST_ELIGIBLE_DATE,
       'd20_paths_matured_after_freeze':0,
       'full_market_pit_history_224_sessions_complete':False,
       'live_changed':False,'shadow_changed':False,'telegram_changed':False,'orders_changed':False,
       'blockers':[
            'No 224-session complete as-of all-market history and corporate-action price adjustment parity.',
            'Need persistent append-only daily membership and OHLCV amount archives with independent timestamp authority.',
            'Need one-time event-date membership and 20 subsequent official trading sessions, including delisted names.',
            'R1C1 detector and R124 H1/H2/H3 prospective signals NOT generated by this ingestion release.',
            'Full-market exchange provenance is provider-adapter evidence, not independent official KRX certification.'
        ],
       'frozen_source_git_blobs':{p:v['git_blob'] for p,v in baselines.items()}}
    return m


def write_readiness(out,baselines,captured=None):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    obj=readiness(baselines,captured)
    (out/'r1250_READINESS_RESEARCH_ONLY.json').write_text(json.dumps(obj,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    (out/'r1250_READINESS_RESEARCH_ONLY.md').write_text(
        '# R1.25.0 사전 데이터 확보 준비 결과\n\n'
        '상태: **WAIT_NEW_REAL_PIT_DATA_AND_224_HISTORY**.\n\n'
        '기존 R1.24 가설과 LOW224 R1C1 구조를 동결한 채 실행만 확인했습니다.\n'
        '현재 단계는 자료수집기이며 224일선 첫 파동 분석이나 미래 OOS 결과를 생성하지 않습니다.\n'
        '당일 수집은 지정한 거래일 오후 4시 이후 같은 KST 날짜에만 가능합니다.\n'
        'GitHub 아티팩트는 영구 보존소가 아니므로 외부 원본 보관이 별도로 필요합니다.\n'
        '이 워크플로는 LIVE, Shadow, Telegram, 자동매도에 접근하지 않습니다.\n',encoding='utf-8')
    return obj


def run_cli(argv=None,stock=None,now=None):
    p=argparse.ArgumentParser()
    p.add_argument('--mode',choices=('preflight','capture','verify'),required=True)
    p.add_argument('--repo',default='.')
    p.add_argument('--out',required=True)
    p.add_argument('--signal-day')
    p.add_argument('--snapshot-root')
    x=p.parse_args(argv)
    pins=check_baselines(x.repo)
    capture=None
    if x.mode=='capture':
        fail(bool(x.signal_day),'MISSING_SIGNAL_DAY')
        # Never call external source before rejecting future/past/not-yet-EOD session.
        validate_capture_time(x.signal_day,now)
        if stock is None:
            from pykrx import stock as krx_stock
            stock=krx_stock
        try:
            pv=importlib.metadata.version('pykrx')
        except importlib.metadata.PackageNotFoundError:
            pv='MOCK_ONLY_TEST_SOURCE'
        capture=create_snapshot(stock,x.signal_day,Path(x.out)/'snapshot',now=now,provider_version=pv)
        verify_snapshot(Path(x.out)/'snapshot')
    elif x.mode=='verify':
        fail(bool(x.snapshot_root),'MISSING_SNAPSHOT_ROOT')
        capture=verify_snapshot(x.snapshot_root)
    r=write_readiness(Path(x.out)/'readiness',pins,capture)
    print('R1250_RESEARCH_ONLY '+json.dumps({'status':r['status'],'snapshot_captured':r['fresh_KRX_session_captured'],
          'snapshot_scope_pass':r['latest_snapshot_scope_pass'],'independent_forward_OOS':r['independent_forward_OOS']},ensure_ascii=False))
    return r

if __name__=='__main__':
    try:
        run_cli()
    except (ValueError,RuntimeError) as exc:
        print('R1250_FAIL_CLOSED',str(exc),file=sys.stderr)
        raise SystemExit(2)
