#!/usr/bin/env python3
"""RESEARCH_ONLY, source-locked PIT security identity/lifecycle and sampled KRX market lists.

This is an *audit*, NOT a usable investable equity universe. The existing full-market
price source is pinned to GitHub Actions run 39295295197. An observed last price day
is NEVER interpreted as delisting, and 6-character issue codes are NEVER classified
as common equity by syntax. Neither PIT security types nor suspended/delist events
are independently certified in this stage.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import json
import pathlib
import re
import time
from collections import Counter

import krx_full_market_pit as old

PINNED_RUN = 39295295197
PINNED_SHA = '08e328c101f0774a2236c0a44cc1fdbbaa59e96e'
PINNED_CALENDAR_SHA = '2d5a868e68f969072b0a4d6a9ee097e954f46e7b7eb528c6c5c6a0af95e1a795'
PINNED_EXPECTED_ROWS = 5090428
PINNED_DATES = 1907
MARKETS = ('KOSPI', 'KOSDAQ', 'KONEX')
DEFAULT_SAMPLE_DATES = ('2019-01-02','2022-01-03','2024-01-02','2026-10-12')
CODE_RE = re.compile(r'[0-9][0-9A-Z]{5}\Z')


def hard_stop(message):
    raise RuntimeError('FAIL_CLOSED_' + str(message))


def sha_bytes(blob):
    return hashlib.sha256(blob).hexdigest()


def read_manifest_dates(root, reference_path, verification_report=None):
    root = pathlib.Path(root)
    ref = old.read_reference(reference_path)
    days = sorted(ref)
    if len(days) != PINNED_DATES or sha_bytes('\n'.join(days).encode('utf-8')) != PINNED_CALENDAR_SHA:
        hard_stop('PINNED_REFERENCE_NOT_MATCHED')
    # Price verify rereads every file, independently checks stored SHA, index series,
    # original per-session row-count anchors, and all eight complete shard manifests.
    audit = old.verify(root, reference_path, verification_report or (root.parent / 'source_fullmarket_price_verification.json'))
    if audit['full_market_price_rows'] != PINNED_EXPECTED_ROWS or audit['sessions'] != PINNED_DATES:
        hard_stop('PINNED_SOURCE_ROW_TOTAL_CHANGED')
    loc = {}
    for p in sorted(root.glob('shard_*/manifest_shard_*.json')):
        manifest = json.loads(p.read_text(encoding='utf-8'))
        for f in manifest['files']:
            if f['date'] in loc:
                hard_stop('DUPLICATE_DATE_FILE_' + f['date'])
            loc[f['date']] = p.parent / f['file']
    if set(loc) != set(days):
        hard_stop('SOURCE_DATE_SET_CHANGED')
    return days, loc, audit


def derive_lifecycle(root, reference, output_dir):
    """Derive observed source lifetimes, not delisting or listing facts."""
    out = pathlib.Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    days, loc, price_audit = read_manifest_dates(root, reference, out / 'source_fullmarket_price_verification.json')
    by_code = {}
    per_date = {}
    total = 0
    for d in days:
        positive, zero = 0, 0
        alpha, numeric = 0, 0
        codes = set()
        with gzip.open(loc[d], 'rt', encoding='utf-8', newline='') as fd:
            reader = csv.DictReader(fd)
            if reader.fieldnames != old.SCHEMA:
                hard_stop('WRONG_SOURCE_HEADER_' + d)
            for row in reader:
                code = row['code']
                if not CODE_RE.fullmatch(code) or row['date'] != d or code in codes:
                    hard_stop('CORRUPT_ROW_ID_' + d)
                codes.add(code)
                total += 1
                alpha += bool(re.search('[A-Z]',code))
                numeric += code.isdecimal()
                vol = int(row['volume']); amt = int(row['amount'])
                traded = (vol > 0 and amt > 0 and all(float(row[k]) > 0 for k in ('open','high','low','close')))
                positive += int(traded)
                zero += int(not traded)
                if code not in by_code:
                    by_code[code] = dict(code=code, first_seen=d, last_seen=d,
                                         seen_sessions=0, observed_tradeable_proxy_sessions=0,
                                         zero_or_nontradeable_proxy_sessions=0,
                                         code_has_letters=bool(re.search('[A-Z]',code)))
                b = by_code[code]
                b['last_seen'] = d
                b['seen_sessions'] += 1
                b['observed_tradeable_proxy_sessions'] += int(traded)
                b['zero_or_nontradeable_proxy_sessions'] += int(not traded)
        per_date[d] = dict(date=d, raw_price_rows=len(codes), numeric_code_rows=numeric,
                           alphanumeric_code_rows=alpha, tradeable_proxy_rows=positive,
                           zero_or_nontradeable_proxy_rows=zero)
    if total != PINNED_EXPECTED_ROWS:
        hard_stop('DERIVED_SOURCE_TOTAL_MISMATCH')
    latest = days[-1]
    lifecycle = []
    for b in sorted(by_code.values(), key=lambda x:x['code']):
        # These flags are OBSERVATIONS, not verified listing/delisting facts.
        b['last_observation_before_end'] = b['last_seen'] < latest
        b['delisted_status'] = 'NOT_VERIFIED_EXTERNAL_DELISTING_REGISTER'
        b['security_type'] = 'NOT_VERIFIED_OFFICIAL_TYPE_REGISTER'
        b['tradeability_status'] = 'OHLCV_PROXY_ONLY_NOT_OFFICIAL_SUSPENSION'
        lifecycle.append(b)
    write_csv(out / 'observed_ticker_lifecycles.csv', lifecycle)
    write_csv(out / 'all_session_observation_counts.csv', [per_date[d] for d in days])
    summary = dict(status='PRICE_OBSERVATIONS_ONLY_SECURITY_MASTER_UNVERIFIED',
                   source_run_id=PINNED_RUN, source_head_sha=PINNED_SHA,
                   sessions=len(days), price_rows=total, observed_codes=len(lifecycle),
                   codes_last_observed_before_final=sum(x['last_observation_before_end'] for x in lifecycle),
                   alphanumeric_codes=sum(x['code_has_letters'] for x in lifecycle),
                   numeric_codes=sum(not x['code_has_letters'] for x in lifecycle),
                   source_price_integrity='PASS_PINNED_1907_DAY_PRICE_AND_INDEX_SHA',
                   asof_membership='NOT_YET_CHECKED', type_registry='NOT_YET_CHECKED',
                   delisting_registry='NOT_YET_CHECKED',
                   strategy_ready=False,
                   warning='Last observed price is not delisting evidence; source price presence does not prove tradeability or ordinary shares.')
    old.atomic_json(out / 'lifecycle_summary.json', summary)
    return summary, per_date


def write_csv(path, rows):
    path=pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        hard_stop('EMPTY_CSV_' + path.name)
    fields=list(rows[0])
    with path.open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def membership_sample(provider, source_daily, dates, output_dir, pause=1.5, retries=3):
    """Historical KRX per-market roster sample, separate endpoint, same KRX authority.

    Sample mismatches are persisted as explicit UNCERTAINTY, never silently removed.
    This is not a complete 1907-day PIT master and not a securities-type registry.
    """
    out=pathlib.Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    comparisons=[]
    snapshots=[]
    for date in dates:
        if date not in source_daily:
            hard_stop('SAMPLE_DATE_NOT_IN_PINNED_CALENDAR_'+date)
        seen={}
        for market in MARKETS:
            last_error=None
            for n in range(retries):
                try:
                    values=provider.get_market_ticker_list(date.replace('-',''),market=market)
                    if not isinstance(values,(list,tuple)) or len(values)<15:
                        hard_stop('EMPTY_MARKET_MEMBERSHIP_'+date+'_'+market)
                    codes=[str(x) for x in values]
                    if any(not CODE_RE.fullmatch(c) for c in codes) or len(set(codes))!=len(codes):
                        hard_stop('INVALID_MEMBERSHIP_CODES_'+date+'_'+market)
                    seen[market]=set(codes)
                    last_error=None
                    break
                except Exception as ex:
                    last_error=ex
                    if n+1<retries:time.sleep(pause*(n+1))
            if last_error is not None:
                hard_stop('MEMBERSHIP_SOURCE_ERROR_'+date+'_'+market+':'+str(last_error))
            if pause:time.sleep(pause)
        duplicates=(seen['KOSPI'] & seen['KOSDAQ']) | (seen['KOSPI'] & seen['KONEX']) | (seen['KOSDAQ'] & seen['KONEX'])
        if duplicates:
            hard_stop('MULTIMARKET_DUPLICATE_CODES_'+date)
        union=set().union(*seen.values())
        # Existing price-source rows and date-matched membership may be different
        # KRX endpoints, so record counts and identities without forcing equality.
        price_codes=source_daily[date]['codes']
        extra=sorted(price_codes-union)
        absent=sorted(union-price_codes)
        comparisons.append(dict(date=date,price_codes=len(price_codes),
                                kospi=len(seen['KOSPI']),kosdaq=len(seen['KOSDAQ']),
                                konex=len(seen['KONEX']),listed_union=len(union),
                                intersection=len(price_codes&union),
                                price_missing_in_market_rosters=len(extra),
                                roster_missing_in_price=len(absent),
                                market_roster_same_as_price=(not extra and not absent)))
        snapshots.append(dict(date=date,market_codes={k:sorted(v) for k,v in seen.items()},
                              price_only_codes=extra,roster_only_codes=absent,
                              source='KRX pykrx get_market_ticker_list(dated) - cross-endpoint only, not independent exchange',
                              definitely_delisted=False))
    write_csv(out/'market_membership_sample_comparisons.csv',comparisons)
    old.atomic_json(out/'dated_market_membership_snapshots.json',snapshots)
    return dict(sampled_sessions=len(dates),sampled_dates=list(dates),
                mismatch_sessions=sum(not x['market_roster_same_as_price'] for x in comparisons),
                price_only_rows=sum(x['price_missing_in_market_rosters'] for x in comparisons),
                roster_only_rows=sum(x['roster_missing_in_price'] for x in comparisons),
                market_membership_status='SAMPLED_KRX_CROSS_ENDPOINT_EVIDENCE_ONLY',
                strategy_ready=False)


def read_sample_day_codes(root, dates):
    """Read source identities for sampled dates only; full 1907 SHA checked first."""
    root=pathlib.Path(root)
    mapping={}
    for p in root.glob('shard_*/manifest_shard_*.json'):
        meta=json.loads(p.read_text('utf-8'))
        for f in meta['files']:
            if f['date'] in dates:
                file=p.parent/f['file']
                codes=set()
                with gzip.open(file,'rt',encoding='utf-8',newline='') as fd:
                    for row in csv.DictReader(fd):
                        codes.add(row['code'])
                if len(codes)!=f['stored_rows']:
                    hard_stop('SOURCE_SAMPLE_DUPLICATE_OR_COUNT_'+f['date'])
                mapping[f['date']]={'codes':codes}
    if set(mapping)!=set(dates):
        hard_stop('SOURCE_SAMPLE_MISSING')
    return mapping


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',default='pitdata')
    p.add_argument('--reference',default='research_pit/historical_market_counts_20190102_20261012.csv')
    p.add_argument('--output',default='reports/pit-stage2')
    p.add_argument('--sample-dates',default=','.join(DEFAULT_SAMPLE_DATES))
    p.add_argument('--offline-only',action='store_true')
    p.add_argument('--pause',type=float,default=1.5)
    x=p.parse_args(argv)
    summary,observations=derive_lifecycle(x.root,x.reference,x.output)
    dates=x.sample_dates.split(',')
    if len(dates)!=len(set(dates)) or not dates:
        hard_stop('BAD_SAMPLE_DATES')
    if not x.offline_only:
        from pykrx import stock
        samples=read_sample_day_codes(x.root,dates)
        check=membership_sample(stock,samples,dates,x.output,pause=x.pause)
        summary.update(check)
        summary['asof_membership']='SAMPLED_4_DATES_NOT_FULL_MASTER'
    old.atomic_json(pathlib.Path(x.output)/'lifecycle_summary.json',summary)
    print(json.dumps(summary,ensure_ascii=False,sort_keys=True),flush=True)
    return summary


if __name__=='__main__':
    main()
