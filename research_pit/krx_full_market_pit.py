#!/usr/bin/env python3
"""RESEARCH ONLY: historical, unfiltered, per-session KRX OHLCV/actual traded amount.

NO production import, NO trades, NO today-selected universe, NO zero-fill of missing days.
This is a price-collection integrity layer; it does NOT attest PIT security master,
PIT market capitalization, listing/delisting completeness, or historical fundamentals.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import pathlib
import re
import sys
import time
from datetime import datetime, timezone

import pandas as pd

SCHEMA = ['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']
SOURCE_COLS = {'시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close',
               '거래량': 'volume', '거래대금': 'amount'}
FORMAT = 'STOCKHUNTER_KRX_ALL_SESSION_V1'
MIN_ALL_MARKET_ROWS = 500
# KRX session source uses several six-character issue-code families. Examples observed
# in REAL historical KRX responses: 03473K (2019) and 0099X0 (2026). Character
# positions are not a security-type classifier; don't exclude a valid source record
# for placing an uppercase letter in a different position.
# Syntax is deliberately permissive for 6-character ASCII uppercase alphanumerics
# STARTING WITH A DIGIT. Historical constituent/tradability checks come LATER.
STORED_TICKER_PATTERN = r'[0-9][0-9A-Z]{5}'
# Extra diagnostics only; never filter out syntactically valid KRX codes.
COMMON_TICKER_PATTERN = r'(?:[0-9]{6}|[0-9]{5}[A-Z])'


def fail(msg):
    raise RuntimeError('FAIL_CLOSED_' + str(msg))


def h(b):
    return hashlib.sha256(b).hexdigest()


def atomic_bytes(path, data):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.part')
    temp.write_bytes(data)
    os.replace(temp, path)


def atomic_json(path, value):
    atomic_bytes(path, (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n').encode('utf-8'))


def canonical_ticker(x):
    s = str(x).strip()
    if re.fullmatch(r'[0-9]{1,6}', s):
        return s.zfill(6)
    if re.fullmatch(STORED_TICKER_PATTERN, s):
        return s  # Preserve exactly; alphanumeric can occur BEFORE final character.
    fail('BAD_TICKER:' + s)


def read_reference(path):
    with open(path, encoding='utf-8-sig', newline='') as f:
        rows = list(csv.DictReader(f))
    counts = {}
    for r in rows:
        date = r['date']
        pd.Timestamp(date)  # malformed dates fail closed
        n = int(r['expected_all_market_rows'])
        if date in counts or n < MIN_ALL_MARKET_ROWS:
            fail('BAD_REFERENCE:' + date)
        counts[date] = n
    return counts


def normalize_day(raw, date):
    if not isinstance(raw, pd.DataFrame) or len(raw) < MIN_ALL_MARKET_ROWS:
        fail('EMPTY_OR_UNDERSIZED_KRX_SNAPSHOT:' + date)
    if any(col not in raw for col in SOURCE_COLS):
        fail('MISSING_SOURCE_COLUMNS:' + date)
    # Never consult a current/recent list of desired ticker codes.
    frame = raw[list(SOURCE_COLS)].rename(columns=SOURCE_COLS).copy()
    frame.insert(0, 'code', [canonical_ticker(t) for t in raw.index])
    frame.insert(1, 'date', date)
    if frame['code'].duplicated().any():
        fail('DUPLICATE_TICKER:' + date)
    for col in SCHEMA[2:]:
        frame[col] = pd.to_numeric(frame[col], errors='coerce')
        if frame[col].isna().any() or not pd.Series(frame[col].map(lambda x: bool(pd.notna(x) and x != float('inf') and x != float('-inf')))).all():
            fail('NON_NUMERIC_OR_NONFINITE:' + date + ':' + col)
        if (frame[col] < 0).any():
            fail('NEGATIVE_PRICE_OR_AMOUNT:' + date + ':' + col)
    if (frame['high'] < frame['low']).any():
        fail('HIGH_BELOW_LOW:' + date)
    # KRX fills suspended stocks with close>0 and O/H/L/V/amount=0.
    # Keep those AS-IS but never classify them as tradeable just by their close.
    tradable_proxy = (frame['open'] > 0) & (frame['high'] > 0) & (frame['low'] > 0) & (frame['close'] > 0) & (frame['volume'] > 0) & (frame['amount'] > 0)
    invalid_traded = tradable_proxy & ((frame['high'] < frame[['open','close']].max(axis=1)) | (frame['low'] > frame[['open','close']].min(axis=1)))
    if invalid_traded.any():
        fail('TRADED_BAR_INCONSISTENT_WITH_OHLC:' + date)
    flags = {
        'rows': int(len(frame)),
        'alphanumeric_code_rows': int(frame['code'].str.contains(r'[A-Z]').sum()),
        'noncommon_code_shape_rows': int((~frame['code'].str.fullmatch(COMMON_TICKER_PATTERN)).sum()),
        'positive_volume_and_amount_rows': int((frame['volume'].gt(0) & frame['amount'].gt(0)).sum()),
        'tradeable_ohlcv_proxy_rows': int(tradable_proxy.sum()),
        'zero_or_nontradeable_proxy_rows': int((~tradable_proxy).sum()),
        'zero_volume_rows': int(frame['volume'].eq(0).sum()),
        'zero_amount_rows': int(frame['amount'].eq(0).sum()),
    }
    return frame[SCHEMA].sort_values('code').reset_index(drop=True), flags


def encode_day(frame):
    data = frame.to_csv(index=False, lineterminator='\n').encode('utf-8')
    return gzip.compress(data, mtime=0)


def validate_stored_day(data, date, expected_rows):
    try:
        obj = pd.read_csv(io.BytesIO(gzip.decompress(data)), dtype={'code': str, 'date': str})
    except Exception as e:
        fail('BAD_STORED_GZIP_OR_CSV:' + date + ':' + type(e).__name__)
    if list(obj.columns) != SCHEMA or len(obj) != expected_rows or len(obj) == 0:
        fail('BAD_STORED_SCHEMA_OR_ROW_COUNT:' + date)
    if set(obj['date']) != {date} or not obj['code'].str.fullmatch(STORED_TICKER_PATTERN).all() or obj['code'].duplicated().any():
        fail('BAD_STORED_IDENTITIES:' + date)
    if any(obj[c].isna().any() for c in SCHEMA):
        fail('STORED_NAN:' + date)
    if (obj[['open','high','low','close','volume','amount']] < 0).any().any():
        fail('STORED_NEGATIVE:' + date)
    return obj


def load_calendar_and_indices(stock, start, end):
    charts = {}
    for label, code in [('KOSPI', '1001'), ('KOSDAQ', '2001')]:
        # name_display=False avoids historical ticker-name lookups, which can fail in pykrx.
        data = stock.get_index_ohlcv_by_date(start.replace('-', ''), end.replace('-', ''), code, name_display=False)
        if not isinstance(data, pd.DataFrame) or data.empty or '종가' not in data.columns:
            fail('OFFICIAL_INDEX_NOT_AVAILABLE:' + label)
        x = data.copy()
        x.index = pd.DatetimeIndex(pd.to_datetime(x.index)).normalize()
        x = x.loc[(x.index >= pd.Timestamp(start)) & (x.index <= pd.Timestamp(end))].sort_index()
        if x.empty or x.index.duplicated().any() or (pd.to_numeric(x['종가'],errors='coerce') <= 0).any():
            fail('OFFICIAL_INDEX_INVALID:' + label)
        charts[label] = x
    dates = [d.strftime('%Y-%m-%d') for d in charts['KOSPI'].index]
    if set(charts['KOSPI'].index) != set(charts['KOSDAQ'].index):
        fail('KOSPI_KOSDAQ_TRADING_CALENDAR_MISMATCH')
    if not dates or dates != sorted(set(dates)):
        fail('BAD_TRADING_CALENDAR')
    idx_rows = []
    for label, table in charts.items():
        for idx, row in table.iterrows():
            idx_rows.append({'date': idx.strftime('%Y-%m-%d'), 'index': label,
                             'index_code': '1001' if label == 'KOSPI' else '2001',
                             'close': float(row['종가'])})
    return dates, idx_rows


def encode_indices(records):
    lines = ['date,index,index_code,close']
    for r in sorted(records, key=lambda r:(r['date'],r['index'])):
        lines.append(f"{r['date']},{r['index']},{r['index_code']},{r['close']:.12g}")
    return ('\n'.join(lines)+'\n').encode('utf-8')


def fetch(stock, start, end, reference_csv, dest, shard, shards, retries=3, pause=1):
    reference = read_reference(reference_csv)
    dates, index_records = load_calendar_and_indices(stock, start, end)
    if set(dates) != {d for d in reference if start <= d <= end}:
        fail('OFFICIAL_CALENDAR_REFERENCE_MISMATCH')
    if not 0 <= shard < shards:
        fail('INVALID_SHARD')
    dest = pathlib.Path(dest) / f'shard_{shard:02d}'
    dest.mkdir(parents=True, exist_ok=True)
    idx_data = encode_indices(index_records)
    atomic_bytes(dest/'official_indices.csv', idx_data)
    cal_hash = h('\n'.join(dates).encode())
    meta_path = dest/f'manifest_shard_{shard:02d}.json'
    previous = json.loads(meta_path.read_text('utf-8')) if meta_path.exists() else {}
    if previous and (previous.get('calendar_sha256') != cal_hash or previous.get('shards') != shards or previous.get('index_sha256') != h(idx_data)):
        fail('RESUME_SOURCE_OR_CALENDAR_MISMATCH')
    done = {x['date']:x for x in previous.get('files',[])}
    meta = {'format':FORMAT,'status':'INCOMPLETE','source':'pykrx_stock.get_market_ohlcv_by_ticker(market=ALL)',
            'universe_filter':'NONE','historical_delisted_inclusion':'REQUIRES_EXTERNAL_VALIDATION',
            'price_completeness_authority':'H7_ORIGINAL_ALL_MARKET_ROWS',
            'start':start,'end':end,'shard':shard,'shards':shards,
            'official_days':len(dates),'calendar_sha256':cal_hash,'index_sha256':h(idx_data),
            'files':list(done.values())}
    assigned = dates[shard::shards]
    for n,date in enumerate(assigned,1):
        target = dest/(date.replace('-','')+'.csv.gz')
        if date in done and target.is_file() and h(target.read_bytes()) == done[date]['sha256']:
            validate_stored_day(target.read_bytes(),date,reference[date])
            continue
        if date in done: del done[date]
        err = None
        for attempt in range(retries):
            try:
                raw = stock.get_market_ohlcv_by_ticker(date.replace('-',''), market='ALL',alternative=False)
                df, flags = normalize_day(raw,date)
                if len(df) != reference[date]:
                    fail('H7_ANCHOR_COUNT_MISMATCH:' + date + ':new=' + str(len(df)) + ':old=' + str(reference[date]))
                encoded = encode_day(df)
                validate_stored_day(encoded,date,reference[date])
                atomic_bytes(target,encoded)
                done[date] = {'date':date,'file':target.name,'sha256':h(encoded),
                              'source_rows':len(raw),'stored_rows':len(df),**flags,
                              'retrieved_at_utc':datetime.now(timezone.utc).isoformat(timespec='seconds')}
                err = None
                break
            except Exception as exc:
                err = exc
                if attempt + 1 < retries:time.sleep(pause*(attempt+1))
        meta['files'] = sorted(done.values(), key=lambda x:x['date'])
        atomic_json(meta_path,meta)
        if err is not None:
            fail('SESSION_FETCH_INCOMPLETE:' + date + ':' + str(err))
        if n % 25 == 0:print(f'FETCH_PROGRESS shard={shard} {n}/{len(assigned)} date={date}',flush=True)
        if pause:time.sleep(pause)
    meta['files'] = sorted(done.values(),key=lambda x:x['date'])
    if len(done) != len(assigned):
        fail('SHARD_INCOMPLETE')
    meta['status'] = 'COMPLETE_PRICE_COLLECTION_ONLY'
    atomic_json(meta_path,meta)
    print(json.dumps({'status':meta['status'],'shard':shard,'sessions':len(assigned),'rows':sum(x['stored_rows'] for x in done.values()),'dir':str(dest)},ensure_ascii=False),flush=True)
    return meta


def verify(root, reference_csv, report):
    root = pathlib.Path(root)
    reference = read_reference(reference_csv)
    paths = sorted(root.glob('shard_*/manifest_shard_*.json'))
    if not paths:
        fail('NO_MANIFESTS')
    manifests = [json.loads(p.read_text('utf-8')) for p in paths]
    n = manifests[0]['shards']
    if len(manifests) != n or {m['shard'] for m in manifests} != set(range(n)):
        fail('SHARDS_MISSING_OR_DUPLICATED')
    expected_common = {(m['format'],m['start'],m['end'],m['calendar_sha256'],m['index_sha256'],m['official_days'],m['shards']) for m in manifests}
    if len(expected_common)!=1 or any(m['status']!='COMPLETE_PRICE_COLLECTION_ONLY' or m['universe_filter']!='NONE' for m in manifests):
        fail('MANIFEST_MISMATCH_OR_NOT_COMPLETE')
    start,end = manifests[0]['start'],manifests[0]['end']
    dates = sorted(d for d in reference if start <= d <= end)
    if h('\n'.join(dates).encode()) != manifests[0]['calendar_sha256'] or len(dates)!=manifests[0]['official_days']:
        fail('EXPECTED_OFFICIAL_CALENDAR_MISMATCH')
    records = {}
    totals = {'sessions':0,'full_market_price_rows':0,'zero_volume_rows':0,'zero_or_nontradeable_proxy_rows':0}
    for path,meta in zip(paths,manifests):
        expected_days = dates[meta['shard']::n]
        found = [e['date'] for e in meta['files']]
        if sorted(found)!=expected_days or len(found)!=len(set(found)):
            fail('SHARD_DAY_GAP:' + path.name)
        index_path = path.parent/'official_indices.csv'
        if not index_path.is_file() or h(index_path.read_bytes())!=meta['index_sha256']:
            fail('OFFICIAL_INDEX_HASH_MISMATCH:' + str(index_path))
        ix = pd.read_csv(index_path,dtype={'date':str,'index_code':str})
        if len(ix)!=2*len(dates) or set(ix['date'])!=set(dates) or set(ix['index'])!={'KOSPI','KOSDAQ'} or ix.duplicated(['date','index']).any():
            fail('OFFICIAL_INDEX_COVERAGE_OR_DUPLICATES')
        for e in meta['files']:
            d=e['date']; filename=path.parent/e['file']
            if not filename.is_file() or h(filename.read_bytes())!=e['sha256']:
                fail('MISSING_OR_BAD_SHA:' + d)
            obj=validate_stored_day(filename.read_bytes(),d,reference[d])
            if len(obj)!=e['source_rows'] or len(obj)!=e['stored_rows'] or e['rows']!=len(obj):
                fail('FILTERING_OR_ROW_COUNT_DRIFT:' + d)
            if d in records:fail('DUPLICATE_DAY:' + d)
            records[d]=e
            totals['sessions']+=1;totals['full_market_price_rows']+=len(obj)
            totals['zero_volume_rows']+=(obj['volume']==0).sum()
            totals['zero_or_nontradeable_proxy_rows']+=e['zero_or_nontradeable_proxy_rows']
    if set(records)!=set(dates):fail('FINAL_CALENDAR_MISSING')
    out={'status':'PASS_UNFILTERED_PRICE_COLLECTION_ONLY', 'whole_market_pit_strategy_ready':False,
         'reason':'PIT security master, delisted verification, security type, tradability, cap, and financials are NOT validated',
         'source':'KRX via pykrx; original H7 per-session KRX count reference',
         'reference_date_range':[start,end], 'calendar_sha256':manifests[0]['calendar_sha256'],
         'index_sha256':manifests[0]['index_sha256'], **{k:int(v) for k,v in totals.items()}}
    atomic_json(report,out)
    print(json.dumps(out,ensure_ascii=False),flush=True)
    return out


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=['fetch','verify'],required=True)
    p.add_argument('--reference',required=True,help='H7 historical per-date all-market row count anchor')
    p.add_argument('--root',required=True)
    p.add_argument('--start',default='2019-01-02')
    p.add_argument('--end',default='2026-10-12')
    p.add_argument('--shard',type=int,default=0)
    p.add_argument('--shards',type=int,default=8)
    p.add_argument('--retries',type=int,default=3)
    p.add_argument('--pause',type=float,default=1.0)
    p.add_argument('--report',default='full_market_price_collection_audit.json')
    a=p.parse_args(argv)
    if a.mode=='verify':
        verify(a.root,a.reference,a.report)
    else:
        from pykrx import stock
        fetch(stock,a.start,a.end,a.reference,a.root,a.shard,a.shards,a.retries,a.pause)


if __name__=='__main__':
    try:main()
    except Exception as e:
        print('FAIL_CLOSED:',e,file=sys.stderr,flush=True)
        raise SystemExit(2)
