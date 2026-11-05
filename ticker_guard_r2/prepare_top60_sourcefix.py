#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Produce a patched copy of Stock-Hunter's v90 TOP60 converter. NEVER mutate the source.

Research-only optional patch. Does NOT claim to repair original KRX data or affect LIVE.
Requires a manual decision before the patched copy is ever promoted.
"""
import argparse
import ast
import hashlib
import pathlib
import re

VERSION = 'TOP60_SOURCEFIX_RESEARCH_ONLY_R1_20261105'
SOURCE_SHA_FROM_R22 = 'c3654bede79baa5db1571541729da7e494d1f50d26dadc3a664e3c9c80e41b35'
OLD = '''        for i, (_, row) in enumerate(work.iterrows(), 1):
            item = _v90_row_to_amount_item(row, rank=i)
            if item:
                items.append(item)'''
NEW = '''        # Integrity gate: preserve actual ticker identity, never make fake TOP60 entries.
        # Index recovery is permitted only for an explicit six-character string ticker.
        for i, (row_index, row) in enumerate(work.iterrows(), 1):
            row_code = _v90_guard_first_code(row)
            if row_code is None:
                index_code = _v90_guard_code(row_index) if isinstance(row_index, str) else None
                if index_code:
                    row = row.copy()
                    row['code'] = index_code
            item = _v90_row_to_amount_item(row, rank=i)
            if not _v90_guard_valid_amount_item(item):
                try:
                    log_error('TOP60_SOURCE_INVALID: skipped rank=%s code=%r index=%r' % (
                        i, item.get('code') if isinstance(item, dict) else None, row_index))
                except Exception:
                    pass
                continue
            items.append(item)'''
HELPERS = '''def _v90_guard_code(value):
    """Format-only KRX ticker check; no KRX listing membership certification."""
    import re
    if value is None or isinstance(value, bool) or isinstance(value, float):
        return None
    s = str(value).strip().upper()
    if s.endswith('.0') and s[:-2].isdigit():
        s = s[:-2]
    for suffix in ('.KS', '.KQ', '.KRX'):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break
    if len(s) == 7 and s.startswith('A'):
        s = s[1:]
    if s.isdigit() and len(s) <= 5 and len(s) > 0:
        s = s.zfill(6)
    return s if s != '000000' and re.fullmatch(r'[0-9][0-9A-Z]{5}', s) else None


def _v90_guard_first_code(row):
    """Missing code can be recovered from an explicit code index, not bad/placeholder codes."""
    for key in ('code', '종목코드', 'Code', 'ticker', 'Ticker'):
        if key in row:
            val = row.get(key)
            if val is None or str(val).strip().lower() in ('', 'none', 'nan'):
                continue
            return str(val)
    return None


def _v90_guard_positive(v):
    import math
    try:
        z = float(str(v).replace(',', '').strip())
        return math.isfinite(z) and z > 0
    except (TypeError, ValueError):
        return False


def _v90_guard_valid_amount_item(item):
    """Fail-closed. Reject missing identity/name/price/actual amount. No substitutions."""
    if not isinstance(item, dict):
        return False
    a = _v90_guard_code(item.get('code'))
    b = _v90_guard_code(item.get('종목코드'))
    if not a or not b or a != b:
        return False
    name = str(item.get('name') or item.get('종목명') or '').strip()
    if not name or name == a or name == '000000':
        return False
    if not _v90_guard_positive(item.get('현재가')):
        return False
    if not _v90_guard_positive(item.get('거래대금')):
        return False
    return True


'''
MARKER = 'def _v90_df_to_amount_items(df, limit=None):'

def patch_text(src):
    if src.count(OLD) != 1:
        raise RuntimeError(f'Expected exact TOP60 source anchor once; found {src.count(OLD)}. No change.')
    if src.count(MARKER) != 1:
        raise RuntimeError(f'Expected one TOP60 conversion function; found {src.count(MARKER)}. No change.')
    if 'def _v90_guard_valid_amount_item(' in src:
        raise RuntimeError('Already patched. No change.')
    new = src.replace(OLD, NEW, 1)
    new = new.replace(MARKER, HELPERS + MARKER, 1)
    ast.parse(new)
    return new

def self_test():
    import pandas as pd
    original = '''def _v90_df_to_amount_items(df, limit=None):
    items = []
    try:
        if df is None or getattr(df, 'empty', True):
            return []
        n = int(limit or _v90_amount_top_n())
        work = df.copy()
        if 'Amount' in work.columns:
            try:
                work = coerce_numeric_columns(work, ['Amount'])
            except Exception:
                work['Amount'] = pd.to_numeric(work['Amount'], errors='coerce').fillna(0)
            work = work.sort_values('Amount', ascending=False, na_position='last')
        work = work.head(n)
        for i, (_, row) in enumerate(work.iterrows(), 1):
            item = _v90_row_to_amount_item(row, rank=i)
            if item:
                items.append(item)
    except Exception as e:
        try: log_error(str(e))
        except Exception: pass
    return items
'''
    patched = patch_text(original)
    ns = {'pd': pd, '_v90_amount_top_n': lambda:60,
          'coerce_numeric_columns': lambda df, cols:df,
          'log_error': lambda msg:None}
    def mocked_wrapper(row, rank):
        code = str(row.get('code') or row.get('종목코드') or '')
        if not code:
            # Represents the vulnerable existing fallback, not a verified listing.
            return {'code':'000000','종목코드':'000000','rank':rank}
        return {'code': code,'종목코드':code,'rank':rank,'name':row.get('Name'),
                '종목명':row.get('Name'),'현재가':row.get('Close'),
                '거래대금':row.get('Amount')}
    ns['_v90_row_to_amount_item'] = mocked_wrapper
    exec(compile(patched, '<test_source>', 'exec'), ns)
    fn = ns['_v90_df_to_amount_items']
    good = pd.DataFrame([{'Name':'삼성전자','Close':65000,'Amount':2500000000}],index=['005930'])
    assert [a['code'] for a in fn(good)] == ['005930'], 'valid code index must be preserved'
    explicit = pd.DataFrame([{'code':'0126Z0','Name':'코드유지','Close':1234,'Amount':52000000}])
    assert [a['code'] for a in fn(explicit)] == ['0126Z0'], 'alphanumeric identity must survive'
    bad = pd.DataFrame([{'Name':'원천없음','Close':0,'Amount':0}],index=[0])
    assert fn(bad) == [], 'RangeIndex must never create synthetic ticker'
    placeholder = pd.DataFrame([{'code':'000000','Name':'가짜','Close':100,'Amount':100000}])
    assert fn(placeholder) == [], 'placeholder must not pass'
    missing_price = pd.DataFrame([{'code':'005930','Name':'삼성전자','Close':None,'Amount':100000}])
    assert fn(missing_price) == [], 'missing price must fail closed'
    missing_amount = pd.DataFrame([{'code':'005930','Name':'삼성전자','Close':65000,'Amount':0}])
    assert fn(missing_amount) == [], 'missing amount must fail closed'
    assert original != patched
    print('SELF_TEST_PASS: valid-index, alphanumeric, RangeIndex, placeholder, no-price, no-amount (6 cases)')
    replay_saved_json_snapshot()


def replay_saved_json_snapshot(base=pathlib.Path('.')):
    """Read-only, real committed JSON *negative replay*, not an upstream DataFrame replay.

    Detects whether the already serialized TOP60 has enough identity, quote and
    actual-amount fields to be used. Data lost before JSON cannot be restored.
    No stock scanner import, no source rewrite, no network or market requests.
    """
    import json
    from collections import Counter
    file_map = {
        'amount_top60': base/'amount_top60.json',
        'candidate': base/'today_candidates.json',
        'full': base/'today_candidates_full.json',
    }
    for path in file_map.values():
        if not path.is_file():
            print('SAVED_JSON_REPLAY_SKIPPED missing=' + str(path))
            return None
    original_hashes = {k: hashlib.sha256(p.read_bytes()).hexdigest() for k,p in file_map.items()}
    try:
        docs = {k: json.loads(p.read_text(encoding='utf-8-sig')) for k,p in file_map.items()}
    except (ValueError, UnicodeDecodeError) as err:
        raise AssertionError('Saved JSON schema could not be read: '+repr(err)) from err
    dates = {str(doc.get('date', '')) for doc in docs.values()}
    assert len(dates)==1 and next(iter(dates)), 'Cross-file session date mismatch'
    amount = docs['amount_top60'].get('amount_top60')
    merged_amount = docs['candidate'].get('amount_top60')
    full_amount = docs['full'].get('groups',{}).get('거래대금TOP60')
    assert isinstance(amount, list) and isinstance(merged_amount, list), 'TOP60 group missing'
    assert isinstance(full_amount, list), 'TOP60 full-audit group missing'
    # Distinct file/group copies must be identical in the same snapshot.
    mirrored = amount == merged_amount == full_amount
    assert mirrored, 'TOP60 copies differ across same-day files'

    # The integrity filter to be integrated into the intended source fix;
    # execute only these source-fix helpers (no app imports).
    helper_ns = {}
    exec(compile(HELPERS, '<research_helpers>', 'exec'), helper_ns)
    valid_item = helper_ns['_v90_guard_valid_amount_item']
    normalize = helper_ns['_v90_guard_code']
    flags = Counter()
    valid = []
    for row in amount:
        if not isinstance(row, dict):
            flags['NOT_OBJECT'] += 1
            continue
        if normalize(row.get('code')) is None or normalize(row.get('종목코드')) is None:
            flags['INVALID_TICKER'] += 1
        if not str(row.get('name') or row.get('종목명') or '').strip():
            flags['MISSING_NAME'] += 1
        if not helper_ns['_v90_guard_positive'](row.get('현재가')):
            flags['MISSING_PRICE'] += 1
        if not helper_ns['_v90_guard_positive'](row.get('거래대금')):
            flags['MISSING_AMOUNT'] += 1
        if valid_item(row):
            valid.append(row)
    # The saved JSON is already after the lossy conversion. Reconstructed
    # rows are used ONLY to test fail-closed behavior, NOT to recover identity.
    import pandas as pd
    frame = pd.DataFrame([{
        'code': row.get('code'),
        'Name': row.get('name') or row.get('종목명'),
        'Close': row.get('현재가'),
        'Amount': row.get('거래대금'),
    } for row in amount if isinstance(row, dict)])
    patched_ns = {'pd': pd, '_v90_amount_top_n': lambda: 60,
                  'coerce_numeric_columns': lambda df, cols: df,
                  'log_error': lambda message: None}
    def replay_converter(row, rank):
        c = str(row.get('code') or '')
        return {'code': c, '종목코드': c, 'rank': rank,
                'name': row.get('Name'), '종목명': row.get('Name'),
                '현재가': row.get('Close'), '거래대금': row.get('Amount')}
    patched_ns['_v90_row_to_amount_item'] = replay_converter
    sample_function = """def _v90_df_to_amount_items(df, limit=None):\n    items = []\n    try:\n        if df is None or getattr(df, 'empty', True):\n            return []\n        n = int(limit or _v90_amount_top_n())\n        work = df.copy()\n        if 'Amount' in work.columns:\n            try:\n                work = coerce_numeric_columns(work, ['Amount'])\n            except Exception:\n                work['Amount'] = pd.to_numeric(work['Amount'], errors='coerce').fillna(0)\n            work = work.sort_values('Amount', ascending=False, na_position='last')\n        work = work.head(n)\n        for i, (_, row) in enumerate(work.iterrows(), 1):\n            item = _v90_row_to_amount_item(row, rank=i)\n            if item:\n                items.append(item)\n    except Exception as e:\n        try: log_error(str(e))\n        except Exception: pass\n    return items\n"""
    patched_fn = patch_text(sample_function)
    exec(compile(patched_fn, '<saved_json_replay>', 'exec'), patched_ns)
    replayed = patched_ns['_v90_df_to_amount_items'](frame)
    assert all(valid_item(row) for row in replayed), 'Fail-closed converter emitted invalid candidate'
    # The stock quick-candidate rows originate from separate strategy + TOP60 paths.
    quick = docs['candidate'].get('quick_candidates') or []
    strategy = docs['candidate'].get('strategy_candidates') or []
    invalid_quick = sum(not normalize(x.get('code')) for x in quick if isinstance(x, dict))
    strategy_codes = [normalize(x.get('code')) for x in strategy if isinstance(x,dict)]
    assert all(strategy_codes), 'Strategy source contains unvalidated codes; check separately'
    assert len(strategy_codes) == len(set(strategy_codes)), 'Strategy source has duplicate codes'
    quick_valid_set = {normalize(x.get('code')) for x in quick if isinstance(x,dict) and normalize(x.get('code'))}
    # Some strategy rows can be intentionally excluded from quick display (e.g., blocked).
    strategy_codes_excluded_from_quick = len(set(strategy_codes) - quick_valid_set)
    # Verify no saved JSON file was touched.
    after = {k:hashlib.sha256(path.read_bytes()).hexdigest() for k,path in file_map.items()}
    assert original_hashes==after, 'Source JSON unexpectedly changed'
    output = {
        'status': 'SAVED_JSON_NEGATIVE_REPLAY_ONLY_NOT_FULL_SOURCE_RECOVERY',
        'date': next(iter(dates)),
        'amount_top60_rows': len(amount),
        'usable_saved_amount_rows': len(valid),
        'invalid_saved_amount_rows': len(amount)-len(valid),
        'invalidity_flags': dict(flags),
        'patched_converter_accepted_rows': len(replayed),
        'strategy_candidates_unchanged_count': len(strategy),
        'strategy_codes_not_in_quick_display': strategy_codes_excluded_from_quick,
        'quick_candidates_rows': len(quick),
        'quick_candidates_invalid_codes': invalid_quick,
        'all_three_stored_top60_copies_identical': mirrored,
        'input_json_sha256': original_hashes,
        'root_cause_of_missing_input_identity': 'NOT_CERTIFIED',
        'krx_origin_and_live_recovery': 'NOT_CHECKED',
        'original_json_files_unchanged': True,
    }
    print('SAVED_JSON_REPLAY_RESULT ' + json.dumps(output, ensure_ascii=False, sort_keys=True))
    return output


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--self-test', action='store_true')
    p.add_argument('--source',type=pathlib.Path)
    p.add_argument('--out',type=pathlib.Path,help='WRITE PATCHED COPY elsewhere, never overwrite source')
    p.add_argument('--allow-different-sha',action='store_true', help='Override R2.2 source SHA pin after manually verifying diff')
    a=p.parse_args()
    if a.self_test:
        self_test()
        return
    if not a.source:
        p.error('--source is required unless --self-test')
    if a.out and a.source.resolve()==a.out.resolve():
        raise SystemExit('REFUSED: overwriting source is not allowed')
    b=a.source.read_bytes(); sha=hashlib.sha256(b).hexdigest()
    if sha!=SOURCE_SHA_FROM_R22 and not a.allow_different_sha:
        raise SystemExit('REFUSED: source SHA differs from inspected R2.2 run. Re-audit before patching. actual='+sha)
    orig=b.decode('utf-8-sig')
    patched=patch_text(orig)
    print('VALIDATED_PATCH_DRY_RUN',VERSION,'source_sha256='+sha,'source_bytes='+str(len(b)),'new_bytes='+str(len(patched.encode('utf-8'))))
    if a.out:
        a.out.parent.mkdir(parents=True,exist_ok=True)
        a.out.write_text(patched,encoding='utf-8')
        print('PATCHED_COPY_CREATED',a.out,'ORIGINAL_LEFT_UNCHANGED')
    else:
        print('No --out passed: original source and disk remain unchanged.')

if __name__=='__main__':main()
