#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stock-Hunter ticker identity guard R2; read-only, no scanner imports or data edits.

Format validation is NOT proof of actual KRX listing or an authoritative price basis.
Optional point-in-time membership CSV has date,code,name columns and must be
independently certified as a dated, complete KRX universe before claiming authority.
"""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

VERSION = 'STOCKHUNTER_TICKER_IDENTITY_GUARD_R2_20261105'
CODE_FIELDS = ('code', '종목코드', 'Code', 'ticker', 'Ticker', 'stock_code')
LIST_FIELDS = ('quick_candidates', 'amount_top60', 'strategy_candidates', 'candidates',
               'ai_candidates', 'today_candidates', 'wyckoff_candidates', 'items', 'rows', 'stocks', 'top15')
PATTERN = re.compile(r'[0-9][0-9A-Z]{5}\Z')


def normalized_code(raw: Any):
    """Return (canonical_code, reason). Preserve KRX alphanumeric codes."""
    if raw is None or isinstance(raw, bool):
        return None, 'EMPTY_OR_BAD_TYPE'
    if isinstance(raw, float):
        # Floating-point codes silently drop leading zeros; refuse for strict identity.
        return None, 'FLOAT_CODE_LOSES_IDENTITY'
    val = str(raw).strip().upper()
    if not val or val in {'NONE', 'NULL', 'NAN'}:
        return None, 'EMPTY_OR_BAD_TYPE'
    if val.endswith('.0') and val[:-2].isdigit():
        val = val[:-2]  # backwards-compatible CSV artifact of integer 6-digit code
    for suffix in ('.KS', '.KQ', '.KRX'):
        if val.endswith(suffix):
            val = val[:-len(suffix)]
            break
    if len(val) == 7 and val.startswith('A') and PATTERN.fullmatch(val[1:]):
        val = val[1:]
    if val.isdigit() and 1 <= len(val) <= 5:
        val = val.zfill(6)
    if not PATTERN.fullmatch(val):
        return None, 'MALFORMED_CODE'
    if val == '000000':
        return None, 'PLACEHOLDER_000000'
    return val, ''


def _session(value):
    try:
        return dt.date.fromisoformat(str(value)[:10]).isoformat()
    except (ValueError, TypeError):
        return None


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _walk(obj, label='root'):
    """Yield candidate arrays without conflating different groups and their duplicates."""
    if isinstance(obj, dict):
        for key, val in obj.items():
            if key in LIST_FIELDS and isinstance(val, list):
                yield f'{label}.{key}', val
            elif key == 'groups' and isinstance(val, dict):
                for group, members in val.items():
                    if isinstance(members, list):
                        yield f'{label}.groups.{group}', members
            elif key in ('data', 'results') and isinstance(val, dict):
                yield from _walk(val, f'{label}.{key}')
    elif isinstance(obj, list):
        yield label, obj


def _membership(path):
    if path is None:
        return None, {'status': 'NOT_PROVIDED'}
    path = Path(path)
    with path.open(encoding='utf-8-sig', newline='') as fh:
        reader = csv.DictReader(fh)
        if not {'date', 'code'}.issubset(reader.fieldnames or []):
            raise ValueError('MEMBERSHIP_MISSING_DATE_CODE_COLUMNS')
        seen = {}
        for item in reader:
            when = _session(item['date'])
            code, err = normalized_code(item['code'])
            if not when or err:
                raise ValueError('MEMBERSHIP_BAD_CODE_OR_DATE')
            key = (when, code)
            if key in seen:
                raise ValueError('MEMBERSHIP_DUPLICATE_DATE_CODE')
            seen[key] = str(item.get('name', '')).strip()
        if not seen:
            raise ValueError('MEMBERSHIP_EMPTY')
    return seen, {'status': 'USER_SUPPLIED_MEMBERSHIP_UNCERTIFIED', 'sha256': _hash(path), 'rows': len(seen)}


def audit(paths, out, membership_path=None, require_membership=False):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    member, membership_meta = _membership(membership_path)
    issues, samples, counts = [], [], Counter()
    files = []
    for user_path in paths:
        path = Path(user_path)
        if not path.is_file():
            issues.append({'file': str(path), 'group': '', 'row': '', 'raw_code': '', 'canonical_code': '', 'reason': 'FILE_MISSING'})
            continue
        try:
            obj = json.loads(path.read_text(encoding='utf-8-sig'))
        except Exception:
            issues.append({'file': str(path), 'group': '', 'row': '', 'raw_code': '', 'canonical_code': '', 'reason': 'INVALID_JSON'})
            continue
        session = _session(obj.get('date')) if isinstance(obj, dict) else None
        if not session:
            issues.append({'file': str(path), 'group': '', 'row': '', 'raw_code': '', 'canonical_code': '', 'reason': 'SIGNAL_DATE_MISSING'})
        groups = list(_walk(obj))
        if not groups:
            issues.append({'file': str(path), 'group': '', 'row': '', 'raw_code': '', 'canonical_code': '', 'reason': 'NO_RECOGNIZED_CANDIDATE_GROUP'})
        files.append({'file': str(path), 'sha256': _hash(path), 'session': session, 'groups': len(groups)})
        for group, group_rows in groups:
            seen = {}
            for index, row in enumerate(group_rows, 1):
                counts['rows'] += 1
                if not isinstance(row, dict):
                    issues.append({'file': str(path), 'group': group, 'row': index, 'raw_code': '', 'canonical_code': '', 'reason': 'ROW_NOT_OBJECT'})
                    continue
                aliases = [(field, row[field]) for field in CODE_FIELDS if field in row]
                normalized = [(field, raw, *normalized_code(raw)) for field, raw in aliases]
                valid = {canon for _, _, canon, err in normalized if not err}
                bad = [err for _, _, canon, err in normalized if err]
                if not aliases:
                    reasons = ['MISSING_CODE']
                    canonical = None
                elif bad:
                    reasons = sorted(set(bad))
                    canonical = next(iter(valid), None)
                elif len(valid) != 1:
                    reasons = ['CONFLICTING_CODE_ALIASES']
                    canonical = None
                else:
                    canonical = next(iter(valid))
                    reasons = []
                if canonical and canonical in seen:
                    reasons.append('DUPLICATE_IN_GROUP')
                if canonical:
                    seen[canonical] = index
                # Do not pretend a shape-valid code is an actual listed instrument.
                if canonical and member is not None and session is not None:
                    if (session, canonical) not in member:
                        reasons.append('NOT_IN_SUPPLIED_KRX_MEMBERSHIP')
                    else:
                        counts['membership_hits'] += 1
                elif canonical and member is None:
                    counts['membership_unverified'] += 1
                elif canonical and session is None:
                    reasons.append('CANNOT_CHECK_MEMBERSHIP_NO_DATE')
                if reasons:
                    for reason in sorted(set(reasons)):
                        issues.append({'file': str(path), 'group': group, 'row': index,
                                       'raw_code': '|'.join(str(v) for _, v in aliases),
                                       'canonical_code': canonical or '', 'reason': reason})
                else:
                    counts['valid_format_rows'] += 1
                if len(samples) < 15 and reasons:
                    samples.append({'group': group, 'row': index, 'reasons': reasons})
    if require_membership and member is None:
        issues.append({'file': '', 'group': '', 'row': '', 'raw_code': '', 'canonical_code': '', 'reason': 'REQUIRED_KRX_MEMBERSHIP_NOT_PROVIDED'})
    flags = Counter(x['reason'] for x in issues)
    cols = ('file', 'group', 'row', 'raw_code', 'canonical_code', 'reason')
    with (out/'ticker_guard_violations.csv').open('w', encoding='utf-8-sig', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(issues)
    # Preserve all files unchanged; diagnostic outputs are separate.
    result = {
        'version': VERSION, 'scope': 'READ_ONLY_RESEARCH_ONLY', 'source_files_unchanged': True,
        'result': 'REJECT' if issues else 'PASS_FORMAT_ONLY_NOT_KRX_CERTIFIED',
        'checked_candidate_rows': counts['rows'], 'valid_format_rows': counts['valid_format_rows'],
        'issue_count': len(issues), 'issues_by_reason': dict(flags),
        'membership': membership_meta, 'membership_hits': counts['membership_hits'],
        'membership_unverified': counts['membership_unverified'], 'files': files,
        'samples': samples, 'note': 'KRX listing and price authority are NOT certified by code-shape validation.'
    }
    (out/'ticker_guard_summary.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', type=Path, action='append', required=True, help='Repeat for each candidate JSON')
    p.add_argument('--out', type=Path, default=Path('reports/ticker_guard_r2'))
    p.add_argument('--membership', type=Path, help='Optional point-in-time KRX membership CSV date,code,name')
    p.add_argument('--require-membership', action='store_true', help='Reject if authoritative membership not supplied')
    p.add_argument('--fail-on-invalid', action='store_true', help='Set exit code 1 when issues exist (isolated QA only)')
    args = p.parse_args()
    try:
        result = audit(args.input, args.out, args.membership, args.require_membership)
    except Exception as err:
        print(f'TICKER_GUARD_INPUT_ERROR: {type(err).__name__}: {err}', file=sys.stderr)
        return 2
    print(json.dumps({k: result[k] for k in ('result','checked_candidate_rows','issue_count','issues_by_reason','membership')},ensure_ascii=False,indent=2))
    return 1 if args.fail_on_invalid and result['issue_count'] else 0

if __name__ == '__main__':
    sys.exit(main())
