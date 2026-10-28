#!/usr/bin/env python3
"""RESEARCH_ONLY KRX authentication and HTTP-metadata diagnostics.
Never prints/persists secrets, cookies, request/response bodies, headers or URLs.
No trades, no rankings, no LIVE modifications, no bulk market requests.
"""
from __future__ import annotations
import argparse
import contextlib
from collections import Counter
import datetime as dt
import importlib.metadata
import io
import json
import os
from pathlib import Path
import re
import socket
import sys

VERSION = 'R1D27B_KRX_AUTH_AND_PROVIDER_METADATA_DIAGNOSTIC_V1'
CODE_RE = re.compile(r'\b(?:CD|ERR|E)\d{3,7}\b')
SECRETS = ('KRX_ID', 'KRX_PW')
TICKERS = ('005930', '000660')
DAYS = ('20250106', '20250110')


def provider_codes(text):
    """Whitelist-shaped error identifiers only. Never return arbitrary provider text."""
    return sorted(set(CODE_RE.findall(text or '')))[:8]


def secrets_status(env):
    # Boolean presence ONLY; no length, hash, prefix, suffix, repr or value.
    return {name: bool(env.get(name)) for name in SECRETS}


def configured_secret_pair(env):
    # Four GitHub Actions bool expression flags; no credential values are accessed here.
    flags = {name: str(env.get(name,'')).strip().lower() == 'true' for name in (
        'KRX_DATA_ID_DEFINED','KRX_DATA_PW_DEFINED','KRX_LEGACY_ID_DEFINED','KRX_LEGACY_PW_DEFINED')}
    if not any(name in env for name in flags):
        return {'source':'NOT_INSTRUMENTED','flags':flags,'mixed_pair_risk':False}
    if flags['KRX_DATA_ID_DEFINED'] and flags['KRX_DATA_PW_DEFINED']:
        source='KRX_DATA_ID_KRX_DATA_PW_PAIR'
    elif not flags['KRX_DATA_ID_DEFINED'] and not flags['KRX_DATA_PW_DEFINED'] and flags['KRX_LEGACY_ID_DEFINED'] and flags['KRX_LEGACY_PW_DEFINED']:
        source='KRX_ID_KRX_PW_LEGACY_PAIR'
    else:
        source='MIXED_OR_PARTIAL_CREDENTIAL_PAIR'
    return {'source':source, 'flags':flags, 'mixed_pair_risk':source=='MIXED_OR_PARTIAL_CREDENTIAL_PAIR'}


def classify(calls, http):
    if not all(calls.get('secret_presence', {}).values()):
        return 'SECRETS_NOT_INJECTED'
    if calls.get('configured_secret_pair',{}).get('mixed_pair_risk'):
        return 'MIXED_OR_PARTIAL_CREDENTIAL_PAIR'
    statuses = [x.get('http_status') for x in http if isinstance(x.get('http_status'), int)]
    if 401 in statuses or 403 in statuses:
        return 'HTTP_AUTH_OR_PERMISSION_REJECTED'
    if 429 in statuses:
        return 'HTTP_RATE_LIMITED'
    if any(x >= 500 for x in statuses):
        return 'HTTP_UPSTREAM_ERROR'
    if any(x.get('content_kind') == 'HTML' for x in http):
        return 'HTML_INSTEAD_OF_EXPECTED_KRX_DATA'
    if calls.get('complete_data_endpoints', 0) == calls.get('attempted_data_endpoints', 0) and calls.get('attempted_data_endpoints', 0) >= 2:
        return 'KRX_PROVIDER_DIAGNOSTIC_DATA_COMPLETE'
    if calls.get('attempted_data_endpoints', 0) and not http:
        return 'NO_HTTP_METADATA_CAPTURED_PROVIDER_OR_NETWORK_ERROR'
    return 'KRX_PROVIDER_BAD_OR_INCOMPLETE_RESPONSE'


def diagnostic(out, policy=None, fetch=None, env=None, requests_module=None):
    """Dependency injection is only used for safe unit tests."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    env = os.environ if env is None else env
    secret = secrets_status(env)
    data = dict(version=VERSION, phase='diagnostic', research_only=True,
                live_changed=False, orders_placed=0, strategy_ready=False,
                prospective_OOS=False, PIT_certified=False,
                secret_presence=secret, configured_secret_pair=configured_secret_pair(env), auth_not_proven_by_secret_presence=True,
                run_utc=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'),
                requested_codes=list(TICKERS), requested_window=list(DAYS),
                provider='PYKRX_SESSION_KRX_HISTORICAL_NOT_PIT',
                attempted_data_endpoints=0, complete_data_endpoints=0,
                http_records=[], calls=[], observed_provider_error_codes=[],
                diag_status='NOT_RUN')
    try:
        data['pykrx_version'] = importlib.metadata.version('pykrx')
    except importlib.metadata.PackageNotFoundError:
        data['pykrx_version'] = 'NOT_INSTALLED'

    if not all(secret.values()) or data['configured_secret_pair']['mixed_pair_risk']:
        data['diag_status'] = classify(data, [])
        _write(out, data)
        return data
    if fetch is None:
        from pykrx import stock
        fetch = {
            'NET_VALUE_KRW': stock.get_market_trading_value_by_date,
            'NET_VOLUME_SHARES': stock.get_market_trading_volume_by_date,
        }
    if requests_module is None:
        import requests as requests_module
    original = requests_module.sessions.Session.request
    http_records = []
    safe_output_codes = set()
    internal_log = io.StringIO()
    def wrapped(session, method, url, **kwargs):
        """Observe metadata only. NEVER persist URLs, request bodies or headers."""
        try:
            response = original(session, method, url, **kwargs)
        except Exception as e:
            http_records.append({'http_status': None, 'transport_exception':type(e).__name__})
            raise
        from urllib.parse import urlparse
        hostname = (urlparse(str(url)).hostname or '').lower()
        if hostname == 'krx.co.kr' or hostname.endswith('.krx.co.kr'):
            contenttype = str(response.headers.get('content-type', '')).lower()
            kind = 'JSON' if 'json' in contenttype else 'HTML' if 'html' in contenttype else 'OTHER'
            # Only structured network metadata. Never log provider payload, path or query.
            record = {'http_status': int(response.status_code), 'content_kind': kind}
            # Detect known vendor error-code patterns, but never copy any body text.
            # Some KRX endpoints return auth codes in an otherwise opaque response.
            if kind in ('JSON','HTML'):
                record['recognized_codes'] = provider_codes(response.text[:16000])
            http_records.append(record)
        return response

    try:
        requests_module.sessions.Session.request = wrapped
        socket.setdefaulttimeout(20)
        for typ in ('NET_VALUE_KRW', 'NET_VOLUME_SHARES'):
            for ticker in TICKERS:
                data['attempted_data_endpoints'] += 1
                entry = {'measure':typ,'ticker':ticker,'status':'FAILED','exception_type':'',
                         'row_count':0,'has_institution_foreign_columns':False}
                try:
                    # pykrx may write provider payload or errors to stdout; discard it entirely.
                    with contextlib.redirect_stdout(internal_log), contextlib.redirect_stderr(internal_log):
                        df = fetch[typ](DAYS[0], DAYS[1], ticker, on='순매수')
                    import pandas as pd
                    entry['row_count'] = len(df) if isinstance(df,pd.DataFrame) else 0
                    entry['has_institution_foreign_columns'] = bool(isinstance(df,pd.DataFrame) and {'기관합계','외국인합계'}.issubset(df.columns))
                    if entry['row_count'] and entry['has_institution_foreign_columns']:
                        entry['status'] = 'DATA_FRAME_VALID_NONEMPTY'
                        data['complete_data_endpoints'] += 1
                    else:
                        entry['status'] = 'DATA_FRAME_EMPTY_OR_SCHEMA_MISMATCH'
                except Exception as exc:
                    safe_output_codes.update(provider_codes(str(exc)))
                    entry['exception_type'] = type(exc).__name__
                    # No exception text: may include sensitive endpoint URLs or credentials.
                data['calls'].append(entry)
                safe_output_codes.update(provider_codes(internal_log.getvalue()))
                internal_log.seek(0)
                internal_log.truncate(0)
    finally:
        requests_module.sessions.Session.request = original
    # Keep HTTP metadata concise and free of session/host/request details.
    data['http_records'] = http_records[:30]
    data['observed_provider_error_codes'] = sorted(safe_output_codes | {c for x in http_records for c in x.get('recognized_codes', [])})
    data['http_status_counts'] = {str(k):int(v) for k,v in Counter(x.get('http_status') for x in http_records).items()}
    data['diag_status'] = classify(data, http_records)
    _write(out, data)
    return data


def _write(out, data):
    (Path(out)/'R1D27B_KRX_AUTH_DIAGNOSTIC.json').write_text(json.dumps(data,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out',default='reports/r1d27b/diagnostic')
    p.add_argument('--require-success',action='store_true')
    args=p.parse_args()
    result=diagnostic(args.out)
    safe=dict(version=result['version'],diag_status=result['diag_status'],secret_presence=result['secret_presence'],
              configured_secret_pair=result['configured_secret_pair'],
              pykrx_version=result['pykrx_version'],attempted=result['attempted_data_endpoints'],
              completed=result['complete_data_endpoints'],http_status_counts=result.get('http_status_counts',{}),
              observed_provider_error_codes=result['observed_provider_error_codes'])
    print('R1D27B_DIAGNOSTIC_METADATA_ONLY',json.dumps(safe,ensure_ascii=False))
    if args.require_success and result['diag_status']!='KRX_PROVIDER_DIAGNOSTIC_DATA_COMPLETE':
        print('::error::R1D27B_KRX_PROVIDER_OR_CREDENTIAL_DIAGNOSTIC_FAILED_CHECK_ARTIFACT')
        sys.exit(2)

if __name__=='__main__':main()
