#!/usr/bin/env python3
"""R1-D40: two-session, two-product, read-only KRX-login futures canary.

This probe establishes provider response/schema validity ONLY. It must never
claim live tradeability, a rolled futures series, OOS, or predictive power.
Credentials are read by pykrx from KRX_ID and KRX_PW. Never echo credentials,
URLs, session cookies, or provider exception text. No orders or LIVE import.
"""
from __future__ import annotations
import argparse
import contextlib
import datetime as dt
import hashlib
import importlib.metadata
import io
import json
import os
import pathlib
import sys

DATES = ('20250923', '20250924')
PRODUCTS = {'KOSPI200':'KRDRVFUK2I', 'KOSDAQ150':'KRDRVFUKQI'}
REQUIRED = ('종목명', '시가', '고가', '저가', '종가', '거래량')
VERSION = 'R1D40_LOGIN_FUTURES_2DAY_CANARY_V1'


def _safe_json(path: pathlib.Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    payload = (json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + '\n')
    tmp.write_text(payload, encoding='utf-8')
    tmp.replace(path)


def _n(value):
    try:
        x = float(str(value).replace(',', '').strip())
        from math import isfinite
        if isfinite(x): return x
    except (TypeError, ValueError, OverflowError):
        pass
    return None


def validate_frame(df, date: str, product: str):
    """Validate actual returned contracts; retain ONLY safe numeric summaries."""
    import pandas as pd
    if not isinstance(df, pd.DataFrame) or df.empty:
        return {'status':'NO_DATA', 'reason':'EMPTY_OR_NOT_DATAFRAME', 'contracts_checked':0}
    if not set(REQUIRED).issubset(df.columns):
        return {'status':'SCHEMA_INVALID', 'reason':'REQUIRED_COLUMNS_ABSENT', 'contracts_checked':0}
    valid = []
    bad = 0
    for code, r in df.iterrows():
        name = str(r['종목명']).strip()
        # Distinguish actual FUTURE CONTRACT from calendar spread (SP) and options.
        # Provider series identity is separately archived as plain public metadata.
        normalized = name.replace(' ', '').upper()
        target = '코스피200' if product == 'KOSPI200' else '코스닥150'
        if target not in normalized or 'SP' in normalized or '옵션' in normalized:
            continue
        vals = {k:_n(r[k]) for k in ('시가','고가','저가','종가','거래량')}
        if any(vals[k] is None or vals[k]<=0 for k in ('시가','고가','저가','종가')) or vals['거래량'] is None or vals['거래량']<0:
            bad+=1;continue
        if vals['고가']+1e-7<max(vals['시가'],vals['종가']) or vals['저가']-1e-7>min(vals['시가'],vals['종가']):
            bad+=1;continue
        if 'F' not in normalized and '선물' not in normalized:
            continue
        # no raw/cookie/request headers or unique identifier used to infer expiration
        valid.append({'contract_code':str(code)[:48], 'contract_name':name[:120],
                      'close':vals['종가'],'volume_contracts':int(vals['거래량'])})
    valid.sort(key=lambda x:-x['volume_contracts'])
    return {'status':'DATA_VALID' if valid else 'CONTRACT_NOT_CONFIRMED',
            'date':date, 'product':product,'all_provider_rows':len(df),
            'contracts_checked':len(valid), 'invalid_price_rows':bad,
            'highest_volume_contracts':valid[:3]}


def run(out: pathlib.Path, provider=None, env=None):
    env=os.environ if env is None else env
    has_id=bool(str(env.get('KRX_ID','')).strip())
    has_pw=bool(str(env.get('KRX_PW','')).strip())
    report = {'version':VERSION,'research_only':True, 'live_changed':False,
              'orders_placed':0,'source':'PYKRX_KRX_LOGIN_ENV_2DAY_PROVIDER_PROBE',
              'dates':list(DATES),'products':PRODUCTS,
              'secret_presence':{'KRX_ID':has_id,'KRX_PW':has_pw},
              'secret_values_logged':False,'no_future_outcomes_used':True,
              'no_pit_or_oos_claim':True,'results':[], 'status':'NOT_RUN'}
    if not (has_id and has_pw):
        report['status']='CREDENTIALS_NOT_INJECTED'
        _safe_json(out/'CANARY_SAFE_AUDIT.json',report)
        return report
    if provider is None:
        try:
            from pykrx import stock
            provider=stock.get_future_ohlcv_by_ticker
            report['provider_version']=importlib.metadata.version('pykrx')
        except Exception:
            report['status']='PYKRX_FUTURES_API_UNAVAILABLE'
            _safe_json(out/'CANARY_SAFE_AUDIT.json',report)
            return report
    for date in DATES:
        for label, prod in PRODUCTS.items():
            entry={'date':date,'product':label,'product_code':prod,'status':'ERROR'}
            try:
                # pykrx dependencies can print arbitrary upstream errors. Suppress all.
                with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
                    df=provider(date, prod, alternative=False)
                entry.update(validate_frame(df,date,label))
            except Exception as exc:
                # exception NAME only; never str(exc) (can contain server/auth data)
                entry['status']='PROVIDER_EXCEPTION'
                entry['exception_class']=type(exc).__name__[:80]
            report['results'].append(entry)
    report['success_count']=sum(x['status']=='DATA_VALID' for x in report['results'])
    report['attempt_count']=len(DATES)*len(PRODUCTS)
    report['status']='PASS_2DAY_2PRODUCT_SOURCE_ONLY' if report['success_count']==report['attempt_count'] else 'FAIL_CLOSED_PROBE_INCOMPLETE'
    _safe_json(out/'CANARY_SAFE_AUDIT.json',report)
    return report


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--out',default='reports/r1d40-login-futures-canary')
    args=p.parse_args()
    result=run(pathlib.Path(args.out))
    # Only safe status, no values of secrets or upstream raw exception/response.
    print('R1D40_FUTURES_LOGIN_CANARY',result['status'],
          'verified',result.get('success_count',0),'of',result.get('attempt_count',4))
    return 0 if result['status']=='PASS_2DAY_2PRODUCT_SOURCE_ONLY' else 2

if __name__=='__main__':sys.exit(main())
