from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _norm_code(v: Any) -> str:
    s=''.join(ch for ch in str(v or '') if ch.isdigit())
    return s.zfill(6) if s else ''


def _sha_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def _numeric_map_sha(mapping: dict[str, Any]) -> str:
    rows=[]
    for k in sorted(_norm_code(x) for x in mapping.keys() if _norm_code(x)):
        try:
            x=float(mapping.get(k,0) or 0)
            vv='nan' if not np.isfinite(x) else format(x,'.12g')
        except Exception:
            vv='nan'
        rows.append(f'{k}|{vv}')
    return _sha_text('\n'.join(rows))


def _extract_observed_dates(frame: Any, base: pd.Timestamp) -> list[pd.Timestamp]:
    if not isinstance(frame,pd.DataFrame) or frame.empty:
        return []
    vals=[]
    try:
        idx=pd.to_datetime(frame.index,errors='coerce')
        vals.extend(pd.Timestamp(x).normalize() for x in idx if pd.notna(x))
    except Exception:
        pass
    for col in ('Date','날짜','date','일자'):
        if col in frame.columns:
            try:
                z=pd.to_datetime(frame[col],errors='coerce')
                vals.extend(pd.Timestamp(x).normalize() for x in z if pd.notna(x))
            except Exception:
                pass
    return sorted(set(x for x in vals if x<=base))


def resolve_krx_trading_asof(scanner, requested_end_date: str, *, lookback_days: int | None = None) -> dict[str,Any]:
    """Resolve the *observed KRX trading date* at or before requested_end_date.

    v49.73 accepted a non-empty market-cap response on the requested calendar date and
    labeled that request date as the MARCAP as-of date. Some data APIs can return the
    prior trading day's values for a weekend/holiday request, which makes the label
    ambiguous. v49.74 first resolves a date from an OHLCV/index series whose returned
    index explicitly contains actual trading dates, then requests MARCAP on that exact date.

    Fail closed by default if no observed trading date can be proven.
    """
    try:
        base=pd.Timestamp(requested_end_date).normalize()
    except Exception as exc:
        raise RuntimeError(f'v49.74 invalid requested end date: {requested_end_date!r}: {exc}')
    if lookback_days is None:
        lookback_days=int(getattr(scanner,'CLOSING_BET_V4948_REPAIR_LOOKBACK_DAYS',10) or 10)
    lookback=max(1,int(lookback_days))
    start=(base-pd.Timedelta(days=lookback)).strftime('%Y%m%d')
    end=base.strftime('%Y%m%d')
    pykrx_stock=getattr(scanner,'pykrx_stock',None)
    if pykrx_stock is None:
        raise RuntimeError('v49.74 pykrx unavailable for KRX trading-day resolution')

    attempts=[]
    # Prefer market-index bars because they are not affected by a single stock suspension.
    index_fn=getattr(pykrx_stock,'get_index_ohlcv_by_date',None)
    if callable(index_fn):
        for ticker in ('1001','2001'):
            try:
                df=index_fn(start,end,ticker)
                dates=_extract_observed_dates(df,base)
                attempts.append({'method':'INDEX_OHLCV','probe':ticker,'rows':int(len(df)) if isinstance(df,pd.DataFrame) else 0,'error':''})
                if dates:
                    actual=max(dates)
                    return {
                        'status':'VALID','trading_asof_date':actual.strftime('%Y-%m-%d'),
                        'requested_end_date':base.strftime('%Y-%m-%d'),
                        'requested_is_trading_day':bool(actual==base),
                        'lookback_days_used':int((base-actual).days),
                        'calendar_source':'PYKRX_INDEX_OHLCV_OBSERVED_DATE','calendar_probe':ticker,
                        'calendar_window_start':pd.Timestamp(start).strftime('%Y-%m-%d'),
                        'calendar_window_end':base.strftime('%Y-%m-%d'),'attempts':attempts,
                    }
            except Exception as exc:
                attempts.append({'method':'INDEX_OHLCV','probe':ticker,'rows':0,'error':f'{type(exc).__name__}: {exc}'})

    stock_fn=getattr(pykrx_stock,'get_market_ohlcv_by_date',None)
    if callable(stock_fn):
        for code in ('005930','000660','005380','035420'):
            try:
                df=stock_fn(start,end,code)
                dates=_extract_observed_dates(df,base)
                attempts.append({'method':'STOCK_OHLCV','probe':code,'rows':int(len(df)) if isinstance(df,pd.DataFrame) else 0,'error':''})
                if dates:
                    actual=max(dates)
                    return {
                        'status':'VALID','trading_asof_date':actual.strftime('%Y-%m-%d'),
                        'requested_end_date':base.strftime('%Y-%m-%d'),
                        'requested_is_trading_day':bool(actual==base),
                        'lookback_days_used':int((base-actual).days),
                        'calendar_source':'PYKRX_STOCK_OHLCV_OBSERVED_DATE','calendar_probe':code,
                        'calendar_window_start':pd.Timestamp(start).strftime('%Y-%m-%d'),
                        'calendar_window_end':base.strftime('%Y-%m-%d'),'attempts':attempts,
                    }
            except Exception as exc:
                attempts.append({'method':'STOCK_OHLCV','probe':code,'rows':0,'error':f'{type(exc).__name__}: {exc}'})

    allow_unverified=str(os.environ.get('CLOSING_BET_V4974_ALLOW_UNVERIFIED_TRADING_DATE','0') or '0').lower() in ('1','true','yes','y','on')
    if allow_unverified:
        return {
            'status':'DEGRADED_UNVERIFIED','trading_asof_date':base.strftime('%Y-%m-%d'),
            'requested_end_date':base.strftime('%Y-%m-%d'),'requested_is_trading_day':None,
            'lookback_days_used':0,'calendar_source':'UNVERIFIED_REQUEST_DATE_FALLBACK','calendar_probe':'',
            'calendar_window_start':pd.Timestamp(start).strftime('%Y-%m-%d'),'calendar_window_end':base.strftime('%Y-%m-%d'),
            'attempts':attempts,
        }
    raise RuntimeError('v49.74 could not prove KRX trading as-of date from observed OHLCV dates; unverified fallback disabled')


def resolve_authoritative_marcap(scanner, codes, requested_end_date: str, *, allow_fallback: bool | None = None, trading_calendar: dict[str,Any] | None = None) -> tuple[dict[str,float], dict[str,Any]]:
    """Resolve one immutable, trading-day-verified market-cap snapshot for the entire run."""
    codes=sorted(set(_norm_code(c) for c in codes if _norm_code(c)))
    if allow_fallback is None:
        raw=os.environ.get('CLOSING_BET_V4974_ALLOW_MARCAP_FALLBACK',os.environ.get('CLOSING_BET_V4973_ALLOW_MARCAP_FALLBACK','0'))
        allow_fallback=str(raw or '0').lower() in ('1','true','yes','y','on')

    cal=dict(trading_calendar or resolve_krx_trading_asof(scanner,requested_end_date))
    actual=pd.Timestamp(cal.get('trading_asof_date')).normalize()
    ds=actual.strftime('%Y%m%d')
    pykrx_stock=getattr(scanner,'pykrx_stock',None)
    last_err=''
    if pykrx_stock is not None:
        endpoint_errors=[]
        endpoints=[
            ('get_market_cap_by_ticker',getattr(pykrx_stock,'get_market_cap_by_ticker',None)),
            ('get_market_cap',getattr(pykrx_stock,'get_market_cap',None)),
        ]
        for api_name,cap_fn in endpoints:
            if not callable(cap_fn):
                continue
            try:
                try:
                    cap=cap_fn(ds,market='ALL')
                except TypeError:
                    cap=cap_fn(ds)
                if not isinstance(cap,pd.DataFrame) or cap.empty:
                    raise RuntimeError(f'empty market-cap frame on verified trading date {actual:%Y-%m-%d}')
                mcol=next((c for c in ['시가총액','Marcap','MarCap','MarketCap','market_cap'] if c in cap.columns),None)
                if not mcol:
                    raise RuntimeError(f'market-cap column missing: {list(cap.columns)}')
                idx=[_norm_code(x) for x in cap.index.astype(str).tolist()]
                vals=pd.to_numeric(cap[mcol],errors='coerce').fillna(0).astype(float).tolist()
                full={c:float(v) for c,v in zip(idx,vals) if c}
                result={c:float(full.get(c,0) or 0) for c in codes}
                positive=sum(1 for v in result.values() if float(v)>0)
                _top_n=max(1,int(getattr(scanner,'TOP_N',100) or 100))
                _top_codes=[c for c,_ in sorted(full.items(),key=lambda kv:float(kv[1] or 0),reverse=True)[:_top_n]]
                meta={
                    'status':'VALID' if cal.get('status')=='VALID' else 'DEGRADED_TRADING_DATE',
                    'authority_quality':'AUTHORITATIVE_VERIFIED_TRADING_DAY' if cal.get('status')=='VALID' else 'DEGRADED_UNVERIFIED_TRADING_DAY',
                    'source':'PYKRX_TRADING_DAY_MARKET_CAP',
                    'requested_end_date':pd.Timestamp(requested_end_date).strftime('%Y-%m-%d'),
                    'asof_date':actual.strftime('%Y-%m-%d'),'trading_asof_date':actual.strftime('%Y-%m-%d'),
                    'requested_is_trading_day':cal.get('requested_is_trading_day'),
                    'calendar_source':cal.get('calendar_source',''),'calendar_probe':cal.get('calendar_probe',''),
                    'calendar_lookback_days':cal.get('lookback_days_used'),
                    'calendar_status':cal.get('status'),'calendar_attempts':cal.get('attempts',[]),
                    'market_cap_request_date':actual.strftime('%Y-%m-%d'),
                    'unit':'KRW','market':'ALL','source_rows':len(cap),'prepared_codes':len(codes),
                    'positive_codes':positive,'zero_codes':len(codes)-positive,
                    'marcap_map_sha256':_numeric_map_sha(result),'fallback_used':False,'api':api_name,'error':'',
                    'endpoint_errors':endpoint_errors,
                    'top_mcap_n':_top_n,'top_mcap_codes':_top_codes,
                }
                return result,meta
            except Exception as exc:
                endpoint_errors.append(f'{api_name}: {type(exc).__name__}: {exc}')
        last_err='; '.join(endpoint_errors) if endpoint_errors else 'pykrx market-cap endpoint unavailable'

    if not allow_fallback:
        raise RuntimeError(f'v49.74 authoritative trading-day MARCAP unavailable; fallback disabled: {last_err or "no pykrx rows"}')
    current={_norm_code(k):float(v or 0) for k,v in dict(getattr(scanner,'MARCAP_MAP',{}) or {}).items() if _norm_code(k)}
    result={c:float(current.get(c,0) or 0) for c in codes}
    positive=sum(1 for v in result.values() if float(v)>0)
    meta={
        'status':'DEGRADED_FALLBACK','authority_quality':'DEGRADED_CURRENT_LISTING_FALLBACK','source':'CURRENT_LISTING_FALLBACK',
        'requested_end_date':pd.Timestamp(requested_end_date).strftime('%Y-%m-%d'),'asof_date':'CURRENT','trading_asof_date':str(cal.get('trading_asof_date','')),
        'requested_is_trading_day':cal.get('requested_is_trading_day'),'calendar_source':cal.get('calendar_source',''),
        'calendar_probe':cal.get('calendar_probe',''),'calendar_lookback_days':cal.get('lookback_days_used'),'calendar_status':cal.get('status'),
        'unit':'KRW_ASSUMED','market':'KRX_LISTING','source_rows':len(current),'prepared_codes':len(codes),
        'positive_codes':positive,'zero_codes':len(codes)-positive,'marcap_map_sha256':_numeric_map_sha(result),
        'fallback_used':True,'error':last_err,
    }
    return result,meta


def build_prepared_snapshot(universe_doc: dict[str,Any], marcap_meta: dict[str,Any], *, mcap_or_min: float) -> pd.DataFrame:
    codes=[_norm_code(c) for c in universe_doc.get('codes',[]) if _norm_code(c)]
    names={_norm_code(k):str(v or '') for k,v in dict(universe_doc.get('names') or {}).items() if _norm_code(k)}
    mm={_norm_code(k):float(v or 0) for k,v in dict(universe_doc.get('marcap_map') or {}).items() if _norm_code(k)}
    im={_norm_code(k):str(v or '') for k,v in dict(universe_doc.get('index_map') or {}).items() if _norm_code(k)}
    market={_norm_code(k):str(v or '') for k,v in dict(universe_doc.get('market_map') or {}).items() if _norm_code(k)}
    sector={_norm_code(k):str(v or '') for k,v in dict(universe_doc.get('sector_map') or {}).items() if _norm_code(k)}
    top=set(_norm_code(c) for c in universe_doc.get('top_mcap_codes',[]) if _norm_code(c))
    rows=[]
    for c in codes:
        mc=float(mm.get(c,0) or 0); idx=im.get(c,'')
        idx_member=idx in ('코스피200','코스닥150')
        mcap_pass=mc>=float(mcap_or_min)
        rows.append({
            'code':c,'name':names.get(c,''),'marcap_krw':mc,'index_label':idx,
            'is_index_member':int(idx_member),'mcap_or_pass':int(mcap_pass),'universe_allowed':int(idx_member or mcap_pass),
            'top_mcap_flag':int(c in top),'market_label':market.get(c,''),'sector_label':sector.get(c,''),
            'marcap_source':str(marcap_meta.get('source','')),'marcap_asof_date':str(marcap_meta.get('asof_date','')),
            'marcap_unit':str(marcap_meta.get('unit','')),'requested_end_date':str(marcap_meta.get('requested_end_date','')),
            'requested_is_trading_day':marcap_meta.get('requested_is_trading_day'),
            'calendar_source':str(marcap_meta.get('calendar_source','')),
            'calendar_probe':str(marcap_meta.get('calendar_probe','')),
        })
    return pd.DataFrame(rows)


def snapshot_manifest(snapshot: pd.DataFrame, marcap_meta: dict[str,Any], *, mcap_or_min: float) -> dict[str,Any]:
    x=snapshot.copy() if isinstance(snapshot,pd.DataFrame) else pd.DataFrame()
    if x.empty:
        sha=_sha_text('EMPTY')
    else:
        cols=['code','marcap_krw','index_label','is_index_member','mcap_or_pass','universe_allowed','top_mcap_flag','market_label','sector_label']
        for c in cols:
            if c not in x: x[c]=''
        x=x.sort_values('code')
        lines=[]
        for r in x[cols].itertuples(index=False,name=None):
            vals=[]
            for i,v in enumerate(r):
                if cols[i]=='marcap_krw':
                    try: vals.append(format(float(v),'.12g'))
                    except Exception: vals.append('nan')
                else: vals.append(str(v if v is not None else ''))
            lines.append('|'.join(vals))
        sha=_sha_text('\n'.join(lines))
    return {
        'schema':'V49_74_PREPARED_SNAPSHOT_SCHEMA_2_TRADING_DAY_VERIFIED','snapshot_sha256':sha,'rows':int(len(x)),
        'mcap_or_min_krw':float(mcap_or_min),**dict(marcap_meta or {}),
    }


def compare_marcap_snapshots(current_path: str|Path|None, baseline_path: str|Path|None, *, mcap_or_min: float, require_same_asof: bool = True) -> tuple[dict[str,Any],pd.DataFrame]:
    result={
        'status':'NO_BASELINE','comparable':False,'changed_codes_n':0,'changed_codes_pct':0.0,
        'mcap_pass_to_fail_n':0,'mcap_fail_to_pass_n':0,'universe_pass_to_fail_n':0,'universe_fail_to_pass_n':0,
        'zero_to_positive_n':0,'positive_to_zero_n':0,'ratio_ge_10x_n':0,'ratio_le_0_1x_n':0,
        'ratio_ge_2x_n':0,'ratio_le_0_5x_n':0,'median_ratio':None,'median_abs_delta_krw':None,
        'baseline_path':str(baseline_path or ''),'top_changes':[],
    }
    cp=Path(current_path) if current_path else None; bp=Path(baseline_path) if baseline_path else None
    if cp is None or not cp.exists():
        result['status']='CURRENT_SNAPSHOT_MISSING'; return result,pd.DataFrame()
    if bp is None or not bp.exists(): return result,pd.DataFrame()
    try:
        c=pd.read_csv(cp,dtype={'code':str}); b=pd.read_csv(bp,dtype={'code':str})
    except Exception as exc:
        result['status']='BASELINE_READ_ERROR'; result['error']=f'{type(exc).__name__}: {exc}'; return result,pd.DataFrame()
    for x in (c,b):
        x['code']=x['code'].astype(str).str.replace(r'\.0$','',regex=True).str.zfill(6)
    c_req=str(c.get('requested_end_date',pd.Series([''])).iloc[0] if len(c) else '')
    b_req=str(b.get('requested_end_date',pd.Series([''])).iloc[0] if len(b) else '')
    c_asof=str(c.get('marcap_asof_date',pd.Series([''])).iloc[0] if len(c) else '')
    b_asof=str(b.get('marcap_asof_date',pd.Series([''])).iloc[0] if len(b) else '')
    result.update({'current_requested_end':c_req,'baseline_requested_end':b_req,'current_asof':c_asof,'baseline_asof':b_asof,
                   'requested_end_match':bool(c_req==b_req),'asof_label_match':bool(c_asof==b_asof),'require_same_asof':bool(require_same_asof)})
    if c_req!=b_req:
        result['status']='NOT_COMPARABLE_PERIOD'; return result,pd.DataFrame()
    if require_same_asof and c_asof!=b_asof:
        result['status']='NOT_COMPARABLE_ASOF'; return result,pd.DataFrame()
    cols=['code','name','marcap_krw','index_label','universe_allowed','mcap_or_pass']
    for x in (c,b):
        for col in cols:
            if col not in x: x[col]=''
    m=b[cols].merge(c[cols],on='code',how='outer',suffixes=('_baseline','_current'),indicator=True)
    for col in ['marcap_krw_baseline','marcap_krw_current']:
        m[col]=pd.to_numeric(m[col],errors='coerce').fillna(0.0)
    for side in ['baseline','current']:
        mc=m[f'marcap_krw_{side}']
        m[f'mcap_pass_{side}']=mc.ge(float(mcap_or_min))
        idx=m[f'index_label_{side}'].fillna('').astype(str).isin(['코스피200','코스닥150'])
        m[f'universe_pass_{side}']=idx | m[f'mcap_pass_{side}']
    bmc=m.marcap_krw_baseline; cmc=m.marcap_krw_current
    m['name']=m['name_current'].fillna('').astype(str)
    m.loc[m['name'].eq(''),'name']=m.loc[m['name'].eq(''),'name_baseline'].fillna('').astype(str)
    m['delta_krw']=cmc-bmc; m['abs_delta_krw']=m.delta_krw.abs()
    m['ratio']=np.where(bmc.gt(0)&cmc.gt(0),cmc/bmc,np.nan)
    m['ratio_log10_abs']=np.where(pd.Series(m['ratio']).gt(0),np.abs(np.log10(m['ratio'])),np.inf)
    m['changed']=~np.isclose(bmc,cmc,rtol=0,atol=0)
    m['zero_to_positive']=bmc.le(0)&cmc.gt(0); m['positive_to_zero']=bmc.gt(0)&cmc.le(0)
    m['mcap_pass_to_fail']=m.mcap_pass_baseline & ~m.mcap_pass_current
    m['mcap_fail_to_pass']=~m.mcap_pass_baseline & m.mcap_pass_current
    m['universe_pass_to_fail']=m.universe_pass_baseline & ~m.universe_pass_current
    m['universe_fail_to_pass']=~m.universe_pass_baseline & m.universe_pass_current
    rr=m.loc[m.ratio.notna(),'ratio']
    changed_n=int(m.changed.sum())
    result.update({
        'status':('IDENTICAL_LABEL_MISMATCH' if changed_n==0 and (m['_merge']=='both').all() and c_asof!=b_asof else ('IDENTICAL' if changed_n==0 and (m['_merge']=='both').all() else 'CHANGED')),
        'comparable':True,'codes_union_n':len(m),'changed_codes_n':changed_n,
        'changed_codes_pct':float(changed_n/max(1,len(m))*100.0),
        'missing_current_n':int((m['_merge']=='left_only').sum()),'new_current_n':int((m['_merge']=='right_only').sum()),
        'zero_to_positive_n':int(m.zero_to_positive.sum()),'positive_to_zero_n':int(m.positive_to_zero.sum()),
        'mcap_pass_to_fail_n':int(m.mcap_pass_to_fail.sum()),'mcap_fail_to_pass_n':int(m.mcap_fail_to_pass.sum()),
        'universe_pass_to_fail_n':int(m.universe_pass_to_fail.sum()),'universe_fail_to_pass_n':int(m.universe_fail_to_pass.sum()),
        'ratio_ge_10x_n':int(rr.ge(10).sum()),'ratio_le_0_1x_n':int(rr.le(.1).sum()),
        'ratio_ge_2x_n':int(rr.ge(2).sum()),'ratio_le_0_5x_n':int(rr.le(.5).sum()),
        'median_ratio':float(rr.median()) if len(rr) else None,
        'median_abs_delta_krw':float(m.loc[m.changed,'abs_delta_krw'].median()) if changed_n else 0.0,
    })
    detail=m[m.changed | (m['_merge']!='both') | m.mcap_pass_to_fail | m.mcap_fail_to_pass | m.universe_pass_to_fail | m.universe_fail_to_pass].copy()
    if not detail.empty:
        detail['eligibility_cross']=detail[['universe_pass_to_fail','universe_fail_to_pass','mcap_pass_to_fail','mcap_fail_to_pass']].any(axis=1)
        detail=detail.sort_values(['eligibility_cross','ratio_log10_abs','abs_delta_krw'],ascending=[False,False,False])
        top=[]
        for r in detail.head(10).itertuples(index=False):
            top.append({
                'code':str(getattr(r,'code','')).zfill(6),'name':str(getattr(r,'name','')),
                'baseline_marcap_krw':float(getattr(r,'marcap_krw_baseline',0) or 0),
                'current_marcap_krw':float(getattr(r,'marcap_krw_current',0) or 0),
                'ratio':None if not np.isfinite(float(getattr(r,'ratio',np.nan))) else float(getattr(r,'ratio')),
                'mcap_pass_to_fail':bool(getattr(r,'mcap_pass_to_fail',False)),
                'mcap_fail_to_pass':bool(getattr(r,'mcap_fail_to_pass',False)),
                'universe_pass_to_fail':bool(getattr(r,'universe_pass_to_fail',False)),
                'universe_fail_to_pass':bool(getattr(r,'universe_fail_to_pass',False)),
            })
        result['top_changes']=top
    return result,detail
