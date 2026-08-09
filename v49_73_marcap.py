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


def resolve_authoritative_marcap(scanner, codes, requested_end_date: str, *, allow_fallback: bool | None = None) -> tuple[dict[str,float], dict[str,Any]]:
    """Resolve one immutable market-cap snapshot for the entire backtest run.

    Primary authority is pykrx market cap *as of the backtest end date* (with bounded
    trading-day lookback). This prevents current-listing market caps from leaking into
    historical reruns. A current-listing fallback is disabled by default and, if explicitly
    enabled, is visibly DEGRADED.
    """
    codes=sorted(set(_norm_code(c) for c in codes if _norm_code(c)))
    if allow_fallback is None:
        allow_fallback=str(os.environ.get('CLOSING_BET_V4973_ALLOW_MARCAP_FALLBACK','0') or '0').lower() in ('1','true','yes','y','on')
    try:
        base=pd.Timestamp(requested_end_date).normalize()
    except Exception:
        base=pd.Timestamp.now().normalize()
    lookback=int(getattr(scanner,'CLOSING_BET_V4948_REPAIR_LOOKBACK_DAYS',10) or 10)
    pykrx_stock=getattr(scanner,'pykrx_stock',None)
    last_err=''
    if pykrx_stock is not None:
        for off in range(max(0,lookback)+1):
            ds=(base-pd.Timedelta(days=off)).strftime('%Y%m%d')
            try:
                # pykrx has exposed this endpoint under both the legacy
                # get_market_cap_by_ticker name and the current get_market_cap name.
                # Prefer the legacy name when present to preserve existing runtime behavior,
                # but accept the current public API without changing authority semantics.
                cap_fn=getattr(pykrx_stock,'get_market_cap_by_ticker',None)
                api_name='get_market_cap_by_ticker'
                if not callable(cap_fn):
                    cap_fn=getattr(pykrx_stock,'get_market_cap',None)
                    api_name='get_market_cap'
                if not callable(cap_fn):
                    raise AttributeError('pykrx market-cap endpoint unavailable')
                try:
                    cap=cap_fn(ds,market='ALL')
                except TypeError:
                    cap=cap_fn(ds)
                if not isinstance(cap,pd.DataFrame) or cap.empty:
                    continue
                mcol=next((c for c in ['시가총액','Marcap','MarCap','MarketCap','market_cap'] if c in cap.columns),None)
                if not mcol:
                    last_err=f'market-cap column missing: {list(cap.columns)}'; continue
                idx=[_norm_code(x) for x in cap.index.astype(str).tolist()]
                vals=pd.to_numeric(cap[mcol],errors='coerce').fillna(0).astype(float).tolist()
                full={c:float(v) for c,v in zip(idx,vals) if c}
                result={c:float(full.get(c,0) or 0) for c in codes}
                positive=sum(1 for v in result.values() if float(v)>0)
                meta={
                    'status':'VALID','authority_quality':'AUTHORITATIVE_ASOF','source':'PYKRX_ASOF_MARKET_CAP',
                    'requested_end_date':str(requested_end_date),'asof_date':pd.Timestamp(ds).strftime('%Y-%m-%d'),
                    'lookback_days':off,'unit':'KRW','market':'ALL','source_rows':len(cap),'prepared_codes':len(codes),
                    'positive_codes':positive,'zero_codes':len(codes)-positive,'marcap_map_sha256':_numeric_map_sha(result),
                    'fallback_used':False,'api':api_name,'error':'',
                }
                return result,meta
            except Exception as exc:
                last_err=f'{type(exc).__name__}: {exc}'
    if not allow_fallback:
        raise RuntimeError(f'v49.73 authoritative as-of MARCAP unavailable; fallback disabled: {last_err or "no pykrx rows"}')
    current={_norm_code(k):float(v or 0) for k,v in dict(getattr(scanner,'MARCAP_MAP',{}) or {}).items() if _norm_code(k)}
    result={c:float(current.get(c,0) or 0) for c in codes}
    positive=sum(1 for v in result.values() if float(v)>0)
    meta={
        'status':'DEGRADED_FALLBACK','authority_quality':'DEGRADED_CURRENT_LISTING_FALLBACK','source':'CURRENT_LISTING_FALLBACK',
        'requested_end_date':str(requested_end_date),'asof_date':'CURRENT','lookback_days':None,'unit':'KRW_ASSUMED',
        'market':'KRX_LISTING','source_rows':len(current),'prepared_codes':len(codes),'positive_codes':positive,'zero_codes':len(codes)-positive,
        'marcap_map_sha256':_numeric_map_sha(result),'fallback_used':True,'error':last_err,
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
        'schema':'V49_73_PREPARED_SNAPSHOT_SCHEMA_1','snapshot_sha256':sha,'rows':int(len(x)),
        'mcap_or_min_krw':float(mcap_or_min),**dict(marcap_meta or {}),
    }


def compare_marcap_snapshots(current_path: str|Path|None, baseline_path: str|Path|None, *, mcap_or_min: float) -> tuple[dict[str,Any],pd.DataFrame]:
    result={'status':'NO_BASELINE','comparable':False,'changed_codes_n':0,'mcap_pass_to_fail_n':0,'mcap_fail_to_pass_n':0,
            'universe_pass_to_fail_n':0,'universe_fail_to_pass_n':0,'zero_to_positive_n':0,'positive_to_zero_n':0,
            'ratio_ge_10x_n':0,'ratio_le_0_1x_n':0,'median_ratio':None,'baseline_path':str(baseline_path or '')}
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
    # Same requested end date / MARCAP as-of date is required for deterministic parity.
    c_req=str(c.get('requested_end_date',pd.Series([''])).iloc[0] if len(c) else '')
    b_req=str(b.get('requested_end_date',pd.Series([''])).iloc[0] if len(b) else '')
    c_asof=str(c.get('marcap_asof_date',pd.Series([''])).iloc[0] if len(c) else '')
    b_asof=str(b.get('marcap_asof_date',pd.Series([''])).iloc[0] if len(b) else '')
    result.update({'current_requested_end':c_req,'baseline_requested_end':b_req,'current_asof':c_asof,'baseline_asof':b_asof})
    if c_req!=b_req or c_asof!=b_asof:
        result['status']='NOT_COMPARABLE_PERIOD'; return result,pd.DataFrame()
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
    m['delta_krw']=cmc-bmc
    m['abs_delta_krw']=m.delta_krw.abs()
    m['ratio']=np.where(bmc.gt(0)&cmc.gt(0),cmc/bmc,np.nan)
    m['changed']=~np.isclose(bmc,cmc,rtol=0,atol=0)
    m['zero_to_positive']=bmc.le(0)&cmc.gt(0); m['positive_to_zero']=bmc.gt(0)&cmc.le(0)
    m['mcap_pass_to_fail']=m.mcap_pass_baseline & ~m.mcap_pass_current
    m['mcap_fail_to_pass']=~m.mcap_pass_baseline & m.mcap_pass_current
    m['universe_pass_to_fail']=m.universe_pass_baseline & ~m.universe_pass_current
    m['universe_fail_to_pass']=~m.universe_pass_baseline & m.universe_pass_current
    rr=m.loc[m.ratio.notna(),'ratio']
    result.update({
        'status':'IDENTICAL' if int(m.changed.sum())==0 and (m['_merge']=='both').all() else 'CHANGED',
        'comparable':True,'codes_union_n':len(m),'changed_codes_n':int(m.changed.sum()),
        'missing_current_n':int((m['_merge']=='left_only').sum()),'new_current_n':int((m['_merge']=='right_only').sum()),
        'zero_to_positive_n':int(m.zero_to_positive.sum()),'positive_to_zero_n':int(m.positive_to_zero.sum()),
        'mcap_pass_to_fail_n':int(m.mcap_pass_to_fail.sum()),'mcap_fail_to_pass_n':int(m.mcap_fail_to_pass.sum()),
        'universe_pass_to_fail_n':int(m.universe_pass_to_fail.sum()),'universe_fail_to_pass_n':int(m.universe_fail_to_pass.sum()),
        'ratio_ge_10x_n':int(rr.ge(10).sum()),'ratio_le_0_1x_n':int(rr.le(.1).sum()),
        'median_ratio':float(rr.median()) if len(rr) else None,
    })
    detail=m[m.changed | (m['_merge']!='both') | m.mcap_pass_to_fail | m.mcap_fail_to_pass | m.universe_pass_to_fail | m.universe_fail_to_pass].copy()
    detail=detail.sort_values(['universe_pass_to_fail','universe_fail_to_pass','abs_delta_krw'],ascending=[False,False,False])
    return result,detail
