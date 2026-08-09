from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _sha_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def canonical_map_sha(mapping: dict[str, Any], *, numeric: bool = False) -> str:
    rows=[]
    for k in sorted(str(x) for x in mapping.keys()):
        v=mapping.get(k)
        if numeric:
            try:
                x=float(v)
                vv='nan' if not np.isfinite(x) else format(x,'.12g')
            except Exception:
                vv='nan'
        else:
            vv=str(v if v is not None else '').strip()
        rows.append(f'{k}|{vv}')
    return _sha_text('\n'.join(rows))


def canonical_list_sha(values) -> str:
    return _sha_text('\n'.join(sorted(set(str(x).strip() for x in values if str(x).strip()))))


def prepare_component_fingerprints(doc: dict[str, Any]) -> dict[str, Any]:
    codes=[str(c).zfill(6) for c in doc.get('codes',[]) if str(c).strip()]
    names={str(k).zfill(6):str(v or '') for k,v in dict(doc.get('names') or {}).items()}
    index_map={str(k).zfill(6):str(v or '') for k,v in dict(doc.get('index_map') or {}).items()}
    marcap_map={str(k).zfill(6):v for k,v in dict(doc.get('marcap_map') or {}).items()}
    market_map={str(k).zfill(6):str(v or '') for k,v in dict(doc.get('market_map') or {}).items()}
    sector_map={str(k).zfill(6):str(v or '') for k,v in dict(doc.get('sector_map') or {}).items()}
    top=[str(c).zfill(6) for c in doc.get('top_mcap_codes',[]) if str(c).strip()]
    fps={
        'codes_sha256':canonical_list_sha(codes),
        'names_sha256':canonical_map_sha(names),
        'index_map_sha256':canonical_map_sha(index_map),
        'marcap_map_sha256':canonical_map_sha(marcap_map,numeric=True),
        'top_mcap_sha256':canonical_list_sha(top),
        'market_map_sha256':canonical_map_sha(market_map),
        'sector_map_sha256':canonical_map_sha(sector_map),
    }
    fps['prepared_authority_sha256']=_sha_text('\n'.join(f'{k}|{fps[k]}' for k in sorted(fps)))
    fps['codes_n']=len(codes)
    fps['index_map_nonempty_n']=sum(bool(v) for v in index_map.values())
    fps['marcap_positive_n']=sum((float(v or 0)>0) for v in marcap_map.values())
    fps['market_map_nonempty_n']=sum(bool(v) for v in market_map.values())
    fps['sector_map_nonempty_n']=sum(bool(v) for v in sector_map.values())
    fps['top_mcap_n']=len(set(top))
    return fps


def _norm_num(v: Any) -> str:
    try:
        x=float(v)
        if not np.isfinite(x): return ''
        return format(x,'.12g')
    except Exception:
        return ''


def history_authority_row(code: str, df: pd.DataFrame, requested_start: str = '', requested_end: str = '', *, load_meta: dict[str, Any] | None = None, name: str = '') -> dict[str, Any]:
    code=str(code).zfill(6)
    lm=dict(load_meta or {})
    load_status=str(lm.get('status') or '').strip().upper() or ('LOADED' if isinstance(df,pd.DataFrame) and not df.empty else 'NO_DATA')
    load_reason=str(lm.get('reason') or '').strip()
    observed_rows=int(lm.get('observed_rows',0) or 0)
    if df is None or not isinstance(df,pd.DataFrame) or df.empty:
        return {'code':code,'name':str(name or lm.get('name') or ''),'history_rows':0,'first_date':'','last_date':'','amount_nonnull_rows':0,'amount_positive_rows':0,
                'amount_coverage_pct':0.0,'amount_positive_pct':0.0,'amount_source':'','data_sources':'','history_sha256':_sha_text('EMPTY'),
                'requested_start':str(requested_start or ''),'requested_end':str(requested_end or ''),
                'load_status':load_status,'load_reason':load_reason,'observed_rows_before_min':observed_rows,
                'status':'NO_DATA'}
    # Normalize the resident cache to a unique positional index before row-level hashing.
    # This avoids ambiguous .loc lookups if an upstream source supplied duplicate/non-unique indices.
    x=df.copy().reset_index(drop=True)
    dates=pd.to_datetime(x.get('Date',pd.Series(pd.NaT,index=x.index)),errors='coerce').dt.normalize()
    cols={c:pd.to_numeric(x.get(c,pd.Series(np.nan,index=x.index)),errors='coerce') for c in ['Open','High','Low','Close','Volume']}
    if 'Amount' in x.columns:
        amount=pd.to_numeric(x['Amount'],errors='coerce')
        amount_source='Amount'
    else:
        amount=cols['Close']*cols['Volume']
        amount_source='Close*Volume'
    src=x.get('data_source',pd.Series('unknown',index=x.index)).fillna('unknown').astype(str).str.strip()
    valid=dates.notna()
    # Fingerprint the exact history resident in the shard cache (including warmup/future rows used by predicates/evaluation).
    h=hashlib.sha256()
    rows=0
    for i in x.index[valid]:
        rec=[dates.loc[i].strftime('%Y-%m-%d')]
        rec += [_norm_num(cols[c].loc[i]) for c in ['Open','High','Low','Close','Volume']]
        rec += [_norm_num(amount.loc[i]),str(src.loc[i])]
        h.update(('|'.join(rec)+'\n').encode('utf-8')); rows+=1
    amount_nonnull=int(amount[valid].notna().sum())
    amount_positive=int(amount[valid].gt(0).sum())
    sources=','.join(sorted(set(src[valid].replace('', 'unknown').tolist())))
    return {
        'code':code,'name':str(name or lm.get('name') or ''),'history_rows':rows,
        'first_date':dates[valid].min().strftime('%Y-%m-%d') if valid.any() else '',
        'last_date':dates[valid].max().strftime('%Y-%m-%d') if valid.any() else '',
        'amount_nonnull_rows':amount_nonnull,'amount_positive_rows':amount_positive,
        'amount_coverage_pct':(amount_nonnull/rows*100.0 if rows else 0.0),
        'amount_positive_pct':(amount_positive/rows*100.0 if rows else 0.0),
        'amount_source':amount_source,'data_sources':sources,'history_sha256':h.hexdigest(),
        'requested_start':str(requested_start or ''),'requested_end':str(requested_end or ''),
        'load_status':load_status,'load_reason':load_reason,'observed_rows_before_min':observed_rows or rows,
        'status':'VALID' if rows else 'NO_DATA',
    }


def aggregate_history_fingerprint(history: pd.DataFrame) -> dict[str, Any]:
    if history is None or history.empty:
        return {'history_codes':0,'history_rows':0,'history_global_sha256':_sha_text('EMPTY'),'amount_coverage_pct':0.0,'source_set':''}
    h=history.copy()
    h['code']=h.get('code',pd.Series('',index=h.index)).astype(str).str.replace(r'\.0$','',regex=True).str.zfill(6)
    h=h.sort_values('code').drop_duplicates('code',keep='last')
    text='\n'.join(f"{r.code}|{r.history_sha256}|{int(r.history_rows)}|{r.first_date}|{r.last_date}|{r.data_sources}|{r.amount_source}" for r in h.itertuples())
    rows=int(pd.to_numeric(h.get('history_rows',0),errors='coerce').fillna(0).sum())
    amount_n=int(pd.to_numeric(h.get('amount_nonnull_rows',0),errors='coerce').fillna(0).sum())
    src=set()
    for v in h.get('data_sources',pd.Series(dtype=str)).fillna('').astype(str):
        src.update(x for x in v.split(',') if x)
    return {'history_codes':len(h),'history_rows':rows,'history_global_sha256':_sha_text(text),
            'amount_coverage_pct':(amount_n/rows*100.0 if rows else 0.0),'source_set':','.join(sorted(src))}


def population_fingerprint(raw: pd.DataFrame, funnel: pd.DataFrame | None = None) -> dict[str, Any]:
    if raw is None or raw.empty:
        raw_sha=_sha_text('EMPTY'); raw_n=0
    else:
        r=raw.copy()
        r['code']=r.get('code',pd.Series('',index=r.index)).astype(str).str.replace(r'\.0$','',regex=True).str.zfill(6)
        r['signal_date']=pd.to_datetime(r.get('signal_date'),errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
        r['mode']=r.get('mode',pd.Series('',index=r.index)).astype(str)
        score=pd.to_numeric(r.get('score',0),errors='coerce').fillna(0)
        keys=[f'{d}|{c}|{m}|{_norm_num(s)}' for d,c,m,s in zip(r.signal_date,r.code,r['mode'],score)]
        raw_sha=_sha_text('\n'.join(sorted(keys))); raw_n=len(r)
    gate_counts={}
    if funnel is not None and not funnel.empty:
        for rr in funnel.itertuples():
            mode=str(getattr(rr,'strategy',''))
            if mode:
                gate_counts[mode]={k:int(getattr(rr,k,0) or 0) for k in ['eligible_dates','gate_admitted','predicate_called','predicate_hit_dates','raw_emitted'] if hasattr(rr,k)}
    gate_sha=_sha_text(json.dumps(gate_counts,sort_keys=True,separators=(',',':')))
    return {'raw_population_rows':raw_n,'raw_population_sha256':raw_sha,'funnel_population_sha256':gate_sha,'gate_counts':gate_counts}



def assess_history_completeness(history: pd.DataFrame, universe_codes, names: dict[str,Any] | None = None) -> dict[str,Any]:
    """Separate data completeness from reproducibility.

    DEGRADED gaps are fingerprinted and baseline-eligible. Structural corruption or explicit
    fetch/timeout failures remain fail-closed and are not accepted as a healthy baseline.
    """
    names={str(k).zfill(6):str(v or '') for k,v in dict(names or {}).items()}
    universe=sorted(set(str(c).zfill(6) for c in universe_codes if str(c).strip()))
    h=history.copy() if isinstance(history,pd.DataFrame) else pd.DataFrame()
    if h.empty:
        h=pd.DataFrame(columns=['code','history_rows','status','load_status','load_reason','name'])
    h['code']=h.get('code',pd.Series('',index=h.index)).astype(str).str.replace(r'\.0$','',regex=True).str.zfill(6)
    counts=h['code'].value_counts() if not h.empty else pd.Series(dtype=int)
    duplicate_codes=sorted(counts[counts.gt(1)].index.tolist())
    hist_codes=sorted(set(h.code.tolist()))
    missing_codes=sorted(set(universe)-set(hist_codes))
    extra_codes=sorted(set(hist_codes)-set(universe))
    rows=pd.to_numeric(h.get('history_rows',pd.Series(0,index=h.index)),errors='coerce').fillna(0)
    zero=h.loc[rows.le(0)].copy() if not h.empty else h.copy()
    if 'load_status' not in zero: zero['load_status']='NO_DATA'
    if 'load_reason' not in zero: zero['load_reason']=''
    if 'name' not in zero: zero['name']=''
    zero['load_status']=zero['load_status'].fillna('NO_DATA').astype(str).str.upper().str.strip().replace('', 'NO_DATA')
    hard_fetch_status={'FETCH_ERROR','TIMEOUT','NO_RESULT','LOAD_EXCEPTION'}
    hard_fetch=zero[zero.load_status.isin(hard_fetch_status)].copy()
    degraded=zero[~zero.load_status.isin(hard_fetch_status)].copy()
    issues=[]
    for code in duplicate_codes:
        issues.append({'code':code,'name':names.get(code,''),'issue_type':'DUPLICATE_HISTORY_AUTHORITY','severity':'INVALID','detail':f'rows={int(counts.get(code,0))}'})
    for code in missing_codes:
        issues.append({'code':code,'name':names.get(code,''),'issue_type':'MISSING_HISTORY_AUTHORITY_ROW','severity':'INVALID','detail':'prepared code absent from merged history authority'})
    for code in extra_codes:
        issues.append({'code':code,'name':names.get(code,''),'issue_type':'EXTRA_HISTORY_AUTHORITY_ROW','severity':'INVALID','detail':'history authority code absent from prepared universe'})
    for _,r in zero.sort_values('code').iterrows():
        code=str(r.get('code','')).zfill(6); ls=str(r.get('load_status','NO_DATA')).upper()
        sev='INVALID' if ls in hard_fetch_status else 'DEGRADED'
        issues.append({'code':code,'name':str(r.get('name') or names.get(code,'') or ''),'issue_type':ls,'severity':sev,
                       'detail':str(r.get('load_reason') or ''),'observed_rows':int(pd.to_numeric(pd.Series([r.get('observed_rows_before_min',0)]),errors='coerce').fillna(0).iloc[0])})
    if duplicate_codes or missing_codes or extra_codes:
        status='INVALID_STRUCTURE'
    elif len(hard_fetch):
        status='INVALID_FETCH_FAILURE'
    elif len(degraded):
        # A clean insufficient-history case is an expected listing/history gap. Plain NO_DATA is
        # kept semantically separate because absence without an exception is not proof that the gap is expected.
        _dset=set(degraded.load_status.astype(str).str.upper().tolist())
        if _dset.issubset({'INSUFFICIENT_HISTORY'}): status='DEGRADED_EXPECTED_GAP'
        elif _dset.issubset({'NO_DATA'}): status='DEGRADED_NO_DATA'
        else: status='DEGRADED_MIXED_GAP'
    else:
        status='FULL_VALID'
    issue_rows=pd.DataFrame(issues)
    issue_text='\n'.join(json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':')) for x in issues)
    return {
        'status':status,
        'hard_invalid':bool(status.startswith('INVALID')),
        'baseline_eligible':not bool(status.startswith('INVALID')),
        'universe_codes':len(universe),'history_authority_rows':len(h),'history_unique_codes':len(hist_codes),
        'duplicate_codes_n':len(duplicate_codes),'missing_codes_n':len(missing_codes),'extra_codes_n':len(extra_codes),
        'zero_history_codes_n':len(zero),'degraded_gap_codes_n':len(degraded),'fetch_failure_codes_n':len(hard_fetch),
        'duplicate_code_sample':duplicate_codes[:20],'missing_code_sample':missing_codes[:20],'extra_code_sample':extra_codes[:20],
        'zero_history_code_sample':zero.code.astype(str).tolist()[:20] if not zero.empty else [],
        'issue_sha256':_sha_text(issue_text if issue_text else 'NO_ISSUES'),
        'issues':issues,
        'issue_rows':issue_rows,
    }


def resolve_authority_status(data_completeness_status: str, baseline_status: str) -> dict[str,Any]:
    comp=str(data_completeness_status or 'INVALID_STRUCTURE').upper()
    b=str(baseline_status or 'NO_BASELINE').upper()
    if b=='REPRODUCIBILITY_MISMATCH': repro='REPRODUCIBILITY_MISMATCH'
    elif b=='REPRODUCIBLE_MATCH': repro='REPRODUCIBLE_MATCH'
    elif b=='NO_BASELINE': repro='BASELINE_ESTABLISHED'
    elif b=='NOT_COMPARABLE_PERIOD': repro='VALID_NEW_PERIOD'
    else: repro=b or 'BASELINE_UNAVAILABLE'
    hard_repro=repro in {'REPRODUCIBILITY_MISMATCH','BASELINE_READ_ERROR'}
    hard_complete=comp.startswith('INVALID')
    baseline_eligible=not hard_repro and not hard_complete
    if hard_repro:
        overall='INVALID_REPRODUCIBILITY_MISMATCH' if repro=='REPRODUCIBILITY_MISMATCH' else 'INVALID_BASELINE_READ_ERROR'
    elif hard_complete:
        overall='INVALID_DATA_COMPLETENESS'
    else:
        overall=repro
    return {'status':overall,'reproducibility_status':repro,'data_completeness_status':comp,
            'hard_reproducibility_invalid':hard_repro,'hard_completeness_invalid':hard_complete,
            'baseline_eligible':baseline_eligible,'input_authority_valid':baseline_eligible}

def compare_baseline(current: dict[str,Any], baseline_path: str|Path|None) -> dict[str,Any]:
    result={'status':'NO_BASELINE','comparable':False,'mismatches':[],'baseline_path':str(baseline_path or '')}
    if not baseline_path:
        return result
    p=Path(baseline_path)
    if not p.exists() or p.stat().st_size<=0:
        return result
    try:
        base=json.loads(p.read_text(encoding='utf-8'))
    except Exception as exc:
        return {'status':'BASELINE_READ_ERROR','comparable':False,'mismatches':[f'{type(exc).__name__}:{exc}'],'baseline_path':str(p)}
    same_period=(str(base.get('start_date',''))==str(current.get('start_date','')) and str(base.get('end_date',''))==str(current.get('end_date','')))
    result['baseline_run']=base.get('run_identity','')
    result['comparable']=bool(same_period)
    if not same_period:
        result['status']='NOT_COMPARABLE_PERIOD'
        return result
    keys=['authority_schema_version','prepared_authority_sha256','codes_sha256','index_map_sha256','marcap_map_sha256','top_mcap_sha256','market_map_sha256','sector_map_sha256',
          'history_global_sha256','history_codes','history_rows','history_issue_sha256','data_completeness_status',
          'raw_population_sha256','raw_population_rows','funnel_population_sha256']
    mm=[]
    for k in keys:
        if str(base.get(k,''))!=str(current.get(k,'')):
            mm.append({'field':k,'baseline':base.get(k,''),'current':current.get(k,'')})
    result['mismatches']=mm
    result['status']='REPRODUCIBLE_MATCH' if not mm else 'REPRODUCIBILITY_MISMATCH'
    return result
