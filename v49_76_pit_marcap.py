from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PIT_DEFINITIVE_MARCAP_MODES = ('A','B1','B2','S','SLOCK','C','G')
PIT_MIXED_MARCAP_MODES = ('H','L')
PIT_MARCAP_INDEPENDENT_MODES = ('LP','I','IT')
INDEX_OVERRIDE_LABELS = ('코스피200','코스닥150')


def _norm_code(v: Any) -> str:
    s=''.join(ch for ch in str(v or '') if ch.isdigit())
    return s.zfill(6) if s else ''


def _sha_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def _canonical_pit_sha(df: pd.DataFrame) -> str:
    if not isinstance(df,pd.DataFrame) or df.empty:
        return _sha_text('')
    z=df.copy()
    z['signal_date']=pd.to_datetime(z.get('signal_date'),errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    z['code']=z.get('code',pd.Series('',index=z.index)).map(_norm_code)
    z['pit_marcap']=pd.to_numeric(z.get('pit_marcap',np.nan),errors='coerce')
    z=z[['signal_date','code','pit_marcap']].sort_values(['signal_date','code']).drop_duplicates(['signal_date','code'])
    rows=[]
    for r in z.itertuples(index=False):
        vv='NA' if pd.isna(r.pit_marcap) else f'{float(r.pit_marcap):.12g}'
        rows.append(f'{r.signal_date}|{r.code}|{vv}')
    return _sha_text('\n'.join(rows))


def _cache_paths(cache_dir: Path, day: str) -> tuple[Path,Path]:
    token=pd.Timestamp(day).strftime('%Y%m%d')
    return cache_dir/f'{token}.csv.gz', cache_dir/f'{token}.json'


def _load_cached_day(cache_dir: Path, day: str, codes: set[str]) -> tuple[dict[str,float],dict[str,Any]] | None:
    csv_path,meta_path=_cache_paths(cache_dir,day)
    if not csv_path.exists() or not meta_path.exists():
        return None
    try:
        meta=json.loads(meta_path.read_text(encoding='utf-8'))
        if str(meta.get('signal_date','')) != pd.Timestamp(day).strftime('%Y-%m-%d'):
            return None
        frame=pd.read_csv(csv_path,dtype={'code':str},compression='gzip')
        if frame.empty or 'code' not in frame or 'pit_marcap' not in frame:
            return None
        frame['code']=frame['code'].map(_norm_code)
        frame['pit_marcap']=pd.to_numeric(frame['pit_marcap'],errors='coerce')
        cached_codes=set(c for c in frame['code'].tolist() if c)
        # A date cache is reusable only if it contains every code required by the
        # current prepared universe. This prevents a stale smaller-universe cache
        # from silently turning newly prepared codes into PIT_MISSING rows.
        if not set(codes).issubset(cached_codes):
            return None
        mapping={c:(float(v) if pd.notna(v) else np.nan) for c,v in zip(frame['code'],frame['pit_marcap']) if c in codes}
        return mapping,{**meta,'cache_hit':True,'cache_file':str(csv_path)}
    except Exception:
        return None


def _save_cached_day(cache_dir: Path, day: str, mapping: dict[str,float], meta: dict[str,Any]) -> None:
    cache_dir.mkdir(parents=True,exist_ok=True)
    csv_path,meta_path=_cache_paths(cache_dir,day)
    frame=pd.DataFrame({'code':sorted(mapping)})
    frame['pit_marcap']=frame['code'].map(lambda c:(float(mapping.get(c)) if pd.notna(mapping.get(c,np.nan)) else np.nan))
    frame.to_csv(csv_path,index=False,compression='gzip',encoding='utf-8')
    doc=dict(meta)
    doc['signal_date']=pd.Timestamp(day).strftime('%Y-%m-%d')
    doc['cache_file']=str(csv_path)
    doc['cache_hit']=False
    doc['codes']=int(len(mapping))
    doc['source_present_codes']=int(sum(1 for v in mapping.values() if pd.notna(v)))
    doc['positive_codes']=int(sum(1 for v in mapping.values() if pd.notna(v) and float(v)>0))
    doc['sha256']=_canonical_pit_sha(pd.DataFrame({'signal_date':[day]*len(frame),'code':frame['code'],'pit_marcap':frame['pit_marcap']}))
    meta_path.write_text(json.dumps(doc,ensure_ascii=False,indent=2,default=str),encoding='utf-8')


def _fetch_market_cap_day(scanner, day: str, codes: set[str], retries: int = 2) -> tuple[dict[str,float],dict[str,Any]]:
    pykrx_stock=getattr(scanner,'pykrx_stock',None)
    if pykrx_stock is None:
        raise RuntimeError('pykrx unavailable for point-in-time MARCAP audit')
    ds=pd.Timestamp(day).strftime('%Y%m%d')
    endpoint_errors=[]
    endpoints=[
        ('get_market_cap_by_ticker',getattr(pykrx_stock,'get_market_cap_by_ticker',None)),
        ('get_market_cap',getattr(pykrx_stock,'get_market_cap',None)),
    ]
    for api_name,fn in endpoints:
        if not callable(fn):
            continue
        for attempt in range(max(1,int(retries))):
            try:
                try:
                    cap=fn(ds,market='ALL')
                except TypeError:
                    cap=fn(ds)
                if not isinstance(cap,pd.DataFrame) or cap.empty:
                    raise RuntimeError(f'empty market-cap frame for {day}')
                mcol=next((c for c in ['시가총액','Marcap','MarCap','MarketCap','market_cap'] if c in cap.columns),None)
                if not mcol:
                    raise RuntimeError(f'market-cap column missing: {list(cap.columns)}')
                idx=[_norm_code(x) for x in cap.index.astype(str).tolist()]
                vals=pd.to_numeric(cap[mcol],errors='coerce').fillna(0.0).astype(float).tolist()
                full={c:float(v) for c,v in zip(idx,vals) if c}
                mapping={c:(float(full[c]) if c in full else np.nan) for c in codes}
                return mapping,{
                    'status':'VALID','signal_date':pd.Timestamp(day).strftime('%Y-%m-%d'),'api':api_name,
                    'source':'PYKRX_SIGNAL_DATE_MARKET_CAP','unit':'KRW','source_rows':int(len(cap)),
                    'codes':int(len(mapping)),'source_present_codes':int(sum(1 for v in mapping.values() if pd.notna(v))),
                    'positive_codes':int(sum(1 for v in mapping.values() if pd.notna(v) and float(v)>0)),
                    'cache_hit':False,'endpoint_errors':endpoint_errors,
                }
            except Exception as exc:
                endpoint_errors.append({'api':api_name,'attempt':attempt+1,'error':f'{type(exc).__name__}: {exc}'})
                if attempt+1 < max(1,int(retries)):
                    time.sleep(min(1.0,0.25*(attempt+1)))
    raise RuntimeError(f'point-in-time MARCAP fetch failed for {day}: {endpoint_errors[-4:]}')


def build_pit_matrix(scanner, signal_dates, codes, cache_dir: str | Path) -> tuple[pd.DataFrame,pd.DataFrame,dict[str,Any]]:
    dates=sorted(set(pd.Timestamp(x).strftime('%Y-%m-%d') for x in signal_dates if pd.notna(pd.to_datetime(x,errors='coerce'))))
    code_set=set(_norm_code(c) for c in codes if _norm_code(c))
    cache_dir=Path(cache_dir)
    refresh=str(os.environ.get('CLOSING_BET_V4975_PIT_REFRESH','0') or '0').lower() in ('1','true','yes','y','on')
    retries=int(os.environ.get('CLOSING_BET_V4975_PIT_RETRIES','2') or 2)
    min_cov=float(os.environ.get('CLOSING_BET_V4975_PIT_MIN_DATE_COVERAGE_PCT','95') or 95)
    rows=[]; meta_rows=[]; failures=[]; cache_hits=0; fetched=0
    for day in dates:
        cached=None if refresh else _load_cached_day(cache_dir,day,code_set)
        if cached is not None:
            mapping,meta=cached; cache_hits+=1
        else:
            try:
                mapping,meta=_fetch_market_cap_day(scanner,day,code_set,retries=retries)
                _save_cached_day(cache_dir,day,mapping,meta); fetched+=1
            except Exception as exc:
                failures.append({'signal_date':day,'error':f'{type(exc).__name__}: {exc}'})
                meta_rows.append({'signal_date':day,'status':'FAILED','api':'','source':'','cache_hit':False,'error':failures[-1]['error']})
                continue
        meta_rows.append({
            'signal_date':day,'status':str(meta.get('status','VALID')),'api':str(meta.get('api','')),
            'source':str(meta.get('source','')),'cache_hit':bool(meta.get('cache_hit',False)),
            'source_rows':int(meta.get('source_rows',0) or 0),'codes':int(meta.get('codes',len(mapping)) or len(mapping)),
            'source_present_codes':int(meta.get('source_present_codes',sum(1 for v in mapping.values() if pd.notna(v))) or 0),
            'positive_codes':int(meta.get('positive_codes',sum(1 for v in mapping.values() if pd.notna(v) and float(v)>0)) or 0),'error':'',
        })
        for c,v in mapping.items():
            rows.append({'signal_date':day,'code':c,'pit_marcap':(float(v) if pd.notna(v) else np.nan)})
    matrix=pd.DataFrame(rows,columns=['signal_date','code','pit_marcap'])
    day_meta=pd.DataFrame(meta_rows)
    valid_dates=int(day_meta.status.eq('VALID').sum()) if not day_meta.empty else 0
    requested_dates=len(dates)
    date_cov=float(valid_dates/requested_dates*100.0) if requested_dates else 100.0
    status='FULL_VALID' if requested_dates and valid_dates==requested_dates else ('DEGRADED_COVERAGE' if date_cov>=min_cov else 'INVALID_LOW_COVERAGE')
    manifest={
        'version':'v49.76','lane':'HISTORICAL_POINT_IN_TIME_MARCAP_LOOKAHEAD_AUDIT',
        'status':status,'requested_dates':requested_dates,'valid_dates':valid_dates,'failed_dates':requested_dates-valid_dates,
        'date_coverage_pct':date_cov,'min_date_coverage_pct':min_cov,'cache_hits':cache_hits,'network_fetches':fetched,
        'refresh':refresh,'codes':len(code_set),'matrix_rows':int(len(matrix)),'matrix_sha256':_canonical_pit_sha(matrix),
        'source':'PYKRX_SIGNAL_DATE_MARKET_CAP','unit':'KRW','failures':failures[:30],
        'policy':'AUDIT_ONLY_NO_AUTO_APPLY','historical_index_membership':'NOT_RECONSTRUCTED_CURRENT_INDEX_OVERRIDE_HELD_CONSTANT',
        'reverse_addition_scope':'UNOBSERVABLE_FROM_CURRENT_RAW_WITHOUT_FULL_PREDICATE_SHADOW_REPLAY',
    }
    return matrix,day_meta,manifest


def _enrich_population(pop: pd.DataFrame, pit_matrix: pd.DataFrame, prepared_snapshot: pd.DataFrame, mcap_or_min: float) -> pd.DataFrame:
    x=pop.copy()
    x['signal_date']=pd.to_datetime(x.get('signal_date'),errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    x['code']=x.get('code',pd.Series('',index=x.index)).map(_norm_code)
    x['mode']=x.get('mode',pd.Series('',index=x.index)).fillna('').astype(str)
    pit=pit_matrix.copy()
    if pit.empty:
        pit=pd.DataFrame(columns=['signal_date','code','pit_marcap'])
    pit['code']=pit.get('code',pd.Series('',index=pit.index)).map(_norm_code)
    pit['signal_date']=pd.to_datetime(pit.get('signal_date'),errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    x=x.merge(pit[['signal_date','code','pit_marcap']],on=['signal_date','code'],how='left')
    snap=prepared_snapshot.copy()
    snap['code']=snap.get('code',pd.Series('',index=snap.index)).map(_norm_code)
    if 'marcap' not in snap.columns:
        for c in ('market_cap','시가총액','marcap_krw'):
            if c in snap.columns:
                snap['marcap']=snap[c]; break
    if 'index_label' not in snap.columns:
        for c in ('index','index_map','지수'):
            if c in snap.columns:
                snap['index_label']=snap[c]; break
    if 'name' not in snap.columns: snap['name']=''
    snap['end_marcap']=pd.to_numeric(snap.get('marcap',0),errors='coerce').fillna(0.0)
    snap['pit_index_label']=snap.get('index_label',pd.Series('',index=snap.index)).fillna('').astype(str)
    x=x.merge(snap[['code','end_marcap','pit_index_label']].drop_duplicates('code'),on='code',how='left')
    x['end_marcap']=pd.to_numeric(x.get('end_marcap',0),errors='coerce').fillna(0.0)
    x['pit_marcap']=pd.to_numeric(x.get('pit_marcap'),errors='coerce')
    x['pit_marcap_available']=x['pit_marcap'].notna().astype(int)
    x['index_override']=x['pit_index_label'].isin(INDEX_OVERRIDE_LABELS).astype(int)
    x['end_mcap_pass']=x['end_marcap'].ge(float(mcap_or_min)).astype(int)
    x['pit_mcap_pass']=x['pit_marcap'].ge(float(mcap_or_min)).where(x['pit_marcap'].notna(),False).astype(int)
    x['end_universe_ok']=((x['index_override']==1)|(x['end_mcap_pass']==1)).astype(int)
    x['pit_marcap_shadow_ok']=((x['index_override']==1)|(x['pit_mcap_pass']==1)).where(x['pit_marcap_available']==1,False).astype(int)
    x['pit_gate_scope']=np.select(
        [x['mode'].isin(PIT_DEFINITIVE_MARCAP_MODES),x['mode'].isin(PIT_MIXED_MARCAP_MODES),x['mode'].isin(PIT_MARCAP_INDEPENDENT_MODES)],
        ['DEFINITIVE_MARCAP_OR_INDEX','MIXED_MARCAP_LOGIC','MARCAP_INDEPENDENT_FAST_GATE'],default='OTHER')
    x['pit_eligibility_class']=np.select(
        [
            x['pit_marcap_available'].eq(0),
            x['end_universe_ok'].eq(1)&x['pit_marcap_shadow_ok'].eq(1),
            x['end_universe_ok'].eq(1)&x['pit_marcap_shadow_ok'].eq(0),
            x['end_universe_ok'].eq(0)&x['pit_marcap_shadow_ok'].eq(1),
        ],
        ['PIT_MISSING','BOTH_ELIGIBLE','FUTURE_MCAP_INCLUDED','HISTORICAL_MCAP_INCLUDED'],default='BOTH_INELIGIBLE')
    x['future_mcap_included_definitive']=((x['pit_gate_scope']=='DEFINITIVE_MARCAP_OR_INDEX')&(x['pit_eligibility_class']=='FUTURE_MCAP_INCLUDED')).astype(int)
    return x


def _net50(df: pd.DataFrame) -> pd.Series:
    return pd.to_numeric(df.get('rule35_pnl'),errors='coerce')-0.50


def _oos(df: pd.DataFrame, split_date: str | pd.Timestamp) -> pd.DataFrame:
    d=pd.to_datetime(df.get('signal_date'),errors='coerce')
    return df[d.ge(pd.Timestamp(split_date))].copy()


def run_audit(scanner, canonical: pd.DataFrame, selected: pd.DataFrame, prepared_snapshot_path: str | Path,
              out_dir: str | Path, cache_dir: str | Path, split_date: str | pd.Timestamp,
              top_per_strategy: int = 5, mcap_or_min: float = 200_000_000_000) -> dict[str,Any]:
    out=Path(out_dir); out.mkdir(parents=True,exist_ok=True)
    snap=pd.read_csv(prepared_snapshot_path,dtype={'code':str})
    codes=sorted(set(snap.get('code',pd.Series(dtype=str)).map(_norm_code)) - {''})
    signal_dates=canonical.get('signal_date',pd.Series(dtype=str)).dropna().astype(str).tolist()
    matrix,day_meta,manifest=build_pit_matrix(scanner,signal_dates,codes,cache_dir)
    matrix.to_csv(out/'v49_76_pit_marcap_matrix.csv.gz',index=False,compression='gzip',encoding='utf-8')
    day_meta.to_csv(out/'v49_76_pit_marcap_date_coverage.csv',index=False,encoding='utf-8-sig')
    current=_enrich_population(canonical,matrix,snap,mcap_or_min)
    selected_enriched=_enrich_population(selected,matrix,snap,mcap_or_min)
    current.to_csv(out/'v49_76_pit_marcap_current_raw_enriched.csv.gz',index=False,compression='gzip',encoding='utf-8')
    selected_enriched.to_csv(out/'v49_76_pit_marcap_current_selected_enriched.csv',index=False,encoding='utf-8-sig')

    definitive=current[current['pit_gate_scope'].eq('DEFINITIVE_MARCAP_OR_INDEX')].copy()
    covered=definitive[definitive.pit_marcap_available.eq(1)]
    future=covered[covered.future_mcap_included_definitive.eq(1)].copy()
    selected_def=selected_enriched[selected_enriched['pit_gate_scope'].eq('DEFINITIVE_MARCAP_OR_INDEX')].copy()
    selected_cov=selected_def[selected_def.pit_marcap_available.eq(1)]
    selected_future=selected_cov[selected_cov.future_mcap_included_definitive.eq(1)].copy()

    # One-sided shadow: remove currently emitted definitive-mode signals that only pass because
    # of the END-DATE MARCAP. It intentionally cannot add signals that the fixed-end gate never emitted.
    shadow_input=current.copy()
    shadow_input=shadow_input[~((shadow_input['pit_gate_scope']=='DEFINITIVE_MARCAP_OR_INDEX')&(shadow_input['future_mcap_included_definitive']==1))].copy()
    shadow_selected=scanner._select_backtest_top(shadow_input,top_per_strategy=max(1,int(top_per_strategy)),all_candidates=False)
    shadow_selected.to_csv(out/'v49_76_pit_marcap_one_sided_shadow_top5.csv',index=False,encoding='utf-8-sig')

    mode_rows=[]
    modes=list(getattr(scanner,'CLOSING_BET_V4958_PRIMARY_PRIORITY',[]))
    for mode in modes:
        curm=selected_enriched[selected_enriched['mode'].eq(mode)].copy()
        shm=shadow_selected[shadow_selected.get('mode',pd.Series('',index=shadow_selected.index)).astype(str).eq(mode)].copy()
        rawm=current[current['mode'].eq(mode)].copy()
        covm=rawm[rawm.pit_marcap_available.eq(1)]
        futurem=covm[(covm.pit_gate_scope=='DEFINITIVE_MARCAP_OR_INDEX')&(covm.future_mcap_included_definitive.eq(1))]
        sf=selected_future[selected_future['mode'].eq(mode)]
        oo=_oos(curm,split_date); so=_oos(shm,split_date)
        mode_rows.append({
            'strategy':mode,'gate_scope':str(rawm.pit_gate_scope.mode().iloc[0]) if len(rawm) else '',
            'current_raw_n':len(rawm),'pit_covered_raw_n':len(covm),'future_mcap_included_raw_n':len(futurem),
            'future_mcap_included_raw_pct':float(len(futurem)/len(covm)*100.0) if len(covm) else np.nan,
            'current_selected_n':len(curm),'future_mcap_included_selected_n':len(sf),
            'affected_selected_dates_n':int(sf.signal_date.nunique()) if len(sf) else 0,
            'current_oos_n':len(oo),'current_oos_net50_mean_pct':float(_net50(oo).mean()) if len(oo) else np.nan,
            'shadow_oos_n':len(so),'shadow_oos_net50_mean_pct':float(_net50(so).mean()) if len(so) else np.nan,
            'shadow_minus_current_oos_net50_pctp':(float(_net50(so).mean()-_net50(oo).mean()) if len(oo) and len(so) else np.nan),
            'shadow_scope':'ONE_SIDED_REMOVE_ONLY_CURRENT_RAW',
        })
    mode_summary=pd.DataFrame(mode_rows)
    mode_summary.to_csv(out/'v49_76_pit_marcap_strategy_impact.csv',index=False,encoding='utf-8-sig')

    future_cols=[c for c in ['signal_date','code','name','mode','score','amount_b','end_marcap','pit_marcap','pit_index_label','end_mcap_pass','pit_mcap_pass','rule35_pnl'] if c in future.columns]
    future.sort_values(['signal_date','mode','code']).to_csv(out/'v49_76_pit_marcap_future_included_cases.csv',columns=future_cols,index=False,encoding='utf-8-sig')
    selected_future.sort_values(['signal_date','mode','code']).to_csv(out/'v49_76_pit_marcap_future_included_selected_cases.csv',index=False,encoding='utf-8-sig')

    code_rows=[]
    if not future.empty:
        g=future.groupby('code',dropna=False)
        for code,q in g:
            end_cap=float(pd.to_numeric(q.end_marcap,errors='coerce').median())
            pit_cap=float(pd.to_numeric(q.pit_marcap,errors='coerce').median())
            code_rows.append({
                'code':str(code).zfill(6),'name':str(q.get('name',pd.Series([''])).iloc[0] if 'name' in q else ''),
                'future_included_raw_n':len(q),'signal_dates_n':int(q.signal_date.nunique()),
                'median_signal_marcap_krw':pit_cap,'end_marcap_krw':end_cap,
                'end_to_signal_marcap_ratio':float(end_cap/pit_cap) if pit_cap>0 else np.nan,
                'first_signal_date':str(q.signal_date.min()),'last_signal_date':str(q.signal_date.max()),
            })
    code_summary=pd.DataFrame(code_rows)
    if not code_summary.empty:
        code_summary=code_summary.sort_values(['future_included_raw_n','end_to_signal_marcap_ratio'],ascending=[False,False])
    code_summary.to_csv(out/'v49_76_pit_marcap_future_included_code_summary.csv',index=False,encoding='utf-8-sig')

    raw_row_cov=float(current.pit_marcap_available.mean()*100.0) if len(current) else 100.0
    def_row_cov=float(definitive.pit_marcap_available.mean()*100.0) if len(definitive) else 100.0
    min_row_cov=float(os.environ.get('CLOSING_BET_V4975_PIT_MIN_ROW_COVERAGE_PCT','98') or 98)
    base_status=str(manifest.get('status','INVALID_LOW_COVERAGE'))
    if base_status=='FULL_VALID' and def_row_cov < min_row_cov:
        manifest['status']='DEGRADED_ROW_COVERAGE'
    manifest['min_definitive_row_coverage_pct']=min_row_cov
    manifest.update({
        'raw_rows':int(len(current)),'raw_row_coverage_pct':raw_row_cov,
        'definitive_modes':list(PIT_DEFINITIVE_MARCAP_MODES),'mixed_modes':list(PIT_MIXED_MARCAP_MODES),'independent_modes':list(PIT_MARCAP_INDEPENDENT_MODES),
        'definitive_raw_rows':int(len(definitive)),'definitive_row_coverage_pct':def_row_cov,
        'future_mcap_included_raw_n':int(len(future)),'future_mcap_included_raw_pct':float(len(future)/len(covered)*100.0) if len(covered) else 0.0,
        'future_mcap_included_selected_n':int(len(selected_future)),'future_mcap_included_selected_dates_n':int(selected_future.signal_date.nunique()) if len(selected_future) else 0,
        'one_sided_shadow_rows':int(len(shadow_selected)),
        'full_reverse_replay_performed':False,
        'interpretation_guard':'REMOVAL_EFFECT_IS_MEASURABLE; ADDITION_OF_HISTORICALLY_ELIGIBLE_BUT_END_DATE_INELIGIBLE SIGNALS IS NOT MEASURED',
    })
    (out/'v49_76_pit_marcap_audit_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return {
        'manifest':manifest,'date_coverage':day_meta,'matrix':matrix,'raw_enriched':current,'selected_enriched':selected_enriched,
        'strategy_impact':mode_summary,'future_cases':future,'future_selected':selected_future,'code_summary':code_summary,'shadow_selected':shadow_selected,
    }
