"""Fail-closed provider-provenance and native-shard-completeness checks."""
from __future__ import annotations
import argparse,json,hashlib
from pathlib import Path
import pandas as pd

def check_shard(root,shard):
    root=Path(root);idx=int(shard)
    raw=root/f'shard_{idx:02d}_raw.csv'
    manifest=root/f'shard_{idx:02d}_manifest.json'
    complete=root/f'shard_{idx:02d}_complete.json'
    for f in (raw,manifest,complete):
        if not f.is_file() or f.stat().st_size<=0:raise RuntimeError(f'NATIVE_SHARD_FILE_MISSING:{f}')
    m=json.loads(manifest.read_text());c=json.loads(complete.read_text())
    if m.get('engine_status')!='VALID' or m.get('control_flow')!='V49_75_SHARD_EXPORT_RETURN_MAIN_EXIT':
        raise RuntimeError('NATIVE_SHARD_ENGINE_NOT_VALID')
    if m.get('shard_index')!=idx or c.get('status')!='SHARD_ONLY_COMPLETE':
        raise RuntimeError('NATIVE_SHARD_IDENTITY_INVALID')
    if hashlib.sha256(raw.read_bytes()).hexdigest()!=m.get('raw_sha256'):
        raise RuntimeError('NATIVE_SHARD_RAW_SHA_MISMATCH')
    traces=[json.loads(x.read_text()) for x in root.glob('krx_provider_trace_*.json')]
    if not traces:raise RuntimeError('NO_KRX_PROVIDER_TRACE:shim may not have been activated')
    if not all(t.get('patch_active') and t.get('provider')=='PYKRX_KRX_DAILY_SESSION_SQLITE' for t in traces):
        raise RuntimeError('NON_KRX_PROVIDER_TRACE_FOUND')
    total=sum(int(t.get('fdr_calls',0)) for t in traces)
    errors=sum(int(t.get('fdr_failures',0))+int(t.get('direct_raw_errors',0)) for t in traces)
    if total<=0:raise RuntimeError('NO_FDR_HISTORY_INTERCEPTS:untraced price loader may be in use')
    if sum(int(t.get('sqlite_reads',0)) for t in traces)<=0:raise RuntimeError('NO_SQLITE_HISTORY_USED')
    if errors>0:raise RuntimeError(f'KRX_HISTORY_FETCH_FAILURES:{errors}')
    data={'status':'KRX_PROVIDER_INTERCEPTS_PASS_NOT_YET_MARKET_PARITY_VERIFIED',
          'shard':idx,'fdr_calls':total,'sqlite_reads':sum(int(t.get('sqlite_reads',0)) for t in traces),'krx_price_calls':sum(int(t.get('krx_price_calls',0)) for t in traces),
          'history_fetch_errors':errors,'offline_materialized_krx':True,'native_raw_rows':int(m.get('raw_rows',-1)),'native_raw_sha256':m['raw_sha256'],
          'warning':'Input timing/universe and KRX regular-session-specific semantics remain unverified.'}
    (root/f'shard_{idx:02d}_krx_gate.json').write_text(json.dumps(data,indent=2),encoding='utf-8')
    return data

def compare_selected(research_path,legacy_path,out):
    a=pd.read_csv(research_path,dtype={'code':str},low_memory=False)
    b=pd.read_csv(legacy_path,dtype={'code':str},low_memory=False)
    key=['signal_date','code','mode']
    if not all(set(key).issubset(x.columns) for x in [a,b]):raise RuntimeError('SELECTED_SCHEMA_MISSING')
    for x in (a,b):
        x['code']=x.code.astype(str).str.zfill(6)
        if x.duplicated(key).any():raise RuntimeError('DUPLICATE_SELECTED_KEY')
    ai=a[key+['entry_price']].rename(columns={'entry_price':'krx_only_entry'})
    bi=b[key+['entry_price']].rename(columns={'entry_price':'legacy_entry'})
    full=ai.merge(bi,on=key,how='outer',indicator=True,validate='one_to_one')
    full['class']=full['_merge'].map({'left_only':'KRX_ONLY','right_only':'LEGACY_ONLY','both':'BOTH'}).astype(str)
    both=full['class'].eq('BOTH')
    full.loc[both,'entry_price_diff_pct']=(pd.to_numeric(full.loc[both,'krx_only_entry'])/pd.to_numeric(full.loc[both,'legacy_entry'])-1)*100
    Path(out).mkdir(parents=True,exist_ok=True)
    full.to_csv(Path(out)/'r17_native_signal_churn.csv',index=False,encoding='utf-8-sig')
    summary={'research_rows':len(a),'legacy_rows':len(b),'krx_only':int(full['class'].eq('KRX_ONLY').sum()),
             'legacy_only':int(full['class'].eq('LEGACY_ONLY').sum()),'same_key':int(both.sum()),
             'different_entry_over_0_5pct':int((full['entry_price_diff_pct'].abs()>0.5).sum()),
             'claim':'search result churn diagnostic only, not PIT universe validation'}
    (Path(out)/'r17_signal_churn_summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    return summary

def strict_price_equality(selected_path,prices_path,out):
    """Require bytewise-equivalent numeric OHLCV on EVERY selected signal day.

    No entry-price substitution and no tolerance-based repair. Uses the separate
    daily cross-sectional KRX fetch produced in prepare_native_krx_prices.py.
    """
    import numpy as np
    a=pd.read_csv(selected_path,dtype={'code':str},low_memory=False)
    b=pd.read_csv(prices_path,dtype={'code':str},low_memory=False)
    need_a={'signal_date','code','entry_price','_open','_high','_low','_close','_vol'}
    need_b={'date','code','open','high','low','close','volume'}
    if not need_a.issubset(a.columns) or not need_b.issubset(b.columns):
        raise RuntimeError('STRICT_KRX_SOURCE_COLUMNS_MISSING')
    for f in [a,b]:f['code']=f.code.astype(str).str.zfill(6)
    key=['signal_date','code']
    if (a.groupby(key)[['entry_price','_open','_high','_low','_close','_vol']].nunique(dropna=False)>1).any().any():
        raise RuntimeError('STRICT_SAME_TICKER_DATE_SIGNAL_CONFLICT')
    src=a[list(need_a)].drop_duplicates(key)
    if b.duplicated(['date','code']).any():raise RuntimeError('STRICT_KRX_DAILY_DUPLICATES')
    merged=src.merge(b,left_on=key,right_on=['date','code'],how='left',validate='one_to_one')
    comparators=[('entry_price','close'),('_open','open'),('_high','high'),('_low','low'),('_close','close'),('_vol','volume')]
    all_match=pd.Series(True,index=merged.index)
    for sf,k in comparators:
        one=pd.to_numeric(merged[sf],errors='coerce')
        two=pd.to_numeric(merged[k],errors='coerce')
        # Integer KRX prices/volume: exact, no split adjust and no rounding.
        matches=one.notna()&two.notna()&one.eq(two)
        merged['strict_match_'+sf]=matches
        all_match=all_match&matches
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    merged.loc[~all_match].to_csv(out/'r17_strict_price_disagreements.csv',index=False,encoding='utf-8-sig')
    summary={'status':'STRICT_ALL_SELECTED_KRX_BARS_MATCH' if all_match.all() else 'FAIL_CLOSED_KRX_PRICE_SOURCE_MISMATCH',
             'unique_signal_bars':len(merged),'exact_matched':int(all_match.sum()),
             'exact_mismatched_or_missing':int((~all_match).sum()),
             'definition':'Six fields, all exactly equal to independent dated KRX cross-sectional OHLCV; no tolerance or substitution.'}
    (out/'r17_strict_krx_price_audit.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    if not all_match.all():raise RuntimeError(f"STRICT_KRX_SOURCE_MISMATCH:{summary['exact_mismatched_or_missing']}/{len(merged)}")
    return summary

def main():
    p=argparse.ArgumentParser();p.add_argument('--shard-root');p.add_argument('--shard',type=int)
    p.add_argument('--selected');p.add_argument('--legacy');p.add_argument('--prices');p.add_argument('--out',default='reports/r17')
    a=p.parse_args()
    r=check_shard(a.shard_root,a.shard) if a.shard_root else (strict_price_equality(a.selected,a.prices,a.out) if a.prices else compare_selected(a.selected,a.legacy,a.out))
    print(json.dumps(r,ensure_ascii=False),flush=True)
if __name__=='__main__':main()
