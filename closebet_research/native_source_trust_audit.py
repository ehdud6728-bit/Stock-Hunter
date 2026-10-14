#!/usr/bin/env python3
"""Research-only trust audit of ORIGINAL v49.76 selected signals.

Source-internal checks are NOT independent market-data verification. Any native
outcome whose evaluation end date exceeds the requested as-of is excluded.
"""
import argparse, json
from pathlib import Path
import pandas as pd
import numpy as np

REQ={'signal_date','data_date','code','mode','_open','_high','_low','_close','_vol','close','entry_price',
     'eval_end_date','eval_available_days','eval_is_partial','hold_days','requested_hold_days','ret_close_hd'}

def evaluate(source, asof, out):
    src=Path(source);o=Path(out);o.mkdir(parents=True,exist_ok=True)
    df=pd.read_csv(src,dtype={'code':str},low_memory=False)
    if not REQ.issubset(df.columns):raise ValueError('SOURCE_SCHEMA_INCOMPLETE:'+','.join(sorted(REQ-set(df.columns))))
    df['date']=pd.to_datetime(df.signal_date,errors='coerce')
    df['eval_end']=pd.to_datetime(df.eval_end_date,errors='coerce')
    cut=pd.Timestamp(asof)
    s=pd.to_numeric(df._close,errors='coerce')
    q=pd.to_numeric(df.close,errors='coerce')
    e=pd.to_numeric(df.entry_price,errors='coerce')
    openv=pd.to_numeric(df._open,errors='coerce');high=pd.to_numeric(df._high,errors='coerce')
    low=pd.to_numeric(df._low,errors='coerce');vol=pd.to_numeric(df._vol,errors='coerce')
    source_consistent= (s>0)&(s==q)&(q==e)&(df.signal_date==df.data_date)
    ohlc_valid=(openv>0)&(high>0)&(low>0)&(vol>0)&(high>=pd.concat([openv,s,low],axis=1).max(axis=1))&(low<=pd.concat([openv,s,high],axis=1).min(axis=1))
    asof_valid=df.date.notna()&(df.date<=cut)
    full=(df.requested_hold_days==20)&(df.hold_days==20)&(df.eval_available_days>=20)&(df.eval_is_partial==0)&df.eval_end.notna()
    native_mature=full & (df.eval_end<=cut)&asof_valid
    native_postcutoff=full & (df.eval_end>cut)
    invalid_end=(df.eval_end>cut)
    df['trust_cohort']=np.select([~source_consistent|~ohlc_valid|~asof_valid,native_mature,native_postcutoff,invalid_end],
        ['SOURCE_INTERNAL_INVALID','NATIVE_D20_MATURED_BY_ASOF_UNVERIFIED_PRICE',
         'NATIVE_D20_POST_CUTOFF_EXCLUDED','POST_CUTOFF_EVALUATION_NOT_D20'],
        default='NATIVE_D20_NOT_MATURED_BY_ASOF')
    eligible=df[df.trust_cohort=='NATIVE_D20_MATURED_BY_ASOF_UNVERIFIED_PRICE'].copy()
    def summarise(df):
        arr=pd.to_numeric(df.ret_close_hd,errors='coerce').dropna()
        return {'n':int(len(arr)),'win_pct':float((arr>0).mean()*100) if len(arr) else None,
                'mean_gross_pct':float(arr.mean()) if len(arr) else None,
                'median_gross_pct':float(arr.median()) if len(arr) else None,
                'mean_net50_illustrative_pct':float(arr.mean()-0.50) if len(arr) else None}
    summaries=[]
    for mode,z in [('ALL_MODES',eligible),('UNIQUE_CODE_DATE',eligible.sort_values(['signal_date','code','mode']).drop_duplicates(['signal_date','code']))]+list(eligible.groupby('mode')):
        summaries.append({'mode':mode,**summarise(z)})
    by_mode=pd.DataFrame(summaries)
    by_mode.to_csv(o/'native_source_d20_by_mode.csv',index=False,encoding='utf-8-sig')
    by_date=pd.DataFrame([{'signal_date':d, 'signals':int(len(z)), 'internal_consistent':int((source_consistent.loc[z.index]&ohlc_valid.loc[z.index]).sum()),
        'd20_within_cutoff':int((z.trust_cohort=='NATIVE_D20_MATURED_BY_ASOF_UNVERIFIED_PRICE').sum()),
        'after_cutoff_eval_rows':int((z.eval_end>cut).sum())}
        for d,z in df.groupby('signal_date',sort=True)])
    by_date.to_csv(o/'native_source_date_coverage.csv',index=False,encoding='utf-8-sig')
    cols=['signal_date','code','name','mode','entry_price','eval_end_date','eval_available_days','ret_close_hd','trust_cohort']
    df[[c for c in cols if c in df]].to_csv(o/'native_source_d20_events.csv',index=False,encoding='utf-8-sig')
    srcroot=src.parent
    def read_json(name):
        f=srcroot/name
        try:return json.loads(f.read_text(encoding='utf-8-sig'))
        except (FileNotFoundError,ValueError):return {}
    pit=read_json('v49_76_pit_marcap_audit_manifest.json')
    inp=read_json('v49_76_input_authority_manifest.json')
    manifest=read_json('v49_76_global_manifest.json')
    insight=manifest.get('search_gap_insight_manifest',{})
    oos_start=str(insight.get('standard_oos_start',''))
    oos_matured=int((eligible.date>=pd.Timestamp(oos_start)).sum()) if oos_start else None
    code_date_duplicate=int(df.duplicated(['signal_date','code','mode']).sum())
    bad_src=int((~source_consistent|~ohlc_valid|~asof_valid).sum())
    quality={
        'conclusion':'SOURCE_INTERNALLY_CONSISTENT_BUT_NOT_INDEPENDENT_PRICE_OR_PIT_VERIFIED' if bad_src==0 else 'SOURCE_INTERNAL_INCONSISTENCY',
        'asof':str(cut.date()),'source_selected_file':str(src),
        'selected_signal_rows':int(len(df)), 'unique_code_date_rows':int(len(df.drop_duplicates(['signal_date','code']))),
        'distinct_codes':int(df.code.nunique()),'signal_dates':int(df.signal_date.nunique()),
        'internal_ohlcv_price_date_consistent_rows':int((source_consistent&ohlc_valid&asof_valid).sum()),
        'internal_ohlcv_price_date_invalid_rows':bad_src,'duplicate_signal_code_mode':code_date_duplicate,
        'native_eval_after_cutoff_rows':int(invalid_end.sum()),
        'native_d20_complete_but_after_cutoff_EXCLUDED':int(native_postcutoff.sum()),
        'native_d20_matured_within_cutoff_internal_ONLY':int(len(eligible)),
        'native_d20_matured_unique_code_date':int(len(eligible.drop_duplicates(['signal_date','code']))),
        'native_source_d20_provisional':summarise(eligible),
        'standard_oos_start':oos_start or 'UNVERIFIED',
        'standard_oos_d20_matured_rows_by_asof':oos_matured,
        'amount_close_times_volume_proxy_rows':int(df.amount_source.astype(str).eq('CLOSE_X_VOLUME').sum()) if 'amount_source' in df else None,
        'historical_i_flow_cache_only_proxy_rows':int(df.historical_i_flow_mode.astype(str).eq('CACHE_ONLY_PROXY').sum()) if 'historical_i_flow_mode' in df else None,
        'predicate_payload_signal_date_missing_rows':int(df.predicate_payload_signal_date.isna().sum()) if 'predicate_payload_signal_date' in df else None,
        'native_test_purity_warning':manifest.get('test_purity_warning','UNVERIFIED'),
        'independent_krx_ohlcv_price_verification':'NOT_DONE',
        'search_predicate_replay_independently_verified':False,
        'input_history_status':inp.get('data_completeness_status','UNVERIFIED'),
        'input_missing_history_codes':inp.get('history_zero_codes_n','UNVERIFIED'),
        'end_date_marcap_snapshot':inp.get('marcap_authority_asof_date','UNVERIFIED'),
        'pit_marcap_dates_verified':pit.get('valid_dates',0),
        'pit_marcap_future_selected_rows':pit.get('future_mcap_included_selected_n','UNVERIFIED'),
        'pit_historical_index_membership':pit.get('historical_index_membership','UNVERIFIED'),
        'pit_reverse_replay_performed':pit.get('full_reverse_replay_performed',False),
        'independent_out_of_sample':False,
        'native_internal_manifest_status':manifest.get('status','UNVERIFIED'),
        'output_use':'DIAGNOSTIC_ONLY_NO_STRATEGY_RANKING_NO_LIVE_CHANGE',
        'warning':'Source-native ret_close_hd is not independently priced; cutoff-matured cohorts cannot be treated as final strategy results. No independent historical universe reverse replay; signal close fill not assured.'
    }
    (o/'native_source_trust_audit.json').write_text(json.dumps(quality,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(quality,ensure_ascii=False,indent=2));print(by_mode.to_string(index=False))
    return quality

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--selected',required=True);p.add_argument('--asof',required=True);p.add_argument('--out',required=True)
    a=p.parse_args();evaluate(a.selected,a.asof,a.out)
