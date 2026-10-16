"""R117: immutable, source-limited v49.76 candidate denominator x shadow delivery audit.

RESEARCH ONLY. The historical KRX file covers an as-of selected universe subset,
not a point-in-time full-market universe. Future returns are outcomes only.
No LIVE, Telegram, orders, source mutation, or threshold selection.
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
from zipfile import ZipFile
import numpy as np
import pandas as pd

VERSION = "R117_V4976_SELECTED_VS_COVERED_COHORT_AND_SHADOW_FROZEN_V1"
RUN_171 = 38664095610
SOURCE_NAME = 'v49_76_global_selected_top5.csv'
CSV_DATE = 'signal_date'

def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()

def check(ok: bool, msg: str):
    if not ok:
        raise RuntimeError(msg)

def six(s) -> str:
    s=str(s).strip()
    if s.endswith('.0'):s=s[:-2]
    return s.zfill(6)

def read_selected(root: Path) -> tuple[pd.DataFrame,str]:
    cand=[root/'v49_76_global_selected_top5.csv', root/'r17_krx/native/v49_76_global_selected_top5.csv']
    f=next((x for x in cand if x.is_file()),None)
    check(f is not None,'MISSING_R171_PINNED_SELECTED')
    raw=f.read_bytes(); d=pd.read_csv(io.BytesIO(raw),dtype={'code':str},low_memory=False)
    check('signal_date' in d and 'code' in d and 'mode' in d,'SELECTED_SCHEMA_MISSING')
    d['code']=d['code'].map(six); d['signal_date']=d['signal_date'].astype(str).str.slice(0,10)
    check(d['signal_date'].nunique()>=30 and len(d)>=1000,'SELECTED_SAMPLE_SIZE_OR_SCHEMA_CHANGE')
    return d,sha(raw)

def load_shards(hist: Path, start: str='2025-01-01') -> tuple[pd.DataFrame,dict]:
    """Verify every KRX gz compressed byte against source manifests; only load requested window."""
    manifests=[]; observed={}; frames=[]; seen=set();
    sources=sorted(hist.glob('*.zip'))
    check(len(sources)==4,'FOUR_KRX_HISTORY_ARCHIVES_REQUIRED')
    for src in sources:
        with ZipFile(src) as z:
            manifests_in=[n for n in z.namelist() if n.startswith('krx_history_manifest_') and n.endswith('.json')]
            check(len(manifests_in)==1,'KRX_MANIFEST_MISSING '+src.name)
            m=json.loads(z.read(manifests_in[0]));
            check(m.get('status')=='COMPLETE' and m.get('official_days')==1907, 'KRX_MANIFEST_BAD '+src.name)
            manifests.append(m)
            for ent in m['files']:
                name=str(ent['file']); day=str(ent['day']); check(name in z.namelist(),'DAY_FILE_MISSING '+name)
                raw=z.read(name);check(sha(raw)==ent['sha256'],'SHA_BAD '+name)
                check(day not in seen,'DUPLICATE_KRX_DAY '+day);seen.add(day)
                observed[day]=(int(ent['rows']),int(ent['all_market_rows']))
                if day >= start:
                    tab=pd.read_csv(io.BytesIO(gzip.decompress(raw)),dtype={'code':str})
                    check(len(tab)==int(ent['rows']), 'ROW_COUNT_BAD '+day)
                    check(set(['code','date','open','high','low','close','volume','amount']).issubset(tab.columns),'KRX_COLUMNS_BAD '+day)
                    tab['date']=day;tab['code']=tab['code'].map(six)
                    frames.append(tab)
    check(len(seen)==1907,'KRX_1907_DAYS_NOT_COMPLETE')
    check(len({m['official_calendar_sha256'] for m in manifests})==1,'CALENDAR_DISAGREEMENT')
    check({m['shard_index'] for m in manifests}==set(range(4)),'SHARD_INDICES_BAD')
    d=pd.concat(frames,ignore_index=True)
    check(d[['date','code']].duplicated().sum()==0,'DUP_KRX_CODE_DAY')
    for c in ('open','high','low','close','volume','amount'):
        d[c]=pd.to_numeric(d[c],errors='coerce')
    d=d.sort_values(['code','date']).reset_index(drop=True)
    diag={'sha_days_verified':len(seen),'history_start':min(seen),'history_asof':max(seen),
          'source_files':[x.name for x in sources],'raw_history_rows_in_window':len(d),
          'daily_coverage_last_day':{'stored_rows':observed[max(seen)][0],'all_market_rows':observed[max(seen)][1]},
          'recent_daily_source_scope':{day:{'stored_rows':v[0],'all_market_rows':v[1]} for day,v in observed.items() if day>='2026-08-01'},
          'universe_codes_marked_in_manifest':sorted(set(m.get('universe_codes') for m in manifests))}
    return d,diag

def features(d: pd.DataFrame) -> pd.DataFrame:
    """Only data through time t for feature calculation. D1..D5 are separate outcomes."""
    d=d.sort_values(['code','date']).copy()
    g=d.groupby('code',sort=False)
    d['ma224_asof']=g['close'].transform(lambda s:s.rolling(224,min_periods=224).mean())
    d['vma20_asof']=g['volume'].transform(lambda s:s.rolling(20,min_periods=20).mean())
    d['vma20_prev20_asof']=g['volume'].transform(lambda s:s.shift(20).rolling(20,min_periods=20).mean())
    d['vma5_asof']=g['volume'].transform(lambda s:s.rolling(5,min_periods=5).mean())
    d['vol20_to_prev20']=d['vma20_asof']/d['vma20_prev20_asof'].replace(0,np.nan)
    d['vol5_to_prev20']=d['vma5_asof']/d['vma20_prev20_asof'].replace(0,np.nan)
    d['close_vs_ma224_pct']=(d['close']/d['ma224_asof']-1)*100
    d['prior40_low_asof']=g['low'].transform(lambda s:s.rolling(40,min_periods=20).min())
    d['rise_from_prior40low_pct']=(d['close']/d['prior40_low_asof']-1)*100
    d['prior1_close']=g['close'].shift(1)
    d['gap_change_abs']=((d['close']/d['prior1_close']-1)).abs()
    d['corp_jump_suspect_asof']=g['gap_change_abs'].transform(lambda s:s.rolling(224,min_periods=1).max().gt(0.35))
    d['code_day_valid_asof']=(d['open'].gt(0)&d['high'].gt(0)&d['low'].gt(0)&d['close'].gt(0)&d['volume'].gt(0))
    # shift means future outcome (NOT feature); point-index alignment will be checked with global sessions
    return d

def build_cohort(d:pd.DataFrame,selected:pd.DataFrame,all_days:list[str]) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    sigdays=sorted(set(selected['signal_date']))
    valid_days=[x for x in sigdays if x in set(all_days)]
    check(len(valid_days)==len(sigdays), 'SIGNAL_DATE_OUT_OF_KRX_RANGE')
    # only D0 market observations; no future OHLCV in predictor fields
    cols=['code','date','close','open','high','low','volume','amount','ma224_asof','vol20_to_prev20',
          'vol5_to_prev20','close_vs_ma224_pct','rise_from_prior40low_pct','corp_jump_suspect_asof','code_day_valid_asof']
    a=d.loc[d.date.isin(valid_days),cols].copy().rename(columns={'date':'signal_date','close':'d0_close','volume':'d0_volume'})
    chosen=selected[['signal_date','code','mode','score']].copy() if 'score' in selected else selected[['signal_date','code','mode']].copy()
    # One (date,code) may be selected in multiple strategies. Never inflate denominator with modes.
    pairs=chosen.groupby(['signal_date','code'],sort=False).agg(selected_modes=('mode',lambda x:','.join(sorted(set(map(str,x))))), selected_mode_rows=('mode','size')).reset_index()
    a=a.merge(pairs,on=['signal_date','code'],how='left',validate='one_to_one',indicator='selection_match')
    a['selected_any_mode']=a.selection_match.eq('both')
    a=a.drop(columns='selection_match')
    # available future official days. Missing stock data on D+1 or D+5 => no outcome.
    all_day_pos={x:i for i,x in enumerate(all_days)}
    sub=[]
    for date in valid_days:
        j=all_day_pos[date]
        if j+5 <len(all_days):
            sub.extend([(date,all_days[j+1],all_days[j+5])])
    mapped=pd.DataFrame(sub,columns=['signal_date','d1_day','d5_day'])
    a=a.merge(mapped,on='signal_date',how='left',validate='many_to_one')
    simple=d[['date','code','open','close','volume']]
    d1=simple.rename(columns={'date':'d1_day','open':'d1_open','volume':'d1_volume'}).drop(columns=['close'])
    d5=simple.rename(columns={'date':'d5_day','close':'d5_close','volume':'d5_volume'}).drop(columns=['open'])
    a=a.merge(d1,on=['d1_day','code'],how='left',validate='many_to_one')
    a=a.merge(d5,on=['d5_day','code'],how='left',validate='many_to_one')
    a['has_ma224']=a['ma224_asof'].notna()
    a['asof_quality_gate']=(a['has_ma224']&a['code_day_valid_asof']&~a['corp_jump_suspect_asof'])
    a['outcome_eligible']=(a['asof_quality_gate']&a['d1_open'].gt(0)&a['d1_volume'].gt(0)&a['d5_close'].gt(0)&a['d5_volume'].gt(0))
    a['gross_d1open_d5close_pct']=np.where(a.outcome_eligible,(a.d5_close/a.d1_open-1)*100,np.nan)
    a['illustrative_net_05pct']=a['gross_d1open_d5close_pct']-0.5
    a['illustrative_net_win']=np.where(a.outcome_eligible,a.illustrative_net_05pct>0,np.nan)
    a['data_scope']='R171_KRX_STORED_907_CODE_SELECTION_NOT_PIT_MARKET'
    a['analysis_authority']='DESCRIPTIVE_RESEARCH_ONLY'
    # RAW eligible comparand is only stored selected subset, not full KRX (~2400 stocks).
    rows=[]
    for scope, q in [('COVERED_ALL',a),('R171_SELECTED_ANY_MODE',a[a.selected_any_mode]),('COVERED_NOT_SELECTED',a[~a.selected_any_mode])]:
        clean=q[q.outcome_eligible]
        rows.append({'scope':scope,'d0_code_days':len(q),'d0_pit_quality_count':int(q.asof_quality_gate.sum()),'d1_d5_evaluable':len(clean),
                     'net05_win_pct':float(clean.illustrative_net_win.mean()*100) if len(clean) else None,
                     'gross_mean_pct':float(clean.gross_d1open_d5close_pct.mean()) if len(clean) else None,
                     'net05_mean_pct':float(clean.illustrative_net_05pct.mean()) if len(clean) else None,
                     'scope_warning':'NOT_FULL_MARKET; NOT_PIT_UNIVERSE; SELECTED_CODE_SET_FROM_FUTURE'})
    summary=pd.DataFrame(rows)
    # code-day denominators: same date universe, same D1/D5 outcomes. Hindsight wins for evaluation only.
    daily=a.groupby('signal_date',as_index=False).agg(covered_count=('code','size'),selected_count=('selected_any_mode','sum'),
        asof_clean_count=('asof_quality_gate','sum'),evaluable_count=('outcome_eligible','sum'),d5_pending_count=('d5_day',lambda x:x.isna().sum()))
    return a,summary,daily

def build_date_matched_benchmark(cohort: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Date-stratified context only: same dates, NOT causal or valid full-market recall.

    Date-wise comparator must not use any future return to choose comparison stocks.
    Returns are joined only after membership and price eligibility were fixed.
    """
    clean=cohort[cohort.outcome_eligible].copy()
    sel=clean[clean.selected_any_mode].copy()
    control=clean[~clean.selected_any_mode].copy()
    controls=control.groupby('signal_date').agg(
        baseline_n=('code','size'), baseline_net_mean=('illustrative_net_05pct','mean'),
        baseline_win_pct=('illustrative_net_win', lambda x:float(x.mean()*100)))
    if sel.empty or control.empty:
        return pd.DataFrame(),pd.DataFrame()
    matched=sel[['signal_date','code','selected_modes','illustrative_net_05pct','illustrative_net_win']].join(controls,on='signal_date',how='left')
    matched['selected_minus_same_date_control_net_pct']=matched.illustrative_net_05pct-matched.baseline_net_mean
    matched['scope_label']='DESCRIPTIVE_NOT_PIT_NOT_RANDOMIZED'
    byday=matched.groupby('signal_date',as_index=False).agg(
        selected_n=('code','size'), control_n=('baseline_n','first'),
        selected_mean_net_pct=('illustrative_net_05pct','mean'),
        control_mean_net_pct=('baseline_net_mean','first'),
        selected_minus_control_net_pct=('selected_minus_same_date_control_net_pct','mean'))
    return matched,byday

def parent_shadow_compare(parent_path:Path,shadow_path:Path)->tuple[pd.DataFrame,dict]:
    """Parent saved source CSV to rendered shadow rows. Absent rows are not assumed scanner false negatives."""
    with ZipFile(parent_path) as z:
        ps=[n for n in z.namelist() if n.endswith('.csv') and '/live_shadow_execution_' in n]
        ms=[n for n in z.namelist() if '/live_shadow_source_meta_' in n and n.endswith('.json')]
        check(len(ps)==len(ms)==1,'PARENT_SOURCE_META_COUNT_NOT_ONE')
        parent=pd.read_csv(io.BytesIO(z.read(ps[0])),dtype={'code':str},low_memory=False)
        m=json.loads(z.read(ms[0]));check(len(parent)==int(m['rows']),'PARENT_ROW_MISMATCH')
    with ZipFile(shadow_path) as z:
        childmeta=json.loads(z.read('meta.json'))
        check(childmeta['status']=='PASS','CHILD_NOT_PASS')
        check('shadow_board_structured.csv' in z.namelist(),'SHADOW_STRUCTURED_MISSING')
        board=pd.read_csv(io.BytesIO(z.read('shadow_board_structured.csv')),dtype={'code':str},low_memory=False)
    check(len(board)==int(childmeta['rows']),'SHADOW_ROW_MISMATCH')
    parent['code']=parent['code'].map(six)
    board['code']=board['code'].map(six)
    check(parent.code.is_unique and board.code.is_unique,'DUPLICATE_PARENT_SHADOW_CODE')
    check(set(board.code).issubset(set(parent.code)),'CHILD_NOT_SUBSET_OF_PARENT')
    out=parent[[x for x in ['code','name','signal_date','decision_decision','decision_stage','shadow_pattern_explicit','mode','shadow_final_rank'] if x in parent]].copy()
    out['shadow_displayed']=out.code.isin(set(board.code))
    out['absence_observation']=np.where(out.shadow_displayed,'RENDERED',
        np.where(out.decision_decision.astype(str).eq('REPLAY'),'REPLAY_EXCLUDED',
        np.where(out.decision_decision.astype(str).eq('EXCLUDE'),'EXCLUDE_EXCLUDED',
        np.where(out.signal_date.isna(),'MISSING_SIGNAL_DATE_NEEDS_SOURCE_AUDIT','OTHER_NOT_RENDERED_REQUIRES_CODE_TRACE'))))
    out['source_run_id']=childmeta['source_run_id']
    info={'source_run_id':childmeta['source_run_id'],'source_rows':len(parent),'shadow_rows':len(board),
          'not_rendered':int((~out.shadow_displayed).sum()),'parent_lane':m.get('effective_lane'),
          'child_status':childmeta['status'],'source_kind':m.get('source_kind')}
    return out,info

def shadow_inventory(shadow_dir:Path)->pd.DataFrame:
    rows=[]
    for path in sorted(shadow_dir.glob('*.zip')):
        with ZipFile(path) as z:
            check('meta.json' in z.namelist(),'SHADOW_META_MISSING '+path.name)
            m=json.loads(z.read('meta.json'))
            status=m.get('status',''); check(status in ('PASS','SKIP_NON_CANDIDATE_LANE','NO_LIVE_SIDECAR'), 'SHADOW_UNKNOWN_STATUS '+path.name)
            rows.append({'artifact_name':path.name,'artifact_sha256':sha(path.read_bytes()),'source_run_id':m.get('source_run_id'),
                         'status':status,'source_lane':m.get('source_lane'), 'n_board_rows':int(m.get('rows') or 0),
                         'telegram_delivery_status':m.get('telegram_delivery_status'),
                         'research_only':m.get('research_only'), 'source':m.get('source'),
                         'future_probability_calibration':'NOT_PROVEN'})
    check(rows,'NO_SHADOW_ARTIFACTS')
    return pd.DataFrame(rows)

def write_outputs(out:Path, selected:pd.DataFrame, diag:dict, cohort:pd.DataFrame, summary:pd.DataFrame,
                  daily:pd.DataFrame, shadow:pd.DataFrame, cross:pd.DataFrame|None, crossmeta:dict|None, selected_sha:str)->dict:
    out.mkdir(parents=True,exist_ok=True)
    files={
     'r117_covered_cohort_d0_features_d1open_d5close.csv':cohort,
     'r117_selected_vs_covered_outcome_descriptive.csv':summary,
     'r117_daily_covered_denominator_coverage.csv':daily,
     'r117_shadow_execution_ledger.csv':shadow,
    }
    matched,byday=build_date_matched_benchmark(cohort)
    scope=[]
    for day in sorted(set(selected.signal_date)):
        val=diag['recent_daily_source_scope'].get(day)
        check(val is not None,'MISSING_DENOM_SOURCE_DAY '+day)
        scope.append({'signal_date':day,'stored_krx_source_rows':val['stored_rows'],
                      'full_market_krx_source_rows':val['all_market_rows'],
                      'stored_source_share_pct':round(100*val['stored_rows']/val['all_market_rows'],3),
                      'historic_universe_warning':'STORED_SUBSET_NOT_FULL_MARKET_NO_PIT_UNIVERSE'})
    files['r117_source_coverage_vs_full_market_45_dates.csv']=pd.DataFrame(scope)
    files['r117_date_matched_selected_vs_control.csv']=matched
    files['r117_date_matched_daily_aggregate.csv']=byday
    if cross is not None:files['r117_parent_shadow_reason_crosswalk.csv']=cross
    hashes={}
    for f,df in files.items():
        p=out/f;df.to_csv(p,index=False,encoding='utf-8-sig'); hashes[f]=sha(p.read_bytes())
    counts={k:int(v) for k,v in shadow['status'].value_counts().items()}
    manifest={
     'version':VERSION,'status':'PASS_DESCRIPTIVE_PARTIAL_DENOMINATOR_NO_PROSPECTIVE_EDGE_CLAIM',
     'research_only':True,'live_authority':False,'production_search_changed':False,'shadow_production_changed':False,
     'selected_source_run':RUN_171,'r171_selected_sha256':selected_sha,
     'selected_rows':len(selected),'selected_unique_code_days':int(selected[['signal_date','code']].drop_duplicates().shape[0]),
     'selected_days':int(selected.signal_date.nunique()),'krx':diag,
     'covered_cohort_rows':len(cohort),'cohort_outcome_evaluable':int(cohort.outcome_eligible.sum()),
     'shadow_run_count':len(shadow),'shadow_run_status':counts,
     'date_matched_evaluable_selected':len(matched), 'date_matched_days':len(byday),
     'date_matched_mean_daily_difference_net_pct':float(byday.selected_minus_control_net_pct.mean()) if len(byday) else None,
     'parent_shadow':crossmeta,'file_sha256':hashes,
     'limits':{'true_pit_full_market_denominator':'NOT_PROVEN',
               'retrospective_stored_universe_survivorship':'YES',
               'opportunity_recall_true_full_market':'NOT_PROVEN',
               'raw_v49_predicate_hits':'NOT_INCLUDED_IN_R171_SELECTED_EVIDENCE',
               'r1_7_9_prospective_probability_calibration':'NOT_PROVEN',
               'execution_fills':'NOT_PROVEN',
               'historical_adjustment':'NOT_PROVEN',
               'KRX_1530_precise_session':'NOT_PROVEN',
               'D0_15_03_features_with_D0_close':'FORBIDDEN_AS_OF_15_03',
               'cost_assumption_pct':'0.50 illustrative only',
               'returns_label_only_not_feature':True},
     'decision':'DO_NOT_MODIFY_PRODUCTION_OR_TRADE_ON_OUTPUT'}
    mp=out/'r117_integrity_manifest.json';mp.write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    return manifest

def main(argv=None):
    pa=argparse.ArgumentParser()
    pa.add_argument('--r171-root',type=Path,required=True)
    pa.add_argument('--history-dir',type=Path,required=True)
    pa.add_argument('--shadow-dir',type=Path,required=True)
    pa.add_argument('--parent-artifact',type=Path)
    pa.add_argument('--child-artifact',type=Path)
    pa.add_argument('--out',type=Path,required=True)
    args=pa.parse_args(argv)
    selected,s_sha=read_selected(args.r171_root)
    d,diag=load_shards(args.history_dir)
    g=features(d)
    days=sorted(set(g.date))
    cohort,summary,daily=build_cohort(g,selected,days)
    shadow=shadow_inventory(args.shadow_dir)
    cross=crossmeta=None
    if args.parent_artifact or args.child_artifact:
        check(bool(args.parent_artifact) and bool(args.child_artifact),'PARENT_CHILD_BOTH_REQUIRED')
        cross,crossmeta=parent_shadow_compare(args.parent_artifact,args.child_artifact)
    man=write_outputs(args.out,selected,diag,cohort,summary,daily,shadow,cross,crossmeta,s_sha)
    print('R117_RESEARCH_PASS',json.dumps({k:man[k] for k in ['status','selected_rows','selected_unique_code_days','covered_cohort_rows','cohort_outcome_evaluable','shadow_run_count','shadow_run_status','parent_shadow']},ensure_ascii=False))

if __name__=='__main__':main()
