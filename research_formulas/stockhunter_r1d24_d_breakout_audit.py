#!/usr/bin/env python3
"""R1-D24 RESEARCH_ONLY: frozen D breakout diagnostics on immutable R1-D23 results.

Historical descriptive comparisons, NOT independent OOS, not production or trading.
All factor definitions locked before any return path is read. Do not recompute A-D.
"""
from __future__ import annotations
import argparse
import hashlib
import io
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

DONE = 'PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
PREFIX = 'reports/r1d23/result/historical/'


def fail(reason):
    raise RuntimeError('R1D24_FAIL_CLOSED_' + str(reason))


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def write_csv(frame, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding='utf-8-sig')


def write_json(obj, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True, default=str) + '\n', encoding='utf-8')


def read_protocol(path):
    x = json.loads(Path(path).read_text('utf-8'))
    if x.get('research_id') != 'R1D24_FROZEN_R1D23_D_BREAKOUT_TAIL_RISK_RESEARCH_V1':
        fail('PROTOCOL_NOT_FIXED')
    if not (x.get('research_only') and x.get('no_threshold_optimization_after_outcome_read')
            and x.get('periods_are_reused_historical_not_new_OOS')
            and x.get('no_live_rank_search_entry_exit_order_changes')):
        fail('PROTOCOL_GUARDS_CHANGED')
    if len(x['binary_conditions_frozen_before_return_join']) != 11:
        fail('BINARY_FACTOR_COUNT_DRIFT')
    if len(x['combinations_frozen_before_return_join']) != 3:
        fail('COMBINATION_COUNT_DRIFT')
    return x


def extract_verified_asof(source_zip, p):
    if digest(Path(source_zip).read_bytes()) != p['source']['artifact_zip_sha256']:
        fail('SOURCE_GITHUB_ARTIFACT_ZIP_SHA_MISMATCH')
    with zipfile.ZipFile(source_zip, 'r') as z:
        raw = z.read(PREFIX + 'R1D23_FULLMARKET_A_B_C_D_ASOF_ONLY.csv.gz')
        manifest = json.loads(z.read(PREFIX + 'R1D23_MANIFEST.json'))
        if digest(raw) != p['source']['asof_gz_sha256']:
            fail('FROZEN_SIGNAL_ASOF_SHA_MISMATCH')
        if manifest['asof_feature_sha256'] != digest(raw):
            fail('ASOF_MANIFEST_SHA_DRIFT')
        if (manifest['fullmarket_signal_rows'] != p['source']['expected_all_events']
            or manifest['signal_counts_by_strategy'][p['strategy_target']] != p['source']['expected_D_events']
            or manifest['price_path_rows'] != p['source']['expected_paths']
            or manifest['krx_sha256_all_days_verified'] != 1907
            or not manifest['no_production_changes'] or manifest['orders_placed'] != 0):
            fail('ORIGINAL_R1D23_PROVENANCE_DRIFT')
    f = pd.read_csv(io.BytesIO(raw), compression='gzip', dtype={'date':str,'code':str}, low_memory=False)
    keys = ['strategy', 'date', 'code']
    if len(f) != p['source']['expected_all_events'] or f[keys].duplicated().any():
        fail('DUPLICATE_OR_MISSING_R1D23_KEYS')
    if not f.date.eq(f.asof_last_date).all() or not f.asof_status.eq('ASOF_OK').all():
        fail('ASOF_TIME_OR_VALIDITY_DRIFT')
    if any(t in f.columns for t in ('net_20bps_pct','mfe_intraday_pct','mae_intraday_pct','status_out','exit_day')):
        fail('OUTCOME_IN_FROZEN_ASOF')
    d = f[f.strategy.eq(p['strategy_target'])].copy()
    if len(d) != p['source']['expected_D_events']:
        fail('TARGET_D_COUNT_DRIFT')
    if set(f.period) != set(p['populations']):
        fail('PERIOD_DRIFT')
    return f, d, manifest


def define_flags(d, p):
    # Runs ONLY on original signal-day feature values; no return data available here.
    a = d.copy()
    fixed = p['binary_conditions_frozen_before_return_join']
    for name, spec in fixed.items():
        v = pd.to_numeric(a[spec['column']], errors='coerce')
        if not (np.isfinite(v).all() and not v.isna().any()):
            fail('FACTOR_MISSING_ON_D_' + name)
        if spec['op'] == 'le': flag = v <= float(spec['value'])
        elif spec['op'] == 'ge': flag = v >= float(spec['value'])
        else: fail('UNAPPROVED_OPERATOR_' + str(spec['op']))
        a[name] = flag.astype('int8')
    for name, parts in p['combinations_frozen_before_return_join'].items():
        if not parts or any(f not in fixed for f in parts):
            fail('UNKNOWN_COMBINATION_COMPONENT_' + name)
        a[name] = a[parts].all(axis=1).astype('int8')
    return a, list(fixed) + list(p['combinations_frozen_before_return_join'])


def read_outcomes(source_zip, p, all_asof):
    # Caller writes and seals signal-time flags before calling this function.
    with zipfile.ZipFile(source_zip, 'r') as z:
        raw = z.read(PREFIX + 'R1D23_FROZEN_FUTURE_PATHS.csv.gz')
    if digest(raw) != p['source']['paths_gz_sha256']:
        fail('FROZEN_OUTCOME_SHA_DRIFT')
    x = pd.read_csv(io.BytesIO(raw), compression='gzip', dtype={'date':str,'code':str},low_memory=False)
    keys = ['strategy','date','code']
    if len(x) != p['source']['expected_paths'] or x[keys + ['horizon']].duplicated().any():
        fail('FROZEN_OUTCOME_ROWS_OR_KEYS_DRIFT')
    if set(x.horizon) != set(p['horizons']): fail('UNEXPECTED_HORIZONS')
    n = x.groupby(keys).horizon.nunique()
    if not len(n) == len(all_asof) or not n.eq(4).all():
        fail('FOUR_PATHS_PER_SIGNAL_MISSING')
    if set(map(tuple, x[keys].drop_duplicates().to_numpy())) != set(map(tuple, all_asof[keys].to_numpy())):
        fail('PATH_KEYS_DO_NOT_MATCH_ASOF')
    # censored rows remain in raw tables and are excluded only from outcome statistics
    return x


def summarize(g, period, flag, value, horizon, segment):
    valid = g[g.status_out.eq(DONE)]
    n = len(valid)
    returns = valid.net_20bps_pct
    if n and (returns.isna().any() or not np.isfinite(returns).all()):
        fail('FINISHED_PATH_MISSING_RETURN')
    win = returns[returns > 0]
    loss = returns[returns <= 0]
    return {'period':period, 'segment':segment, 'flag':flag,'flag_value':value,
            'horizon':int(horizon), 'signals':len(g),'complete':n,'censored':len(g)-n,
            'unique_dates':valid.date.nunique(), 'unique_codes':valid.code.nunique(),
            'win_pct':100*returns.gt(0).mean() if n else np.nan,
            'mean_net_pct':returns.mean() if n else np.nan,
            'median_net_pct':returns.median() if n else np.nan,
            'final_ge10_pct':100*returns.ge(10).mean() if n else np.nan,
            'final_ge15_pct':100*returns.ge(15).mean() if n else np.nan,
            'loss_le_minus5_pct':100*returns.le(-5).mean() if n else np.nan,
            'profit_factor':win.sum()/abs(loss.sum()) if n and loss.sum()<0 else np.nan,
            'mfe_ge15_pct':100*valid.mfe_intraday_pct.ge(15).mean() if n else np.nan,
            'mfe_ge20_pct':100*valid.mfe_intraday_pct.ge(20).mean() if n else np.nan,
            'mfe_ge15_final_nonpositive_pct':100*((valid.mfe_intraday_pct >= 15)&returns.le(0)).mean() if n else np.nan,
            'median_mae_pct':valid.mae_intraday_pct.median() if n else np.nan,
            'median_mfe_pct':valid.mfe_intraday_pct.median() if n else np.nan,
            'small_sample_warning':bool(n<30 or valid.date.nunique()<8),
            'future_prospective_OOS':False}



def verify_parent_D_scores(source_zip, p, score):
    """Independent scoreboard parity vs source run, tolerant only of 3-decimal display rounding."""
    with zipfile.ZipFile(source_zip, 'r') as z:
        ref=pd.read_csv(io.BytesIO(z.read(PREFIX+'R1D23_FOUR_STRATEGY_SCORECARDS.csv')))
    q=ref[ref.strategy.eq(p['strategy_target'])]
    b=score[score.segment.eq('D_BASELINE')]
    if len(q)!=8 or len(b)!=8:
        fail('PARENT_D_SCORECARD_CELLS_DRIFT')
    joint=b.merge(q,on=['period','horizon'],validate='one_to_one',suffixes=('_local','_parent'))
    if len(joint)!=8: fail('PARENT_D_SCORECARD_JOIN_DRIFT')
    for k in ('complete','win_pct','mean_net_pct','median_net_pct','loss5_pct'):
        lk='loss_le_minus5_pct' if k=='loss5_pct' else k+'_local'
        rk=k if k=='loss5_pct' else k+'_parent'
        if any(abs(joint[lk].astype(float)-joint[rk].astype(float))>.011):
            fail('PARENT_D_SCORE_DRIFT_'+k)
    return {'cells_checked':len(joint),'source_baseline_parity':True,
            'allowed_rounding_tolerance_percent_points':.011}


def date_match_within_D(g, name):
    """Vectorized equal-date-weighted comparison. No industry/ticker matching."""
    if g.empty:return {'flag':name,'matched_dates':0,'mean_net_delta_pp':np.nan,
        'win_delta_pp':np.nan,'final15_delta_pp':np.nan,'loss5_delta_pp':np.nan,
        'bootstrap_mean_net_ci_lo':np.nan,'bootstrap_mean_net_ci_hi':np.nan}
    a=g.assign(_win=g.net_20bps_pct.gt(0).astype(float),
               _final15=g.net_20bps_pct.ge(15).astype(float),
               _loss5=g.net_20bps_pct.le(-5).astype(float))
    summary=(a.groupby(['date',name])
        .agg(n=('code','size'),ret=('net_20bps_pct','mean'),win=('_win','mean'),
             strong=('_final15','mean'),loss=('_loss5','mean')))
    have=set(summary.index.get_level_values(name).unique())
    if not {0,1}.issubset(have):
        return {'flag':name,'matched_dates':0,'mean_net_delta_pp':np.nan,
                'win_delta_pp':np.nan,'final15_delta_pp':np.nan,'loss5_delta_pp':np.nan,
                'bootstrap_mean_net_ci_lo':np.nan,'bootstrap_mean_net_ci_hi':np.nan}
    yes=summary.xs(1,level=name)
    no=summary.xs(0,level=name)
    matched=yes.join(no,how='inner',lsuffix='_yes',rsuffix='_no')
    if matched.empty:
        return {'flag':name,'matched_dates':0,'mean_net_delta_pp':np.nan,
                'win_delta_pp':np.nan,'final15_delta_pp':np.nan,'loss5_delta_pp':np.nan,
                'bootstrap_mean_net_ci_lo':np.nan,'bootstrap_mean_net_ci_hi':np.nan}
    mean_delta=matched.ret_yes-matched.ret_no
    rng=np.random.default_rng(20261026+len(name))
    vals=mean_delta.to_numpy(dtype=float)
    draws=vals[rng.integers(0,len(vals),size=(800,len(vals)))].mean(axis=1)
    lo,hi=np.quantile(draws,[.025,.975])
    return {'flag':name,'matched_dates':len(matched),
            'yes_signals_across_matches':int(matched.n_yes.sum()),
            'no_signals_across_matches':int(matched.n_no.sum()),
            'mean_net_delta_pp':mean_delta.mean(),
            'win_delta_pp':100*(matched.win_yes-matched.win_no).mean(),
            'final15_delta_pp':100*(matched.strong_yes-matched.strong_no).mean(),
            'loss5_delta_pp':100*(matched.loss_yes-matched.loss_no).mean(),
            'bootstrap_mean_net_ci_lo':lo,'bootstrap_mean_net_ci_hi':hi,
            'CI_is_exploratory_not_multiplicity_corrected':True}


def compare_date_vs_controls(paths, marked, p):
    # D all & simple signal-day filter candidates vs A/C on overlapping dates.
    d = paths[paths.strategy.eq(p['strategy_target']) & paths.horizon.eq(20) & paths.status_out.eq(DONE)]
    d = d.merge(marked[['date','code']+list(p['binary_conditions_frozen_before_return_join'])+list(p['combinations_frozen_before_return_join'])],on=['date','code'],validate='one_to_one')
    base = paths[paths.horizon.eq(20) & paths.status_out.eq(DONE)]
    rows=[]
    selections={'all_D':pd.Series(True,index=d.index),
                'narrow_10d_range_le_10pct':d['narrow_10d_range_le_10pct'].eq(1),
                'rel_amount_ge_3x':d['rel_amount_ge_3x'].eq(1),
                'narrow10_and_rel_amount3':d['narrow10_and_rel_amount3'].eq(1)}
    for period in p['populations']:
        for label,sel in selections.items():
            D=d.loc[sel & d.period.eq(period)]
            for ctrl in p['controls']:
                C=base[(base.period.eq(period)) & (base.strategy.eq(ctrl))]
                a=D.groupby('date').net_20bps_pct.agg(['mean','count']).rename(columns={'mean':'d_mean','count':'d_n'})
                b=C.groupby('date').net_20bps_pct.agg(['mean','count']).rename(columns={'mean':'ctrl_mean','count':'ctrl_n'})
                m=a.join(b,how='inner')
                rows.append({'period':period,'D_selection':label,'control':ctrl,'matched_dates':len(m),
                             'D_signals':int(m.d_n.sum()) if len(m) else 0,
                             'control_signals':int(m.ctrl_n.sum()) if len(m) else 0,
                             'equal_date_mean_net_D_minus_control_pp':(m.d_mean-m.ctrl_mean).mean() if len(m) else np.nan,
                             'no_ticker_industry_matching':True})
    return pd.DataFrame(rows)


def run(source_zip, protocol_path, out, mode):
    p = read_protocol(protocol_path)
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    if mode=='lock':
        write_json({'version':p['research_id'],'protocol_sha256':digest(Path(protocol_path).read_bytes()),
                    'expected_source_zip_sha256':p['source']['artifact_zip_sha256'],
                    'flags':list(p['binary_conditions_frozen_before_return_join'])+list(p['combinations_frozen_before_return_join']),
                    'outcomes_loaded':False,'LIVE_affected':False,'independent_OOS':False},out/'R1D24_LOCK.json')
        return
    if mode!='analyze':fail('UNAPPROVED_MODE')
    # Explicit protocol lock MUST exist before feature and outcome access.
    lock_path=out/'R1D24_LOCK.json'
    if not lock_path.exists():fail('PROTOCOL_LOCK_NOT_RUN_FIRST')
    lock=json.loads(lock_path.read_text('utf-8'))
    if lock['protocol_sha256']!=digest(Path(protocol_path).read_bytes()):fail('LOCK_OR_PROTOCOL_DRIFT')
    all_f,d,m=extract_verified_asof(source_zip,p)
    d,flag_names=define_flags(d,p)
    # Freeze and cryptographically seal factor statuses BEFORE paths read.
    sealed_file=out/'R1D24_D_ONLY_FROZEN_SIGNALTIME_FLAGS_NO_OUTCOME.csv.gz'
    d.to_csv(sealed_file,index=False,compression={'method':'gzip','mtime':0})
    seal=digest(sealed_file.read_bytes())
    write_json({'factors_asof_sealed_sha256':seal,'frozen_D_n':len(d),'no_outcome_in_factors':True,
                'frozen_source_zip_sha256':p['source']['artifact_zip_sha256']},out/'R1D24_ASOF_SEAL.json')
    paths=read_outcomes(source_zip,p,all_f)
    merged=paths.merge(d[['strategy','date','code','period']+flag_names+['base10_range_pct','amount_vs_prior20','amount_KRW','ma224_slope20_pct','distance224_pct']],on=['strategy','date','code','period'],validate='many_to_one',how='inner')
    if len(merged)!=len(d)*4:fail('D_ONLY_MERGE_CARDINALITY')
    result=[]; match=[]
    for period in p['populations']:
        for h in p['horizons']:
            group=merged[(merged.period.eq(period))&(merged.horizon.eq(h))]
            result.append(summarize(group,period,'__ALL_D__',1,h,'D_BASELINE'))
            for flag in flag_names:
                for val in (0,1):
                    result.append(summarize(group[group[flag].eq(val)],period,flag,val,h,'D_WITHIN_FACTOR'))
                if h==20:
                    complete=group[group.status_out.eq(DONE)]
                    match.append({'period':period,'horizon':20,**date_match_within_D(complete,flag)})
    score = pd.DataFrame(result)
    parent_parity=verify_parent_D_scores(source_zip,p,score)
    write_json(parent_parity,out/'R1D24_SOURCE_D_SCORECARD_PARITY.json')
    match=pd.DataFrame(match)
    controls=compare_date_vs_controls(paths,d,p)
    write_csv(score,out/'R1D24_D_FACTOR_SCORECARD_D5_D10_D15_D20.csv')
    write_csv(match,out/'R1D24_D_SAME_DATE_FACTOR_CONTRASTS_D20.csv')
    write_csv(controls,out/'R1D24_D_VS_A_C_SAME_DATE_CONTRASTS_D20.csv')
    # Robustness: by-calendar-year windows are descriptive (ALL years already exposed).
    annual=[]
    for yr, frame in merged[merged.horizon.eq(20)].groupby(merged.date.str[:4], sort=True):
        annual.append(summarize(frame,'YEAR_'+str(yr),'__ALL_D__',1,20,'YEAR_ALL_D'))
        for flag in ('narrow_10d_range_le_10pct','rel_amount_ge_3x','narrow10_and_rel_amount3'):
            annual.append(summarize(frame[frame[flag].eq(1)],'YEAR_'+str(yr),flag,1,20,'YEAR_D_CANDIDATE'))
    write_csv(pd.DataFrame(annual),out/'R1D24_YEAR_BY_YEAR_D20_SENSITIVITY.csv')
    # Never use these retrospective success/failure labels to change the locked signal definitions.
    profile=[]
    compare_cols=['base10_range_pct','amount_vs_prior20','amount_KRW','ma224_slope20_pct','distance224_pct']
    val=merged[(merged.horizon.eq(20))&(merged.status_out.eq(DONE))].copy()
    val['outcome_bucket']=np.where(val.net_20bps_pct.ge(15),'FINAL_GE15',
                          np.where(val.net_20bps_pct.le(-5),'FINAL_LE_MINUS5','OTHER'))
    for (period,bucket),part in val.groupby(['period','outcome_bucket'],sort=True):
        row={'period':period,'outcome_bucket':bucket,'n':len(part),'unique_dates':part.date.nunique()}
        for col in compare_cols:
            row[col+'_median']=part[col].median()
            row[col+'_p25']=part[col].quantile(.25)
            row[col+'_p75']=part[col].quantile(.75)
        profile.append(row)
    write_csv(pd.DataFrame(profile),out/'R1D24_D_BIGWIN_FAIL_ASOF_PROFILES_DESCRIPTIVE.csv')
    # No conditional future-data filtering in flags; data quality report keeps censor status.
    write_csv(merged.groupby(['period','horizon','status_out'],dropna=False).size().reset_index(name='n'),out/'R1D24_CENSORED_PRICE_PATH_AUDIT.csv')
    summary={'research_id':p['research_id'],'source_run':p['source']['run_id'],
             'source_artifact_id':p['source']['artifact_id'],
             'frozen_original_market_sha_verified_by_R1D23':1907,
             'original_A_B_C_D_allmarket_signals':len(all_f),'D_signals':len(d),
             'D_path_rows':len(merged),'source_path_rows':len(paths),
             'D_dates':int(d.date.nunique()),'D_codes':int(d.code.nunique()),
             'D_by_period':d.period.value_counts().to_dict(),
             'asof_sha256':seal,'protocol_sha256':lock['protocol_sha256'],
             'frozen_factor_count':len(flag_names),'scorecard_rows':len(score),
             'same_day_factor_rows':len(match),'same_day_controls_rows':len(controls),
             'historical_reuse_not_new_OOS':True,'selection_multiple_testing_risk':True,
             'source_immutable':True,'parent_scorecard_parity_pass':parent_parity['source_baseline_parity'],'R1D23_strategy_selection_and_pricing_unchanged':True,
             'data_corporate_actions_tradeability_not_PIT_certified':True,
             'live_changed':False,'orders_placed':0,'strategy_ready':False}
    write_json(summary,out/'R1D24_MANIFEST.json')
    print('R1D24_RESEARCH_ONLY_COMPLETE',json.dumps(summary,ensure_ascii=False),flush=True)


def main():
    q=argparse.ArgumentParser()
    q.add_argument('--mode',choices=['lock','analyze'],required=True)
    q.add_argument('--source-zip',default='')
    q.add_argument('--protocol',default='R1D24_PREDECLARED_PROTOCOL.json')
    q.add_argument('--out',default='reports/r1d24/result')
    a=q.parse_args()
    run(a.source_zip,a.protocol,a.out,a.mode)

if __name__=='__main__':main()
