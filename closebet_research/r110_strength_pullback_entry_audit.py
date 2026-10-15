"""R1.10: pinned R1.8/R1.9 evidence; strictly descriptive A/B/C research, no LIVE.

Only pre-signal features enter rule flags. Forward outcomes are kept separately.
No policy fitting, threshold optimization, or account/trading actions.
Python 3.10 compatible. Fail closed on source identity, schema, cohort, or duplicates.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

SOURCE_RUNS = {'r171': '38664095610', 'r18': '38693918216', 'r19': '38729925104'}
EXPECTED_SHA = {
    'r18_all_unique_signal_features.csv': '801f3eff3b51437a0addcd7e270dd6fc5fe6feb3329a5eacce2fec3c949ad179',
    'r18_entry_policy_trade_level.csv': 'b73989d2c2b7272e5f5559377717d5123342e1e57448283153e7bcc279c3e758',
    'r18_research_manifest.json': '3824d104fd6dd9ffe70d91a9dfae0aa44e52c5ecdd9702256520a4952cb261f4',
    'r19_integrity_manifest.json': '1c26ca5c815326ce0e3be1f47dc49be7567f0e552bf17343cfedc235a8579314',
    'r19_matched_contrast_pairs.csv': '093923c39a9462db088312edfa040f50154b2fa9c22d32865abbd29535d9ad85',
    'r19_corporate_action_suspect_exclusions.csv': '1ec45b7900deeaa334e346c512888d41d6c59b5956815f28aa6c894f68568b7f',
}
PRESIGNAL_NUMERIC = (
    'prewave_impulse_pct', 'prewave_pullback_vol_ratio', 'prewave_retrace_fraction',
    'prewave_prevolume_accum_ratio', 'ma20_signal_dist_pct', 'ma224_dist_signal_pct',
    'volume_dryup_10_30', 'candle_close_location',
)
FLAG_NAMES = (
    'impulse_ge30', 'impulse_ge50', 'dryup10_lt070', 'dryup10_lt100',
    'pullbackvol_lt070', 'retrace_20_80', 'origin_below224',
    'signal_below224', 'wave_stage4', 'ma20_extended_ge5',
    'impulse_ge30_and_dryup10_lt070',
    'retrace_20_80_and_pullbackvol_lt070',
)
TARGETS = ('D5_close_pct', 'D5_max_high_pct', 'D5_min_low_pct')
MODELS = ('NEXT_OPEN', 'BREAKOUT_CONFIRMED', 'PULLBACK_RECLAIM', 'MA20_RECOVERY')


def require(v, reason):
    if not bool(v):
        raise RuntimeError('R110_FAIL_CLOSED:' + str(reason))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_source(r18, r19):
    pairs = {
        'r18_all_unique_signal_features.csv': r18,
        'r18_entry_policy_trade_level.csv': r18,
        'r18_research_manifest.json': r18,
        'r19_integrity_manifest.json': r19,
        'r19_matched_contrast_pairs.csv': r19,
        'r19_corporate_action_suspect_exclusions.csv': r19,
    }
    for filename, base in pairs.items():
        p = Path(base)/filename
        require(p.is_file(), 'MISSING_PINNED_SOURCE:'+filename)
        require(sha(p)==EXPECTED_SHA[filename], 'SOURCE_HASH_MISMATCH:'+filename)
    m18 = json.loads((Path(r18)/'r18_research_manifest.json').read_text(encoding='utf-8'))
    m19 = json.loads((Path(r19)/'r19_integrity_manifest.json').read_text(encoding='utf-8'))
    require(m18['source_run_id']==SOURCE_RUNS['r171'], 'R18_UPSTREAM_RUN_MISMATCH')
    require(m18['unique_signal_date_codes']==754 and m18['D20_mature_unique']==570, 'R18_COHORT_MISMATCH')
    require(m19['source_runs']['r171']==SOURCE_RUNS['r171'] and m19['source_runs']['r18']==SOURCE_RUNS['r18'], 'R19_UPSTREAM_RUN_MISMATCH')
    require(m19['all_native_unique_signal_price_bars_verified']==1059, 'R19_BAR_COUNT_MISMATCH')
    require(m19['R18_feature_rows_recalculated']==754 and m19['D20_outcomes_independently_recalculated']==570, 'R19_INTEGRITY_GATE_FAILED')
    require(m19['status']=='PASS_WITH_KNOWN_LIMITATIONS', 'R19_STATUS_NOT_PASS')
    for fn in ('r19_matched_contrast_pairs.csv','r19_corporate_action_suspect_exclusions.csv'):
        require(m19['output_sha256'][fn]==EXPECTED_SHA[fn], 'R19_MANIFEST_HASH_MISMATCH:'+fn)
    return m18,m19


def load_events(folder):
    e=pd.read_csv(Path(folder)/'r18_all_unique_signal_features.csv', dtype={'code':str}, low_memory=False)
    cols={'signal_date','code','all_modes','D20_complete','D5_complete','D5_close_pct',
          'D5_max_high_pct','D5_min_low_pct','D20_close_pct','D20_max_high_pct',
          'D20_min_low_pct','outcome_class','wave_setup_stage','prewave_trough_below224',
          'prior_224_jump_above35pct'}|set(PRESIGNAL_NUMERIC)
    require(not (cols-set(e.columns)), 'MISSING_EVENT_FIELDS:'+str(sorted(cols-set(e.columns))))
    require(len(e)==754, 'EVENT_COUNT_NOT_754')
    require(e[['signal_date','code']].notna().all().all(), 'NULL_EVENT_ID')
    require(e.code.str.fullmatch('[0-9]{6}').all(), 'INVALID_KRX_CODE')
    require(pd.to_datetime(e.signal_date,format='%Y-%m-%d',errors='coerce').notna().all(), 'BAD_SIGNAL_DATE')
    require(not e[['signal_date','code']].duplicated().any(), 'DUPLICATE_KRX_ID')
    require(set(e.D20_complete.unique())=={0,1} and e.D5_complete.eq(1).all(), 'MATURITY_STATUS_INVALID')
    old=e[e.D20_complete.eq(1)]
    new=e[e.D20_complete.eq(0)]
    require(len(old)==570 and len(new)==184, 'COHORT_SPLIT_WRONG')
    require((old.signal_date<'2026-09-21').all() and (new.signal_date>='2026-09-21').all(), 'RECENT_HORIZON_VIOLATION')
    require(new.D20_close_pct.isna().all(), 'FUTURE_D20_DATA_LEAK')
    require(old[['D5_close_pct','D5_max_high_pct','D5_min_low_pct','D20_close_pct','D20_max_high_pct','D20_min_low_pct']].notna().all().all(), 'MATURE_OUTCOMES_MISSING')
    require(((e.D5_max_high_pct + 1e-8)>=e.D5_close_pct).all(), 'D5_MFE_LT_CLOSE')
    require(((e.D5_min_low_pct - 1e-8)<=e.D5_close_pct).all(), 'D5_MAE_GT_CLOSE')
    require(((old.D20_max_high_pct + 1e-8)>=old.D20_close_pct).all(), 'D20_MFE_LT_CLOSE')
    require(((old.D20_min_low_pct - 1e-8)<=old.D20_close_pct).all(), 'D20_MAE_GT_CLOSE')
    expected={"STRONG_PERSISTENT":145,"OTHER_PROFIT":91,"GIVEBACK":153,"NO_5PCT_RISE":181}
    require(old.outcome_class.value_counts().to_dict()==expected, 'CLASS_RECALC_MISMATCH')
    # Future-outcome class may never be used to form the ex-ante candidate flags.
    e=e.sort_values(['signal_date','code']).reset_index(drop=True)
    e['cohort']=np.where(e.D20_complete.eq(1),'OLDER_D20_COMPLETE','RECENT_D5_ONLY')
    return e


def make_flags(e):
    """Declared thresholds were chosen *before this run* as descriptive checks.
    All columns referenced here are knowable at the signal close; never use D+ outcomes.
    """
    f=pd.DataFrame(index=e.index)
    num=lambda col: pd.to_numeric(e[col],errors='coerce')
    f['impulse_ge30']=num('prewave_impulse_pct')>=30
    f['impulse_ge50']=num('prewave_impulse_pct')>=50
    f['dryup10_lt070']=num('volume_dryup_10_30')<.70
    f['dryup10_lt100']=num('volume_dryup_10_30')<1.0
    f['pullbackvol_lt070']=num('prewave_pullback_vol_ratio')<.70
    f['retrace_20_80']=num('prewave_retrace_fraction').between(.20,.80,inclusive='both')
    f['origin_below224']=e.prewave_trough_below224.eq(True)
    f['signal_below224']=num('ma224_dist_signal_pct')<0
    f['wave_stage4']=e.wave_setup_stage.eq(4)
    f['ma20_extended_ge5']=num('ma20_signal_dist_pct')>=5
    f['impulse_ge30_and_dryup10_lt070']=f.impulse_ge30 & f.dryup10_lt070
    f['retrace_20_80_and_pullbackvol_lt070']=f.retrace_20_80 & f.pullbackvol_lt070
    # A missing pre-signal measure is UNKNOWN, never silently assigned to the false cohort.
    dependencies={
        'impulse_ge30':('prewave_impulse_pct',), 'impulse_ge50':('prewave_impulse_pct',),
        'dryup10_lt070':('volume_dryup_10_30',), 'dryup10_lt100':('volume_dryup_10_30',),
        'pullbackvol_lt070':('prewave_pullback_vol_ratio',),
        'retrace_20_80':('prewave_retrace_fraction',),
        'signal_below224':('ma224_dist_signal_pct',),
        'ma20_extended_ge5':('ma20_signal_dist_pct',),
        'impulse_ge30_and_dryup10_lt070':('prewave_impulse_pct','volume_dryup_10_30'),
        'retrace_20_80_and_pullbackvol_lt070':('prewave_retrace_fraction','prewave_pullback_vol_ratio'),
    }
    for col,deps in dependencies.items():
        missing=pd.Series(False,index=e.index)
        for dep in deps:missing|=num(dep).isna()
        f[col]=f[col].astype('boolean')
        f.loc[missing,col]=pd.NA
    require(tuple(f.columns)==FLAG_NAMES, 'FLAG_CONTRACT')
    return f.astype('Int64')


def summaries(e,flags):
    x=pd.concat([e,flags],axis=1)
    # Relative to *other selected signals on same day*, not a market benchmark.
    x['D5_same_date_selected_residual_pct']=x.D5_close_pct-x.groupby('signal_date').D5_close_pct.transform('mean')
    x['D20_same_date_selected_residual_pct']=x.D20_close_pct-x.groupby('signal_date').D20_close_pct.transform('mean')
    out=[]
    for subset in ('ALL', 'EARLIEST_PER_CODE'):
        for corp in ('INCLUDE_SUSPECT', 'EXCLUDE_SUSPECT'):
            for cohort in ('OLDER_D20_COMPLETE','RECENT_D5_ONLY'):
                base=x[x.cohort.eq(cohort)]
                if corp=='EXCLUDE_SUSPECT': base=base[base.prior_224_jump_above35pct.ne(True)]
                if subset=='EARLIEST_PER_CODE': base=base.sort_values(['signal_date','code']).drop_duplicates('code',keep='first')
                for flag in ('ALL',)+FLAG_NAMES:
                    for v in ((1,) if flag=='ALL' else (1,0)):
                        g=base if flag=='ALL' else base[base[flag].eq(v)]
                        if len(g)==0:continue
                        n,uc,nd=len(g),g.code.nunique(),g.signal_date.nunique()
                        q={'period':cohort,'sampling':subset,'corp_action_policy':corp,'feature':flag,
                           'flag_value':v,'n':n,'unique_codes':uc,'unique_days':nd,
                           'descriptive_sample_gate_pass':int(n>=30 and uc>=15 and nd>=5),
                           'd5_close_win_pct':(g.D5_close_pct>0).mean()*100,
                           'd5_close_mean_pct':g.D5_close_pct.mean(),
                           'd5_close_median_pct':g.D5_close_pct.median(),
                           'd5_mfe_median_pct':g.D5_max_high_pct.median(),
                           'd5_high5_touch_pct':(g.D5_max_high_pct>=5).mean()*100,
                           'd5_mae_median_pct':g.D5_min_low_pct.median(),
                           'd5_same_day_selected_residual_mean_pct':g.D5_same_date_selected_residual_pct.mean(),
                           'd20_same_day_selected_residual_mean_pct':np.nan,
                           'd20_close_win_pct':np.nan,'d20_close_mean_pct':np.nan,
                           'd20_high5_touch_pct':np.nan,'d20_mae_median_pct':np.nan}
                        if cohort=='OLDER_D20_COMPLETE':
                            q.update(d20_close_win_pct=(g.D20_close_pct>0).mean()*100,
                                     d20_close_mean_pct=g.D20_close_pct.mean(),
                                     d20_high5_touch_pct=(g.D20_max_high_pct>=5).mean()*100,
                                     d20_mae_median_pct=g.D20_min_low_pct.median(),
                                     d20_same_day_selected_residual_mean_pct=g.D20_same_date_selected_residual_pct.mean())
                        out.append(q)
    return pd.DataFrame(out)


def paired_report(pairs):
    require(len(pairs)==36 and not pairs.pair_id.duplicated().any(), 'PAIR_COUNT_OR_DUPLICATE')
    require(pairs[['left_code','right_code']].notna().all().all(), 'NULL_PAIR_CODE')
    require((pairs.left_code!=pairs.right_code).all(), 'SAME_STOCK_PAIR')
    require((pairs.signal_session_gap<=12).all(), 'PAIR_DATE_GAP_EXCEEDED')
    # R1.9 pairing uses a distinctness contract PER contrast group, not globally.
    # Cross-group reuse is explicitly audited, NEVER claim 36 independent pairs.
    for group,g in pairs.groupby('pair_type'):
        events=list(zip(g.left_signal_date,g.left_code))+list(zip(g.right_signal_date,g.right_code))
        require(len(events)==len(set(events)), 'MATCH_REUSES_EVENT_WITHIN_GROUP:'+group)
    all_events=list(zip(pairs.left_signal_date,pairs.left_code))+list(zip(pairs.right_signal_date,pairs.right_code))
    require(len(set(all_events))==68, 'R19_CROSS_CONTRAST_REUSE_COUNT_CHANGED')
    presignal=['prewave_impulse_pct','prewave_pullback_vol_ratio','volume_dryup_10_30',
               'prewave_prevolume_accum_ratio','prewave_pullback_pct','ma20_signal_dist_pct',
               'ma224_dist_signal_pct','candle_close_location']
    out=[]
    for kind,g in pairs.groupby('pair_type'):
        for col in presignal:
            l=pd.to_numeric(g['left_'+col],errors='coerce');r=pd.to_numeric(g['right_'+col],errors='coerce')
            good=l.notna()&r.notna();d=(l[good]-r[good]);n=int(good.sum())
            out.append({'pair_type':kind,'pre_signal_feature':col,'n_pairs':n,
                        'left_greater_count':int((d>0).sum()),'left_less_count':int((d<0).sum()),
                        'left_minus_right_median':d.median() if n else np.nan,
                        'description':'OUTCOME_MATCHED_EXPLORATION_ONLY_NOT_PREDICTIVE'})
    return pd.DataFrame(out)


def entry_report(e, flags, trades):
    require(len(trades)==754*4, 'POLICY_TRADE_COUNT')
    require(trades[['signal_date','code','entry_policy']].notna().all().all(), 'ENTRY_ID_NULL')
    require(trades.code.str.fullmatch('[0-9]{6}').all(), 'ENTRY_CODE_FORMAT')
    require(not trades[['signal_date','code','entry_policy']].duplicated().any(), 'DUPLICATE_ENTRY_POLICY')
    require(set(trades.entry_policy.unique())==set(MODELS), 'POLICY_NAME_CHANGED')
    validate=e[['signal_date','code','cohort']].merge(flags,left_index=True,right_index=True)
    d=trades.merge(validate,on=['signal_date','code'],how='left',validate='many_to_one',indicator=True)
    require(d._merge.eq('both').all(), 'POLICY_EVENT_MISSING')
    require(d.groupby(['signal_date','code']).entry_policy.nunique().eq(4).all(), 'POLICY_COVERAGE_MISSING')
    require(d.loc[d.filled.eq(0),'signal_level_net_pct'].fillna(0).eq(0).all(), 'NO_FILL_RETURN_NOT_ZERO')
    require(d.loc[d.filled.eq(1),'net_pct'].notna().all(), 'MISSING_REALIZED_NET_FOR_FILL')
    rows=[]
    for cohort in ('OLDER_D20_COMPLETE','RECENT_D5_ONLY'):
        c=d[d.cohort.eq(cohort)]
        for feature in ('ALL','impulse_ge30','dryup10_lt070','pullbackvol_lt070','wave_stage4','retrace_20_80'):
            for value in ((1,) if feature=='ALL' else (1,0)):
                f=c if feature=='ALL' else c[c[feature].eq(value)]
                for policy,g in f.groupby('entry_policy'):
                    if len(g)==0:continue
                    eligible=g[g.censored.eq(0)];filled=eligible[eligible.filled.eq(1)]
                    rows.append({'period':cohort,'feature':feature,'feature_value':value,'policy':policy,
                                 'n_signals':len(g),'n_eligible':len(eligible),'n_filled':len(filled),
                                 'n_censored':int(g.censored.sum()),'n_unfilled':int(((g.censored.eq(0))&(g.filled.eq(0))).sum()),
                                 'filled_pct_of_all':100*len(filled)/len(g),
                                 'filled_win_pct':100*filled.win.mean() if len(filled) else np.nan,
                                 'net_mean_on_fill_pct':filled.net_pct.mean() if len(filled) else np.nan,
                                 'net_median_on_fill_pct':filled.net_pct.median() if len(filled) else np.nan,
                                 'signal_level_net_mean_pct':eligible.signal_level_net_pct.mean() if len(eligible) else np.nan,
                                 'n_distinct_stock_codes':g.code.nunique(),
                                 'descriptive_sample_gate_pass':int(len(g)>=30 and g.code.nunique()>=15)})
    return pd.DataFrame(rows)


def transition_report(e):
    g=e[e.D20_complete.eq(1)]
    a=[]
    for group,v in g.groupby('outcome_class'):
        n=len(v)
        a.append({'outcome_class':group,'n':n,'D1_close_positive_pct':(v.D1_close_pct>0).mean()*100,
                  'D3_close_positive_pct':(v.D3_close_pct>0).mean()*100,
                  'D5_close_positive_pct':(v.D5_close_pct>0).mean()*100,
                  'D5_high_plus5_touch_pct':(v.D5_max_high_pct>=5).mean()*100,
                  'D5_max_high_median_pct':v.D5_max_high_pct.median(),
                  'D5_min_low_median_pct':v.D5_min_low_pct.median(),
                  'D20_close_median_pct':v.D20_close_pct.median(),
                  'CAUTION':'USES_FUTURE_OUTCOME_CLASS_ONLY_FOR_RETROSPECTIVE_ANALYSIS'})
    return pd.DataFrame(a)


def main(r18dir,r19dir,outdir):
    r18=Path(r18dir);r19=Path(r19dir);out=Path(outdir)
    _,m19=validate_source(r18,r19)
    e=load_events(r18)
    flags=make_flags(e)
    suspects=pd.read_csv(r19/'r19_corporate_action_suspect_exclusions.csv',dtype={'code':str})
    expected=e.loc[(e.D20_complete.eq(1))&(e.prior_224_jump_above35pct.eq(True)),['signal_date','code']].sort_values(['signal_date','code'])
    actual=suspects[['signal_date','code']].sort_values(['signal_date','code'])
    require(len(suspects)==13 and expected.reset_index(drop=True).equals(actual.reset_index(drop=True)), 'CORP_ACTION_SUSPECT_ID_MISMATCH')
    pairs=pd.read_csv(r19/'r19_matched_contrast_pairs.csv',dtype={'left_code':str,'right_code':str})
    trades=pd.read_csv(r18/'r18_entry_policy_trade_level.csv',dtype={'code':str},low_memory=False)
    out.mkdir(parents=True,exist_ok=True)
    from collections import Counter
    pair_event_reuse=Counter(list(zip(pairs.left_signal_date,pairs.left_code))+list(zip(pairs.right_signal_date,pairs.right_code)))
    repeat_rows=[{'signal_date':date,'code':code,'appearances_across_R19_pairs':n,
                  'scope':'CROSS_CONTRAST_REUSE_NOT_INDEPENDENT_PAIRS'}
                 for (date,code),n in sorted(pair_event_reuse.items()) if n>1]
    require(len(repeat_rows)==4, 'R19_REUSE_COUNT_CHANGED')
    items={
        'r110_r19_pair_reuse_disclosure.csv':pd.DataFrame(repeat_rows),
        'r110_signal_flags_and_outcomes.csv':pd.concat([e,flags],axis=1),
        'r110_predeclared_wave_pullback_cohort_comparison.csv':summaries(e,flags),
        'r110_matched_36_pair_presignal_differences.csv':paired_report(pairs),
        'r110_entry_policy_signal_level_analysis.csv':entry_report(e,flags,trades),
        'r110_success_failure_early_price_path.csv':transition_report(e),
    }
    for filename,df in items.items():
        df.to_csv(out/filename,index=False,encoding='utf-8-sig')
    j={'status':'PASS_DISCOVERY_ONLY_NOT_TRADE_APPROVED','source_runs':SOURCE_RUNS,
       'verified_upstream_artifact_manifest':'R19 PASS_WITH_KNOWN_LIMITATIONS',
       'r19_historical_calendar_sha256':m19['historical_calendar_sha256'],
       'r18_unique_analyzed':754,'older_complete_D20':570,'recent_complete_D5_only':184,
       'r19_matched_pairs':36,'r19_unique_pair_slots':68,'r19_cross_contrast_repeated_events':4,
       'r19_global_independence_verified':False,'mature_corporate_action_suspect_signals':13,
       'selection_scope':'R17_SELECTED_TOP5_ONLY_NOT_FULL_UNIVERSE',
       'hypothesis_scope':'PREDECLARED_PRESIGNAL_FLAGS_ONLY',
       'independence_policy':'same_stock_repeated_signals_reported_AND_first_signal_per_stock_robustness',
       'recent_study_policy':'RECENT_HAS_D5_ONLY_NOT_OOS_D20; reviewed in R18/R19, not pristine holdout',
       'exit_sim_policy':'R18_FIXED_TP5_SL5_MAX_5_ENTRY_SESSIONS_05PCT_COST; does not prove executable fills',
       'NOT_PROVEN':['historical_universe_PIT','historical_index_constituents_PIT','regular_session_15_30_only',
                     'full_corporate_action_adjustment','actual_market_fills','prospective_untouched_OOS','unselected_candidates'],
       'candidate_feature_flags':list(FLAG_NAMES),
       'unknown_feature_counts':{c:int(flags[c].isna().sum()) for c in FLAG_NAMES},
       'source_sha256':EXPECTED_SHA,'output_sha256':{name:sha(out/name) for name in items},
       'warning':'No adaptive threshold selection or parameter fitting. D5 lookback cohorts differ by date/market regime. Every flag is a hypothesis, not an approved policy.'}
    (out/'r110_integrity_manifest.json').write_text(json.dumps(j,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R110_RESEARCH_PASS',json.dumps({'n':len(e),'old':570,'recent':184,'pairs':36,'files':len(items)+1},ensure_ascii=False))
    return j

if __name__=='__main__':
    p=argparse.ArgumentParser(description='R1.10 immutable KRX evidence qualitative factor and entry comparison; LIVE untouched')
    p.add_argument('--r18-dir',required=True)
    p.add_argument('--r19-dir',required=True)
    p.add_argument('--out',required=True)
    a=p.parse_args()
    main(a.r18_dir,a.r19_dir,a.out)
