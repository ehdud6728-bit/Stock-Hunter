"""R1.21 immutable research-only, time-blocked descriptive hypotheses from R1.20 sources."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="R121_FROZEN_ASOF_TYPE_ENTRY_SCALE_STOP_TEMPORAL_REPLAY_V1"
SOURCE_FILES=("r120_episodes_asof_and_evidence.csv", "r120_entry_scale_stop_scenario_ledger.csv")
KEY=["scope", "type", "code", "signal_date"]
ALLOWED_CANDIDATE_FEATURES={"scope", "type", "volume_context", "r112_stage_asof"}
SCOPES=("V4976_SELECTED", "V4976_RAW_NOT_TOP5", "BROAD_NOT_RAW", "OTHER_COVERED")
STATES=("TRAIN_MATURE", "EMBARGO", "VALIDATION_MATURE", "RIGHT_CENSORED")
FROZEN_POLICY_SIGNATURE=(
    ("BASE_D1_STOP5","ALL","D1_FULL","STOP5"),
    ("H1_PB_ACCUM_D1","PB_ACCUM_DRY","D1_FULL","STOP5"),
    ("H2_PB_ACCUM_CONFIRM","PB_ACCUM_DRY","D2TO6_CONFIRM_D0HIGH","STOP5"),
    ("H3_PB_ACCUM_SCALE","PB_ACCUM_DRY","D1_40_D2TO5_DIP2_D2TO8_DIP4","STOP5"),
    ("H4_AVOID_DEEP_AGED","NOT_DEEP_AGED","D1_FULL","STOP5"),
    ("H5_R112_CONFIRMED","R112_CONFIRMED_SELECTED_ONLY","D1_FULL","STOP5"),
    ("CONTROL_BOTH_RISING_PB","PB_BOTH_RISING","D1_FULL","STOP5"),
)


def sha256(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def require(test, message):
    if not bool(test): raise ValueError(message)


def source_load(root):
    root=Path(root)
    mfile=root/"r120_integrity_manifest.json"
    require(mfile.is_file(),"R120_SOURCE_MANIFEST_MISSING")
    m=json.loads(mfile.read_text(encoding="utf-8"))
    require(m.get('version')=='R120_ENTRY_SCALE_STOP_ASOF_EPISODE_V1',"R120_VERSION_INCOMPATIBLE")
    require(m.get('status')=='PASS_EPISODE_ENTRY_SCALE_STOP_DESCRIPTIVE_NOT_OOS_NOT_TRADE_APPROVED',"R120_STATUS_BAD")
    require(m.get('trade_authority') is False and m.get('true_full_market_PIT_available') is False,"R120_AUTHORITY_SCOPE_BAD")
    require(m.get('eligible_scope_episode_n')==4567 and m.get('d20_scenario_eligible_n')==3160 and m.get('scenario_rows')==63200,"R120_SOURCE_COUNTS_MISMATCH")
    require(m.get('krx_sha_verified_days')==1907,"R120_KRX_SOURCE_PROVENANCE_BAD")
    for fn,sha in m.get("file_sha256",{}).items():
        require((root/fn).is_file() and sha256(root/fn)==sha,"R120_MEMBER_SHA_FAIL:"+fn)
    for fn in SOURCE_FILES:
        require(fn in m.get("file_sha256",{}),"R120_REQUIRED_FILE_NOT_PINNED:"+fn)
    a=pd.read_csv(root/SOURCE_FILES[0],dtype={"code":"string"},low_memory=False)
    s=pd.read_csv(root/SOURCE_FILES[1],dtype={"code":"string"},low_memory=False)
    require(len(a)==4567 and len(s)==63200,"R120_LENGTH_BAD")
    require(set(KEY+['volume_context','r112_stage_asof','simulation_status','d20_complete','t5_first_day_r119','t5_vs_s5_r119']).issubset(a.columns),"EPISODE_REQUIRED_FIELDS_MISSING")
    require(set(KEY+['entry_policy','stop_policy','profit_pct_budget_net05','status','invested_fraction','exit_reason','exit_day']).issubset(s.columns),"SCENARIO_REQUIRED_FIELDS_MISSING")
    require(not a.duplicated(KEY).any(),"DUPLICATE_EPISODE")
    require(not s.duplicated(KEY+['entry_policy','stop_policy']).any(),"DUPLICATE_POLICY_SCENARIO")
    require(a.code.str.fullmatch(r'\d{6}').all() and s.code.str.fullmatch(r'\d{6}').all(),"CODE_ZEROS_LOST")
    require(set(a.scope.unique())==set(SCOPES),"SCOPE_UNEXPECTED")
    eligible=a[a.simulation_status.eq('BAR_SCENARIO_ELIGIBLE')]
    require(len(eligible)==3160,"ELIGIBILITY_COUNT_INCORRECT")
    require(len(s)==len(eligible)*20,"POLICY_GRID_INCOMPLETE")
    return a,s,m


def time_window(dates):
    dates=sorted(pd.Series(dates).dropna().astype(str).unique().tolist())
    require(len(dates)==45,"HISTORICAL_45_DATE_LOCK_FAILED")
    require(dates[0]=='2026-08-03' and dates[21]=='2026-09-02' and dates[25]=='2026-09-08',"TEMPORAL_SPLIT_PIN_FAIL")
    require(dates[20]=='2026-09-01',"TRAIN_FUTURE_LABEL_CROSSES_VALIDATION")
    return {v:('TRAIN_MATURE' if i==0 else 'EMBARGO' if 1<=i<=20 else 'VALIDATION_MATURE' if 21<=i<=25 else 'RIGHT_CENSORED') for i,v in enumerate(dates)}


def gate_match(a,gate):
    if gate=='ALL':return pd.Series(True,index=a.index)
    pb=a['type'].isin(['SHALLOW_PULLBACK','MID_PULLBACK'])
    if gate=='PB_ACCUM_DRY':return pb & a['volume_context'].eq('ACCUM20_RECENT_DRY5')
    if gate=='PB_BOTH_RISING':return pb & a['volume_context'].eq('BOTH_RISING')
    if gate=='NOT_DEEP_AGED':return ~a['type'].isin(['DEEP_PULLBACK','AGED_PULLBACK_AFTER_PEAK'])
    if gate=='R112_CONFIRMED_SELECTED_ONLY':return a['scope'].eq('V4976_SELECTED')&a['r112_stage_asof'].isin(['PULLBACK_QUALITY','REBOUND_CONFIRMED'])
    raise ValueError('UNKNOWN_GATE:'+gate)


def ticker_cluster_ci(values,codes,seed=121,draws=1000):
    data=pd.DataFrame({'v':pd.to_numeric(values,errors='coerce'), 'code':pd.Series(codes).astype('string')}).dropna()
    if len(data)<30 or data.code.nunique()<20:return (float('nan'),float('nan'))
    grouped=data.groupby('code',sort=True).v.agg(['sum','count'])
    sums=grouped['sum'].to_numpy(float);ns=grouped['count'].to_numpy(float)
    rng=np.random.default_rng(seed); bootstrap=[]
    for _ in range(draws):
        idx=rng.integers(0,len(sums),len(sums)); bootstrap.append(float(sums[idx].sum()/ns[idx].sum()))
    return tuple(np.quantile(bootstrap,[.025,.975]).tolist())


def metrics_for(x,selected,scope,window,pid):
    g=x[x.scope.eq(scope) & x.window.eq(window)].copy()
    n_universe=len(g)
    g=g[g.policy_gate]
    n_gate=len(g)
    complete=g[g.simulation_status.eq('BAR_SCENARIO_ELIGIBLE')].copy()
    valid=complete[complete.status.ne('STOP_CONTEXT_INVALID') & complete.profit_pct_budget_net05.notna() & complete.baseline_profit.notna()].copy()
    nvalid=len(valid)
    mean=lambda v: float(pd.to_numeric(v,errors='coerce').mean()) if len(v) else float('nan')
    paired=valid.profit_pct_budget_net05 - valid.baseline_profit
    low,hi=ticker_cluster_ci(paired,valid.code,seed=121)
    count_lost=int(n_gate-len(complete))
    dates=int(valid.signal_date.nunique()); tickers=int(valid.code.nunique())
    sample_gate= bool(nvalid>=30 and tickers>=20 and dates>=3)
    return dict(window=window,scope=scope,policy=pid,all_episodes=n_universe,gate_matched_episodes=n_gate,
        right_censored_or_quality_excluded=count_lost,complete_gate_episodes=len(complete),valid_scenarios=nvalid,
        entered_n=int(valid.status.eq('SIMULATED_BARS_NOT_FILLS').sum()),
        skipped_no_entry_n=int(valid.status.eq('NO_CONFIRMATION_NO_ENTRY').sum()),
        distinct_tickers=tickers,distinct_signal_days=dates,
        avg_reserved_capital_net_pct=mean(valid.profit_pct_budget_net05),
        matched_baseline_net_pct=mean(valid.baseline_profit),paired_delta_pp=mean(paired),
        paired_ticker_cluster_ci95_low_pp=low,paired_ticker_cluster_ci95_high_pp=hi,
        positive_budget_net_pct=mean((valid.profit_pct_budget_net05>0).astype(float))*100 if nvalid else float('nan'),
        entry_exposure_pct=mean(valid.invested_fraction)*100 if nvalid else float('nan'),
        target_exit_pct=mean(valid.exit_reason.eq('TARGET_TOUCH').astype(float))*100 if nvalid else float('nan'),
        stop_exit_pct=mean(valid.exit_reason.astype(str).str.contains('STOP').astype(float))*100 if nvalid else float('nan'),
        stop_d1_d3_n=int((valid.exit_reason.astype(str).str.contains('STOP') & pd.to_numeric(valid.exit_day,errors='coerce').le(3)).sum()),
        d1_reference_high5_touch_pct=mean(pd.to_numeric(valid.t5_first_day_r119,errors='coerce').notna().astype(float))*100 if nvalid else float('nan'),
        d1_reference_tp_first_stop5_pct=mean(valid.t5_vs_s5_r119.eq('TP_FIRST_HIGH_TOUCH').astype(float))*100 if nvalid else float('nan'),
        same_bar_stop_tp_n=int(pd.to_numeric(valid.same_bar_stop_tp,errors='coerce').fillna(0).sum()),
        interpretability_gate_pass=sample_gate,
        authority='RETROSPECTIVE_TIME_BLOCK_REPLAY_SOURCE_LIMITED_NOT_OOS_NOT_TRADE_APPROVED')


def run_research(source,contract,out):
    a,s,m=source_load(source)
    c=json.loads(Path(contract).read_text(encoding='utf-8'))
    require(c['revision']==REVISION,'FROZEN_CONTRACT_REVISION_INVALID')
    require(c['expected_r120_run_id']==38871858855,'WRONG_PINNED_PARENT')
    policies=c['policies']; names=[x['id'] for x in policies]
    require(tuple((x['id'],x['gate'],x['entry_policy'],x['stop_policy']) for x in policies)==FROZEN_POLICY_SIGNATURE,'FROZEN_POLICIES_MODIFIED')
    require({(p['entry_policy'],p['stop_policy']) for p in policies}.issubset(set(map(tuple,s[['entry_policy','stop_policy']].drop_duplicates().to_numpy()))),'POLICY_GRID_INCOMPLETE')
    time_map=time_window(a.signal_date)
    a['window']=a.signal_date.map(time_map)
    require(a.window.notna().all(),'UNCATEGORIZED_SIGNAL_DATE')
    # The evaluator never uses t5_first_day / forward returns / execution status in gate_match.
    eligibility=a[KEY+['window','simulation_status','t5_first_day_r119','t5_vs_s5_r119']].copy()
    baseline=s[(s.entry_policy=='D1_FULL')&(s.stop_policy=='STOP5')][KEY+['profit_pct_budget_net05']].copy().rename(columns={'profit_pct_budget_net05':'baseline_profit'})
    require(len(baseline)==3160,'BASELINE_GRID_WRONG')
    require(not baseline.duplicated(KEY).any(),'BASELINE_DUP')
    eligible=a[a.simulation_status.eq('BAR_SCENARIO_ELIGIBLE')]
    require(set(map(tuple,baseline[KEY].astype(str).to_numpy()))==set(map(tuple,eligible[KEY].astype(str).to_numpy())),'BASELINE_MISSING_EPISODE')
    rows=[]; compact=[]
    for p in policies:
        v=s[(s.entry_policy==p['entry_policy'])&(s.stop_policy==p['stop_policy'])].copy()
        require(len(v)==3160,'POLICY_INPUT_MISSING:'+p['id'])
        v=v.merge(eligibility,on=KEY,validate='one_to_one',how='left')
        v=v.merge(baseline,on=KEY,validate='one_to_one',how='left')
        require(v.window.notna().all(),'POLICY_WINDOW_JOIN_FAILED')
        mask=gate_match(v,p['gate']);v['policy_gate']=mask.fillna(False)
        # right-censored eligible gate count for this policy
        ag=a.copy();ag['policy_gate']=gate_match(ag,p['gate']).fillna(False)
        for scope in SCOPES:
            for period in ('TRAIN_MATURE','VALIDATION_MATURE','REFERENCE_ALL_COMPLETE'):
                if period=='REFERENCE_ALL_COMPLETE':
                    g=v[v.window.isin(['TRAIN_MATURE','EMBARGO','VALIDATION_MATURE'])].copy()
                    fullg=ag[ag.window.isin(['TRAIN_MATURE','EMBARGO','VALIDATION_MATURE'])].copy()
                    g['window']=period;fullg['window']=period
                else:
                    g=v.copy();fullg=ag.copy()
                selected=fullg[fullg.scope.eq(scope)&fullg.window.eq(period)]
                # pass both complete and ALL episode counts; policy matches are ASOF only
                obs=metrics_for(g,selected,scope,period,p['id'])
                obs['all_episodes']=int(len(selected))
                obs['gate_matched_episodes']=int(selected.policy_gate.sum())
                obs['right_censored_or_quality_excluded']=int(selected.policy_gate.sum()-len(g[g.scope.eq(scope)&g.window.eq(period)&g.policy_gate]))
                rows.append(obs)
        keep=v[v.policy_gate & v.window.isin(['TRAIN_MATURE','VALIDATION_MATURE'])][KEY+['window','entry_policy','stop_policy','status','profit_pct_budget_net05','baseline_profit','invested_fraction','exit_reason','exit_day','same_bar_stop_tp','t5_first_day_r119','t5_vs_s5_r119']].copy()
        keep.insert(0,'policy',p['id']);compact.append(keep)
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    summary=pd.DataFrame(rows)
    detail=pd.concat(compact,ignore_index=True)
    overlap=[]
    for scope in SCOPES:
        train=a[(a.scope==scope)&(a.window=='TRAIN_MATURE')].code
        val=a[(a.scope==scope)&(a.window=='VALIDATION_MATURE')].code
        overlap.append({'scope':scope,'train_episodes':len(train),'validation_episodes':len(val),'train_tickers':train.nunique(), 'validation_tickers':val.nunique(),'tickers_reused_across_periods':len(set(train)&set(val)),'validation_ticker_overlap_pct':100*len(set(train)&set(val))/max(1,val.nunique()),'source_population':'EXPOST_SELECTED_907_CODE_KRX_SUBSET_NOT_FULL_PIT'})
    ov=pd.DataFrame(overlap)
    # Breadth + coverage under specific fixed D0 cutoff, no automatic choice of new parameters.
    sensitivity=[]
    all_b=s[(s.entry_policy.eq('D1_FULL'))&s.stop_policy.isin(['STOP3','STOP5','STOP7'])].copy()
    all_b=all_b.merge(eligibility,on=KEY,validate='many_to_one')
    for scope in SCOPES:
        for win in ['TRAIN_MATURE','VALIDATION_MATURE']:
            for stop,g in all_b[(all_b.scope==scope)&(all_b.window==win)].groupby('stop_policy'):
                sensitivity.append({'scope':scope,'window':win,'stop_policy':stop,'n':len(g),'average_net_reserved_pct':float(g.profit_pct_budget_net05.mean()),'tp_exit_pct':100*float(g.exit_reason.eq('TARGET_TOUCH').mean()),'stop_exit_pct':100*float(g.exit_reason.astype(str).str.contains('STOP').mean()),'caveat':'NOT_RISK_NORMALIZED; STOP3_5_7_HAVE_DIFFERENT_LOSS_CAPS'})
    sens=pd.DataFrame(sensitivity)
    # Descriptive stage/volume linkage for research continuity. No candidate gate uses the labels below.
    trace=a[['scope','type','code','signal_date','window','volume_context','r112_stage_asof','r110_wave_label_asof','r110_prevolume_ratio_asof','r110_dryup_ratio_asof','t5_first_day_r119','t5_vs_s5_r119','simulation_status']].copy()
    trace['r110_info_present']=trace.r110_prevolume_ratio_asof.notna()
    trace['r112_info_present']=trace.r112_stage_asof.notna()
    diagnostic=[]
    for (scope,period),g in trace.groupby(['scope','window'],sort=True):
        for volume,sub in g.groupby('volume_context',dropna=False):
            good=sub[sub.simulation_status.eq('BAR_SCENARIO_ELIGIBLE')]
            diagnostic.append(dict(scope=scope,window=period,volume_context=volume,episode_n=len(sub),d20_complete_quality_n=len(good),
                r110_join_available_n=int(sub.r110_info_present.sum()),r112_join_available_n=int(sub.r112_info_present.sum()),
                r110_prevol_mean_asof=float(pd.to_numeric(sub.r110_prevolume_ratio_asof,errors='coerce').mean()),
                r110_dryup_mean_asof=float(pd.to_numeric(sub.r110_dryup_ratio_asof,errors='coerce').mean()),
                d1_reference_high5_touch_pct=100*float(pd.to_numeric(good.t5_first_day_r119,errors='coerce').notna().mean()) if len(good) else float('nan'),
                d1_reference_tp_before_stop5_pct=100*float(good.t5_vs_s5_r119.eq('TP_FIRST_HIGH_TOUCH').mean()) if len(good) else float('nan'),
                caveat='R110_R112_NOT_FULL_MARKET_AND_FEATURES_NOT_VALIDATED_FOR_ENTRY'))
    diags=pd.DataFrame(diagnostic)
    days=[]
    base=s[(s.entry_policy.eq('D1_FULL')) & (s.stop_policy.eq('STOP5'))][KEY+['profit_pct_budget_net05']].merge(
        a[KEY+['window','simulation_status']],on=KEY,validate='one_to_one')
    for (date,scope),g in a.groupby(['signal_date','scope'],sort=True):
        valid=base[(base.signal_date.eq(date))&(base.scope.eq(scope))]
        days.append({'signal_date':date,'scope':scope,'window':time_map[date],
            'eligible_d0_episode_n':len(g),'d20_complete_quality_n':len(valid),
            'reference_baseline_avg_net_pct':float(valid.profit_pct_budget_net05.mean()),
            'caution':'date-level descriptive only; no regime-adjusted causal claim'})
    daily=pd.DataFrame(days)
    files={
        'r121_r110_r112_volume_diagnostic.csv':diags,
        'r121_daily_baseline_time_break.csv':daily,
        'r121_frozen_type_policy_comparison.csv':summary,
        'r121_time_split_ticker_overlap.csv':ov,
        'r121_frozen_hypothesis_paired_ledger.csv':detail,
        'r121_stop3_5_7_sensitivity.csv':sens,
    }
    for fn,v in files.items(): v.to_csv(out/fn,index=False,encoding='utf-8-sig',float_format='%.8f')
    manifest={
        'revision':REVISION,
        'status':'PASS_DESCRIPTIVE_TEMPORAL_REPLAY_NOT_OOS_NOT_TRADE_APPROVED',
        'research_only':True,'trade_authority':False,'full_pit_market_available':False,
        'train_d0':'2026-08-03','train_full_d20_by':'2026-09-01',
        'embargo_last_d0':'2026-09-01','validation_first_d0':'2026-09-02','validation_last_d0':'2026-09-08',
        'train_window_is_one_day_left_censor_risk':True,
        'past_studies_reused_same_sources':True,
        'source_r120_run':38871858855,'source_mature_episodes':3160,
        'source_episodes':4567,'source_scenarios':63200,
        'pinned_original_krx_dates_sha_verified_via_r120':1907,
        'r110_join_rows_via_r120':754,'r112_join_rows_via_r120':1059,
        'source_r120_manifest_sha256':sha256(Path(source)/'r120_integrity_manifest.json'),
        'policy_contract_sha256':sha256(contract),
        'train_tickers':int(a[a.window.eq('TRAIN_MATURE')].code.nunique()),
        'validation_tickers':int(a[a.window.eq('VALIDATION_MATURE')].code.nunique()),
        'shared_tickers':len(set(a[a.window.eq('TRAIN_MATURE')].code)&set(a[a.window.eq('VALIDATION_MATURE')].code)),
        'holdout_partially_reused_codes':True,
        'no_parameter_tuned_from_validation':True,
        'policy_count':len(policies),
        'output_files_sha256':{fn:sha256(out/fn) for fn in files},
        'limitations':c['warnings']
    }
    (out/'r121_integrity_manifest.json').write_text(json.dumps(manifest,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print('R121_RESEARCH_PASS',json.dumps({k:manifest[k] for k in ['status','source_episodes','source_mature_episodes','train_tickers','validation_tickers','shared_tickers','policy_count']},ensure_ascii=False))
    return manifest


def main():
    pa=argparse.ArgumentParser();pa.add_argument('--source',required=True);pa.add_argument('--contract',default='closebet_research/r121_frozen_hypothesis_contract.json');pa.add_argument('--output',required=True);v=pa.parse_args()
    run_research(v.source,v.contract,v.output)
if __name__=='__main__': main()
