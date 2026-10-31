#!/usr/bin/env python3
"""R1D73 as-of signal audit, RESEARCH ONLY; not an execution-ready classifier.

Input is a frozen provenance-checked merge of R1D72 stage rows with R1D72.1
KRX same-date volume and amount. Uses no outcomes to decide signal quality.
The only known historical market exception is 279060 2024-02-26, previously
verified as KONEX; all other market membership requires independent PIT proof.
"""
import argparse, hashlib, json, math, pathlib
import pandas as pd
import numpy as np

THRESHOLDS = {
    'signal_amount_min_krw': 100_000_000,
    'prior20_average_amount_min_krw': 100_000_000,
    'band_close_above_max_pct': 5.0,
    'close_above_pullback_low_max_pct': 8.0,
    'very_low_amount_watch_krw': 10_000_000,
    'minimum_volume': 10,
}
KNOWN_NON_TARGET_MARKET = {('2024-02-26','279060'): ('KONEX','KRX historical delisting-market research; manual-case only')}


def sha(path): return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def read_inputs(stage_file, wave_file):
    a = pd.read_csv(stage_file, dtype={'code':str}, low_memory=False)
    b = pd.read_csv(wave_file, dtype={'code':str}, low_memory=False)
    assert len(a)==2049 and len(a[['date','code']].drop_duplicates())==1730, 'STAGE_POPULATION_DRIFT'
    assert len(b)==5247 and not b[['date','code']].duplicated().any(), 'WAVE_POPULATION_DRIFT'
    assert not a[['date','code','band']].duplicated().any(), 'BAND_DUPLICATE'
    needed=['date','code','band','level','source_close','signal_close','signal_high','signal_low', 'signal_open',
       'volume','amount_krw','near','low_touch','touch_reject','fresh_reclaim','signal_amount_ratio_prev20',
       'entry1_open','entry1_day','entry1_net_pct','price_jump_or_mae_watch']
    assert not set(needed).difference(a.columns), 'STAGE_COLUMN_MISSING'
    assert not set(['date','code','pullback_min_low','wave_trough_low','wave_gain_pct','pullback_min_date',
       'wave_trough_date','wave_peak_date']).difference(b.columns), 'WAVE_COLUMN_MISSING'
    out=a.merge(b[['date','code','pullback_min_low','wave_trough_low','wave_gain_pct',
          'pullback_min_date','wave_trough_date','wave_peak_date','status_feature']],
          on=['date','code'],validate='many_to_one',how='left')
    assert len(out)==len(a) and out.pullback_min_low.notna().all(), 'PIT_WAVE_KEY_MISSING'
    return out


def classify_signal_only(df):
    """Every gating feature is known by D0 close. Never use D+1 or outcomes."""
    d=df.copy()
    d['asof_close_vs_pullback_low_pct'] = 100*(d.signal_close/d.pullback_min_low-1)
    d['asof_close_vs_wave_low_pct'] = 100*(d.signal_close/d.wave_trough_low-1)
    d['asof_close_vs_band_pct'] = 100*(d.signal_close/d.level-1)
    d['prior20_amount_average_inferred_krw'] = np.where(d.signal_amount_ratio_prev20>0,
                            d.amount_krw/d.signal_amount_ratio_prev20,np.nan)
    # Source-ratio times asof volume is only approximate when ratio rounded;
    # valid for research-only thresholds, never asserted to be a true 20-session replay.
    valid_ohlc = (d.signal_open>0)&(d.signal_close>0)&(d.signal_low>0)&(d.signal_high>0)&(
        d.signal_low<=d[['signal_open','signal_close']].min(axis=1))&(
        d.signal_high>=d[['signal_open','signal_close']].max(axis=1))&(
        d.volume>0)&(d.amount_krw>0)&(d.pullback_min_low>0)&(d.wave_trough_low>0)&(d.level>0)&(
        d.signal_close.sub(d.source_close).abs()<=0.011)
    asof_dates_ok = (pd.to_datetime(d.pullback_min_date,errors='coerce')<=pd.to_datetime(d.date)) & (
        pd.to_datetime(d.wave_trough_date,errors='coerce')<=pd.to_datetime(d.date)) &(
        pd.to_datetime(d.wave_peak_date,errors='coerce')<=pd.to_datetime(d.date))
    d['audit_f_invalid_ohlc_or_asof'] = (~valid_ohlc|~asof_dates_ok).astype(int)
    d['audit_f_under10m'] = (d.amount_krw<THRESHOLDS['very_low_amount_watch_krw']).astype(int)
    d['audit_f_under100m'] = (d.amount_krw<THRESHOLDS['signal_amount_min_krw']).astype(int)
    d['audit_f_prior20_under100m'] = ((d.prior20_amount_average_inferred_krw<THRESHOLDS['prior20_average_amount_min_krw'])|d.prior20_amount_average_inferred_krw.isna()).astype(int)
    d['audit_f_volume_lt10'] = (d.volume<THRESHOLDS['minimum_volume']).astype(int)
    d['audit_f_reclaim_no_contact'] = ((d.fresh_reclaim==1)&(d.near==0)&(d.low_touch==0)).astype(int)
    d['audit_f_band_chase_gt5'] = (d.asof_close_vs_band_pct>THRESHOLDS['band_close_above_max_pct']).astype(int)
    d['audit_f_pullback_chase_gt8'] = (d.asof_close_vs_pullback_low_pct>THRESHOLDS['close_above_pullback_low_max_pct']).astype(int)
    d['audit_f_below_band_not_reclaimed'] = ((d.signal_close<d.level)&(d.fresh_reclaim==0)).astype(int)
    d['audit_f_no_actual_contact'] = ((d.near==0)&(d.low_touch==0)).astype(int)
    # Physical consistency check. A signed trade amount is not a proxy for buy/sell pressure.
    d['audit_f_amount_ohlc_inconsistent'] = ((d.amount_krw<d.signal_low*d.volume*.999)|(
        d.amount_krw>d.signal_high*d.volume*1.001)).astype(int)
    d['market_asof_evidence'] = 'UNKNOWN_NOT_PIT_CERTIFIED'
    d['market_evidence_source'] = 'NO_DATE_SPECIFIC_MARKET_LEDGER'
    match=((d.code=='279060')&(d.date=='2024-02-26'))
    d.loc[match,'market_asof_evidence']='KONEX_PRIOR_VERIFIED_CASE'
    d.loc[match,'market_evidence_source']='KRX_HISTORIC_CASE_REVIEW'
    d['audit_f_known_non_target_market'] = match.astype(int)
    hard_flags=['audit_f_invalid_ohlc_or_asof','audit_f_amount_ohlc_inconsistent',
      'audit_f_volume_lt10','audit_f_known_non_target_market']
    research_flags=['audit_f_under100m','audit_f_prior20_under100m',
      'audit_f_band_chase_gt5','audit_f_pullback_chase_gt8',
      'audit_f_no_actual_contact','audit_f_below_band_not_reclaimed']
    d['structural_quality_hard_fail']=(d[hard_flags].sum(axis=1)>0).astype(int)
    d['research_location_liquidity_warning']=(d[research_flags].sum(axis=1)>0).astype(int)
    # Two-dimensional review verdict; no row is automatically 'trade ready'.
    d['research_quality_tier']=np.select([
       d.structural_quality_hard_fail.eq(1),d.research_location_liquidity_warning.eq(1),
       (d.near.eq(1)|d.low_touch.eq(1)) & (d.signal_close>=d.level)
       ],['REJECT_KNOWN_UNTRADEABLE_OR_INVALID','WATCH_LOW_LIQUIDITY_OR_LATE_PRICE','PROVISIONAL_ASOF_PATTERN_REVIEW'],
       default='WAIT_FOR_CLOSE_RECOVERY')
    d['LIVE_TRADE_READY']='NO_CA_LEDGER_AND_MARKET_PIT_UNCERTIFIED'
    return d


def summarise(d):
    flags=[c for c in d if c.startswith('audit_f_')]
    unique=d.sort_values(['date','code','band']).groupby(['date','code'],as_index=False).agg(
     quality_hard_fail=('structural_quality_hard_fail','max'),
     quality_watch=('research_location_liquidity_warning','max'),
     amount_krw=('amount_krw','first'),
     volume=('volume','first'),
     n_band_signals=('band','size'),
     min_abs_distance_band_pct=('asof_close_vs_band_pct',lambda x:float(x.abs().min())),
     signal_close_above_pullback_pct=('asof_close_vs_pullback_low_pct','first'),
     market_evidence=('market_asof_evidence','first'),
     estimated_prior20_avg_amount_krw=('prior20_amount_average_inferred_krw','first'),
     any_asof_candidate=('research_quality_tier',lambda s: int((s=='PROVISIONAL_ASOF_PATTERN_REVIEW').any())),
    )
    unique['any_bad_flag']=((unique.quality_hard_fail==1)|(unique.quality_watch==1)).astype(int)
    band=d.groupby(['band','research_quality_tier'],dropna=False).size().reset_index(name='n')
    counts={'band_rows':int(len(d)),'distinct_date_code':int(len(unique)),
         'known_konex_datecode':int(unique.market_evidence.eq('KONEX_PRIOR_VERIFIED_CASE').sum()),
         'market_unknown_datecode':int(unique.market_evidence.eq('UNKNOWN_NOT_PIT_CERTIFIED').sum()),
         'not_trade_ready':int((d.LIVE_TRADE_READY!='YES').sum()),
         'hard_fail_rows':int((d.structural_quality_hard_fail==1).sum()),
         'watch_rows':int((d.research_location_liquidity_warning==1).sum()),
         'provisional_asof_rows':int(d.research_quality_tier.eq('PROVISIONAL_ASOF_PATTERN_REVIEW').sum()),
         'provisional_unique_datecode':int(unique.any_asof_candidate.sum()),
         'unique_any_undesirable_flag':int(unique.any_bad_flag.sum()),
         'flags':{f:{'rows':int(d[f].sum()), 'datecode':int(d.loc[d[f].eq(1),['date','code']].drop_duplicates().shape[0])} for f in flags},
         'tiers':{str(k):int(v) for k,v in d.research_quality_tier.value_counts().items()}}
    return unique,band,counts


def sensitivity(d):
    # Predeclared sensitivity-only thresholds, no outcome optimization.
    rows=[]
    for amount in (10_000_000,100_000_000,500_000_000):
      for band_limit in (3.0,5.0,10.0):
       for trough in (5.0,8.0,12.0):
        ok=(d.audit_f_invalid_ohlc_or_asof.eq(0)&d.audit_f_amount_ohlc_inconsistent.eq(0)&
            d.audit_f_volume_lt10.eq(0)&d.audit_f_known_non_target_market.eq(0)&
            d.amount_krw.ge(amount)&d.prior20_amount_average_inferred_krw.ge(amount)&
            d.asof_close_vs_band_pct.between(-5,band_limit)&
            d.asof_close_vs_pullback_low_pct.le(trough)&
            (d.near.eq(1)|d.low_touch.eq(1)))
        rows.append({'signal_amount_and_avg_floor_krw':amount,'band_upper_gap_limit_pct':band_limit,
                    'pullback_upper_gap_limit_pct':trough,'rows':int(ok.sum()),
                    'distinct_datecode':int(d.loc[ok,['date','code']].drop_duplicates().shape[0]),
                    'market_certified_trade_ready':False})
    return pd.DataFrame(rows)


def run(stage, wave, out):
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    a=read_inputs(stage,wave)
    d=classify_signal_only(a)
    unique,by_band,counts=summarise(d)
    checks={
       'asof_no_outcome_fields_in_signal_function':True,
       'source_stage_sha256':sha(stage),
       'source_wave_sha256':sha(wave),
       'signals_all_unverified_corporate_action':bool((d.CA_LEDGER_STATUS=='UNCERTIFIED').all()),
       'population_preserved':len(d)==2049,
       'known_279060_excluded_from_provisional':bool((d[(d.code=='279060')&(d.date=='2024-02-26')].structural_quality_hard_fail==1).all()),
       'outcome_inference_disabled':True,
       'market_snapshot_source_missing_for_full_population':True,
       'not_a_new_marketwide_search':True,
       'no_live_changes':True,
       'thresholds':THRESHOLDS,
       **counts}
    # The main signal-only table is deliberately free of future trade outcomes.
    future_cols = [c for c in d.columns if c.startswith(('entry1_', 'entry2_', 'exit1_', 'exit2_', 'D20_'))]
    future_cols += ['next_day_confirm','price_jump_or_mae_watch','source_future_status', 'future_reject_reason']
    d.drop(columns=list(set(future_cols)), errors='ignore').to_csv(
        out/'01_BAND_ASOF_QUALITY_AUDIT_2049.csv',index=False,encoding='utf-8-sig')
    unique.to_csv(out/'02_UNIQUE_DATECODE_1730_QUALITY.csv',index=False,encoding='utf-8-sig')
    by_band.to_csv(out/'03_BAND_QUALITY_MATRIX.csv',index=False,encoding='utf-8-sig')
    sensitivity(d).to_csv(out/'04_SENSITIVITY_NON_OPTIMIZED.csv',index=False,encoding='utf-8-sig')
    d[(d.code=='279060')&(d.date=='2024-02-26')].to_csv(out/'05_KONEX_ONE_SHARE_CASE.csv',index=False,encoding='utf-8-sig')
    # Reproduce opposite cases from source, but no cherry-picking for wins.
    d[(d.code.isin(['279060','214420']))].sort_values(['code','date','band']).to_csv(out/'06_NAMED_CASE_REVIEW.csv',index=False,encoding='utf-8-sig')
    (out/'00_AUDIT.json').write_text(json.dumps(checks,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'n_bands':len(d),'n_unique':len(unique),'tiers':counts['tiers'],'flag_counts':counts['flags'],
          'market_unknown':counts['market_unknown_datecode']},ensure_ascii=False,indent=2))
    return checks

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--stage',required=True)
    p.add_argument('--wave',required=True)
    p.add_argument('--out',required=True)
    z=p.parse_args();run(z.stage,z.wave,z.out)
