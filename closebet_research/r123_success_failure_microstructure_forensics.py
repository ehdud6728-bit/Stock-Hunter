#!/usr/bin/env python3
"""R1.23: ex-ante EOD OHLCV supply proxies x *retrospective* stop/recovery paths.

NO intent/manipulation labels; daily bar data cannot prove shakeout actor/causality.
Scope = R1.22's ex-post partial universe, not full-market PIT. Research only.
"""
from __future__ import annotations
import argparse,hashlib,json,math
from pathlib import Path
import numpy as np
import pandas as pd
from closebet_research.r118_partial_market_wave_geometry import read_immutable_krx

REVISION='R123_SHAKEOUT_LIKE_RECOVERY_VS_OVERHEAD_SUPPLY_PROXY_DIAGNOSTICS_V1'
SOURCE_RUN=38893723498
SOURCE_ARTIFACT='closebet-r122-candle-support-actions-38893723498'
PINNED_R122_MANIFEST_SHA='ae3792359801a005c1171ee6c37717d73730822e5b3f10f6fc5b13c7c8d5b70f'
FEE_PCT=0.5

def require(condition,label):
    if not bool(condition):raise RuntimeError(label)

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def load_r122(root):
    root=Path(root); require((root/'r122_integrity_manifest.json').is_file(),'R122_MANIFEST_MISSING')
    require(sha(root/'r122_integrity_manifest.json')==PINNED_R122_MANIFEST_SHA,'R122_PINNED_MANIFEST_MISMATCH')
    m=json.loads((root/'r122_integrity_manifest.json').read_text(encoding='utf-8'))
    require(m['status']=='PASS_SOURCE_LIMITED_ASOF_CANDLE_SUPPORT_DESCRIPTIVE_NOT_OOS_NOT_TRADE_APPROVED','R122_STATUS_DRIFT')
    require(m['feature_rows']==4567 and m['feature_valid_rows']==4453 and m['scenario_events']==3077 and m['scenario_rows']==36924,'R122_COUNTS_DRIFT')
    require(m['krx_day_sha_verified']==1907,'R122_DAY_SHA_SCOPE_BAD')
    require(m['trade_authority'] is False and m['full_market_PIT_recall_valid'] is False,'R122_AUTHORITY_WRONG')
    for name,digest in m['sha256'].items():
        require((root/name).is_file() and sha(root/name)==digest,'R122_SOURCE_MEMBER_TAMPERED:'+name)
    f=pd.read_csv(root/'r122_d0_candle_ma_support_features.csv',dtype={'code':str},low_memory=False)
    x=pd.read_csv(root/'r122_entry_scale_structure_stop_ledger.csv',dtype={'code':str},low_memory=False)
    for a in [f,x]:a['code']=a.code.str.zfill(6)
    keys=['code','signal_date','scope']
    require(not f.duplicated(keys).any() and len(f)==4567,'R122_FEATURE_DUP')
    base=x[(x.entry=='E0_D1_FULL') & (x.stop=='S0_FIXED5')].copy()
    require(len(base)==3077 and not base.duplicated(keys).any(),'R122_BASELINE_INVALID')
    assert base.code.str.fullmatch(r'\d{6}').all()
    return f,base,m

def overhead_d0_only(prior60:pd.DataFrame,price:float):
    """Last 60 *completed pre-D0 daily bars*. Approximate volume at close, NOT true volume profile.
    Fixed ±8% zones, purely descriptive bins. No outcome-based thresholds.
    """
    if len(prior60)!=60 or not math.isfinite(price) or price<=0:
        return {'overhead_valid':False,'overhead_reason':'MISSING_60_PRIOR_BARS'}
    closes=prior60.close.to_numpy(float); vols=prior60.volume.to_numpy(float)
    if np.any(~np.isfinite(closes)) or np.any(~np.isfinite(vols)) or np.any(closes<=0) or np.any(vols<0) or sum(vols)<=0:
        return {'overhead_valid':False,'overhead_reason':'BAD_60_PRIOR_CLOSE_VOLUME'}
    total=float(sum(vols))
    overhead=float(vols[(closes>=price)&(closes<=price*1.08)].sum())
    under=float(vols[(closes>=price*.92)&(closes<price)].sum())
    local=(overhead+under)/total
    relative=overhead/(overhead+under) if overhead+under>0 else float('nan')
    proxy=('UNKNOWN_SPARSE_CLOSE_PROXY' if local<.2 or not math.isfinite(relative) else
           'ABOVE_HEAVY_PROXY' if relative>=.6 else
           'BELOW_HEAVY_PROXY' if relative<=.4 else 'BALANCED_PROXY')
    # Extra pre-D0 supply proxies. Daily upper wick tests are NOT limit order book data.
    upper_tests=int(((prior60.high>=price*1.05)&(prior60.close<price*1.05)).sum())
    return {'overhead_valid':True,'overhead_reason':'','overhead_close_volume_share_60':overhead/total,
            'near_below_close_volume_share_60':under/total,'local_zone_volume_share_60':local,
            'overhead_share_of_local_60':relative,'overhead_proxy_type':proxy,
            'upper_zone_intraday_rejection_days_60':upper_tests}

def first_touch_path(bars:list[tuple],ref_entry:float,prev_low:float):
    """Read ONLY D+1..D+20 for ex-post outcomes. Both sides same daily bar => UNKNOWN order.
    Gap open has a knowable first price, so resolve it before intraday touches.
    """
    require(len(bars)==20 and ref_entry>0,'D20_BARS_REQUIRED')
    tp=ref_entry*1.05;stop=ref_entry*.95
    first_tp=None; first_stop=None; first='NONE';ambig=False
    maxhigh=-math.inf;minlow=math.inf;back_to_d0=math.nan
    closes=[]; lows=[]; highs=[]
    for d,(o,h,l,c) in enumerate(bars,1):
        require(min(o,h,l,c)>0 and l<=min(o,c)<=max(o,c)<=h,'INVALID_FUTURE_BAR')
        closes.append(c);lows.append(l);highs.append(h)
        maxhigh=max(maxhigh,h);minlow=min(minlow,l)
        tp_now=h>=tp; stop_now=l<=stop
        if first_tp is None and tp_now:first_tp=d
        if first_stop is None and stop_now:first_stop=d
        if first=='NONE':
            if d>1 and o>=tp:first='TP_FIRST_GAP'
            elif d>1 and o<=stop:first='STOP_FIRST_GAP'
            elif tp_now and stop_now:first='SAME_DAY_ORDER_UNKNOWN';ambig=True
            elif tp_now:first='TP_FIRST'
            elif stop_now:first='STOP_FIRST'
    # If stop first, +5 later is a 'stop-then-rebound opportunity', not realizable under stop policy.
    stop_first=first.startswith('STOP_FIRST')
    later_reached=bool(stop_first and first_tp is not None and first_stop is not None and first_tp>first_stop)
    reclaim_3=False;support_breach=False;post_breach_any_reclaim=False
    if math.isfinite(prev_low) and prev_low>0:
        breach=np.flatnonzero(np.array(lows)<prev_low)
        if breach.size:
            support_breach=True
            k=int(breach[0]);reclaims=[j for j in range(k+1,min(k+4,20)) if closes[j]>=prev_low]
            reclaim_3=bool(reclaims)
            post_breach_any_reclaim=any(c>=prev_low for c in closes[k+1:])
    shakeout_like=bool(later_reached and first_stop<=5 and reclaim_3)
    if first.startswith('TP_FIRST'): outcome='TP5_FIRST'
    elif ambig:outcome='AMBIGUOUS_TP_STOP_SAME_DAY'
    elif stop_first and later_reached:outcome='STOP5_FIRST_THEN_LATER_TP5'
    elif stop_first:outcome='STOP5_FIRST_NO_LATER_TP5'
    elif maxhigh/ref_entry>=1.03:outcome='NO_TOUCH_NEAR_TP5'
    else:outcome='NO_TOUCH_WEAK_LIFT'
    return {'first_touch_label':first,'path_failure_class':outcome,
            'first_tp5_day':float(first_tp) if first_tp is not None else np.nan,
            'first_stop5_day':float(first_stop) if first_stop is not None else np.nan,
            'd20_tp5_ever':first_tp is not None,'tp5_first_conservative':first.startswith('TP_FIRST'),
            'stop5_first_conservative':stop_first or ambig,
            'stop_first_later_tp5':later_reached,'shakeout_like_fast_reclaim_later_tp5':shakeout_like,
            'prev_low_breached_postd0':support_breach,'prev_low_reclaimed_within3':reclaim_3,
            'prev_low_reclaimed_anytime':post_breach_any_reclaim,
            'd20_path_maxhigh_from_d1open_pct':100*(maxhigh/ref_entry-1),
            'd20_path_minlow_from_d1open_pct':100*(minlow/ref_entry-1),
            'd20_close_ret_gross_pct':100*(closes[-1]/ref_entry-1)}

def all_d0_preknown_proxy(features,hist,official):
    """Separate all 4,453 D0-valid events: NO outcome-maturity / future performance filter."""
    groups={code:x.sort_values('sess').reset_index(drop=True) for code,x in hist.groupby('code',sort=False)}
    pos={d:i for i,d in enumerate(official)}
    rows=[]
    for row in features[features.feature_valid.eq(True)].itertuples(index=False):
        g=groups[row.code];lookup={int(sess):idx for idx,sess in enumerate(g.sess)}
        t=pos[row.signal_date]
        require(all(t-j in lookup for j in range(1,61)),'D0_PREKNOWN_MISSING_PRIOR60')
        past=g.iloc[[lookup[t-j] for j in range(60,0,-1)]]
        oh=overhead_d0_only(past,float(row.d0_close))
        rows.append({'code':row.code,'signal_date':row.signal_date,'scope':row.scope,
          'type':row.type,'prev_low_relation':row.prev_low_relation,'candle_type':row.candle_type,
          'volume_context':row.volume_context,'setup_tag':row.setup_tag,'ma_band':row.ma_band,
          'd0_close':row.d0_close,'d0_volume_prev20_ratio':row.d0_volume_prev20_ratio,
          'ma20_slope20_pct':row.ma20_slope20_pct,'support_level_d0':row.support_level,
          'prev_low_d0':row.prev_low,**oh})
    result=pd.DataFrame(rows)
    require(len(result)==4453,'PREKNOWN_OUTPUT_4453_DRIFT')
    require(not result.duplicated(['code','signal_date','scope']).any(),'D0_FEATURE_DUP')
    require(not any(z in result.columns for z in ('d20_complete','first_tp5_day','first_stop5_day','path_failure_class','budget_net_pct')),'D0_FEATURE_LEAK')
    return result


def assemble(features,base,hist,official):
    keys=['code','signal_date','scope']
    pool=features.merge(base[keys+['budget_net_pct','exit_reason','status']],on=keys,how='left',validate='one_to_one')
    pool=pool[pool.budget_net_pct.notna() & pool.feature_valid.eq(True)].copy()
    require(len(pool)==3077,'EXPECTED_MATURE_CAUSAL_COHORT_3077')
    groups={code:x.sort_values('sess').reset_index(drop=True) for code,x in hist.groupby('code',sort=False)}
    pos={d:i for i,d in enumerate(official)}
    records=[]
    for row in pool.itertuples(index=False):
        g=groups[row.code];loc={int(s):i for i,s in enumerate(g.sess)};t=pos[row.signal_date]
        require(all(t-j in loc for j in range(1,61)) and all(t+j in loc for j in range(1,21)),'REQUIRES_60_PRIOR_AND_20_POST_CONTIGUOUS_DATES')
        past=g.iloc[[loc[t-j] for j in range(60,0,-1)]]
        future=g.iloc[[loc[t+j] for j in range(1,21)]]
        entry=float(future.iloc[0].open)
        # no D0 or future bars in supply proxy. Post D0 volume is OUTCOME ONLY.
        oh=overhead_d0_only(past,float(row.d0_close))
        ev=first_touch_path(list(future[['open','high','low','close']].itertuples(index=False,name=None)),entry,float(row.prev_low))
        prior20vol=float(past.volume.tail(20).mean())
        post5vol=float(future.volume.head(5).mean())
        future_volume_ratio_5d=post5vol/prior20vol if prior20vol>0 else float('nan')
        rejected_attempts_10d=int(((future.high.head(10)>=entry*1.03)&
            (future.high.head(10)<entry*1.05)&(future.close.head(10)<entry*1.02)).sum())
        if ev['path_failure_class']=='TP5_FIRST':diagnostic='TP_FIRST'
        elif ev['path_failure_class']=='AMBIGUOUS_TP_STOP_SAME_DAY':diagnostic='INTRADAY_UNORDERED'
        elif ev['shakeout_like_fast_reclaim_later_tp5']:diagnostic='EARLY_STOP_RECLAIM_LATER_TP_LIKE_SHAKEOUT'
        elif ev['stop_first_later_tp5']:diagnostic='STOP_THEN_LATER_TP_NOT_EARLY_RECLAIM'
        elif ev['path_failure_class']=='STOP5_FIRST_NO_LATER_TP5':
            diagnostic=('STOP_NO_TP_WITH_OVERHEAD_PROXY' if oh['overhead_proxy_type']=='ABOVE_HEAVY_PROXY' else
                        'STOP_NO_TP_WITHOUT_HIGH_OVERHEAD_PROXY')
        elif rejected_attempts_10d>=1 and oh['overhead_proxy_type']=='ABOVE_HEAVY_PROXY':
            diagnostic='NEAR_TARGET_REJECTION_WITH_OVERHEAD_PROXY'
        elif ev['d20_path_maxhigh_from_d1open_pct']<3 and math.isfinite(future_volume_ratio_5d) and future_volume_ratio_5d<.65:
            diagnostic='LOW_VOL_NO_FOLLOWTHROUGH_EXPOST'
        else:diagnostic='STALLED_OTHER_EXPOST'
        baseline=str(row.exit_reason)
        expect=('TARGET' if ev['tp5_first_conservative'] else 'STOP' if ev['stop5_first_conservative'] else 'D20')
        require(baseline.startswith(expect),'FIRST_TOUCH_DISAGREES_WITH_R122_BASELINE:'+row.code+':'+row.signal_date+':'+baseline+':'+expect)
        records.append({'code':row.code,'signal_date':row.signal_date,'scope':row.scope,'type':row.type,
          'setup_tag':row.setup_tag,'volume_context':row.volume_context,'prev_low_relation':row.prev_low_relation,
          'candle_type':row.candle_type,'ma_band':row.ma_band,
          'd0_close':row.d0_close,'d1_open':entry,'prev_low_d0':row.prev_low,
          'support_level_d0':row.support_level,'d0_volume_prev20_ratio':row.d0_volume_prev20_ratio,
          'd0_close_location':row.close_location,'d0_upper_wick_fraction':row.upper_wick_fraction,
          'ma20_slope20_pct':row.ma20_slope20_pct,'close_vs_ma20_pct':row.close_vs_ma20_pct,
          'r122_baseline_exit_reason':baseline,'r122_baseline_budget_net_pct':float(row.budget_net_pct),
          'post5_volume_to_prior20_mean_ratio_EXPOST':future_volume_ratio_5d,
          'rejected_attempts_3to5_first10_EXPOST':rejected_attempts_10d,
          'failure_diagnostic_EXPOST_NOT_CAUSAL':diagnostic,
          **oh,**ev})
    out=pd.DataFrame(records)
    require(len(out)==3077 and not out.duplicated(keys).any(),'OUTPUT_NOT_UNIQUE')
    return out

def aggregate(events):
    d=[]
    groups=[('ALL',['scope']),('CANDLE_SUPPORT',['scope','prev_low_relation']),
            ('PRE_D0_SUPPLY_PROXY',['scope','overhead_proxy_type']),
            ('SUPPLY_X_SUPPORT',['scope','overhead_proxy_type','prev_low_relation']),
            ('R122_TYPE',['scope','type'])]
    for dims,cols in groups:
        for key,g in events.groupby(cols,dropna=False):
            key=key if isinstance(key,tuple) else (key,)
            def pct(c):return float(g[c].mean()*100)
            d.append(dict(dimension=dims,**dict(zip(cols,key)),episode_n=len(g),
                tp5_ever_pct=pct('d20_tp5_ever'),tp5_first_pct=pct('tp5_first_conservative'),
                stop5_first_pct=pct('stop5_first_conservative'),stop_then_later_tp5_pct=pct('stop_first_later_tp5'),
                shakeout_like_strict_pct=pct('shakeout_like_fast_reclaim_later_tp5'),
                prev_low_breach_pct=pct('prev_low_breached_postd0'),
                prev_low_early_reclaim_pct=pct('prev_low_reclaimed_within3'),
                avg_simulated_budget_net_pct=float(g.r122_baseline_budget_net_pct.mean()),
                avg_maxhigh_d20_pct=float(g.d20_path_maxhigh_from_d1open_pct.mean()),
                avg_maxdrawdown_d20_pct=float(g.d20_path_minlow_from_d1open_pct.mean())))
    return pd.DataFrame(d)

def audit_cases(events):
    cases=events.copy()
    cases['case'] = np.select([cases.shakeout_like_fast_reclaim_later_tp5,
       cases.stop_first_later_tp5,
       cases.path_failure_class.eq('STOP5_FIRST_NO_LATER_TP5')&cases.overhead_proxy_type.eq('ABOVE_HEAVY_PROXY'),
       cases.path_failure_class.eq('NO_TOUCH_NEAR_TP5')&cases.overhead_proxy_type.eq('ABOVE_HEAVY_PROXY')],
      ['EARLY_STOP_RECOVERY_PATTERN','STOP_THEN_LATER_TARGET','HIGH_OVERHEAD_PROXY_STOP_NO_RECOVERY',
       'HIGH_OVERHEAD_PROXY_NEAR_TARGET_NOT_REACHED'],default='OTHER')
    # Cases selected by path category, not as positive training input. First 12 per group for manual blind review.
    c=cases[cases.case!='OTHER'].sort_values(['scope','case','signal_date','code']).groupby(['scope','case'],group_keys=False).head(12)
    return c[['scope','case','code','signal_date','type','prev_low_relation','overhead_proxy_type','overhead_share_of_local_60',
       'd0_volume_prev20_ratio','first_tp5_day','first_stop5_day','d20_path_maxhigh_from_d1open_pct','d20_path_minlow_from_d1open_pct']]

def run(root,archives,out):
    f,base,m=load_r122(root)
    h,official,seen=read_immutable_krx(Path(archives),lowest_date='2025-01-01')
    require(len(seen)==1907,'KRX_SOURCE_DAYS_CHANGED')
    d0only=all_d0_preknown_proxy(f,h,official)
    events=assemble(f,base,h,official)
    common=events.merge(d0only[['scope','code','signal_date','overhead_close_volume_share_60']],on=['scope','code','signal_date'],how='left',validate='one_to_one')
    require(len(common)==3077 and float((common.overhead_close_volume_share_60_x-common.overhead_close_volume_share_60_y).abs().max())<1e-10,'D0_PROXY_REPRODUCIBILITY_DRIFT')
    diagnostic_counts=(events.groupby(['scope','failure_diagnostic_EXPOST_NOT_CAUSAL'])
                    .agg(events=('code','size'),mean_gross_d20_close_pct=('d20_close_ret_gross_pct','mean'),
                         mean_post5_volume_ratio=('post5_volume_to_prior20_mean_ratio_EXPOST','mean'))
                    .reset_index())
    results={'r123_d0_asof_overhead_proxy_NO_FUTURE_COLUMNS.csv':d0only,
             'r123_success_failure_path_ledger.csv':events,
             'r123_candle_support_overhead_proxy_stratified.csv':aggregate(events),
             'r123_failure_mechanism_diagnostics_NONCAUSAL.csv':diagnostic_counts,
             'r123_forensic_casebook_HINDSIGHT_SAMPLED.csv':audit_cases(events)}
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    checks={}
    for name,x in results.items():
        x.to_csv(out/name,index=False,float_format='%.9g');checks[name]=sha(out/name)
    meta={'revision':REVISION,'status':'PASS_SOURCE_LIMITED_D20_PATH_FORENSIC_DIAGNOSTIC_ONLY',
          'r122_source_run':SOURCE_RUN,'r122_source_artifact':SOURCE_ARTIFACT,
          'r122_manifest_sha256':PINNED_R122_MANIFEST_SHA,
          'source_events':4567,'d0_feature_valid_events':4453,'source_eligible_events':3077,'output_events':len(events),
          'krx_day_sha_verified':len(seen),
          'scope_counts':events.scope.value_counts().to_dict(),
          'path_failure_class_counts':events.path_failure_class.value_counts().to_dict(),
          'shakeout_like_count':int(events.shakeout_like_fast_reclaim_later_tp5.sum()),
          'stop_first_later_tp5_count':int(events.stop_first_later_tp5.sum()),
          'overhead_proxy_high_count':int(events.overhead_proxy_type.eq('ABOVE_HEAVY_PROXY').sum()),
          'future_outcomes_not_used_for_D0_features':True,
          'supply_proxy_method':'60 prior official days; use all volume allocated to each daily CLOSE (NOT exchange VAP); 0~+8pct vs -8~0pct of D0 close. Zone must hold >=20pct of prior volume; above-heavy>=60pct above fraction.',
          'shakeout_like_definition':'Stop-5 touched before +5, within 5 official days after entry; prior D0 candle low breached, next 3 daily closes regain D0 prior-day low, and +5 reached later within 20 days. Retrospective pattern, not manipulation evidence.',
          'limitations':['Future outcomes used ONLY to describe path labels, never source candidate eligibility or pre-D0 supply features',
           '907-code EX-POST saved universe, missing 69% historical market; full-market recall invalid',
           'daily OHLC ambiguous intraday first-touch; same-bar target/stop explicitly UNKNOWN and conservative stop in R122',
           'daily close-volume at price is crude proxy, cannot prove real overhang/seller intent or order book',
           'stop-first then later rise is NOT an actual winning trade; post-stop rise is hindsight',
           'no full-market historical PIT, real auction fill or prospective OOS; no tuning or LIVE authority'],
          'full_market_PIT_available':False,'trade_authority':False,'live_changed':False,'shadow_changed':False,
          'autoexit_changed':False,'sha256':checks}
    (out/'r123_integrity_manifest.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R123_RESEARCH_PASS',json.dumps({k:meta[k] for k in ('status','source_eligible_events','krx_day_sha_verified','scope_counts','path_failure_class_counts','shakeout_like_count','stop_first_later_tp5_count')},ensure_ascii=False))
    return meta

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--r122-root',required=True);ap.add_argument('--history-dir',required=True);ap.add_argument('--output-dir',required=True)
    a=ap.parse_args();run(a.r122_root,a.history_dir,a.output_dir)
if __name__=='__main__':main()
