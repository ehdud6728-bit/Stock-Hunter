#!/usr/bin/env python3
"""R1.19: fixed D1-open to D20 target-touch and first-touch research on R1.18's limited cohort.

Never a LIVE gate. Full market historical PIT denominator still UNKNOWN. Only verified
stored KRX code-days, no future feature filtering, and no fill/execution claim.
"""
from __future__ import annotations
import argparse, hashlib, json, math
from pathlib import Path
import numpy as np
import pandas as pd
from closebet_research import r118_partial_market_wave_geometry as r118

VERSION='R119_D20_TARGET_FIRST_TOUCH_COVERED_COHORT_V1'
HORIZONS=(5,10,15,20)
TARGETS=(3.,5.,10.,15.)
STOPS=(3.,5.,7.)
SHAFILE='r118_40305_broad_224_wave_geometry.csv'
STATUS='PASS_LIMITED_COHORT_D20_DESCRIPTIVE_NOT_TRADE_APPROVED'


def require(p,s):
    if not p:raise RuntimeError(s)


def read_r118(root:Path)->tuple[pd.DataFrame,dict]:
    m=json.loads((root/'r118_integrity_manifest.json').read_text(encoding='utf-8'))
    require(m.get('version')=='R118_PARTIAL_MARKET_GAP_224_ORIGIN_BROAD_GEOMETRY_V1','R118_WRONG_VERSION')
    require(m.get('status')=='PASS_FULL_MARKET_GAP_AND_BROAD_WAVE_COVERED_COHORT_ONLY_NOT_FULL_PIT_RECALL','R118_STATUS_UNTRUSTED')
    raw=(root/SHAFILE).read_bytes()
    require(hashlib.sha256(raw).hexdigest()==m['file_sha256'][SHAFILE],'R118_COHORT_SHA_MISMATCH')
    q=pd.read_csv(root/SHAFILE,dtype={'code':str},low_memory=False)
    q['code']=q.code.map(r118.sym)
    q['signal_date']=q.signal_date.astype(str).str[:10]
    require(len(q)==40305 and len(q[['code','signal_date']].drop_duplicates())==40305,'R118_COHORT_INCORRECT')
    require(int(q.selected_any_mode.sum())==1059,'R118_SELECTED_DRIFT')
    return q,m


def path_outcome(signal_day:str, official:list[str], t:int, d0_close:float, arr:dict[str,np.ndarray], lookup:dict[int,int])->dict:
    out={'entry_day':official[t+1] if t+1<len(official) else '',
         'entry_d1_open':np.nan,'observed_contiguous_days':0,
         'd20_complete':False,'corp_action_future_suspect':False,
         'corp_action_future_suspect_count':0,
         'd20_close_net05pct':np.nan}
    for h in HORIZONS:out[f'd{h}_complete']=False
    for target in TARGETS:
        key='t'+str(int(target))
        out[f'{key}_first_day']=np.nan
        for h in HORIZONS:out[f'{key}_by_d{h}']='UNKNOWN_CENSORED'
    for stop in STOPS:
        key='s'+str(int(stop))
        out[f'{key}_first_day']=np.nan
        out[f't5_vs_{key}_d20']='UNKNOWN_CENSORED'
        out[f't5_vs_{key}_d20_best']='UNKNOWN_CENSORED'
        out[f't5_vs_{key}_idealized_net05_lower_pct']=np.nan
        out[f't5_vs_{key}_idealized_net05_upper_pct']=np.nan
        out[f't5_intraday_same_day_{key}_ambiguous']=False
    out['t5_drawdown_before_first_touch_pct']=np.nan
    if t+1>=len(official) or (t+1) not in lookup:
        out['input_status']='D1_ENTRY_MISSING';return out
    j=lookup[t+1]
    entry_px=float(arr['open'][j])
    if not math.isfinite(entry_px) or entry_px<=0:
        out['input_status']='D1_ENTRY_PRICE_INVALID';return out
    out['entry_d1_open']=entry_px
    obs=[]
    for dt in range(t+1,min(t+21,len(official))):
        i=lookup.get(dt)
        if i is None:break
        v=tuple(float(arr[k][i]) for k in ('open','high','low','close'))
        oo,hh,ll,cc=v
        if not all(math.isfinite(x) for x in v) or min(v)<=0 or not (ll<=min(oo,cc)<=max(oo,cc)<=hh):break
        obs.append(v)
    n=len(obs)
    out['observed_contiguous_days']=n
    close=d0_close
    jump_count=0
    for oo,hh,ll,cc in obs:
        if math.isfinite(close) and close>0 and abs(cc/close-1)>.35:jump_count+=1
        close=cc
    out['corp_action_future_suspect']=bool(jump_count)
    out['corp_action_future_suspect_count']=jump_count
    for h in HORIZONS:out[f'd{h}_complete']=bool(n>=h)
    out['d20_complete']=bool(n>=20)
    if n>=20:out['d20_close_net05pct']=(obs[19][3]/entry_px-1)*100-.5
    if not n:
        out['input_status']='NO_FORWARD_CONTIGUOUS_BARS';return out
    out['input_status']='D20_COMPLETE' if n>=20 else 'RIGHT_CENSORED_BEFORE_D20'
    for target in TARGETS:
        key='t'+str(int(target)); ix=next((i for i,v in enumerate(obs) if v[1]>=entry_px*(1+target/100)),None)
        if ix is not None:out[f'{key}_first_day']=ix+1
        for h in HORIZONS:
            if ix is not None and ix<h:out[f'{key}_by_d{h}']='HIT_HIGH_NOT_FILL'
            elif n>=h:out[f'{key}_by_d{h}']='NOT_HIT_WITH_COMPLETE_HORIZON'
    tp=entry_px*1.05
    for stop in STOPS:
        key='s'+str(int(stop)); stop_px=entry_px*(1-stop/100)
        ix=next((i for i,v in enumerate(obs) if v[2]<=stop_px),None)
        if ix is not None:out[f'{key}_first_day']=ix+1
        decided='NO_FIRST_TOUCH_WITH_COMPLETE_D20' if n>=20 else 'UNKNOWN_CENSORED'
        best=decided
        for oo,hh,ll,cc in obs:
            if oo>=tp:decided=best='TP_OPEN_GAP_FIRST';break
            if oo<=stop_px:decided=best='STOP_OPEN_GAP_FIRST';break
            t_hit=hh>=tp;s_hit=ll<=stop_px
            if t_hit and s_hit:
                decided='AMBIGUOUS_BOTH_SAME_DAY_CONSERVATIVE_STOP'
                best='AMBIGUOUS_BOTH_SAME_DAY_OPTIMISTIC_TP'
                out[f't5_intraday_same_day_{key}_ambiguous']=True
                break
            if t_hit:decided=best='TP_FIRST_HIGH_TOUCH';break
            if s_hit:decided=best='STOP_FIRST_LOW_TOUCH';break
        out[f't5_vs_{key}_d20']=decided
        out[f't5_vs_{key}_d20_best']=best
        if decided=='TP_OPEN_GAP_FIRST' or decided=='STOP_OPEN_GAP_FIRST':
            # Open is observable but assumes the strategy can exit at the opening print.
            gross=(oo/entry_px-1)*100
            lo_net=hi_net=gross-.5
        elif decided=='TP_FIRST_HIGH_TOUCH':
            lo_net=hi_net=5.-.5
        elif decided=='STOP_FIRST_LOW_TOUCH':
            lo_net=hi_net=-stop-.5
        elif decided=='AMBIGUOUS_BOTH_SAME_DAY_CONSERVATIVE_STOP':
            lo_net=-stop-.5;hi_net=5.-.5
        elif decided=='NO_FIRST_TOUCH_WITH_COMPLETE_D20':
            lo_net=hi_net=(obs[19][3]/entry_px-1)*100-.5
        else:lo_net=hi_net=np.nan
        out[f't5_vs_{key}_idealized_net05_lower_pct']=lo_net
        out[f't5_vs_{key}_idealized_net05_upper_pct']=hi_net
    ix=next((i for i,v in enumerate(obs) if v[1]>=tp),None)
    if ix is not None:out['t5_drawdown_before_first_touch_pct']=(min(v[2] for v in obs[:ix+1])/entry_px-1)*100
    return out


def outcome_for_days(signal_day:str, df:pd.DataFrame, official_idx:dict[str,int], official:list[str])->dict:
    """Testable dataframe adapter for strict D+1 to D+20 array-based analysis."""
    q=df.sort_values('sess')
    arr={k:q[k].to_numpy(dtype=float) for k in ('open','high','low','close')}
    lookup={int(s):i for i,s in enumerate(q['sess'])}
    t=official_idx[signal_day]
    d0=arr['close'][lookup[t]] if t in lookup else np.nan
    return path_outcome(signal_day,official,t,d0,arr,lookup)


def compute(cohort:pd.DataFrame, hist:pd.DataFrame, official:list[str])->pd.DataFrame:
    pos={d:i for i,d in enumerate(official)}
    require(cohort.signal_date.isin(pos).all(),'SIGNAL_DATE_NOT_IN_OFFICIAL_MARKET_CALENDAR')
    needed={code:set(g.signal_date) for code,g in cohort.groupby('code')}
    outs=[]
    for code,g in hist.groupby('code',sort=False):
        if code not in needed:continue
        g=g.sort_values('sess')
        arr={k:g[k].to_numpy(dtype=float) for k in ('open','high','low','close')}
        lookup={int(s):i for i,s in enumerate(g.sess)}
        for day in needed.pop(code):
            t=pos[day]
            d0=arr['close'][lookup[t]] if t in lookup else np.nan
            q=path_outcome(day,official,t,d0,arr,lookup)
            q.update({'code':code,'signal_date':day})
            outs.append(q)
    require(not needed,'MISSING_TICKERS_IN_KRX_SOURCE')
    o=pd.DataFrame(outs)
    require(len(o)==len(cohort) and not o.duplicated(['code','signal_date']).any(),'D20_CODE_DATE_INCOMPLETE')
    return o

def grouped_summary(a:pd.DataFrame, *, label:str='all')->pd.DataFrame:
    # Complete-horizon denominator independent of event: prevents censoring from artificially boosting hits.
    rows=[]
    groups=['ALL_COVERED','V4976_SELECTED','V4976_RAW_NOT_TOP5','NOT_RAW_BROAD_PROXY','BROAD_V4976_SELECTED','BROAD_NOT_V4976','NOT_BROAD_V4976_SELECTED']
    for name in groups:
        if name=='ALL_COVERED':g=a
        elif name=='V4976_SELECTED':g=a[a.selected_any_mode]
        elif name=='V4976_RAW_NOT_TOP5':g=a[a.v4976_raw_predicate_hit&~a.selected_any_mode]
        elif name=='NOT_RAW_BROAD_PROXY':g=a[a.broad_research_eligible&~a.v4976_raw_predicate_hit]
        elif name=='BROAD_V4976_SELECTED':g=a[a.broad_research_eligible&a.selected_any_mode]
        elif name=='BROAD_NOT_V4976':g=a[a.broad_research_eligible&~a.selected_any_mode]
        else:g=a[~a.broad_research_eligible&a.selected_any_mode]
        # Descriptive all as-of valid rows, plus future corporate action suspect-free subgroup.
        for quality in ['ASOF_ELIGIBLE','ASOF_ELIGIBLE_NO_POSTD0_SUSPECT_JUMP']:
            y=g[g.asof_quality_gate].copy()
            if quality.endswith('JUMP'):y=y[~y.corp_action_future_suspect]
            for h in HORIZONS:
                z=y[y[f'd{h}_complete']].copy()
                for target in TARGETS:
                    state=z[f't{int(target)}_by_d{h}']
                    require(not state.astype(str).str.contains('UNKNOWN').any(),'COMPLETE_HIT_UNKNOWN')
                    hits=state.eq('HIT_HIGH_NOT_FILL')
                    rows.append({'panel':label,'group':name,'quality':quality,'horizon':h,'target_pct':target,
                     'cohort_code_days':len(g),'asof_quality_eligible':int(g.asof_quality_gate.sum()),
                     'complete_horizon_n':len(z), 'complete_horizon_days':int(z.signal_date.nunique()),
                     'high_touch_n':int(hits.sum()),'high_touch_pct':float(hits.mean()*100) if len(z) else np.nan,
                     'first_touch_median_d':float(z.loc[hits,f't{int(target)}_first_day'].median()) if hits.any() else np.nan,
                     'incomplete_horizon_n':int(len(y)-len(z)),
                     'future_action_suspect_in_complete':int(z.corp_action_future_suspect.sum()),
                     'interpretation':'HIGH_TOUCH_OPPORTUNITY_ONLY_NO_FILL_NO_PIT_FULL_MARKET'})
    return pd.DataFrame(rows)


def stop_summary(a:pd.DataFrame)->pd.DataFrame:
    rows=[]
    for group,mask in [('V4976_SELECTED',a.selected_any_mode),
                       ('BROAD_NOT_V4976',a.broad_research_eligible & ~a.selected_any_mode),
                       ('BROAD_V4976_SELECTED',a.broad_research_eligible & a.selected_any_mode),
                       ('V4976_RAW_NOT_TOP5',a.v4976_raw_predicate_hit & ~a.selected_any_mode),
                       ('ALL_COVERED',pd.Series(True,index=a.index))]:
        z=a[mask & a.asof_quality_gate & a.d20_complete & ~a.corp_action_future_suspect]
        for stop in STOPS:
            s=z[f't5_vs_s{int(stop)}_d20'].astype(str)
            tp=s.isin(['TP_FIRST_HIGH_TOUCH','TP_OPEN_GAP_FIRST'])
            stop_first=s.isin(['STOP_FIRST_LOW_TOUCH','STOP_OPEN_GAP_FIRST','AMBIGUOUS_BOTH_SAME_DAY_CONSERVATIVE_STOP'])
            both=s.eq('AMBIGUOUS_BOTH_SAME_DAY_CONSERVATIVE_STOP')
            neither=s.eq('NO_FIRST_TOUCH_WITH_COMPLETE_D20')
            require(int(tp.sum()+stop_first.sum()+neither.sum())==len(z),'BAD_STOP_CLASS_COUNTS')
            rows.append({'group':group,'horizon':20,'take_profit_pct':5,'stop_loss_pct':-stop,'complete_valid_n':len(z),
             'tp_first_conservative_n':int(tp.sum()),'tp_first_conservative_pct':float(tp.mean()*100) if len(z) else np.nan,
             'stop_first_conservative_n':int(stop_first.sum()),'stop_first_conservative_pct':float(stop_first.mean()*100) if len(z) else np.nan,
             'same_day_unknown_order_n':int(both.sum()),'same_day_unknown_order_pct':float(both.mean()*100) if len(z) else np.nan,
             'neither_touched_n':int(neither.sum()),
             'tp_first_optimistic_upper_bound_pct':float((tp|both).mean()*100) if len(z) else np.nan,
             'idealized_net05_lower_mean_pct':float(z[f't5_vs_s{int(stop)}_idealized_net05_lower_pct'].mean()) if len(z) else np.nan,
             'idealized_net05_upper_mean_pct':float(z[f't5_vs_s{int(stop)}_idealized_net05_upper_pct'].mean()) if len(z) else np.nan,
             'idealized_net05_lower_win_pct':float(z[f't5_vs_s{int(stop)}_idealized_net05_lower_pct'].gt(0).mean()*100) if len(z) else np.nan,
             'idealized_net05_upper_win_pct':float(z[f't5_vs_s{int(stop)}_idealized_net05_upper_pct'].gt(0).mean()*100) if len(z) else np.nan,
             'method':'IDEALIZED_TP_STOP_ENTRY_FILL_THRESHOLDS_NOT_REAL_EXECUTABLE_RETURN'})
    return pd.DataFrame(rows)


def episode_and_cooldown(a:pd.DataFrame,official:list[str])->pd.DataFrame:
    pos={d:i for i,d in enumerate(official)}
    z=a.sort_values(['code','signal_date']).copy()
    # For all stock-days: first per code then enforce non-overlapping 20-session windows.
    # Synthetic de-overlapping evaluation ONLY; does not change search membership.
    kept=set();last={}
    for r in z[['code','signal_date']].itertuples(index=False):
        day=pos[r.signal_date]
        if r.code not in last or day-last[r.code]>20:
            kept.add((r.code,r.signal_date));last[r.code]=day
    z['deoverlap20_start']=[(c,d) in kept for c,d in zip(z.code,z.signal_date)]
    require(z.deoverlap20_start.sum()<=len(z),'DEOVERLAP_BAD')
    return z


def date_matched(a:pd.DataFrame)->pd.DataFrame:
    z=a[a.asof_quality_gate & a.d20_complete & ~a.corp_action_future_suspect].copy()
    z['hit5']=z.t5_by_d20.eq('HIT_HIGH_NOT_FILL')
    rows=[]
    for day,g in z.groupby('signal_date'):
        for name,mask in [('V4976_SELECTED',g.selected_any_mode),('BROAD_NOT_V4976',g.broad_research_eligible&~g.selected_any_mode)]:
            h=g[mask];rows.append({'signal_date':day,'group':name,'n':len(h),'hit5_n':int(h.hit5.sum()),'hit5_pct':float(h.hit5.mean()*100) if len(h) else np.nan})
    q=pd.DataFrame(rows)
    a=q[q.group.eq('V4976_SELECTED')][['signal_date','n','hit5_pct']].rename(columns={'n':'v4976_n','hit5_pct':'v4976_hit5_pct'})
    b=q[q.group.eq('BROAD_NOT_V4976')][['signal_date','n','hit5_pct']].rename(columns={'n':'broad_outside_n','hit5_pct':'broad_outside_hit5_pct'})
    x=a.merge(b,on='signal_date',how='inner',validate='one_to_one')
    x['both_nonempty']=x.v4976_n.gt(0)&x.broad_outside_n.gt(0)
    x['diff_v4976_minus_broad_pp']=x.v4976_hit5_pct-x.broad_outside_hit5_pct
    return x


def run(args):
    c,m=read_r118(args.r118_root)
    hist,official,seen=r118.read_immutable_krx(args.history_dir,lowest_date='2025-01-01')
    require(len(official)==1907,'KRX_1907_SESSION_HASH_CONTRACT')
    o=compute(c,hist,official)
    a=c.merge(o,on=['code','signal_date'],validate='one_to_one')
    require(a.selected_any_mode.sum()==1059,'R119_SELECTED_COUNT_DRIFT')
    require(len(a)==40305,'R119_COHORT_COUNT_DRIFT')
    a=episode_and_cooldown(a,official)
    panels=[grouped_summary(a)]
    panels.append(grouped_summary(a[a.d20_complete],label='same_D20_complete_population_all_horizons'))
    # Non-overlapping signals reduce false confidence from repeated ticker dates.
    panels.append(grouped_summary(a[a.deoverlap20_start],label='deoverlap20_within_available_window'))
    panels.append(grouped_summary(a[a.broad_episode_start_within_window],label='broad_episode_observed_starts_only'))
    summary=pd.concat(panels,ignore_index=True)
    stop=stop_summary(a)
    dates=date_matched(a)
    args.output.mkdir(parents=True,exist_ok=True)
    files={'r119_40305_d20_first_touch_ledger.csv':a,
           'r119_target_high_touch_rates_d5_d10_d15_d20.csv':summary,
           'r119_d20_t5_vs_stop3_5_7_first_touch.csv':stop,
           'r119_date_matched_hit5_d20.csv':dates}
    hashes={}
    for n,d in files.items():
        f=args.output/n;d.to_csv(f,index=False,encoding='utf-8-sig');hashes[n]=hashlib.sha256(f.read_bytes()).hexdigest()
    manifest={'version':VERSION,'status':STATUS,'research_only':True,'live_search_changed':False,'shadow_board_changed':False,'orders_changed':False,
              'source_r118_version':m['version'],'source_r117_run':38856340802,'source_r171_run':38664095610,
              'source_original_r118_40305_sha256':m['file_sha256'][SHAFILE],
              'krx_source_daily_sha_days_verified':len(official),'source_rows':len(c),
              'selected_unique_code_days':int(a.selected_any_mode.sum()),
              'last_immutable_krx_day':official[-1],
              'd20_complete_rows':int(a.d20_complete.sum()),
              'd20_incomplete_right_censored_rows':int((~a.d20_complete).sum()),
              'asof_quality_and_d20_complete_rows':int((a.asof_quality_gate & a.d20_complete).sum()),
              'postd0_corp_jump_suspect_rows':int(a.corp_action_future_suspect.sum()),
              'deoverlap20_episode_count':int(a.deoverlap20_start.sum()),
              'broad_episode_starts_count':int(a.broad_episode_start_within_window.sum()),
              'file_sha256':hashes,
              'limits':{'pit_full_market_denominator':'UNKNOWN_NOT_AVAILABLE',
                'historical_code_universe':'EXPOST_COVERED_907_NAME_SUBSET',
                'opportunity_not_trade':'DAILY_HIGH_IS_NOT_A_REALIZED_FILL',
                'stop_first_same_daily_bar':'UNKNOWN_BOUNDS_CONSERVATIVE_STOP_VS_OPTIMISTIC_TP',
                'strategy_performance':'NOT_COMPUTED_NO_GUARANTEED_FILLS_OR_FINAL_EXIT_POLICY',
                'event_repeats':'ALL_CODE_DAYS_AND_DEOVERLAPPED20_SHOWN_SEPARATELY',
                'right_censoring':'HORIZON_COMPLETE_ONLY_IN_RATES',
                'feature_leakage':'D0_INDEPENDENT_FUTURE_ONLY_LABELS',
                'd0_close_as_1503_proxy':'FORBIDDEN',
                'corporate_action':'ASOF_35PCT_JUMP_FLAGS_AND_FUTURE_JUMP_SENSITIVITY_NOT_OFFICIAL_CORP_ACTION_DATA',
                'candidate_improvement_claim':'FORBIDDEN_DESCRIPTIVE_NOT_OOS',
                'live_trading_approval':False}}
    (args.output/'r119_integrity_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R119_DESCRIPTIVE_PASS',json.dumps({k:v for k,v in manifest.items() if k in ('status','source_rows','d20_complete_rows','d20_incomplete_right_censored_rows','asof_quality_and_d20_complete_rows','selected_unique_code_days','postd0_corp_jump_suspect_rows')},ensure_ascii=False))
    return manifest


def main(argv=None):
    p=argparse.ArgumentParser(description='D+20 opportunity and stop-first research, never LIVE')
    p.add_argument('--r118-root',type=Path,required=True)
    p.add_argument('--history-dir',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    return run(p.parse_args(argv))

if __name__=='__main__':main()
