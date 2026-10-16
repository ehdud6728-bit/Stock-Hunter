#!/usr/bin/env python3
"""R1.20 research-only: pre-D0 classifications x entry timing x scale-ins x stop levels.

This is a hypothetical end-of-day OHLC scenario comparison; it is NOT a historical
fill reconstruction, optimized strategy, full-market denominator or trade approval.
No post-D0 future field is used for cohort membership, type, or policy choice.
"""
from __future__ import annotations
import argparse, hashlib, json, math
from pathlib import Path
import numpy as np
import pandas as pd

VERSION='R120_ENTRY_SCALE_STOP_ASOF_EPISODE_V1'
SOURCE='R119_D20_TARGET_FIRST_TOUCH_COVERED_COHORT_V1'
STATUS='PASS_EPISODE_ENTRY_SCALE_STOP_DESCRIPTIVE_NOT_OOS_NOT_TRADE_APPROVED'
SCOPE=('V4976_SELECTED','V4976_RAW_NOT_TOP5','BROAD_NOT_RAW','OTHER_COVERED')
ENTRY_POLICIES=('D1_FULL','D1_HALF_D2TO5_DIP3','D1_40_D2TO5_DIP2_D2TO8_DIP4','D2TO6_CONFIRM_D0HIGH')
STOP_POLICIES=('STOP3','STOP5','STOP7','D0_SUPPORT5','D0_ATR14_X2')
FEE_PCT=0.50

def require(x,msg):
    if not x:raise RuntimeError(msg)

def sha(p:Path)->str:return hashlib.sha256(p.read_bytes()).hexdigest()

def b(v):return str(v).strip().lower() in ('true','1','yes') if not isinstance(v,(bool,np.bool_)) else bool(v)

def code(v):return str(v).strip().split('.')[0].zfill(6)

def read_checked(root:Path, mf:str, fn:str, version:str|None=None)->tuple[pd.DataFrame,dict]:
    man=json.loads((root/mf).read_text(encoding='utf-8'))
    if version:require(man.get('version')==version,'SOURCE_VERSION_DRIFT '+mf)
    expected=(man.get('file_sha256') or man.get('output_sha256') or {}).get(fn)
    require(bool(expected) and sha(root/fn)==expected,'SOURCE_SHA_INVALID '+fn)
    q=pd.read_csv(root/fn,dtype={'code':str},low_memory=False)
    q['code']=q['code'].map(code)
    q['signal_date']=q.signal_date.astype(str).str.slice(0,10)
    require(not q.duplicated(['code','signal_date']).any(),'SOURCE_DUPLICATE '+fn)
    return q,man

def load_sources(r119:Path,r112:Path,r110:Path)->tuple[pd.DataFrame,dict]:
    main,m119=read_checked(r119,'r119_integrity_manifest.json','r119_40305_d20_first_touch_ledger.csv',SOURCE)
    require(m119.get('status')=='PASS_LIMITED_COHORT_D20_DESCRIPTIVE_NOT_TRADE_APPROVED','R119_NOT_PASS')
    require(len(main)==40305 and int(main.selected_any_mode.sum())==1059,'R119_POPULATION_DRIFT')
    p,m112=read_checked(r112,'r112_integrity_manifest.json','r112_all_selected_1059_pit_wave_stage.csv')
    require(m112.get('status')=='PASS_EXPLORATORY_NOT_TRADE_APPROVED','R112_NOT_VALID')
    require(len(p)==1059,'R112_POPULATION_DRIFT')
    prior,m110=read_checked(r110,'r110_integrity_manifest.json','r110_signal_flags_and_outcomes.csv')
    require(m110.get('status')=='PASS_DISCOVERY_ONLY_NOT_TRADE_APPROVED','R110_NOT_VALID')
    require(len(prior)==754,'R110_POPULATION_DRIFT')
    # R110 future outcome columns must never enter types, ranking, or policy signals.
    safe110=['code','signal_date','wave_stage_label','prewave_prevolume_accum_ratio',
             'prewave_pullback_vol_ratio','volume_dryup_10_30','wave_setup_stage']
    out=main.merge(p[['code','signal_date','stage','episode_first_c10','age_from_impulse',
                      'last5_to_impulse_volume','prior_224_corporate_action_suspect',
                      'prior_224_suspended_session','day_suspended']],on=['code','signal_date'],how='left',validate='one_to_one')
    out=out.merge(prior[safe110],on=['code','signal_date'],how='left',validate='one_to_one')
    require(int(out.loc[out.selected_any_mode,'stage'].notna().sum())==1059,'R112_SELECTED_JOIN_DRIFT')
    require(out.loc[~out.selected_any_mode,'stage'].isna().all(),'R112_OUTSIDE_SELECTED_LEAK')
    info={'r119_sha256':sha(r119/'r119_40305_d20_first_touch_ledger.csv'),
          'r112_sha256':sha(r112/'r112_all_selected_1059_pit_wave_stage.csv'),
          'r110_sha256':sha(r110/'r110_signal_flags_and_outcomes.csv'),
          'r110_asof_join_n':int(out.wave_stage_label.notna().sum()),
          'r112_asof_join_n':int(out.stage.notna().sum()),'source_run_ids':[38865542460,38823870672,38745541969]}
    return out,info

def classify(row)->str:
    """Purely as-of broad geometry, descriptive bins not optimized efficacy thresholds."""
    if not b(row['asof_quality_gate']):return 'DATA_ASOF_INVALID'
    if str(row.get('stage') or '') in ('CORP_ACTION_QUARANTINE','SUSPENSION_224_QUARANTINE'):
        return 'DATA_ASOF_QUARANTINE'
    if b(row['broad_research_eligible']):
        age=float(row['peak_to_d0_sessions']); retrace=float(row['pullback_fraction'])
        if age>20:return 'AGED_PULLBACK_AFTER_PEAK'
        if retrace<=1/3:return 'SHALLOW_PULLBACK'
        if retrace<=2/3:return 'MID_PULLBACK'
        return 'DEEP_PULLBACK'
    if b(row['origin_below224_asof']) and float(row['close_vs_ma224_pct'])<0:
        return 'BELOW224_ORIGIN_NOT_BROAD_PB'
    return 'NO_BROAD_224_WAVE_GEOMETRY'

def scope(row)->str:
    if b(row['selected_any_mode']):return 'V4976_SELECTED'
    if b(row['v4976_raw_predicate_hit']):return 'V4976_RAW_NOT_TOP5'
    if b(row['broad_research_eligible']):return 'BROAD_NOT_RAW'
    return 'OTHER_COVERED'

def choose_episodes(a:pd.DataFrame, official:list[str])->pd.DataFrame:
    """Deterministic first as-of-eligible event for each code x scope, 20-session embargo.

    Never prefilter on D20 completeness, future action or future returns (would leak).
    Separate scopes to avoid selecting only dates on which the ex-post outcome exists.
    """
    pos={d:i for i,d in enumerate(official)}
    require(a.signal_date.isin(pos).all(),'UNKNOWN_SIGNAL_DATE')
    v=a[a.asof_quality_gate.astype(bool) & ~a['type'].isin(['DATA_ASOF_INVALID','DATA_ASOF_QUARANTINE'])].sort_values(['scope','code','signal_date']).copy()
    lasts={};keep=[]
    for row in v[['scope','code','signal_date']].itertuples(index=False):
        key=(row.scope,row.code); idx=pos[row.signal_date]
        yes=key not in lasts or idx-lasts[key]>20
        keep.append(yes)
        if yes:lasts[key]=idx
    v['episode_first_by_scope']=keep
    return v[v.episode_first_by_scope].copy()

def pre_d0_context(g:pd.DataFrame,t:int,lookup:dict[int,int])->dict:
    """Only D0 or prior official-session bars for proposed support/ATR levels."""
    recent=[]
    for n in range(t-13,t+1):
        if n not in lookup:return {'support5':np.nan,'atr14':np.nan,'context_valid':False}
        v=g.iloc[lookup[n]]
        if min(float(v[c]) for c in ('open','high','low','close'))<=0:
            return {'support5':np.nan,'atr14':np.nan,'context_valid':False}
        recent.append(v)
    support=float(min(float(v.low) for v in recent[-5:]))
    tr=[]
    for n in range(t-13,t+1):
        v=g.iloc[lookup[n]]
        prev=g.iloc[lookup[n-1]] if n-1 in lookup else None
        if prev is None:return {'support5':np.nan,'atr14':np.nan,'context_valid':False}
        h,l,c=float(v.high),float(v.low),float(prev.close)
        tr.append(max(h-l,abs(h-c),abs(l-c)))
    return {'support5':support,'atr14':float(np.mean(tr)),'context_valid':True}

def stop_level(stop:str, anchor:float, pre:dict)->float:
    if stop=='STOP3':return anchor*.97
    if stop=='STOP5':return anchor*.95
    if stop=='STOP7':return anchor*.93
    if stop=='D0_SUPPORT5':return float(pre.get('support5',np.nan))
    if stop=='D0_ATR14_X2':return anchor-2*float(pre.get('atr14',np.nan))
    raise ValueError(stop)

def simul(bars:list[tuple],d0_high:float,pre:dict,entry:str,stop:str)->dict:
    """Conservative daily-bar thought experiment. bars[0] is D+1.

    Once first entry exists, all TP/stop levels use that first open anchor, not
    an ex-post low or moving average. Same-bar stop+TP uses stop. A dip-order
    touching TP on its fill bar conservatively defers TP to a subsequent bar.
    Cash reserve return includes uninvested capital at 0%.
    """
    assert entry in ENTRY_POLICIES and stop in STOP_POLICIES
    z={'entry_policy':entry,'stop_policy':stop,'status':'UNKNOWN','entry_first_day':np.nan,
       'exit_day':np.nan,'exit_reason':'UNKNOWN','invested_fraction':0.,'fills':0,
       'same_bar_stop_tp':0,'same_bar_add_tp':0,'profit_pct_budget_net05':np.nan,
       'profit_pct_invested_net05':np.nan,'entry_anchor':np.nan,'stop_distance_pct':np.nan}
    if len(bars)!=20:
        z.update(status='UNKNOWN_CENSORED',exit_reason='UNKNOWN_CENSORED');return z
    first=None;positions=[];slevel=np.nan;tlevel=np.nan
    def put(day,frac,price):
        nonlocal first,slevel,tlevel
        if first is None:
            first=price;slevel=stop_level(stop,price,pre);tlevel=1.05*price
            z['entry_first_day']=day;z['entry_anchor']=price
            z['stop_distance_pct']=(1-slevel/price)*100
        positions.append((float(frac),float(price)))
        z['fills']+=1
    fills=set();exit_px=None;exit_day=None;exit_reason='D20_CLOSE'
    for d,(o,h,l,c) in enumerate(bars,1):
        if not all(math.isfinite(x) and x>0 for x in (o,h,l,c)) or not l<=min(o,c)<=max(o,c)<=h:
            z.update(status='INVALID_DAILY_BAR',exit_reason='INVALID_DAILY_BAR');return z
        if d==1 and entry!='D2TO6_CONFIRM_D0HIGH':put(d,1. if entry=='D1_FULL' else .5 if entry=='D1_HALF_D2TO5_DIP3' else .4,o)
        if entry=='D2TO6_CONFIRM_D0HIGH' and first is None:
            # D+1..D+5 CLOSE breaks pre-known D0 high => ENTER NEXT DAY OPEN.
            # For d=2..6 look at bars[d-2] close, not current d close.
            if 2<=d<=6 and bars[d-2][3]>d0_high:put(d,1.,o)
            else:continue
        if first is None:continue
        if not math.isfinite(slevel) or slevel<=0 or slevel>=first:
            z.update(status='STOP_CONTEXT_INVALID',exit_reason='STOP_CONTEXT_INVALID');return z
        # On D1 entry open the open itself is not an old-position gap event.
        if d>int(z['entry_first_day']):
            if o<=slevel:exit_px=o;exit_day=d;exit_reason='STOP_GAP';break
            if o>=tlevel:exit_px=tlevel;exit_day=d;exit_reason='TARGET_GAP_LIMIT';break
        stop_hit=l<=slevel;tp_hit=h>=tlevel
        if stop_hit and tp_hit:z['same_bar_stop_tp']+=1
        if stop_hit:
            exit_px=slevel;exit_day=d;exit_reason='STOP_SAME_BAR_CONSERVATIVE' if tp_hit else 'STOP_TOUCH';break
        # staged limit orders active from D2; never D1 and never re-open after exits.
        added=False
        if d>=2:
            bids=[]
            if entry=='D1_HALF_D2TO5_DIP3' and d<=5:bids=[('dip3',.5,first*.97)]
            if entry=='D1_40_D2TO5_DIP2_D2TO8_DIP4':
                if d<=5:bids.append(('dip2',.3,first*.98))
                if d<=8:bids.append(('dip4',.3,first*.96))
            for tag,frac,limit in bids:
                if tag not in fills and l<=limit:
                    put(d,frac,min(o,limit));fills.add(tag);added=True
        if tp_hit:
            if added:
                z['same_bar_add_tp']+=1
            else:
                exit_px=tlevel;exit_day=d;exit_reason='TARGET_TOUCH';break
    if first is None:
        z.update(status='NO_CONFIRMATION_NO_ENTRY',exit_reason='NO_ENTRY',
                 invested_fraction=0.,profit_pct_budget_net05=0.,profit_pct_invested_net05=np.nan)
        return z
    if exit_px is None:exit_px=float(bars[-1][3]);exit_day=20
    invested=sum(w for w,p in positions)
    require(invested<=1.00000001,'OVER_FILLED_CAPITAL')
    gross=sum(w*(exit_px/p-1)*100 for w,p in positions)
    net=gross-FEE_PCT*invested
    z.update(status='SIMULATED_BARS_NOT_FILLS',exit_day=exit_day,exit_reason=exit_reason,
             invested_fraction=invested,profit_pct_budget_net05=net,
             profit_pct_invested_net05=net/invested)
    return z

def build(a:pd.DataFrame,hist:pd.DataFrame,official:list[str])->tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    a=a.copy();a['scope']=a.apply(scope,axis=1);a['type']=a.apply(classify,axis=1)
    a['volume_context']=np.select([
        a.vol20_to_prev20.ge(1.) & a.vol5_to_prev20.lt(1.),
        a.vol20_to_prev20.ge(1.) & a.vol5_to_prev20.ge(1.),
        a.vol20_to_prev20.lt(1.) & a.vol5_to_prev20.lt(1.),],
        ['ACCUM20_RECENT_DRY5','BOTH_RISING','BOTH_SUBDUED'],default='MIXED_OR_MISSING')
    pos={d:i for i,d in enumerate(official)}
    episodes=choose_episodes(a,official)
    groups={c:q.sort_values('sess').reset_index(drop=True) for c,q in hist.groupby('code',sort=False)}
    output=[];simrows=[]
    for row in episodes.itertuples(index=False):
        key=(row.code,row.signal_date); d=row._asdict()  # no post-D0 field used for episode selection
        g=groups.get(row.code)
        t=pos[row.signal_date]
        mark={'scope':row.scope,'type':row.type,'code':row.code,'signal_date':row.signal_date,
              'volume_context':row.volume_context,'selected_modes':row.selected_modes,
              'r112_stage_asof':row.stage,'r110_wave_label_asof':row.wave_stage_label,
              'r110_prevolume_ratio_asof':row.prewave_prevolume_accum_ratio,
              'r110_dryup_ratio_asof':row.volume_dryup_10_30,
              'vol20_to_prev20':row.vol20_to_prev20,'vol5_to_prev20':row.vol5_to_prev20,
              'retrace':row.pullback_fraction,'peak_age':row.peak_to_d0_sessions,
              'd20_complete':bool(row.d20_complete),
              'future_jump_suspect':bool(row.corp_action_future_suspect),
              'asof_quality_gate':bool(row.asof_quality_gate),
              't5_first_day_r119':row.t5_first_day,'t5_vs_s5_r119':row.t5_vs_s5_d20,
              'reference_only_not_entry_selection':True}
        if g is None:raise RuntimeError('MISSING_KRX_CODE '+row.code)
        look={int(s):i for i,s in enumerate(g.sess)}
        pre=pre_d0_context(g,t,look)
        mark.update(pre)
        if not b(row.d20_complete) or b(row.corp_action_future_suspect):
            mark['simulation_status']='D20_CENSORED' if not b(row.d20_complete) else 'POSTD0_CORP_SUSPECT_EXCLUDED'
            output.append(mark);continue
        bars=[]
        for offset in range(1,21):
            j=look.get(t+offset)
            if j is None:raise RuntimeError('R119_COMPLETE_BUT_KRX_BAR_MISSING')
            bar=g.iloc[j]
            bars.append(tuple(float(bar[k]) for k in ('open','high','low','close')))
        require(abs(float(bars[0][0])-float(row.entry_d1_open))<1e-8,'R119_D1_OPEN_AUTHORITY_MISMATCH')
        mark['simulation_status']='BAR_SCENARIO_ELIGIBLE'
        output.append(mark)
        for en in ENTRY_POLICIES:
            for st in STOP_POLICIES:
                z=simul(bars,float(row.high),pre,en,st)
                simrows.append({**{k:mark[k] for k in ('scope','type','code','signal_date','volume_context','r112_stage_asof')},**z})
    ep=pd.DataFrame(output);simd=pd.DataFrame(simrows)
    require(len(ep)==len(episodes),'EPISODE_COUNT_DRIFT')
    require(not ep.duplicated(['code','signal_date','scope']).any(),'EPISODE_NOT_UNIQUE')
    require(len(simd)==int(ep.simulation_status.eq('BAR_SCENARIO_ELIGIBLE').sum())*len(ENTRY_POLICIES)*len(STOP_POLICIES),'SCENARIO_GRID_INCOMPLETE')
    return a,ep,simd

def summarize(ep:pd.DataFrame,sims:pd.DataFrame)->pd.DataFrame:
    rows=[]
    for keys,g in sims.groupby(['scope','type','entry_policy','stop_policy'],dropna=False,sort=True):
        eligible=g[g.status.eq('SIMULATED_BARS_NOT_FILLS')]
        p=eligible.profit_pct_budget_net05
        rows.append(dict(zip(['scope','type','entry_policy','stop_policy'],keys),
             complete_source_episodes=int(len(g)),entered_n=int(len(eligible)),no_entry_n=int(g.status.eq('NO_CONFIRMATION_NO_ENTRY').sum()),
             scenario_invalid_n=int(len(g)-len(eligible)-int(g.status.eq('NO_CONFIRMATION_NO_ENTRY').sum())),
             mean_net_pct_reserved_capital=float(g.profit_pct_budget_net05.mean()),
             win_pct_entered=float(p.gt(0).mean()*100) if len(eligible) else np.nan,
             mean_net_pct_entered=float(p.mean()) if len(eligible) else np.nan,
             median_net_pct_entered=float(p.median()) if len(eligible) else np.nan,
             tp_exit_pct_entered=float(eligible.exit_reason.str.startswith('TARGET').mean()*100) if len(eligible) else np.nan,
             stop_exit_pct_entered=float(eligible.exit_reason.str.startswith('STOP').mean()*100) if len(eligible) else np.nan,
             mean_exposure_pct=float(g.invested_fraction.mean()*100),
             mean_entry_day=float(eligible.entry_first_day.mean()) if len(eligible) else np.nan,
             mean_exit_day=float(eligible.exit_day.mean()) if len(eligible) else np.nan,
             intrabar_stop_tp_unknown_n=int(g.same_bar_stop_tp.sum()),
             intrabar_add_tp_unknown_n=int(g.same_bar_add_tp.sum()),
             mean_stop_distance_pct=float(eligible.stop_distance_pct.mean()) if len(eligible) else np.nan,
             p90_stop_distance_pct=float(eligible.stop_distance_pct.quantile(.9)) if len(eligible) else np.nan,
             median_loss_net_pct=float(p[p.lt(0)].median()) if (p.lt(0)).any() else np.nan,
             stop_by_d3_n=int((eligible.exit_reason.str.startswith('STOP') & eligible.exit_day.le(3)).sum()),
             source_scope='COVERED_907_CODE_SUBSET_EPISODES_NOT_FULL_PIT_MARKET',
             valid_comparison='DESCRIPTIVE_NON_CAUSAL_UNVALIDATED_FILLS'))
    return pd.DataFrame(rows)

def paired_selected_entry_delta(sims:pd.DataFrame)->pd.DataFrame:
    """Same historical signals, same stop; differences are descriptive, not optimized OOS."""
    x=sims[sims.scope.eq('V4976_SELECTED')].copy()
    base=x[x.entry_policy.eq('D1_FULL')][['code','signal_date','type','stop_policy','profit_pct_budget_net05']].rename(
        columns={'profit_pct_budget_net05':'baseline_net_pct'})
    y=x.merge(base,on=['code','signal_date','type','stop_policy'],how='left',validate='many_to_one')
    y['paired_delta_pp']=y.profit_pct_budget_net05-y.baseline_net_pct
    out=[]
    for (typ,entry,stop),g in y.groupby(['type','entry_policy','stop_policy']):
        out.append({'type':typ,'entry_policy':entry,'stop_policy':stop,'matched_episodes':int(g.paired_delta_pp.notna().sum()),
                    'invalid_scenario_excluded_n':int(g.paired_delta_pp.isna().sum()),
                    'unique_tickers':g.loc[g.paired_delta_pp.notna(),'code'].nunique(),'mean_delta_pp_vs_d1_full':g.paired_delta_pp.mean(),
                    'median_delta_pp_vs_d1_full':g.paired_delta_pp.median(),
                    'improved_frac_pct':g.paired_delta_pp.gt(0).mean()*100,
                    'worse_frac_pct':g.paired_delta_pp.lt(0).mean()*100,
                    'same_frac_pct':g.paired_delta_pp.eq(0).mean()*100,
                    'no_trade_frac_pct':g.status.eq('NO_CONFIRMATION_NO_ENTRY').mean()*100,
                    'baseline_mean_pct':g.baseline_net_pct.mean(),
                    'scenario_mean_pct':g.profit_pct_budget_net05.mean(),
                    'authority':'PAIRED_DESCRIPTIVE_NO_MULTITEST_OOS_NO_TRADE_APPROVAL'})
    return pd.DataFrame(out)


def stop_timing(sims:pd.DataFrame)->pd.DataFrame:
    x=sims[sims.entry_policy.eq('D1_FULL')].copy()
    s=x[x.exit_reason.str.startswith('STOP')]
    out=[]
    for (scope,typ,stop),g in x.groupby(['scope','type','stop_policy']):
        g=g[g.status.eq('SIMULATED_BARS_NOT_FILLS')]
        hit=s[(s.scope==scope)&(s.type==typ)&(s.stop_policy==stop)]
        out.append({'scope':scope,'type':typ,'stop_policy':stop,'evaluated_n':len(g),
                    'stop_n':len(hit),'stop_d1_to_d3_n':int(hit.exit_day.le(3).sum()),
                    'stop_d4_to_d5_n':int(hit.exit_day.between(4,5).sum()),
                    'stop_d6_to_d10_n':int(hit.exit_day.between(6,10).sum()),
                    'stop_d11_to_d20_n':int(hit.exit_day.between(11,20).sum()),
                    'median_stop_day':float(hit.exit_day.median()) if len(hit) else np.nan,
                    'stop_distance_mean_pct':float(g.stop_distance_pct.mean()),
                    'stop_gap_n':int(hit.exit_reason.eq('STOP_GAP').sum()),
                    'same_day_stop_tp_unknown_n':int(g.same_bar_stop_tp.sum()),
                    'authority':'DAILY_BAR_STOP_TOUCH_TIMING_NOT_EXECUTION'})
    return pd.DataFrame(out)


def save(args)->dict:
    co,inp=load_sources(args.r119_root,args.r112_root,args.r110_root)
    # Reuse SHA-proven read of original 1,907 KRX official days.
    from closebet_research.r118_partial_market_wave_geometry import read_immutable_krx
    hist,official,_=read_immutable_krx(args.history_dir,lowest_date='2025-01-01')
    require(len(official)==1907,'KRX_CALENDAR_NOT_PINNED')
    _,ep,sims=build(co,hist,official)
    summary=summarize(ep,sims)
    paired=paired_selected_entry_delta(sims)
    timing=stop_timing(sims)
    first=ep.groupby(['scope','type'],dropna=False).agg(
         first_episode_n=('code','size'),fully_observed_n=('d20_complete','sum'),
         modeled_n=('simulation_status',lambda x:int((x=='BAR_SCENARIO_ELIGIBLE').sum())),
         r110_feature_join_n=('r110_wave_label_asof','count'),
         r112_stage_join_n=('r112_stage_asof','count')).reset_index()
    out=Path(args.output_dir);out.mkdir(parents=True,exist_ok=True)
    docs={'r120_episodes_asof_and_evidence.csv':ep,
          'r120_episode_type_and_source_coverage.csv':first,
          'r120_entry_scale_stop_scenario_ledger.csv':sims,
          'r120_type_x_entry_x_stop_aggregate.csv':summary,
          'r120_selected_paired_entry_delta_vs_d1.csv':paired,
          'r120_stop_timing_by_type.csv':timing}
    hashes={}
    for name,frame in docs.items():
        f=out/name;frame.to_csv(f,index=False,encoding='utf-8-sig');hashes[name]=sha(f)
    manifest={'version':VERSION,'status':STATUS,'research_only':True,'trade_authority':False,
       'live_search_changed':False,'shadow_board_changed':False,'order_logic_changed':False,
       'true_full_market_PIT_available':False,'data_selection':'R171_KRX_EXPOST_COVERED_907_CODE_SUBSET',
       'source_run_r119':38865542460,'source_sha':inp,'krx_sha_verified_days':1907,
       'covered_code_days':40305,'selected_code_days':1059,'r110_join_rows':inp['r110_asof_join_n'],
       'r112_join_rows':inp['r112_asof_join_n'],
       'eligible_scope_episode_n':len(ep),'d20_scenario_eligible_n':int(ep.simulation_status.eq('BAR_SCENARIO_ELIGIBLE').sum()),
       'd20_censored_episode_n':int(ep.simulation_status.eq('D20_CENSORED').sum()),
       'scenario_rows':len(sims),'entries':list(ENTRY_POLICIES),'stops':list(STOP_POLICIES),
       'file_sha256':hashes,
       'warnings':['All entry/stop/exit prices are hypothetical daily OHLC thresholds, not executed orders.',
         'Same-day TP/stop is conservative stop-first, but bar order remains unknown.',
         'Same-day add/TP is conservative TP deferred; limit fill does not equal real fill.',
         'R110/R112 as-of features are joined only to historical selected signals; no future-label leakage.',
         'Non-random group selection; 20-session episode embargo separately by scope and ticker.',
         'Post-D0 jump quarantine used only as outcome-quality sensitivity, never in episode selection.',
         'No parameter selection, forward OOS, official full-market PIT eligibility, or trade approval.']}
    (out/'r120_integrity_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R120_RESEARCH_PASS',json.dumps({k:manifest[k] for k in ('status','eligible_scope_episode_n','d20_scenario_eligible_n','scenario_rows','r110_join_rows','r112_join_rows')},ensure_ascii=False))
    return manifest

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--r119-root',required=True,type=Path)
    ap.add_argument('--r112-root',required=True,type=Path)
    ap.add_argument('--r110-root',required=True,type=Path)
    ap.add_argument('--history-dir',required=True,type=Path)
    ap.add_argument('--output-dir',required=True,type=Path)
    save(ap.parse_args())

if __name__=='__main__':main()
