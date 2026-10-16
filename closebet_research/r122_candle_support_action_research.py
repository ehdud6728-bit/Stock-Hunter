#!/usr/bin/env python3
"""R1.22 immutable-source candle/volume/MA/support-conditioned decision research.

RESEARCH_ONLY, no LIVE changes. PIT-as-of *features*, not full-market PIT membership.
Fixed hypothetical daily OHLC fills and D+20 outcomes are NOT broker execution.
Research hypotheses are fixed in source BEFORE any future data are accessed.
"""
from __future__ import annotations
import argparse, hashlib, json, math
from pathlib import Path
import numpy as np
import pandas as pd
from closebet_research.r118_partial_market_wave_geometry import read_immutable_krx

REVISION='R122_CANDLE_VOLUME_MA_SUPPORT_ACTION_DESCRIPTIVE_V1'
SOURCE_RUN=38871858855
SOURCE_ARTIFACT='closebet-r120-entry-scale-stop-38871858855'
PINNED_R120_MANIFEST_SHA='c8723c98048dbb6a034a10f54c248b4a8d2a0e4b5e76c01f45052c3863b748ba'
SOURCE_FILE='r120_episodes_asof_and_evidence.csv'
FEE_PCT=0.50
ENTRIES=('E0_D1_FULL','E1_SUPPORT_RECLAIM_NEXT_OPEN','E2_BREAK_D0HIGH_NEXT_OPEN','E3_CONTEXT_GATED_40_30_30')
STOPS=('S0_FIXED5','S1_STATIC_SUPPORT_CAPPED8','S2_SUPPORT_CLOSE_NEXT_OPEN')
# Fixed categorical anatomy: 0.5 body/range, 0.35 wick/range; no outcome-based optimization.
# Price supports fixed from information known on D0: D-1 candle low, most recent
# qualifying bullish candle body-low over D-1..D-10, and D-4..D0 low.
MAX_STRUCTURAL_RISK_PCT=8.
MIN_STRUCTURAL_RISK_PCT=.5
EXPECTED_EPISODES=4567
EXPECTED_ELIGIBLE=3160


def require(cond,msg):
    if not cond:raise RuntimeError(msg)


def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def truth(v): return v is True or isinstance(v,(bool,np.bool_)) and bool(v) or str(v).lower() in ('true','1')

def checked_source(root):
    root=Path(root); require(sha(root/'r120_integrity_manifest.json')==PINNED_R120_MANIFEST_SHA,'PINNED_R120_MANIFEST_SHA_MISMATCH')
    m=json.loads((root/'r120_integrity_manifest.json').read_text(encoding='utf-8'))
    require(m.get('version')=='R120_ENTRY_SCALE_STOP_ASOF_EPISODE_V1','R120_VERSION_DRIFT')
    require(m.get('status')=='PASS_EPISODE_ENTRY_SCALE_STOP_DESCRIPTIVE_NOT_OOS_NOT_TRADE_APPROVED','R120_STATUS_DRIFT')
    require(m.get('eligible_scope_episode_n')==EXPECTED_EPISODES and m.get('d20_scenario_eligible_n')==EXPECTED_ELIGIBLE,'R120_POPULATION_DRIFT')
    require(m.get('krx_sha_verified_days')==1907 and not m.get('true_full_market_PIT_available'),'R120_SCOPE_BAD')
    require(m.get('trade_authority') is False,'R120_TRADE_AUTHORITY_UNEXPECTED')
    for k,v in m['file_sha256'].items():
        require((root/k).is_file() and sha(root/k)==v,'R120_MEMBER_SHA_BAD:'+k)
    a=pd.read_csv(root/SOURCE_FILE,dtype={'code':str},low_memory=False)
    a.code=a.code.str.zfill(6)
    require(len(a)==EXPECTED_EPISODES and not a.duplicated(['code','signal_date','scope']).any(),'R120_EPISODE_SCOPE_DRIFT')
    require(a.code.str.fullmatch(r'\d{6}').all(),'CODE_LEADING_ZEROS_INVALID')
    require(int(a.simulation_status.eq('BAR_SCENARIO_ELIGIBLE').sum())==EXPECTED_ELIGIBLE,'R120_MATURE_DRIFT')
    return a,m


def featurize(g,t,lookup):
    """D0 or earlier bars only. Return precommitted *potential* support levels.

    Any missing official session in 224d historic window invalidates features.
    We never choose a support by its future ability to hold.
    """
    if any(s not in lookup for s in range(t-223,t+1)):
        return {'feature_valid':False,'missing_reason':'PRIOR224_GAP'}
    x=g.iloc[[lookup[s] for s in range(t-223,t+1)]]
    a=x.iloc[-1];prev=x.iloc[-2]; last5=x.iloc[-5:];last20=x.iloc[-20:]
    if (x[['open','high','low','close']].to_numpy()<=0).any() or (x['volume']<0).any():
        return {'feature_valid':False,'missing_reason':'BAD_OHLCV'}
    if bool((x['high']<x[['open','close','low']].max(axis=1)).any()) or bool((x['low']>x[['open','close','high']].min(axis=1)).any()):
        return {'feature_valid':False,'missing_reason':'BAD_BAR_RANGE'}
    c=float(a.close);o=float(a.open);h=float(a.high);l=float(a.low)
    r=max(h-l,1e-10);body=abs(c-o)
    ma=lambda k:float(x.close.iloc[-k:].mean())
    slope20=(ma(20)/float(x.close.iloc[-25:-5].mean())-1)*100
    vma20=float(x.volume.iloc[-21:-1].mean())  # strictly pre-D0
    vma5=float(x.volume.iloc[-6:-1].mean())
    # Find latest qualified bullish candle from past 10 sessions, using only its
    # own history and pre-D0 information. Preceding 20-volume-bar mean is available.
    bull=None
    for j in range(len(x)-2,len(x)-12,-1):
        b=x.iloc[j]
        br=max(float(b.high-b.low),1e-10)
        vv=float(x.volume.iloc[j-20:j].mean())
        if float(b.close)>float(b.open) and (float(b.close-b.open)/br)>=.5 and vv>0 and float(b.volume)>=vv:
            bull=b;break
    impulse_low=float(bull.low) if bull is not None else np.nan
    impulse_body_low=float(bull.open) if bull is not None else np.nan
    # Prefer a previously known bullish body support, if below D0 close;
    # otherwise the 5-day low as a fallback. Do not peek at future reversals.
    support=impulse_body_low if np.isfinite(impulse_body_low) and impulse_body_low<c else float(last5.low.min())
    risk=(1-support/c)*100
    relation=('CLOSE_BELOW_PREV_LOW' if c<float(prev.low) else
              'RECLAIM_PREV_LOW' if l<float(prev.low) and c>=float(prev.low) else
              'HOLD_PREV_LOW')
    candle=('BULL_STRONG' if c>o and body/r>=.5 else 'BEAR_STRONG' if c<o and body/r>=.5 else
            'BULL_SMALL' if c>o and body/r>=.1 else 'BEAR_SMALL' if c<o and body/r>=.1 else 'DOJI')
    location=('ABOVE_IMPULSE_BODY' if np.isfinite(impulse_body_low) and c>=impulse_body_low else
              'BELOW_IMPULSE_BODY' if np.isfinite(impulse_body_low) else 'NO_IMPULSE_REFERENCE')
    return {
      'feature_valid':True,'missing_reason':'',
      'd0_open':o,'d0_high':h,'d0_low':l,'d0_close':c,'d0_volume':float(a.volume),
      'prev_low':float(prev.low),'prev_high':float(prev.high),'prev_body_low':float(min(prev.open,prev.close)),
      'bull_reference_low':impulse_low,'bull_reference_body_low':impulse_body_low,
      'support_level':support,'support_risk_from_d0_pct':risk,
      'prev_low_relation':relation,'impulse_body_relation':location,'candle_type':candle,
      'body_fraction':body/r,'close_location':(c-l)/r,'upper_wick_fraction':(h-max(c,o))/r,
      'lower_wick_fraction':(min(c,o)-l)/r,'has_upper_wick':(h-max(c,o))/r>=.35,
      'has_lower_wick':(min(c,o)-l)/r>=.35,
      'd0_volume_prev20_ratio':float(a.volume)/vma20 if vma20>0 else np.nan,
      'd0_volume_prev5_ratio':float(a.volume)/vma5 if vma5>0 else np.nan,
      'ma5':ma(5),'ma20':ma(20),'ma60':ma(60),'ma224':ma(224),
      'close_vs_ma5_pct':(c/ma(5)-1)*100,'close_vs_ma20_pct':(c/ma(20)-1)*100,
      'close_vs_ma60_pct':(c/ma(60)-1)*100,'close_vs_ma224_pct':(c/ma(224)-1)*100,
      'ma20_slope20_pct':slope20,
      'ma_band':('ABOVE_20_60' if c>=ma(20) and c>=ma(60) else
                 'BELOW_20_60' if c<ma(20) and c<ma(60) else 'MIXED_20_60'),
      'recent5_low':float(last5.low.min())
    }


def setup_tags(row):
    """Fixed as-of tags for *descriptive* comparisons, not learned entry gates."""
    if not truth(row.get('feature_valid')):return 'INVALID'
    # Important: D0 close and actual structure are both read from D0 or earlier.
    if row['prev_low_relation']=='CLOSE_BELOW_PREV_LOW':return 'BROKEN_PREVIOUS_LOW'
    if row['prev_low_relation']=='RECLAIM_PREV_LOW' and row['lower_wick_fraction']>=.20:return 'PREV_LOW_RECLAIM'
    if row['ma_band']=='ABOVE_20_60' and row['candle_type'].startswith('BULL'):return 'MA_ABOVE_BULLISH'
    if row['candle_type']=='BEAR_STRONG' and row['d0_volume_prev20_ratio']>=1.5:return 'HIGH_VOL_SELL_OFF'
    return 'OTHER_ASOF'


def daily_bars(g,t,lookup):
    if any(t+i not in lookup for i in range(1,21)):return None
    return [tuple(float(g.iloc[lookup[t+i]][col]) for col in ('open','high','low','close','volume')) for i in range(1,21)]


def simulate(bars,ctx,entry,stop):
    """OHLC paths: next-open after EOD confirmations, conservative same-bar ambiguity.

    Static price stops execute at stop or worse next gap-open. Close-based stops
    trigger exit NEXT open (no same-bar lookahead). Full budget denominator incl cash.
    +5% target uses initial filled price anchor (independent of later adds).
    """
    require(entry in ENTRIES and stop in STOPS,'UNKNOWN_POLICY')
    result={'entry':entry,'stop':stop,'status':'UNKNOWN','entry_day':np.nan,'exit_day':np.nan,
            'exit_reason':'UNKNOWN','invested_fraction':0.,'n_fills':0,
            'stop_first3':False,'both_on_same_day':False,'budget_net_pct':np.nan,
            'invested_net_pct':np.nan,'min_low_pct_from_entry':np.nan,'max_high_pct_from_entry':np.nan}
    if bars is None or len(bars)!=20:
        result.update(status='CENSORED',exit_reason='CENSORED');return result
    s=float(ctx.get('support_level',np.nan));d0_h=float(ctx['d0_high']);d0_c=float(ctx['d0_close'])
    prev_l=float(ctx['prev_low'])
    guard = math.isfinite(s) and s>0 and d0_c>=s and d0_c>=prev_l and not (
        ctx['candle_type']=='BEAR_STRONG' and float(ctx.get('d0_volume_prev20_ratio',0))>=1.5)
    if entry=='E3_CONTEXT_GATED_40_30_30' and not guard:
        result.update(status='NO_ENTRY_CONTEXT_GUARD',exit_reason='NO_ENTRY',budget_net_pct=0.);return result
    # Support-dependent stops require entry price and pre-D0 support to give a
    # strictly positive and bounded risk budget. Risk caps cause SKIP, not adjustment.
    support_based = stop!='S0_FIXED5'
    pos=[];first_px=None;stop_px=None;target_px=None;pending_exit=False
    queued=[];pending_initial=False;tested=False;reclaim_placed=False;break_placed=False
    maxh=-math.inf;minl=math.inf;both=False;support_breached_close=False
    exit_price=None;why=None;exit_day=None
    for day,b in enumerate(bars,1):
        o,h,l,c,v=b
        require(all(math.isfinite(z) and z>0 for z in (o,h,l,c)) and l<=min(o,c)<=max(o,c)<=h,'INVALID_FUTURE_BAR')
        if pending_exit and first_px is not None:
            exit_price=o;exit_day=day;why='SUPPORT_CLOSE_NEXT_OPEN';break
        # Existing overnight positions always take priority over a queued add.
        # A gap beyond the pre-existing stop/target closes the position at open
        # (fixed stop) or pre-existing target limit BEFORE any new buy fills.
        if first_px is not None:
            if stop!='S2_SUPPORT_CLOSE_NEXT_OPEN' and o<=stop_px:
                exit_price=o;exit_day=day;why='STOP_GAP';break
            if o>=target_px:
                exit_price=target_px;exit_day=day;why='TARGET_GAP_LIMIT';break
        # At this open, process yesterday's confirmed EOD actions only.
        add=queued;queued=[]
        if day==1 and entry in ('E0_D1_FULL','E3_CONTEXT_GATED_40_30_30'):
            add=[1. if entry=='E0_D1_FULL' else .4]
        if pending_initial:add=[1.];pending_initial=False
        if add:
            if first_px is None:
                first_px=o;stop_px=first_px*.95 if stop=='S0_FIXED5' else s;target_px=first_px*1.05
                risk=(1-stop_px/first_px)*100
                if stop!='S0_FIXED5' and (not math.isfinite(risk) or risk<MIN_STRUCTURAL_RISK_PCT or risk>MAX_STRUCTURAL_RISK_PCT):
                    result.update(status='NO_ENTRY_INVALID_STRUCTURAL_RISK',exit_reason='NO_ENTRY',budget_net_pct=0.);return result
                result['entry_day']=day
            for fraction in add:pos.append((fraction,o))
        if first_px is not None:
            maxh=max(maxh,h);minl=min(minl,l)
            if stop!='S2_SUPPORT_CLOSE_NEXT_OPEN':
                # No old-position gap on first entry day; low after open still checks.
                if day>result['entry_day'] and o<=stop_px:
                    exit_price=o;exit_day=day;why='STOP_GAP';break
                sh=l<=stop_px
            else:sh=False
            ph=h>=target_px
            if sh and ph:both=True
            if sh:
                exit_price=stop_px;exit_day=day;why='STOP_SAME_BAR_CONSERVATIVE' if ph else 'STOP_INTRADAY';break
            # Same bar that opens with a queued add may ALSO touch target;
            # no ambiguity about chronology after open; target limit achievable
            # only under idealized continuous intraday OHLC and not real fills.
            if ph:
                exit_price=target_px;exit_day=day;why='TARGET_INTRADAY';break
            if c<s:
                support_breached_close=True
                if stop=='S2_SUPPORT_CLOSE_NEXT_OPEN':pending_exit=True
        # Entry decisions from CLOSE only -> next session OPEN.
        if first_px is None:
            if day<=5 and day<20:
                if entry=='E1_SUPPORT_RECLAIM_NEXT_OPEN' and l<=s and c>=s and c>o:
                    pending_initial=True
                elif entry=='E2_BREAK_D0HIGH_NEXT_OPEN' and c>d0_h:
                    pending_initial=True
        elif entry=='E3_CONTEXT_GATED_40_30_30' and day<=6 and not pending_exit and not support_breached_close:
            if len(pos)<3 and day<20:
                # One add per EOD condition, no adds below support, no averaging down
                # into a closing breach. The 30% cap is independent per add.
                if len(pos)==1 and not reclaim_placed and l<=prev_l and c>=prev_l and c>=s and c>o:
                    queued=[.3];reclaim_placed=True
                elif len(pos)==2 and not break_placed and c>max(d0_h,float(bars[day-2][1]) if day>1 else d0_h) and c>=s:
                    queued=[.3];break_placed=True
    if first_px is None:
        result.update(status='NO_SIGNAL_NO_ENTRY',exit_reason='NO_ENTRY',budget_net_pct=0.,invested_fraction=0.)
        return result
    if exit_price is None:
        exit_price=float(bars[-1][3]);exit_day=20;why='D20_CLOSE' if not pending_exit else 'D20_CLOSE_PENDING_SUPPORT'
    invested=sum(w for w,p in pos)
    require(invested<=1+1e-9,'OVER_INVESTMENT')
    pnl=sum(w*(exit_price/p-1)*100 for w,p in pos)-FEE_PCT*invested
    result.update(status='SIMULATED_OHLC_NOT_EXECUTIONS',exit_day=exit_day,exit_reason=why,
                  invested_fraction=invested,n_fills=len(pos),budget_net_pct=pnl,
                  invested_net_pct=pnl/invested if invested>0 else np.nan,
                  stop_first3=bool(why.startswith('STOP') or why=='SUPPORT_CLOSE_NEXT_OPEN') and exit_day-result['entry_day']<=2,
                  both_on_same_day=both,min_low_pct_from_entry=(minl/first_px-1)*100,
                  max_high_pct_from_entry=(maxh/first_px-1)*100)
    return result


def calc(a,hist,official):
    pos={d:i for i,d in enumerate(official)}
    hist=hist.sort_values(['code','sess']);groups={c:g.reset_index(drop=True) for c,g in hist.groupby('code',sort=False)}
    lookups={c:{int(v):i for i,v in enumerate(g.sess)} for c,g in groups.items()}
    features=[];simrows=[]
    for j,row in enumerate(a.itertuples(index=False)):
        d=row._asdict();k=d['code'];date=str(d['signal_date']);t=pos[date]
        g=groups.get(k)
        require(g is not None and t in lookups[k],'R120_EVENT_KRX_D0_MISSING:'+k+date)
        ctx=featurize(g,t,lookups[k]);ctx.update({'code':k,'signal_date':date,'scope':d['scope'],'type':d['type'],
             'volume_context':d['volume_context'],'r112_stage_asof':d['r112_stage_asof'],
             'r120_simulation_status':d['simulation_status'],'d20_complete':truth(d['d20_complete'])})
        ctx['setup_tag']=setup_tags(ctx)
        features.append(ctx)
        if d['simulation_status']!='BAR_SCENARIO_ELIGIBLE' or not truth(ctx.get('feature_valid')):
            continue
        bars=daily_bars(g,t,lookups[k])
        require(bars is not None,'R120_MATURE_BUT_KRX_FUTURE_MISSING')
        for entry in ENTRIES:
            for stop in STOPS:
                q=simulate(bars,ctx,entry,stop)
                simrows.append({c:ctx[c] for c in ['code','signal_date','scope','type','volume_context','setup_tag','ma_band','prev_low_relation','impulse_body_relation','candle_type']}|q)
    out=pd.DataFrame(features);sim=pd.DataFrame(simrows)
    require(len(out)==EXPECTED_EPISODES and not out.duplicated(['scope','code','signal_date']).any(),'FEATURE_COUNT_DRIFT')
    require(len(sim)==int((out.r120_simulation_status.eq('BAR_SCENARIO_ELIGIBLE')&out.feature_valid.astype(bool)).sum())*len(ENTRIES)*len(STOPS),'SIMULATION_GRID_INCOMPLETE')
    return out,sim


def aggregate(feat,sim):
    results=[]
    for cols in [['scope'],['type'],['volume_context'],['setup_tag'],['prev_low_relation'],['candle_type'],['ma_band'],
                 ['scope','setup_tag'],['type','setup_tag'],['scope','volume_context']]:
        for key,subset in sim.groupby(cols,dropna=False):
            keys=key if isinstance(key,tuple) else (key,);basis=dict(zip(cols,keys))
            for (en,st),g in subset.groupby(['entry','stop']):
                sample=g[g.status.isin(['SIMULATED_OHLC_NOT_EXECUTIONS','NO_SIGNAL_NO_ENTRY','NO_ENTRY_CONTEXT_GUARD','NO_ENTRY_INVALID_STRUCTURAL_RISK'])]
                traded=sample[sample.status.eq('SIMULATED_OHLC_NOT_EXECUTIONS')]
                if not len(sample):continue
                results.append({**{k:('' if pd.isna(v) else v) for k,v in basis.items()},'dimensions':'+'.join(cols),
                    'entry':en,'stop':st,'episodes':len(sample),'entered':len(traded),'entry_rate_pct':100*len(traded)/len(sample),
                    'budget_mean_net_pct':float(sample.budget_net_pct.fillna(0).mean()),
                    'invested_mean_net_pct':float(traded.invested_net_pct.mean()) if len(traded) else np.nan,
                    'target_first_pct_of_episodes':100*float(traded.exit_reason.eq('TARGET_INTRADAY').sum())/len(sample),
                    'early_stop_pct_of_entered':100*float(traded.stop_first3.mean()) if len(traded) else np.nan,
                    'stop_or_close_failure_count':int(traded.exit_reason.str.startswith('STOP').sum()+traded.exit_reason.eq('SUPPORT_CLOSE_NEXT_OPEN').sum()),
                    'mean_invested_fraction_all':float(sample.invested_fraction.mean()),
                    'gap_stop_count':int(traded.exit_reason.eq('STOP_GAP').sum())})
    return pd.DataFrame(results)


def paired(sim):
    base=sim[(sim.entry=='E0_D1_FULL') & (sim.stop=='S0_FIXED5')][['scope','code','signal_date','budget_net_pct','status']].rename(columns={'budget_net_pct':'baseline_net','status':'baseline_status'})
    a=sim.merge(base,on=['scope','code','signal_date'],how='left',validate='many_to_one')
    a['delta_vs_base_budget_pct']=a.budget_net_pct.fillna(0)-a.baseline_net.fillna(0)
    a['entered']=a.status.eq('SIMULATED_OHLC_NOT_EXECUTIONS')
    return a


def run(r120,archive_dir,outdir):
    a,m=checked_source(Path(r120))
    full,official,seen=read_immutable_krx(Path(archive_dir),lowest_date='2025-01-01')
    require(len(seen)==1907 and len(official)==1907 and len(set(official))==1907,'KRX_PROVENANCE_DRIFT')
    require(max(a.signal_date)<=max(official) and min(a.signal_date)>=min(official),'DATES_OUTSIDE_KRX_CALENDAR')
    feat,sim=calc(a,full,official)
    ag=aggregate(feat,sim);pr=paired(sim)
    # Untuned split (from R121) is diagnostic only; overwhelmingly shared tickers.
    dates=sorted(a.signal_date.unique())
    require(len(dates)==45 and dates[0]=='2026-08-03','DATE_COHORT_LOCK_BAD')
    windows={d:('EARLY_FIRST_DATE' if i==0 else 'EMBARGO_20_SESSION' if 1<=i<=20 else 'LATE_5_DATES' if 21<=i<=25 else 'RIGHT_CENSORED') for i,d in enumerate(dates)}
    pr['date_window']=pr.signal_date.map(windows)
    trend=(pr.groupby(['date_window','scope','setup_tag','entry','stop'],dropna=False)
             .agg(episodes=('code','size'),entered=('entered','sum'),mean_delta_budget_pct=('delta_vs_base_budget_pct','mean'))
             .reset_index())
    out=Path(outdir);out.mkdir(parents=True,exist_ok=True)
    objs={'r122_d0_candle_ma_support_features.csv':feat,
          'r122_entry_scale_structure_stop_ledger.csv':sim,
          'r122_structure_conditioned_policy_descriptive.csv':ag,
          'r122_paired_delta_vs_d1_fixed5.csv':pr,
          'r122_early_late_scope_diagnostics.csv':trend}
    checks={}
    for fn,obj in objs.items():
        obj.to_csv(out/fn,index=False,float_format='%.9g');checks[fn]=sha(out/fn)
    meta={'revision':REVISION,'status':'PASS_SOURCE_LIMITED_ASOF_CANDLE_SUPPORT_DESCRIPTIVE_NOT_OOS_NOT_TRADE_APPROVED',
          'source_r120_run':SOURCE_RUN,'r120_episodes':len(a),'full_d20_episodes_in_r120':EXPECTED_ELIGIBLE,
          'feature_rows':len(feat),'feature_valid_rows':int(feat.feature_valid.sum()),
          'scenario_events':int(len(sim)//(len(ENTRIES)*len(STOPS))), 'scenario_rows':len(sim),
          'entries':list(ENTRIES),'stops':list(STOPS),'krx_day_sha_verified':len(seen),
          'r120_manifest_sha256':sha(Path(r120)/'r120_integrity_manifest.json'),
          'r120_episode_member_sha256':sha(Path(r120)/SOURCE_FILE),
          'limitations':['907-name EX-POST covered source, not historical full-market PIT','No real fill data or auction','D+20 right-censoring','Daily high/low order unknown; conservatively stop first','Same source as prior studies; no independent OOS','Multiple days / tickers may repeat across evaluation groups','EOD-confirm policy uses following open and may gap','Static candle/support heuristics are not optimized/approved'],
          'full_market_PIT_recall_valid':False,'research_only':True,'trade_authority':False,'live_changed':False,'shadow_changed':False,'auto_exit_changed':False,
          'sha256':checks}
    (out/'r122_integrity_manifest.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R122_RESEARCH_PASS',json.dumps({k:meta[k] for k in ['status','feature_rows','feature_valid_rows','scenario_events','scenario_rows','krx_day_sha_verified']},ensure_ascii=False))
    return meta


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--r120-root',required=True);ap.add_argument('--history-dir',required=True);ap.add_argument('--output-dir',required=True)
    q=ap.parse_args();run(q.r120_root,q.history_dir,q.output_dir)

if __name__=='__main__':main()
