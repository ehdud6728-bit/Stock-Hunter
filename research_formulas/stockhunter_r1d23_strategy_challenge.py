#!/usr/bin/env python3
"""StockHunter R1-D23: full-market research-only alternative-thesis challenge.

A below-MA224 wave/pullback; B reclaim/hold MA224; C rising above-MA224
wave/pullback; D high-strength consolidation/breakout. New full-market, asof-only
operational proxies; NOT native 66 formulas. Original R1D6/R1D9 population is a
separate audit/control, not an input universe. Historical hindsight and NO OOS.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from collections import Counter, defaultdict, deque
from pathlib import Path
import numpy as np
import pandas as pd

VERSION = 'STOCKHUNTER_R1D23_FULL_MARKET_FOUR_THESIS_A_B_C_D_V1'
PERIODS = {'EARLY_2021_2023':('2021-01-04','2023-11-29'),
           'LATE_2024_2025':('2024-01-02','2025-09-30')}
STRATEGIES = ('A_BELOW224_WAVE_PULLBACK','B_MA224_RECLAIM_SUPPORT',
              'C_ABOVE224_RISING_WAVE_PULLBACK','D_LEADER_CONSOLIDATION_BREAKOUT')
HORIZONS=(5,10,15,20)
DONE='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'

def require(cond, msg):
    if not cond:raise RuntimeError('R1D23_FAIL_CLOSED_'+str(msg))

def sha(data):return hashlib.sha256(data).hexdigest()

def save_json(x,p):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(x,ensure_ascii=False,sort_keys=True,indent=2,default=str)+'\n',encoding='utf-8')

def save_csv(x,p):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    x.to_csv(p,index=False,compression='gzip' if p.suffix=='.gz' else None,encoding='utf-8-sig')

def check_protocol(path):
    p=json.loads(Path(path).read_text('utf-8'))
    require(p.get('research_id')==VERSION,'PROTOCOL_ID')
    require(p['horizons']==list(HORIZONS) and p['periods']=={k:list(v) for k,v in PERIODS.items()},'LOCKED_PERIOD_HORIZON')
    require(p['strategies']==list(STRATEGIES),'STRATEGY_UNIVERSE')
    require(p['source']['raw_krx_run']==39295295197 and p['source']['krx_days']==1907,'SOURCE_IDENTITY')
    require(p['common']['min_history']==350 and p['common']['cooldown']==20,'COMMON_WARMUP_COOLDOWN')
    require(p['research_only'] and all(p['prohibitions'].values()),'RESEARCH_ISOLATION')
    return p

def source_files(root):
    files={};root=Path(root)
    for k in range(8):
        manifest=root/f'shard_{k:02}'/f'manifest_shard_{k:02}.json'
        require(manifest.is_file(),'MISSING_SHARD_MANIFEST_'+str(k))
        o=json.loads(manifest.read_text('utf-8'))
        require(len(o['files'])>0,'EMPTY_SHARD')
        for ent in o['files']:
            day=ent['date'];f=manifest.parent/ent['file']
            require(day not in files,'DUPLICATED_MARKET_DAY_'+day)
            files[day]=(f,ent['sha256'])
    days=sorted(files)
    require(len(days)==1907 and days[0]=='2019-01-02' and days[-1]=='2026-10-12','KRX_CALENDAR')
    return days,files

def check_raw_sha(files):
    n=0
    for day,(p,digest) in sorted(files.items()):
        require(p.is_file(),'MISSING_KRX_DAY_'+day)
        require(sha(p.read_bytes())==digest,'KRX_DAILY_SHA_'+day)
        n+=1
    require(n==1907,'NOT_ALL_KRX_DAYS_CHECKED')
    return n

def read_day(files,day):
    f=files[day][0]
    df=pd.read_csv(f,compression='gzip',dtype={'code':str,'date':str},
                   usecols=['code','date','open','high','low','close','volume','amount'])
    require(len(df)>0 and df.date.eq(day).all(),'ROW_DATE_DRIFT_'+day)
    require(not df.code.duplicated().any(),'DUPLICATE_TICKER_DAY_'+day)
    return df

def valid_recent(z):
    """350 contiguous OHLCV sessions, no detected large price discontinuity.
    This is NOT split-adjustment / corporate-action certification.
    """
    if len(z)!=350:return False
    t=np.asarray(z,dtype=float)
    if not np.isfinite(t).all():return False
    i,o,h,l,c,v,a=t.T
    if not np.array_equal(i,np.arange(int(i[-1])-349,int(i[-1])+1)):return False
    if np.any((o<=0)|(h<=0)|(l<=0)|(c<=0)|(h<np.maximum(o,c))|(l>np.minimum(o,c))|(h<l)|(v<0)|(a<0)):return False
    if np.any(np.abs(o[1:]/c[:-1]-1)>.40):return False
    return True

def wave1_pullback(arr, mode):
    """Same 1st-wave pattern definition for A (below) and C (above).
    mode='BELOW'/'ABOVE'. Uses only historical bars ending on signal day.
    """
    _,o,h,l,c,v,a=arr.T
    n=len(c)
    # Causal 224d MA at each observed session, including at wave trough.
    ma=pd.Series(c).rolling(224,min_periods=224).mean().to_numpy()
    today_ma=ma[-1]
    if mode=='BELOW':
        if not (c[-1]<today_ma and np.mean(c[-60:]<ma[-60:])>=.70):return None
    else:
        if not (c[-1]>today_ma and today_ma>ma[-21] and np.mean(c[-60:]>ma[-60:])>=.60):return None
    for ago in range(3,16):
        pk=n-1-ago
        for gap in range(3,21):
            tr=pk-gap
            if tr<284:continue
            top=float(h[pk]);bottom=float(l[tr]);rng=top-bottom
            if bottom<=0 or (top/bottom-1)<.12:continue
            if top+1e-6<max(h[tr:pk+1]) or bottom-1e-6>min(l[tr:pk+1]):continue
            retr=(top-c[-1])/rng
            if not .15<=retr<=.618:continue
            before=float(np.median(a[tr-5:tr])/np.median(a[tr-25:tr-5])) if np.median(a[tr-25:tr-5])>0 else float('nan')
            dry=float(np.median(a[-3:])/np.median(a[tr:pk+1])) if np.median(a[tr:pk+1])>0 else float('nan')
            if not (1.03<=before<=2.5 and dry<=.85):continue
            earlier=float(np.mean((c[tr-59:tr+1]<ma[tr-59:tr+1]) if mode=='BELOW' else (c[tr-59:tr+1]>ma[tr-59:tr+1])))
            if mode=='BELOW' and not (c[tr]<ma[tr] and earlier>=.70):continue
            if mode=='ABOVE' and not (c[tr]>ma[tr] and earlier>=.60):continue
            return dict(wave_gain_pct=100*(top/bottom-1),pullback_retrace_pct=100*retr,
                        prewave_amount_ratio=before,pullback_dry_ratio=dry,
                        peak_days_ago=ago,trough_days_before_peak=gap)
    return None

def detect_strategies(rows,protocol):
    """Signal-time only. Pass 350 seq (session,o,h,l,c,volume,amount)."""
    if not valid_recent(rows):return {},'INVALID_350_ASOF_HISTORY'
    a=np.asarray(rows,dtype=float)
    ix,o,h,l,c,v,amt=a.T
    now=float(c[-1]);prev20=np.median(amt[-21:-1]);common=protocol['common']
    if not (now>=common['min_close'] and amt[-1]>=common['min_signal_amount'] and
            prev20>=common['min_prior20_median_amount'] and v[-1]>0):return {},'LOW_LIQUIDITY'
    ma224=float(c[-224:].mean());ma224prev20=float(c[-244:-20].mean())
    out={};short={'ma224':ma224,'ma224_slope20_pct':100*(ma224/ma224prev20-1),
                  'distance224_pct':100*(now/ma224-1),'amount_vs_prior20':float(amt[-1]/prev20)}
    a_wave=wave1_pullback(a,'BELOW') if now<ma224 else None
    if a_wave:out[STRATEGIES[0]]={**short,**a_wave}
    # MA224 reclaim at least 3 sessions before signal, within last 10.
    # Use historic MA for every referenced day; neither future support nor current-day crossover.
    if now>ma224 and c[-1]>c[-2] and c[-1]>np.mean(c[-20:]):
        for ago in range(3,11):
            # Index the crossed day by positive position to avoid wraparound.
            pos=len(c)-1-ago
            earlier=np.mean(c[pos-223:pos+1]);prior_ma=np.mean(c[pos-224:pos])
            if c[pos]>earlier and c[pos-1]<=prior_ma:
                # support 3 daily lows not far below current MA224.
                if min(l[-3:])>=ma224*.98:
                    out[STRATEGIES[1]]={**short,'ma224_first_reclaim_days_ago':ago}
                    break
    c_wave=wave1_pullback(a,'ABOVE') if now>ma224 else None
    if c_wave:out[STRATEGIES[2]]={**short,**c_wave}
    # 120-session prior HIGH resistance breakout after a 10-session compact base.
    # Trigger close > prior resistance; price above rising MA224 and current amount expansion.
    if (now>ma224 and ma224>ma224prev20 and
        now>=np.max(h[-121:-1])*1.005 and
        np.max(h[-11:-1])/np.min(l[-11:-1])-1 <=.18 and
        amt[-1]/prev20>=1.5 and
        np.max(h[-11:-1])>=.95*np.max(h[-121:-1])):
        out[STRATEGIES[3]]={**short,'base10_range_pct':100*(np.max(h[-11:-1])/np.min(l[-11:-1])-1),
                            'breakout_prior120_high':float(np.max(h[-121:-1]))}
    return out,'ASOF_OK'

def discover(days,files,protocol,out):
    hist=defaultdict(lambda:deque(maxlen=350));previous={};events=[];counters=Counter();
    upper=max(end for start,end in PERIODS.values())
    for t,day in enumerate(days):
        if day>upper:break
        df=read_day(files,day)
        active=[tag for tag,(lo,hi) in PERIODS.items() if lo<=day<=hi]
        counters['raw_daily_ticker_rows']+=len(df)
        for rr in df.itertuples(index=False):
            code=str(rr.code)
            if not __import__('re').fullmatch('[0-9A-Z]{6}',code):continue
            z=hist[code]
            z.append((t,float(rr.open),float(rr.high),float(rr.low),float(rr.close),float(rr.volume),float(rr.amount)))
            if not active or len(z)!=350 or z[0][0]!=t-349:continue
            if float(rr.amount)<protocol['common']['min_signal_amount'] or float(rr.close)<protocol['common']['min_close']:continue
            # Avoid unnecessary expensive 350-history checks when no common liquidity.
            prv=np.median([bar[-1] for bar in list(z)[-21:-1]])
            if prv<protocol['common']['min_prior20_median_amount']:continue
            found,status=detect_strategies(z,protocol)
            if status=='INVALID_350_ASOF_HISTORY':counters['invalid_asof_candidate']+=1
            for strategy,info in found.items():
                key=(strategy,code)
                if key in previous and t-previous[key]<protocol['common']['cooldown']:
                    counters['cooldown_suppressed_'+strategy]+=1;continue
                previous[key]=t
                events.append({'strategy':strategy,'date':day,'code':code,'period':active[0],
                               'signal_ix':t,'signal_close':float(rr.close),
                               'amount_KRW':float(rr.amount),'asof_last_date':day,
                               'asof_status':'ASOF_OK',**info})
                counters['signals_'+strategy]+=1
        if t%125==0:print('R1D23_DISCOVERY',day,'raw_events',len(events),flush=True)
    f=pd.DataFrame(events)
    require(not f.empty,'NO_FULL_MARKET_SIGNALS')
    require(not f.duplicated(['strategy','date','code']).any(),'DUPLICATE_STRATEGY_EVENTS')
    require(f.asof_last_date.eq(f.date).all(),'ASOF_DATE_LEAK')
    require(f.period.isin(PERIODS).all(),'DATE_SPOOF')
    forbidden=('status_out','net_20bps_pct','entry_open','exit_close','gross_pct','future_')
    require(not any(str(s).startswith(forbidden) for s in f.columns),'OUTCOME_IN_ASOF')
    f=f.sort_values(['period','strategy','date','code']).reset_index(drop=True)
    fpath=Path(out)/'R1D23_FULLMARKET_A_B_C_D_ASOF_ONLY.csv.gz'
    save_csv(f,fpath)
    digest=sha(fpath.read_bytes())
    return f,digest,dict(counters)

def outcome_paths(features,days,files,out):
    """Called ONLY after frozen ASOF file exists on disk with SHA; horizon 5/10/15/20.
    Each missing ticker/date or raw-action gap is censored, never a loss.
    """
    seq=features.reset_index(drop=True);wanted=defaultdict(set);endix={};limits={}
    for id,r in seq.iterrows():
        ix=int(r.signal_ix);endix[id]=ix+max(HORIZONS)
        for t in range(ix+1,min(ix+max(HORIZONS)+1,len(days))):wanted[t].add(str(r.code))
    price={};maxday=max(wanted) if wanted else -1
    for t in sorted(wanted):
        if t>maxday:break
        df=read_day(files,days[t])
        subset=df[df.code.isin(wanted[t])]
        for rr in subset.itertuples(index=False):price[(t,str(rr.code))]=(float(rr.open),float(rr.high),float(rr.low),float(rr.close))
    result=[]
    for id,r in seq.iterrows():
        i=int(r.signal_ix);code=str(r.code)
        entry=price.get((i+1,code));good=[];bad=False;lastclose=float(r.signal_close)
        for h in range(1,max(HORIZONS)+1):
            t=i+h
            if t>=len(days):bad=True
            bar=price.get((t,code)) if t<len(days) else None
            if bar is None:bad=True
            if not bad and (not all(np.isfinite(bar)) or min(bar)<=0 or abs(bar[0]/lastclose-1)>.40):bad=True
            if not bad:
                good.append(bar);lastclose=bar[3]
            if h in HORIZONS:
                row={'strategy':r.strategy,'date':r.date,'code':code,'period':r.period,'horizon':h,
                     'status_out':'CENSORED_MISSING_OR_ACTION_UNCERTAIN' if bad else DONE,
                     'net_20bps_pct':np.nan,'mfe_intraday_pct':np.nan,'mae_intraday_pct':np.nan,
                     'entry_day':days[i+1] if i+1<len(days) else '',
                     'exit_day':days[t] if t<len(days) else ''}
                if not bad:
                    opening=float(good[0][0]);closing=float(good[-1][3]);
                    row.update(net_20bps_pct=100*(closing/opening-1)-.20,
                               mfe_intraday_pct=100*(max(p[1] for p in good)/opening-1),
                               mae_intraday_pct=100*(min(p[2] for p in good)/opening-1))
                result.append(row)
    paths=pd.DataFrame(result)
    require(len(paths)==len(features)*len(HORIZONS),'OUTCOME_CARDINALITY')
    require(not paths.duplicated(['strategy','date','code','horizon']).any(),'OUTCOME_KEYS_DUPLICATE')
    save_csv(paths,Path(out)/'R1D23_FROZEN_FUTURE_PATHS.csv.gz')
    return paths

def controls_and_scores(features,paths,out):
    j=paths.merge(features[['strategy','date','code','period','distance224_pct','ma224_slope20_pct','amount_KRW']],
                  on=['strategy','date','code','period'],validate='many_to_one')
    complete=j[j.status_out.eq(DONE)].copy()
    complete['win']=(complete.net_20bps_pct>0).astype(int)
    complete['loss5']=(complete.net_20bps_pct<=-5).astype(int)
    complete['strong10']=(complete.net_20bps_pct>=10).astype(int)
    rows=[]
    for (period,s,h),g in j.groupby(['period','strategy','horizon']):
        d=g[g.status_out.eq(DONE)]
        rows.append({'period':period,'strategy':s,'horizon':int(h),'signals':len(g),'complete':len(d),
                     'censored':len(g)-len(d),'unique_codes':d.code.nunique(),'unique_dates':d.date.nunique(),
                     'win_pct':round(float(100*d.net_20bps_pct.gt(0).mean()),3) if len(d) else None,
                     'mean_net_pct':round(float(d.net_20bps_pct.mean()),3) if len(d) else None,
                     'median_net_pct':round(float(d.net_20bps_pct.median()),3) if len(d) else None,
                     'loss5_pct':round(float(100*d.net_20bps_pct.le(-5).mean()),3) if len(d) else None,
                     'strong10_pct':round(float(100*d.net_20bps_pct.ge(10).mean()),3) if len(d) else None,
                     'mfe_plus10_touched_pct':round(float(100*d.mfe_intraday_pct.ge(10).mean()),3) if len(d) else None,
                     'mean_winner_pct':round(float(d.loc[d.net_20bps_pct>0,'net_20bps_pct'].mean()),3) if d.net_20bps_pct.gt(0).any() else None,
                     'mean_loser_pct':round(float(d.loc[d.net_20bps_pct<0,'net_20bps_pct'].mean()),3) if d.net_20bps_pct.lt(0).any() else None,
                     'profit_factor':round(float(d.loc[d.net_20bps_pct>0,'net_20bps_pct'].sum()/-d.loc[d.net_20bps_pct<0,'net_20bps_pct'].sum()),3) if d.net_20bps_pct.lt(0).any() else None,
                     'median_mae_pct':round(float(d.mae_intraday_pct.median()),3) if len(d) else None,
                     'median_mfe_pct':round(float(d.mfe_intraday_pct.median()),3) if len(d) else None,
                     'no_independent_oos':True,'not_real_fill':True})
    scores=pd.DataFrame(rows);save_csv(scores,Path(out)/'R1D23_FOUR_STRATEGY_SCORECARDS.csv')
    yearly=[]
    for (yr,strategy,h),g in complete.groupby([complete.date.str[:4],'strategy','horizon']):
        yearly.append({'year':yr,'strategy':strategy,'horizon':int(h),'complete':len(g),
                       'win_pct':100*g.win.mean(),'mean_net_pct':g.net_20bps_pct.mean(),
                       'loss5_pct':100*g.loss5.mean(),'strong10_pct':100*g.strong10.mean()})
    save_csv(pd.DataFrame(yearly),Path(out)/'R1D23_STRATEGY_YEAR_BY_YEAR.csv')
    # Equal-date controls: only days with BOTH alternatives in source cohort; never
    # treat unmatched days as zeros. Pairwise all strategy pairs, no best-selector.
    pairs=[]
    for (period,h),sub in complete.groupby(['period','horizon']):
        gp=sub.groupby(['date','strategy'])[['win','loss5','net_20bps_pct']].mean()
        for i,a in enumerate(STRATEGIES):
            for b in STRATEGIES[i+1:]:
                ga=gp.xs(a,level='strategy') if a in sub.strategy.values else pd.DataFrame()
                gb=gp.xs(b,level='strategy') if b in sub.strategy.values else pd.DataFrame()
                both=ga.join(gb,how='inner',lsuffix='_a',rsuffix='_b') if not ga.empty and not gb.empty else pd.DataFrame()
                pairs.append({'period':period,'horizon':int(h),'strategy_a':a,'strategy_b':b,'matched_dates':len(both),
                              'equal_date_win_a_minus_b_pp':float(100*(both.win_a-both.win_b).mean()) if len(both) else None,
                              'equal_date_loss5_a_minus_b_pp':float(100*(both.loss5_a-both.loss5_b).mean()) if len(both) else None,
                              'equal_date_mean_net_a_minus_b_pp':float((both.net_20bps_pct_a-both.net_20bps_pct_b).mean()) if len(both) else None,
                              'weak_match':len(both)<20})
    controls=pd.DataFrame(pairs);save_csv(controls,Path(out)/'R1D23_SAME_DATE_STRATEGY_PAIRS.csv')
    return scores,controls

def run(args):
    p=check_protocol(args.protocol);out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    if args.mode=='lock':
        save_json({'version':VERSION,'locked':True,'locked_four_strategies':list(STRATEGIES),
                   'outcomes_read':False,'research_only':True,'new_OOS':False},out/'R1D23_LOCK.json');return
    require(args.mode=='compare','MODE')
    days,files=source_files(args.krx_root);n=check_raw_sha(files)
    feat,seal,counters=discover(days,files,p,out)
    require(sha((out/'R1D23_FULLMARKET_A_B_C_D_ASOF_ONLY.csv.gz').read_bytes())==seal,'ASOF_SEAL_DRIFT')
    paths=outcome_paths(feat,days,files,out)
    scores,pairs=controls_and_scores(feat,paths,out)
    manifest={'version':VERSION,'krx_sha256_all_days_verified':n,'fullmarket_signal_rows':len(feat),
              'signal_counts_by_strategy':feat.strategy.value_counts().to_dict(),
              'price_path_rows':len(paths),'source_original_KRX_only':True,'asof_feature_sha256':seal,
              'data_quality_counts':counters,'score_rows':len(scores),'paired_comparison_rows':len(pairs),
              'historical_research_only':True,'independent_future_OOS':False,
              'not_native_66_scanner':True,'pit_liquidity_certified':False,
              'corporate_actions_pit_certified':False,'no_production_changes':True,'orders_placed':0,
              'strategy_ready':False,'benchmark_A_is_new_symmetric_proxy_not_old_D1':True}
    save_json(manifest,out/'R1D23_MANIFEST.json')
    print('R1D23_COMPLETE_RESEARCH_ONLY',json.dumps({k:v for k,v in manifest.items() if k!='data_quality_counts'},ensure_ascii=False),flush=True)

def main():
    pa=argparse.ArgumentParser();pa.add_argument('--mode',required=True,choices=('lock','compare','legacy-audit','regime-audit'))
    pa.add_argument('--protocol',default='R1D23_PREDECLARED_PROTOCOL.json');pa.add_argument('--krx-root',default='reports/r1d23/input/krx')
    pa.add_argument('--out',default='reports/r1d23/result')
    pa.add_argument('--legacy-r1d9',default='')
    pa.add_argument('--legacy-r1d6',default='')
    args=pa.parse_args()
    if args.mode=='legacy-audit':
        audit_legacy_A(args.out,args.legacy_r1d9,args.legacy_r1d6)
    elif args.mode=='regime-audit':
        audit_market_regime(args.out,args.krx_root)
    else:
        run(args)


# Separate entry point used after a complete historical comparison. It never
# changes new A-D selections or outcomes; old A is a diagnostic, not a label.
def audit_legacy_A(result_dir, r1d9asof_dir, r1d6asof_dir):
    f=Path(result_dir)/'R1D23_FULLMARKET_A_B_C_D_ASOF_ONLY.csv.gz'
    require(f.is_file(),'MISSING_NEW_ASOF_FOR_LEGACY_AUDIT')
    fresh=pd.read_csv(f,dtype={'date':str,'code':str},low_memory=False)
    data=[]
    for period,root,base,expected in [
        ('EARLY_2021_2023',r1d9asof_dir,'R1D9_ASOF_ONLY_2021_23_NO_OUTCOME.csv.gz',7096),
        ('LATE_2024_2025',r1d6asof_dir,'R1D6_ASOF_ONLY_FROZEN_FEATURES_NO_OUTCOME.csv.gz',5247)]:
        src=list(Path(root).rglob(base));require(len(src)==1,'LEGACY_A_SOURCE_MISSING_'+period)
        old=pd.read_csv(src[0],dtype={'date':str,'code':str},low_memory=False)
        require(len(old)==expected and not old[['date','code']].duplicated().any(),'LEGACY_A_IDENTITY_DRIFT')
        new=fresh[(fresh.period==period)&(fresh.strategy==STRATEGIES[0])]
        a=set(map(tuple,old[['date','code']].values));b=set(map(tuple,new[['date','code']].values))
        data.append({'period':period,'old_A_original_proxy_signal_events':len(a),
                     'new_A_fullmarket_symmetric_signal_events':len(b),
                     'identical_date_code_overlap':len(a&b),'old_only':len(a-b),
                     'new_only':len(b-a),'same_A_definition_claimed':False,
                     'cause_of_difference':'DIFFERENT_COMMON_LIQUIDITY_WARMUP_AND_SIGNAL_RULES_NOT_YET_ATTRIBUTED'})
    save_csv(pd.DataFrame(data),Path(result_dir)/'R1D23_OLD_VS_NEW_A_IDENTITY_AUDIT.csv')
    save_json({'old_vs_new_A_comparison':data,'new_A_is_not_equal_old_A':True,
               'claim_of_original_66_parity':False},Path(result_dir)/'R1D23_OLD_A_AUDIT.json')
    return data

def audit_market_regime(result_dir, krx_root):
    """Optional supplementary ASOF 20-day KOSPI/KOSDAQ regime stratification.
    Index values at signal_date and 20 sessions before, NEVER future index values.
    The score is descriptive and not a gating feature.
    """
    idx=Path(krx_root)/'shard_00/official_indices.csv'
    require(idx.is_file(),'KRX_OFFICIAL_INDEX_SOURCE_MISSING')
    x=pd.read_csv(idx,dtype={'date':str,'index':str})
    require(not x[['date','index']].duplicated().any(),'INDEX_DUPLICATE_DATE_OR_NAME')
    w=x.pivot(index='date',columns='index',values='close').sort_index()
    require(set(w.columns)=={'KOSPI','KOSDAQ'} and len(w)==1907 and not w.isna().any().any(),'INDEX_CALENDAR_DRIFT')
    # Preserve the source index labels explicitly. pivot() sorts index labels,
    # so positional renaming would otherwise SWAP KOSPI and KOSDAQ returns.
    ret=(w[['KOSPI','KOSDAQ']]/w[['KOSPI','KOSDAQ']].shift(20)-1)*100
    ret=ret.rename(columns={'KOSPI':'KOSPI_RET20','KOSDAQ':'KOSDAQ_RET20'})
    ret=ret.rename_axis('date').reset_index()  # date is a column, not also an index level
    def regime(a,b):
        if pd.isna(a) or pd.isna(b):return 'UNKNOWN'
        if a<=-5 and b<=-5:return 'BOTH_INDEX_BEAR'
        if a>=5 and b>=5:return 'BOTH_INDEX_BULL'
        if a<=-5 or b<=-5:return 'ONE_INDEX_BEAR'
        return 'MIXED_OR_MODERATE'
    ret['regime']=[regime(a,b) for a,b in zip(ret.KOSPI_RET20,ret.KOSDAQ_RET20)]
    f=pd.read_csv(Path(result_dir)/'R1D23_FULLMARKET_A_B_C_D_ASOF_ONLY.csv.gz',dtype={'date':str,'code':str})
    z=f.merge(ret[['date','KOSPI_RET20','KOSDAQ_RET20','regime']],on='date',validate='many_to_one')
    require(len(z)==len(f),'REGIME_JOIN_CARDINALITY')
    require(z[['KOSPI_RET20','KOSDAQ_RET20']].notna().all().all(),'REGIME_INDEX_MISSING_ON_SIGNAL_DATE')
    save_csv(z[['strategy','date','code','period','KOSPI_RET20','KOSDAQ_RET20','regime']],
             Path(result_dir)/'R1D23_ASOF_MARKET_REGIME_DESCRIPTIVE.csv.gz')
    p=pd.read_csv(Path(result_dir)/'R1D23_FROZEN_FUTURE_PATHS.csv.gz',dtype={'date':str,'code':str})
    v=p.merge(z[['strategy','date','code','period','regime']],on=['strategy','date','code','period'],validate='many_to_one')
    v=v[v.status_out.eq(DONE)]
    rows=[]
    for (tag,reg,st,h),g in v.groupby(['period','regime','strategy','horizon']):
        rows.append({'period':tag,'regime':reg,'strategy':st,'horizon':int(h),
                    'n':len(g),'dates':g.date.nunique(),
                    'win_pct':100*g.net_20bps_pct.gt(0).mean(),
                    'mean_net_pct':g.net_20bps_pct.mean(),
                    'loss5_pct':100*g.net_20bps_pct.le(-5).mean(),
                    'small_cell':len(g)<30 or g.date.nunique()<8})
    save_csv(pd.DataFrame(rows),Path(result_dir)/'R1D23_ASOF_MARKET_REGIME_SENSITIVITY.csv')
    print('R1D23_INDEX_REGIME_DESCRIPTIVE_QA',len(rows),flush=True)
    return len(rows)

if __name__=='__main__':
    main()
