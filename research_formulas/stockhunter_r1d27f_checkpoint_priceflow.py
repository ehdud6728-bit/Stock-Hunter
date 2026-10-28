#!/usr/bin/env python3
"""R1-D27F: retrospective, checkpoint-timestamp-safe D+3/5/7 study.

Input: fixed R1D27E final data; optional original signal-candle/MA source.
At checkpoint D+t close, only D+1..D+t flows and prices can enter features.
Outcomes use NEXT SESSION OPEN (D+t+1) through D+15; no intraday fill claim.
Research only, no Live signals, no strategy optimization and NO prospective OOS.
"""
import argparse,hashlib,io,json,pathlib,zipfile
import numpy as np
import pandas as pd

KEY=['date','code','bb_period','event_type']
POINTS=(3,5,7)
THRESHOLDS=(5,7,10,12,15)
# SHA256 of source members from GitHub run 40702069936, artifact 12822131773.
PINNED_SOURCE_SHA={
 'R1D27E_POST15_DAILY_FLOW_PRICE_1_15.csv':'581e83ce719d71c3d4246463aab66d8a326a4f7294cb3db8f0a0e858a1880309',
 'R1D27E_POST15_INST_FOREIGN_DAILY_SEPARATE.csv':'996a1862172aeaa3501740e65f8daa881219d769eea5639ebd96cca202a78294',
 'R1D27E_POST15_QUARANTINE_NO_IMPUTATION.csv':'e262beb234aaca7688b0f32143a7bf296c61a32be6a86f72d14eecc4817cb537',
 'R1D27E_POST15_ANALYSIS_MANIFEST.json':'dfa1f281a1c25d0930a7c11a3f892a90d08cf95f27dc7198428204f86fb07d59'
}
PINNED_CANDLE_SHA='49a64a566b0799a6e9023593c4970545bf91fd2f1dabba884eb5786170e75f5a'
PINNED_OHLC_SHA='8b6b10c430e5c326b012bb4dd90685e6e305f73a956bbd1039a487f060fb0cf3'
CANDLE_NAME='R1D27D_02_CANDLE_MA_BB_ENV_SIGNAL_DAY_1500.csv'
FLOWFILE='R1D27E_POST15_DAILY_FLOW_PRICE_1_15.csv'
ACTORFILE='R1D27E_POST15_INST_FOREIGN_DAILY_SEPARATE.csv'
AUDITFILE='R1D27E_POST15_QUARANTINE_NO_IMPUTATION.csv'
ANALYSIS_MAN='R1D27E_POST15_ANALYSIS_MANIFEST.json'

def ensure(a,msg):
    if not a:raise RuntimeError('R1D27F_'+msg)

def sha(b):return hashlib.sha256(b).hexdigest()

def csv_from_zip(z,n,code=True):
    # Exact basenames in the GitHub archive; no ambiguous suffix selection.
    names=[p for p in z.namelist() if p.split('/')[-1]==n]
    ensure(len(names)==1,'FILE_NOT_UNIQUE_'+n)
    d=pd.read_csv(io.BytesIO(z.read(names[0])),dtype={'date':str,'code':str,'bb_period':str,'event_type':str},low_memory=False)
    return d

def load_inputs(zip_path,candle_path,price_path):
    zb=pathlib.Path(zip_path).read_bytes()
    ensure(zipfile.is_zipfile(io.BytesIO(zb)),'FINAL_ZIP_INVALID')
    with zipfile.ZipFile(io.BytesIO(zb)) as z:
        for filename,expected_sha in PINNED_SOURCE_SHA.items():
            matches=[p for p in z.namelist() if p.split('/')[-1]==filename]
            ensure(len(matches)==1 and sha(z.read(matches[0]))==expected_sha,'SOURCE_MEMBER_SHA_'+filename)
        flow=csv_from_zip(z,FLOWFILE)
        actor=csv_from_zip(z,ACTORFILE)
        audit=csv_from_zip(z,AUDITFILE)
        mbytes=[p for p in z.namelist() if p.split('/')[-1]==ANALYSIS_MAN]
        ensure(len(mbytes)==1,'MANIFEST_NOT_UNIQUE')
        m=json.loads(z.read(mbytes[0]))
    ensure(m['post_flow_complete']==1483 and m['post_flow_quarantined']==16 and m['post_flow_retry_pending_missing']==1,'FINAL_POPULATION')
    ensure(m['missing_17_excluded_not_zero'] and m['paired_pre_signal_and_post_flow']==1465,'FINAL_SOURCE_GUARDS')
    ensure(len(flow)==1483*15 and len(actor)==1483*15*2,'DAILY_ROWS')
    ensure(set(flow.day)==set(range(1,16)),'DAILY_INDEX')
    ensure(not flow.duplicated(KEY+['day']).any(),'DAILY_DUP')
    ensure(flow.groupby(KEY,sort=False).size().eq(15).all(),'DAILY_COMPLETENESS')
    ensure(flow.price_status.eq('COMPLETE').all(),'PRICE_GAPS')
    ensure((flow.entry_open>0).all() and (flow.daily_traded_krw>0).all(),'INVALID_PRICE')
    ensure((flow.buy_krw-flow.sell_krw-flow.net_krw).abs().le(3).all(),'NET_IDENTITY')
    ensure(not actor.duplicated(KEY+['actor','day']).any(),'ACTOR_DUP')
    ensure(set(actor.actor)=={'기관합계','외국인합계'},'ACTORS')
    ensure((actor.buy_krw-actor.sell_krw-actor.net_krw).abs().le(3).all(),'ACTOR_NET_IDENTITY')
    q=actor.groupby(KEY+['day'],as_index=False).net_krw.sum().merge(flow[KEY+['day','net_krw']],on=KEY+['day'],validate='one_to_one')
    ensure((q.net_krw_x-q.net_krw_y).abs().le(3).all(),'COMBINED_NET_IDENTITY')
    # Restore exact D+t+1 OPEN from the independent R1D27D validated daily OHLC source.
    # The final D27E artifact deliberately had no 'daily_open': never substitute D+1 entry open.
    pb=pathlib.Path(price_path).read_bytes()
    ensure(sha(pb)==PINNED_OHLC_SHA,'PINNED_OHLC_SHA')
    baseprice=pd.read_csv(io.BytesIO(pb),dtype={'date':str,'code':str,'bb_period':str,'event_type':str},low_memory=False)
    ensure(len(baseprice)==22500 and not baseprice.duplicated(KEY+['day']).any(),'PRICE_SOURCE_COUNTS')
    need=baseprice[KEY+['day','price_day','daily_open','daily_close','daily_high','entry_open','daily_traded_krw']]
    flow=flow.merge(need,on=KEY+['day','price_day'],how='left',validate='one_to_one',suffixes=('','_original'),indicator=True)
    ensure(flow['_merge'].eq('both').all(),'PRICE_DATE_KEY_MISMATCH')
    flow=flow.drop(columns=['_merge'])
    for col in ('daily_close','daily_high','entry_open','daily_traded_krw'):
        a=flow[col].to_numpy(dtype=float);b=flow[col+'_original'].to_numpy(dtype=float)
        ensure(np.allclose(a,b,rtol=1e-9,atol=1e-5,equal_nan=True),'PRICE_DISAGREEMENT_'+col)
        flow=flow.drop(columns=[col+'_original'])
    ensure(flow.daily_open.notna().all() and flow.daily_open.gt(0).all(),'FUTURE_OPEN_MISSING')
    cb=pathlib.Path(candle_path).read_bytes()
    ensure(sha(cb)==PINNED_CANDLE_SHA,'PINNED_CANDLE_SHA')
    candle=pd.read_csv(io.BytesIO(cb),dtype={'date':str,'code':str,'bb_period':str,'event_type':str})
    ensure(len(candle)==1500 and not candle.duplicated(KEY).any(),'CANDLE_SOURCE_COUNTS')
    keys=set(map(tuple,flow[KEY].drop_duplicates().to_numpy()))
    ensure(keys.issubset(set(map(tuple,candle[KEY].to_numpy()))),'CANDLE_KEYS')
    ensure(len(audit)==17 and not audit.duplicated(KEY).any(),'MISSING_17')
    ensure(not keys.intersection(set(map(tuple,audit[KEY].to_numpy()))),'FLOW_AND_MISSING_DISJOINT')
    return flow,actor,candle,m,{'github_final_zip_sha256':sha(zb),'candle_source_sha256':sha(cb),'original_daily_OHLC_sha256':sha(pb)}

def make_checkpoints(flow,actor,candle):
    # Evidence fence: first derive ALL checkpoint predictors from history <= D+t.
    f=flow.sort_values(KEY+['day']).copy()
    a=actor.sort_values(KEY+['actor','day']).copy()
    features=[]
    for t in POINTS:
        hist=f[f.day<=t]
        now=f[f.day==t].set_index(KEY)
        window=hist.groupby(KEY,sort=False)
        aggregate=window.agg(cum_net_krw=('net_krw','sum'),cum_turnover_krw=('daily_traded_krw','sum'),
                             buy_days=('net_krw',lambda s:int((s>0).sum())), prior_high=('daily_high','max'),
                             last_close=('daily_close','last'),last_turnover=('daily_traded_krw','last')).reset_index()
        recent=hist[hist.day>t-3].groupby(KEY,sort=False).net_krw.sum().rename('last3_net_krw').reset_index()
        aggregate=aggregate.merge(recent,on=KEY,validate='one_to_one')
        aggregate['checkpoint_day']=t
        aggregate['cum_net_pct_turnover']=100*aggregate.cum_net_krw/aggregate.cum_turnover_krw
        aggregate['cum_positive']=(aggregate.cum_net_krw>0).astype(int)
        aggregate['last3_positive']=(aggregate.last3_net_krw>0).astype(int)
        aggregate['BOTH_POS']=(aggregate.cum_positive.eq(1)&aggregate.last3_positive.eq(1)).astype(int)
        aggregate['KNOWN_PULLBACK_3PCT']=(100*(aggregate.prior_high-aggregate.last_close)/aggregate.prior_high>=3).astype(int)
        aggregate['KNOWN_PRICE_ABOVE_INITIAL_ENTRY']=(aggregate.last_close>=now.entry_open.reindex(pd.MultiIndex.from_frame(aggregate[KEY])).to_numpy()).astype(int)
        agg_actor=a[a.day<=t].groupby(KEY+['actor'],sort=False).net_krw.sum().unstack('actor').reset_index()
        agg_actor=agg_actor.rename(columns={'기관합계':'institution_cum_net_krw','외국인합계':'foreign_cum_net_krw'})
        aggregate=aggregate.merge(agg_actor,on=KEY,validate='one_to_one')
        aggregate['INST_POS']=(aggregate.institution_cum_net_krw>0).astype(int)
        aggregate['FOREIGN_POS']=(aggregate.foreign_cum_net_krw>0).astype(int)
        aggregate=aggregate.merge(candle[KEY+['signal_close','MA224_level_reconstructed','lower_wick_pct','close_location_pct','candle_body_pct','BB20_width_pct','MA20_level_reconstructed','MA60_level_reconstructed']],on=KEY,validate='many_to_one')
        aggregate['SIGNAL_BELOW_MA224']=(aggregate.signal_close<aggregate.MA224_level_reconstructed).astype(int)
        aggregate['SIGNAL_CLOSE_ABOVE_MA20']=(aggregate.signal_close>=aggregate.MA20_level_reconstructed).astype(int)
        aggregate['SIGNAL_CLOSE_ABOVE_MA60']=(aggregate.signal_close>=aggregate.MA60_level_reconstructed).astype(int)
        aggregate['SIGNAL_CLOSE_ABOVE_ENV20_5UPPER']=(aggregate.signal_close>=aggregate.MA20_level_reconstructed*1.05).astype(int)
        aggregate['SIGNAL_BB20_WIDTH_LE20']=(aggregate.BB20_width_pct<=20).astype(int)
        features.append(aggregate)
    feat=pd.concat(features,ignore_index=True)
    ensure(len(feat)==1483*3 and not feat.duplicated(KEY+['checkpoint_day']).any(),'CHECKPOINT_N')
    return feat

def make_future_outcomes(flow):
    # Price outcomes are read only AFTER predictors have been sealed.
    future=[]
    for t in POINTS:
        nextday=flow[flow.day==t+1][KEY+['daily_open']].rename(columns={'daily_open':'decision_next_open'})
        fut=flow[flow.day>=t+1].groupby(KEY,sort=False).agg(future_max_high=('daily_high','max'),future_min_low=('daily_close','min')).reset_index()
        last=flow[flow.day==15][KEY+['daily_close']].rename(columns={'daily_close':'d15_close'})
        df=nextday.merge(fut,on=KEY,validate='one_to_one').merge(last,on=KEY,validate='one_to_one')
        df['checkpoint_day']=t
        df['future_d15_net20bps_pct']=100*(df.d15_close/df.decision_next_open-1)-0.2
        df['future_max_high_gross_pct']=100*(df.future_max_high/df.decision_next_open-1)
        df['future_peak_to_d15_giveback_pct']=100*(df.future_max_high-df.d15_close)/df.decision_next_open
        df['future_profitable']=(df.future_d15_net20bps_pct>0).astype(int)
        df['future_loss10']=(df.future_d15_net20bps_pct<=-10).astype(int)
        for target in THRESHOLDS:
            df[f'future_hit{target}_gross']=(df.future_max_high_gross_pct>=target).astype(int)
        future.append(df)
    out=pd.concat(future,ignore_index=True)
    ensure(len(out)==1483*3,'OUTCOME_N')
    return out

def summarize(g):
    return dict(n=int(len(g)),mean_d15_net_pct=float(g.future_d15_net20bps_pct.mean()),
                median_d15_net_pct=float(g.future_d15_net20bps_pct.median()),
                win_rate_pct=float(g.future_profitable.mean()*100),
                loss10_rate_pct=float(g.future_loss10.mean()*100),
                mean_future_peak_pct=float(g.future_max_high_gross_pct.mean()),
                median_peak_to_close_giveback_pct=float(g.future_peak_to_d15_giveback_pct.median()),
                **{f'hit{k}_pct':float(100*g[f'future_hit{k}_gross'].mean()) for k in THRESHOLDS})

def fixed_effect_contrast(df,flag,outcol='future_d15_net20bps_pct',groups=('year','bb_period','event_type')):
    v=df[flag].to_numpy(dtype=float);y=df[outcol].to_numpy(dtype=float)
    groupidx=[np.asarray(ii,dtype=int) for _,ii in df.groupby(list(groups),sort=False).indices.items()]
    vx=v.copy();yx=y.copy()
    for ix in groupidx:vx[ix]-=v[ix].mean();yx[ix]-=y[ix].mean()
    denom=np.dot(vx,vx)
    return float(np.dot(vx,yx)/denom) if denom>0 else float('nan')

def permutation(df,flag,seed=20261028,iters=2000):
    original=fixed_effect_contrast(df,flag)
    rng=np.random.default_rng(seed)
    raw=df[flag].to_numpy(dtype=float)
    strata=[np.asarray(ix,dtype=int) for _,ix in df.groupby(['year','bb_period','event_type'],sort=False).indices.items()]
    y=df.future_d15_net20bps_pct.to_numpy(dtype=float)
    centered_y=y.copy();centered_x=raw.copy()
    for ix in strata:centered_y[ix]-=y[ix].mean();centered_x[ix]-=raw[ix].mean()
    denom=np.dot(centered_x,centered_x)
    if denom<=0:return original,float('nan')
    exceed=0
    for _ in range(iters):
        x=np.empty(len(df),dtype=float)
        for ix in strata:x[ix]=rng.permutation(centered_x[ix])
        stat=float(np.dot(x,centered_y)/denom)
        exceed+=abs(stat)>=abs(original)-1e-12
    return original,(exceed+1)/(iters+1)

def main(finalzip,candle,price,outdir):
    out=pathlib.Path(outdir);out.mkdir(parents=True,exist_ok=True)
    flow,actor,signal,m,inputs=load_inputs(finalzip,candle,price)
    predictors=make_checkpoints(flow,actor,signal)
    # Sealed feature file; any signal-time classification may read this but NEVER future values.
    predictors.to_csv(out/'01_FROZEN_CHECKPOINT_FEATURES_D3_D5_D7.csv',index=False,encoding='utf-8-sig')
    outcomes=make_future_outcomes(flow)
    joint=predictors.merge(outcomes,on=KEY+['checkpoint_day'],how='inner',validate='one_to_one')
    ensure(len(joint)==1483*3,'MERGED_N')
    joint['year']=joint.date.str[:4].astype(int)
    ensure(joint.year.between(2021,2025).all(),'YEARS')
    joint.to_csv(out/'02_FUTURE_D15_EVENT_EVALUATIONS.csv',index=False,encoding='utf-8-sig')
    analysis=[]
    for t in POINTS:
        d=joint[joint.checkpoint_day==t]
        for grouping in ('BOTH_POS','cum_positive','last3_positive','INST_POS','FOREIGN_POS','SIGNAL_BELOW_MA224','KNOWN_PULLBACK_3PCT'):
            for q,g in d.groupby(grouping):
                analysis.append(dict(day=t,dimension=grouping,group=int(q),**summarize(g)))
    summary=pd.DataFrame(analysis)
    summary.to_csv(out/'03_TIMEPOINT_GROUP_METRICS.csv',index=False,encoding='utf-8-sig')
    year_rows=[]
    for t in POINTS:
        d=joint[joint.checkpoint_day==t]
        for y,g0 in d.groupby('year'):
            for q,g in g0.groupby('BOTH_POS'):
                year_rows.append(dict(day=t,year=int(y),BOTH_POS=int(q),**summarize(g)))
    year=pd.DataFrame(year_rows)
    year.to_csv(out/'04_YEAR_2021_2025_DIAGNOSTICS.csv',index=False,encoding='utf-8-sig')
    # Primary preregistered here: BOTH_POS at D+3/5/7, D+15 remaining-horizon net return.
    primary=[]
    for t in POINTS:
        d=joint[joint.checkpoint_day==t].reset_index(drop=True)
        yes=d[d.BOTH_POS==1];no=d[d.BOTH_POS==0]
        stat,p=permutation(d,'BOTH_POS',seed=20261028+t,iters=2000)
        split=d[d.year<=2024];hold=d[d.year==2025]
        row=dict(day=t,year_all_n=len(d),yes_n=len(yes),no_n=len(no),yes_mean_net=float(yes.future_d15_net20bps_pct.mean()),
                 no_mean_net=float(no.future_d15_net20bps_pct.mean()),raw_gap_pp=float(yes.future_d15_net20bps_pct.mean()-no.future_d15_net20bps_pct.mean()),
                 fe_year_BB_event_gap_pp=stat,permutation_2000_two_sided_p=p,
                 train_2021_24_n=len(split),test_2025_n=len(hold),
                 train_2021_24_raw_gap_pp=float(split.loc[split.BOTH_POS.eq(1),'future_d15_net20bps_pct'].mean()-split.loc[split.BOTH_POS.eq(0),'future_d15_net20bps_pct'].mean()),
                 test_2025_raw_gap_pp=float(hold.loc[hold.BOTH_POS.eq(1),'future_d15_net20bps_pct'].mean()-hold.loc[hold.BOTH_POS.eq(0),'future_d15_net20bps_pct'].mean()),
                 year_test_not_prospective_oos=True)
        primary.append(row)
    primary=pd.DataFrame(primary)
    ps=primary.permutation_2000_two_sided_p.to_numpy();order=np.argsort(ps)
    holm=np.empty(3)
    running=0
    for r,j in enumerate(order):running=max(running,min(1.,(3-r)*ps[j]));holm[j]=running
    primary['holm_3_checkpoints_p']=holm
    primary.to_csv(out/'05_PRIMARY_STRATIFIED_PERMUTATION.csv',index=False,encoding='utf-8-sig')
    # Exploratory joint condition is not a new adopted trading rule. Compare price-path groups only.
    exploratory=[]
    for t in POINTS:
        d=joint[joint.checkpoint_day==t].copy()
        d['FOUR_QUADRANTS']=np.where(d.BOTH_POS.eq(1),'FLOW_POS','FLOW_NOT_POS')+' / '+np.where(d.KNOWN_PULLBACK_3PCT.eq(1),'PULLBACK_3PCT','NOT_PULLBACK')
        for k,g in d.groupby('FOUR_QUADRANTS'):
            exploratory.append(dict(day=t,condition=k,**summarize(g)))
    pd.DataFrame(exploratory).to_csv(out/'06_EXPLORATORY_FLOW_PRICE_QUADRANTS.csv',index=False,encoding='utf-8-sig')
    structural=[]
    for t in POINTS:
        d=joint[joint.checkpoint_day==t]
        for dimension in ('SIGNAL_BELOW_MA224','SIGNAL_CLOSE_ABOVE_MA20','SIGNAL_CLOSE_ABOVE_MA60','SIGNAL_CLOSE_ABOVE_ENV20_5UPPER','SIGNAL_BB20_WIDTH_LE20'):
            for state,group in d.groupby(dimension):
                structural.append(dict(day=t,signal_time_only_dimension=dimension,group=int(state),**summarize(group)))
    pd.DataFrame(structural).to_csv(out/'07_SIGNAL_CANDLE_MA_BB_ENV_EXPLORATORY.csv',index=False,encoding='utf-8-sig')
    # Checksum audit includes evidence sources and result CSVs.
    mani=dict(name='R1D27F_CHECKPOINT_CAUSAL_ORDER_AUDIT_RESEARCH_ONLY',
              prepared_date='2026-10-28',inputs=inputs,source_phase=m['phase'],frozen_n=1500,
              source_postflow_n=1483,source_quarantined=16,source_missing_retry=1,
              observed_event_rows=len(joint),checkpoint_days=list(POINTS),
              predict_at='D+t AFTER CLOSE',hypothetical_entry='D+t+1 OPEN',exit='D+15 CLOSE',
              cost_model='20bps round-trip as end return subtraction; no fills/slippage simulation',
              primary='BOTH_POS: cumulative net>0 AND last 3 sessions net>0 (institution+foreign KRW); checkpoints fixed 3/5/7',
              primary_tests=3,permutations=2000,year_strata='2021-2025 x BB period x event type',
              historical_2025_separate_not_prospective_oos=True,PIT_certified=False,
              source_no_lookahead_at_checkpoint=True, signal_day_candle_ma_preexisting_only=True,
              no_LIVE_or_order_or_rank_changes=True,
              outputs={p.name:sha(p.read_bytes()) for p in out.glob('*.csv')})
    (out/'08_RESEARCH_AUDIT_MANIFEST.json').write_text(json.dumps(mani,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('R1D27F_FINAL',json.dumps(primary.to_dict('records'),ensure_ascii=False,indent=2))
    return primary

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--final-artifact-zip',required=True)
    p.add_argument('--signal-candle-csv',required=True)
    p.add_argument('--original-daily-price-csv',required=True)
    p.add_argument('--out',required=True)
    a=p.parse_args()
    main(a.final_artifact_zip,a.signal_candle_csv,a.original_daily_price_csv,a.out)
