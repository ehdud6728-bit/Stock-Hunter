from pathlib import Path
import argparse,json,hashlib,sys,time,urllib.request
import numpy as np
import pandas as pd
from core import indicators,observe,trade,outcomes
HERE=Path(__file__).resolve().parent;sys.path.insert(0,str(HERE/'vendor'))
from loader import load_prices
from profile import volume_profile
from structure import zones
from auto_data import normalize_marcap,blob_hash
from legacy_sim import simulate_fixed_or_structural
WARM_SHA='f7e4d8eb9df21089c7a6da437722795af6d8e3a6'
FEATURES=['open','high','low','close','volume','amount','ma60','ma112','ma224','ma448','slope60','slope112','slope224','UPPER','LOWER','rsi','rsi_signal','rsi_gc','slow_k','slow_d','slow_gc','body_atr','upper_wick','close_location','volume_ratio20','amount_ratio20','volume_gradual_ratio','amount_gradual_ratio','contraction','range20_pct','close_ma224_pct','ma_alignment','video_alignment','blue_UPPER','blue_LOWER']

def inputs(manifest):
    for name,digest in json.loads((HERE/'reference/SHA256.json').read_text()).items():assert hashlib.sha256((HERE/'reference'/name).read_bytes()).hexdigest()==digest,name
    frames=load_prices(Path(manifest));f=json.loads((HERE/'reference/FROZEN.json').read_text(encoding='utf-8'))
    assert set(frames)==set(f['cohort_codes'])
    for code,digest in f['event_stock_prices'].items():assert hashlib.sha256(frames[code].to_csv(index=False,lineterminator='\n').encode('utf-8-sig')).hexdigest()==digest,code
    warm=HERE/'cache/marcap-2021.parquet';warm.parent.mkdir(exist_ok=True)
    if not warm.exists():
        urllib.request.urlretrieve('https://raw.githubusercontent.com/FinanceData/marcap/50f4d1a74e87c720a03e576218036b1b8414afa1/data/marcap-2021.parquet',warm)
    assert blob_hash(warm.read_bytes())==WARM_SHA
    w,bad=normalize_marcap(pd.read_parquet(warm),2021);w=w[w.code.isin(frames)]
    baseline=pd.read_csv(HERE/'reference/BASELINE.csv.gz',dtype={'code':str})
    return frames,w,baseline,bad

def resistance(d,j):
    history=d.iloc[:j];close=d.close.iloc[j];record={};allzones=[]
    if len(history)<120:record.update(strong_overhead='UNKNOWN_INSUFFICIENT_HISTORY',repeat_overhead='UNKNOWN_INSUFFICIENT_HISTORY',target_zone_count=np.nan)
    else:
        candidates,_=zones(history,120);above=[z for z in candidates if z['low']>close]
        strong=[z for z in above if z['count']>=3 and z['median_drop_atr']>=1]
        record.update(strong_overhead='EVIDENCE_PRESENT' if strong else 'NONE_DETECTED_120D',repeat_overhead='EVIDENCE_PRESENT' if above else 'NONE_DETECTED_120D',
                      target_zone_count=sum(z['high']>=close and z['low']<=close*1.1 for z in candidates),all_repeat_zone_count=len(candidates))
        if above:
            z=min(above,key=lambda z:z['low']);record.update(nearest_resistance_low=z['low'],nearest_resistance_high=z['high'],resistance_room_pct=100*(z['low']/close-1))
        for z in candidates:allzones.append(dict(low=z['low'],high=z['high'],tests=z['count'],median_drop_atr=z['median_drop_atr'],position='ABOVE' if z['low']>close else 'BELOW' if z['high']<close else 'INSIDE',test_dates='|'.join(t['date'] for t in z['tests'])))
    for n in [60,120,240]:
        window=d.iloc[max(0,j-n+1):j+1]
        if len(window)<n or window.volume.sum()<=0:record[f'vp{n}_status']='UNKNOWN';continue
        p=volume_profile(window,32,'range')
        if p is None:record[f'vp{n}_status']='UNKNOWN';continue
        k=int(p['weights'].argmax());record.update({f'vp{n}_status':'DAILY_PROXY_UNVERIFIED',f'vp{n}_poc':p['poc'],f'vp{n}_vah':p['vah'],f'vp{n}_val':p['val'],f'vp{n}_peak_pct':100*p['weights'][k]/p['weights'].sum()})
    return record,allzones

def main(args):
    out=HERE/'results';out.mkdir(exist_ok=True);frames,warm,base,warm_bad=inputs(args.manifest)
    calendar=pd.Index(sorted(set(date for d in frames.values() for date in d.date)));sessions={date:i for i,date in enumerate(calendar)}
    allsignals=[];allepisodes=[];transitions=[];daily=[];decorated={};replays=[];started=time.time()
    for num,(code,raw) in enumerate(sorted(frames.items())):
        pre=warm[warm.code.eq(code)][['date','Close']].rename(columns={'Close':'close'})
        extended=pd.concat([pre,raw[['date','close']]],ignore_index=True).sort_values('date');extended['ma448']=extended.close.rolling(448,min_periods=448).mean()
        d=indicators(raw,extended.set_index('date').ma448);d['session']=d.date.map(sessions);decorated[code]=d
        sig,ep,tr=observe(d);allsignals+=sig;allepisodes+=ep;transitions+=tr
        mask=d.date.between('2023-01-01','2024-12-31')
        daily.append(d.loc[mask,['code','date','session','p0_condition','p1_raw','p1_signal','ma_alignment','video_alignment','ma448','rsi_gc','slow_gc','blue_UPPER','blue_LOWER']])
        for e in base[base.code.eq(code)].itertuples():
            j=int(d.index[d.date.eq(e.date)][0]);entry=int(d.index[d.date.eq(e.entry_date)][0]);b=d.iloc[j]
            assert bool(b['blue_'+e.band]),('BAND_CHANGED',code,e.date,e.band)
            assert bool(d.rsi_gc.iloc[int(d.index[d.date.eq(e.gc_date)][0])]),('RSI_GC_CHANGED',code,e.date)
            result=simulate_fixed_or_structural(raw,j,entry,float(raw.open.iloc[entry]))
            assert abs(result['net_pct']-e.baseline_net_pct)<1e-8 and result['exit_date']==e.baseline_exit_date
            replays.append(dict(code=code,date=e.date,band=e.band,net_pct=result['net_pct'],exit_date=result['exit_date'],gc_offset=e.gc_offset))
        if (num+1)%100==0:print(json.dumps({'indicators_completed':num+1,'seconds':round(time.time()-started,1)}),flush=True)
    assert len(replays)==1804
    p0=next((x for x in allsignals if x['stage']=='P0'),None)
    signals=pd.DataFrame(allsignals).sort_values(['code','date','stage']).reset_index(drop=True);signals['nonoverlap60']=False
    for (_,stage),idxs in signals.groupby(['code','stage']).groups.items():
        last=-10000
        for idx in idxs:
            if signals.at[idx,'session']-last>60:signals.at[idx,'nonoverlap60']=True;last=signals.at[idx,'session']
    ep=pd.DataFrame(allepisodes);p1ep=ep[ep.origin_stage.eq('P1')].set_index(['code','origin_date'])
    anomalies=pd.read_csv(HERE/'reference/KNOWN_ANOMALIES.csv',dtype={'code':str});gaps=pd.read_csv(HERE/'reference/KNOWN_DISCONTINUITIES.csv',dtype={'code':str})
    signal_rows=[];zone_rows=[];trade_rows=[]
    for num,s in enumerate(signals.to_dict('records')):
        d=decorated[s['code']];j=int(s['index']);b=d.iloc[j];cp=int(b.session)
        feat={**s,**{name:b[name] for name in FEATURES},**outcomes(d,j,calendar)}
        history_start=d.date.iloc[max(0,j-240)]
        feat['has_known_source_issue']=bool(((anomalies.code==s['code'])&anomalies.date.between(history_start,s['date'])).any())
        deadline=calendar[cp+20] if cp+20<len(calendar) else None
        feat['quality_gap_lookback240']=bool(((gaps.code==s['code'])&gaps.date.between(history_start,s['date'])).any())
        feat['quality_gap_through20']=bool(((gaps.code==s['code'])&gaps.date.between(history_start,deadline or calendar[-1])).any())
        r,zs=resistance(d,j);feat.update(r)
        for z in zs:zone_rows.append(dict(signal_id=s['signal_id'],code=s['code'],date=s['date'],stage=s['stage'],**z))
        if s['stage']=='P0':
            er=ep[ep.episode_id.eq(s['signal_id'])].iloc[0];feat['next_stage_status']=er.final_status;feat['next_stage_date']=er.p1_date
        else:
            er=p1ep.loc[(s['code'],s['date'] if s['stage']=='P1' else s['p1_date'])]
            feat['episode_status']=er.final_status;feat['next_stage_date']=er.p2_date if s['stage']=='P1' else er.p3_date if s['stage']=='P2' else ''
        original=base[base.code.eq(s['code'])&base.date.ge(s['date'])]
        if len(original):
            first=original.sort_values('date').iloc[0];lead=sessions[first.date]-cp
            feat['lead_to_next_baseline60']=lead if lead<=60 else np.nan
        signal_rows.append(feat)
        result=trade(d,j,deadline,calendar);trade_rows.append(dict(signal_id=s['signal_id'],code=s['code'],date=s['date'],year=int(s['date'][:4]),stage=s['stage'],policy='STAGE_PLUS20',nonoverlap60=s['nonoverlap60'],quality_gap_through20=feat['quality_gap_through20'],**result))
        if (num+1)%1000==0:print(json.dumps({'signal_features_completed':num+1,'total':len(signals),'seconds':round(time.time()-started,1)}),flush=True)
    # One P1 opportunity per row per policy, including no-entry opportunities.
    opportunities=[]
    for er in ep[ep.origin_stage.eq('P1')].itertuples():
        d=decorated[er.code];p1=int(d.index[d.date.eq(er.origin_date)][0]);cp=int(d.session.iloc[p1]);deadline=calendar[cp+20] if cp+20<len(calendar) else None
        path=outcomes(d,p1,calendar)
        for stage,date in [('P1',er.p1_date),('P2',er.p2_date),('P3',er.p3_date)]:
            row=dict(opportunity_id=er.episode_id,code=er.code,p1_date=er.origin_date,year=int(er.origin_date[:4]),policy=stage,episode_status=er.final_status,
                     original_max_up20=path.get('max_up_20',np.nan),path_coverage=path.get('coverage_20','MISSING'))
            if pd.isna(date) or date=='':result={'trade_status':'NO_STAGE_SIGNAL'}
            else:result=trade(d,int(d.index[d.date.eq(date)][0]),deadline,calendar)
            row.update(result);entered=result['trade_status']=='CLOSED'
            row['entered']=entered;row['opportunity_net_cash']=result['net_pct'] if entered else 0. if result['trade_status'] in ['NO_STAGE_SIGNAL','ENTRY_AFTER_DEADLINE'] else np.nan
            row['missed10']=not entered and path.get('max_up_20',-np.inf)>=10;row['missed20']=not entered and path.get('max_up_20',-np.inf)>=20
            opportunities.append(row)
    # Preserve exact baseline horizons. Same-stock earlier new stages are a descriptive paired subset, not a deployed selection rule.
    comparisons=[]
    sig=pd.DataFrame(signal_rows)
    for e in base.itertuples():
        old=dict(code=e.code,date=e.date,year=e.year,band=e.band,gc_offset=e.gc_offset,rsi_group='GC0' if e.gc_offset==0 else 'GC_DELAY_1_10',baseline_net=e.baseline_net_pct,baseline_exit_date=e.baseline_exit_date)
        for stage in ['P0','P1','P2','P3']:
            nearby=sig[sig.code.eq(e.code)&sig.stage.eq(stage)&sig.session.between(sessions[e.date]-60,sessions[e.date]+20)]
            before=nearby[nearby.session.le(sessions[e.date])]
            chosen=before.sort_values('date').iloc[-1] if len(before) else None
            row={**old,'stage':stage,'matched_prior60':chosen is not None}
            if chosen is not None:
                row.update(stage_date=chosen.date,lead_sessions=sessions[e.date]-chosen.session)
                tr=next(x for x in trade_rows if x['signal_id']==chosen.signal_id)
                row.update(stage_trade_status=tr['trade_status'],stage_net=tr.get('net_pct',np.nan),comparison_horizon='STAGE_PLUS20_VS_ORIGINAL_D0_PLUS20_NOT_IDENTICAL')
            comparisons.append(row)
    for name,data in [('SIGNALS',signal_rows),('EPISODES',allepisodes),('TRANSITIONS',transitions),('TRADES',trade_rows),('P1_SAME_OPPORTUNITIES',opportunities),('BASELINE_COMPARISONS',comparisons),('PRICE_RESISTANCE_ZONES',zone_rows),('BASELINE_REPLAY',replays)]:pd.DataFrame(data).to_csv(out/(name+'.csv.gz'),index=False)
    pd.concat(daily,ignore_index=True).to_csv(out/'ALL_STOCK_DAYS.csv.gz',index=False)
    sig[sig.stage.eq('P0')].to_csv(out/'P0_CANDIDATES.csv.gz',index=False)
    status=dict(original_events=1804,upper_events=int(base.band.eq('UPPER').sum()),lower_events=int(base.band.eq('LOWER').sum()),stocks=500,
                all_stock_days=int(sum(len(x) for x in daily)),signals_by_stage=sig.stage.value_counts().to_dict(),baseline_replayed=1804,warmup_2021_invalid_rows_removed=warm_bad,
                video_formula='UNVERIFIED',dminus1_R1_patch='NOT_FOUND_IN_REMOTE_TREE_NOT_ASSUMED_APPLIED',price_adjustment='UNVERIFIED',volume_profile='DAILY_PROXY_UNVERIFIED',independent_2025_2026='NOT_ACCESSED',strategy_status='RESEARCH_ONLY')
    (out/'STATUS.json').write_text(json.dumps(status,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(status,ensure_ascii=False),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args())
