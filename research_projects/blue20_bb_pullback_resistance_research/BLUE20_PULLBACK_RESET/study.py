from pathlib import Path
import sys,json,argparse,hashlib,time
import numpy as np
import pandas as pd
from engine import detect,candidate,retry_candidate,simulate,path
HERE=Path(__file__).resolve().parent;sys.path.insert(0,str(HERE/'vendor'))
from loader import load_prices
from legacy_observer import indicators
from structure import zones
from profile import volume_profile
ENTRIES=['A_FIRST','B_RECOVERY','C_RETEST','C_BROKEN_BODY','D_NEW_BLUE','D_BLUE_RSI','R0_P2','R0_P3']
STOPS=['FIXED5','PULLBACK_LOW','LOW_MINUS_025ATR']

def context(d,j):
    old=d.iloc[:j];bar=d.iloc[j];close=float(bar.close);previous=float(d.close.iloc[j-1]);result={};pool=[];price=[];vol=[];body_price=[];body_vol=[]
    if len(old)<120:return dict(context_status='UNKNOWN_HISTORY',strong_active=False,repeat_active=False,stable_volume_active=False,strong_body=False,any_body=False),[],None,None
    zs,_=zones(old,120)
    for n,z in enumerate(zs):
        active=z['high']>=close;strong=z['count']>=3 and z['median_drop_atr']>=1
        row=dict(zone_id='P'+str(n),kind='PRICE_REJECTION',low=z['low'],high=z['high'],count=z['count'],drop_atr=z['median_drop_atr'],strong=strong,
                 position='ABOVE' if z['low']>close else 'BELOW' if z['high']<close else 'INSIDE',test_dates='|'.join(x['date'] for x in z['tests']))
        overlap=max(0,min(close,z['high'])-max(float(bar.open),z['low']))
        row['newly_cleared_body']=bool(previous<=z['high']<close and overlap>0)
        row['body_overlap_ratio']=overlap/max(float(bar.body),1e-12)
        if row['newly_cleared_body']:body_price.append(row)
        pool.append(row)
        if active:price.append(row)
    w=old.iloc[-120:];a=volume_profile(w,32,'range');b=volume_profile(w,32,'typical')
    if a is not None and b is not None:
        for k,c in enumerate(a['centers']):
            local=(k==0 or a['weights'][k]>a['weights'][k-1]) and (k==31 or a['weights'][k]>=a['weights'][k+1])
            if local and a['weights'][k]/a['weights'].sum()>=2/32 and b['weights'][k]/b['weights'].sum()>=1.5/32:
                row=dict(zone_id='V'+str(k),kind='DAILY_MODEL_PROXY',low=c-a['width']/2,high=c+a['width']/2,count=np.nan,drop_atr=np.nan,strong=False,
                    position='ABOVE' if c-a['width']/2>close else 'BELOW' if c+a['width']/2<close else 'INSIDE',test_dates='',range_pct=100*a['weights'][k]/a['weights'].sum(),typical_pct=100*b['weights'][k]/b['weights'].sum())
                pool.append(row)
                if row['high']>=close:vol.append(row)
                overlap=max(0,min(close,row['high'])-max(float(bar.open),row['low']))
                row['newly_cleared_body']=bool(previous<=row['high']<close and overlap>0);row['body_overlap_ratio']=overlap/max(float(bar.body),1e-12)
                if row['newly_cleared_body']:body_vol.append(row)
    strong=[x for x in price if x['strong']]
    preferred=strong or price or vol
    selected=min(preferred,key=lambda x:(max(0,x['low']-close),x['low'],x['high'])) if preferred else None
    strong_body=[x for x in body_price if x['strong']];body_preferred=strong_body or body_price or body_vol
    body_selected=min(body_preferred,key=lambda x:(close-x['high'],-x['body_overlap_ratio'],x['low'])) if body_preferred else None
    result.update(context_status='OBSERVED_UNVERIFIED_SOURCE',strong_active=bool(strong),repeat_active=bool(price),stable_volume_active=bool(vol),
        any_active=bool(price or vol),strong_blocking10=any(x['low']<=close*1.1 for x in strong),repeat_blocking10=any(x['low']<=close*1.1 for x in price),
        initial_close=close,initial_atr=float(d.prior_atr20.iloc[j]),initial_body_atr=float(d.body_atr.iloc[j]),selected_zone_id=selected['zone_id'] if selected else '',
        selected_zone_low=selected['low'] if selected else np.nan,selected_zone_high=selected['high'] if selected else np.nan,selected_zone_kind=selected['kind'] if selected else '',
        strong_body=bool(strong_body),any_body=bool(body_price or body_vol),strong_relevant=bool(strong or strong_body),
        body_zone_id=body_selected['zone_id'] if body_selected else '',body_zone_low=body_selected['low'] if body_selected else np.nan,body_zone_high=body_selected['high'] if body_selected else np.nan)
    return result,pool,selected,body_selected

def main(manifest):
    out=HERE/'results';out.mkdir(exist_ok=True)
    for name,digest in json.loads((HERE/'reference/SHA256.json').read_text()).items():assert hashlib.sha256((HERE/'reference'/name).read_bytes()).hexdigest()==digest,name
    frames=load_prices(Path(manifest));frozen=json.loads((HERE/'reference/FROZEN.json').read_text(encoding='utf-8'));assert set(frames)==set(frozen['cohort_codes'])
    for code,digest in frozen['event_stock_prices'].items():assert hashlib.sha256(frames[code].to_csv(index=False,lineterminator='\n').encode('utf-8-sig')).hexdigest()==digest,code
    baseline=pd.read_csv(HERE/'reference/BASELINE.csv.gz',dtype={'code':str});early=pd.read_csv(HERE/'reference/EARLY_SIGNALS.csv.gz',dtype={'code':str})
    assert len(baseline)==1804
    calendar=pd.Index(sorted(set(v for d in frames.values() for v in d.date)));sessions={v:i for i,v in enumerate(calendar)}
    ds={code:indicators(d) for code,d in frames.items()};events_by_p1={key:g for key,g in early[early.stage.isin(['P2','P3'])].groupby(['code','p1_date'])}
    anchors=[]
    for e in baseline.itertuples():anchors.append(dict(universe='BASELINE_1804',code=e.code,date=e.date,band=e.band,gc_offset=e.gc_offset))
    for e in early[early.stage.eq('P1')].itertuples():anchors.append(dict(universe='ALL_DAYS_P1',code=e.code,date=e.date,band='P1',gc_offset=np.nan))
    anchors=pd.DataFrame(anchors).sort_values(['universe','code','date','band']).reset_index(drop=True);anchors['nonoverlap60']=False
    for _,idxs in anchors.groupby(['universe','code']).groups.items():
        last=-10000
        for idx in idxs:
            cp=sessions[anchors.at[idx,'date']]
            if cp-last>60:anchors.at[idx,'nonoverlap60']=True;last=cp
    gaps=pd.read_csv(HERE/'reference/KNOWN_DISCONTINUITIES.csv',dtype={'code':str});issues=pd.read_csv(HERE/'reference/KNOWN_ANOMALIES.csv',dtype={'code':str})
    records=[];trade_rows=[];zone_rows=[];confirm_rows=[];traces=[];retry_rows=[];baseline_replays=[];cache={};started=time.time()
    for number,anchor in enumerate(anchors.to_dict('records')):
        code,date=anchor['code'],anchor['date'];d=ds[code];j=int(d.index[d.date.eq(date)][0]);cp=sessions[date];oid=f'{anchor["universe"]}:{code}:{date}:{anchor["band"]}'
        identity=dict(opportunity_id=oid,**anchor,year=int(date[:4]))
        if (code,date) not in cache:
            ctx,pool,chosen,body_chosen=context(d,j);ev,reasons,trace=detect(d,j,calendar,chosen)
            if body_chosen is not None:
                body_events,body_reasons,body_trace=detect(d,j,calendar,body_chosen)
                if 'C_RETEST' in body_events:ev['C_BROKEN_BODY']=body_events['C_RETEST']
                else:reasons['C_BROKEN_BODY']=body_reasons.get('C_RETEST','NO_BODY_RETEST')
                trace += [{**x,'zone_policy':'C_BROKEN_BODY'} for x in body_trace if x['state'] in ['FROZEN_ZONE_RETEST','RETEST_SUPPORT_NOT_HELD','C_SUPPORT_CONFIRMED']]
            else:reasons['C_BROKEN_BODY']='NO_NEWLY_CLEARED_BODY_ZONE'
            # Actual frozen R0 entry observations, not reconstructed future pivots.
            for e in events_by_p1.get((code,date),pd.DataFrame()).itertuples():
                k=int(d.index[d.date.eq(e.date)][0]);window=d.iloc[j+1:k+1];low=float(window.low.min());lowidx=int(window.index[window.low.eq(low)][-1])
                assert abs(low-e.pullback_low_known)<1e-7
                ev['R0_'+e.stage]=candidate(d,k,low,lowidx,'FROZEN_R0_OBSERVED_LOW')
            cache[(code,date)]=(ctx,pool,ev,reasons,trace,path(d,j,calendar))
        ctx,pool,ev,reasons,trace,paths=cache[(code,date)]
        first=candidate(d,j,np.nan,j,'FIRST_SIGNAL_NO_PULLBACK');decisions={'A_FIRST':first,**ev}
        initial_start=d.date.iloc[max(0,j-240)];end60=calendar[min(cp+60,len(calendar)-1)]
        qualities=dict(known_issue_initial=bool(((issues.code==code)&issues.date.between(initial_start,date)).any()),
            known_gap_lookback_through60=bool(((gaps.code==code)&gaps.date.between(initial_start,end60)).any()))
        records.append({**identity,**ctx,**paths,**qualities})
        for z in pool:zone_rows.append({**identity,'known_at':d.date.iloc[j-1],**z})
        for tr in trace:traces.append({**identity,'observed_date':tr['date'],**{k:v for k,v in tr.items() if k!='date'}})
        if anchor['universe']=='BASELINE_1804':
            ref=baseline[(baseline.code==code)&baseline.date.eq(date)&baseline.band.eq(anchor['band'])].iloc[0];k=int(d.index[d.date.eq(ref.gc_date)][0]);e=candidate(d,k,np.nan,k,'ORIGINAL_RSI_GC')
            assert bool(d['blue_'+anchor['band']].iloc[j]) and bool(d.rsi_gc.iloc[k])
            replay=simulate(d,j,e,calendar,'FIXED5','OLD20');assert replay['closed'] and abs(replay['net_pct']-ref.baseline_net_pct)<1e-8 and replay['exit_date']==ref.baseline_exit_date,(oid,replay)
            baseline_replays.append({**identity,'net_pct':replay['net_pct'],'exit_date':replay['exit_date']})
        for policy in ENTRIES:
            event=decisions.get(policy)
            if event:
                confirm_rows.append({**identity,'policy':policy,**event,'confirmation_age':sessions[event['confirm_date']]-cp})
            for stop in (['FIXED5'] if policy=='A_FIRST' else STOPS):
                for horizon in ['FRESH20','OLD20']:
                    if event is None:
                        why=reasons.get(policy,'NO_R0_STAGE')
                        status='NO_ENTRY_SIGNAL' if why not in ['RIGHT_CENSORED','MISSING_BAR'] else why
                        result=dict(status=status,entered=False,closed=False,no_entry_reason=why)
                    else:result=simulate(d,j,event,calendar,stop,horizon)
                    complete=paths.get('coverage60')=='COMPLETE' if horizon=='FRESH20' else paths.get('coverage20')=='COMPLETE'
                    noentry0=result['status'] in ['NO_ENTRY_SIGNAL','INVALID_STOP_AT_ENTRY','ENTRY_AFTER_DEADLINE']
                    row={**identity,**qualities,'policy':policy,'stop_mode':stop,'horizon':horizon,'opportunity_complete':complete,
                        **result,'opportunity_net_pct':result.get('net_pct',0.) if result['closed'] or noentry0 else np.nan,
                        'missed10':bool(not result['entered'] and complete and paths.get('peak60_pct' if horizon=='FRESH20' else 'peak20_pct',-np.inf)>=10),
                        'missed20':bool(not result['entered'] and complete and paths.get('peak60_pct' if horizon=='FRESH20' else 'peak20_pct',-np.inf)>=20)}
                    trade_rows.append(row)
                    if policy=='D_NEW_BLUE' and horizon=='FRESH20':
                        aggregate=dict(**identity,stop_mode=stop,first_status=result['status'],attempts=int(result['entered']),roundtrips=int(result['closed']),retry_confirm_date='',total_cost_pct_points=.25*int(result['closed']))
                        parts=[result['net_pct']] if result['closed'] else []
                        retry=None
                        if result['closed'] and result['reason'].startswith('SL'):
                            retry=retry_candidate(d,result['exit_date'],j,calendar)
                        if retry:
                            second=simulate(d,j,retry,calendar,stop,'FRESH20');aggregate.update(retry_confirm_date=retry['confirm_date'],retry_low_date=retry['low_date'],retry_stop_low=retry['stop_low'],retry_status=second['status'],
                                attempts=aggregate['attempts']+int(second['entered']),roundtrips=aggregate['roundtrips']+int(second['closed']))
                            aggregate.update({'retry_'+key:value for key,value in second.items() if key in ['entry_date','entry_price','exit_date','exit_price','reason','net_pct','stop_price','distance_pct']})
                            if second['closed']:parts.append(second['net_pct'])
                            elif second['status'] in ['RIGHT_CENSORED','MISSING_ENTRY_BAR','MISSING_HOLDING_BAR']:parts=[]
                        aggregate['total_cost_pct_points']=.25*aggregate['roundtrips'];aggregate['opportunity_complete']=complete
                        aggregate['net_compounded_pct']=100*(np.prod([1+x/100 for x in parts])-1) if parts and all(1+x/100>0 for x in parts) else 0. if noentry0 else np.nan
                        aggregate['overall_net_known']=bool(parts) or noentry0;retry_rows.append(aggregate)
        if (number+1)%400==0:print(json.dumps({'completed_opportunities':number+1,'total':len(anchors),'seconds':round(time.time()-started,1)}),flush=True)
    for name,data in [('OPPORTUNITIES',records),('TRADES',trade_rows),('FROZEN_ZONES',zone_rows),('CONFIRMATIONS',confirm_rows),('WAIT_TRACE',traces),('RETRY_MAX2',retry_rows),('BASELINE_REPLAY',baseline_replays)]:pd.DataFrame(data).to_csv(out/(name+'.csv.gz'),index=False)
    assert len(baseline_replays)==1804
    status=dict(baseline_opportunities=1804,independent_p1_opportunities=int((anchors.universe=='ALL_DAYS_P1').sum()),unique_anchor_charts=len(cache),baseline_replayed=1804,
        first_signal_resistance_frozen=True,stop_reset_only_at_new_confirmation=True,price_adjustment='UNVERIFIED',volume_profile='DAILY_PROXY_UNVERIFIED',independent_2025_2026='NOT_ACCESSED',performance_optimized=False)
    (out/'STATUS.json').write_text(json.dumps(status,indent=2),encoding='utf-8');print(json.dumps(status),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args().manifest)
