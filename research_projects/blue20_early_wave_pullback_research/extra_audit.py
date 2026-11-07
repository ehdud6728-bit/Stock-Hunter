from pathlib import Path
import argparse,json
import numpy as np
import pandas as pd
from study import HERE,load_prices,volume_profile
from core import trade,outcomes

def main(manifest):
    frames=load_prices(Path(manifest));calendar=pd.Index(sorted(set(v for d in frames.values() for v in d.date)))
    out=HERE/'results';s=pd.read_csv(out/'SIGNALS.csv.gz',dtype={'code':str});base=pd.read_csv(HERE/'reference/BASELINE.csv.gz',dtype={'code':str});episodes=pd.read_csv(out/'EPISODES.csv.gz',dtype={'code':str})
    # Fixed descriptive stability screen, not a trading filter or certificate of true sell volume.
    evidence=[]
    for r in s.itertuples():
        d=frames[r.code];j=int(d.index[d.date.eq(r.date)][0]);record=dict(signal_id=r.signal_id,code=r.code,date=r.date,stage=r.stage,price_strong_overhead=r.strong_overhead)
        for kind,end in [('PAST_ONLY',j),('SIGNAL_CLOSE',j+1)]:
            w=d.iloc[max(0,end-120):end]
            if len(w)<120 or w.volume.sum()<=0:record[kind+'_volume_evidence']='UNKNOWN';continue
            a=volume_profile(w,32,'range');b=volume_profile(w,32,'typical')
            if a is None or b is None:record[kind+'_volume_evidence']='UNKNOWN';continue
            centers=a['centers'];width=a['width'];counts=[]
            for i in range(32):
                local=(i==0 or a['weights'][i]>a['weights'][i-1]) and (i==31 or a['weights'][i]>=a['weights'][i+1])
                if local and centers[i]-width/2>r.close and a['weights'][i]/a['weights'].sum()>=2/32 and b['weights'][i]/b['weights'].sum()>=1.5/32:counts.append(i)
            record[kind+'_volume_evidence']='MODEL_STABLE_OVERHEAD_CANDIDATE' if counts else 'NONE_DETECTED_NOT_PROOF_OF_ABSENCE'
            record[kind+'_candidate_count']=len(counts)
        record['overall_evidence']='PRESENT' if record['price_strong_overhead']=='EVIDENCE_PRESENT' or record.get('PAST_ONLY_volume_evidence')=='MODEL_STABLE_OVERHEAD_CANDIDATE' else 'UNKNOWN_OR_NOT_DETECTED'
        record['actual_sell_inventory']='UNVERIFIED';evidence.append(record)
    pd.DataFrame(evidence).to_csv(out/'VOLUME_RESISTANCE_EVIDENCE.csv.gz',index=False)
    # Refresh only trade bookkeeping under the documented entry-vs-closed distinction.
    trades=[]
    for r in s.itertuples():
        d=frames[r.code];j=int(d.index[d.date.eq(r.date)][0]);cp=calendar.get_loc(r.date);deadline=calendar[cp+20] if cp+20<len(calendar) else None
        result=trade(d,j,deadline,calendar)
        trades.append(dict(signal_id=r.signal_id,code=r.code,date=r.date,year=r.year,stage=r.stage,policy='STAGE_PLUS20',nonoverlap60=r.nonoverlap60,quality_gap_through20=r.quality_gap_through20,
                           **result,entered='entry_date' in result,closed=result['trade_status']=='CLOSED'))
    pd.DataFrame(trades).to_csv(out/'TRADES.csv.gz',index=False)
    p1op=pd.read_csv(out/'P1_SAME_OPPORTUNITIES.csv.gz',dtype={'code':str});refreshed=[]
    emap=episodes[episodes.origin_stage.eq('P1')].set_index('episode_id')
    for r in p1op.to_dict('records'):
        er=emap.loc[r['opportunity_id']];d=frames[r['code']];cp=calendar.get_loc(r['p1_date']);deadline=calendar[cp+20] if cp+20<len(calendar) else None
        date=er.p1_date if r['policy']=='P1' else er.p2_date if r['policy']=='P2' else er.p3_date
        result={'trade_status':'NO_STAGE_SIGNAL'} if pd.isna(date) or date=='' else trade(d,int(d.index[d.date.eq(date)][0]),deadline,calendar)
        for key in ['trade_status','entry_date','entry_price','exit_date','exit_price','reason','net_pct']:r.pop(key,None)
        r.update(result);r['entered']='entry_date' in result;r['closed']=result['trade_status']=='CLOSED'
        r['opportunity_net_cash']=result.get('net_pct',0.) if result['trade_status'] in ['CLOSED','NO_STAGE_SIGNAL','ENTRY_AFTER_DEADLINE'] else np.nan
        r['missed10']=not r['entered'] and r['original_max_up20']>=10;r['missed20']=not r['entered'] and r['original_max_up20']>=20;refreshed.append(r)
    pd.DataFrame(refreshed).to_csv(out/'P1_SAME_OPPORTUNITIES.csv.gz',index=False)
    p1=episodes[episodes.origin_stage.eq('P1')].set_index(['code','origin_date']);paired=[]
    for e in base.itertuples():
        d=frames[e.code];j=int(d.index[d.date.eq(e.date)][0]);cp=calendar.get_loc(e.date);deadline=calendar[cp+20];path=outcomes(d,j,calendar)
        old=dict(code=e.code,date=e.date,year=e.year,band=e.band,gc_offset=e.gc_offset,baseline_net=e.baseline_net_pct,baseline_exit_date=e.baseline_exit_date,original_path_up20=path.get('max_up_20',np.nan))
        er=p1.loc[(e.code,e.date)] if (e.code,e.date) in p1.index else None
        for stage in ['P1','P2','P3']:
            date=(e.date if stage=='P1' else er.p2_date if stage=='P2' else er.p3_date) if er is not None else None
            if pd.isna(date) or not date:result=dict(trade_status='NO_STAGE_SIGNAL')
            else:result=trade(d,int(d.index[d.date.eq(date)][0]),deadline,calendar)
            entered='entry_date' in result;closed=result['trade_status']=='CLOSED';row={**old,'policy':stage,'has_p1_same_day':er is not None,**result,'entered':entered,'closed':closed,
                'net_if_cash_else_zero':result['net_pct'] if closed else 0. if result['trade_status'] in ['NO_STAGE_SIGNAL','ENTRY_AFTER_DEADLINE'] else np.nan,
                'missed10':not entered and path.get('max_up_20',-np.inf)>=10,'missed20':not entered and path.get('max_up_20',-np.inf)>=20}
            paired.append(row)
    pd.DataFrame(paired).to_csv(out/'BASELINE_IDENTICAL_OPPORTUNITIES.csv.gz',index=False)
    # Causal BLUE20 formula on ALL days is distinct from the future-RSI-selected 1,804 reference cohort.
    days=pd.read_csv(out/'ALL_STOCK_DAYS.csv.gz',dtype={'code':str});leads=[]
    blue_groups={code:g for code,g in days[days.blue_UPPER|days.blue_LOWER].groupby('code')}
    for r in s.itertuples():
        g=blue_groups.get(r.code,days.iloc[:0]);part=g[g.session.ge(r.session)]
        first=part.iloc[0] if len(part) else None;lead=int(first.session-r.session) if first is not None else None
        leads.append(dict(signal_id=r.signal_id,code=r.code,date=r.date,stage=r.stage,year=r.year,next_blue_date=first.date if lead is not None and lead<=60 else '',
                          lead_sessions=lead if lead is not None and lead<=60 else np.nan,coverage60='COMPLETE' if days.session.max()-r.session>=60 else 'RIGHT_CENSORED'))
    pd.DataFrame(leads).to_csv(out/'LEAD_COMPARISONS.csv.gz',index=False)
    lineages=[];chains=[];p0=s[s.stage.eq('P0')]
    for r in s[s.stage.eq('P1')].itertuples():
        prior=p0[p0.code.eq(r.code)&p0.session.between(r.session-20,r.session-1)].sort_values('session')
        lineages.append(dict(p1_signal_id=r.signal_id,code=r.code,p1_date=r.date,prior_p0_date=prior.date.iloc[-1] if len(prior) else '',origin='P0_PRECEDED' if len(prior) else 'INDEPENDENT_P1'))
    pd.DataFrame(lineages).to_csv(out/'P1_P0_LINEAGE.csv.gz',index=False)
    em=episodes[episodes.origin_stage.eq('P1')].set_index(['code','origin_date'])
    for r in episodes[episodes.origin_stage.eq('P0')].itertuples():
        record=r._asdict();record.pop('Index',None);record['p1_status']='NO_P1'
        if pd.notna(r.p1_date) and (r.code,r.p1_date) in em.index:
            tail=em.loc[(r.code,r.p1_date)];record.update(p2_date=tail.p2_date,p3_date=tail.p3_date,p1_status=tail.final_status)
        chains.append(record)
    pd.DataFrame(chains).to_csv(out/'P0_CHAIN_OUTCOMES.csv.gz',index=False)
    print(json.dumps({'signal_evidence_rows':len(evidence),'baseline_opportunity_rows':len(paired),'strong_volume_policy':'DESCRIPTIVE_ONLY_NOT_TRADING_FILTER'}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args().manifest)
