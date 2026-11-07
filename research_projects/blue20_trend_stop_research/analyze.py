"""Frozen-line exploratory study; no production strategy imports or mutations."""
from pathlib import Path
import argparse, sys, json,hashlib
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
import importlib.util
def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
loader=module('support_loader',HERE/'vendor/loader.py' if (HERE/'vendor/loader.py').exists() else ROOT/'BLUE20_STRUCTURE_VALIDATION/analyze.py')
structure=module('price_structure',HERE/'vendor/structure.py' if (HERE/'vendor/structure.py').exists() else ROOT/'BLUE20_INTERACTION/structure.py')

def lows(d,j):
    start=max(0,j-120)
    points=[i for i in range(start+2,j-2) if d.low.iloc[i]<d.low.iloc[i-2:i].min() and d.low.iloc[i]<=d.low.iloc[i+1:i+3].min()]
    valid=[i for i in points if i>=j-60 and d.low.iloc[i+1:j].min()>=d.low.iloc[i]]
    if not valid:return dict(pullback=np.nan,origin=np.nan,chain=0,pivot_date='',known_date='',origin_date='')
    latest=valid[-1];chain=[latest]
    for i in reversed([p for p in points if p<latest]):
        if d.low.iloc[i]>=d.low.iloc[chain[-1]]:break
        chain.append(i)
    return dict(pullback=float(d.low.iloc[latest]),origin=float(d.low.iloc[chain[-1]]) if len(chain)>1 else np.nan,
                chain=len(chain),pivot_date=d.date.iloc[latest],known_date=d.date.iloc[latest+2],origin_date=d.date.iloc[chain[-1]])

def simulate(d,j,entry,line,close_stop=False):
    price=float(d.open.iloc[entry]);tp=price*1.1;end=j+20;pending=False
    def result(i,px,reason):
        path=d.iloc[entry:i+1];peak=100*(path.high.max()/price-1);mae=100*(path.low.min()/price-1)
        # Daily close equity drawdown including actual liquidation; not intraday portfolio MDD.
        equity=np.r_[price,path.close.iloc[:-1].to_numpy(),px];dd=100*np.min(equity/np.maximum.accumulate(equity)-1)
        return dict(exit_date=d.date.iloc[i],exit_price=float(px),reason=reason,net=100*(px/price-1)-.25,
                    peak_until_exit=peak,mae_until_exit=mae,close_mdd=dd)
    for i in range(entry,end+1):
        b=d.iloc[i]
        if pending:return result(i,b.open,'CLOSE_BREAK_NEXT_OPEN')
        if i>entry:
            if not close_stop and b.open<=line:return result(i,b.open,'SL_GAP')
            if b.open>=tp:return result(i,b.open,'TP_GAP')
        if not close_stop and b.low<=line:return result(i,line,'SL_BOTH_HIT' if b.high>=tp else 'SL')
        if b.high>=tp:return result(i,tp,'TP')
        if close_stop and b.close<line:pending=True
    return result(end,d.close.iloc[end],'TIME_D20')

def credibility(d,j):
    h=d.iloc[max(0,j-120):j];discovery=h.iloc[:80];validation=h.iloc[80:]
    if len(h)!=120:return []
    candidates,_=structure.zones(discovery,80)
    active=[z for z in candidates if z['low']>discovery.close.iloc[-1]]
    if not active:return []
    z=min(active,key=lambda z:(z['low']-discovery.close.iloc[-1],-z['count'],z['anchor']))
    trials=[];last=-100
    for k in range(len(validation)):
        b=validation.iloc[k];prev=h.close.iloc[79+k]
        if prev>=z['low'] or b.high<z['low'] or k-last<5:continue
        last=k
        future=validation.iloc[k:min(k+4,len(validation))]
        if len(future)<4:outcome='PENDING'
        elif future.close.gt(z['high']).any():outcome='BREAK'
        elif future.close.min()<z['low'] and (b.high-future.close.min())/b.prior_atr20>=.5:outcome='REJECT'
        else:outcome='OTHER'
        trials.append(dict(discovery_tests=z['count'],discovery_drop_atr=z['median_drop_atr'],zone_low=z['low'],zone_high=z['high'],
                           discovery_cutoff=discovery.date.iloc[-1],touch_date=b.date,outcome=outcome))
    return trials

def main(args):
    out=HERE/'results';out.mkdir(exist_ok=True)
    frames=loader.load_prices(Path(args.manifest))
    frozen_file=HERE/'reference/FROZEN.json'
    if frozen_file.exists():
        frozen=json.loads(frozen_file.read_text(encoding='utf-8'))
        assert set(frames)==set(frozen['cohort_codes'])
        for code,digest in frozen['event_stock_prices'].items():
            raw=frames[code].to_csv(index=False,lineterminator='\n').encode('utf-8-sig')
            assert hashlib.sha256(raw).hexdigest()==digest,('NORMALIZED_PRICE_CHANGED',code)
    for d in frames.values():
        prev=d.close.shift();tr=pd.concat([d.high-d.low,(d.high-prev).abs(),(d.low-prev).abs()],axis=1).max(axis=1)
        d['prior_atr20']=tr.rolling(20).mean().shift()
    refs=HERE/'reference'
    if refs.exists():
        for name,digest in json.loads((refs/'SHA256.json').read_text()).items():
            assert hashlib.sha256((refs/name).read_bytes()).hexdigest()==digest,name
    baseline=pd.read_csv(refs/'BASELINE.csv.gz' if refs.exists() else ROOT/'BLUE20_VP_AUDIT/run0/results/EVENTS.csv.gz',dtype={'code':str,'date':str})
    zones=pd.read_csv(refs/'ZONES.csv.gz' if refs.exists() else ROOT/'BLUE20_ACTIONS_42369445361/results/EVENTS.csv.gz',dtype={'code':str,'date':str})
    zones=zones[zones.window.eq(120)].set_index(['code','date','band'])
    assert len(baseline)==1804 and not baseline.duplicated(['code','date','band']).any()
    assert sorted(baseline.groupby('band').size())==[834,970]
    rows=[];features=[];touches=[];replays=[]
    for e in baseline.itertuples(index=False):
        d=frames[e.code];j=int(d.index[d.date.eq(e.date)][0]);z=zones.loc[(e.code,e.date,e.band)];f=lows(d,j)
        ident=dict(code=e.code,date=e.date,band=e.band,year=e.year,gc_offset=e.gc_offset,long_bull=z.long_bull)
        feat={**ident,**f,'resistance_state':z.state,'resistance_low':z.low,'resistance_high':z.high,'has_known7':z.has_known7,'nonoverlap':z.nonoverlap}
        for name in ['pullback','origin']:
            line=f[name];future=d.iloc[j+1:j+21]
            feat[name+'_d0_low_hold']=bool(d.low.iloc[j]>=line) if np.isfinite(line) else None
            feat[name+'_close_hold20']=float(future.close.ge(line).mean()) if np.isfinite(line) else np.nan
            for field in ['low','close']:
                breach=future[future[field]<line]
                feat[name+'_'+field+'_breach_date']=breach.date.iloc[0] if len(breach) else ''
        features.append(feat)
        for t in credibility(d,j):touches.append({**ident,**t})
        for timing in ['D1','ORIGINAL_GC']:
            entry=j+1 if timing=='D1' else int(d.index[d.date.eq(e.entry_date)][0]);price=d.open.iloc[entry]
            atr=d.prior_atr20.iloc[j]
            paths=d.iloc[entry:j+21];mfe=100*(paths.high.max()/price-1)
            levels={'FIXED5':price*.95,'RESISTANCE':z.low if z.state in ['BREAK_WEAK','BREAK_STRONG','ALREADY_CLEARED'] else np.nan,
                    'PULLBACK':f['pullback'],'ORIGIN':f['origin']}
            for name,raw in levels.items():
                for buffer in ([0] if name=='FIXED5' else [0,.25]):
                    line=raw-buffer*atr
                    for mode in (['INTRADAY'] if name=='FIXED5' else ['INTRADAY','CLOSE']):
                        valid=np.isfinite(line) and 0<line<price
                        row={**ident,'timing':timing,'stop':name,'buffer_atr':buffer,'mode':mode,'eligible':valid,
                             'entry_date':d.date.iloc[entry],'entry_price':price,'line':line,'distance_pct':100*(1-line/price),
                             'd20_peak_pct':mfe,'miss10':not valid and mfe>=10,'miss20':not valid and mfe>=20,
                             'has_known7':z.has_known7,'nonoverlap':z.nonoverlap}
                        if valid:
                            result=simulate(d,j,entry,line,mode=='CLOSE');row.update(result);row['net_R']=result['net']/row['distance_pct']
                            row['example_weight']=min(1,1/row['distance_pct']);row['example_account_net']=row['example_weight']*result['net']
                            if timing=='ORIGINAL_GC' and name=='FIXED5':
                                assert abs(result['net']-e.baseline_net_pct)<1e-8,(ident,result,e.baseline_net_pct)
                                assert result['exit_date']==e.baseline_exit_date
                                replays.append(ident)
                        rows.append(row)
    trades=pd.DataFrame(rows);ft=pd.DataFrame(features);tt=pd.DataFrame(touches)
    trades.to_csv(out/'TRADES.csv.gz',index=False);ft.to_csv(out/'TREND_FEATURES.csv.gz',index=False);tt.to_csv(out/'FROZEN_RESISTANCE_TOUCHES.csv.gz',index=False)
    summaries=[]
    subsets={'ALL':trades,'GC0':trades[trades.gc_offset.eq(0)],'LONG_BULL':trades[trades.long_bull],'CLEAN':trades[~trades.has_known7], 'NONOVERLAP':trades[trades.nonoverlap]}
    # Fixed pair set for exact, intraday structural alternatives; other variants retain their own eligibility.
    common=trades[(trades.buffer_atr.eq(0)) & trades['mode'].eq('INTRADAY')].groupby(['code','date','band','timing']).eligible.all()
    keys=set(common[common].index)
    subsets['COMMON_ALL4']=trades[[tuple(x) in keys for x in trades[['code','date','band','timing']].to_numpy()]]
    for subset,t in subsets.items():
        for (timing,stop,buffer,mode,year,band),g in t.groupby(['timing','stop','buffer_atr','mode','year','band']):
            a=g[g.eligible];summaries.append(dict(subset=subset,timing=timing,stop=stop,buffer_atr=buffer,mode=mode,year=year,band=band,
                opportunities=len(g),entries=len(a),entry_rate=100*len(a)/len(g),mean_net=a.net.mean(),median_net=a.net.median(),
                stop_rate=100*a.reason.str.contains('SL|CLOSE_BREAK').mean(),tp_rate=100*a.reason.str.startswith('TP').mean(),
                mean_distance=a.distance_pct.mean(),mean_R=a.net_R.mean(),mean_example_account_net=a.example_account_net.mean(),mean_close_mdd=a.close_mdd.mean(),mean_d20_peak=g.d20_peak_pct.mean(),
                missed10=g.miss10.sum(),missed20=g.miss20.sum()))
    pd.DataFrame(summaries).to_csv(out/'SUMMARY.csv',index=False)
    (out/'VALIDATION.json').write_text(json.dumps({'events':len(baseline),'baseline_exact_replays':len(replays),'source_limit':'Daily estimates; adjustment basis and real tick volume UNVERIFIED'},indent=2),encoding='utf-8')
    print(json.dumps({'events':len(baseline),'trades':len(trades),'replayed':len(replays),'validation_touches':len(tt)}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args())
