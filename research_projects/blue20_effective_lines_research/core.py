"""R0 observer. No outcomes, future pivots or baseline event labels in signals."""
import sys
from pathlib import Path
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parent/'vendor'))
from blue20_rsi_delay import wilder_rsi14
from multifactor import slow_stochastic
from blue20_threeway_audit import calculate_bands

def indicators(d,ma448=None):
    d=calculate_bands(d.copy()).reset_index(drop=True)
    for n in [5,20,60,112,224]:
        d[f'ma{n}']=d.close.rolling(n,min_periods=n).mean();d[f'slope{n}']=100*(d[f'ma{n}']/d[f'ma{n}'].shift(10)-1)
    d['ma448']=d.date.map(ma448) if ma448 is not None else np.nan
    d['rsi']=wilder_rsi14(d.close);d['rsi_signal']=d.rsi.rolling(9,min_periods=9).mean()
    d['rsi_gc']=(d.rsi.shift().le(d.rsi_signal.shift())&d.rsi.gt(d.rsi_signal)).fillna(False)
    d['slow_k'],d['slow_d'],d['slow_gc']=slow_stochastic(d)
    prev=d.close.shift();tr=pd.concat([d.high-d.low,(d.high-prev).abs(),(d.low-prev).abs()],axis=1).max(axis=1)
    d['prior_atr20']=tr.rolling(20,min_periods=20).mean().shift()
    d['amount_ratio20']=d.amount/d.amount.rolling(20,min_periods=20).mean().shift()
    d['volume_ratio20']=d.volume/d.volume.rolling(20,min_periods=20).mean().shift()
    for field in ['volume','amount']:
        d[field+'_gradual_ratio']=d[field].rolling(5,min_periods=5).mean()/d[field].shift(5).rolling(20,min_periods=20).mean()
    d['contraction']=tr.rolling(5,min_periods=5).mean()/tr.shift(5).rolling(20,min_periods=20).mean()
    d['range20_pct']=100*(d.high.rolling(20).max()-d.low.rolling(20).min())/d.close
    d['close_ma224_pct']=100*(d.close/d.ma224-1)
    d['body']=d.close-d.open;d['body_atr']=d.body/d.prior_atr20
    d['close_location']=(d.close-d.low)/(d.high-d.low).replace(0,np.nan)
    d['upper_wick']=(d.high-d[['open','close']].max(axis=1))/(d.high-d.low).replace(0,np.nan)
    for band in ['UPPER','LOWER']:
        line=d[band];body=d.body
        d['blue_'+band]=(line.lt(line.shift())&prev.le(line.shift())&d.open.lt(line)&d.close.gt(line)&body.gt(0)&
            ((line-d.open)/body).ge(.2)&((d.close-line)/body).ge(.2)&(100*body/d.open).ge(.5)).fillna(False)
    d['ma_alignment']=np.where(d.ma224.isna(),'UNKNOWN',np.where((d.ma60<d.ma112)&(d.ma112<d.ma224),'INVERSE',np.where((d.ma60>d.ma112)&(d.ma112>d.ma224),'BULL','MIXED')))
    d['video_alignment']=np.where(d.ma448.isna(),'UNKNOWN',np.where((d.ma112<d.ma224)&(d.ma224<d.ma448),'INVERSE','OTHER'))
    d['p0_condition']=(d.ma224.notna()&d.close.le(1.05*d.ma224)&d.range20_pct.le(20)&d.contraction.le(.9)&
        d.volume_gradual_ratio.ge(1.1)&d.amount_gradual_ratio.ge(1.1)&(d.rsi.gt(d.rsi.shift(3))|d.slow_k.gt(d.slow_d))).fillna(False)
    raw=(d.body_atr.ge(1.5)&(100*d.body/d.open).ge(3)&d.volume_ratio20.ge(2)&d.amount_ratio20.ge(2)&d.close_location.ge(.7)).fillna(False)
    d['p1_raw']=raw;d['p1_signal']=raw&~raw.shift().fillna(False).rolling(20,min_periods=1).max().astype(bool)
    # P0 must precede, never coincide with or immediately follow, an observed P1-type impulse.
    d['p0_condition']&=~raw&~raw.shift().fillna(False).rolling(20,min_periods=1).max().astype(bool)
    d['prior_high5']=d.high.rolling(5,min_periods=5).max().shift();d['prior_amount5']=d.amount.rolling(5,min_periods=5).mean().shift()
    d['large_gap']=(d.open/prev-1).abs().gt(.35)
    return d

def observe(d):
    """Generate P0 independently, all first P1s, then causal P2/P3 observations."""
    signals=[];episodes=[];daily=[];lastp0=-10000
    def event(stage,i,p1=-1,extra=None):
        b=d.iloc[i];sid=f'{b.code}:{b.date}:{stage}'+(':'+d.date.iloc[p1] if stage in ['P2','P3'] else '')
        r=dict(signal_id=sid,code=b.code,date=b.date,year=int(b.date[:4]),stage=stage,index=i,session=int(b.session),p1_date=d.date.iloc[p1] if p1>=0 else '',p1_index=p1)
        if extra:r.update(extra)
        signals.append(r);return r
    for i,b in d.iterrows():
        if not ('2023-01-01'<=b.date<='2024-12-31'):continue
        previous=bool(d.p0_condition.iloc[i-1]) if i else False
        if b.p0_condition and not previous and b.session-lastp0>=20:
            event('P0',i);lastp0=b.session
        if b.p1_signal:event('P1',i,i)
    p0s=[s for s in signals if s['stage']=='P0'];p1s=[s for s in signals if s['stage']=='P1']
    for p in p0s:
        candidates=[x for x in p1s if 1<=x['session']-p['session']<=20]
        reached=min(candidates,key=lambda x:x['session']) if candidates else None
        complete=int(d.session.iloc[-1])-p['session']>=20
        missing=complete and len(d[d.session.between(p['session']+1,p['session']+20)])<20
        episodes.append(dict(episode_id=p['signal_id'],code=p['code'],origin_stage='P0',origin_date=p['date'],p1_date=reached['date'] if reached else '',p2_date='',p3_date='',
            final_status='P1_REACHED' if reached else 'MISSING_BARS_UNVERIFIED' if missing else 'NO_P1_20D' if complete else 'RIGHT_CENSORED',days_to_p1=reached['session']-p['session'] if reached else np.nan,known_date=reached['date'] if reached else d.date.iloc[min(len(d)-1,p['index']+20)]))
    for p in p1s:
        j=p['index'];base=d.iloc[j];p2=None;p3=None;runninghigh=base.high;pullbacklow=np.inf;terminal=None;known=''
        for i in range(j+1,len(d)):
            b=d.iloc[i];age=int(b.session-base.session)
            if age>40:break
            if b.session-d.session.iloc[i-1]>1:
                terminal='MISSING_BARS_UNVERIFIED';known=b.date;break
            if b.close<base.low:
                terminal='P2_BASE_LOW_FAILED' if p2 else 'P1_BASE_LOW_FAILED';known=b.date
                daily.append(dict(code=base.code,p1_date=base.date,date=b.date,age=age,state=terminal,known_low=min(pullbacklow,b.low)));break
            priorlow=pullbacklow
            runninghigh=max(runninghigh,b.high);pullbacklow=min(pullbacklow,b.low)
            state='P1_WAIT_PULLBACK' if p2 is None else 'P2_WAIT_REBOUND'
            if p2 is None and age<=20 and (1-b.low/runninghigh)>=.03 and abs(b.body)<=.5*base.body and b.volume<=.6*base.volume:
                p2=event('P2',i,j,dict(pullback_low_known=pullbacklow,reference_open=base.open,reference_low=base.low,reference_mid=(base.open+base.close)/2));state='P2_OBSERVED'
            elif p2 is not None and b.session>p2['session'] and b.low>=priorlow and b.body>0 and b.amount>=1.2*b.prior_amount5 and b.close>b.prior_high5:
                p3=event('P3',i,j,dict(pullback_low_known=priorlow,reference_open=base.open,reference_low=base.low,reference_mid=(base.open+base.close)/2));state='P3_CONFIRMED';terminal='P3_REACHED';known=b.date
            daily.append(dict(code=base.code,p1_date=base.date,date=b.date,age=age,state=state,known_low=pullbacklow))
            if p3:break
            if p2 is None and age>=20:terminal='NO_P2_20D';known=b.date;break
        if terminal is None:
            complete=int(d.session.iloc[-1]-base.session)>=40
            terminal='NO_P3_40D' if complete else 'RIGHT_CENSORED';known=d.date.iloc[min(len(d)-1,j+40)]
        episodes.append(dict(episode_id=p['signal_id'],code=p['code'],origin_stage='P1',origin_date=p['date'],p1_date=p['date'],p2_date=p2['date'] if p2 else '',p3_date=p3['date'] if p3 else '',final_status=terminal,days_to_p1=0,known_date=known))
    return signals,episodes,daily

def trade(d,j,expiry_date,calendar):
    """Next MARKET session open, daily stop first, gaps at actual open."""
    date=d.date.iloc[j];cp=calendar.get_loc(date)
    if cp+1>=len(calendar):return dict(trade_status='RIGHT_CENSORED')
    entry_date=calendar[cp+1]
    loc={v:i for i,v in enumerate(d.date)}
    if entry_date not in loc:return dict(trade_status='MISSING_ENTRY_BAR')
    price=float(d.open.iloc[loc[entry_date]])
    if expiry_date is None:return dict(trade_status='RIGHT_CENSORED',entry_date=entry_date,entry_price=price)
    if entry_date>expiry_date:return dict(trade_status='ENTRY_AFTER_DEADLINE')
    sl=price*.95;tp=price*1.1;days=calendar[cp+1:calendar.get_loc(expiry_date)+1]
    def pack(day,px,reason):return dict(trade_status='CLOSED',entry_date=entry_date,entry_price=price,exit_date=day,exit_price=float(px),reason=reason,net_pct=100*(px/price-1)-.25)
    for k,day in enumerate(days):
        if day not in loc:return dict(trade_status='MISSING_HOLDING_BAR',entry_date=entry_date,entry_price=price)
        b=d.iloc[loc[day]]
        if k and b.open<=sl:return pack(day,b.open,'SL_GAP')
        if k and b.open>=tp:return pack(day,b.open,'TP_GAP')
        if b.low<=sl:return pack(day,sl,'SL_BOTH_HIT' if b.high>=tp else 'SL')
        if b.high>=tp:return pack(day,tp,'TP')
    return pack(expiry_date,float(d.close.iloc[loc[expiry_date]]),'TIME_D20')

def outcomes(d,j,calendar):
    cp=calendar.get_loc(d.date.iloc[j]);loc={v:i for i,v in enumerate(d.date)}
    if cp+1>=len(calendar) or calendar[cp+1] not in loc:return {'path_status':'MISSING_NEXT_SESSION'}
    price=d.open.iloc[loc[calendar[cp+1]]];result={'path_status':'OBSERVED','path_entry_date':calendar[cp+1],'path_entry_price':price}
    for h in [5,10,20,40,60]:
        if cp+h>=len(calendar):result[f'coverage_{h}']='RIGHT_CENSORED';continue
        days=calendar[cp+1:cp+h+1]
        if not all(v in loc for v in days):result[f'coverage_{h}']='MISSING_BAR';continue
        w=d.iloc[[loc[v] for v in days]]
        result.update({f'coverage_{h}':'COMPLETE',f'return_{h}':100*(w.close.iloc[-1]/price-1),f'max_up_{h}':100*(w.high.max()/price-1),f'max_down_{h}':100*(w.low.min()/price-1)})
    return result
