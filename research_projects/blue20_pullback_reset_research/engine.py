"""Causal wait-for-entry observations and immutable pullback stops."""
import numpy as np

def candidate(d,k,low,low_idx,kind,**extra):
    return dict(confirm_date=d.date.iloc[k],confirm_index=int(k),stop_low=float(low),low_date=d.date.iloc[low_idx],low_known_by=d.date.iloc[k],
                atr=float(d.prior_atr20.iloc[k]),kind=kind,**extra)

def detect(d,j,calendar,zone=None):
    pos={day:i for i,day in enumerate(d.date)};start=calendar.get_loc(d.date.iloc[j]);events={};trace=[]
    initial=d.iloc[j];peak=float(initial.high);pull=False;dry_day=None;minimum=np.inf;minimum_idx=-1;first_failed=False
    break_day=start if zone is not None and initial.close>zone['high'] else None
    retest_day=None;retest_min=np.inf;retest_idx=-1;above_count=0;blue_queue=[]
    end=min(start+40,len(calendar)-1);data_gap=False
    for cp in range(start+1,end+1):
        day=calendar[cp]
        if day not in pos:data_gap=True;trace.append(dict(date=day,state='UNVERIFIED_MISSING_BAR'));break
        k=pos[day];b=d.iloc[k];age=cp-start;old_low=minimum
        # Today's low is compared to a peak already known before today's bar.
        if not pull and b.low<=.97*peak:pull=True;trace.append(dict(date=day,state='PULLBACK_OBSERVED'))
        if pull and b.low<minimum:minimum=float(b.low);minimum_idx=k
        dry=bool(pull and abs(b.body)<=.5*abs(initial.body) and b.volume<=.6*initial.volume)
        if b.close<initial.low and not first_failed:first_failed=True;trace.append(dict(date=day,state='ORIGINAL_BASE_CLOSE_FAILED'))
        if ('B_RECOVERY' not in events and not first_failed and pull and dry_day is not None and cp>dry_day and b.low>=old_low
            and b.body>0 and b.amount>=1.2*b.prior_amount5 and b.close>b.prior_high5):
            events['B_RECOVERY']=candidate(d,k,minimum,minimum_idx,'OBSERVED_PULLBACK_LOW');trace.append(dict(date=day,state='B_CONFIRMED'))
        if zone is not None and 'C_RETEST' not in events and not first_failed:
            hi,lo=zone['high'],zone['low']
            if break_day is None:
                if b.close>hi:break_day=cp;trace.append(dict(date=day,state='FROZEN_ZONE_CLOSE_BROKEN'))
            elif cp>break_day:
                tol=.25*float(b.prior_atr20)
                if retest_day is None and lo<=b.low<=hi+tol and b.close>hi:
                    retest_day=cp;retest_min=float(b.low);retest_idx=k;above_count=1;trace.append(dict(date=day,state='FROZEN_ZONE_RETEST'))
                elif retest_day is not None:
                    if b.close<=hi:
                        trace.append(dict(date=day,state='RETEST_SUPPORT_NOT_HELD'));break_day=None;retest_day=None;above_count=0
                    else:
                        above_count+=1
                        if b.low<retest_min:retest_min=float(b.low);retest_idx=k
                        if above_count>=2:
                            events['C_RETEST']=candidate(d,k,retest_min,retest_idx,'RETEST_OBSERVED_LOW');trace.append(dict(date=day,state='C_SUPPORT_CONFIRMED'))
                elif b.close<=hi:break_day=None
        blue=bool(b.blue_UPPER or b.blue_LOWER)
        if age>=3 and pull and blue:
            blue_queue.append((cp,k));trace.append(dict(date=day,state='NEW_BLUE_AFTER_PULLBACK'))
            if 'D_NEW_BLUE' not in events:events['D_NEW_BLUE']=candidate(d,k,minimum,minimum_idx,'OBSERVED_PULLBACK_LOW',new_cycle=first_failed,blue_date=day,followup_gc_offset=None)
        blue_queue=[x for x in blue_queue if cp-x[0]<=10]
        if blue_queue and b.rsi_gc and 'D_BLUE_RSI' not in events:
            source_cp,source_k=blue_queue[0]
            events['D_BLUE_RSI']=candidate(d,k,minimum,minimum_idx,'OBSERVED_PULLBACK_LOW',new_cycle=first_failed,blue_date=d.date.iloc[source_k],followup_gc_offset=cp-source_cp)
            trace.append(dict(date=day,state='NEW_BLUE_RSI_CONFIRMED'))
        if dry and dry_day is None:dry_day=cp
        peak=max(peak,float(b.high))
    complete=not data_gap and start+40<len(calendar)
    reasons={name:('NO_ACTIVE_FROZEN_ZONE' if name=='C_RETEST' and zone is None else 'ORIGINAL_BASE_FAILED' if name in ['B_RECOVERY','C_RETEST'] and first_failed else 'NO_CONFIRMATION_40D' if complete else 'MISSING_BAR' if data_gap else 'RIGHT_CENSORED') for name in ['B_RECOVERY','C_RETEST','D_NEW_BLUE','D_BLUE_RSI'] if name not in events}
    return events,reasons,trace

def last_fresh_pivot(d,k,earliest_idx):
    """Two later bars must already exist at confirmation k; no undercut since."""
    for q in range(k-2,max(earliest_idx, k-20)-1,-1):
        if q<2:continue
        price=d.low.iloc[q]
        if price<d.low.iloc[q-2:q].min() and price<=d.low.iloc[q+1:q+3].min() and d.low.iloc[q+1:k+1].min()>=price:return float(price),q
    return None

def retry_candidate(d,first_exit,anchor_idx,calendar):
    pos={day:i for i,day in enumerate(d.date)};start=calendar.get_loc(d.date.iloc[anchor_idx]);exit_cp=calendar.get_loc(first_exit)
    if first_exit not in pos:return None
    for cp in range(exit_cp,min(start+40,len(calendar)-1)+1):
        day=calendar[cp]
        if day not in pos:return None
        k=pos[day];b=d.iloc[k]
        if not (b.blue_UPPER or b.blue_LOWER):continue
        found=last_fresh_pivot(d,k,pos[first_exit])
        if found is not None:return candidate(d,k,found[0],found[1],'FRESH_CONFIRMED_PIVOT_AFTER_STOP')
    return None

def simulate(d,anchor_idx,event,calendar,stop_mode,horizon='FRESH20'):
    pos={day:i for i,day in enumerate(d.date)};cp=calendar.get_loc(event['confirm_date']);anchor_cp=calendar.get_loc(d.date.iloc[anchor_idx])
    base=dict(confirm_date=event['confirm_date'],stop_mode=stop_mode,horizon=horizon,stop_low=event.get('stop_low',np.nan),low_date=event.get('low_date',''),atr=event.get('atr',np.nan))
    expiry=anchor_cp+20 if horizon=='OLD20' else min(cp+20,anchor_cp+60)
    if cp+1>expiry:return {**base,'status':'ENTRY_AFTER_DEADLINE','entered':False,'closed':False}
    if cp+1>=len(calendar):return {**base,'status':'RIGHT_CENSORED','entered':False,'closed':False}
    entry_day=calendar[cp+1]
    if entry_day not in pos:return {**base,'status':'MISSING_ENTRY_BAR','entered':False,'closed':False}
    price=float(d.open.iloc[pos[entry_day]])
    line=price*.95 if stop_mode=='FIXED5' else event.get('stop_low',np.nan)-(.25*event.get('atr',np.nan) if stop_mode=='LOW_MINUS_025ATR' else 0)
    base.update(entry_date=entry_day,entry_price=price,stop_price=line,distance_pct=100*(1-line/price))
    if not np.isfinite(line) or not 0<line<price:return {**base,'status':'INVALID_STOP_AT_ENTRY','entered':False,'closed':False}
    if expiry>=len(calendar):return {**base,'status':'RIGHT_CENSORED','entered':True,'closed':False}
    tp=price*1.1;equity=[price];held=[]
    def finish(cp0,px,reason):
        day=calendar[cp0];seq=np.r_[equity,px];mdd=100*np.min(seq/np.maximum.accumulate(seq)-1);net=100*(px/price-1)-.25
        return {**base,'status':'CLOSED','entered':True,'closed':True,'exit_date':day,'exit_price':float(px),'reason':reason,'net_pct':net,
                'planned_R':net/base['distance_pct'],'close_mdd_pct':mdd,'example_risk_weight':min(1,1/base['distance_pct']),
                'example_risk_scaled_net':net*min(1,1/base['distance_pct']),'cost_pct_points':.25}
    for q in range(cp+1,expiry+1):
        day=calendar[q]
        if day not in pos:return {**base,'status':'MISSING_HOLDING_BAR','entered':True,'closed':False}
        b=d.iloc[pos[day]]
        if q>cp+1:
            if b.open<=line:return finish(q,b.open,'SL_GAP')
            if b.open>=tp:return finish(q,b.open,'TP_GAP')
        if b.low<=line:return finish(q,line,'SL_BOTH_HIT' if b.high>=tp else 'SL')
        if b.high>=tp:return finish(q,tp,'TP')
        if q==expiry:return finish(q,b.close,'TIME')
        equity.append(float(b.close))

def path(d,j,calendar):
    pos={day:i for i,day in enumerate(d.date)};cp=calendar.get_loc(d.date.iloc[j]);result={}
    for h in [20,40,60]:
        if cp+h>=len(calendar):result[f'coverage{h}']='RIGHT_CENSORED';continue
        days=calendar[cp+1:cp+h+1]
        if any(day not in pos for day in days):result[f'coverage{h}']='MISSING_BAR';continue
        w=d.iloc[[pos[day] for day in days]];price=float(w.open.iloc[0]);result.update({f'coverage{h}':'COMPLETE',f'peak{h}_pct':100*(w.high.max()/price-1),f'min{h}_pct':100*(w.low.min()/price-1),f'close{h}_pct':100*(w.close.iloc[-1]/price-1)})
    return result
