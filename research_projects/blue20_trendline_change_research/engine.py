import numpy as np

def prepare(d,calendar):
 d=d.copy().reset_index(drop=True);cp={v:i for i,v in enumerate(calendar)};d['session']=d.date.map(cp);prev=d.close.shift();tr=np.maximum.reduce([(d.high-d.low).to_numpy(),(d.high-prev).abs().fillna(0).to_numpy(),(d.low-prev).abs().fillna(0).to_numpy()]);import pandas as pd
 d['atr']=pd.Series(tr).rolling(20,min_periods=20).mean().shift();high=d.high.to_numpy();low=d.low.to_numpy();session=d.session.to_numpy();hp=[];lp=[]
 for i in range(2,len(d)-2):
  if not np.all(np.diff(session[i-2:i+3])==1):continue
  if high[i]>high[i-2:i].max() and high[i]>=high[i+1:i+3].max():hp.append(i)
  if low[i]<low[i-2:i].min() and low[i]<=low[i+1:i+3].min():lp.append(i)
 d.attrs['_trend_arrays']={k:d[k].to_numpy() for k in ['session','date','high','low','close','atr']}
 return d,{'HIGH':hp,'LOW':lp}

def value(line,session):return line['p1']+line['slope']*(session-line['s1'])

def snapshot(d,pivots,j,n=120):
 arr=d.attrs.get('_trend_arrays')
 if arr is None:arr={k:d[k].to_numpy() for k in ['session','date','high','low','close','atr']}
 sessions=arr['session'];dates=arr['date'];now=int(sessions[j]);out={'status':'KNOWN','lookback':n};lines={}
 if j-int(np.searchsorted(sessions,now-n))!=n:return {'status':'UNKNOWN_HISTORY_OR_GAP','lookback':n},lines
 atr=float(arr['atr'][j]);out['atr']=atr
 if not np.isfinite(atr) or atr<=0:return {'status':'UNKNOWN_ATR','lookback':n},lines
 for kind,field,direction in [('HIGH','high','DESC'),('LOW','low','SUPPORT')]:
  raw=np.asarray(pivots[kind],dtype=int);points=raw[(sessions[raw]>=now-n)&(sessions[raw+2]<now)];second=int(points[-1]) if len(points) else None;eligible=points[sessions[second]-sessions[points]>=5] if second is not None else np.array([],dtype=int);first=int(eligible[-1]) if len(eligible) else None
  key=direction+'_state'
  if first is None:out[key]='NO_LINE';continue
  p1=float(arr[field][first]);p2=float(arr[field][second]);s1=int(sessions[first]);s2=int(sessions[second]);slope=(p2-p1)/(s2-s1)
  if (direction=='DESC' and slope>=0) or (direction=='SUPPORT' and slope<=0):out[key]='WRONG_DIRECTION';continue
  line=dict(kind=direction,p1=p1,p2=p2,s1=s1,s2=s2,slope=slope,anchor1=dates[first],anchor2=dates[second],known_date=dates[second+2],known_session=int(sessions[second+2]));today=value(line,now);prior=value(line,now-1)
  if min(today,prior)<=0:out[key]='INVALID_PROJECTION';continue
  tol=.25*atr;close=float(arr['close'][j]);previous=float(arr['close'][j-1]);lines[direction]=line;out[direction+'_value']=today
  if direction=='DESC':out[key]='BREAK_NOW' if close>today+tol and previous<=prior+tol else 'ALREADY_ABOVE' if close>today+tol else 'BELOW_OR_NEAR'
  else:out[key]='BREAK_NOW' if close<today-tol and previous>=prior-tol else 'ALREADY_BELOW' if close<today-tol else 'ABOVE_OR_NEAR'
 return out,lines

def wait_events(d,pos,calendar,anchor,line):
 start=calendar.get_loc(anchor);events={};break_cp=None;test_cp=None;complete=start+20<len(calendar)
 for p in range(start,min(start+20,len(calendar)-1)+1):
  day=calendar[p]
  if day not in pos:return events,'MISSING_WAIT_BAR'
  k=pos[day];level=value(line,p);atr=float(d.atr.iloc[k])
  if level<=0:return events,'INVALID_PROJECTION'
  above=d.close.iloc[k]>level+.25*atr
  if break_cp is None and above:break_cp=p;events.setdefault('C_WAIT_BREAK',day)
  elif break_cp is not None and p>break_cp:
   if not above:break_cp=None;test_cp=None;continue
   if test_cp is None:
    if level-.25*atr<=d.low.iloc[k]<=level+.25*atr and above:test_cp=p
   elif p>test_cp:
    if above:events['D_RETEST']=day;break
    test_cp=None
 return events,'NO_CONFIRMATION' if complete else 'RIGHT_CENSORED_WAIT'

def simulate(d,pos,calendar,confirm,target,support=None):
 p=calendar.get_loc(confirm)+1
 if p>=len(calendar):return dict(status='NO_ENTRY_SESSION',entered=False,closed=False)
 days=calendar[p:min(p+60,len(calendar))];full=p+59<len(calendar)
 if any(v not in pos for v in days):return dict(status='MISSING_BAR',entered=True,closed=False)
 price=float(d.open.iloc[pos[days[0]]]);tp=price*(1+target/100);base=dict(confirm_date=confirm,entry_date=days[0],entry_price=price,target_pct=target,full60=full,entered=True);pending=False
 for age,day in enumerate(days):
  k=pos[day];bar=d.iloc[k];reason=None;px=None
  if age>0 and bar.open>=tp:reason='TP';px=float(bar.open)
  elif pending:reason='SUPPORT_EXIT';px=float(bar.open)
  elif bar.high>=tp:reason='TP';px=tp
  elif age==59:reason='TIME';px=float(bar.close)
  if reason:return base|dict(status=reason,closed=True,exit_date=day,exit_price=px,holding_sessions=age+1,net_pct=100*(px/price-1)-.25)
  if support is not None:
   level=value(support,p+age)
   if level>0 and bar.close<level-.25*bar.atr:pending=True
 return base|dict(status='RIGHT_CENSORED',closed=False)
