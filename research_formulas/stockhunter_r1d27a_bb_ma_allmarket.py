#!/usr/bin/env python3
"""R1-D27A historical full-KRX BB20/30/40 breaks/reclaims + MA5..448 + D30.

Outcome-blind all-market signal census first; separately sampled actual KRX
foreign/institution net KRW second; only then historical daily price paths.
RESEARCH ONLY; no LIVE/order/score/rank changes, no genuine prospective OOS.
Historical KRX source including delisted securities is NOT security/PIT certified.
"""
from __future__ import annotations
import argparse,csv,datetime as dt,gzip,hashlib,io,json,math,statistics,time
from collections import defaultdict,deque,Counter
from pathlib import Path
import numpy as np
import pandas as pd
from research_formulas import formula66_r1d6_oos_holdout as d6

PFILE='R1D27A_PREDECLARED_PROTOCOL.json'
MA=(5,10,20,40,60,112,224,448)
BB=(20,30,40)
H=(5,10,15,20,30)
EV='R1D27A_ALLMARKET_BB203040_ASOF_ONLY.csv.gz'
FLOWSEL='R1D27A_FROZEN_FLOW_SAMPLE_OUTCOME_BLIND.csv'
FLOWS='R1D27A_INVESTOR_SAMPLE_PRIOR20_SIGNED_KRW.csv.gz'
AUD='R1D27A_FLOW_SOURCE_AUDIT.csv'
OUT='R1D27A_ALLMARKET_D5_10_15_20_30_OUTCOMES.csv.gz'
SUM='R1D27A_BB_MA_BWIDTH_D30_CONTRASTS.csv'
HSUM='R1D27A_BB_MA_BWIDTH_D5_D30_HORIZON_COMPARISONS.csv'
FLOWCMP='R1D27A_SAMPLE_FLOW_PLUS_D30_DESCRIPTIVE.csv'
MAN='R1D27A_PHASE_MANIFEST.json'
PFX='R1D27A_'

def check(ok,why):
 if not ok:raise ValueError(PFX+why)
def sha(b):return hashlib.sha256(b).hexdigest()
def digest(path):return sha(Path(path).read_bytes())
def fmt(x):return round(float(x),7) if np.isfinite(x) else ''
def validbar(o,h,l,c,v,a):return 0<l<=min(o,c)<=max(o,c)<=h and v>=0 and a>=0
def readcfg(path=PFILE):
 p=json.loads(Path(path).read_text(encoding='utf-8'))
 check(p['version']=='R1D27A_FULLMARKET_BB_MA_BWIDTH_D30_FLOW_SAMPLE_FROZEN_V1','VERSION')
 check(p['research_only'] and not p['live_changed'] and p['orders_placed']==0 and not p['strategy_ready'] and not p['prospective_OOS'],'NO_LIVE')
 check(p['ma_periods']==list(MA) and p['bb_periods']==list(BB) and p['horizons']==list(H),'DEFINITIONS')
 check(p['root_krx_run']==39295295197 and p['historical_end']=='2025-09-30','SOURCE_PROTOCOL')
 return p

def seal(root,phase,**kwargs):
 root=Path(root);files={p.name:digest(p) for p in sorted(root.iterdir()) if p.is_file() and p.name!=MAN}
 obj={'version':'R1D27A','phase':phase,'research_only':True,'strategy_ready':False,'orders_placed':0,'live_changed':False,'prospective_OOS':False,
      'PIT_certified':False,'source_verified_or_missing':'SEPARATELY_REPORTED','files_sha256':files,**kwargs}
 (root/MAN).write_text(json.dumps(obj,indent=2,sort_keys=True,ensure_ascii=False)+'\n',encoding='utf8');return obj

def verify(root,phase):
 root=Path(root);m=json.loads((root/MAN).read_text('utf8'))
 check(m['phase']==phase and m['research_only'] and not m['strategy_ready'] and not m['live_changed'],'BAD_PHASE')
 for file,h in m['files_sha256'].items():check(digest(root/file)==h,'SHA_DRIFT_'+file)
 return m

def writer(path,fields):
 f=gzip.open(path,'wt',encoding='utf8',newline='') if str(path).endswith('.gz') else open(path,'w',encoding='utf8',newline='')
 w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n');w.writeheader();return f,w

def readrows(path):
 f=gzip.open(path,'rt',encoding='utf-8-sig') if str(path).endswith('.gz') else open(path,'r',encoding='utf-8-sig')
 with f:return list(csv.DictReader(f))

def bbstate(close,n,lag=(0,5,10,20)):
 """At each lag compute the *contemporaneous* lower/upper/width."""
 x=np.asarray(close,dtype=float)
 if len(x)<n+max(lag):return None
 res={}
 for ago in lag:
  y=x[-n-ago:len(x)-ago if ago else None]
  m=float(y.mean());sd=float(y.std(ddof=0))
  if not m>0:return None
  res[ago]=(m-2*sd,m+2*sd,400*sd/m)
 return res

def asof_features(history,signal_ix,market,p):
 """Only calculate expensive MA/width features on genuine BB cross events."""
 if len(history)<468:return None
 recent=list(history)
 if recent[-1][0]!=signal_ix or recent[-468][0]!=signal_ix-467:return None
 o,h,l,c,v,amt=recent[-1][2:]
 if not validbar(o,h,l,c,v,amt) or c<p['min_close'] or amt<p['min_signal_amount_krw']:return None
 prior20=[r[-1] for r in recent[-21:-1]]
 if len(prior20)!=20 or min(prior20)<0 or statistics.median(prior20)<p['min_pre20_median_amount_krw']:return None
 # Corporate actions are not PIT certified; quarantine suspicious recent 35% opening gaps.
 for i in range(-40,0):
  previous=recent[i-1][5];op=recent[i][2]
  if not previous>0 or abs(op/previous-1)>p['corporate_gap_quarantine_frac']:return None
 arr=np.fromiter((x[1] for x in recent),dtype=float,count=len(recent))
 if np.any(~np.isfinite(arr)) or arr.min()<=0:return None
 prev=float(arr[-2]);active=[]
 for n in BB:
  current=arr[-n:];before=arr[-n-1:-1]
  low=float(current.mean()-2*current.std(ddof=0))
  plow=float(before.mean()-2*before.std(ddof=0))
  if prev>=plow and c<low:active.append((n,'BREAK'))
  if prev<plow and c>=low:active.append((n,'RECLAIM'))
 if not active:return None
 row={'signal_ix':signal_ix,'signal_close':fmt(c),'signal_amount_krw':int(amt),
      'pre20_amount_sum_krw':int(sum(prior20)),'pre20_median_amount_krw':fmt(statistics.median(prior20)),
      'KOSPI_ret20_pct':fmt(market['KOSPI_ret20_pct']),'KOSDAQ_ret20_pct':fmt(market['KOSDAQ_ret20_pct']),
      'joint_index_20d_regime':market['joint_index_20d_regime']}
 for n in MA:
  av=float(arr[-n:].mean());prev5=float(arr[-n-5:-5].mean());prev20=float(arr[-n-20:-20].mean())
  row.update({f'MA{n}_dist_pct':fmt(100*(c/av-1)),f'MA{n}_slope5_pct':fmt(100*(av/prev5-1)),
              f'MA{n}_slope20_pct':fmt(100*(av/prev20-1)),f'MA{n}_above':int(c>=av)})
 for n in BB:
  bn=bbstate(arr,n)
  check(bn is not None,'BB_WARMUP')
  lower,upper,width=bn[0];prevbn=bbstate(arr[:-1],n,(0,))
  plower=prevbn[0][0]
  breach=int(prev>=plower and c<lower)
  reclaim=int(prev<plower and c>=lower)
  row.update({f'BB{n}_lower':fmt(lower),f'BB{n}_width_pct':fmt(width),
              f'BB{n}_width_change5_pct':fmt(100*(width/bn[5][2]-1)) if bn[5][2]>0 else '',
              f'BB{n}_width_change10_pct':fmt(100*(width/bn[10][2]-1)) if bn[10][2]>0 else '',
              f'BB{n}_width_change20_pct':fmt(100*(width/bn[20][2]-1)) if bn[20][2]>0 else '',
              f'BB{n}_break_today':breach,f'BB{n}_reclaim_today':reclaim})
 return row,active

def event_fields():
 c=['date','code','signal_ix','event_type','bb_period','signal_close','signal_amount_krw','pre20_amount_sum_krw','pre20_median_amount_krw',
   'KOSPI_ret20_pct','KOSDAQ_ret20_pct','joint_index_20d_regime']
 for n in MA:c.extend([f'MA{n}_dist_pct',f'MA{n}_slope5_pct',f'MA{n}_slope20_pct',f'MA{n}_above'])
 for n in BB:c.extend([f'BB{n}_lower',f'BB{n}_width_pct',f'BB{n}_width_change5_pct',f'BB{n}_width_change10_pct',f'BB{n}_width_change20_pct',f'BB{n}_break_today',f'BB{n}_reclaim_today'])
 return c

def select_flow(root,p,cal):
 """Deterministic, outcome-blind 10/year/band/event max; distinct tickers."""
 best=defaultdict(list)
 for r in readrows(root/EV):
  year=r['date'][:4];key=(year,int(r['bb_period']),r['event_type'])
  rank=sha(f"R1D27A_FROZEN_FLOW_ASOF_V1|{year}|{r['bb_period']}|{r['event_type']}|{r['date']}|{r['code']}".encode())
  best[key].append((rank,r['date'],r['code'],r['signal_ix']))
 selected=[];used=set()
 for year in map(str,range(2021,2026)):
  for n in BB:
   for event in ('BREAK','RECLAIM'):
    key=(year,n,event);picked=0
    for rank,date,code,ix in sorted(best[key]):
     if code in used:continue
     t=int(ix);check(cal[t]==date and t>=20,'FLOW_ASOF_CALENDAR')
     selected.append({'date':date,'code':code,'bb_period':n,'event_type':event,'year':year,'signal_ix':t,'pre20_from':cal[t-20],
                    'pre20_to':cal[t-1],'select_hash':rank,'source':'ALLMARKET_ASOF_ONLY_NO_OUTCOME'})
     used.add(code);picked+=1
     if picked==p['flow_sample_per_year_band_event']:break
  # Not enough cases should be transparent, not supplemented from other cohorts
 check(len({r['code'] for r in selected})==len(selected),'FLOW_TICKER_REUSE')
 fields=list(selected[0]) if selected else ['date','code','bb_period','event_type','year','signal_ix','pre20_from','pre20_to','select_hash','source']
 f,w=writer(root/FLOWSEL,fields)
 with f:w.writerows(selected)
 return {'flow_sample_selected':len(selected),'flow_sample_target':5*3*2*p['flow_sample_per_year_band_event'],
         'flow_strata_shortfall':5*3*2*p['flow_sample_per_year_band_event']-len(selected)}

def census(source_root,out,p):
 root=Path(out);root.mkdir(parents=True,exist_ok=True)
 src=d6.source_manifest(source_root);cal,market=d6.index_asof(source_root)
 check(d6.verify_all_daily_raw_sha(src)==1907,'KRX_SHA_VERIFY')
 hist=defaultdict(lambda:deque(maxlen=468))
 f,w=writer(root/EV,event_fields())
 ct=Counter();eligible=0
 with f:
  for t,date in enumerate(cal):
   if date>p['historical_end']:break
   df=d6.priceday(src[date][0],date)
   for row in df.itertuples(index=False):
    code=str(row.code); vals=(float(row.close),float(row.open),float(row.high),float(row.low),float(row.close),float(row.volume),float(row.amount))
    hist[code].append((t,*vals))
    if date<p['historical_start']:continue
    try:computed=asof_features(hist[code],t,market[date],p)
    except (ValueError,OverflowError,FloatingPointError):computed=None
    if computed is None:continue
    feat,events=computed;eligible+=1
    for n,kind in events:
     full={**feat,'date':date,'code':code,'bb_period':n,'event_type':kind}
     w.writerow(full);ct[f'{n}_{kind}']+=1
   if t%125==0:print('R1D27A_SCAN',date,'eligible_observations',eligible,'events',sum(ct.values()),flush=True)
 check(sum(ct.values())>100,'ZERO_BB_EVENTS')
 flow=select_flow(root,p,cal)
 return seal(root,'census',price_days_sha_verified=1907,source_period=[cal[0],cal[-1]],n_events=sum(ct.values()),
             counts_by_band_event=dict(ct),n_liquid_eligible_stock_dates=eligible,all_market_without_ma224_gate=True,
             historical_not_prospective_oos=True,**flow)

def parseflow(df,days):
 check(isinstance(df,pd.DataFrame) and not df.empty,'FLOW_PROVIDER_EMPTY')
 check({'기관합계','외국인합계'}.issubset(df.columns),'FLOW_KRX_SCHEMA')
 val={}
 for index,r in df.iterrows():
  day=pd.Timestamp(index).strftime('%Y-%m-%d')
  check(day not in val,'FLOW_DUP_DAY')
  vv=[]
  for col in ('외국인합계','기관합계'):
   raw=r[col];check(pd.notna(raw) and float(raw).is_integer(),'FLOW_NON_INTEGER')
   vv.append(int(raw))
  val[day]=vv
 check(set(val)==set(days),'FLOW_DATE_MISMATCH')
 check(any(any(z) for z in val.values()),'FLOW_ZERO_ALL')
 return val

def flow(census_dir,out,source_root,p,sleep=0.45):
 c=verify(census_dir,'census');cal,_=d6.index_asof(source_root)
 rows=readrows(Path(census_dir)/FLOWSEL)
 root=Path(out);root.mkdir(parents=True,exist_ok=True)
 fields=['date','code','bb_period','event_type','year','trade_date','foreign_net_krw','institution_net_krw','source','unit']
 f,w=writer(root/FLOWS,fields);audit=[];ok=0
 from pykrx import stock
 with f:
  for ix,r in enumerate(rows):
   t=int(r['signal_ix']);expected=cal[t-20:t]
   try:
    df=stock.get_market_trading_value_by_date(expected[0].replace('-',''),expected[-1].replace('-',''),r['code'],on='순매수')
    values=parseflow(df,expected)
    for day in expected:
     x,y=values[day];w.writerow({**r,'trade_date':day,'foreign_net_krw':x,'institution_net_krw':y,
                                 'source':'KRX_VIA_PYKRX_HISTORICAL_NOT_PIT','unit':'KRW'})
    status='COMPLETE_20_NOT_PIT';ok+=1;reason=''
   except Exception as ex:status='QUARANTINED_NO_IMPUTATION';reason=type(ex).__name__
   audit.append({**r,'status':status,'error_type':reason})
   if (ix+1)%25==0:print('R1D27A_FLOW',ix+1,'/',len(rows),'complete',ok,flush=True)
   if sleep:time.sleep(sleep)
 fa,wa=writer(root/AUD,list(audit[0].keys()) if audit else ['date','code','status','error_type'])
 with fa:wa.writerows(audit)
 return seal(root,'flow',n_frozen=len(rows),n_flow_complete=ok,n_flow_incomplete=len(rows)-ok,
             investor_source='KRX_VIA_PYKRX_HISTORICAL_NOT_PIT',official_independent_crosscheck='PENDING',
             historical_publication_PIT='NOT_VERIFIED',source_census_sha=c['files_sha256'][EV])

def evaluate(census_dir,flow_dir,source_root,out,p):
 cm=verify(census_dir,'census');fm=verify(flow_dir,'flow')
 check(fm['source_census_sha']==cm['files_sha256'][EV],'FLOW_VS_CENSUS_SOURCE')
 root=Path(out);root.mkdir(parents=True,exist_ok=True)
 src=d6.source_manifest(source_root);cal,_=d6.index_asof(source_root)
 check(d6.verify_all_daily_raw_sha(src)==1907,'KRX_SHA_VERIFY_EVAL')
 signal=readrows(Path(census_dir)/EV)
 check(len(signal)==cm['n_events'],'CENSUS_COUNT_CHANGED')
 by_day=defaultdict(list);count=Counter()
 for r in signal:
  idx=int(r['signal_ix']);check(cal[idx]==r['date'],'SIGNAL_ASOF_DRIFT')
  if idx+30>=len(cal):count['out_of_30_calendar']+=1
  by_day[idx+1].append(r)
 # Event exposure tracked independently. No dependence on any technical signal after entry.
 pending=defaultdict(list);cols=['date','code','event_type','bb_period','horizon','status_out','entry_open','exit_close',
  'net_20bps_pct','MFE_intraday_pct','MAE_intraday_pct','hit5','hit10','hit15','max_gap_abs_pct']
 f,w=writer(root/OUT,cols);n_out=0;valid=0;bad=Counter()
 with f:
  last=min(len(cal)-1,cal.index(p['historical_end'])+30)
  for t in range(cal.index(p['historical_start'])+1,last+1):
   date=cal[t];df=d6.priceday(src[date][0],date)
   day={str(r.code):(float(r.open),float(r.high),float(r.low),float(r.close),float(r.volume),float(r.amount)) for r in df.itertuples(index=False)}
   for r in by_day.get(t,[]):pending[r['code']].append({'r':r,'entry_ix':t,'entry':None,'last':None,'mfe':-float('inf'),'mae':float('inf'),'err':'','max_gap':0.0})
   for code in list(pending):
    current=pending[code];q=day.get(code)
    for st in current:
     d=t-st['entry_ix']+1
     if d>30:continue
     if not st['err']:
      if q is None:st['err']='MISSING_STOCK_DAY'
      elif not validbar(*q):st['err']='OHLC_INVALID'
      else:
       o,hi,lo,cl,vol,amt=q
       if st['last'] is not None:
        gap=abs(o/st['last']-1)
        st['max_gap']=max(st['max_gap'],gap*100)
        if gap>p['corporate_gap_quarantine_frac']:st['err']='GAP_ABOVE_35_PCT'
       if not st['err']:
        if st['entry'] is None:st['entry']=o
        st['last']=cl
        if st['entry']<=0:st['err']='NONPOSITIVE_ENTRY'
        else:
         st['mfe']=max(st['mfe'],100*(hi/st['entry']-1));st['mae']=min(st['mae'],100*(lo/st['entry']-1))
     if d in H:
      if not st['err'] and st['entry'] is not None:
       v={'status_out':'PRICE_PATH_COMPLETE_NOT_FILL_CERTIFIED','entry_open':fmt(st['entry']),'exit_close':fmt(st['last']),
          'net_20bps_pct':fmt(100*(st['last']/st['entry']-1)-p['roundtrip_fee_bps']/100),'MFE_intraday_pct':fmt(st['mfe']),
          'MAE_intraday_pct':fmt(st['mae']),'hit5':int(st['mfe']>=5),'hit10':int(st['mfe']>=10),'hit15':int(st['mfe']>=15)}
       valid+=1
      else:
       v={'status_out':'CENSORED_'+(st['err'] or 'NOT_COMPLETE'),'entry_open':'','exit_close':'',
         'net_20bps_pct':'','MFE_intraday_pct':'','MAE_intraday_pct':'','hit5':'','hit10':'','hit15':''}
       bad[v['status_out']]+=1
      w.writerow({'date':st['r']['date'],'code':code,'event_type':st['r']['event_type'],'bb_period':st['r']['bb_period'],
                  'horizon':d,**v,'max_gap_abs_pct':fmt(st['max_gap'])});n_out+=1
    pending[code]=[st for st in current if t-st['entry_ix']+1<30]
    if not pending[code]:del pending[code]
   if t%130==0:print('R1D27A_D30',date,'outcomes',n_out,'valid',valid,'active',sum(len(s) for s in pending.values()),flush=True)
 check(n_out==len(signal)*len(H),'D30_OUTCOME_CENSORED_MISSING')
 # Same denominator for BB-only and MA/BWIDTH: ASOF features and completed D30, no outcome-driven selection.
 outcome=pd.read_csv(root/OUT,dtype={'code':str,'date':str,'bb_period':str},low_memory=False)
 feat=pd.DataFrame(signal);feat['code']=feat.code.astype(str).str.zfill(6);feat['bb_period']=feat.bb_period.astype(str)
 d30=outcome[(outcome.horizon==30)&(outcome.status_out=='PRICE_PATH_COMPLETE_NOT_FILL_CERTIFIED')]
 j=feat.merge(d30,on=['date','code','event_type','bb_period'],validate='one_to_one',how='inner')
 tab=[]
 def add_summary(c,tag,group,ref=None):
  if len(c)<p['min_report_cohort']:return
  ret=c.net_20bps_pct.astype(float)
  paired_n=0;paired_return='';paired_win=''
  if ref is not None and len(c)<len(ref):
   mark=set(zip(c.date,c.code))
   opp=ref.loc[[((d,k) not in mark) for d,k in zip(ref.date,ref.code)]]
   x=c.assign(win=(ret>0).astype(float)).groupby('date')[['net_20bps_pct','win']].mean()
   y=opp.assign(win=(opp.net_20bps_pct.astype(float)>0).astype(float)).groupby('date')[['net_20bps_pct','win']].mean()
   dates=x.index.intersection(y.index)
   paired_n=len(dates)
   if paired_n:
    paired_return=fmt((x.loc[dates,'net_20bps_pct']-y.loc[dates,'net_20bps_pct']).mean())
    paired_win=fmt(100*(x.loc[dates,'win']-y.loc[dates,'win']).mean())
  tab.append({'comparison':tag,'group':group,'n_complete':len(c),'avg_return30':fmt(ret.mean()),'median_return30':fmt(ret.median()),
    'win_rate30_pct':fmt(100*(ret>0).mean()),'hit5_pct':fmt(100*c.hit5.astype(float).mean()),'hit10_pct':fmt(100*c.hit10.astype(float).mean()),
    'hit15_pct':fmt(100*c.hit15.astype(float).mean()),'loss_le5_pct':fmt(100*(ret<=-5).mean()),
    'n_same_day_matched_dates':paired_n,'mean_date_matched_return_diff_pp':paired_return,'mean_date_matched_win_diff_pp':paired_win})
 for (bb,event),a in j.groupby(['bb_period','event_type']):
  add_summary(a,'BB_ONLY',f'BB{bb}_{event}')
  # Width quantiles within year+BB+event fixed *as-of* only; labels aren't learned from outcome.
  for m in MA:
   for k,group in a.groupby(f'MA{m}_above'):add_summary(group,f'MA{m}_POSITION',f'BB{bb}_{event}_above_{k}',ref=a)
   for k,group in a.groupby(a[f'MA{m}_slope20_pct'].astype(float)>0):add_summary(group,f'MA{m}_SLOPE20',f'BB{bb}_{event}_slope_positive_{int(k)}',ref=a)
  for m in BB:
   width=pd.to_numeric(a[f'BB{m}_width_pct'],errors='coerce')
   for tag,mask in [('NARROW_LT12',width<12),('WIDE_GE25',width>=25),('MID_12_25',(width>=12)&(width<25))]:
    add_summary(a[mask],f'BB{m}_WIDTH',f'BB{bb}_{event}_{tag}',ref=a)
   for tag,mask in [('SHRINK5',pd.to_numeric(a[f'BB{m}_width_change5_pct'],errors='coerce')<0),('EXPAND5',pd.to_numeric(a[f'BB{m}_width_change5_pct'],errors='coerce')>=0)]:
    add_summary(a[mask],f'BB{m}_WIDTH_DIRECTION',f'BB{bb}_{event}_{tag}',ref=a)
  for r,x in a.groupby('joint_index_20d_regime'):add_summary(x,'MARKET_20D',f'BB{bb}_{event}_{r}')
 f1,w1=writer(root/SUM,list(tab[0]) if tab else ['comparison','group','n_complete'])
 with f1:w1.writerows(tab)
 # Same frozen event population summarized across all five horizons; same predeclared attributes.
 allh=feat.merge(outcome[outcome.status_out=='PRICE_PATH_COMPLETE_NOT_FILL_CERTIFIED'],
                 on=['date','code','event_type','bb_period'],validate='one_to_many',how='inner')
 hrows=[]
 def hrs(z,tag,group,h):
  if len(z)<p['min_report_cohort']:return
  rr=z.net_20bps_pct.astype(float)
  hrows.append({'comparison':tag,'group':group,'horizon':h,'n_completed':len(z),
                'mean_net_pct':fmt(rr.mean()),'median_net_pct':fmt(rr.median()),
                'win_pct':fmt(100*(rr>0).mean()),'hit5_pct':fmt(100*z.hit5.astype(float).mean()),
                'hit10_pct':fmt(100*z.hit10.astype(float).mean()),'hit15_pct':fmt(100*z.hit15.astype(float).mean())})
 for (bb,event,h),a in allh.groupby(['bb_period','event_type','horizon']):
  hrs(a,'BB_ONLY',f'BB{bb}_{event}',int(h))
  for ma in MA:
   for flag,g in a.groupby(f'MA{ma}_above'):hrs(g,f'MA{ma}_ABOVE',f'BB{bb}_{event}_above_{flag}',int(h))
  for n in BB:
   width=pd.to_numeric(a[f'BB{n}_width_pct'],errors='coerce')
   for name,mask in [('NARROW_LT12',width<12),('WIDE_GE25',width>=25),('MID_12_25',(width>=12)&(width<25))]:
    hrs(a[mask],f'BB{n}_WIDTH',f'BB{bb}_{event}_{name}',int(h))
   diff=pd.to_numeric(a[f'BB{n}_width_change5_pct'],errors='coerce')
   for name,mask in [('SHRINK5',diff<0),('EXPAND5',diff>=0)]:
    hrs(a[mask],f'BB{n}_WIDTH_DIRECTION',f'BB{bb}_{event}_{name}',int(h))
 fh,wh=writer(root/HSUM,list(hrows[0]) if hrows else ['comparison','group','horizon','n_completed'])
 with fh:wh.writerows(hrows)
 # Investor subset only: never fill missing KRX flow with fabricated 0.
 fs=readrows(Path(flow_dir)/FLOWS);group=defaultdict(list)
 for x in fs:group[(x['date'],x['code'],x['event_type'],x['bb_period'])].append(x)
 flowrows=[]
 for s in readrows(Path(census_dir)/FLOWSEL):
  key=(s['date'],s['code'],s['event_type'],str(s['bb_period']))
  xs=sorted(group.get(key,[]),key=lambda x:x['trade_date'])
  if len(xs)!=20:continue
  a=[int(x['foreign_net_krw']) for x in xs];b=[int(x['institution_net_krw']) for x in xs]
  dfrow=j[(j.date==s['date'])&(j.code==s['code'])&(j.event_type==s['event_type'])&(j.bb_period==str(s['bb_period']))]
  if len(dfrow)!=1:continue
  q=dfrow.iloc[0];sumamt=float(q.pre20_amount_sum_krw)
  record={'date':s['date'],'code':s['code'],'event_type':s['event_type'],'bb_period':s['bb_period'],
    'year':s['year'],'D30_net_pct':fmt(q.net_20bps_pct),'hit15':int(q.hit15),
    'foreign_net5_krw':sum(a[-5:]),'foreign_net10_krw':sum(a[-10:]),'foreign_net20_krw':sum(a),
    'institution_net5_krw':sum(b[-5:]),'institution_net10_krw':sum(b[-10:]),'institution_net20_krw':sum(b),
    'foreign_5_vs_prev5_delta_krw':sum(a[-5:])-sum(a[-10:-5]),
    'institution_5_vs_prev5_delta_krw':sum(b[-5:])-sum(b[-10:-5]),
    'foreign_pos_days20':sum(v>0 for v in a),'institution_pos_days20':sum(v>0 for v in b),
    'both_net20_pos':int(sum(a)>0 and sum(b)>0),
    'foreign_net20_pct_total_traded':fmt(100*sum(a)/sumamt) if sumamt>0 else '',
    'institution_net20_pct_total_traded':fmt(100*sum(b)/sumamt) if sumamt>0 else '',
    'source':'KRX_VIA_PYKRX_RETROSPECTIVE_NOT_PIT'}
  flowrows.append(record)
 f2,w2=writer(root/FLOWCMP,list(flowrows[0]) if flowrows else ['date','code','source'])
 with f2:w2.writerows(flowrows)
 return seal(root,'evaluate',n_asof_events=cm['n_events'],n_outcome_rows=n_out,n_completed_paths=valid,
             censored_reason_counts=dict(bad),n_d30_completed=len(j),n_flow_sample_selected=fm['n_frozen'],
             n_flow_sample_source_complete=fm['n_flow_complete'],n_flow_joined_d30=len(flowrows),
             no_imputation=True,not_fullmarket_flow=True,historical_not_future_OOS=True,
             risk_warning='SAME_HISTORICAL_DATA_EXPLORATORY_MULTIPLE_COMPARISONS')

def cli():
 a=argparse.ArgumentParser();a.add_argument('--mode',required=True,choices=['census','flow','evaluate','verify']);
 a.add_argument('--krx-root',default='reports/r1d27a/input/krx');a.add_argument('--census',default='reports/r1d27a/result/census');
 a.add_argument('--flow',default='reports/r1d27a/result/flow');a.add_argument('--out');a.add_argument('--protocol',default=PFILE)
 x=a.parse_args();p=readcfg(x.protocol)
 if x.mode=='census':m=census(x.krx_root,x.out or x.census,p)
 elif x.mode=='flow':m=flow(x.census,x.out or x.flow,x.krx_root,p)
 elif x.mode=='evaluate':m=evaluate(x.census,x.flow,x.krx_root,x.out or 'reports/r1d27a/result/evaluate',p)
 else:m=verify(x.out or x.census,'census')
 print('R1D27A_'+x.mode.upper()+'_RESEARCH_ONLY',json.dumps({k:v for k,v in m.items() if k!='files_sha256'},ensure_ascii=False),flush=True)
if __name__=='__main__':cli()
