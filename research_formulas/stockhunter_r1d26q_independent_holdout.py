#!/usr/bin/env python3
"""R1-D26Q historical ticker-disjoint price/foreign-flow confirmatory-style holdout.

Research-only, no orders. Hypotheses were discovered in prior sample and are thus
*not* fresh prospective OOS; this tests on distinct historical tickers. Raw KRX via
pykrx is retrospective and is NOT independently certified PIT. No source data is
silently imputed, no outcome-conditioned selection, no after-reentry feature use.
"""
from __future__ import annotations
import argparse,csv,gzip,hashlib,io,json,math,statistics,time,zipfile
from collections import defaultdict
from datetime import date,timedelta
from pathlib import Path
import pandas as pd

D='D_LEADER_CONSOLIDATION_BREAKOUT'
EVENT=('strategy','date','code')
PRE='R1D26Q_NEW_D125_PRE_SIGNAL_PRICE.csv.gz'
POST='R1D26Q_NEW_D125_POST30_PRICE.csv.gz'
SUP='R1D26Q_NEW_D125_SUPPORT_LOCK.csv'
PRICE_AUD='R1D26Q_PRICE_FETCH_AUDIT.csv'
NET='R1D26Q_NEW_D125_REENTRY_INVESTOR_NET.csv'
NET_AUD='R1D26Q_INVESTOR_FETCH_AUDIT.csv'
RES='R1D26Q_NEW_D125_INDEPENDENT_CASES.csv'
SUM='R1D26Q_PREDECLARED_HYPOTHESES_SUMMARY.csv'
YEAR='R1D26Q_YEAR_STRATEGY_SUMMARY.csv'
PARITY='R1D26Q_D25_D30_REFERENCE_PARITY_AUDIT.csv'
D25FUT='reports/r1d25/result/historical/R1D25_D5_D10_D15_D20_D30_FUTURE_PRICE_PATHS.csv.gz'
MAN='R1D26Q_QA_MANIFEST.json'
FROZEN='data/R1D26Q_FROZEN_D125_ASOF_ONLY.csv'
PILOT='data/R1D26L_FROZEN_60_ASOF_EVENTS.csv'
D25ASOF='reports/r1d25/result/historical/R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz'
COLS=['strategy','date','code','trade_date','open','high','low','close','volume','amount']


def sha(x):return hashlib.sha256(x).hexdigest()
def read_csv_bytes(b,zipped=False):
 if zipped:b=gzip.decompress(b)
 return list(csv.DictReader(io.StringIO(b.decode('utf-8-sig'))))
def csv_write(path,rows,columns):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 with path.open('w',encoding='utf8',newline='') as f:
  w=csv.DictWriter(f,columns,extrasaction='ignore');w.writeheader();w.writerows(rows)
def csv_zip(path,rows,columns):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 s=io.StringIO();w=csv.DictWriter(s,columns,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rows)
 with path.open('wb') as f:
  with gzip.GzipFile(fileobj=f,mode='wb',mtime=0,filename='') as g:g.write(s.getvalue().encode())
def manifest(path,**more):
 p=Path(path);p.mkdir(parents=True,exist_ok=True)
 d={'version':'R1D26Q_TICKER_DISJOINT_HISTORICAL_HOLDOUT_V1','research_only':True,'strategy_ready':False,'live_changed':False,'orders_placed':0,'prospective_OOS':False,'historical_ticker_disjoint_from_pilot':True,'price_PIT':'NOT_CERTIFIED','investor_PIT':'NOT_CERTIFIED','official_independent_source':'PENDING',**more}
 d['file_sha256']={x.name:sha(x.read_bytes()) for x in p.iterdir() if x.is_file() and x.name!=MAN}
 (p/MAN).write_text(json.dumps(d,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf8');return d

def check_dir(path):
 p=Path(path);m=json.loads((p/MAN).read_text(encoding='utf8'))
 if m['strategy_ready'] or m['orders_placed'] or m['live_changed']:raise ValueError('RESEARCH_ONLY_FAILED')
 for name,d in m['file_sha256'].items():
  if sha((p/name).read_bytes())!=d:raise ValueError('MANIFEST_DATA_TAMPER:'+name)
 return m

def d25_asof_selection(d25_blob,pilot_blob,n_per_year=25):
 with zipfile.ZipFile(io.BytesIO(d25_blob)) as z:
  pool=read_csv_bytes(z.read(D25ASOF),True)
 old=read_csv_bytes(pilot_blob)
 old_codes={r['code'] for r in old};chosen=[];used=set(old_codes)
 for year in map(str,range(2021,2026)):
  elig=[r for r in pool if r['strategy']==D and r['date'][:4]==year and r['code'] not in used]
  elig.sort(key=lambda r:(sha(f"R1D26Q_LOCK_V1|{year}|{r['date']}|{r['code']}".encode()),r['date'],r['code']))
  n=0
  for r in elig:
   if r['code'] in used:continue
   chosen.append({'strategy':D,'date':r['date'],'code':r['code'],'year':year,'selection_basis':'ASOF_SHA256_TICKER_DISJOINT_NO_OUTCOMES'})
   used.add(r['code']);n+=1
   if n>=n_per_year:break
  if n!=n_per_year:raise ValueError('INSUFFICIENT_NEW_HOLDOUT_TICKERS_'+year)
 if len(chosen)!=n_per_year*5 or len(set(r['code'] for r in chosen))!=len(chosen):raise ValueError('SELECT_TICKER_REPEAT')
 return chosen

def selection_load(path,protocol):
 b=Path(path).read_bytes()
 if sha(b)!=protocol['selection_sha256']:raise ValueError('FROZEN_NEW_ASOF_SELECTION_SHA_CHANGED')
 r=read_csv_bytes(b)
 if len(r)!=125 or len({x['code'] for x in r})!=125 or {y:sum(a['year']==y for a in r) for y in map(str,range(2021,2026))}!={y:25 for y in map(str,range(2021,2026))}:raise ValueError('HOLDOUT_BALANCE_DRIFT')
 return r

def verify_select(d25_path,protocol,selection):
 if sha(Path(d25_path).read_bytes())!=protocol['parent_d25_zip_sha256']:raise ValueError('PARENT_D25_SHA_CHANGED')
 with zipfile.ZipFile(d25_path) as z:
  if sha(z.read(D25ASOF))!=protocol['d25_asof_csv_gz_sha256']:raise ValueError('D25_SIGNAL_ONLY_SOURCE_DRIFT')
  pilot=z.read('R1D25_PREDECLARED_PROTOCOL.json') if 'R1D25_PREDECLARED_PROTOCOL.json' in z.namelist() else None
 old=Path(PILOT).read_bytes()
 if sha(old)!=protocol['pilot_selection_sha256']:raise ValueError('PILOT_SELECTION_SHA_CHANGED')
 expected=d25_asof_selection(Path(d25_path).read_bytes(),old)
 actual=selection_load(selection,protocol)
 if actual!=expected:raise ValueError('FROZEN_NEW_SIGNAL_SELECTION_RECOMPUTE_DIFFERENT')
 if set(r['code'] for r in actual)&set(r['code'] for r in read_csv_bytes(old)):raise ValueError('PILOT_TICKER_OVERLAP')
 return actual

def parse_barframe(raw,r):
 if raw is None or raw.empty or not raw.index.is_unique:raise ValueError('EMPTY_OR_DUPLICATE_PRICES')
 mapping={'open':'시가','high':'고가','low':'저가','close':'종가','volume':'거래량','amount':'거래대금'}
 if not set(mapping.values()).issubset(raw.columns):raise ValueError('OHLC_SCHEMA_DRIFT')
 out=[]
 for k,row in raw.sort_index().iterrows():
  d=pd.Timestamp(k).date().isoformat();v={x:int(row[y]) for x,y in mapping.items()}
  if not 0<v['low']<=min(v['open'],v['close'])<=max(v['open'],v['close'])<=v['high'] or v['volume']<0 or v['amount']<0:raise ValueError('INVALID_PRICE_BAR')
  out.append({**{x:r[x] for x in EVENT},'trade_date':d,**v})
 if len(set(x['trade_date'] for x in out))!=len(out):raise ValueError('DUPLICATE_PRICE_DAYS')
 return out

def get_price(event,phase):
 from pykrx import stock
 d=date.fromisoformat(event['date'])
 if phase=='pre':first=d-timedelta(days=250);last=d
 elif phase=='post':first=d+timedelta(days=1);last=d+timedelta(days=110)
 else:raise ValueError('BAD_PHASE')
 raw=stock.get_market_ohlcv_by_date(first.strftime('%Y%m%d'),last.strftime('%Y%m%d'),event['code'],adjusted=False)
 return parse_barframe(raw,event)

def support_from_pre(pre,event):
 if len(pre)<65 or pre[-1]['trade_date']!=event['date'] or any(r['trade_date']>event['date'] for r in pre):raise ValueError('PRE_SUPPORT_FUTURE_OR_INCOMPLETE')
 # D signal's previous20 high. The signal day is EXCLUDED from resistance discovery.
 top=max(b['high'] for b in pre[-21:-1]);median_amount=statistics.median(b['amount'] for b in pre[-21:-1]);median_volume=statistics.median(b['volume'] for b in pre[-21:-1]);signal_close=pre[-1]['close']
 return {'support':top,'signal_close':signal_close,'median_pre20_amount':median_amount,'median_pre20_volume':median_volume,'asof_support_status':'VALID_AT_SIGNAL_CLOSE' if top>0 and top<signal_close and median_amount>0 and median_volume>0 else 'ASOF_LEVEL_NOT_VALID'}

def collect_price(selection,protocol,out,phase,fetch=get_price,sleep=1.2):
 p=Path(out);p.mkdir(parents=True,exist_ok=True)
 events=selection_load(selection,protocol)
 support_in={};all_rows=[];audit=[];supports=[]
 if phase=='post':
  pm=check_dir(protocol['pre_dir']);support_in={(r['strategy'],r['date'],r['code']):r for r in read_csv_bytes((Path(protocol['pre_dir'])/SUP).read_bytes())}
  if len(support_in)!=pm['valid'] or pm['attempted']!=125:raise ValueError('PRE_SUPPORT_NOT_SEALED')
 for idx,r in enumerate(events,1):
  status='VALID';reason='';bars=[]
  try:
   bars=fetch(r,phase)
   if phase=='pre':
    bars=[x for x in bars if x['trade_date']<=r['date']][-120:]
    if len(bars)<65 or bars[-1]['trade_date']!=r['date']:raise ValueError('SIGNAL_DAY_OR_PRE_HISTORY_MISSING')
    if any(abs(bars[i]['open']/bars[i-1]['close']-1)>.35 for i in range(1,len(bars))):raise ValueError('PRE_GAP_ABOVE35PCT')
    s=support_from_pre(bars,r)
    supports.append({**r,**s,'pre_rows':len(bars)})
   else:
    bars=[x for x in bars if x['trade_date']>r['date']][:30]
    if len(bars)!=30:raise ValueError('NOT_30_TRADING_DAYS')
    if any(abs(bars[i]['open']/bars[i-1]['close']-1)>.35 for i in range(1,len(bars))):raise ValueError('POST_GAP_ABOVE35PCT')
   all_rows.extend(bars)
  except Exception as ex:
   status='CENSORED_SOURCE_NOT_IMPUTED';reason=type(ex).__name__+':'+str(ex)[:135];bars=[]
  audit.append({**r,'phase':phase,'status':status,'bars_retained':len(bars) if status=='VALID' else 0,'reason':reason})
  if idx%25==0:print('R1D26Q',phase,idx,'/125 valid',sum(x['status']=='VALID' for x in audit),flush=True)
  if sleep:time.sleep(sleep)
 cols=[*EVENT,'trade_date','open','high','low','close','volume','amount']
 csv_zip(p/(PRE if phase=='pre' else POST),all_rows,cols)
 csv_write(p/PRICE_AUD,audit,[*EVENT,'year','selection_basis','phase','status','bars_retained','reason'])
 if phase=='pre':
  csv_write(p/SUP,supports,[*EVENT,'year','selection_basis','support','signal_close','median_pre20_amount','median_pre20_volume','asof_support_status','pre_rows'])
 m=manifest(p,phase=phase,attempted=len(events),valid=sum(x['status']=='VALID' for x in audit),valid_bars=len(all_rows),support_sealed_before_future=phase=='pre',source='pykrx_KRX_unadjusted_FALSE')
 print('R1D26Q_PRICE_SEALED',json.dumps({k:m[k] for k in ('phase','attempted','valid','valid_bars')}),flush=True)
 return m

def f(x):return float(x)
def simulate_one(s,ft,protocol):
 if len(ft)!=30:raise ValueError('POST_NOT30')
 ft=sorted(ft,key=lambda x:x['trade_date']);support=f(s['support']);entry=f(ft[0]['open']);close=f(s['signal_close'])
 result={**{k:s[k] for k in EVENT},'year':s['date'][:4],'support':support,'signal_close':close,'entry_date':ft[0]['trade_date'], 'entry_open':entry, 'support_distance_pct':round((entry-support)/entry*100,4), 'eligibility':'', 'first_stop_date':'', 'break_close_date':'','reclaim_close_date':'','reentry_date':'','reentry_open':'','second_stop_date':'','second_stop':0,'reentered':0,'after_reentry_sessions':'', 'label':'','reclaim_amount_ratio':'', 'foreign_break_net_krw':'', 'foreign_break_net_pct':'', 'institution_break_net_pct':'','foreign_reclaim_net_pct':'','institution_reclaim_net_pct':'','hold_net_pct':round((f(ft[-1]['close'])/entry-1)*100-.2,6),'stop_only_net_pct':'', 'reentry_combined_net_pct':'', 'delta_reentry_vs_stop_pp':'','baseline_pre20_amount':s['median_pre20_amount'],'exposure_uncensored':0,'sweep_count_asof_reclaim':'','close_break_count_asof_reclaim':''}
 if s['asof_support_status']!='VALID_AT_SIGNAL_CLOSE' or not 0<support<min(close,entry):result['eligibility']='NO_ELIGIBLE_SUPPORT';return result
 if float(s['median_pre20_amount'])<=0:result['eligibility']='INVALID_PRIOR_VOLUME';return result
 result['eligibility']='ELIGIBLE_STATIC_SUPPORT'
 breach=next((i for i in range(len(ft)-1) if f(ft[i]['close'])<support),None)
 if breach is None:
  result.update(label='NO_INITIAL_STOP',stop_only_net_pct=result['hold_net_pct'],reentry_combined_net_pct=result['hold_net_pct']);return result
 exit_i=breach+1;exit_price=f(ft[exit_i]['open']);exit_return=(exit_price/entry-1)*100-.2
 result.update(first_stop_date=ft[exit_i]['trade_date'],break_close_date=ft[breach]['trade_date'], stop_only_net_pct=round(exit_return,6))
 # Reclaim on execution day close at earliest; cannot use breached close; buy next open only if >support.
 rec=next((j for j in range(exit_i,len(ft)-1) if f(ft[j]['close'])>=support),None)
 if rec is None:
  result.update(label='STOP_NO_RECLAIM',reentry_combined_net_pct=round(exit_return,6));return result
 ent=rec+1
 if f(ft[ent]['open'])<=support:
  result.update(reclaim_close_date=ft[rec]['trade_date'],label='RECLAIM_NEXT_OPEN_UNDER_SUPPORT_SKIP',reentry_combined_net_pct=round(exit_return,6));return result
 # Counts are based ONLY on observations through the reclaiming close.
 prev_up=True;close_breaks=0;sweeps=0
 for b in ft[:rec+1]:
  now_up=f(b['close'])>=support
  if prev_up and not now_up:close_breaks+=1
  if f(b['low'])<support and now_up:sweeps+=1
  prev_up=now_up
 result.update(reentered=1,reclaim_close_date=ft[rec]['trade_date'],reentry_date=ft[ent]['trade_date'],reentry_open=f(ft[ent]['open']),after_reentry_sessions=len(ft)-ent,sweep_count_asof_reclaim=sweeps,close_break_count_asof_reclaim=close_breaks)
 # stop if next CLOSE under static support, execute following OPEN.
 two=next((i for i in range(ent,len(ft)-1) if f(ft[i]['close'])<support),None)
 if two is None:
  second_price=f(ft[-1]['close']);result.update(label='SURVIVED_TO_D30',second_stop=0)
 else:
  second_price=f(ft[two+1]['open']);result.update(label='SECOND_STOP',second_stop=1,second_stop_date=ft[two+1]['trade_date'])
 second_return=(second_price/f(ft[ent]['open'])-1)*100-.2
 result['reentry_combined_net_pct']=round(exit_return+second_return,6)
 result['delta_reentry_vs_stop_pp']=round(second_return,6)
 result['reclaim_amount_ratio']=round(f(ft[rec]['amount'])/f(s['median_pre20_amount']),6)
 result['exposure_uncensored']=int(len(ft)-ent>=protocol['min_remaining_post_reentry_sessions'])
 if not result['exposure_uncensored'] and not result['second_stop']:result['label']='LATE_REENTRY_RIGHT_CENSORED'
 return result

def price_evaluate(sel,pre_dir,post_dir,p,out,d25_zip=None):
 dest=Path(out);dest.mkdir(parents=True,exist_ok=True)
 a=check_dir(pre_dir);b=check_dir(post_dir)
 if a['phase']!='pre' or b['phase']!='post':raise ValueError('INPUT_PHASE_WRONG')
 frozen=selection_load(sel,p)
 rr=read_csv_bytes((Path(pre_dir)/SUP).read_bytes());rrmap={(r['strategy'],r['date'],r['code']):r for r in rr}
 bars=read_csv_bytes((Path(post_dir)/POST).read_bytes(),True);postmap=defaultdict(list)
 for r in bars:postmap[tuple(r[k] for k in EVENT)].append(r)
 outrows=[];audit=[]
 for s in frozen:
  k=tuple(s[x] for x in EVENT)
  if k not in rrmap or k not in postmap: audit.append({**s,'status':'CENSORED_NO_SEALED_PRE_POST'});continue
  ft=postmap[k]
  try:
   d=simulate_one(rrmap[k],ft,p);outrows.append(d);audit.append({**s,'status':d['eligibility']})
  except Exception as ex:audit.append({**s,'status':'CENSORED_VALIDATION_'+type(ex).__name__})
 if not outrows:raise ValueError('NO_VALID_REAL_PRICE_CASES')
 csv_write(dest/RES,outrows,list(outrows[0]));csv_write(dest/PRICE_AUD,audit,[*EVENT,'year','selection_basis','status'])
 parity_n=0;comparable=0
 if d25_zip:
  if sha(Path(d25_zip).read_bytes())!=p['parent_d25_zip_sha256']:raise ValueError('D25_PARITY_ZIP_SHA_CHANGED')
  with zipfile.ZipFile(d25_zip) as z:ref=[r for r in read_csv_bytes(z.read(D25FUT),True) if r['horizon']=='30' and r['strategy']==D]
  refmap={(r['strategy'],r['date'],r['code']):r for r in ref};pdrows=[]
  for r in outrows:
   key=tuple(r[x] for x in EVENT);t=refmap.get(key);status='D25_REFERENCE_MISSING_OR_CENSORED'
   if t and t['status_out']=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED':
    comparable+=1
    if r['entry_date']==t['entry_day'] and len(postmap[key])==30 and sorted(postmap[key],key=lambda b:b['trade_date'])[-1]['trade_date']==t['exit_day'] and abs(float(r['hold_net_pct'])-float(t['net_20bps_pct']))<.01:status='PARITY_PASS';parity_n+=1
    else:status='D25_PRICE_PARITY_MISMATCH'
   pdrows.append({**{k:r[k] for k in EVENT},'source_status':t['status_out'] if t else '', 'reference_entry_day':t['entry_day'] if t else '', 'reference_exit_day':t['exit_day'] if t else '', 'reference_D30_net_pct':t['net_20bps_pct'] if t else '', 'actual_D30_net_pct':r['hold_net_pct'],'parity_status':status})
  csv_write(dest/PARITY,pdrows,[*EVENT,'source_status','reference_entry_day','reference_exit_day','reference_D30_net_pct','actual_D30_net_pct','parity_status'])
 m=manifest(dest,phase='price_evaluate',reference_price_comparable=comparable,reference_price_parity_pass=parity_n,signals_frozen=125,signals_complete=len(outrows),support_eligible=sum(x['eligibility']=='ELIGIBLE_STATIC_SUPPORT' for x in outrows),initial_stops=sum(bool(x['first_stop_date']) for x in outrows),reentries=sum(x['reentered'] for x in outrows),uncensored_reentries=sum(x['reentered'] and x['exposure_uncensored'] for x in outrows),post_source_sha256=b['file_sha256'][POST],pre_source_sha256=a['file_sha256'][PRE],selection_sha256=p['selection_sha256'])
 print('R1D26Q_POST_PRICE_RESULTS',json.dumps({z:m[z] for z in ('signals_frozen','signals_complete','support_eligible','initial_stops','reentries','uncensored_reentries')}),flush=True)
 return m

def normalize_net(df,dates,mode):
 if df is None or df.empty or not {'외국인합계','기관합계'}.issubset(df.columns):raise ValueError('FLOW_SCHEMA_EMPTY')
 m={}
 for dt,row in df.iterrows():
  key=pd.Timestamp(dt).date().isoformat()
  if key in m:raise ValueError('NET_DUPLICATE_DATE')
  v=[]
  for col in ('외국인합계','기관합계'):
   x=float(row[col])
   if not math.isfinite(x) or int(x)!=x:raise ValueError('NET_NOT_INTEGER')
   v.append(int(x))
  m[key]=v
 if not dates.issubset(m):raise ValueError('NET_MISSING_REQUIRED_DATES')
 return m

def fetch_flow(code,start,end,mode):
 from pykrx import stock
 x=stock.get_market_trading_value_by_date if mode=='krw' else stock.get_market_trading_volume_by_date
 return x(start.replace('-',''),end.replace('-',''),code,on='순매수')

def collect_flow(price_dir,post_dir,out,fetch=fetch_flow,sleep=1.2):
 dest=Path(out);dest.mkdir(parents=True,exist_ok=True);check_dir(price_dir);check_dir(post_dir)
 rows=read_csv_bytes((Path(price_dir)/RES).read_bytes());postrows=read_csv_bytes((Path(post_dir)/POST).read_bytes(),True)
 postmap=defaultdict(dict)
 for x in postrows:postmap[tuple(x[k] for k in EVENT)][x['trade_date']]=x
 audit=[];rr=[];todo=[r for r in rows if r['reentered']==1 or str(r['reentered'])=='1']
 for ix,r in enumerate(todo,1):
  dates={r['break_close_date'],r['reclaim_close_date']};code=r['code'];status='VALID';reason=''
  try:
   all_days=sorted(postmap[tuple(r[k] for k in EVENT)])
   if len(all_days)!=30:raise ValueError('NET_FETCH_POST_PRICE_NOT_30')
   start=all_days[0];end=all_days[-1]
   v=normalize_net(fetch(code,start,end,'krw'),dates,'krw')
   shares=normalize_net(fetch(code,start,end,'shares'),dates,'shares')
   row=postmap[tuple(r[k] for k in EVENT)]
   if not dates.issubset(row):raise ValueError('FLOW_PRICE_DAY_MISMATCH')
   for tag,day in [('break',r['break_close_date']),('reclaim',r['reclaim_close_date'])]:
    item=row[day];amount=float(item['amount']);volume=float(item['volume'])
    if amount<=0 or volume<=0:raise ValueError('ZERO_DAY_DENOM')
    rr.append({**{k:r[k] for k in EVENT},'phase':tag,'trade_date':day,'total_amount_krw':int(amount),'total_volume_shares':int(volume),'foreign_net_krw':v[day][0],'institution_net_krw':v[day][1],'foreign_net_shares':shares[day][0],'institution_net_shares':shares[day][1],'foreign_net_amount_pct':round(100*v[day][0]/amount,6),'institution_net_amount_pct':round(100*v[day][1]/amount,6),'source':'KRX_VIA_PYKRX_HISTORICAL_NOT_PIT'})
  except Exception as ex:
   status='CENSORED_FLOW_NO_IMPUTE';reason=type(ex).__name__+':'+str(ex)[:100]
   rr=[x for x in rr if not (x['date']==r['date'] and x['code']==r['code'])]
  audit.append({**{k:r[k] for k in EVENT},'status':status,'reason':reason})
  if ix%10==0:print('R1D26Q_NET',ix,'/',len(todo),'valid',sum(x['status']=='VALID' for x in audit),flush=True)
  if sleep:time.sleep(sleep)
 cols=[*EVENT,'phase','trade_date','total_amount_krw','total_volume_shares','foreign_net_krw','institution_net_krw','foreign_net_shares','institution_net_shares','foreign_net_amount_pct','institution_net_amount_pct','source']
 csv_write(dest/NET,rr,cols);csv_write(dest/NET_AUD,audit,[*EVENT,'status','reason'])
 m=manifest(dest,phase='flow_collect',attempted=len(todo),valid=sum(x['status']=='VALID' for x in audit),flow_rows=len(rr),both_phases_required=True,net_not_gross=True)
 print('R1D26Q_FLOW_COLLECTION',json.dumps({k:m[k] for k in ('attempted','valid','flow_rows')}),flush=True)
 return m

def rate(a):return sum(a)/len(a) if a else None

def year_bootstrap_diff(rows,feature,seed=20261027):
 import random
 rng=random.Random(seed);by=defaultdict(list)
 for r in rows:by[r['year']].append(r)
 vals=[]
 for i in range(2000):
  sample=[rng.choice(rr) for year,rr in sorted(by.items()) for _ in range(len(rr))]
  yes=[x['outcome_survived'] for x in sample if x[feature]];no=[x['outcome_survived'] for x in sample if not x[feature]]
  if yes and no:vals.append(rate(yes)-rate(no))
 vals.sort()
 return (round(vals[int(.025*len(vals))]*100,3),round(vals[int(.975*len(vals))]*100,3)) if len(vals)>500 else ('','')

def evaluate(price_dir,flow_dir,p,out):
 d=Path(out);d.mkdir(parents=True,exist_ok=True);a=check_dir(price_dir);b=check_dir(flow_dir)
 p_rows=read_csv_bytes((Path(price_dir)/RES).read_bytes());flows=read_csv_bytes((Path(flow_dir)/NET).read_bytes());fm=defaultdict(dict)
 for r in flows:
  k=tuple(r[z] for z in EVENT)
  if r['phase'] in fm[k]:raise ValueError('FLOW_PHASE_DUPLICATE')
  fm[k][r['phase']]=r
 assembled=[]
 for r in p_rows:
  if str(r['reentered'])!='1':continue
  k=tuple(r[z] for z in EVENT)
  if not {'break','reclaim'}.issubset(fm.get(k,{})):
   assembled.append({**r,'flow_status':'INCOMPLETE_CENSORED_NOT_CLASSIFIED','outcome_survived':'','foreign_break_pos':'','turnover_reclaim_ge2':'','joint_gate':''});continue
  q=fm[k];foreign_break=float(q['break']['foreign_net_krw']);foreign_break_pct=float(q['break']['foreign_net_amount_pct']); inst_break=float(q['break']['institution_net_amount_pct']); foreign_reclaim=float(q['reclaim']['foreign_net_amount_pct']); inst_reclaim=float(q['reclaim']['institution_net_amount_pct']);amount_ratio=float(r['reclaim_amount_ratio']); uncens=str(r['exposure_uncensored'])=='1'
  survived=str(r['second_stop'])=='0' and uncens
  status='EVALUABLE' if uncens or str(r['second_stop'])=='1' else 'LATE_REENTRY_CENSORED'
  assembled.append({**r,'flow_status':status,'outcome_survived':int(survived) if status=='EVALUABLE' else '', 'foreign_break_pos':int(foreign_break>0),'turnover_reclaim_ge2':int(amount_ratio>=p['reclaim_turnover_multiplier_gate']),'joint_gate':int(foreign_break>0 and amount_ratio>=p['reclaim_turnover_multiplier_gate']),'foreign_break_net_pct':foreign_break_pct,'institution_break_net_pct':inst_break,'foreign_reclaim_net_pct':foreign_reclaim,'institution_reclaim_net_pct':inst_reclaim})
 evaluable=[x for x in assembled if x['flow_status']=='EVALUABLE'];stat=[]
 for key,label in [('foreign_break_pos','H1_BREAK_FOREIGN_NET_POSITIVE'),('turnover_reclaim_ge2','H2_RECLAIM_AMOUNT_GE_2X'),('joint_gate','H3_JOINT_DESCRIPTIVE')]:
  yes=[x for x in evaluable if x[key]==1];no=[x for x in evaluable if x[key]==0];r1=rate([x['outcome_survived'] for x in yes]);r0=rate([x['outcome_survived'] for x in no]);ci=year_bootstrap_diff(evaluable,key)
  stat.append({'hypothesis':label,'evaluable_new_unique_D':len(evaluable),'gate_yes_n':len(yes),'gate_no_n':len(no),'yes_survived_n':sum(x['outcome_survived'] for x in yes),'no_survived_n':sum(x['outcome_survived'] for x in no),'yes_survived_pct':round(r1*100,3) if r1 is not None else '', 'no_survived_pct':round(r0*100,3) if r0 is not None else '', 'difference_pp':round((r1-r0)*100,3) if r1 is not None and r0 is not None else '', 'year_strat_bootstrap_low_pp':ci[0],'year_strat_bootstrap_high_pp':ci[1], 'decision':'DESCRIPTIVE_INSUFFICIENT_N' if min(len(yes),len(no))<p['min_per_gate_arm'] else 'HISTORICAL_HOLDOUT_ONLY_NOT_PROSPECTIVE_OOS'})
 yr=[]
 for y in map(str,range(2021,2026)):
  a=[q for q in evaluable if q['year']==y]
  yr.append({'year':y,'evaluable':len(a),'survived':sum(x['outcome_survived'] for x in a),'second_stopped':len(a)-sum(x['outcome_survived'] for x in a),'foreign_break_positive':sum(x['foreign_break_pos'] for x in a),'reclaim_amount_ge2x':sum(x['turnover_reclaim_ge2'] for x in a)})
 csv_write(d/RES,assembled,list(assembled[0]) if assembled else [*EVENT,'flow_status'])
 csv_write(d/SUM,stat,list(stat[0]));csv_write(d/YEAR,yr,list(yr[0]))
 m=manifest(d,phase='final',holdout_selection=125,real_complete_price=len(p_rows),reentries=len(assembled),new_flow_complete=b['valid'],new_evaluable=len(evaluable),gate_min_arm=p['min_per_gate_arm'],diagnostic_only=True,p_value_claimed=False,permissive_selection_from_future=False,pilot_codes_excluded=True)
 print('R1D26Q_HOLDOUT_REPORT_NOT_OOS',json.dumps({k:m[k] for k in ('holdout_selection','reentries','new_flow_complete','new_evaluable')}) ,flush=True)
 return m

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--mode',choices=['verify','pre','post','price_evaluate','flow','evaluate'],required=True);ap.add_argument('--protocol',default='R1D26Q_PREDECLARED_PROTOCOL.json');ap.add_argument('--d25');ap.add_argument('--selection',default=FROZEN);ap.add_argument('--pre-dir');ap.add_argument('--post-dir');ap.add_argument('--price-dir');ap.add_argument('--flow-dir');ap.add_argument('--out');ap.add_argument('--sleep-seconds',type=float,default=1.2);args=ap.parse_args();p=json.loads(Path(args.protocol).read_text())
 if not p['research_only'] or p['strategy_ready'] or p['live_changed'] or p['orders_placed']:raise ValueError('LIVE_OR_NOT_RESEARCH')
 if args.mode=='verify':
  verify_select(args.d25,p,args.selection);print('R1D26Q_OUTCOME_BLIND_D125_TICKER_DISJOINT_SELECTION_QA_PASS')
 elif args.mode in ('pre','post'):
  if args.mode=='post':p['pre_dir']=args.pre_dir
  collect_price(args.selection,p,args.out,args.mode,sleep=args.sleep_seconds)
 elif args.mode=='price_evaluate':price_evaluate(args.selection,args.pre_dir,args.post_dir,p,args.out,args.d25)
 elif args.mode=='flow':collect_flow(args.price_dir,args.post_dir,args.out,sleep=args.sleep_seconds)
 elif args.mode=='evaluate':evaluate(args.price_dir,args.flow_dir,p,args.out)
if __name__=='__main__':main()
