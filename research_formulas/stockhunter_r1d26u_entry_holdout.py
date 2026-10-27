#!/usr/bin/env python3
"""R1-D26U signal-time selected ticker-disjoint historical entry-location study.
No LIVE, ranking, orders, prospective OOS or certified fills. Python >=3.10.
Only ASOF D25 input is opened by freeze(), never the D25 future outcome file.
"""
from __future__ import annotations
import argparse,csv,gzip,hashlib,io,json,statistics,time,zipfile,math
from pathlib import Path
from collections import defaultdict,Counter

A='A_BELOW224_WAVE_PULLBACK'
D='D_LEADER_CONSOLIDATION_BREAKOUT'
EVENT=('strategy','date','code')
ASOF='reports/r1d25/result/historical/R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz'
D25_SHA='583579a9c009de248c87a590d343f9b762e87482dbac01e4379f0db8a792cff9'
PRE='U_PRE_120_OHLCV.csv.gz';POST='U_POST_D30_OHLCV.csv.gz'
SELECT='U_FROZEN_ASOF_200.csv';SUP='U_ASOF_SUPPORTS.csv';AUD='U_PRICE_AUDIT.csv'
MAN='U_SHA_MANIFEST.json'
OLD='data/R1D26L_FROZEN_60_ASOF_EVENTS.csv'
QSEL='data/R1D26Q_FROZEN_D125_ASOF_ONLY.csv'
RSEL='data/R1D26R_FROZEN_A_D_200_ASOF_ONLY.csv'
UFROZEN='data/R1D26U_FROZEN_A_D_200_ASOF_ONLY.csv'
def gitblob(b):return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\x00'+b).hexdigest()
COLUMNS=[*EVENT,'trade_date','open','high','low','close','volume','amount']
CONFIG='R1D26U_PREDECLARED_PROTOCOL.json'

def sha(b):return hashlib.sha256(b).hexdigest()
def digest(p):return sha(Path(p).read_bytes())
def read_csv(blob,compressed=False):
 if compressed:blob=gzip.decompress(blob)
 return list(csv.DictReader(io.StringIO(blob.decode('utf-8-sig'))))
def readfile(p):return read_csv(Path(p).read_bytes())
def write_csv(p,rows,cols):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w',encoding='utf8',newline='') as f:
  w=csv.DictWriter(f,cols,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rows)
def write_gz(p,rows,cols):
 s=io.StringIO();w=csv.DictWriter(s,cols,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rows)
 with Path(p).open('wb') as f:
  with gzip.GzipFile(fileobj=f,mode='wb',mtime=0,filename='') as g:g.write(s.getvalue().encode('utf8'))
def seal(path,phase,**kw):
 p=Path(path);p.mkdir(parents=True,exist_ok=True)
 out={'study':'R1-D26U','phase':phase,'research_only':True,'strategy_ready':False,'live_changed':False,'orders_placed':0,
      'historical_ticker_disjoint':True,'prospective_OOS':False,'PIT_certified':False,'fills_certified':False,**kw}
 out['file_sha256']={x.name:digest(x) for x in sorted(p.iterdir()) if x.is_file() and x.name!=MAN}
 (p/MAN).write_text(json.dumps(out,indent=2,ensure_ascii=False,sort_keys=True)+'\n',encoding='utf8');return out

def verify_seal(p,phase=None):
 p=Path(p);m=json.loads((p/MAN).read_text(encoding='utf8'))
 if phase and m['phase']!=phase:raise ValueError('SEAL_PHASE_CHANGED')
 if not m['research_only'] or m['live_changed'] or m['orders_placed'] or m['strategy_ready'] or m['prospective_OOS']:raise ValueError('RESEARCH_GUARD_CHANGED')
 for name,d in m['file_sha256'].items():
  if digest(p/name)!=d:raise ValueError('SEALED_SOURCE_DRIFT:'+name)
 return m

def protocol(p=CONFIG):
 z=json.loads(Path(p).read_text(encoding='utf8'))
 if not z['research_only'] or z['strategy_ready'] or z['live_changed'] or z['orders_placed']!=0 or z['prospective_OOS']:
  raise ValueError('LIVE_FORBIDDEN')
 if z['parent_d25_zip_sha256']!=D25_SHA:raise ValueError('D25_PROTOCOL_MISMATCH')
 return z

def select_only_asof(d25, excludes, cfg):
 """No future outcomes used. Returns deterministic outcome-blind selection."""
 with zipfile.ZipFile(io.BytesIO(d25)) as z:pool=read_csv(z.read(ASOF),True)
 used=set(excludes);chosen=[]
 for strat in (A,D):
  for year in map(str,range(2021,2026)):
   elig=[r for r in pool if r['strategy']==strat and r['date'][:4]==year and r['code'] not in excludes]
   elig.sort(key=lambda r:(sha(f"R1D26U_ASOF_LOCK_V1|{strat}|{year}|{r['date']}|{r['code']}".encode()),r['date'],r['code']))
   count=0
   for r in elig:
    if r['code'] in used:continue
    if strat==A and not float(r['signal_close'])<float(r['ma224_recomputed']):raise ValueError('A_NOT_BELOW224')
    chosen.append({'strategy':strat,'date':r['date'],'code':r['code'],'year':year,'selection_basis':'OUTCOME_BLIND_SHA_NO_D25_FUTURE'})
    used.add(r['code']);count+=1
    if count==cfg['per_year_per_strategy']:break
   if count!=cfg['per_year_per_strategy']:raise ValueError('NOT_ENOUGH_DISJOINT_SIGNALS:'+strat+':'+year)
 return chosen

def freeze(d25_path,out,cfg):
 d25=Path(d25_path).read_bytes()
 if sha(d25)!=D25_SHA:raise ValueError('D25_ARCHIVE_SHA_MISMATCH')
 excluded=set();exclusion_sha={}
 for p in (OLD,QSEL,RSEL):
  exclusion_sha[p]=digest(p)
  if gitblob(Path(p).read_bytes())!=cfg['exclusion_git_blob_sha'][p]:raise ValueError('FROZEN_EXCLUSION_GIT_BLOB_DRIFT:'+p)
  excluded|={r['code'] for r in readfile(p)}
 chosen=select_only_asof(d25,excluded,cfg)
 if digest(UFROZEN)!=cfg['frozen_selection_sha256']:raise ValueError('U_PREDECLARED_SELECTION_SHA_CHANGED')
 frozen_bytes=Path(UFROZEN).read_bytes()
 stream=io.StringIO();writer=csv.DictWriter(stream,[*EVENT,'year','selection_basis'],lineterminator='\n');writer.writeheader();writer.writerows(chosen)
 if stream.getvalue().encode('utf8')!=frozen_bytes:raise ValueError('U_PREDECLARED_SELECTION_RECOMPUTATION_DIFFERENT')
 root=Path(out);root.mkdir(parents=True,exist_ok=True)
 write_csv(root/SELECT,chosen,[*EVENT,'year','selection_basis'])
 with zipfile.ZipFile(io.BytesIO(d25)) as z:
  asof_sha=sha(z.read(ASOF))
 if asof_sha!=cfg['parent_d25_asof_sha256']:raise ValueError('D25_ASOF_FILE_CHANGED')
 return seal(root,'selection',n_selected=len(chosen),n_old_ticker_exclusions=len(excluded),
             excluded_source_sha256=exclusion_sha,D25_zip_sha256=D25_SHA,D25_asof_sha256=asof_sha,
             frozen_selection_sha256=digest(root/SELECT), selection_ready_before_post=True)

def selection(selection_dir,cfg):
 m=verify_seal(selection_dir,'selection')
 if m['D25_zip_sha256']!=D25_SHA or m['D25_asof_sha256']!=cfg['parent_d25_asof_sha256']:raise ValueError('ORIGIN_MISMATCH')
 if digest(Path(selection_dir)/SELECT)!=cfg['frozen_selection_sha256'] or digest(UFROZEN)!=cfg['frozen_selection_sha256']:raise ValueError('LOCKED_U_SELECTION_DRIFT')
 rows=readfile(Path(selection_dir)/SELECT)
 if len(rows)!=2*5*cfg['per_year_per_strategy'] or len({r['code'] for r in rows})!=len(rows):raise ValueError('INVALID_SELECTION')
 for st in (A,D):
  for yr in map(str,range(2021,2026)):
   if sum(r['strategy']==st and r['year']==yr for r in rows)!=cfg['per_year_per_strategy']:raise ValueError('UNBALANCED_SELECTION')
 for p in (OLD,QSEL,RSEL):
  if digest(p)!=m['excluded_source_sha256'][p]:raise ValueError('EXCLUDED_SOURCE_CHANGED')
 return rows

def wave_candidate(bars,cfg):
 """S1 ASOF A support: latest confirmed high; last two followup bars observed."""
 n=len(bars)
 if n<85:return None
 peaks=[];params=cfg['A_wave_support']
 for j in range(max(params['previous_trough_bars'],n-params['peak_scan_bars']),n-3):
  top=float(bars[j]['high']);lo=min(float(x['low']) for x in bars[j-params['previous_trough_bars']:j])
  if lo<=0 or top<=max(float(bars[k]['high']) for k in (j-2,j-1)):continue
  if top<max(float(bars[k]['high']) for k in (j+1,j+2)):continue
  rise=(top/lo-1)*100
  if rise<params['min_wave_rise_pct']:continue
  post=min(float(x['low']) for x in bars[j+1:]); retrace=(top-post)/(top-lo)
  if not params['retrace_min']<=retrace<=params['retrace_max']:continue
  if float(bars[-1]['close'])<=post:continue
  peaks.append({'support':post,'peak_date':bars[j]['trade_date'],'confirmation_date':bars[j+2]['trade_date'],
                'wave_rise_pct':round(rise,5),'retrace_fraction':round(retrace,5)})
 return peaks[-1] if peaks else None

def pre_support(event,bars,cfg):
 if len(bars)<85 or bars[-1]['trade_date']!=event['date'] or any(x['trade_date']>event['date'] for x in bars):raise ValueError('ASOF_HISTORY_REQUIRED')
 if any(float(x['close'])<=0 for x in bars):raise ValueError('NONPOSITIVE_BAR')
 st=event['strategy'];s0=(min(float(b['low']) for b in bars[-20:]) if st==A else max(float(b['high']) for b in bars[-21:-1]))
 w=wave_candidate(bars,cfg) if st==A else None
 s1=w['support'] if w else ''
 return {**event,'signal_close':float(bars[-1]['close']),'S0_support':s0,'S1_A_support':s1,
         'S0_valid':int(0<s0<float(bars[-1]['close'])),'S1_valid':int(w is not None and 0<s1<float(bars[-1]['close'])),
         'S1_peak_date':w['peak_date'] if w else '', 'S1_confirmed_date':w['confirmation_date'] if w else '',
         'S1_wave_rise_pct':w['wave_rise_pct'] if w else '', 'S1_retrace_fraction':w['retrace_fraction'] if w else '',
         'pre_rows':len(bars)}

def check_bars(raw,evt,phase):
 bars=sorted(raw,key=lambda x:x['trade_date'])
 if len({x['trade_date'] for x in bars})!=len(bars):raise ValueError('DUPLICATE_TRADING_DATES')
 if phase=='pre':bars=[x for x in bars if x['trade_date']<=evt['date']][-120:]
 else:bars=[x for x in bars if x['trade_date']>evt['date']][:30]
 if phase=='pre' and (len(bars)<85 or bars[-1]['trade_date']!=evt['date']):raise ValueError('INSUFFICIENT_PRE')
 if phase=='post' and len(bars)!=30:raise ValueError('POST_NOT_D30')
 for i,b in enumerate(bars):
  if not 0<float(b['low'])<=min(float(b['open']),float(b['close']))<=max(float(b['open']),float(b['close']))<=float(b['high']):raise ValueError('INVALID_OHLC')
  if i and abs(float(b['open'])/float(bars[i-1]['close'])-1)>.35:raise ValueError('GAP_CORPORATE_ACTION_RISK')
 return bars

def collect(phase,selection_dir,pre_dir,out,cfg,fetch=None,sleep=0.8):
 if phase not in ('pre','post'):raise ValueError('BAD_PHASE')
 if fetch is None:
  from research_formulas import stockhunter_r1d26q_independent_holdout as q
  fetch=q.get_price
 ev=selection(selection_dir,cfg)
 if phase=='post':
  m=verify_seal(pre_dir,'pre');assert m['selection_sha256']==digest(Path(selection_dir)/SELECT)
 root=Path(out);root.mkdir(parents=True,exist_ok=True)
 history=[];supports=[];audit=[]
 for i,r in enumerate(ev,1):
  try:
   bars=check_bars(fetch(r,phase),r,phase)
   if phase=='pre':supports.append(pre_support(r,bars,cfg))
   history.extend(bars);status='VALID';reason=''
  except Exception as ex:status='CENSORED_NO_IMPUTATION';reason=type(ex).__name__+':'+str(ex)[:125]
  audit.append({**r,'phase':phase,'status':status,'reason':reason})
  if i%25==0:print('R1D26U',phase,i,'/',len(ev),'valid',sum(x['status']=='VALID' for x in audit),flush=True)
  if sleep:time.sleep(sleep)
 write_gz(root/(PRE if phase=='pre' else POST),history,COLUMNS)
 write_csv(root/AUD,audit,[*EVENT,'year','selection_basis','phase','status','reason'])
 if phase=='pre':
  cols=[*EVENT,'year','selection_basis','signal_close','S0_support','S1_A_support','S0_valid','S1_valid','S1_peak_date','S1_confirmed_date','S1_wave_rise_pct','S1_retrace_fraction','pre_rows']
  write_csv(root/SUP,supports,cols)
 return seal(root,phase,attempted=len(ev),valid=sum(x['status']=='VALID' for x in audit),selection_sha256=digest(Path(selection_dir)/SELECT),
             support_sealed_before_future=phase=='pre',source='KRX_via_pykrx_adjusted_false')

def simulate(bars,signal_close,support,policy,cfg,fill='conservative'):
 """D0 close -> pending instruction -> next-session OPEN. No LOW touch fills.
 Delayed cap entries pay cap in conservative bound and opening price in optimistic.
 Return percentage of initial notional (zero for cash); second tranche weighted.
 """
 if policy not in ('E0','E1','E2','E3'):raise ValueError('UNKNOWN_POLICY')
 if not (support>0 and signal_close>support and bars and bars[0]['trade_date']):return dict(net_pct=0.,traded=0,added=0,capital_days=0.,stop_events=0,late_stop=0,entry_day='',cash_fraction=1.,MAE_pct='',MFE_pct='')
 end=len(bars)-1;limit_days=cfg['entry_limit_sessions'];min_remain=cfg['minimum_post_entry_sessions']
 fee=cfg['roundtrip_fee_bps']/100
 policy_cfg=cfg['policies']
 first=None;retest=(signal_close>=support and signal_close<=support*(1+policy_cfg['retest_close_max_pct']/100))
 # pending D0 for E0 market or E1/E2 limit; E3 only after second close confirms
 pending={'kind':'buy','part':'first','limit':None} if policy=='E0' else (
   {'kind':'buy','part':'first','limit':support*(1+policy_cfg['first_limit_cap_pct']/100)} if policy in ('E1','E2') and retest else None)
 rets=[];openlots=[];added=0;stop_events=0;late_stop=0;capital_days=0.;entry_day='';first_price=None;first_exit_day=None;first_retest_day=0 if retest else None
 for day,b in enumerate(bars,1):
  o,c=float(b['open']),float(b['close'])
  if pending and pending['kind']=='stop':
   for lot in openlots:rets.append(lot['share']*((o/lot['px']-1)*100-fee))
   openlots=[];pending=None;stop_events+=1
   if first_exit_day is None:first_exit_day=day-1
   # policy makes one initial entry only; stopped entry may not reenter
  elif pending and pending['kind']=='buy':
   ticket=pending;pending=None
   if ticket['limit'] is None or o<=ticket['limit']:
    px=o if ticket['limit'] is None or fill=='optimistic' else ticket['limit']
    share=(.5 if policy=='E2' and ticket['part']=='first' else (1. if ticket['part']=='first' else .5))
    openlots.append({'share':share,'px':px,'day':day})
    if first is None:first=day;entry_day=b['trade_date'];first_price=px
    else:added=1
  capital_days+=sum(x['share'] for x in openlots)
  if day==len(bars):
   if openlots and c<support:late_stop=1
   break
  if openlots and c<support:
   pending={'kind':'stop'};continue
  if stop_events or (first is not None and not openlots):continue
  # All new entry orders have at least min_remain sessions after fill.
  can_buy=(day+1<=limit_days and len(bars)-(day+1)>=min_remain)
  if first is not None:
   if policy=='E2' and len(openlots)==1 and not added and can_buy and day>=first+1:
    prev=signal_close if day==1 else float(bars[day-2]['close'])
    if prev>=support and c>=support and c>prev:
     pending={'kind':'buy','part':'second','limit':support*(1+policy_cfg['add_limit_cap_pct']/100)}
   continue
  if not can_buy:continue
  near=support<=c<=support*(1+policy_cfg['retest_close_max_pct']/100)
  if policy in ('E1','E2') and near:
   pending={'kind':'buy','part':'first','limit':support*(1+policy_cfg['first_limit_cap_pct']/100)}
  elif policy=='E3':
   if first_retest_day is not None and day>first_retest_day:
    prev=signal_close if day==1 else float(bars[day-2]['close'])
    if prev>=support and c>=support and c>prev:
     pending={'kind':'buy','part':'first','limit':support*(1+policy_cfg['add_limit_cap_pct']/100)};first_retest_day=None
   if not pending:
    if near:first_retest_day=day
    elif c<support:first_retest_day=None
 if openlots:
  lastc=float(bars[-1]['close'])
  rets.extend(l['share']*((lastc/l['px']-1)*100-fee) for l in openlots)
 traded=int(first is not None)
 # MAE/MFE uses full excursion from executed leg entry; descriptive not time-order executable.
 extrema=[]
 if traded:
  # First lot only, through its last invested session; no post-stop extrema contamination.
  ref=float(first_price)
  through=first_exit_day if first_exit_day is not None else len(bars)
  active_bars=bars[first-1:through]
  extrema=[(min(float(x['low']) for x in active_bars)/ref-1)*100,(max(float(x['high']) for x in active_bars)/ref-1)*100]
 return {'net_pct':round(sum(rets),7),'traded':traded,'added':added,'capital_days':round(capital_days,4),
         'stop_events':stop_events,'late_stop':late_stop,'entry_day':entry_day,
         'cash_fraction':round(1-capital_days/len(bars),5),'MAE_pct':round(extrema[0],4) if extrema else '',
         'MFE_pct':round(extrema[1],4) if extrema else ''}

def evaluation(selection_dir,pre_dir,post_dir,out,cfg):
 sel=selection(selection_dir,cfg);mp=verify_seal(pre_dir,'pre');mf=verify_seal(post_dir,'post')
 if mf['selection_sha256']!=mp['selection_sha256']:raise ValueError('SELECTION_PRE_POST_MISMATCH')
 if not mp['support_sealed_before_future']:raise ValueError('NO_ASOF_SEAL')
 supports={(x['strategy'],x['date'],x['code']):x for x in readfile(Path(pre_dir)/SUP)}
 posts=read_csv((Path(post_dir)/POST).read_bytes(),True);groups=defaultdict(list)
 for r in posts:groups[tuple(r[k] for k in EVENT)].append(r)
 results=[];censored=[]
 for r in sel:
  k=tuple(r[k0] for k0 in EVENT)
  if k not in supports or len(groups[k])!=30:
   censored.append({**r,'status':'PRE_OR_POST_MISSING_NOT_IMPUTED'});continue
  bars=sorted(groups[k],key=lambda x:x['trade_date'])
  if len({b['trade_date'] for b in bars})!=30:raise ValueError('POST_DUPLICATE')
  s=supports[k]
  for stype in (('S0','S1') if r['strategy']==A else ('S0',)):
   level=float(s[f'{stype}_support' if stype=='S0' else 'S1_A_support'] or 0)
   valid=bool(int(s[f'{stype}_valid']))
   for pol in ('E0','E1','E2','E3'):
    for fill in (('market_open',) if pol=='E0' else ('conservative','optimistic')):
     obj=simulate(bars,float(s['signal_close']),level if valid else 0.,pol,cfg,fill)
     results.append({**r,'support_type':stype,'candidate_available':int(valid),'support':level,'risk_distance_pct':round((float(bars[0]['open'])-level)/float(bars[0]['open'])*100,4) if valid else '',
                     'policy':pol,'fill_bound':fill,**obj})
 root=Path(out);root.mkdir(parents=True,exist_ok=True)
 cols=list(dict.fromkeys(key for x in results for key in x));write_csv(root/'U_CASE_LEVEL.csv',results,cols)
 write_csv(root/'U_CENSORED.csv',censored,[*EVENT,'year','selection_basis','status'])
 sums=[]
 for st in (A,D):
  for stype in (('S0','S1') if st==A else ('S0',)):
   for pol in ('E0','E1','E2','E3'):
    for bound in (('market_open',) if pol=='E0' else ('conservative','optimistic')):
     x=[t for t in results if t['strategy']==st and t['support_type']==stype and t['policy']==pol and t['fill_bound']==bound]
     if not x:continue
     nums=[z['net_pct'] for z in x];tr=[z for z in x if z['traded']];tv=[z['net_pct'] for z in tr]
     sums.append({'strategy':st,'support_type':stype,'entry_policy':pol,'fill_bound':bound,
        'n_selected':sum(r['strategy']==st for r in sel),'n_evaluable':len(x),'n_censored':sum(r['strategy']==st for r in censored),
        'n_candidate':sum(z['candidate_available'] for z in x),'n_traded':len(tr),'n_untraded':len(x)-len(tr),
        'n_second_half':sum(z['added'] for z in tr),'n_stop':sum(z['stop_events'] for z in tr),
        'n_minus10':sum(t<=-10 for t in nums),'n_minus10_traded':sum(t<=-10 for t in tv),
        'avg_all_signal_pct':round(statistics.mean(nums),4),'median_all_signal_pct':round(statistics.median(nums),4),
        'avg_traded_pct':round(statistics.mean(tv),4) if tv else '',
        'median_traded_pct':round(statistics.median(tv),4) if tv else '',
        'win_pct_all':round(100*sum(t>0 for t in nums)/len(nums),3),
        'avg_cash_fraction':round(statistics.mean(t['cash_fraction'] for t in x),4),
        'mean_ex_top2_pct':round(statistics.mean(sorted(nums)[:-2]),4) if len(nums)>2 else '',
        'mean_capital_days':round(statistics.mean(t['capital_days'] for t in x),4)})
 write_csv(root/'U_SUMMARY.csv',sums,list(sums[0].keys()))
 return seal(root,'evaluate',n_selected=len(sel),n_evaluable=len(sel)-len(censored),n_censored=len(censored),
             label='HISTORICAL_TICKER_DISJOINT_NOT_FUTURE_OOS_NOT_PIT',original_selection_sha256=digest(Path(selection_dir)/SELECT),
             support_seal_sha256=digest(Path(pre_dir)/SUP))

def cli():
 ap=argparse.ArgumentParser();ap.add_argument('--mode',choices=['freeze','pre','post','evaluate','verify'],required=True)
 ap.add_argument('--protocol',default=CONFIG);ap.add_argument('--d25');ap.add_argument('--selection-dir',default='reports/r1d26u/result/selection')
 ap.add_argument('--pre-dir',default='reports/r1d26u/result/pre');ap.add_argument('--post-dir',default='reports/r1d26u/result/post');ap.add_argument('--out');ap.add_argument('--sleep-seconds',type=float,default=.8)
 a=ap.parse_args();p=protocol(a.protocol)
 if a.mode=='freeze':result=freeze(a.d25,a.out or a.selection_dir,p)
 elif a.mode=='verify':result=verify_seal(a.out or a.selection_dir)
 elif a.mode in ('pre','post'):result=collect(a.mode,a.selection_dir,a.pre_dir,a.out or (a.pre_dir if a.mode=='pre' else a.post_dir),p,sleep=a.sleep_seconds)
 else:result=evaluation(a.selection_dir,a.pre_dir,a.post_dir,a.out or 'reports/r1d26u/result/comparison',p)
 print('R1D26U_'+a.mode.upper()+'_RESEARCH_ONLY_HISTORICAL_NOT_OOS',json.dumps({k:v for k,v in result.items() if k!='file_sha256'},ensure_ascii=False))
if __name__=='__main__':cli()
