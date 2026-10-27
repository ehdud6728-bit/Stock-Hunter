#!/usr/bin/env python3
"""R1-D26R: frozen historical A vs D support failures and pre-entry instability.
RESEARCH_ONLY. Fresh historical ticker-disjoint ASOF selection; NOT prospective OOS.
No live/rank/order code, and future outcomes never determine selection or entry gate.
"""
from __future__ import annotations
import argparse,csv,hashlib,io,json,zipfile,statistics,time,math,random
from collections import defaultdict
from pathlib import Path
from research_formulas import stockhunter_r1d26q_independent_holdout as q

A='A_BELOW224_WAVE_PULLBACK'
D=q.D
EVENT=q.EVENT
ASOF=q.D25ASOF
PRE='R1D26R_PRE_SIGNAL_120_OHLCV.csv.gz'
POST='R1D26R_POST_SIGNAL_30_OHLCV.csv.gz'
SUP='R1D26R_ASOF_SUPPORT_AND_PRE_GATE.csv'
CASE='R1D26R_200_FROZEN_CASES.csv'
SUMMARY='R1D26R_A_D_PORTFOLIO_PAIRED_SUMMARY.csv'
RISK='R1D26R_EARLY_WARNING_SUBGROUPS.csv'
PARITY='R1D26R_D25_D30_PRICE_PARITY_AUDIT.csv'
AUDIT='R1D26R_SOURCE_AUDIT.csv'
FROZEN='data/R1D26R_FROZEN_A_D_200_ASOF_ONLY.csv'
QSEL='data/R1D26Q_FROZEN_D125_ASOF_ONLY.csv'
OLD=q.PILOT
MAN='R1D26R_QA_MANIFEST.json'
F='reports/r1d25/result/historical/R1D25_D5_D10_D15_D20_D30_FUTURE_PRICE_PATHS.csv.gz'

def sha(b):return hashlib.sha256(b).hexdigest()
def digest_file(p):return sha(Path(p).read_bytes())
def output(path,rows,cols):q.csv_write(path,rows,cols)
def file_seal(d,phase,**extras):
 root=Path(d);root.mkdir(parents=True,exist_ok=True)
 m={'version':'R1D26R_FROZEN_HISTORICAL_A_D_200_V1','phase':phase,'research_only':True,'prospective_OOS':False,
 'strategy_ready':False,'orders_placed':0,'live_changed':False,'independent_KRX_crosscheck':'PENDING',
 'corporate_action_PIT':'NOT_CERTIFIED','historical_unadjusted_prices':True,'fills_certified':False,**extras}
 m['file_sha256']={f.name:digest_file(f) for f in root.iterdir() if f.is_file() and f.name!=MAN}
 (root/MAN).write_text(json.dumps(m,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf8');return m

def check_seal(d):
 root=Path(d);m=json.loads((root/MAN).read_text())
 assert m['research_only'] and not m['strategy_ready'] and not m['live_changed'] and m['orders_placed']==0,'RESEARCH_ONLY_GUARD'
 for name,expected in m['file_sha256'].items():
  if sha((root/name).read_bytes())!=expected:raise ValueError('SOURCE_BYTES_DRIFT:'+name)
 return m

def choose_events(d25_zip, oldpilot, qsel, n=20):
 with zipfile.ZipFile(io.BytesIO(d25_zip)) as z:pool=q.read_csv_bytes(z.read(ASOF),True)
 forbidden={r['code'] for r in q.read_csv_bytes(oldpilot)}|{r['code'] for r in q.read_csv_bytes(qsel)}
 used=set(forbidden);result=[]
 for st in (A,D):
  for year in map(str,range(2021,2026)):
   candidates=[r for r in pool if r['strategy']==st and r['date'][:4]==year and r['code'] not in forbidden]
   candidates.sort(key=lambda r:(sha(f"R1D26R_LOCK_V1|{st}|{year}|{r['date']}|{r['code']}".encode()),r['date'],r['code']))
   selected=0
   for r in candidates:
    if r['code'] in used:continue
    if st==A and not float(r['signal_close'])<float(r['ma224_recomputed']):raise ValueError('A_NOT_ACTUALLY_BELOW224')
    result.append({'strategy':st,'date':r['date'],'code':r['code'],'year':year,'selection_basis':'OUTCOME_BLIND_SHA_TICKER_DISJOINT_D25'})
    used.add(r['code']);selected+=1
    if selected==n:break
   if selected!=n:raise ValueError('INSUFFICIENT_A_D_NEW_TICKERS_'+st+'_'+year)
 return result

def selection(path,p):
 raw=Path(path).read_bytes()
 if sha(raw)!=p['selection_sha256']:raise ValueError('R_SELECTION_SHA_DRIFT')
 rows=q.read_csv_bytes(raw)
 if len(rows)!=200 or len(set(r['code'] for r in rows))!=200:raise ValueError('R_SELECTION_IDENTITY_DRIFT')
 if any(sum(r['strategy']==st and r['year']==yr for r in rows)!=20 for st in (A,D) for yr in map(str,range(2021,2026))):raise ValueError('R_SELECTION_BALANCE_DRIFT')
 return rows

def verify(d25_path,p):
 if digest_file(d25_path)!=p['parent_d25_zip_sha256']:raise ValueError('PARENT_D25_BYTES_CHANGED')
 with zipfile.ZipFile(d25_path) as z:
  if sha(z.read(ASOF))!=p['d25_asof_csv_gz_sha256']:raise ValueError('PARENT_ASOF_SOURCE_CHANGED')
 for path,key in [(OLD,'pilot_selection_sha256'),(QSEL,'q_selection_sha256')]:
  if digest_file(path)!=p[key]:raise ValueError('PARENT_SELECTION_CHANGED:'+path)
 expected=choose_events(Path(d25_path).read_bytes(),Path(OLD).read_bytes(),Path(QSEL).read_bytes())
 if selection(FROZEN,p)!=expected:raise ValueError('FROZEN_R_NOT_ASOF_RECOMPUTABLE')
 return expected

def preinstability(bars,window=40,lookback=20,recovery=3):
 """Count prior20-MA DOWN-cross -> UP-recovery in <=3 sessions, as-of signal day.
 Uses previous 20 CLOSES, *not* the candidate bar itself for MA; counts distinct
 down-cross episodes, and requires fully observed recovery by signal CLOSE.
 """
 if len(bars)<window+lookback+1:raise ValueError('PRE_ROLLING_MA_HISTORY_TOO_SHORT')
 # Close observations at the end of each day, all <= signal date.
 down=None;count=0
 for i in range(len(bars)-window,len(bars)):
  prev20=sum(float(x['close']) for x in bars[i-lookback:i])/lookback
  was_below=float(bars[i-1]['close'])<sum(float(x['close']) for x in bars[i-lookback-1:i-1])/lookback
  now_below=float(bars[i]['close'])<prev20
  if now_below and not was_below:down=i
  if not now_below and was_below and down is not None:
   if i-down<=recovery:count+=1
   down=None
  if down is not None and i-down>recovery:down=None
 return count

def pre_support(bars,evt):
 if len(bars)<85 or bars[-1]['trade_date']!=evt['date'] or any(x['trade_date']>evt['date'] for x in bars):raise ValueError('ASOF_PRE_BARS_REQUIRED')
 if evt['strategy']==D:
  level=max(x['high'] for x in bars[-21:-1]);label='D_PRE20_BREAKOUT_RETEST';reference=q.support_from_pre(bars,evt)
  assert level==reference['support']
 else:
  level=min(x['low'] for x in bars[-20:]);label='A_PULLBACK_LOW20_INCLUDING_SIGNAL';reference={'median_pre20_amount':statistics.median(x['amount'] for x in bars[-21:-1]),'median_pre20_volume':statistics.median(x['volume'] for x in bars[-21:-1]),'signal_close':bars[-1]['close']}
 num=preinstability(bars);status='VALID_AT_SIGNAL_CLOSE' if level>0 and level<bars[-1]['close'] and reference['median_pre20_amount']>0 else 'ASOF_LEVEL_UNUSABLE'
 return {**evt,'support':level,'support_name':label,'signal_close':bars[-1]['close'],'median_pre20_amount':reference['median_pre20_amount'],'median_pre20_volume':reference['median_pre20_volume'],'pre_ma20_fast_recoveries':num,'pre_gate_skip':int(num>=2),'asof_support_status':status,'pre_rows':len(bars)}

def collect(phase,p,out,sleep=0.8,fetch=q.get_price):
 root=Path(out);root.mkdir(parents=True,exist_ok=True);rows=selection(FROZEN,p);allbars=[];audit=[];supports=[]
 if phase=='post':
  m=check_seal(p['pre_dir']);assert m['phase']=='pre' and m['attempted']==200 and m['pre_decision_sealed'], 'PRE_FEATURES_NOT_SEALED'
 for i,r in enumerate(rows,1):
  result='VALID';reason='';bars=[]
  try:
   bars=fetch(r,phase)
   if phase=='pre':
    bars=[b for b in bars if b['trade_date']<=r['date']][-120:]
    if len(bars)<85 or bars[-1]['trade_date']!=r['date']:raise ValueError('PRE_NOT85_OR_MISSING_SIGNAL_DAY')
    x=pre_support(bars,r);supports.append(x)
   else:
    bars=[b for b in bars if b['trade_date']>r['date']][:30]
    if len(bars)!=30:raise ValueError('POST_MISSING_D30')
   if len(set(b['trade_date'] for b in bars))!=len(bars):raise ValueError('DATE_DUPLICATED')
   if any(abs(bars[j]['open']/bars[j-1]['close']-1)>.35 for j in range(1,len(bars))):raise ValueError('LARGE_GAP_CORPORATE_ACTION_RISK')
  except Exception as ex:result='CENSORED_NO_IMPUTATION';reason=type(ex).__name__+':'+str(ex)[:120]
  if result=='VALID':allbars.extend(bars)
  else:
   if phase=='pre' and supports and supports[-1]['code']==r['code'] and supports[-1]['date']==r['date']:supports.pop()
  audit.append({**r,'phase':phase,'status':result,'rows':len(bars) if result=='VALID' else 0,'reason':reason})
  if i%25==0:print('R1D26R',phase,i,'/200 valid',sum(x['status']=='VALID' for x in audit),flush=True)
  if sleep:time.sleep(sleep)
 q.csv_zip(root/(PRE if phase=='pre' else POST),allbars,q.COLS)
 output(root/AUDIT,audit,[*EVENT,'year','selection_basis','phase','status','rows','reason'])
 if phase=='pre':output(root/SUP,supports,[*EVENT,'year','selection_basis','support','support_name','signal_close','median_pre20_amount','median_pre20_volume','pre_ma20_fast_recoveries','pre_gate_skip','asof_support_status','pre_rows'])
 return file_seal(root,phase,attempted=200,valid=sum(x['status']=='VALID' for x in audit),valid_bars=len(allbars),pre_decision_sealed=phase=='pre',source='KRX_via_pykrx_adjusted_false')

def simulate(s,bars,p):
 """One complete signal; no exit can occur before its observable decision. Returns % of
 initial notional; second leg gain uses reentry share gain as additive fixed-notional
 research proxy, NOT portfolio compounding, position sizing or certified execution.
 """
 support=float(s['support']);ft=sorted(bars,key=lambda x:x['trade_date']);entry=float(ft[0]['open']);close=float(s['signal_close']);cost=p['cost_round_trip_bps']/100
 out={**{k:s[k] for k in EVENT},'year':s['year'],'support_name':s['support_name'],'support':support,'signal_close':close,'entry_open':entry,'pre_ma20_fast_recoveries':int(s['pre_ma20_fast_recoveries']),'pre_gate_skip':int(s['pre_gate_skip']), 'usable':0,
      'hold_pct':'','stop_only_pct':'','reclaim_once_pct':'','reclaim_sweep_gate_pct':'','pre_gate_stop_pct':'','pre_gate_reclaim_pct':'','combined_gates_pct':'',
      'first_stop_date':'','reclaim_date':'','reentry_date':'','second_stop_date':'','reentered':0,'second_stop':0,'sweeps_observed_at_reclaim':'','post_gate_skip':'','reentry_remaining_sessions':'','late_reentry_censored':0,'risk_distance_pct':round((entry-support)/entry*100,4)}
 hold=(float(ft[-1]['close'])/entry-1)*100-cost
 out['hold_pct']=round(hold,7)
 if not (s['asof_support_status']=='VALID_AT_SIGNAL_CLOSE' and 0<support<min(entry,close)):return out
 out['usable']=1
 breach=next((i for i in range(len(ft)-1) if float(ft[i]['close'])<support),None)
 if breach is None:
  out.update(stop_only_pct=hold,reclaim_once_pct=hold,reclaim_sweep_gate_pct=hold)
 else:
  stop_i=breach+1;exitprice=float(ft[stop_i]['open']);stopnet=(exitprice/entry-1)*100-cost
  out.update(first_stop_date=ft[stop_i]['trade_date'],stop_only_pct=stopnet,reclaim_once_pct=stopnet,reclaim_sweep_gate_pct=stopnet)
  rec=next((j for j in range(stop_i,len(ft)-1) if float(ft[j]['close'])>=support),None)
  if rec is not None:
   out['reclaim_date']=ft[rec]['trade_date'];buy=rec+1
   if float(ft[buy]['open'])>support:
    sweeps=sum(float(b['low'])<support and float(b['close'])>=support for b in ft[:rec+1])
    out['sweeps_observed_at_reclaim']=sweeps;out['post_gate_skip']=int(sweeps>=2)
    out['reentry_remaining_sessions']=len(ft)-buy
    out['reentered']=1;out['reentry_date']=ft[buy]['trade_date'];second_breach=next((j for j in range(buy,len(ft)-1) if float(ft[j]['close'])<support),None)
    if second_breach is None:secondprice=float(ft[-1]['close'])
    else:
     secondprice=float(ft[second_breach+1]['open']);out['second_stop']=1;out['second_stop_date']=ft[second_breach+1]['trade_date']
    secondleg=(secondprice/float(ft[buy]['open'])-1)*100-cost
    out['reclaim_once_pct']=stopnet+secondleg
    out['late_reentry_censored']=int(len(ft)-buy<p['min_post_reentry_sessions'] and not out['second_stop'])
    if sweeps<2:out['reclaim_sweep_gate_pct']=stopnet+secondleg
 out['pre_gate_stop_pct']=0.0 if out['pre_gate_skip'] else out['stop_only_pct']
 out['pre_gate_reclaim_pct']=0.0 if out['pre_gate_skip'] else out['reclaim_once_pct']
 out['combined_gates_pct']=0.0 if out['pre_gate_skip'] else out['reclaim_sweep_gate_pct']
 for k in ('hold_pct','stop_only_pct','reclaim_once_pct','reclaim_sweep_gate_pct','pre_gate_stop_pct','pre_gate_reclaim_pct','combined_gates_pct'):
  out[k]=round(out[k],7)
 return out

def report(pre_dir,post_dir,p,out,d25_zip=None):
 m=check_seal(pre_dir);n=check_seal(post_dir)
 if m['phase']!='pre' or n['phase']!='post' or not m['pre_decision_sealed']:raise ValueError('PRICE_SEALS_NOT_READY')
 root=Path(out);root.mkdir(parents=True,exist_ok=True)
 supports=q.read_csv_bytes((Path(pre_dir)/SUP).read_bytes());sup={tuple(x[k] for k in EVENT):x for x in supports}
 posts=q.read_csv_bytes((Path(post_dir)/POST).read_bytes(),True); by=defaultdict(list)
 for row in posts:by[tuple(row[k] for k in EVENT)].append(row)
 results=[];audit=[];parity=[];refs={}
 if d25_zip:
  if digest_file(d25_zip)!=p['parent_d25_zip_sha256']:raise ValueError('REFERENCE_D25_ORIGINAL_ARCHIVE_SHA_CHANGED')
  with zipfile.ZipFile(d25_zip) as z:
   refs={(x['strategy'],x['date'],x['code']):x for x in q.read_csv_bytes(z.read(F),True) if x['horizon']=='30'}
 for r in selection(FROZEN,p):
  k=tuple(r[k] for k in EVENT)
  if k not in sup or len(by[k])!=30:
   audit.append({**r,'status':'CENSORED_PRE_POST_OR_D30'});continue
  try:
   x=simulate(sup[k],by[k],p)
   if refs:
    ref=refs.get(k);ref_ok=bool(ref and ref['status_out']=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED')
    if ref_ok:
     bars=sorted(by[k],key=lambda z:z['trade_date'])
     exact=(bars[0]['trade_date']==ref['entry_day'] and bars[-1]['trade_date']==ref['exit_day'] and abs(float(ref['net_20bps_pct'])-x['hold_pct'])<.01)
     if not exact:raise ValueError('D25_D30_PRICE_PARITY_MISMATCH')
     verdict='PASS'
    else:verdict='D25_REFERENCE_NOT_COMPLETE'
    parity.append({**{key:r[key] for key in EVENT},'status':verdict,'D25_net_pct':ref.get('net_20bps_pct','') if ref else '', 'R_net_pct':x['hold_pct']})
   results.append(x);audit.append({**r,'status':'VALID_SUPPORT' if x['usable'] else 'UNUSABLE_ASOF_SUPPORT'})
  except Exception as e:
   if 'PRICE_PARITY' in str(e):raise
   audit.append({**r,'status':'CENSORED_BAD_CHRONOLOGY_'+type(e).__name__})
 if not results:raise ValueError('NO_PAIRED_OBSERVATIONS')
 output(root/CASE,results,list(results[0].keys()));output(root/AUDIT,audit,[*EVENT,'year','selection_basis','status'])
 if refs:output(root/PARITY,parity,[*EVENT,'status','D25_net_pct','R_net_pct'])
 eligible=[r for r in results if r['usable']==1]
 cols=('hold_pct','stop_only_pct','reclaim_once_pct','reclaim_sweep_gate_pct','pre_gate_stop_pct','pre_gate_reclaim_pct','combined_gates_pct')
 summary=[];sub=[]
 for st in (A,D):
  cases=[r for r in eligible if r['strategy']==st]
  if not cases:continue
  for key in cols:
   vals=[r[key] for r in cases]
   # These are *full same-denominator* portfolio comparisons: skipped signals count 0.
   summary.append({'strategy':st,'policy':key,'n_eligible':len(cases),'n_executed':sum(not(r['pre_gate_skip'] and key in ('pre_gate_stop_pct','pre_gate_reclaim_pct','combined_gates_pct')) for r in cases),
    'n_pre_skip':sum(r['pre_gate_skip'] for r in cases),'n_reentries':sum(r['reentered'] for r in cases),
    'mean_net_pct':round(statistics.mean(vals),5),'median_net_pct':round(statistics.median(vals),5),
    'loss_le_minus10':sum(v<=-10 for v in vals),'win_pct':round(100*sum(v>0 for v in vals)/len(vals),3)})
  for group,rows in [('pre_unstable_ge2',[r for r in cases if r['pre_gate_skip']]),('pre_stable_lt2',[r for r in cases if not r['pre_gate_skip']]),
    ('reclaim_sweeps_ge2',[r for r in cases if r['reentered'] and r['sweeps_observed_at_reclaim']>=2 and not r['late_reentry_censored']]),
    ('reclaim_sweeps_lt2',[r for r in cases if r['reentered'] and r['sweeps_observed_at_reclaim']<2 and not r['late_reentry_censored']])]:
   sub.append({'strategy':st,'group':group,'n':len(rows),'mean_stop_only_pct':round(statistics.mean(x['stop_only_pct'] for x in rows),4) if rows else '',
     'mean_reclaim_delta_pp':round(statistics.mean(x['reclaim_once_pct']-x['stop_only_pct'] for x in rows),4) if rows else '',
     'second_stops':sum(x['second_stop'] for x in rows),'reentries':sum(x['reentered'] for x in rows)})
 output(root/SUMMARY,summary,list(summary[0]));output(root/RISK,sub,list(sub[0]));
 return file_seal(root,'evaluate',n_selected=200,n_pre_complete=m['valid'],n_post_complete=n['valid'],n_paired=len(results),n_eligible=len(eligible),A_eligible=sum(x['strategy']==A for x in eligible),D_eligible=sum(x['strategy']==D for x in eligible),n_reentry=sum(x['reentered'] for x in eligible),reference_D25_comparable=sum(x['status']=='PASS' for x in parity),reference_D25_unavailable=sum(x['status']!='PASS' for x in parity),eligible_results_from_signal_time_support=True)

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--mode',choices=['verify','pre','post','evaluate'],required=True);ap.add_argument('--protocol',default='R1D26R_PREDECLARED_PROTOCOL.json');ap.add_argument('--d25');ap.add_argument('--out');ap.add_argument('--pre-dir');ap.add_argument('--post-dir');ap.add_argument('--sleep-seconds',type=float,default=0.8);args=ap.parse_args();p=json.loads(Path(args.protocol).read_text())
 if not p['research_only'] or p['strategy_ready'] or p['live_changed'] or p['orders_placed']:raise ValueError('LIVE_RESEARCH_VIOLATION')
 if args.mode=='verify':verify(args.d25,p);print('R1D26R_FROZEN_200_OUTCOME_BLIND_SELECTION_PASS')
 elif args.mode in ('pre','post'):
  p['pre_dir']=args.pre_dir
  m=collect(args.mode,p,args.out,args.sleep_seconds);print('R1D26R_SOURCE_SEALED',args.mode,m['valid'],'of 200')
 else:
  m=report(args.pre_dir,args.post_dir,p,args.out,args.d25);print('R1D26R_REPORT',json.dumps({k:m[k] for k in ['n_selected','n_paired','n_eligible','A_eligible','D_eligible','n_reentry']}))
if __name__=='__main__':main()
