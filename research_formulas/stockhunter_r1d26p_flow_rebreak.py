#!/usr/bin/env python3
"""R1-D26P: frozen O/N/L post-signal volume, turnover, 2+ support breaks and provisional investor net flow.
RESEARCH ONLY; retrospective non-PIT; not LIVE, no orders, not new independent OOS.
"""
from __future__ import annotations
import argparse,csv,gzip,hashlib,io,json,math,statistics,time,zipfile
from collections import defaultdict,Counter
from pathlib import Path
import pandas as pd

ST_C='C_ABOVE224_RISING_WAVE_PULLBACK';ST_D='D_LEADER_CONSOLIDATION_BREAKOUT'
SEL={ST_C:'C_CONFIRMED_SWING_LOW',ST_D:'D_PRE20_BREAKOUT_RETEST'}
O_FILE='reports/r1d26o/result/historical/R1D26O_PAIRED_REENTRY_590_ROWS.csv'
N_PRE='reports/r1d26n/result/pre_source/R1D26N_PRE_SIGNAL_120_UNADJUSTED_OHLCV.csv.gz'
L_POST='reports/r1d26l/result/source/R1D26L_RETRIEVED_UNADJUSTED_DAILY_OHLCV.csv.gz'
L_SEL='data/R1D26L_FROZEN_60_ASOF_EVENTS.csv'
OUT_FEATURE='R1D26P_PRIMARY_ONE_LINE_PER_SIGNAL_59_DESCRIPTIVE.csv'
OUT_SUM='R1D26P_PRIMARY_C_D_SUCCESS_VS_REBREAK_SUMMARY.csv'
OUT_FLOW='R1D26P_FROZEN_59_POST_DAILY_INVESTOR_NET_FLOW.csv.gz'
OUT_COVER='R1D26P_FLOW_FETCH_AUDIT.csv'
OUT_FLOWJOIN='R1D26P_PRIMARY_INVESTOR_FLOW_ASOF_JOIN.csv'
OUT_MAN='R1D26P_QA_MANIFEST.json'
EVENT=('strategy','date','code')

def sha(b):return hashlib.sha256(b).hexdigest()
def rows(b,gz=False):
 if gz:b=gzip.decompress(b)
 return list(csv.DictReader(io.StringIO(b.decode('utf-8-sig'))))
def wcsv(path,rr,cols):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 with path.open('w',encoding='utf-8',newline='') as f:
  w=csv.DictWriter(f,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(rr)
def zcsv(path,rr,cols):
 s=io.StringIO();w=csv.DictWriter(s,fieldnames=cols,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rr)
 with Path(path).open('wb') as f:
  with gzip.GzipFile(fileobj=f,mode='wb',filename='',mtime=0) as g:g.write(s.getvalue().encode())
def manifest(out,**kwargs):
 out=Path(out);d={'version':'R1D26P_ASOF_VOLUME_REBREAK_INVESTOR_NETFLOW_V1','research_only':True,'prospective_OOS':False,'historical_price_PIT':'NOT_CERTIFIED','investor_net_flow_PIT':'NOT_CERTIFIED','independent_official_crosscheck':'PENDING','strategy_ready':False,'live_changed':False,'orders_placed':0,**kwargs}
 d['file_sha256']={p.name:sha(p.read_bytes()) for p in out.iterdir() if p.is_file() and p.name!=OUT_MAN}
 (out/OUT_MAN).write_text(json.dumps(d,ensure_ascii=False,indent=2,sort_keys=True),encoding='utf8')
 return d

def open_locked(path,expected_sha):
 b=Path(path).read_bytes()
 if sha(b)!=expected_sha:raise ValueError('FROZEN_PARENT_ZIP_SHA_MISMATCH')
 return zipfile.ZipFile(io.BytesIO(b))

def check_manifests(zo,zn,zl):
 for z,k,name,field in [(zo,'O','reports/r1d26o/result/historical/R1D26O_QA_MANIFEST.json','files_sha256'),(zn,'N','reports/r1d26n/result/audit/R1D26N_QA_MANIFEST.json','file_sha256'),(zl,'L','reports/r1d26l/result/exit_pilot/R1D26L_QA_MANIFEST.json','file_sha256')]:
  j=json.loads(z.read(name)); folder=name.rsplit('/',1)[0]
  for file,digest in j[field].items():
   if sha(z.read(folder+'/'+file))!=digest:raise ValueError(k+'_INNER_SHA_MISMATCH')
  if j.get('strategy_ready') is not False or j.get('orders_placed')!=0:raise ValueError('LIVE_NOT_RESEARCH_SOURCE')

def f(v,default=None):
 try:
  if v is None or str(v).strip()=='':return default
  x=float(v)
  return x if math.isfinite(x) else default
 except (ValueError,TypeError):return default

def prepare(o,n,l,prot,out):
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 zo=open_locked(o,prot['parents']['O']['sha256']);zn=open_locked(n,prot['parents']['N']['sha256']);zl=open_locked(l,prot['parents']['L']['sha256'])
 check_manifests(zo,zn,zl)
 if sha(zl.read(L_SEL))!=prot['selection_csv_sha256']:raise ValueError('LOCKED_SELECTION_DRIFT')
 raw=rows(zo.read(O_FILE));pre=rows(zn.read(N_PRE),True);post=rows(zl.read(L_POST),True)
 if len(raw)!=590 or len(pre)!=7200 or len(post)!=1770:raise ValueError('PARENT_DATA_SHAPE_DRIFT')
 pre_map=defaultdict(list);post_map=defaultdict(list)
 for r in pre:pre_map[tuple(r[x] for x in EVENT)].append(r)
 for r in post:post_map[tuple(r[x] for x in EVENT)].append(r)
 primary={}
 for r in raw:
  key=tuple(r[x] for x in EVENT)
  if r['method']==SEL[r['strategy']] and r['exit_type']=='close_confirm_next_open':
   if key in primary:raise ValueError('PRIMARY_DUP')
   primary[key]=r
 if len(primary)!=59 or len(pre_map)!=60 or len(post_map)!=59:raise ValueError('FROZEN_COHORT_MISMATCH')
 res=[]
 for key in sorted(primary):
  r=primary[key];pb=sorted(pre_map[key],key=lambda a:a['trade_date']);ft=sorted(post_map[key],key=lambda a:a['trade_date'])
  if len(pb)!=120 or len(ft)!=30 or pb[-1]['trade_date']!=key[1] or ft[0]['trade_date']<=key[1]:raise ValueError('BARS_SOURCE_DRIFT')
  sup=f(r['support']);status=r['status'];entry=ft[0]['trade_date'];signal=r['date']
  past=pb[-21:-1];baseline_v=statistics.median(int(t['volume']) for t in past);baseline_m=statistics.median(int(t['amount']) for t in past)
  if baseline_v<=0 or baseline_m<=0:raise ValueError('BASELINE_AMOUNT_ZERO')
  exit_day=r['exit_date'];ex_idx=next((i for i,b in enumerate(ft) if b['trade_date']==exit_day),None)
  stopped=(r['exit_reason']=='CONFIRMED_CLOSE_NEXT_OPEN')
  if stopped and (ex_idx is None or ex_idx<1 or f(ft[ex_idx-1]['close'])>=sup):raise ValueError('STOP_TRIGGER_PRICE_INCONSISTENT')
  breakidx=ex_idx-1 if stopped else None
  reenter=int(f(r['reclaim_reentry'],0))==1
  reclaimday=r.get('reclaim_reclaim_close_date','') if reenter else ''
  rcidx=next((i for i,b in enumerate(ft) if b['trade_date']==reclaimday),None) if reenter else None
  if reenter and (rcidx is None or rcidx<ex_idx or f(ft[rcidx]['close'])<sup):raise ValueError('RECLAIM_FUTURE_OR_INVALID')
  # All features at time of the reclaiming CLOSE, not after; signals before a reentry open.
  seen=ft[:rcidx+1] if reenter else []
  prev_up=(f(pb[-1]['close'])>=sup) if sup else False
  close_down=close_up=sweep=0
  for b in seen:
   is_up=f(b['close'])>=sup
   if prev_up and not is_up:close_down+=1
   if (not prev_up) and is_up:close_up+=1
   if f(b['low'])<sup and is_up:sweep+=1
   prev_up=is_up
  # The full-horizon counts are POST OUTCOME LABELS; never features available at reclaim close.
  full_prev_up=(f(pb[-1]['close'])>=sup) if sup else False
  full_down=full_up=full_sweeps=0
  if sup is not None and sup>0:
   for b in ft:
    full_isup=f(b['close'])>=sup
    if full_prev_up and not full_isup:full_down+=1
    if (not full_prev_up) and full_isup:full_up+=1
    if f(b['low'])<sup and full_isup:full_sweeps+=1
    full_prev_up=full_isup
  def ratio(b,k,base):return round(int(b[k])/base,5) if b else '' 
  br=ft[breakidx] if stopped else None
  rc=ft[rcidx] if reenter else None
  label='NO_INITIAL_STOP' if not stopped else 'STOP_NO_REENTRY' if not reenter else 'SECOND_STOP' if f(r['reclaim_second_stop'],0)==1 else 'REENTRY_SURVIVED_D30'
  after_days=30-(rcidx+1) if rcidx is not None else ''
  res.append(dict(strategy=key[0],date=key[1],code=key[2],year=signal[:4],method=r['method'],status=status,
    support=sup if sup is not None else '',entry_open=f(r['entry_open']),stop_exit_date=exit_day,break_close_date=br['trade_date'] if br else '',
    reclaim_close_date=reclaimday,reentry_open_date=r['reclaim_reentry_date'],reentry=f(r['reclaim_reentry'],0),
    second_stop=int(f(r['reclaim_second_stop'],0)),label=label,d30_remaining_after_reclaim=after_days,
    full_d30_close_breaks_POSTLABEL=full_down if sup is not None else '',full_d30_close_reclaims_POSTLABEL=full_up if sup is not None else '',full_d30_intraday_sweeps_POSTLABEL=full_sweeps if sup is not None else '',
    close_breaks_asof_reclaim=close_down if reenter else '',close_reclaims_asof_reclaim=close_up if reenter else '',intraday_undercut_recover_asof_reclaim=sweep if reenter else '',
    baseline_pre20_vol_med=round(baseline_v,2),baseline_pre20_amount_med=round(baseline_m,2),
    break_day_volume=br['volume'] if br else '',break_day_amount=br['amount'] if br else '',
    break_vol_vs_pre20_median=ratio(br,'volume',baseline_v),break_amount_vs_pre20_median=ratio(br,'amount',baseline_m),
    reclaim_day_volume=rc['volume'] if rc else '',reclaim_day_amount=rc['amount'] if rc else '',
    reclaim_vol_vs_pre20_median=ratio(rc,'volume',baseline_v),reclaim_amount_vs_pre20_median=ratio(rc,'amount',baseline_m),
    hold_net_pct=f(r['hold_net_pct']),stop_net_pct=f(r['stop_net_pct']),reclaim_reentry_net_pct=f(r['reclaim_return_net_pct']),
    gain_vs_stop_pp=round(f(r['reclaim_return_net_pct'])-f(r['stop_net_pct']),5) if reenter else '',
    source='FROZEN_L_N_O_PROVISIONAL_NOT_PIT'))
 cols=list(res[0].keys());wcsv(out/OUT_FEATURE,res,cols)
 re=[r for r in res if r['reentry']==1]
 agg=[]
 for strat in (ST_C,ST_D):
  for state in ('SECOND_STOP','REENTRY_SURVIVED_D30'):
   a=[r for r in re if r['strategy']==strat and r['label']==state]
   def mean(k):
    vv=[f(q[k]) for q in a];vv=[x for x in vv if x is not None]
    return round(statistics.mean(vv),5) if vv else ''
   agg.append(dict(strategy=strat,label=state,unique_events=len(a),positive_combined_net_count=sum(f(v['reclaim_reentry_net_pct'],0)>0 for v in a),mean_combined_net_pct=mean('reclaim_reentry_net_pct'),mean_gain_vs_stop_pp=mean('gain_vs_stop_pp'),
     mean_close_breaks=mean('close_breaks_asof_reclaim'),mean_sweep_count=mean('intraday_undercut_recover_asof_reclaim'),
     mean_break_volume_ratio=mean('break_vol_vs_pre20_median'),mean_break_turnover_ratio=mean('break_amount_vs_pre20_median'),
     mean_reclaim_volume_ratio=mean('reclaim_vol_vs_pre20_median'),mean_reclaim_turnover_ratio=mean('reclaim_amount_vs_pre20_median')))
 wcsv(out/OUT_SUM,agg,list(agg[0]))
 m=manifest(out,phase='price_features',all_selected=60,price_complete=59,eligible_primary_reentries=len(re),C_reentry=sum(t['strategy']==ST_C for t in re),D_reentry=sum(t['strategy']==ST_D for t in re),second_stops=sum(t['second_stop']==1 for t in re),flow_ready=False,notes='Descriptive 59 signal sample only; no independent OOS; label post-outcome, pre-reentry features as-of-close.')
 return res,m

def normalize_net(df,dayset,colnames,unit):
 if not isinstance(df,pd.DataFrame) or df.empty or not set(colnames).issubset(df.columns):raise ValueError('KRX_SCHEMA_OR_EMPTY_'+unit)
 a={}
 for idx,r in df.iterrows():
  d=pd.Timestamp(idx).date().isoformat()
  if d in a:raise ValueError('KRX_DUP_TRADE_DATE')
  values=[]
  for c in colnames:
   v=f(r[c]);
   if v is None or int(v)!=v:raise ValueError('KRX_INVALID_SIGNED_VALUE')
   values.append(int(v))
  a[d]=tuple(values)
 if not dayset.issubset(a):raise ValueError('KRX_MISSING_PRICE_SESSION')
 return a

def collect(l,prot,out,fetch=None,sleep=1.0):
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 zl=open_locked(l,prot['parents']['L']['sha256']);pr=rows(zl.read(L_POST),True)
 if len(pr)!=1770:raise ValueError('L_PRICE_ROWS_CHANGED')
 mp=defaultdict(list)
 for r in pr:mp[tuple(r[x] for x in EVENT)].append(r)
 if len(mp)!=59:raise ValueError('COHORT_CHANGED')
 if fetch is None:
  from pykrx import stock
  fetch=lambda a,b,c,mode: (stock.get_market_trading_value_by_date(a,b,c,on='순매수') if mode=='krw' else stock.get_market_trading_volume_by_date(a,b,c,on='순매수'))
 rrs=[];audit=[]
 for ix,(key,raw) in enumerate(sorted(mp.items()),1):
  rr=sorted(raw,key=lambda x:x['trade_date']);days={r['trade_date'] for r in rr};start=min(days).replace('-','');end=max(days).replace('-','')
  reason='OK';v=m=None
  for t in range(2):
   try:
    v=normalize_net(fetch(start,end,key[2],'krw'),days,['외국인합계','기관합계'],'krw')
    m=normalize_net(fetch(start,end,key[2],'shares'),days,['외국인합계','기관합계'],'shares')
    break
   except Exception as e:
    reason=f'{type(e).__name__}:{str(e)[:150]}'
    if t==0:time.sleep(max(.0,sleep))
  if v is not None and m is not None:
   for r in rr:
    d=r['trade_date'];rrs.append(dict(strategy=key[0],date=key[1],code=key[2],trade_date=d,
    foreign_net_krw=v[d][0],institution_net_krw=v[d][1],foreign_net_shares=m[d][0],institution_net_shares=m[d][1],unit_value='KRW',unit_volume='shares',source='KRX_VIA_PYKRX_NOT_INDEPENDENT_NOT_PIT'))
   audit.append(dict(strategy=key[0],date=key[1],code=key[2],status='VALID_PROVISIONAL_NOT_PIT',rows=30,reason=''))
  else:audit.append(dict(strategy=key[0],date=key[1],code=key[2],status='INVALID_NOT_IMPUTED',rows=0,reason=reason))
  if ix%10==0:print('R1D26P_KRX_PROGRESS',ix,'/59',sum(r['rows']==30 for r in audit),flush=True)
  if sleep>0:time.sleep(sleep)
 zcsv(out/OUT_FLOW,rrs,list(rrs[0].keys()) if rrs else ['strategy','date','code','trade_date','foreign_net_krw','institution_net_krw','foreign_net_shares','institution_net_shares','unit_value','unit_volume','source'])
 wcsv(out/OUT_COVER,audit,list(audit[0].keys()))
 return manifest(out,phase='flow_collect',attempted=59,valid_events=sum(r['rows']==30 for r in audit),valid_flow_rows=len(rrs),flow_ready=False,official_crosscheck='PENDING',gross_foreign_institution_buy_sell='NOT_COLLECTED: net-only is not gross trade amount')

def evaluate(price_dir,flow_dir,out):
 out=Path(out);out.mkdir(parents=True,exist_ok=True)
 mp=json.loads((Path(price_dir)/OUT_MAN).read_text()); mf=json.loads((Path(flow_dir)/OUT_MAN).read_text())
 for d,m in [(Path(price_dir),mp),(Path(flow_dir),mf)]:
  for n,s in m['file_sha256'].items():
   if sha((d/n).read_bytes())!=s:raise ValueError('INPUT_REPORT_DRIFT')
 features=rows((Path(price_dir)/OUT_FEATURE).read_bytes());flow=rows((Path(flow_dir)/OUT_FLOW).read_bytes(),True)
 fm=defaultdict(dict)
 for r in flow:fm[tuple(r[x] for x in EVENT)][r['trade_date']]=r
 complete={tuple(r[x] for x in EVENT) for r in features if len(fm.get(tuple(r[x] for x in EVENT),{}))==30}
 if mf['valid_events']!=len(complete):raise ValueError('COVERAGE_MISMATCH')
 enriched=[]
 for r in features:
  if f(r['reentry'],0)!=1:continue
  key=tuple(r[x] for x in EVENT)
  if key not in complete:enriched.append({**r,'flow_status':'FLOW_UNAVAILABLE_CENSORED'});continue
  fm_key=fm[key]
  b=fm_key.get(r['break_close_date']);rc=fm_key.get(r['reclaim_close_date']);
  if b is None or rc is None:raise ValueError('FLOW_DATE_NOT_AVAILABLE')
  def to_rat(row,field,amountfield):
   den=f(r[amountfield]);n=f(row[field]);return round(n/den*100,4) if den is not None and den>0 and n is not None else ''
  ex={}
  for label,row,denvol,denval in [('break',b,'break_day_volume','break_day_amount'),('reclaim',rc,'reclaim_day_volume','reclaim_day_amount')]:
   for who in ('foreign','institution'):
    ex[label+'_'+who+'_net_krw']=int(row[who+'_net_krw']);ex[label+'_'+who+'_net_shares']=int(row[who+'_net_shares'])
    ex[label+'_'+who+'_net_krw_pct_total_amount']=to_rat(row,who+'_net_krw',denval)
    ex[label+'_'+who+'_net_shares_pct_total_volume']=to_rat(row,who+'_net_shares',denvol)
  enriched.append({**r,**ex,'flow_status':'COMPLETE_PROVISIONAL_NOT_PIT'})
 cols=list(features[0].keys())+['flow_status']+[t for t in (list(enriched[-1].keys()) if enriched else []) if t not in features[0] and t!='flow_status']
 # deterministic column names even when no KRX flow
 extra=[a+'_'+w+'_net_'+q+s for a in ('break','reclaim') for w in ('foreign','institution') for q,s in [('krw',''),('shares',''),('krw','_pct_total_amount'),('shares','_pct_total_volume')]]
 cols=list(features[0].keys())+['flow_status']+extra
 wcsv(out/OUT_FLOWJOIN,enriched,cols)
 agg=[]
 for strategy in (ST_C,ST_D):
  for state in ('SECOND_STOP','REENTRY_SURVIVED_D30'):
   a=[r for r in enriched if r['strategy']==strategy and r['label']==state and r['flow_status']=='COMPLETE_PROVISIONAL_NOT_PIT']
   agg.append(dict(strategy=strategy,label=state,eligible_with_net_flow=len(a),mean_break_foreign_net_intensity_pct=round(statistics.mean(float(v['break_foreign_net_krw_pct_total_amount']) for v in a),4) if a else '',mean_break_institution_net_intensity_pct=round(statistics.mean(float(v['break_institution_net_krw_pct_total_amount']) for v in a),4) if a else '',mean_reclaim_foreign_net_intensity_pct=round(statistics.mean(float(v['reclaim_foreign_net_krw_pct_total_amount']) for v in a),4) if a else '',mean_reclaim_institution_net_intensity_pct=round(statistics.mean(float(v['reclaim_institution_net_krw_pct_total_amount']) for v in a),4) if a else ''))
 wcsv(out/'R1D26P_FLOW_LABEL_DESCRIPTIVE.csv',agg,list(agg[0]))
 return manifest(out,phase='flow_join',reentry_events=len(enriched),flows_valid_whole_path=mf['valid_events'],reentry_flows_complete=sum(r['flow_status']=='COMPLETE_PROVISIONAL_NOT_PIT' for r in enriched),flow_ready=mf['valid_events']>=45 and sum(r['flow_status']=='COMPLETE_PROVISIONAL_NOT_PIT' for r in enriched)>=12,provisional_not_PIT=True,prohibition='DO NOT TUNE OR ENABLE LIVE; 2-3 whipsaw and flow diagnostics are exploratory')

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--mode',choices=['prepare','collect','evaluate'],required=True);ap.add_argument('--protocol',default='R1D26P_PREDECLARED_PROTOCOL.json');ap.add_argument('--o');ap.add_argument('--n');ap.add_argument('--l');ap.add_argument('--out',required=True);ap.add_argument('--price-dir');ap.add_argument('--flow-dir');ap.add_argument('--sleep-seconds',type=float,default=1.2);args=ap.parse_args()
 if args.mode=='evaluate':m=evaluate(args.price_dir,args.flow_dir,args.out)
 else:
  p=json.loads(Path(args.protocol).read_text(encoding='utf8'))
  if p['strategy_ready'] or p['live_changed'] or p['orders_placed']!=0:raise ValueError('RESEARCH_ONLY_REQUIRED')
  if args.mode=='prepare':_,m=prepare(args.o,args.n,args.l,p,args.out)
  else:m=collect(args.l,p,args.out,sleep=args.sleep_seconds)
 print('R1D26P_RESEARCH_ONLY',json.dumps({k:v for k,v in m.items() if k!='file_sha256'},ensure_ascii=False),flush=True)
if __name__=='__main__':main()
