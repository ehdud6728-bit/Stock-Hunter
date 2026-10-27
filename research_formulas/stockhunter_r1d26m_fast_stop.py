#!/usr/bin/env python3
"""R1-D26M; retrospective D1/D3 close-confirmed quick-stop audit.

Historical discovery ONLY; no live/order/fill/PIT, no result-driven tuning.
The next-trading-day opening price is a hypothetical model, not an executed fill.
"""
from __future__ import annotations
import argparse, collections, csv, gzip, hashlib, io, json, pathlib, statistics, zipfile
import numpy as np

VERSION='R1D26M_FAST_STOP_D1_D3_FROZEN_EXPLORATORY_V1'
EVENT=['strategy','date','code']
SCENARIOS=('HOLD_D30','D1_CLOSE_WEAK_2P5','D3_NO_REBOUND_4P0','COMBINED_EARLIEST')

def sha(b):return hashlib.sha256(b).hexdigest()
def rows(data):return list(csv.DictReader(io.StringIO(data.decode('utf-8-sig'))))
def put_csv(path,dictrows,fields):
 with open(path,'w',encoding='utf-8',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(dictrows)
def load_zip(z,basename):
 x=[k for k in z.namelist() if k.endswith('/'+basename) or k==basename]
 if len(x)!=1:raise ValueError('R1D26M_ZIP_FILE_NOT_UNIQUE '+basename)
 return z.read(x[0])
def event_key(r):return tuple(str(r[k]) for k in EVENT)

def parse_source(path,protocol):
 if sha(pathlib.Path(path).read_bytes())!=protocol['r1d26l_zip_sha256']:raise ValueError('ORIGINAL_ZIP_SHA_MISMATCH')
 with zipfile.ZipFile(path) as z:
  manifests=json.loads(load_zip(z,'R1D26L_COLLECTION_MANIFEST.json'))
  research=json.loads(load_zip(z,'R1D26L_QA_MANIFEST.json'))
  data=load_zip(z,'R1D26L_RETRIEVED_UNADJUSTED_DAILY_OHLCV.csv.gz')
  if sha(data)!=manifests['source_daily_sha256']:raise ValueError('DAILY_SOURCE_GZIP_SHA_MISMATCH')
  source=rows(gzip.decompress(data))
  old=rows(load_zip(z,'R1D26L_EXIT_SIMULATION_PAIRED_EVENTS.csv'))
  selection=rows(load_zip(z,'R1D26L_FROZEN_60_ASOF_EVENTS.csv'))
  if sha(load_zip(z,'R1D26L_FROZEN_60_ASOF_EVENTS.csv'))!=protocol['frozen_60_sha256']:raise ValueError('EVENT_SELECTION_CHANGED')
  if not (manifests['source_valid_events']==59 and manifests['source_invalid_events']==1 and manifests['source_raw_rows']==1770 and research['baseline_price_parity']==59 and research['baseline_price_mismatch']==0):
   raise ValueError('PINNED_SOURCE_EXPECTATION_CHANGED')
  if len(source)!=1770 or len(selection)!=60:raise ValueError('BAD_SOURCE_CARDINALITY')
  for r in source:
   if r['provider']!='pykrx_KRX_UNADJUSTED' or r['source_PIT']!='NOT_CERTIFIED':raise ValueError('PRICE_PROVIDER_OR_PIT_CHANGED')
  if len({event_key(s) for s in selection})!=60:raise ValueError('DUPLICATE_SELECTION')
  oldhold={event_key(r):r for r in old if r['scenario']=='HOLD_D30' and r['ordering']=='conservative'}
  if len(oldhold)!=59:raise ValueError('ORIGINAL_HOLD_NOT_59')
  return selection,source,oldhold,manifests,research

def evaluate_one(bars,protocol):
 bars=sorted(bars,key=lambda r:r['trade_date'])
 if len(bars)!=30 or len({r['trade_date'] for r in bars})!=30:raise ValueError('BAD_DAILY_COUNT')
 if len({event_key(r) for r in bars})!=1:raise ValueError('MIXED_EVENT')
 for i,b in enumerate(bars):
  o,h,l,c=(float(b[x]) for x in ('open','high','low','close'))
  if min(o,h,l,c)<=0 or l>min(o,c,h) or h<max(o,c,l):raise ValueError('INVALID_OHLC')
  if i>0 and bars[i-1]['trade_date']>=b['trade_date']:raise ValueError('INVALID_ORDER')
 entry=float(bars[0]['open']);c1=float(bars[0]['close']);c3=float(bars[2]['close'])
 maxh3=max(float(x['high']) for x in bars[:3]);d1=(c1/entry-1)*100;d3=(c3/entry-1)*100
 assert protocol['early_exit_d1_close_pct'] == -2.5
 assert protocol['early_exit_d3_close_pct'] == -4.0
 assert protocol['early_exit_d3_max_high_pct_exclusive'] == 5.0
 d1trigger=d1<=-2.5+1e-10
 d3trigger=d3<=-4.0+1e-10 and (maxh3/entry-1)*100 <5.0-1e-10
 a1=float(bars[2]['amount']);a0=float(bars[0]['amount']);a2=float(bars[1]['amount'])
 volume_warn=sum(float(x['close'])<float(x['open']) for x in bars[:3])>=2 and a1>=1.5*(a0+a2)/2 and c3<entry
 scenarios={
  'HOLD_D30':None,
  'D1_CLOSE_WEAK_2P5': 1 if d1trigger else None,
  'D3_NO_REBOUND_4P0': 3 if d3trigger else None,
  'COMBINED_EARLIEST': 1 if d1trigger else (3 if d3trigger else None)
 }
 outputs=[]
 for scene,day in scenarios.items():
  exitdate=bars[29]['trade_date'] if day is None else bars[day]['trade_date']
  px=float(bars[29]['close']) if day is None else float(bars[day]['open'])
  net=100*(px/entry-1)-float(protocol['roundtrip_cost_pct'])
  outputs.append(dict(scenario=scene,exit_reason='HOLD_D30' if day is None else ('D1_SIGNAL_EXIT_D2_OPEN' if day==1 else 'D3_SIGNAL_EXIT_D4_OPEN'),exit_date=exitdate,exit_px=round(px,6),net_return_pct=round(net,8),trigger=int(day is not None),day1_close_vs_entry_pct=round(d1,5),day3_close_vs_entry_pct=round(d3,5),max_high3_vs_entry_pct=round(100*(maxh3/entry-1),5),day3_selling_volume_alert=int(volume_warn)))
 return outputs

def summarize(records,baseline,policy,seed=20261027,bootstrap=1500):
 out=[];rng=np.random.default_rng(seed)
 for scen in SCENARIOS:
  subset=[r for r in records if r['scenario']==scen and r['status']=='COMPLETE']
  for strat in ('ALL','C','D'):
   a=[r for r in subset if strat=='ALL' or r['strategy'].startswith(strat+'_')]
   if not a:continue
   v=np.array([float(r['net_return_pct']) for r in a]);delta=np.array([float(r['delta_vs_hold_pp']) for r in a]);years=np.array([r['year'] for r in a]);
   intervals=[]
   for _ in range(bootstrap):
    choices=np.concatenate([rng.choice(np.where(years==y)[0],size=sum(years==y),replace=True) for y in np.unique(years)])
    intervals.append(float(np.mean(delta[choices])))
   tail_n=sum(v<=-10);hold=np.array([float(r['baseline_hold_pct']) for r in a])
   out.append(dict(strategy=strat,scenario=scen,n=len(a),triggered=sum(int(r['trigger']) for r in a),mean_net_pct=round(float(np.mean(v)),4),median_net_pct=round(float(np.median(v)),4),mean_delta_vs_hold_pp=round(float(np.mean(delta)),4),delta_bootstrap_stratified_year_95_lo=round(float(np.quantile(intervals,.025)),4),delta_bootstrap_stratified_year_95_hi=round(float(np.quantile(intervals,.975)),4),loss10_count=tail_n,loss10_rate_pct=round(100*tail_n/len(a),3),hold_loss10_count=sum(hold<=-10),positive_pct=round(100*np.mean(v>0),3),rescued_loss10_count=sum(int(r['trigger']) and float(r['baseline_hold_pct'])<=-10 and float(r['net_return_pct'])>float(r['baseline_hold_pct']) for r in a),false_alarm_hold_profit10_count=sum(int(r['trigger']) and float(r['baseline_hold_pct'])>=10 and float(r['net_return_pct'])<float(r['baseline_hold_pct']) for r in a),missed_hold_loss10_count=sum(not int(r['trigger']) and float(r['baseline_hold_pct'])<=-10 for r in a)))
 return out

def main(zip_path,protocol_path,out):
 p=json.loads(pathlib.Path(protocol_path).read_text(encoding='utf-8'))
 if (p['version']!=VERSION or not p['research_only'] or p['strategy_ready'] or p['live_changed'] or p['orders_placed']!=0 or p['hypothesis_status']!='EXPLORATORY_RETROSPECTIVE_NOT_INDEPENDENT_OOS'):
  raise ValueError('WRONG_PROTOCOL_OR_LIVE_FORBIDDEN')
 selection,source,oldhold,srcmani,oldmani=parse_source(zip_path,p)
 by=collections.defaultdict(list)
 for r in source:by[event_key(r)].append(r)
 outdir=pathlib.Path(out);outdir.mkdir(parents=True,exist_ok=True)
 results=[];missing=[];flags=[]
 for s in selection:
  k=event_key(s); bars=by.get(k,[])
  if len(bars)==0:
   missing.append({'strategy':k[0],'date':k[1],'code':k[2],'status':'MISSING_SOURCE_NOT_IMPUTED'});continue
  if len(bars)!=30:raise ValueError('PARTIAL_DAILY_SOURCE_FAIL_CLOSED')
  if k not in oldhold:raise ValueError('MISSING_ORIGINAL_HOLD')
  year=str(s['year'])
  scen=evaluate_one(bars,p)
  original=float(oldhold[k]['net_return_pct'])
  if abs(float(scen[0]['net_return_pct'])-original)>.00003:raise ValueError('HOLD_BASELINE_PARITY_FAIL')
  for v in scen:
   v.update(strategy=k[0],date=k[1],code=k[2],year=year,status='COMPLETE',baseline_hold_pct=round(original,8),delta_vs_hold_pp=round(float(v['net_return_pct'])-original,8))
   results.append(v)
  flags.append(dict(strategy=k[0],date=k[1],code=k[2],year=year,trigger_d1=int(scen[1]['trigger']),trigger_d3=int(scen[2]['trigger']),volume_warn=int(scen[0]['day3_selling_volume_alert']),hold_net_pct=original,combo_net_pct=float(scen[3]['net_return_pct']),combo_delta_pp=float(scen[3]['delta_vs_hold_pp']),group=('PREVENTED_LARGE_LOSS' if scen[3]['trigger'] and original<=-10 and float(scen[3]['net_return_pct'])>original else ('COSTLY_FALSE_ALARM' if scen[3]['trigger'] and original>=10 else ('MISSED_LARGE_LOSS' if not scen[3]['trigger'] and original<=-10 else 'OTHER')))))
 if len(results)!=236 or len(missing)!=1:raise ValueError('COVERAGE_CHANGED_OR_INCOMPLETE')
 report=summarize(results,None,p)
 # Compare only same instruments; source L pessimistic stop benchmark is prior frozen result.
 with zipfile.ZipFile(zip_path) as z:
  oldexit=rows(load_zip(z,'R1D26L_EXIT_SIMULATION_PAIRED_EVENTS.csv'))
  bench={(event_key(r),r['scenario']):float(r['net_return_pct']) for r in oldexit if r['ordering']=='conservative' and r['scenario'] in ('TP10_SL5','HALF_TP10_BE')}
 for r in results:
  k=event_key(r)
  if (k,'TP10_SL5') not in bench:raise ValueError('MISSING_SL5_BENCHMARK')
  r['prior_TP10_SL5_net_pct']=round(bench[(k,'TP10_SL5')],8)
  r['prior_HALF_TP10_BE_net_pct']=round(bench[(k,'HALF_TP10_BE')],8)
 fields=list(results[0].keys());put_csv(outdir/'R1D26M_EVENT_SCENARIOS.csv',results,fields)
 put_csv(outdir/'R1D26M_SCENARIO_SUMMARY.csv',report,list(report[0].keys()))
 put_csv(outdir/'R1D26M_CASE_CATEGORIES.csv',flags,list(flags[0].keys()))
 put_csv(outdir/'R1D26M_INCOMPLETE_SOURCE.csv',missing,['strategy','date','code','status'])
 manifest={'version':VERSION,'source_run_id':p['r1d26l_run_id'],'source_artifact_id':p['r1d26l_artifact_id'],'source_zip_sha256':p['r1d26l_zip_sha256'],'locked_source_60':len(selection),'daily_source_complete':len(flags),'daily_source_invalid':len(missing),'ohlc_rows':len(source),'comparison_scenario_rows':len(results),'frozen_rules':p['rules'],'price_baseline_parity':len(flags),'same_day_unknown_not_used_by_close_only_rules':True,'modeled_next_open_not_actual_fill':True,'official_KRX_original_crosscheck':'PENDING','corporate_action_historical_PIT':'NOT_VERIFIED','prospective_OOS':False,'strategy_ready':False,'live_changed':False,'orders_placed':0,'bootstrap_draws':1500,'file_sha256':{x.name:sha(x.read_bytes()) for x in sorted(outdir.glob('*.csv'))}}
 (outdir/'R1D26M_QA_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
 print('R1D26M_FROZEN_FAST_STOP_RETROSPECTIVE_NOT_PIT_NOT_OOS',json.dumps({'source_complete':len(flags),'missing':len(missing),'rows':len(source),'triggered':sum(x['trigger_d1'] or x['trigger_d3'] for x in flags),'research_only':True},ensure_ascii=False))
 return report,flags,manifest

if __name__=='__main__':
 ap=argparse.ArgumentParser();ap.add_argument('--source-zip',required=True);ap.add_argument('--protocol',required=True);ap.add_argument('--out',required=True);a=ap.parse_args();main(a.source_zip,a.protocol,a.out)
