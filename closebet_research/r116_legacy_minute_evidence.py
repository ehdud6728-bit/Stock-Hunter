"""R1.16 research-only immutable main-scan HAM minutes vs R1.14 signal audit.
No trade signal, execution, web refetch, inference of missing periods, or LIVE hooks.
"""
from __future__ import annotations
import argparse, csv, gzip, hashlib, io, json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

R114_METRICS_SHA='02c0db4315037121cea05286f235b233d7ea7e8f5ab537c27a0fc300bc9c17c4'
R114_MANIFEST_SHA='f58876ecc18dbf8894bc3df8718d41b8b2ec2374b91279228d5c67123c6c984a'
HAM_HASHES={
'2026-08-05.csv.gz':'10a8314234ac33b1c3045fd2df3679e5aae15509b42c68920eec0d3f86c6d7a0',
'2026-08-06.csv.gz':'663603f2daedf12fd2dea79e9b9d9c83aebcc7f57d11dbc51eb1e19f858a1c3a',
'2026-08-11.csv.gz':'a005f0891cac64e13b36cc61d4dc9fbbde70d7df6bff962eeb0fa3c9bb6a51ef',
'2026-08-12.csv.gz':'58b7529dd8515134163a29ab6de4df3310ac01397994bebaa8586ddaf15d4f95',
'2026-08-13.csv.gz':'b133c38bab361ade60d1315c17303e07e19a20493b720cb93165e5b3c808ea4a',
'2026-08-14.csv.gz':'16d5bcb94fdf6aafbbfd84d9c2f30fe2226ff06de21fffcb592f0d4be4d42eaa',
'2026-08-26.csv.gz':'d50fbaece909fce5144963c1c316ef77a98eef0dcf04f73c46a92b4ec0757665',
}
BUCKETS=(('OPEN_0900_1000','09:00','10:00',60),('MID_1000_1430','10:00','14:30',270),('LATE_1430_1500','14:30','15:00',30),('PRECLOSE_1500_1520','15:00','15:20',20),('KRX_AUCTION_1530','15:30','15:31',1),('POST_KRX_1540_PLUS','15:40','20:00',1))
CSV_FIELDS={'trade_date','time','code','open','high','low','close','volume','value','source','value_quality'}

def sha(b:bytes)->str:return hashlib.sha256(b).hexdigest()
def read_csv_bytes(blob:bytes):return list(csv.DictReader(io.StringIO(blob.decode('utf-8-sig'))))
def code(s):return str(s).strip().zfill(6)
def write_csv(path,rows,cols):
 with open(path,'w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(rows)

def require_pinned(path,digest):
 b=Path(path).read_bytes();actual=sha(b)
 if actual!=digest:raise ValueError('PINNED_SHA_MISMATCH: '+str(path))
 return b

def validate_minute(row, filename):
 if set(row)<CSV_FIELDS:raise ValueError('MISSING_MINUTE_COLUMNS')
 d=str(row['trade_date']);tm=str(row['time']);c=code(row['code'])
 if d != filename[:10] or not (len(tm)==5 and tm[2]==':'):raise ValueError('ROW_DATE_TIME_MISMATCH')
 try:datetime.strptime(d+' '+tm,'%Y-%m-%d %H:%M')
 except ValueError as e:raise ValueError('INVALID_MINUTE_TIME') from e
 if not tm>= '09:00' or tm>'15:00':raise ValueError('UNEXPECTED_SESSION_RANGE')
 if not row['source'].startswith('YAHOO_CHART:') or row['value_quality']!='PROXY_CLOSE_X_VOLUME':
  raise ValueError('PROVENANCE_CHANGED_NEEDS_REVIEW')
 v=float(row['volume']);close=float(row['close']);reported=float(row['value'])
 if not (v>=0 and close>0):raise ValueError('NON_POSITIVE_PRICE_OR_NEG_VOLUME')
 if abs(reported-close*v)>max(1,abs(reported)*1e-8):raise ValueError('PROXY_CALC_INCONSISTENT')
 return d,tm,c,v,reported

def is_execution_eligible(record):
 # All R1.16 evidence is unverified Yahoo volume and synthetic traded value.
 # This flag must remain FALSE even if all minute labels are present.
 return False

def build(minute_dir, r114_dir, output_dir):
 minute_dir=Path(minute_dir);r114_dir=Path(r114_dir);output_dir=Path(output_dir);output_dir.mkdir(parents=True,exist_ok=True)
 metrics=read_csv_bytes(require_pinned(r114_dir/'r114_1059_exante_descriptive_metrics.csv',R114_METRICS_SHA))
 require_pinned(r114_dir/'r114_integrity_manifest.json',R114_MANIFEST_SHA)
 if len(metrics)!=1059:raise ValueError('R114_1059_COUNT_CHANGED')
 signal={}
 for row in metrics:
  k=(row['signal_date'],code(row['code']))
  if k in signal:raise ValueError('R114_DUPLICATE_SIGNAL')
  signal[k]=row
 rows_by_key=defaultdict(list);seen=set();file_audit=[]
 for filename,digest in sorted(HAM_HASHES.items()):
  path=minute_dir/filename;blob=require_pinned(path,digest)
  data=read_csv_bytes(gzip.decompress(blob))
  if not data:raise ValueError('EMPTY_HAM_SOURCE')
  for row in data:
   d,t,c,v,val=validate_minute(row,filename)
   ident=(d,c,t)
   if ident in seen:raise ValueError('DUPLICATE_MINUTE')
   seen.add(ident)
   rows_by_key[(d,c)].append((t,v,val))
  file_audit.append({'source_file':filename,'sha256':digest,'rows':len(data),'source_label':'YAHOO_CHART','volume_verification':'NOT_INDEPENDENTLY_VERIFIED','value_provenance':'PROXY_CLOSE_X_VOLUME','has_1520':False,'has_1530':False})
 if len(seen)!=3610 or len(rows_by_key)!=10:raise ValueError('PINNED_HAM_SAMPLE_CARDINALITY_CHANGED')
 days={k[0] for k in rows_by_key}
 bucket_rows=[];join_rows=[];stock_days=[]
 for k,data in sorted(rows_by_key.items()):
  d,c=k;h={t:(v,val) for t,v,val in data}
  if len(h)!=len(data):raise ValueError('DUPLICATE_TIMES')
  st=signal.get(k)
  stock_days.append({'trade_date':d,'code':c,'observed_minutes':len(h),'earliest_time':min(h),'latest_time':max(h),'r114_signal_joined':int(st is not None),'r114_stage':st['r112_stage'] if st else 'NO_R114_SIGNAL','quarantined':int(bool(st) and st['descriptive_group']=='SOURCE_QUARANTINE')})
  for nm,start,end,expected in BUCKETS:
   n=sum(start<=t<end for t in h)
   vol=sum(v for t,(v,_) in h.items() if start<=t<end)
   # Observed volume from minute bars can be summarized, but time semantics and exchange parity unverified.
   bucket_rows.append({'trade_date':d,'code':c,'bucket':nm,'observed_minute_labels':n,'expected_minute_labels':expected,'label_coverage':('LABELS_PRESENT' if n>=expected else 'PARTIAL' if n else 'ABSENT'),'reported_minute_volume':int(vol),'reported_value_actual': 'UNKNOWN','reported_value_proxy':int(sum(vl for t,(v,vl) in h.items() if start<=t<end)) if n else '','source':'YAHOO_CHART_UNVERIFIED','trading_eligible':False,'at_1503_available':nm in ('OPEN_0900_1000','MID_1000_1430','LATE_1430_1500')})
  if st:
   join_rows.append({'signal_date':d,'code':c,'r114_wave_stage':st['r112_stage'],'r114_group':st['descriptive_group'],'minute_labels':len(h),'earliest':min(h),'latest':max(h),'source':'YAHOO_CHART_UNVERIFIED','turnover':'CLOSE_X_VOLUME_PROXY','can_analyze_1500_1520_full':False,'can_analyze_1530_auction':False,'valid_signal_case_for_independent_analysis':st['descriptive_group']!='SOURCE_QUARANTINE','trade_authority':False})
 write_csv(output_dir/'r116_legacy_ham_source_files.csv',file_audit,['source_file','sha256','rows','source_label','volume_verification','value_provenance','has_1520','has_1530'])
 write_csv(output_dir/'r116_10_code_day_inventory.csv',stock_days,['trade_date','code','observed_minutes','earliest_time','latest_time','r114_signal_joined','r114_stage','quarantined'])
 write_csv(output_dir/'r116_60_bucket_coverage.csv',bucket_rows,['trade_date','code','bucket','observed_minute_labels','expected_minute_labels','label_coverage','reported_minute_volume','reported_value_actual','reported_value_proxy','source','trading_eligible','at_1503_available'])
 write_csv(output_dir/'r116_3_signal_matches.csv',join_rows,['signal_date','code','r114_wave_stage','r114_group','minute_labels','earliest','latest','source','turnover','can_analyze_1500_1520_full','can_analyze_1530_auction','valid_signal_case_for_independent_analysis','trade_authority'])
 stages=Counter(x['r114_group'] for x in join_rows)
 stats={'source_artifact_run':38736163160,'source_artifact_id':11978660997,'r114_run':38831145566,'r114_artifact_id':12017450988,'source_format':'GITHUB_STOCKHUNTER_LEGACY_HAM_YAHOO_MINUTES','trade_authority':False,'status':'PASS_DATA_DISCOVERED_BUT_NOT_TRADE_ELIGIBLE','cached_minute_files':len(file_audit),'unique_dates':len(days),'minute_rows':len(seen),'code_day_pairs':len(stock_days),'matched_r114_signals':len(join_rows),'matched_quarantined':stages['SOURCE_QUARANTINE'],'matched_nonquarantined':len(join_rows)-stages['SOURCE_QUARANTINE'],'full_1500_1520_windows':0,'verified_1530_auction_events':0,'actual_traded_value_bars':0,'independently_verified_exchange_minutes':0,'blind_human_labels':0,'data_treatment':'UNVERIFIED_VENDOR_1MIN_VOLUME_AND_CLOSE_X_VOLUME_TURNOVER_PROXY','intraday_live_decision_authority':'NONE','notes':['09:00..15:00 timestamps 361 per code-day; bar stamp semantics unverified','3 joined signal-days include 2 quarantined events','Cannot infer 15:20 or 15:30 from 15:00 minute bar or daily OHLCV','No cross-sectional win-rate claims from single non-quarantined join']}
 assert (len(file_audit),len(days),len(stock_days),len(seen),len(join_rows),stages['SOURCE_QUARANTINE'])==(7,7,10,3610,3,2)
 stats['output_sha256']={p.name:sha(p.read_bytes()) for p in sorted(output_dir.glob('r116_*.csv'))}
 (output_dir/'r116_integrity_manifest.json').write_text(json.dumps(stats,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
 print('R116_PINNED_EVIDENCE_PASS',json.dumps({k:stats[k] for k in ('minute_rows','code_day_pairs','matched_r114_signals','matched_quarantined','matched_nonquarantined','status')},ensure_ascii=False))
 return stats

def main():
 p=argparse.ArgumentParser();p.add_argument('--minute-dir',required=True);p.add_argument('--r114-dir',required=True);p.add_argument('--out',required=True)
 a=p.parse_args();build(a.minute_dir,a.r114_dir,a.out)
if __name__=='__main__':main()
