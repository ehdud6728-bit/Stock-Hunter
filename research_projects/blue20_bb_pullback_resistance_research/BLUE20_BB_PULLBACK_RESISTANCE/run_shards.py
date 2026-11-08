from pathlib import Path
import sys,os,subprocess,json,time
import pandas as pd
H=Path(__file__).resolve().parent;O=H/'results';O.mkdir(exist_ok=True)
jobs=[]
for k in range(4):
 env=os.environ.copy();env['BLUE20_DIAG_SHARD']=str(k)
 log=(H/f'SHARD_{k}.log').open('w',encoding='utf-8');p=subprocess.Popen([sys.executable,str(H/'study.py')],env=env,stdout=log,stderr=subprocess.STDOUT);jobs.append((p,log))
for k,(p,log) in enumerate(jobs):
 rc=p.wait();log.close();assert rc==0,(k,rc,(H/f'SHARD_{k}.log').read_text()[-2000:]);print('shard complete',k,flush=True)
for name in ['OPPORTUNITIES.csv.gz','TRADES.csv.gz','SNAPSHOTS.csv.gz','ZONES.csv.gz']:
 parts=[pd.read_csv(H/f'shards/{k}'/name,dtype={'code':str}) for k in range(4)];a=pd.concat(parts,ignore_index=True)
 if name=='SNAPSHOTS.csv.gz':assert not a.snapshot_id.duplicated().any()
 a.to_csv(O/name,index=False)
st=json.loads((H/'shards/0/STATUS.json').read_text());st['opportunities']=len(pd.read_csv(O/'OPPORTUNITIES.csv.gz'));st['trade_rows']=len(pd.read_csv(O/'TRADES.csv.gz'));st['snapshots']=len(pd.read_csv(O/'SNAPSHOTS.csv.gz'));st['shards_verified']=4;(O/'STATUS.json').write_text(json.dumps(st,indent=2),encoding='utf-8');print('all shards merged',flush=True)
