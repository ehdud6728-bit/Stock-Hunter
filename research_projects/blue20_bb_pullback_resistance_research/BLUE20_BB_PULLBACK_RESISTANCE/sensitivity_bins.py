from pathlib import Path
import sys,os
import pandas as pd
import numpy as np
H=Path(__file__).resolve().parent;R=H.parent;O=H/'results';sys.path.insert(0,str(R/'BLUE20_PULLBACK_RESET/vendor'))
from loader import load_prices
from profile import volume_profile
frames=load_prices(Path(os.environ.get('BLUE20_MANIFEST',str(Path(os.environ.get('TEMP','/tmp'))/'stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv'))))
t=pd.read_csv(O/'TRADES.csv.gz',dtype={'code':str});t=t[t.fair100&t.nonoverlap60&t.band.eq('UPPER')&t.policy.eq('A_FIRST')];cache={};rows=[]
for r in t.itertuples():
 key=(r.code,r.date)
 if key not in cache:
  d=frames[r.code];j=int(d.index[d.date.eq(r.date)][0]);old=d.iloc[:j].iloc[-120:];close=float(d.close.iloc[j]);vals=[]
  for bins in [16,32,64]:
   for method in ['range','typical']:
    p=volume_profile(old,bins,method)
    if p is None:share=np.nan
    else:share=100*p['weights'][(p['centers']>=close)&(p['centers']<=1.1*close)].sum()/p['weights'].sum()
    vals.append(dict(bins=bins,method=method,volume_share_above10=share))
  cache[key]=vals
 for v in cache[key]:rows.append(dict(opportunity_id=r.opportunity_id,model=r.model,code=r.code,date=r.date,year=r.year,success=r.status=='TP10',net_pct=r.net_pct,**v))
a=pd.DataFrame(rows);a['high_volume']=a.volume_share_above10.ge(30);a.to_csv(O/'BIN_SENSITIVITY_EVENTS.csv.gz',index=False)
a.groupby(['model','year','bins','method','high_volume']).agg(n=('success','size'),tp_pct=('success',lambda x:100*x.mean()),mean_net=('net_pct','mean')).reset_index().to_csv(O/'BIN_SENSITIVITY.csv',index=False)
print('bin sensitivity complete',len(a))
