#!/usr/bin/env python3
"""Render price-only 224MA and ACTUAL KRX Amount chart, excluding ALL post-signal bars.

No labels revealing the hidden forward price outcome. Does not open outcome CSV.
"""
from __future__ import annotations
import argparse
import csv
import gzip
from collections import defaultdict
from pathlib import Path
import pandas as pd


def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--result-root',required=True)
 p.add_argument('--max-charts',type=int,default=48)
 a=p.parse_args(argv)
 if a.max_charts<1 or a.max_charts>100:raise ValueError('BAD_CHART_COUNT')
 root=Path(a.result_root)
 with (root/'BLIND_SEALED_CHART_REVIEW_KEYS.csv').open(encoding='utf-8-sig',newline='') as f:
  mapping=list(csv.DictReader(f))
 if not mapping:print('R1D2_BLIND_CHARTS 0 NO_STRICT_PROXY_EPISODES');return 0
 keys={x['review_id']:x for x in mapping[:a.max_charts]}
 hist=defaultdict(list)
 with gzip.open(root/'BLIND_CHART_PRE_SIGNAL_ONLY.csv.gz','rt',encoding='utf-8-sig',newline='') as f:
  for row in csv.DictReader(f):
   k=row['review_id']
   if k not in keys:continue
   if row['date']>keys[k]['signal_date']:raise RuntimeError('BLIND_FUTURE_BAR_LEAK_BLOCKED')
   hist[k].append(row)
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 out=root/'blind_recent_224_charts_no_future';out.mkdir(parents=True,exist_ok=True)
 n=0
 for key,rows in hist.items():
  if not rows:continue
  rows=sorted(rows,key=lambda x:x['date'])
  frame=pd.DataFrame(rows)
  for k in ('open','high','low','close','actual_amount_KRW'):
   frame[k]=pd.to_numeric(frame[k],errors='raise')
  if len(frame)>260:raise RuntimeError('CHART_HISTORY_OVER_260')
  ma=frame['close'].rolling(224,min_periods=224).mean()
  fig,(ax,vol)=plt.subplots(2,1,figsize=(12,6.2),sharex=True,gridspec_kw={'height_ratios':[3,1]})
  x=list(range(len(frame)))
  ax.plot(x,frame['close'],linewidth=1.4,label='KRX original unadjusted close')
  ax.plot(x,ma,linewidth=1.0,label='SMA224 (if available)')
  ax.set_title('BLIND PRICE CONTEXT  |  '+key+'  |  forward returns concealed')
  ax.legend(loc='upper left',fontsize=8);ax.grid(alpha=.17)
  vol.bar(x,frame['actual_amount_KRW']/1e8,width=.85)
  vol.set_ylabel('Actual Amount\n(100M KRW)',fontsize=8)
  ticks=list(range(0,len(frame),max(1,len(frame)//7)))
  if ticks[-1]!=len(frame)-1:ticks.append(len(frame)-1)
  vol.set_xticks(ticks,[frame['date'].iloc[z] for z in ticks],rotation=25,ha='right',fontsize=8)
  fig.tight_layout()
  fig.savefig(out/(key+'.png'),dpi=115)
  plt.close(fig)
  n+=1
 print('R1D2_BLIND_CHARTS',n,'NO_FUTURE_OR_OUTCOME_READ',flush=True)
 return 0

if __name__=='__main__':raise SystemExit(main())
