#!/usr/bin/env python3
import argparse,json,re,time,subprocess
from pathlib import Path
import numpy as np,pandas as pd

REV='EARLY_POWER_R7_REFRESH_20261008'

def norm(v):
 s=re.sub(r'\D','',str(v or '')); return s[-6:].zfill(6) if s else ''

def load(paths):
 xs=[]
 for p in paths:
  q=pd.read_parquet(p); q=q.reset_index() if 'Date' not in q.columns else q; xs.append(q)
 x=pd.concat(xs,ignore_index=True,sort=False); x['Date']=pd.to_datetime(x['Date'],errors='coerce').dt.normalize(); x['Code']=x['Code'].map(norm)
 for c in ['Open','High','Low','Close','Volume','Amount','Marcap','Stocks']:
  if c in x: x[c]=pd.to_numeric(x[c],errors='coerce')
 return x.dropna(subset=['Date','Code','Open','High','Low','Close']).sort_values(['Code','Date']).drop_duplicates(['Code','Date'],keep='last')

def fetch_day(ds):
 from pykrx import stock
 d=ds.replace('-','')
 q=stock.get_market_ohlcv_by_ticker(d,market='ALL')
 if q is None or q.empty:return pd.DataFrame()
 q=q.reset_index().rename(columns={'티커':'Code','시가':'Open','고가':'High','저가':'Low','종가':'Close','거래량':'Volume','거래대금':'Amount'})
 if 'Code' not in q:q=q.rename(columns={q.columns[0]:'Code'})
 q['Date']=pd.Timestamp(ds);q['Code']=q['Code'].map(norm)
 for c in ['Open','High','Low','Close','Volume','Amount']:
  if c in q:q[c]=pd.to_numeric(q[c],errors='coerce')
 try:
  m=stock.get_market_cap_by_ticker(d,market='ALL').reset_index().rename(columns={'티커':'Code','시가총액':'Marcap','상장주식수':'Stocks'})
  if 'Code' not in m:m=m.rename(columns={m.columns[0]:'Code'})
  m['Code']=m['Code'].map(norm); q=q.merge(m[[c for c in ['Code','Marcap','Stocks'] if c in m]],on='Code',how='left')
 except Exception as e: print('CAP_WARN',ds,type(e).__name__,str(e)[:120])
 return q[[c for c in ['Date','Code','Open','High','Low','Close','Volume','Amount','Marcap','Stocks'] if c in q]].dropna(subset=['Open','High','Low','Close'])

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--base',nargs='+',required=True);ap.add_argument('--target',default='2026-10-07');ap.add_argument('--outdir',required=True);a=ap.parse_args()
 out=Path(a.outdir);out.mkdir(parents=True,exist_ok=True)
 x=load(a.base);base_last=x.Date.max().normalize();target=pd.Timestamp(a.target).normalize();adds=[];audit=[]
 for d in pd.date_range(base_last+pd.Timedelta(days=1),target,freq='D'):
  ds=d.date().isoformat()
  try:q=fetch_day(ds)
  except Exception as e:q=pd.DataFrame();audit.append({'date':ds,'rows':0,'accepted':False,'error':f'{type(e).__name__}:{e}'});continue
  ok=len(q)>=100;audit.append({'date':ds,'rows':len(q),'accepted':ok});print('R7_DATE',ds,len(q),'TRADING' if ok else 'CLOSED')
  if ok:adds.append(q)
  time.sleep(.3)
 if adds:x=pd.concat([x]+adds,ignore_index=True,sort=False)
 x['Date']=pd.to_datetime(x.Date).dt.normalize();x['Code']=x.Code.map(norm);x=x.sort_values(['Code','Date']).drop_duplicates(['Code','Date'],keep='last')
 final=x.Date.max().normalize();rows=int((x.Date==target).sum())
 if final<target or rows<100:raise SystemExit(f'R7_TARGET_NOT_REACHED:{target.date()}:{final.date()}:{rows}')
 p=out/'marcap_augmented.parquet';x.to_parquet(p,index=False);pd.DataFrame(audit).to_csv(out/'price_date_audit.csv',index=False,encoding='utf-8-sig')
 meta={'revision':REV,'base_last_date':base_last.date().isoformat(),'target_date':target.date().isoformat(),'final_last_date':final.date().isoformat(),'target_rows':rows,'trading_dates_added':[z['date'] for z in audit if z.get('accepted')],'nontrading_dates':[z['date'] for z in audit if not z.get('accepted')]}
 (out/'price_meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8');print('R7_PRICE_PASS',json.dumps(meta,ensure_ascii=False))

if __name__=='__main__':main()
