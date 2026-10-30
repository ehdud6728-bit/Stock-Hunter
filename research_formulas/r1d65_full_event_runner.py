#!/usr/bin/env python3
"""Full R1D56 frozen event keys against independently supplied seven R1D50
source arrays. No outcomes calculated. Fail closed on identity and price basis.
Not available in this distribution because original arrays were not supplied.
"""
from __future__ import annotations
import argparse,io,gzip,json,hashlib,zipfile,pathlib,collections
import numpy as np
import pandas as pd
from r1d65_strict_triangle import geometry

ARRAYS=['cal','codes','open','high','low','close','amount']

def checksum(p):
 h=hashlib.sha256()
 with open(p,'rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()

def read_features(src):
 src=pathlib.Path(src);base='01_ASOF_STRUCTURAL_FEATURES.csv.gz'
 if src.is_dir():
  matches=list(src.rglob(base))
  if len(matches)!=1:raise ValueError('MISSING_OR_DUPLICATE_D56_FEATURE')
  return pd.read_csv(matches[0],dtype={'code':str})
 with zipfile.ZipFile(src) as z:
  matches=[n for n in z.namelist() if n.endswith('/'+base)]
  if len(matches)!=1:raise ValueError('MISSING_OR_DUPLICATE_D56_FEATURE')
  return pd.read_csv(gzip.GzipFile(fileobj=io.BytesIO(z.read(matches[0]))),dtype={'code':str})

def run(d56,root,out):
 out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
 src=pathlib.Path(root)
 paths={k:src/('_cache_'+k+'.npy') for k in ARRAYS}
 for key,p in paths.items():
  if not p.is_file():raise FileNotFoundError('MISSING_R1D50_'+key+':'+str(p))
 shas={key:checksum(p) for key,p in paths.items()}
 a={key:np.load(p,mmap_mode=None if key=='codes' else 'r',allow_pickle=(key=='codes')) for key,p in paths.items()}
 codes=np.asarray(a['codes']).astype(str);days=np.asarray(a['cal']).astype(int)
 if len(codes)!=len(set(codes)) or not np.array_equal(codes,np.sort(codes)) or np.any(np.diff(days)<=0):
  raise ValueError('NON_CANONICAL_SOURCE_INDEX')
 for k in ('open','high','low','close','amount'):
  if a[k].shape!=(len(codes),len(days)):
   raise ValueError('SHAPE_MISMATCH_'+k)
 f=read_features(d56)
 key=['date','code','event_type']
 if f.duplicated(key).any() or len(f)!=84922:raise ValueError('R1D56_FROZEN_EVENT_IDENTITY_BAD')
 f['code']=f.code.str.zfill(6)
 ix=np.searchsorted(codes,f.code.to_numpy());dt=np.searchsorted(days,f.date.to_numpy(dtype=int))
 if (ix>=len(codes)).any() or not np.array_equal(codes[ix],f.code.to_numpy()):
  raise ValueError('SOURCE_MISSING_EVENT_TICKERS')
 if (dt>=len(days)).any() or not np.array_equal(days[dt],f.date.to_numpy(dtype=int)):
  raise ValueError('SOURCE_MISSING_EVENT_DATES')
 rows=[];cnt=collections.Counter()
 # As-of parity check is performed on EVERY candidate event, not a selected sample.
 for n,rec in enumerate(f.itertuples(index=False)):
  t=int(dt[n]);k=int(ix[n])
  legacy=bool(float(rec.tri40_triangle_breakout or 0)==1.0) if pd.notna(rec.tri40_triangle_breakout) else False
  obj={'date':int(rec.date),'code':str(rec.code),'market':str(rec.market),
       'event_type':str(rec.event_type),'original_tri40':legacy,'below224_legacy':bool(pd.notna(rec.ma224_dist_pct) and rec.ma224_dist_pct<0)}
  if rec.asof_status!='OK':
   z={'reason':'FROZEN_ASOF_NOT_OK','status':'SOURCE_INVALID','geometry_valid':False,'qualified_with_amount':False}
  elif t<223:
   z={'reason':'SOURCE_MA224_HISTORY_MISSING','status':'SOURCE_INVALID','geometry_valid':False,'qualified_with_amount':False}
  else:
   v={name:a[name][k] for name in ('open','high','low','close','amount')}
   c=float(v['close'][t]);pr=np.asarray(v['close'][t-223:t+1],float)
   av=np.asarray(v['amount'][t-20:t],float)
   if not np.isfinite(pr).all() or (pr<=0).any() or not np.isfinite(av).all() or (av<0).any() or av.mean()<=0:
    z={'reason':'INVALID_RAW_HISTORY','status':'SOURCE_INVALID','geometry_valid':False,'qualified_with_amount':False}
   else:
    ma224=float(pr.mean());d=100*(c/ma224-1);ratio=float(v['amount'][t])/float(av.mean())
    if not np.isclose(d,float(rec.ma224_dist_pct),atol=1e-5,rtol=0) or not np.isclose(ratio,float(rec.amount_vs_prior20),atol=1e-5,rtol=0):
     z={'reason':'RAW_PRICE_ASOF_PARITY_MISMATCH','status':'SOURCE_INVALID','geometry_valid':False,'qualified_with_amount':False}
    else:z=geometry(v,t,ma224=ma224)
  cnt[z['reason']]+=1
  rows.append({**obj,**z})
 outdf=pd.DataFrame(rows)
 outdf.to_csv(out/'R1D65_D56_ALL_84922_EVENTS_STRICT_GEOMETRY.csv.gz',index=False,compression='gzip')
 old=outdf[outdf.original_tri40]
 old.to_csv(out/'R1D65_OLD_TRI40_2581_RECHECK.csv.gz',index=False,compression='gzip')
 new=outdf[outdf.qualified_with_amount.eq(True)]
 audit={'research_only':True,'live_changes':False,'frozen_events':len(outdf),
        'original_tri40_count':len(old),'new_strict_signal_count':len(new),
        'old_tri40_new_strict_overlap_count':int(old.qualified_with_amount.sum()),
        'new_strict_not_old_tri40':int((~new.original_tri40).sum()),
        'source_sha256s':shas,'d56_archive_sha256':checksum(d56) if pathlib.Path(d56).is_file() else None,
        'failure_reasons':dict(cnt),'no_future_outcomes':True,
        'point_in_time_price_and_CA_uncertainty_still_applies':True,
        'backtest_returns_recomputed':False}
 (out/'R1D65_D56_FULL_GEOMETRY_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf8')
 print(json.dumps(audit,ensure_ascii=False,indent=2))
 return audit

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--d56',required=True);p.add_argument('--price-root',required=True);p.add_argument('--out',required=True)
 x=p.parse_args();run(x.d56,x.price_root,x.out)
