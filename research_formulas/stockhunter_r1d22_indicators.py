#!/usr/bin/env python3
"""StockHunter R1-D22 supplemental indicator causal data audit (RESEARCH_ONLY).

Fixed R1D9 2021-23 + R1D6 2024-25 signals; preserved R1D20 ASOF BB/cloud;
raw KRX sessions checked by SHA; no future prices used for technical features;
seal features before reading frozen D5/D10/D15/D20 outcomes. No LIVE impact.
"""
from __future__ import annotations
import argparse,collections,gzip,hashlib,io,json,math,zipfile
from pathlib import Path
import numpy as np
import pandas as pd
DONE='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
BANDS=(20,30,40)
FEATURES={
 'RSI14_LOW':('rsi14',lambda x:x<40),
 'RSI14_MID':('rsi14',lambda x:(x>=40)&(x<=60)),
 'RSI14_HIGH':('rsi14',lambda x:x>60),
 'ADX14_TREND25':('adx14',lambda x:x>=25),
 'ADX14_WEAK20':('adx14',lambda x:x<20),
 'ATR14_LOW3PCT':('atr14_pct',lambda x:x<3),
 'ATR14_HIGH6PCT':('atr14_pct',lambda x:x>=6),
 'CMF20_POSITIVE':('cmf20',lambda x:x>0),
 'CMF20_ABOVE_010':('cmf20',lambda x:x>0.10),
 'SIGNED_VOLUME20_POSITIVE':('signed_vol20',lambda x:x>0),
 'MFI14_LT50':('mfi14',lambda x:x<50),
 'MFI14_GE50':('mfi14',lambda x:x>=50),
 'BBW20_NARROW':('bb_width20_pct',lambda x:x<12),
 'BBW20_WIDE':('bb_width20_pct',lambda x:x>=25),
 'VOL5_TO20_LT070':('volume5_over20',lambda x:x<.70),
 'VOL5_TO20_GE120':('volume5_over20',lambda x:x>=1.20),
 'RETRACE_LE50':('pullback_intraday_max_retrace_pct',lambda x:x<=50),
 'CLOUD_ABOVE':('cloud_above',lambda x:x==1),
 'CLOUD_BELOW':('cloud_below',lambda x:x==1),
}

def require(cond,msg):
 if not cond:raise RuntimeError('R1D22_FAIL_CLOSED_'+str(msg))
def hash_file(f):return hashlib.sha256(Path(f).read_bytes()).hexdigest()
def read_frozen(p,compression='infer'):
 return pd.read_csv(p,compression=compression,dtype={'date':str,'code':str},low_memory=False)
def save(df,p):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 df.to_csv(p,index=False,encoding='utf-8-sig',compression='gzip' if p.suffix=='.gz' else None)
def savejson(obj,p):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 p.write_text(json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True,default=str)+'\n',encoding='utf-8')
def wilder(x,n=14):
 """Wilder RMA: seed SMA of first n observations (not EMA alpha=2/(n+1))."""
 x=np.asarray(x,dtype=float);out=np.full(len(x),np.nan)
 if len(x)<n or not np.isfinite(x).all():return out
 out[n-1]=np.mean(x[:n]);alpha=1.0/n
 for i in range(n,len(x)):out[i]=out[i-1]+alpha*(x[i]-out[i-1])
 return out

def indicators(ohlcv):
 """Trailing 244 asof OHLCV bars ONLY, no historical future or trade outcomes.

 Return rejection for zero, missing, physically implausible OHLC, gaps >40pct,
 or noncontiguous calendar. RSI/ADX/ATR and other indicators are computed with
 deterministic Wilder 14 smoothing on 244 source days (bounded warmup).
 """
 rows=list(ohlcv)
 if len(rows)!=244:return {'feature_status':'INSUFFICIENT_244'}
 z=np.asarray(rows,dtype=float)
 if z.shape!=(244,7) or not np.isfinite(z).all():return {'feature_status':'INVALID_NONFINITE'}
 ix,o,h,l,c,vol,amt=z.T
 if not np.array_equal(ix,np.arange(int(ix[-1])-243,int(ix[-1])+1)):return {'feature_status':'NONCONTIGUOUS'}
 if np.any((o<=0)|(h<=0)|(l<=0)|(c<=0)|(h<np.maximum(o,c))|(l>np.minimum(o,c))|(h<l)|(vol<0)|(amt<0)):
  return {'feature_status':'INVALID_OHLCV'}
 if np.any(np.abs(c[1:]/c[:-1]-1)>.40):return {'feature_status':'UNADJUSTED_PRICE_GAP_PROXY'}
 if np.any(vol[-20:]<=0) or np.any(amt[-20:]<=0):return {'feature_status':'ZERO_RECENT_TURNOVER'}
 d=np.diff(c);up=np.maximum(d,0);down=np.maximum(-d,0)
 ag=wilder(up,14)[-1];al=wilder(down,14)[-1]
 rsi=100. if al==0 and ag>0 else (50. if al==0 and ag==0 else 100-100/(1+ag/al))
 prev=np.r_[c[0],c[:-1]]
 tr=np.maximum(h-l,np.maximum(np.abs(h-prev),np.abs(l-prev)))
 atr=wilder(tr[1:],14)[-1]
 upmove=np.diff(h);dnmove=-np.diff(l)
 pdm=np.where((upmove>dnmove)&(upmove>0),upmove,0)
 mdm=np.where((dnmove>upmove)&(dnmove>0),dnmove,0)
 atr_series=wilder(tr[1:],14); plus_smooth=wilder(pdm,14);minus_smooth=wilder(mdm,14)
 plus=np.where(atr_series>0,100*plus_smooth/atr_series,0)
 minus=np.where(atr_series>0,100*minus_smooth/atr_series,0)
 dx=np.divide(100*np.abs(plus-minus),plus+minus,out=np.zeros_like(plus),where=(plus+minus)>0)
 # ADX first valid after 14 raw TR smoothing + 14 DX observations,
 # with Wilder smoothing from that point (no future).
 dx_valid=dx[13:]
 adx=wilder(dx_valid,14)[-1]
 mfm=np.where(h>l,(2*c-h-l)/(h-l+1e-300),0)
 cmf=np.sum(mfm[-20:]*vol[-20:])/np.sum(vol[-20:])
 signed20=np.sum(np.sign(np.diff(c[-21:]))*vol[-20:])/np.sum(vol[-20:])
 typical=(h+l+c)/3
 diff=np.diff(typical)
 money=typical[1:]*vol[1:]
 positive=float(np.sum(money[-14:][diff[-14:]>0])); negative=float(np.sum(money[-14:][diff[-14:]<0]))
 mfi=100. if negative==0 and positive>0 else 50. if negative==positive==0 else 100.-100./(1+positive/negative)
 cl20=c[-20:];ma20=cl20.mean();std20=cl20.std(ddof=0)
 return {'feature_status':'ASOF_OK', 'indicator_close':float(c[-1]), 'rsi14':rsi,
   'adx14':float(adx),'atr14_pct':float(100*atr/c[-1]),'cmf20':float(cmf),
   'signed_vol20':float(signed20),'mfi14':float(mfi),
   'bb_width20_pct':float(400*std20/ma20),
   'volume5_over20':float(vol[-5:].mean()/vol[-20:].mean()),
   'rsi14_rising_5':int(rsi>rsi_at(c[:-5])),
   'cmf20_positive':int(cmf>0)}

def rsi_at(c):
 d=np.diff(c);g=wilder(np.maximum(d,0),14)[-1];l=wilder(np.maximum(-d,0),14)[-1]
 return 100. if l==0 and g>0 else 50. if l==g==0 else 100.-100/(1+g/l)

def audit_inputs(inputs,protocol):
 source=protocol['source'];r20=Path(inputs)/'r1d20';r9=Path(inputs)/'r1d9asof';r6=Path(inputs)/'r1d6asof'
 parts=[];keys={};srcfiles=[]
 for tag,name,oldroot,oldname,oldsha,n in [
  ('2021_2023','holdout_2021_23_ASOF_ONLY_NO_OUTCOME.csv.gz',r9,'R1D9_ASOF_ONLY_2021_23_NO_OUTCOME.csv.gz',source['r1d9_asof_csv_sha256'],7096),
  ('2024_2025','exploratory_2024_25_ASOF_ONLY_NO_OUTCOME.csv.gz',r6,'R1D6_ASOF_ONLY_FROZEN_FEATURES_NO_OUTCOME.csv.gz',source['r1d6_asof_csv_sha256'],5247)]:
  files=list(r20.rglob(name));old=list(oldroot.rglob(oldname))
  require(len(files)==len(old)==1,'INPUT_PATH_OR_MULTIPLE_'+tag)
  require(hash_file(old[0])==oldsha,'FROZEN_OLDER_ASOF_SHA_'+tag)
  b=read_frozen(files[0]);a=read_frozen(old[0])
  require(len(a)==len(b)==n,'SAMPLE_CARDINALITY_'+tag)
  for x in (a,b):
   require(not x[['date','code']].duplicated().any(),'DUPLICATE_SIGNAL_KEYS_'+tag)
   require(x.code.str.fullmatch(r'[0-9A-Z]{6}').all(),'SECURITY_ID_SHAPE_'+tag)
  require(set(map(tuple,a[['date','code']].to_numpy()))==set(map(tuple,b[['date','code']].to_numpy())),'KEY_MISMATCH_'+tag)
  require(a.date.eq(a.reference_data_last_bar_date).all(),'ORIGINAL_ASOF_REFERENCE_DATE_'+tag)
  cols=['date','code','wave_gain_pct','pullback_intraday_max_retrace_pct','amount_3d_vs_prior10d','joint_index_20d_regime','KOSPI_ret20_pct','KOSDAQ_ret20_pct','signal_5MA_reclaim_today','signal_20MA_reclaim_today']
  require(set(cols).issubset(a.columns),'OLD_FACTOR_MISSING')
  joined=b.merge(a[cols],on=['date','code'],validate='one_to_one')
  joined['period']=tag;parts.append(joined);keys[tag]=set(map(tuple,joined[['date','code']].to_numpy()))
  srcfiles.extend([str(files[0]),str(old[0])])
 return pd.concat(parts,ignore_index=True),keys,srcfiles

def krx_sha_and_features(krx_root,events):
 """Input layout: action download-artifact merge has shard_00...shard_07 dirs.
 Offline original ZIPs can be extracted first into the same layout.
 """
 root=Path(krx_root);rawfiles={};sources={}
 for i in range(8):
  p=root/f'shard_{i:02d}'/f'manifest_shard_{i:02d}.json'
  require(p.is_file(),'KRX_MANIFEST_MISSING_'+str(i))
  m=json.loads(p.read_text(encoding='utf-8'))
  for ent in m['files']:
   d=ent['date'];path=root/f'shard_{i:02d}'/ent['file']
   require(d not in rawfiles,'DUPLICATE_SOURCE_DAY_'+d)
   rawfiles[d]=(path,ent['sha256']);sources[d]=i
 days=sorted(rawfiles)
 require(len(days)==1907 and days[0]=='2019-01-02' and days[-1]=='2026-10-12','KRX_CALENDAR_LOCK_DRIFT')
 want=collections.defaultdict(set)
 for r in events[['date','code']].itertuples(index=False,name=None):want[r[0]].add(r[1])
 history=collections.defaultdict(lambda:collections.deque(maxlen=244))
 feats={};look_days=0
 for session,dt in enumerate(days):
  path,digest=rawfiles[dt]
  require(path.is_file(),'KRX_PRICE_DAY_MISSING_'+dt)
  raw=path.read_bytes();require(hashlib.sha256(raw).hexdigest()==digest,'KRX_DAILY_SHA_MISMATCH_'+dt)
  look_days+=1
  if dt>max(want):continue
  with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
   d=pd.read_csv(gz,usecols=['code','date','open','high','low','close','volume','amount'],dtype={'code':str,'date':str})
  require(len(d)>0 and d.date.eq(dt).all(),'KRX_ROW_DATE_MISMATCH_'+dt)
  require(not d.code.duplicated().any(),'KRX_TICKER_DUPLICATE_'+dt)
  found=set()
  for rr in d.itertuples(index=False):
   code=str(rr.code)
   if len(code)!=6:continue
   v=(session,rr.open,rr.high,rr.low,rr.close,rr.volume,rr.amount)
   history[code].append(v)
   if code not in want.get(dt,()):continue
   feats[(dt,code)]={'date':dt,'code':code,**indicators(history[code])};found.add(code)
  require(want.get(dt,set())==found,'MISSING_SOURCE_SIGNAL_'+dt)
  if session%400==0:print('R1D22_KRX_AUDIT',dt,'completed_days',session+1,'feature_rows',len(feats),flush=True)
 require(len(feats)==len(events) and look_days==1907,'FROZEN_POPULATION_OR_DAY_COUNT')
 return pd.DataFrame(feats.values()),look_days

def score(joined):
 j=joined[(joined.asof_status=='ASOF_OK')&(joined.feature_status=='ASOF_OK')&
  (joined.status_out==DONE)&(joined.ma224_below==1)].copy()
 j['win']=(j.net_20bps_pct>0).astype('int8');j['loss5']=(j.net_20bps_pct<=-5).astype('int8')
 # Predefined dimensions. Compare each yes/no pair within EACH band and period,
 # equal-day descriptive comparison, no p-value promotion or optimized cutoff.
 rows=[];year=[];matched=[]
 for (period,h),g in j.groupby(['period','horizon']):
  for bb in BANDS:
   s=g[g[f'bb{bb}_reclaim_recent5'].eq(1)]
   if s.empty:continue
   for name,(field,fn) in FEATURES.items():
    yes=fn(s[field]);a=s[yes];b=s[~yes]
    def stats(z):
     return dict(n=int(len(z)),dates=int(z.date.nunique()),win_pct=round(100*z.win.mean(),2) if len(z) else None,
      mean_net_pct=round(float(z.net_20bps_pct.mean()),2) if len(z) else None,
      median_net_pct=round(float(z.net_20bps_pct.median()),2) if len(z) else None,
      loss5_pct=round(100*z.loss5.mean(),2) if len(z) else None)
    # Vectorized same-date contrast, with only dates having both groups.
    tmp=s[['date','win','loss5']].copy();tmp['flag']=np.asarray(yes,dtype=bool)
    mm=tmp.groupby(['date','flag'],sort=False)[['win','loss5']].mean().unstack('flag')
    ok=mm.dropna() if (('win',True) in mm.columns and ('win',False) in mm.columns) else mm.iloc[0:0]
    pair_count=len(ok)
    pair_win=float((ok[('win',True)]-ok[('win',False)]).mean()*100) if pair_count else None
    pair_loss=float((ok[('loss5',True)]-ok[('loss5',False)]).mean()*100) if pair_count else None
    row={'period':period,'horizon':int(h),'band':f'BB{bb}','factor':name,
     **{'yes_'+k:v for k,v in stats(a).items()},**{'no_'+k:v for k,v in stats(b).items()},
     'matched_dates':pair_count,'equal_day_win_diff_pp':round(pair_win,2) if pair_count else None,
     'equal_day_loss5_diff_pp':round(pair_loss,2) if pair_count else None,
     'low_n_or_weak_match': bool(len(a)<30 or len(b)<30 or pair_count<8)}
    rows.append(row)
    # Market-regime sensitivity for each side, no conditional threshold tuning.
    if h==5:
     for regime,sb in s.groupby('joint_index_20d_regime'):
      flag=fn(sb[field]);p=sb[flag];q=sb[~flag]
      year.append({'period':period,'band':f'BB{bb}','factor':name,'regime':str(regime),
       'yes_n':len(p),'yes_win':round(100*p.win.mean(),2) if len(p) else None,
       'no_n':len(q),'no_win':round(100*q.win.mean(),2) if len(q) else None})
  # control: standalone bands vs other same-date below224 signals, equal-day.
  for bb in BANDS:
   m=g[['date','win',f'bb{bb}_reclaim_recent5']].copy()
   m['f']=m[f'bb{bb}_reclaim_recent5'].eq(1)
   day=m.groupby(['date','f'])['win'].mean().unstack('f').dropna()
   if True not in day.columns or False not in day.columns:day=day.iloc[0:0]
   matched.append({'period':period,'horizon':h,'band':f'BB{bb}','matched_dates':len(day),
    'equal_day_win_diff_pp':round(float(100*(day[True]-day[False]).mean()),2) if len(day) else None})
 return pd.DataFrame(rows),pd.DataFrame(year),pd.DataFrame(matched)

def run(args):
 p=json.loads(Path(args.protocol).read_text(encoding='utf-8'))
 require(p.get('research_id')=='STOCKHUNTER_R1D22_SUPPLEMENTAL_INDICATOR_QA','PROTOCOL')
 require(p['indicators_locked']==list(FEATURES),'PREDECLARED_THRESHOLDS')
 require(p['periods']==['2021_2023','2024_2025'] and p['bands']==[20,30,40],'PREDECLARED_POPULATION')
 out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
 if args.mode=='lock':
  savejson({'locked':True,'count':len(FEATURES),'research_only':True,'outcomes_read':False},out/'R1D22_LOCK_MANIFEST.json');return
 b,keys,srcfiles=audit_inputs(args.inputs,p)
 require(len(b)==12343,'POPULATION_MODIFIED')
 f,nsha=krx_sha_and_features(args.krx_root,b)
 x=b.merge(f,on=['date','code'],validate='one_to_one')
 require(len(x)==12343,'FEATURE_JOINS_DRIFT')
 accepted=x.asof_status.eq('ASOF_OK')
 require(int(accepted.sum())==12340 and int((~accepted).sum())==3,'PREVIOUS_QUALITY_STATUS_DRIFT')
 require(x.loc[accepted,'indicator_close'].notna().all(),'INDICATOR_PRICE_MISSING')
 require(np.allclose(x.loc[accepted,'signal_close'].values,x.loc[accepted,'indicator_close'].values,rtol=0,atol=1e-10),'KRX_PRICE_PARITY')
 # Record why any rejected-row feature was unavailable; never treat blocked as 'false'.
 require(x.loc[accepted,'feature_status'].eq('ASOF_OK').all(),'PREVIOUSLY_VALID_FEATURE_NOW_INVALID')
 x=x.sort_values(['period','date','code']).reset_index(drop=True)
 require(not any(z.startswith(('net_','entry_','exit_','future_','gross_')) for z in x.columns),'FUTURE_OUTCOME_IN_ASOF')
 featpath=out/'R1D22_ASOF_ONLY_ADX_ATR_RSI_VOLUME_FLOW_NO_OUTCOME.csv.gz';save(x,featpath)
 digest=hash_file(featpath)
 print('R1D22_ASOF_SEALED',len(x),digest,flush=True)
 outputs=[]
 for tag,root,file in [('2021_2023','r1d9out','R1D9_SEALED_2021_23_D5_D10_D15_D20.csv.gz'),('2024_2025','r1d7','R1D7_D5_D10_D15_D20_FROZEN_FORWARD_PRICE_PATHS.csv.gz')]:
  found=list((Path(args.inputs)/root).rglob(file));require(len(found)==1,'FROZEN_PATH_FILE_MISSING_'+tag)
  o=read_frozen(found[0]);require(len(o)==len(keys[tag])*4,'FROZEN_OUTCOMES_COUNT_'+tag)
  require(set(map(tuple,o[['date','code']].to_numpy()))==keys[tag],'FROZEN_OUTCOMES_SOURCE_POPULATION_'+tag)
  require(not o[['date','code','horizon']].duplicated().any(),'OUTCOME_DUPLICATES_'+tag)
  o['period']=tag;outputs.append(o)
 outcomes=pd.concat(outputs,ignore_index=True)
 j=x.merge(outcomes,on=['period','date','code'],validate='one_to_many')
 require(len(j)==12343*4 and set(j.horizon)=={5,10,15,20},'HORIZON_JOIN_COUNT')
 result_score,regime,control=score(j)
 save(result_score,out/'R1D22_SUPPLEMENTAL_INDICATOR_BB_D5_D10_D15_D20.csv')
 save(regime,out/'R1D22_MARKET_REGIME_SENSITIVITY_D5.csv')
 save(control,out/'R1D22_DATE_MATCHED_BAND_CONTROLS.csv')
 fullqa=x[['date','code','period','asof_status','feature_status','indicator_close','signal_close']].copy()
 save(fullqa,out/'R1D22_QUALITY_ALL_12343.csv.gz')
 manifest={'research_id':p['research_id'],'research_only':True,'krx_daily_sha_verified':nsha,
 'original_signal_count':len(x),'source_2021_23':len(keys['2021_2023']),'source_2024_25':len(keys['2024_2025']),
 'frozen_path_rows':len(outcomes),'asof_valid_r1d20':int(accepted.sum()),
 'asof_valid_r1d22':int(x.feature_status.eq('ASOF_OK').sum()),
 'previously_blocked':int((~accepted).sum()),'new_feature_blocks':int((accepted & x.feature_status.ne('ASOF_OK')).sum()),
 'matched_accepted_raw_close':int(accepted.sum()),'asof_sha256':digest,'scores':len(result_score),
 'market_regime_sensitivity_rows':len(regime),'date_matched_controls':len(control),
 'feature_outcome_sealed_before_join':True,'new_prospective_oos':False,
 'all_market_scanner_validation':False,'security_type_pit_certified':False,'corporate_action_pit_certified':False,
 'actual_fills_certified':False,'multiple_testing_risk':True,'strategy_ready':False,
 'no_live_changes':True,'orders_placed':0}
 savejson(manifest,out/'R1D22_MANIFEST.json');print('R1D22_QA_DONE',json.dumps(manifest,ensure_ascii=False),flush=True)

def main():
 a=argparse.ArgumentParser()
 a.add_argument('--mode',choices=['lock','validate'],required=True)
 a.add_argument('--protocol',default='R1D22_PREDECLARED_PROTOCOL.json')
 a.add_argument('--inputs',default='reports/r1d22/input')
 a.add_argument('--krx-root',default='reports/r1d22/input/krx')
 a.add_argument('--out',default='reports/r1d22/result')
 run(a.parse_args())
if __name__=='__main__':main()
