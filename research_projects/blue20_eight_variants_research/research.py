from pathlib import Path
import sys,os,json,hashlib,importlib.util
import numpy as np
import pandas as pd
H=Path(__file__).resolve().parent;R=H.parent;O=H/'results';O.mkdir(exist_ok=True);sys.path.insert(0,str(H/'vendor'))
from loader import load_prices
from timing_engine import simulate
from pullback_engine import pre_pullback
PERIODS=[20,25,35,40]
CONFIGS=[(n,2.,26) for n in PERIODS]+[(n,1.8,20) for n in PERIODS]

def bands(close,n,multiplier=2.,shift=26):
 r=close.rolling(n,min_periods=n);m=r.mean();s=r.std(ddof=0);return {'UPPER':(m+multiplier*s).shift(shift),'LOWER':(m-multiplier*s).shift(shift)}
def signal(d,line):
 b=d.close-d.open
 return (line.lt(line.shift())&d.close.shift().le(line.shift())&d.open.lt(line)&d.close.gt(line)&b.gt(0)&((line-d.open)/b).ge(.2)&((d.close-line)/b).ge(.2)&(100*b/d.open).ge(.5)).fillna(False)

def main():
 for name,sha in json.loads((H/'reference/SHA256.json').read_text()).items():assert hashlib.sha256((H/'reference'/name).read_bytes()).hexdigest()==sha,name
 manifest=Path(os.environ.get('BLUE20_MANIFEST',str(Path(os.environ.get('TEMP','/tmp'))/'stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv')));f=load_prices(manifest);cal=pd.Index(sorted({v for d in f.values() for v in d.date}));cp={v:i for i,v in enumerate(cal)};market=pd.read_csv(H/'reference/MARKET_MAPPING.csv.gz',dtype={'code':str}).set_index(['code','date']);bad=pd.read_csv(H/'reference/KNOWN_ISSUES.csv',dtype={'code':str});badmap={c:list(g.date) for c,g in bad.groupby('code')};old=pd.read_csv(H/'reference/OLD_BB_EVENTS.csv.gz',dtype={'code':str});old=old[old.entry_rule.eq('SIGNAL_NEXT_OPEN')&old.model.isin(['BB20','BB40'])].set_index(['model','code','date','band']);rows=[];trades=[];pathcache={};featurecache={};replayed=0
 frozen=json.loads((H/'reference/PRICE_SOURCE_HASHES.json').read_text());assert set(f)==set(frozen)
 for c,d in f.items():assert hashlib.sha256(d.to_csv(index=False,lineterminator='\n').encode('utf-8-sig')).hexdigest()==frozen[c],('SOURCE_PRICE_CHANGED',c)
 for code,d in f.items():
  pos={v:i for i,v in enumerate(d.date)};mid=d.close.rolling(20).mean();ecross=list(d.index[d.close.shift().le(mid.shift())&d.close.gt(mid)])
  for n,multiplier,shift in CONFIGS:
   model='BB'+str(n) if multiplier==2. and shift==26 else 'BB'+str(n)+'_S18_SHIFT20'
   for band,line in bands(d.close,n,multiplier,shift).items():
    for j in d.index[signal(d,line)&d.date.between('2023-01-01','2024-12-31')]:
     day=d.date.iloc[j];p=cp[day];oid=f'{model}:{code}:{day}:{band}';prior=[q for q in ecross if 0<p-cp[d.date.iloc[q]]<=40];m=market.loc[(code,day)] if (code,day) in market.index else None
     if (code,day) not in featurecache:
      known=p>=20 and all(v in pos for v in cal[p-20:p]);feat=pre_pullback(d,j,20) if known else dict(status='UNKNOWN_HISTORY');feat['rally_type']='UNKNOWN'
      if feat['status']=='KNOWN':
       peak=feat['peak_index'];pkp=cp[d.date.iloc[peak]];old_day=cal[pkp-20] if pkp>=20 else None;base=float(d.close.iloc[pos[old_day]]) if old_day in pos and all(v in pos for v in cal[max(0,pkp-20):pkp+1]) else np.nan;gain=100*(feat['peak_price']/base-1) if np.isfinite(base) else np.nan;feat['prior_peak_rise20_pct']=gain;feat['rally_type']='UNKNOWN' if not np.isfinite(gain) else 'NO_PULLBACK_LT3' if feat['depth_pct']<3 else 'RALLY_PULLBACK' if gain>=10 else 'DECLINE_OR_WEAK_RALLY'
      featurecache[code,day]=feat
     feat=featurecache[code,day];base=dict(opportunity_id=oid,model=model,period=n,std_multiplier=multiplier,line_shift=shift,configuration='BASE_2_SHIFT26' if shift==26 else 'NEW_1P8_SHIFT20',code=code,date=day,year=int(day[:4]),band=band,listing_market=m.listing_market if m is not None else 'UNKNOWN',name_at_signal=m.name_at_signal if m is not None else code,env_prior40=bool(prior),depth_group=feat.get('depth_group','UNKNOWN'),rally_type=feat['rally_type'],depth_pct=feat.get('depth_pct',np.nan),peak_date=feat.get('peak_date',''),trough_date=feat.get('trough_date',''),recovered_half=feat.get('recovered_half',None),known_source_issue=any(cal[max(0,p-240)]<=v<=cal[min(len(cal)-1,p+60)] for v in badmap.get(code,[])))
     start=p+1;days=cal[start:min(start+60,len(cal))];full=len(days)==60 and all(v in pos for v in days);base['full60']=full;rows.append(base)
     for target in [5,10,20,40]:
      key=(code,day,target)
      if key not in pathcache:
       if not len(days):r=dict(status='NO_ENTRY_SESSION')
       elif any(v not in pos for v in days):r=dict(status='MISSING_BAR')
       else:
        w=d.iloc[[pos[v] for v in days]];r=simulate(w.open.to_numpy(),w.high.to_numpy(),w.close.to_numpy(),list(days),0,59,False,target,full)
       pathcache[key]=r
      result=pathcache[key];trades.append(base|dict(target_pct=target)|result)
      if multiplier==2. and shift==26 and n in [20,40] and target==10:
       ref=old.loc[('BB'+str(n),code,day,band)];expected='TP' if ref.status=='TP10' else 'NO_TP' if ref.status=='NO_TP60' else 'NO_ENTRY_SESSION' if ref.status=='NO_NEXT_SESSION' else ref.status;assert result['status']==expected
       if expected in ['TP','NO_TP']:assert np.isclose(result['net_pct'],ref.net_pct) and result['exit_date']==ref.exit_date
       replayed+=1
 a=pd.DataFrame(rows).sort_values(['model','band','code','date']).reset_index(drop=True);actual=set(zip(a[a.model.isin(['BB20','BB40'])].model,a[a.model.isin(['BB20','BB40'])].code,a[a.model.isin(['BB20','BB40'])].date,a[a.model.isin(['BB20','BB40'])].band));assert actual==set(old.index)
 a['nonoverlap60']=False;a['union_nonoverlap60']=False
 for _,g in a.groupby(['model','band','code']):
  last=-10000
  for ix in g.index:
   p=cp[a.at[ix,'date']]
   if p-last>60:a.at[ix,'nonoverlap60']=True;last=p
 common=a.groupby(['code','date','band']).model.agg(lambda x:'|'.join(sorted(x))).rename('same_day_models');a=a.merge(common,on=['code','date','band'],validate='many_to_one');a['same_day_model_count']=a.same_day_models.str.count('BB')
 keep=set()
 for _,g in a[['code','date','band']].drop_duplicates().sort_values(['code','band','date']).groupby(['code','band']):
  last=-10000
  for q in g.itertuples():
   p=cp[q.date]
   if p-last>60:keep.add((q.code,q.date,q.band));last=p
 a['union_nonoverlap60']=[(c,d,b) in keep for c,d,b in zip(a.code,a.date,a.band)];a.to_csv(O/'SIGNALS.csv.gz',index=False)
 t=pd.DataFrame(trades).merge(a[['opportunity_id','nonoverlap60','union_nonoverlap60','same_day_models','same_day_model_count']],on='opportunity_id',validate='many_to_one')
 prior4=pd.read_csv(H/'reference/BASE4_TRADES.csv.gz',dtype={'code':str},low_memory=False).set_index(['opportunity_id','target_pct']).sort_index()
 now4=t[t.configuration.eq('BASE_2_SHIFT26')].set_index(['opportunity_id','target_pct']).sort_index()
 assert prior4.index.equals(now4.index)
 for column in ['status','entry_price','net_pct','exit_date','holding_sessions','full60']:
  pd.testing.assert_series_equal(prior4[column],now4[column],check_names=False,check_dtype=False,rtol=1e-10,atol=1e-10)
 t.to_csv(O/'TRADES.csv.gz',index=False)
 specs={'SUMMARY':['model','band','target_pct'],'LISTING_MARKET':['listing_market','model','band','target_pct'],'PULLBACK':['model','band','target_pct','depth_group','rally_type'],'ENV_PRIOR':['model','band','target_pct','env_prior40'],'COMMON_SIGNALS':['model','band','target_pct','same_day_model_count']}
 for name,cols in specs.items():
  result=[]
  for scope,mask in [('ALL_SIGNALS',pd.Series(True,index=t.index)),('NONOVERLAP60',t.nonoverlap60),('UNION_NONOVERLAP60',t.union_nonoverlap60),('CLEAN_NONOVERLAP60',t.nonoverlap60&~t.known_source_issue)]:
   for year,g in [('ALL_YEARS',t[mask]),('2023',t[mask&t.year.eq(2023)]),('2024',t[mask&t.year.eq(2024)])]:
    for keys,q in g.groupby(cols,observed=True,dropna=False):
     z=q[q.full60&q.status.isin(['TP','NO_TP'])];win=z[z.net_pct>0].net_pct;loss=z[z.net_pct<0].net_pct;result.append(dict(zip(cols,keys))|dict(scope=scope,year=year,signals=len(q),n=len(z),tp_pct=100*z.status.eq('TP').mean(),mean_net=z.net_pct.mean(),win_pct=100*z.net_pct.gt(0).mean(),payoff=win.mean()/abs(loss.mean()) if len(loss) else np.nan,mean_hold=z.holding_sessions.mean(),sample_status='SPARSE_LT30' if len(z)<30 else 'DESCRIPTIVE_GE30'))
  pd.DataFrame(result).to_csv(O/(name+'.csv'),index=False)
 assert (a.listing_market!='UNKNOWN').all();(O/'VERIFICATION.json').write_text(json.dumps(dict(status='PASS',old_BB20_BB40_signal_set='PASS',old_target10_replayed=replayed,periods=PERIODS,configurations=CONFIGS,base4_replay='PASS',signals=len(a),listing_unknown=int(a.listing_market.eq('UNKNOWN').sum()),new_github_run='NOT_EXECUTED',corporate_actions='UNVERIFIED'),indent=2),encoding='utf-8')
 print(pd.read_csv(O/'SUMMARY.csv').query("scope=='NONOVERLAP60' and band=='UPPER' and year!='ALL_YEARS' and target_pct in [5,20]")[['model','year','target_pct','n','tp_pct','mean_net','mean_hold']].round(2).to_string(index=False))
if __name__=='__main__':main()
