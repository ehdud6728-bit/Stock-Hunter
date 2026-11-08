from pathlib import Path
import sys,os,json,time,warnings,importlib.util
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore',category=FutureWarning)
H=Path(__file__).resolve().parent;R=H.parent;O=H/('shards/'+os.environ['BLUE20_DIAG_SHARD'] if 'BLUE20_DIAG_SHARD' in os.environ else 'results');O.mkdir(parents=True,exist_ok=True)
sys.path.insert(0,str(R/'BLUE20_PULLBACK_RESET'));sys.path.insert(0,str(R/'BLUE20_PULLBACK_RESET/vendor'))
from loader import load_prices
from legacy_observer import indicators
from structure import zones
from profile import volume_profile
from engine import detect
spec=importlib.util.spec_from_file_location('no_stop_source',R/'BLUE20_NO_STOP60/research.py');ns=importlib.util.module_from_spec(spec);spec.loader.exec_module(ns)

def snapshot(d,j,n):
 old=d.iloc[:j];b=d.iloc[j];close=float(b.close);base=dict(period=n,history_known_through=old.date.iloc[-1] if len(old) else '',status='KNOWN' if len(old)>=n else 'UNKNOWN_HISTORY')
 if len(old)<n:return base,[]
 zs,_=zones(old,n);out=[]
 for k,z in enumerate(zs):
  strong=z['count']>=3 and z['median_drop_atr']>=1
  pos='ABOVE' if z['low']>close else 'BELOW' if z['high']<close else 'INSIDE'
  out.append(dict(zone_id=k,low=z['low'],high=z['high'],count=z['count'],drop_atr=z['median_drop_atr'],strong=strong,position=pos,rising_troughs=z['rising_troughs'],median_reapproach=z['median_reapproach'],test_dates='|'.join(t['date'] for t in z['tests']),newly_cleared=bool(old.close.iloc[-1]<=z['high']<close and max(0,min(close,z['high'])-max(float(b.open),z['low']))>0)))
 active=[z for z in out if z['position']!='BELOW'];strong=[z for z in active if z['strong']];near=min([100*max(0,z['low']/close-1) for z in strong],default=np.nan)
 pref=strong or active;selected=min(pref,key=lambda z:max(0,z['low']-close)) if pref else None
 cleared=[z for z in out if z['newly_cleared']];bodystrong=[z for z in cleared if z['strong']];bodypref=bodystrong or cleared;body=min(bodypref,key=lambda z:close-z['high']) if bodypref else None
 base.update(strong_active=bool(strong),repeat_active=bool(active),strong_below=any(z['strong'] and z['position']=='BELOW' for z in out),strong_body=bool(bodystrong),nearest_strong_pct=near,resistance_bucket='NONE' if not strong else '0-3' if near<=3 else '3-10' if near<=10 else '10+',selected_low=selected['low'] if selected else np.nan,selected_high=selected['high'] if selected else np.nan,body_low=body['low'] if body else np.nan,body_high=body['high'] if body else np.nan)
 for mode in ['range','typical']:
  p=volume_profile(old.iloc[-n:],32,mode)
  if p is None:base['volume_above10_'+mode]=np.nan;continue
  weights=p['weights'];centers=p['centers'];base['volume_above10_'+mode]=100*weights[(centers>=close)&(centers<=close*1.1)].sum()/weights.sum() if weights.sum()>0 else np.nan
 return base,out

def main():
 frames=load_prices(Path(os.environ.get('BLUE20_MANIFEST',str(Path(os.environ.get('TEMP','/tmp'))/'stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv'))));cal=pd.Index(sorted({v for d in frames.values() for v in d.date}));cp={v:i for i,v in enumerate(cal)}
 source=pd.read_csv(R/'BLUE20_ENVELOPE_COMPARE/results/EVENTS.csv.gz',dtype={'code':str});e=source[source.model.isin(['BB20','BB40'])&source.entry_rule.eq('SIGNAL_NEXT_OPEN')].copy();ids=set(zip(e.model,e.code,e.date,e.band))
 if 'BLUE20_DIAG_SHARD' in os.environ:
  assigned={c:i%4 for i,c in enumerate(sorted(frames))};e=e[e.code.map(assigned).eq(int(os.environ['BLUE20_DIAG_SHARD']))]
 ds={c:indicators(d) for c,d in frames.items()};cache={};snaps=[];zz=[];trades=[];anchors=[];start=time.time()
 def snap(c,j,n):
  key=(c,ds[c].date.iloc[j],n)
  if key not in cache:
   a,z=snapshot(ds[c],j,n);sid=f'{c}:{key[1]}:{n}';a.update(snapshot_id=sid,code=c,date=key[1]);snaps.append(a)
   for row in z:zz.append(dict(snapshot_id=sid,code=c,date=key[1],period=n,**row))
   cache[key]=a
  return cache[key]
 for number,r in enumerate(e.itertuples(),1):
  d=ds[r.code];pos={v:i for i,v in enumerate(d.date)};j=pos[r.date];k=cp[r.date];oid=f'{r.model}:{r.code}:{r.date}:{r.band}';other='BB40' if r.model=='BB20' else 'BB20';group='COMMON' if (other,r.code,r.date,r.band) in ids else r.model+'_ONLY'
  fair=k+100<len(cal) and all(v in pos for v in cal[k:k+101]);ctx=snap(r.code,j,120)
  for n in [60,240]:snap(r.code,j,n)
  base=dict(opportunity_id=oid,model=r.model,code=r.code,date=r.date,year=r.year,band=r.band,signal_group=group,nonoverlap60=r.nonoverlap60,fair100=fair,initial_snapshot=ctx['snapshot_id'])
  anchors.append(base)
  selected=None if pd.isna(ctx.get('selected_low',np.nan)) else dict(low=ctx['selected_low'],high=ctx['selected_high'])
  body=None if pd.isna(ctx.get('body_low',np.nan)) else dict(low=ctx['body_low'],high=ctx['body_high'])
  events,reasons,_=detect(d,j,cal,selected)
  if body is not None:
   bodyevents,bodyreasons,_=detect(d,j,cal,body)
   if 'C_RETEST' in bodyevents:events['C_BROKEN_BODY']=bodyevents['C_RETEST']
   else:reasons['C_BROKEN_BODY']=bodyreasons.get('C_RETEST','NO_CONFIRMATION')
  else:reasons['C_BROKEN_BODY']='NO_NEWLY_CLEARED_ZONE'
  for policy in ['A_FIRST','B_RECOVERY','C_RETEST','C_BROKEN_BODY']:
   event=dict(confirm_date=r.date,confirm_index=j) if policy=='A_FIRST' else events.get(policy)
   if event is None:trades.append(base|dict(policy=policy,status='NO_ENTRY_SIGNAL',no_entry_reason=reasons.get(policy,'NO_CONFIRMATION')));continue
   day=event['confirm_date'];cj=pos[day];confirm=snap(r.code,cj,120);ent=cp[day]+1
   result=ns.sim(d,cal,cal[ent]) if ent<len(cal) else dict(status='NO_NEXT_SESSION')
   trades.append(base|dict(policy=policy,confirm_date=day,confirm_snapshot=confirm['snapshot_id'],pullback_low=event.get('stop_low',np.nan),low_date=event.get('low_date',''))|result)
  if number%250==0:print(json.dumps(dict(completed=number,total=len(e),seconds=round(time.time()-start,1))),flush=True)
 a=pd.DataFrame(anchors);t=pd.DataFrame(trades);s=pd.DataFrame(snaps);z=pd.DataFrame(zz)
 a.to_csv(O/'OPPORTUNITIES.csv.gz',index=False);t.to_csv(O/'TRADES.csv.gz',index=False);s.to_csv(O/'SNAPSHOTS.csv.gz',index=False);z.to_csv(O/'ZONES.csv.gz',index=False)
 # 初기 정보는 이전에 관측한 자료만 사용한다.
 assert (s.history_known_through<s.date).all()
 assert not (z.strong&z.position.eq('BELOW')&z.low.isna()).any()
 orig=source[source.model.isin(['BB20','BB40'])&source.entry_rule.eq('SIGNAL_NEXT_OPEN')].set_index(['model','code','date','band'])
 for q in t[t.policy.eq('A_FIRST')].itertuples():
  v=orig.loc[(q.model,q.code,q.date,q.band)];assert q.status==v.status
  if q.status in ['TP10','NO_TP60']:assert np.isclose(q.net_pct,v.net_pct) and q.exit_date==v.exit_date
 (O/'STATUS.json').write_text(json.dumps(dict(local='COMPLETE',initial_signal_unchanged=True,no_stop=True,holding_sessions=60,causal_snapshot_cutoff='PASS',first_entry_replay='PASS',opportunities=len(a),trade_rows=len(t),snapshots=len(s),github_run='NOT_EXECUTED',real_volume='UNVERIFIED',corporate_actions='UNVERIFIED'),indent=2),encoding='utf-8')
 print('local calculation complete',len(a),len(t),flush=True)
if __name__=='__main__':main()
