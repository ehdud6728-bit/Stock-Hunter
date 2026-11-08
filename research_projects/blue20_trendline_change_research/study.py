from pathlib import Path
import sys,os,json,hashlib,time
import numpy as np
import pandas as pd
from engine import prepare,snapshot,wait_events,simulate
H=Path(__file__).resolve().parent;R=H.parent;O=H/'results';O.mkdir(exist_ok=True);sys.path.insert(0,str(H/'vendor'))
from loader import load_prices
POLICIES=['A_BASE','B_BREAK_NOW','C_WAIT_BREAK','D_RETEST','E_SUPPORT_EXIT']

def main():
 for name,sha in json.loads((H/'reference/SHA256.json').read_text()).items():assert hashlib.sha256((H/'reference'/name).read_bytes()).hexdigest()==sha,name
 manifest=Path(os.environ.get('BLUE20_MANIFEST',str(Path(os.environ.get('TEMP','/tmp'))/'stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv')));frames=load_prices(manifest);frozen=json.loads((H/'reference/PRICE_SOURCE_HASHES.json').read_text());assert set(frames)==set(frozen)
 for c,d in frames.items():assert hashlib.sha256(d.to_csv(index=False,lineterminator='\n').encode('utf-8-sig')).hexdigest()==frozen[c]
 cal=pd.Index(sorted({v for d in frames.values() for v in d.date}));cp={v:i for i,v in enumerate(cal)};ds={};pivs={};positions={}
 for c,d in frames.items():ds[c],pivs[c]=prepare(d,cal);positions[c]={v:i for i,v in enumerate(d.date)}
 source=pd.read_csv(H/'reference/BASE8_SIGNALS.csv.gz',dtype={'code':str});source=source[source.band.eq('UPPER')].copy();old=pd.read_csv(H/'reference/BASE8_TRADES.csv.gz',dtype={'code':str},low_memory=False);old=old[old.band.eq('UPPER')].set_index(['opportunity_id','target_pct']);cache={};contexts=[];lines=[];rows=[];replayed=0;start=time.time()
 for counter,r in enumerate(source.itertuples(),1):
  code=r.code;day=r.date;d=ds[code];pos=positions[code];j=pos[day];p=cp[day];key=(code,day)
  if key not in cache:
   all_snap={};all_lines={}
   for n in [60,120,240]:
    ctx,ll=snapshot(d,pivs[code],j,n);sid=f'{code}:{day}:{n}';ctx.update(snapshot_id=sid,code=code,date=day);contexts.append(ctx);all_snap[n]=ctx;all_lines[n]=ll
    for line in ll.values():lines.append(dict(snapshot_id=sid,code=code,date=day,lookback=n,**line))
   ctx=all_snap[120];ll=all_lines[120];events={'A_BASE':day,'E_SUPPORT_EXIT':day};reasons={};support=ll.get('SUPPORT') if ctx.get('SUPPORT_state')=='ABOVE_OR_NEAR' else None
   if ctx['status']!='KNOWN':
    for pol in ['B_BREAK_NOW','C_WAIT_BREAK','D_RETEST']:reasons[pol]='UNKNOWN_CONTEXT'
   else:
    if ctx.get('DESC_state')=='BREAK_NOW':events['B_BREAK_NOW']=day
    else:reasons['B_BREAK_NOW']='NO_NEW_BREAK'
    if 'DESC' in ll:
     waits,reason=wait_events(d,pos,cal,day,ll['DESC']);events.update(waits)
     for pol in ['C_WAIT_BREAK','D_RETEST']:
      if pol not in events:reasons[pol]=reason
    else:
     for pol in ['C_WAIT_BREAK','D_RETEST']:reasons[pol]='NO_DESCENDING_LINE'
   results={}
   for policy in POLICIES:
    for target in [5,10,20,40]:
     if policy not in events:
      reason=reasons.get(policy,'NO_CONFIRMATION');rr=dict(status='UNKNOWN' if reason in ['UNKNOWN_CONTEXT','MISSING_WAIT_BAR','RIGHT_CENSORED_WAIT'] else 'NO_ENTRY',entered=False,closed=False,no_entry_reason=reason)
     else:rr=simulate(d,pos,cal,events[policy],target,support if policy=='E_SUPPORT_EXIT' else None)
     results[policy,target]=rr
   fair=p+80<len(cal) and all(v in pos for v in cal[p:p+81]);cache[key]=(ctx,ll,results,fair,support is not None)
  ctx,ll,results,fair,support_enabled=cache[key];base={k:getattr(r,k) for k in ['opportunity_id','model','period','std_multiplier','line_shift','configuration','code','date','year','band','listing_market','name_at_signal','nonoverlap60','env_prior40','depth_group','rally_type','known_source_issue']};base.update(fair81=fair,context_status=ctx['status'],DESC_state=ctx.get('DESC_state','UNKNOWN'),SUPPORT_state=ctx.get('SUPPORT_state','UNKNOWN'),support_rule_enabled=support_enabled,initial_snapshot=f'{code}:{day}:120')
  for (policy,target),rr in results.items():
   rows.append(base|dict(policy=policy,target_pct=target)|rr)
   if policy=='A_BASE':
    prev=old.loc[(r.opportunity_id,target)];expected='TP' if prev.status=='TP' else 'TIME' if prev.status=='NO_TP' else prev.status
    assert rr['status']==expected,(r.opportunity_id,target,rr['status'],expected)
    if expected in ['TP','TIME']:assert np.isclose(rr['net_pct'],prev.net_pct) and rr['exit_date']==prev.exit_date
    replayed+=1
  if counter%500==0:print(json.dumps(dict(done=counter,total=len(source),seconds=round(time.time()-start,1))),flush=True)
 t=pd.DataFrame(rows);pd.DataFrame(contexts).to_csv(O/'SNAPSHOTS.csv.gz',index=False);pd.DataFrame(lines).to_csv(O/'FROZEN_LINES.csv.gz',index=False);t.to_csv(O/'TRADES.csv.gz',index=False)
 for name,cols in {'SUMMARY':['model','policy','target_pct'],'LISTING_MARKET':['listing_market','model','policy','target_pct'],'INITIAL_STATE':['model','policy','target_pct','DESC_state','SUPPORT_state'],'SUPPORT_ENABLED':['model','policy','target_pct','support_rule_enabled'],'PULLBACK':['model','policy','target_pct','rally_type']}.items():
  result=[]
  for scope,mask in [('ALL',pd.Series(True,index=t.index)),('FAIR81',t.fair81),('FAIR81_NONOVERLAP60',t.fair81&t.nonoverlap60),('CLEAN_FAIR81_NONOVERLAP60',t.fair81&t.nonoverlap60&~t.known_source_issue)]:
   for year,g in [('ALL_YEARS',t[mask]),('2023',t[mask&t.year.eq(2023)]),('2024',t[mask&t.year.eq(2024)])]:
    for keys,q in g.groupby(cols,dropna=False,observed=True):
     z=q[q.closed.eq(True)&q.full60.eq(True)];nets=q.net_pct.copy();nets.loc[q.status.eq('NO_ENTRY')]=0;win=z[z.net_pct>0].net_pct;loss=z[z.net_pct<0].net_pct;result.append(dict(zip(cols,keys))|dict(scope=scope,year=year,opportunities=len(q),entries=int(q.entered.sum()),entry_pct=100*q.entered.mean(),n_closed=len(z),tp_pct=100*z.status.eq('TP').mean(),win_pct=100*z.net_pct.gt(0).mean(),mean_net=z.net_pct.mean(),opportunity_net=nets.mean() if scope!='ALL' else np.nan,payoff=win.mean()/abs(loss.mean()) if len(loss) else np.nan,mean_hold=z.holding_sessions.mean(),unknown=int(q.status.isin(['UNKNOWN','MISSING_BAR','RIGHT_CENSORED','NO_ENTRY_SESSION']).sum())))
  pd.DataFrame(result).to_csv(O/(name+'.csv'),index=False)
 pd.DataFrame(contexts).to_csv(O/'SNAPSHOTS.csv.gz',index=False)
 for row in lines:assert row['known_date']<row['date']
 (O/'STATUS.json').write_text(json.dumps(dict(status='COMPLETE',source_run=42495786893,models=8,upper_signals=len(source),baseline_rows_replayed=replayed,initial_lines_known_before_D0=True,source_prices=500,execution_context='GITHUB_ACTIONS' if os.environ.get('GITHUB_ACTIONS')=='true' else 'LOCAL',github_run_id=os.environ.get('GITHUB_RUN_ID'),new_github_run=os.environ.get('GITHUB_RUN_ID','NOT_EXECUTED'),corporate_actions='UNVERIFIED'),indent=2),encoding='utf-8');print('trend study complete',len(t),flush=True)
if __name__=='__main__':main()
