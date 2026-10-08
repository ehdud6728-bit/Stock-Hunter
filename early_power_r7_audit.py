#!/usr/bin/env python3
import argparse,json
from pathlib import Path
import numpy as np,pandas as pd
REV='EARLY_POWER_R7_RESEARCH_AUDIT_20261008'

def classify(g):
 d10=pd.to_numeric(g['d10_close_ret_pct'],errors='coerce');p10=pd.to_numeric(g['first_plus10_day_h10'],errors='coerce');p5=pd.to_numeric(g['first_plus5_day_h10'],errors='coerce');m5=pd.to_numeric(g['first_minus5_day_h10'],errors='coerce')
 o=pd.Series('OTHER',index=g.index);o[(p10.notna())&(m5.isna()|(p10<m5))]='CLEAN_PLUS10';o[(m5.notna())&(p5.isna()|(m5<p5))&d10.lt(0)]='STOP_FIRST_NEG';o[d10.isna()]='PENDING_D10';return o

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--frozen',required=True);ap.add_argument('--fresh',required=True);ap.add_argument('--r5',required=True);ap.add_argument('--r6cand',required=True);ap.add_argument('--r6coh',required=True);ap.add_argument('--price',required=True);ap.add_argument('--out',required=True);a=ap.parse_args();out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
 old=pd.read_csv(a.frozen,dtype={'code':str});new=pd.read_csv(a.fresh,dtype={'code':str});
 for z in [old,new]:z['signal_date']=pd.to_datetime(z.signal_date).dt.strftime('%Y-%m-%d');z['code']=z.code.astype(str).str.zfill(6)
 cutoff=old.signal_date.max();oid=set(zip(old.signal_date,old.code,old.stage));pre=new[new.signal_date<=cutoff];nid=set(zip(pre.signal_date,pre.code,pre.stage));ext=new[new.signal_date>cutoff].copy();ext.to_csv(out/'new_signals_after_cutoff.csv',index=False,encoding='utf-8-sig')
 rows=[]
 for st,g in new.groupby('stage'):
  r={'stage':st,'n':len(g),'signal_days':g.signal_date.nunique()}
  for h in [1,3,5,10,20]:
   s=pd.to_numeric(g[f'd{h}_close_ret_pct'],errors='coerce');r[f'd{h}_n']=int(s.notna().sum());r[f'd{h}_median']=s.median();r[f'd{h}_mean']=s.mean()
  rows.append(r)
 st=pd.DataFrame(rows);st.to_csv(out/'stage_summary.csv',index=False,encoding='utf-8-sig')
 s23=new[new.stage.isin(['S2_CROSS_5_20','S3_CONVERGENCE'])].copy();s23['mature_group']=classify(s23);m=s23[s23.mature_group!='PENDING_D10'];pend=s23[s23.mature_group=='PENDING_D10'];
 rs=[]
 for (stg,grp),g in m.groupby(['stage','mature_group']):rs.append({'stage':stg,'group':grp,'n':len(g),'d10_median':pd.to_numeric(g.d10_close_ret_pct,errors='coerce').median(),'mfe10_median':pd.to_numeric(g.mfe10_pct,errors='coerce').median(),'mae10_median':pd.to_numeric(g.mae10_pct,errors='coerce').median()})
 pd.DataFrame(rs).to_csv(out/'r2_mature_only.csv',index=False,encoding='utf-8-sig');pend.to_csv(out/'r2_pending_d10.csv',index=False,encoding='utf-8-sig')
 r5=pd.read_csv(a.r5,low_memory=False)
 for c in ['bb40_width','conv_change_5d','volume_last5_vs_prev15','low_to_high_sessions','d10_close_ret_pct','mfe10_pct','mae10_pct']:r5[c]=pd.to_numeric(r5[c],errors='coerce')
 zone=r5.bb40_width.le(10)&r5.conv_change_5d.between(-.4,.8)&r5.volume_last5_vs_prev15.between(1.27,1.8)&r5.low_to_high_sessions.between(11,29);r5['r6_zone']=zone
 zr=[]
 for flag,g in r5.groupby('r6_zone'):
  q=g[g.d10_close_ret_pct.notna()];zr.append({'r6_zone':bool(flag),'all_n':len(g),'d10_mature_n':len(q),'d10_median':q.d10_close_ret_pct.median(),'d10_mean':q.d10_close_ret_pct.mean(),'positive_pct':(q.d10_close_ret_pct.gt(0).mean()*100 if len(q) else np.nan),'mfe10_median':q.mfe10_pct.median(),'mae10_median':q.mae10_pct.median()})
 z=pd.DataFrame(zr);z.to_csv(out/'r6_zone_retrospective.csv',index=False,encoding='utf-8-sig')
 cand=pd.read_csv(a.r6cand,dtype={'code':str});coh=pd.read_csv(a.r6coh);px=pd.read_parquet(a.price,columns=['Date']);td={pd.Timestamp(x).date().isoformat() for x in px.Date.unique()};last=max(td);invalid=[]
 for i,r in coh.iterrows():
  sd=str(r.signal_date)[:10]
  if sd<=last and sd not in td:coh.loc[i,'early_power_status']='INVALID_NON_TRADING_DATE';invalid.append(sd)
 cand=cand[~cand.signal_date.astype(str).str[:10].isin(invalid)].copy();cand.to_csv(out/'r6_candidates_calendar_clean.csv',index=False,encoding='utf-8-sig');coh.to_csv(out/'r6_cohorts_calendar_clean.csv',index=False,encoding='utf-8-sig')
 meta={'revision':REV,'research_only':True,'production_changed':False,'same_sample_retuning':False,'cutoff':cutoff,'fresh_last_signal':new.signal_date.max(),'old_rows':len(old),'fresh_rows':len(new),'new_rows':len(ext),'historical_missing':len(oid-nid),'historical_extra':len(nid-oid),'s23_d10_mature':len(m),'s23_pending_d10':len(pend),'invalid_nontrading_dates':sorted(set(invalid))}
 (out/'meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8');(out/'REPORT.txt').write_text('# R7 refresh\n\n'+json.dumps(meta,ensure_ascii=False,indent=2)+'\n\n## Stage\n'+st.to_string(index=False)+'\n\n## R6 frozen-zone retrospective\n'+z.to_string(index=False)+'\n\n## R6 cohorts\n'+coh.to_string(index=False),encoding='utf-8');print('R7_AUDIT_PASS',json.dumps(meta,ensure_ascii=False));print(st.to_string(index=False));print(z.to_string(index=False))
if __name__=='__main__':main()
