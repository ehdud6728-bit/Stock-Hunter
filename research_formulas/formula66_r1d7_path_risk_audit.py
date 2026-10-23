#!/usr/bin/env python3
"""R1-D7 / RESEARCH_ONLY / locked R1-D6 population / retrospective path-risk audit.

No new search, no refitting, no independent OOS; R1-D6's same 2024-2025 cohort.
R1-D6 signals and as-of feature rows are imported byte-for-byte from two pinned
GitHub artifacts. The original KRX unadjusted daily raw-price feed is source
verified and only used *after* the frozen event set has been read and sealed.
Forward D+5,10,15,20 are hypothetical next-session open to horizon close with
20bp fixed friction, not executable signals or validated trading rules.
"""
from __future__ import annotations
import argparse, hashlib, io, json, math, zipfile, gzip
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd
from research_formulas import formula66_r1d6_oos_holdout as d6

VERSION='STOCKHUNTER_R1D7_FROZEN_R1D6_PATH_RISK_V1'
RESEARCH_ONLY=True
D6_RUN=39941707036
D6_COMMIT='019956f9aa85e419c4e8e4ca6fcc6f5dc21a9686'
D6_ASOF_ID=12496827313
D6_ASOF_ZIP_DIGEST='7185b582e94131b7ee09daf55024c80eeac881f5a2cbc8cee7bbd418a106a884'
D6_SEALED_ID=12497006782
D6_SEALED_ZIP_DIGEST='f01d83ab64650b8a39459c17381d320e89d8135f5e488180aed28bbe91c00030'
D6_CODE_BLOB='00c78a26117670768b70772f578edcb5995e48a5'
ORIGINAL_ASOF_SHA256='5062180dbb9d1299773d61074c687e8f7bf7f7ec2fe06b8622b9ed9cc4de1d29'
HORIZONS=(5,10,15,20)
H20_OK='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
FLAGS=('MA20_hold','BB20_L_hold','BB40_L_hold','ENV20_L_10_hold',
       'ENV40_L_10_hold','FIB50_reclaim','FIB618_reclaim','WAVE_LOW_fail',
       'signal_close_above_prev_high','amount3d_above_prior10')


def fail(message):
 raise RuntimeError('R1D7_FAIL_CLOSED_'+message)


def git_blob(raw):
 return hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\x00'+raw).hexdigest()


def sha(b):return hashlib.sha256(b).hexdigest()


def read_result(root,filename):
 p=Path(root)/filename
 if not p.is_file():fail('MISSING_ARTIFACT_FILE_'+filename)
 return p.read_bytes()


def verify_frozen(a_dir,s_dir,repo_root='.'):
 d6path=Path(repo_root)/'research_formulas/formula66_r1d6_oos_holdout.py'
 if not d6path.is_file() or git_blob(d6path.read_bytes())!=D6_CODE_BLOB:
  fail('R1D6_SOURCE_CODE_DRIFT')
 a=read_result(a_dir,'R1D6_ASOF_ONLY_FROZEN_FEATURES_NO_OUTCOME.csv.gz')
 if sha(a)!=ORIGINAL_ASOF_SHA256:fail('ASOF_SHA256_DRIFT')
 m=json.loads(read_result(s_dir,'result/R1D6_MANIFEST.json'))
 if (m.get('asof_features_sha256')!=ORIGINAL_ASOF_SHA256 or m.get('cooldown_20session_episodes')!=5247
     or m.get('outcome_D20_complete')!=5214 or m.get('oos_strong15_count')!=541
     or m.get('oos_failure5_count')!=1902 or m.get('orders_placed')!=0
     or m.get('window')!=['2024-01-02','2025-09-30']):fail('D6_MANIFEST_DRIFT')
 asof=pd.read_csv(io.BytesIO(gzip.decompress(a)),dtype={'code':str,'date':str,'signal_date':str},low_memory=False)
 frozen=pd.read_csv(io.BytesIO(gzip.decompress(read_result(s_dir,'result/R1D6_SEALED_ONLY_FORWARD_PRICE_PATHS.csv.gz'))),dtype={'code':str,'date':str},low_memory=False)
 if len(asof)!=5247 or len(frozen)!=5247:fail('D6_COUNTS_DRIFT')
 if asof[['date','code']].duplicated().any() or frozen[['date','code']].duplicated().any():fail('DUPLICATE_R1D6_KEY')
 if not asof.date.eq(asof.reference_data_last_bar_date).all() or not asof.status_feature.eq('ASOF_OK').all():
  fail('ASOF_DAY_OR_STATUS_DRIFT')
 if any(k in asof for k in ('gross_pct','net_20bps_pct','entry_open','exit_close','status_out')):fail('ASOF_CONTAINS_OUTCOME')
 if set(zip(asof.date,asof.code))!=set(zip(frozen.date,frozen.code)):fail('D6_FEATURE_OUTCOME_KEYS_MISMATCH')
 for f in FLAGS:
  if f not in asof.columns:fail('MISSING_FROZEN_FLAG_'+f)
 if sum(frozen.status_out.eq(H20_OK))!=5214:fail('D6_D20_COMPLETION_DRIFT')
 return asof,frozen,m


def build_horizon(signal_date,code,calendar,ix,rows,h,signal_close):
 """Use exactly the D6 entry/gap/suspension semantics for comparability."""
 ei=ix+1;xi=ix+h
 out={'date':signal_date,'code':code,'horizon':h,'signal_ix':ix,
      'entry_day':calendar[ei] if ei<len(calendar) else '',
      'exit_day':calendar[xi] if xi<len(calendar) else '',
      'entry_rule':'NEXT_SESSION_OPEN_HYPOTHETICAL_NOT_FILL_CERTIFIED',
      'status_out':'','net_20bps_pct':np.nan,'gross_pct':np.nan,
      'mae_intraday_pct':np.nan,'mfe_intraday_pct':np.nan,
      'mfe_first_day_offset':np.nan,'mae_first_day_offset':np.nan,
      'entry_open':np.nan,'exit_close':np.nan,'both_mfe15_mae_minus5':0,
      'same_day_extreme_order_unknown':0,
      'first_touch_up15_day_offset':np.nan,'first_touch_down5_day_offset':np.nan,
      'price_barrier_order':'BLOCKED'}
 if xi>=len(calendar):out['status_out']='RIGHT_CENSORED_SOURCE_END_NO_FUTURE_PRICE';return out
 data=[rows.get(day) for day in calendar[ei:xi+1]]
 if len(data)!=h or any(x is None for x in data):
  out['status_out']='INTERMEDIATE_SUSPENSION_OR_MISSING_PATH';return out
 vals=np.array(data,dtype=float)
 if vals.shape!=(h,6) or not np.isfinite(vals).all() or (vals<=0).any():
  out['status_out']='MISSING_OR_SUSPENDED_ENTRY_OR_EXIT';return out
 priorclose=float(signal_close)
 for price in vals:
  if abs(price[0]/priorclose-1)>.40:
   out['status_out']='UNADJUSTED_FUTURE_CORPORATE_ACTION_OR_EXTREME_GAP_BLOCKED';return out
  priorclose=float(price[3])
 open0=float(vals[0,0]);gross=100*(float(vals[-1,3])/open0-1.)
 fwd_minlow=100*(float(vals[:,2].min())/open0-1.)
 fwd_maxhigh=100*(float(vals[:,1].max())/open0-1.)
 min_i=int(vals[:,2].argmin())+1;max_i=int(vals[:,1].argmax())+1
 up_days=np.flatnonzero(100*(vals[:,1]/open0-1)>=15)
 down_days=np.flatnonzero(100*(vals[:,2]/open0-1)<=-5)
 firstup=int(up_days[0])+1 if len(up_days) else None
 firstdown=int(down_days[0])+1 if len(down_days) else None
 order=('UP_FIRST' if firstup is not None and (firstdown is None or firstup<firstdown) else
        'DOWN_FIRST' if firstdown is not None and (firstup is None or firstdown<firstup) else
        'SAME_DAY_ORDER_UNKNOWN' if firstup is not None and firstup==firstdown else 'NEITHER')
 out.update(status_out=H20_OK,entry_open=open0,exit_close=float(vals[-1,3]),
            gross_pct=gross,net_20bps_pct=gross-.20,
            mae_intraday_pct=fwd_minlow,mfe_intraday_pct=fwd_maxhigh,
            mae_first_day_offset=min_i,mfe_first_day_offset=max_i,
            both_mfe15_mae_minus5=int(fwd_maxhigh>=15 and fwd_minlow<=-5),
            same_day_extreme_order_unknown=int(min_i==max_i),
            first_touch_up15_day_offset=firstup if firstup is not None else np.nan,
            first_touch_down5_day_offset=firstdown if firstdown is not None else np.nan,
            price_barrier_order=order)
 return out


def compare_d20(path,frozen):
 a=path[path.horizon==20].copy()
 if len(a)!=len(frozen):fail('D20_CARDINALITY')
 f=frozen.copy();cols=['date','code','status_out','net_20bps_pct','gross_pct','entry_open','exit_close']
 match=a[cols].merge(f[cols],on=['date','code'],validate='one_to_one',suffixes=('_r7','_r6'))
 for col in ('status_out',):
  if not match[col+'_r7'].equals(match[col+'_r6']):fail('D20_STATUS_NOT_IDENTICAL')
 for col in ('net_20bps_pct','gross_pct','entry_open','exit_close'):
  x=match[col+'_r7'].astype(float).to_numpy();y=match[col+'_r6'].astype(float).to_numpy()
  if not np.allclose(x,y,atol=1e-10,rtol=0,equal_nan=True):fail('D20_PRICE_NUMBER_DRIFT_'+col)
 z=path[path.horizon==20].set_index(['date','code'])
 f=f.set_index(['date','code'])
 for old,new in [('max_intraday_adverse_pct','mae_intraday_pct'),('max_intraday_favorable_pct','mfe_intraday_pct')]:
  x=f[old].reindex(z.index).to_numpy(dtype=float);y=z[new].to_numpy(dtype=float)
  if not np.allclose(x,y,atol=1e-10,rtol=0,equal_nan=True):fail('D20_MAE_MFE_DRIFT_'+old)
 return {'rows_compared':len(match),'complete':int((a.status_out==H20_OK).sum()),'passed':True}


def _stats(x,label,segment,value):
 n=len(x);good=x[x.status_out==H20_OK];m=len(good)
 def mean(col):return float(good[col].mean()) if m else np.nan
 def median(col):return float(good[col].median()) if m else np.nan
 return {'label':label,'segment':segment,'value':str(value),'n_total':n,'n_complete':m,'n_blocked':n-m,
         'unique_dates':int(good.date.nunique()),'unique_codes':int(good.code.nunique()),
         'mean_net20bps_pct':mean('net_20bps_pct'),'median_net20bps_pct':median('net_20bps_pct'),
         'positive_pct':100*float(good.net_20bps_pct.gt(0).mean()) if m else np.nan,
         'strong15_pct':100*float(good.net_20bps_pct.ge(15).mean()) if m else np.nan,
         'failure5_pct':100*float(good.net_20bps_pct.le(-5).mean()) if m else np.nan,
         'median_mae_intraday_pct':median('mae_intraday_pct'),
         'median_mfe_intraday_pct':median('mfe_intraday_pct'),
         'mae_below_minus10_pct':100*float(good.mae_intraday_pct.le(-10).mean()) if m else np.nan,
         'mfe_above15_pct':100*float(good.mfe_intraday_pct.ge(15).mean()) if m else np.nan,
         'both_mfe15_mae5_pct':100*float(good.both_mfe15_mae_minus5.mean()) if m else np.nan}


def summarize(j):
 j=j.copy();j['signal_year']=j.date.str[:4]
 j['regime']=np.where(j.joint_index_20d_regime.str.contains('BEAR20',na=False),'BEAR_ANY20',
                 np.where(j.joint_index_20d_regime.str.contains('BULL20',na=False),'BULL_BOTH20','MIXED_NONEXTREME20'))
 j['first_code_only']=(j.groupby('code').date.transform('min')==j.date).astype(int)
 all_rows=[]
 for h,g in j.groupby('horizon',sort=True):
  all_rows.append(_stats(g,'ALL','HORIZON',h))
  g_common=g[g.d20_complete==1]
  all_rows.append(_stats(g_common,'CONSTANT_D20_ELIGIBLE_COHORT','HORIZON',h))
  for (y,reg),q in g.groupby(['signal_year','regime']):all_rows.append(_stats(q,'YEAR_REGIME','YEAR_REGIME',y+'_'+reg))
  for y,q in g.groupby('signal_year'):all_rows.append(_stats(q,'YEAR','YEAR',y))
  for reg,q in g.groupby('regime'):all_rows.append(_stats(q,'REGIME','REGIME',reg))
  all_rows.append(_stats(g[g.first_code_only==1],'FIRST_SIGNAL_PER_CODE','FIRST_CODE','FIRST_CODE'))
 return j,pd.DataFrame(all_rows)


def support_and_amount(j):
 # Keep 10 frozen boolean flags, no threshold tuning. Analysis panel retains each flag 0 and 1.
 rows=[]
 for h,g in j.groupby('horizon'):
  for flag in FLAGS:
   for v in (0,1):
    q=g[g[flag]==v]
    rows.append(_stats(q,'FROZEN_FLAG_'+flag,'VALUE',str(h)+'__'+str(v)))
   for a in (0,1):
    for b in (0,1):
     q=g[(g[flag]==a)&(g.amount3d_above_prior10==b)]
     item=_stats(q,'FROZEN_SUPPORT_X_AMOUNT_'+flag,'HORIZON_FLAG_AMOUNT',f'{h}__{a}__{b}')
     item['low_n_warn']=int(item['n_complete']<30)
     rows.append(item)
 return pd.DataFrame(rows)


def same_date_contrast(j):
 """Within-date flag differences; equal day weighting only, explicitly descriptive."""
 done=j[(j.status_out==H20_OK)&(j.horizon==20)].copy()
 out=[]
 for flag in FLAGS:
  paired=[]
  for day,g in done.groupby('date'):
   a=g[g[flag]==1];b=g[g[flag]==0]
   if len(a)==0 or len(b)==0:continue
   paired.append((100*(a.net_20bps_pct.le(-5).mean()-b.net_20bps_pct.le(-5).mean()),
                  100*(a.net_20bps_pct.ge(15).mean()-b.net_20bps_pct.ge(15).mean()),
                  float(a.mae_intraday_pct.median()-b.mae_intraday_pct.median())))
  arr=np.asarray(paired,float).reshape(-1,3)
  out.append({'factor':flag,'n_comparable_dates':len(arr),
              'equal_date_fail5_diff_pp':float(arr[:,0].mean()) if len(arr) else np.nan,
              'equal_date_strong15_diff_pp':float(arr[:,1].mean()) if len(arr) else np.nan,
              'equal_date_median_mae_diff_pp':float(arr[:,2].mean()) if len(arr) else np.nan,
              'causal_inference':False})
 return pd.DataFrame(out)


def run(args):
 a,f,m=verify_frozen(args.asof,args.sealed,args.repo_root)
 a=a.sort_values(['date','code'],kind='stable').reset_index(drop=True)
 f=f.sort_values(['date','code'],kind='stable').reset_index(drop=True)
 source=d6.source_manifest(args.krx_root)
 calendar,market=d6.index_asof(args.krx_root)
 if set(source)!=set(calendar):fail('SOURCE_INDEX_DATES_MISMATCH')
 count=d6.verify_all_daily_raw_sha(source)
 if count!=1907:fail('FULL_SOURCE_SHA_NOT_1907')
 dindex={day:i for i,day in enumerate(calendar)}
 events=[];wanted=defaultdict(set);signal_close={}
 for x in a.itertuples(index=False):
  si=dindex.get(x.date)
  if si is None:fail('SIGNAL_DATE_NOT_IN_INDEX')
  if si+20>=len(calendar):fail('HORIZON_OUT_OF_RANGE')
  for day in calendar[si:si+21]:wanted[day].add(x.code)
  events.append((x.date,x.code,si))
 bars_by_code=defaultdict(dict)
 for ti,day in enumerate(sorted(wanted)):
  p,expected=source[day]
  if sha(p.read_bytes())!=expected:fail('REPEATED_DAILY_SHA_DRIFT_'+day)
  d=d6.priceday(p,day)
  d=d[d.code.isin(wanted[day])]
  for rr in d.itertuples(index=False):
   bars_by_code[str(rr.code)][day]=(float(rr.open),float(rr.high),float(rr.low),float(rr.close),float(rr.volume),float(rr.amount))
  if ti%100==0:print('R1D7_PATH_PROGRESS',day,ti,len(wanted),flush=True)
 outcomes=[]
 for date,code,si in events:
  this=bars_by_code[code].get(date)
  if this is None:fail('FROZEN_SIGNAL_RAW_PRICE_MISSING')
  for h in HORIZONS:
   outcomes.append(build_horizon(date,code,calendar,si,bars_by_code[code],h,this[3]))
 p=pd.DataFrame(outcomes).sort_values(['date','code','horizon']).reset_index(drop=True)
 matched=compare_d20(p,f)
 # strict one-to-many join, no feature changes, original feature/price separation preserved
 j=p.merge(a,on=['date','code'],how='left',validate='many_to_one')
 d20keys=set(zip(f.loc[f.status_out==H20_OK,'date'],f.loc[f.status_out==H20_OK,'code']))
 j['d20_complete']=[int((x,y) in d20keys) for x,y in zip(j.date,j.code)]
 j,tables=summarize(j)
 panel=support_and_amount(j)
 same=same_date_contrast(j)
 orderrows=[]
 for (h,kind),grp in j[j.status_out==H20_OK].groupby(['horizon','price_barrier_order']):
  orderrows.append({'horizon':int(h),'barrier_order':kind,'n':len(grp),
                    'median_dplus_h_net_pct':float(grp.net_20bps_pct.median()),
                    'median_mae_intraday_pct':float(grp.mae_intraday_pct.median()),
                    'median_mfe_intraday_pct':float(grp.mfe_intraday_pct.median()),
                    'interpretation':'DESCRIPTIVE_INTRADAY_HIGH_LOW_NO_FILL_GUARANTEE'})
 order_summary=pd.DataFrame(orderrows)
 out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
 d6.write_csv(p,out/'R1D7_D5_D10_D15_D20_FROZEN_FORWARD_PRICE_PATHS.csv.gz')
 d6.write_csv(tables,out/'R1D7_HORIZON_YEAR_REGIME_AND_FIRST_CODE_SCORECARDS.csv')
 d6.write_csv(panel,out/'R1D7_FROZEN_SUPPORT_X_AMOUNT_AND_RISK.csv')
 d6.write_csv(same,out/'R1D7_SAME_DATE_CONTRAST_DESCRIPTIVE.csv')
 d6.write_csv(order_summary,out/'R1D7_PRICE_BARRIER_FIRST_TOUCH_ORDER_DESCRIPTIVE.csv')
 # No outcomes are ever inserted into or uploaded under ASOF name.
 cohort=j[j.horizon==20].copy();counts=cohort.groupby('date').size()
 summary={'version':VERSION,'status':'R1D7_RETROSPECTIVE_PATH_AUDIT_COMPLETE_NOT_NEW_OOS',
          'source_run_D6':D6_RUN,'source_D6_commit':D6_COMMIT,
          'source_KRX_run':d6.SOURCE_RUN,'frozen_signal_population':len(a),
          'frozen_signal_asof_SHA256':ORIGINAL_ASOF_SHA256,'source_KRX_daily_hashes_verified':count,
          'D20_reproduced_exactly':matched,
          'n_unique_codes':int(a.code.nunique()),'n_signal_dates':int(a.date.nunique()),
          'max_episodes_single_day':int(counts.max()),'median_episodes_per_signal_day':float(counts.median()),
          'n_codes_with_multiple_signals':int(a.code.value_counts().gt(1).sum()),
          'horizon_completion':{str(h):int((p[(p.horizon==h)].status_out==H20_OK).sum()) for h in HORIZONS},
          'd20_plus15_count':int(((p.horizon==20)&(p.net_20bps_pct>=15)).sum()),
          'd20_minus5_count':int(((p.horizon==20)&(p.net_20bps_pct<=-5)).sum()),
          'orders_placed':0,'RESEARCH_ONLY':True,'strategy_ready':False,
          'new_independent_OOS':False,'native_66_rescreen':False,
          'same_day_MFE_MAE_order_intraday_unknown':True,
          'fixed_price_hypothetical_fees_bps':20,'unadjusted_KRX_PIT_tradeability_certified':False,
          'param_selection_based_on_returns':False,'no_future_data_used_for_signal':True,
          'sample_depends_on_frozen_R1D6_2024_25_signal_set':True}
 (out/'R1D7_MANIFEST.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
 print('R1D7_COMPLETE_RESEARCH_ONLY',json.dumps(summary,ensure_ascii=False),flush=True)
 return summary


def main():
 ap=argparse.ArgumentParser()
 ap.add_argument('--asof',required=True)
 ap.add_argument('--sealed',required=True)
 ap.add_argument('--krx-root',required=True)
 ap.add_argument('--repo-root',default='.')
 ap.add_argument('--out',default='reports/r1d7/result')
 a=ap.parse_args();run(a)
if __name__=='__main__':main()
