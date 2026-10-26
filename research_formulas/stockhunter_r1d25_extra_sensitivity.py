from pathlib import Path
import numpy as np,pandas as pd
import argparse
pa=argparse.ArgumentParser();pa.add_argument('--results',required=True);args=pa.parse_args()
r=Path(args.results)
p=pd.read_csv(r/'R1D25_D5_D10_D15_D20_D30_FUTURE_PRICE_PATHS.csv.gz',dtype={'code':str,'date':str})
key=['strategy','date','code','period'];a=p[(p.horizon==20)&(p.status_out=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED')][key+['net_20bps_pct','mfe_intraday_pct','mae_intraday_pct']]
b=p[(p.horizon==30)&(p.status_out=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED')][key+['net_20bps_pct','mfe_intraday_pct','mae_intraday_pct']]
x=a.merge(b,on=key,suffixes=('_20','_30'),validate='one_to_one')
rows=[]
for (period,strat),d in x.groupby(['period','strategy']):
 rows.append({'period':period,'strategy':strat,'n_same_signals_complete_both':len(d),
              'mean_D20_net_pct':d.net_20bps_pct_20.mean(),'mean_D30_net_pct':d.net_20bps_pct_30.mean(),
              'paired_mean_D30_minus_D20_pctp':(d.net_20bps_pct_30-d.net_20bps_pct_20).mean(),
              'paired_median_D30_minus_D20_pctp':(d.net_20bps_pct_30-d.net_20bps_pct_20).median(),
              'loss5_D20_pct':100*d.net_20bps_pct_20.le(-5).mean(),'loss5_D30_pct':100*d.net_20bps_pct_30.le(-5).mean(),
              'mfe_touch15_D20_pct':100*d.mfe_intraday_pct_20.ge(15).mean(),
              'mfe_touch15_D30_pct':100*d.mfe_intraday_pct_30.ge(15).mean(),
              'close_net15_D20_pct':100*d.net_20bps_pct_20.ge(15).mean(),
              'close_net15_D30_pct':100*d.net_20bps_pct_30.ge(15).mean()})
pd.DataFrame(rows).to_csv(r/'R1D25_PAIRED_H20_H30_IDENTICAL_SIGNALS.csv',index=False,encoding='utf-8-sig')
# equal-day C-D differences monthly cluster bootstrap, preserving days in each month
z=p[(p.horizon==30)&(p.status_out=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED')].copy()
z['bigwin']=z.mfe_intraday_pct.ge(15)
z['loss5']=z.net_20bps_pct.le(-5)
q=z.groupby(['period','date','strategy']).agg(mean_net=('net_20bps_pct','mean'),mfe15=('bigwin','mean'),loss5=('loss5','mean')).reset_index()
bs=[];rng=np.random.default_rng(20261026)
for period in q.period.unique():
 d=q[q.period.eq(period)];
 c=d[d.strategy.eq('C_ABOVE224_RISING_WAVE_PULLBACK')].set_index('date')
 v=d[d.strategy.eq('D_LEADER_CONSOLIDATION_BREAKOUT')].set_index('date')
 both=v.join(c,how='inner',lsuffix='_D',rsuffix='_C').reset_index()
 both['month']=both['date'].str.slice(0,7)
 months=sorted(both.month.unique());cluster={m:both[both.month.eq(m)] for m in months}
 if not months:continue
 for (metric,label) in [('mean_net','mean_return_diff_pctp'),('mfe15','mfe15_diff_pctp'),('loss5','loss5_diff_pctp')]:
  group=[np.asarray(cluster[m][metric+'_D']-cluster[m][metric+'_C'],float) for m in months]
  observed=np.concatenate(group).mean()*(100 if metric!='mean_net' else 1)
  bootstrap=np.empty(2000)
  for it in range(2000):
   idx=rng.integers(0,len(group),len(group))
   bootstrap[it]=np.concatenate([group[j] for j in idx]).mean()*(100 if metric!='mean_net' else 1)
  bs.append({'period':period,'metric':label,'paired_signal_days':len(both),'month_clusters':len(months),
             'point_estimate':observed,'month_cluster_percentile_CI_2p5':np.quantile(bootstrap,.025),
             'month_cluster_percentile_CI_97p5':np.quantile(bootstrap,.975),
             'uncertainty_warning':'EXPLORATORY_MONTH_RESAMPLING; OVERLAPPING_SIGNALS; NOT OOS'})
pd.DataFrame(bs).to_csv(r/'R1D25_C_D_D30_MONTH_CLUSTER_BOOTSTRAP_EXPLORATORY.csv',index=False,encoding='utf-8-sig')
print('R1D25_PAIRED_20_30_AND_MONTH_CLUSTER_RESULTS',len(rows),len(bs),flush=True)
