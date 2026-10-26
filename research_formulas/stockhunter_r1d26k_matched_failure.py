#!/usr/bin/env python3
"""R1-D26K RESEARCH_ONLY: same strategy-month, near-date matched outcome contrasts.

Retrospective descriptive: NEVER interpret label-driven matches as a tradable strategy
or a prospective holdout. Uses only ASOF timestamp features and sealed D30 outcomes.
"""
from __future__ import annotations
import argparse, csv, gzip, hashlib, io, json, zipfile
from pathlib import Path
import numpy as np
import pandas as pd

VERSION='R1D26K_MATCHED_SUCCESS_FAILURE_RETROSPECTIVE_V1'
C='C_ABOVE224_RISING_WAVE_PULLBACK'
D='D_LEADER_CONSOLIDATION_BREAKOUT'
STATUS='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
LABELS=['HIT15_CLOSE_PROFIT','NO_HIT5_CLOSE_LOSS5','HIT15_CLOSE_LOSS5','OTHER']
MATCHES=[('HIT15_CLOSE_PROFIT','NO_HIT5_CLOSE_LOSS5','SUCCESS_VS_HARD_FAILURE'),('HIT15_CLOSE_PROFIT','HIT15_CLOSE_LOSS5','PROFIT_KEEP_VS_GIVEBACK')]
COMMON=['ma224_slope20_recomputed','rs20_vs_kospi_pctp','rs20_vs_kosdaq_pctp','amount_pre5_vs_pre20','amount_pre20_vs_pre40','distance448_pct','ret20_prior_signal_pct']
FEATS={C:['wave_gain_pct','pullback_retrace_pct','pullback_dry_ratio','prewave_amount_ratio']+COMMON,
       D:['base10_range_pct','amount_vs_prior20','D_compression_amount3']+COMMON}

def sha(b):return hashlib.sha256(b).hexdigest()
def readz(path,tag):
 with zipfile.ZipFile(path) as z:
  matches=[n for n in z.namelist() if tag in n]
  if len(matches)!=1:raise ValueError('SOURCE_PATH_AMBIGUITY_'+tag)
  n=matches[0];b=z.read(n)
  return pd.read_csv(io.BytesIO(gzip.decompress(b) if n.endswith('.gz') else b),dtype={'code':'string'},low_memory=False)
def check_source(path, expected):
 p=Path(path);b=p.read_bytes()
 if sha(b)!=expected:raise ValueError('PINNED_SOURCE_ZIP_SHA_MISMATCH')
 return p

def build_data(path,protocol):
 a=readz(path,'ALL_STRATEGIES_SIGNALTIME'); y=readz(path,'FUTURE_PRICE_PATHS')
 a=a[a.strategy.isin((C,D))].copy();y=y[(y.strategy.isin((C,D)))&(y.horizon==30)].copy()
 key=['strategy','date','code'];
 for f in (a,y):
  if f.duplicated(key).any():raise ValueError('DUPLICATE_KEY')
  f['code']=f.code.astype('string').str.zfill(6)
 if a.duplicated(key).any() or y.duplicated(key).any():raise ValueError('DUPLICATE_PADDED_KEY')
 if len(a)!=10310 or len(y)!=10310:raise ValueError('FROZEN_SIGNAL_COUNT_DRIFT')
 x=a.merge(y[key+['status_out','net_20bps_pct','mfe_intraday_pct','mae_intraday_pct','entry_day','exit_day']],on=key,validate='one_to_one',indicator=True)
 if (x._merge!='both').any():raise ValueError('MISSING_OUTCOME_ROW')
 x=x[x.status_out==STATUS].copy()
 if len(x)!=10198 or sum(x.strategy==C)!=7416 or sum(x.strategy==D)!=2782:raise ValueError('D30_COHORT_COUNT_DRIFT')
 if not (x.asof_lookback_last_index<=x.signal_ix).all():raise ValueError('ASOF_FUTURE_LOOKBACK')
 if not (pd.to_datetime(x.entry_day)>pd.to_datetime(x.date)).all():raise ValueError('ENTRY_DAY_NOT_AFTER_SIGNAL')
 for v in set(sum((FEATS[k] for k in FEATS),[])):
  if v not in a.columns:raise ValueError('MISSING_ASOF_FEATURE_'+v)
 x['year_month']=x.date.str[:7];x['year']=x.date.str[:4].astype(int)
 x['hit5']=(x.mfe_intraday_pct>=5).astype(int)
 x['hit10']=(x.mfe_intraday_pct>=10).astype(int)
 x['hit15']=(x.mfe_intraday_pct>=15).astype(int)
 x['loss5']=(x.net_20bps_pct<=-5).astype(int)
 x['label']=np.select([x.hit15.eq(1)&x.net_20bps_pct.gt(0),x.hit5.eq(0)&x.loss5.eq(1),x.hit15.eq(1)&x.loss5.eq(1)],LABELS[:3],default='OTHER')
 return x

def pairs_same_month(x,maximum_gap_days=10):
 """Outcome labels define study cohorts. Pairing among those labels uses ONLY date/code.
 Each control can be used once per contrast. No same-ticker match.
 """
 results=[]
 for label_a,label_b,kind in MATCHES:
  for (strategy,ym),g in x.groupby(['strategy','year_month'],sort=True):
   aa=g[g.label==label_a].sort_values(['date','code']);bb=g[g.label==label_b].sort_values(['date','code'])
   used=set()
   for ra in aa.itertuples(index=False):
    options=[]
    for rb in bb.itertuples(index=False):
     if rb.code==ra.code or (rb.date,rb.code) in used:continue
     gap=abs((pd.Timestamp(rb.date)-pd.Timestamp(ra.date)).days)
     if gap<=maximum_gap_days:options.append((gap,str(rb.date),str(rb.code),rb))
    if not options:continue
    _,_,_,rb=min(options,key=lambda t:t[:3]);used.add((rb.date,rb.code))
    results.append({'contrast':kind,'strategy':strategy,'year_month':ym,
      'anchor_date':ra.date,'anchor_code':ra.code,'control_date':rb.date,'control_code':rb.code,
      'calendar_gap_days':abs((pd.Timestamp(rb.date)-pd.Timestamp(ra.date)).days),
      'anchor_hit15':ra.hit15,'control_hit15':rb.hit15,'anchor_D30_net_pct':ra.net_20bps_pct,'control_D30_net_pct':rb.net_20bps_pct,
      **{ft+'_anchor':getattr(ra,ft) for ft in FEATS[strategy]},
      **{ft+'_control':getattr(rb,ft) for ft in FEATS[strategy]}})
 return pd.DataFrame(results)

def agg_features(pairs,draws=1200):
 rows=[];rng=np.random.default_rng(26026)
 for (kind,strategy),g in pairs.groupby(['contrast','strategy'],sort=True):
  clusters=sorted(g.year_month.unique())
  for ft in FEATS[strategy]:
   a=pd.to_numeric(g[ft+'_anchor'],errors='coerce');b=pd.to_numeric(g[ft+'_control'],errors='coerce');mask=a.notna()&b.notna()&np.isfinite(a)&np.isfinite(b)
   sub=g[mask].copy();v=(a[mask]-b[mask]).to_numpy(float)
   if len(v)<30:continue
   unit=[(pd.to_numeric(sub.loc[sub.year_month==mo,ft+'_anchor'],errors='coerce')-pd.to_numeric(sub.loc[sub.year_month==mo,ft+'_control'],errors='coerce')).to_numpy(float) for mo in clusters]
   unit=[u for u in unit if len(u)]
   bi=[]
   for _ in range(draws):
    chosen=rng.integers(0,len(unit),size=len(unit));values=np.concatenate([unit[j] for j in chosen]);bi.append(np.median(values))
   ci=np.percentile(bi,[2.5,97.5]);rows.append({'contrast':kind,'strategy':strategy,'feature':ft,'matched_pairs':len(g),'valid_pairs':len(v),'nonmissing_pct':round(100*len(v)/len(g),2),'month_clusters':len(unit),
     'anchor_median':round(float(np.median(a[mask])),5),'control_median':round(float(np.median(b[mask])),5),
     'median_paired_delta':round(float(np.median(v)),5),'paired_positive_pct':round(float((v>0).mean()*100),2),
     'cluster_bootstrap_median_delta_ci_low':round(float(ci[0]),5),'cluster_bootstrap_median_delta_ci_high':round(float(ci[1]),5),
     'direction_diff_consistent_ci_excludes_zero':bool(ci[0]>0 or ci[1]<0),
     'exploratory_multiple_testing_not_corrected':True})
 return pd.DataFrame(rows)

def label_report(x):
 return x.groupby(['strategy','year','label'],dropna=False).agg(n=('code','size'),unique_tickers=('code','nunique'),median_D30_net_pct=('net_20bps_pct','median'),median_MAE_intraday_pct=('mae_intraday_pct','median')).reset_index()

def flow_link(x,flowzip):
 z=readz(flowzip,'COMPLETE_300_PRIOR20_SIGNED_KRW');z['code']=z.code.astype('string').str.zfill(6)
 z['signal_date']=pd.to_datetime(z.signal_date).dt.strftime('%Y-%m-%d');z['trade_date']=pd.to_datetime(z.trade_date).dt.strftime('%Y-%m-%d')
 key=['signal_date','code','strategy','trade_date']
 if len(z)!=6000 or z.duplicated(key).any():raise ValueError('FLOW_COUNT_OR_DUPLICATE')
 if (z.trade_date>=z.signal_date).any():raise ValueError('FUTURE_INVESTOR_INPUT')
 if (z.groupby(['signal_date','code','strategy']).size()!=20).any():raise ValueError('MISSING_FLOW_DAY')
 for s in ['foreign_net_krw','institution_net_krw']:
  v=pd.to_numeric(z[s],errors='coerce')
  if v.isna().any() or (v%1!=0).any():raise ValueError('NONINTEGER_KRW')
  z[s]=v
 z=z.sort_values(key);z['idx']=z.groupby(['signal_date','code','strategy']).cumcount()
 frame=z.groupby(['signal_date','code','strategy'],sort=True).apply(lambda g:pd.Series({
   'institution_20day_total_KRW':g.institution_net_krw.sum(),'foreign_20day_total_KRW':g.foreign_net_krw.sum(),
   'inst_recent5_accel':int(g.iloc[15:].institution_net_krw.mean()>g.iloc[:15].institution_net_krw.mean()),
   'foreign_recent5_accel':int(g.iloc[15:].foreign_net_krw.mean()>g.iloc[:15].foreign_net_krw.mean())}),include_groups=False).reset_index().rename(columns={'signal_date':'date'})
 join=frame.merge(x[['date','code','strategy','year','label','hit5','hit10','hit15','net_20bps_pct']],on=['date','code','strategy'],how='left',validate='one_to_one',indicator=True)
 if len(join)!=300:raise ValueError('FLOW_JOIN_INVALID')
 valid=join[join._merge.eq('both')].copy()
 tab=valid.groupby(['strategy','label'],dropna=False).agg(n=('code','size'),distinct_tickers=('code','nunique'),inst20_positive_rate=('institution_20day_total_KRW',lambda v:(v>0).mean()),foreign20_positive_rate=('foreign_20day_total_KRW',lambda v:(v>0).mean()),inst_recent5_accel_rate=('inst_recent5_accel','mean'),foreign_recent5_accel_rate=('foreign_recent5_accel','mean')).reset_index()
 return join,tab

def main():
 p=argparse.ArgumentParser();p.add_argument('--protocol',required=True);p.add_argument('--r1d25-zip',required=True);p.add_argument('--r1d26h-zip',required=True);p.add_argument('--out',required=True);p.add_argument('--bootstrap-draws',type=int,default=1200);args=p.parse_args()
 proto=json.loads(Path(args.protocol).read_text(encoding='utf-8'))
 if not(proto['research_only'] and proto['live_changed']==False and proto['orders_placed']==0):raise ValueError('PROTOCOL_INVALID')
 check_source(args.r1d25_zip,proto['r1d25_sha256']);check_source(args.r1d26h_zip,proto['r1d26h_sha256'])
 x=build_data(args.r1d25_zip,proto)
 paired=pairs_same_month(x,proto['max_calendar_gap_days']);stats=agg_features(paired,args.bootstrap_draws);labels=label_report(x)
 flow,flowstats=flow_link(x,args.r1d26h_zip)
 out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
 peryear=[]
 for (typ,st,ym),g in paired.groupby(['contrast','strategy','year_month'],sort=True):
  for ft in FEATS[st]:
   a=pd.to_numeric(g[ft+'_anchor'],errors='coerce');b=pd.to_numeric(g[ft+'_control'],errors='coerce');delta=(a-b).dropna()
   if len(delta):peryear.append({'contrast':typ,'strategy':st,'year_month':ym,'year':int(ym[:4]),'feature':ft,'n_valid_pairs':len(delta),'median_paired_delta':float(delta.median()),'pct_anchor_higher':float(100*(delta>0).mean())})
 peryear=pd.DataFrame(peryear)
 outputs={'R1D26K_STRATEGY_YEAR_SUCCESS_FAILURE_CLASSES.csv':labels,'R1D26K_MONTH_YEAR_PAIRED_FEATURE_DELTAS.csv':peryear,'R1D26K_MONTH_MATCHED_PAIRS.csv.gz':paired,
          'R1D26K_ASOF_MATCHED_FEATURE_DIFFERENCES.csv':stats,'R1D26K_REAL_300_FLOW_OUTCOME_JOIN_EXPLORATORY.csv':flow,
          'R1D26K_REAL_300_FLOW_BY_OUTCOME_CATEGORY.csv':flowstats}
 dig={}
 for name,df in outputs.items():
  target=out/name;df.to_csv(target,index=False,encoding='utf-8-sig',compression='gzip' if name.endswith('.gz') else None);dig[name]=sha(target.read_bytes())
 counts={k:int(v) for k,v in x.label.value_counts().items()};gp=paired.groupby(['contrast','strategy']).size();pairsinfo={k[0]+':'+('C' if k[1]==C else 'D'):int(v) for k,v in gp.items()}
 manifest={'version':VERSION,'r1d25_source_sha256':proto['r1d25_sha256'],'r1d26h_source_sha256':proto['r1d26h_sha256'],
   'whole_signals':10310,'complete_D30':len(x),'censored':112,'C_complete':int((x.strategy==C).sum()),'D_complete':int((x.strategy==D).sum()),
   'outcome_classes':counts,'matched_pairs':pairsinfo,'matching':'same_strategy_and_same_year_month; nearest_calendar_date_max10; no_same_ticker; no_reuse_control',
   'bootstrap':'month_block_resampled_1200_research_descriptive_not_multiple_test_corrected','paired_feature_rows':len(stats),'flow_300_n':len(flow),'flow_price_complete':int(flow._merge.eq('both').sum()),
   'KRX_official_independent_crosscheck':'PENDING','historical_PIT_publication':'NOT_VERIFIED','real_execution_15pct_takeprofit':'NOT_VERIFIED',
   'post_touch_exit_inference':'NOT_CERTIFIED_ORDER_OF_INTRADAY_EVENTS','holdout_OOS':False,'new_strategy_claim':False,'strategy_ready':False,'live_changed':False,'orders_placed':0,'file_sha256':dig}
 (out/'R1D26K_QA_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
 print('R1D26K_MATCHED_CAUSAL_GUARDS_PASS',json.dumps({k:v for k,v in manifest.items() if k in ['whole_signals','complete_D30','matched_pairs','flow_300_n','flow_price_complete','strategy_ready']},ensure_ascii=False));return manifest
if __name__=='__main__':main()
