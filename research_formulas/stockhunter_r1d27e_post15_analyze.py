#!/usr/bin/env python3
"""After-the-fact D+1..D+15 price/foreign/institution flow-path diagnostics.
Never rank or select trades with the post-signal future-flow measurements.
"""
from __future__ import annotations
import argparse,gzip,hashlib,io,json,pathlib
import numpy as np,pandas as pd
from research_formulas import stockhunter_r1d27e_post15_flow as f15

def pct(num,den):return 100*num/den if pd.notna(den) and den>0 else np.nan

def analyze(freeze_file,base_dir,checkpoint,out):
    fr,base,bm=f15.read_base(base_dir,freeze_file)
    root=pathlib.Path(checkpoint);m=f15.stage(root/f15.MAN)
    raw,old=f15.read_seed(root,fr,base,bm['base_sha256'])
    f15.chk(m['n_unattempted']==0 and len(old)==1500,'INCOMPLETE_SOURCE_COLLECTION')
    f15.chk(m['n_complete']==sum(str(x['source_complete'])=='1' for x in old.values()),'MANIFEST_COUNTS')
    bp=pathlib.Path(base_dir);prebytes=(bp/f15.PRE).read_bytes()
    f15.chk(f15.sha(prebytes)==bm['pre_compact_sha256'],'PRE_SOURCE_SHA')
    pre=pd.read_csv(io.BytesIO(gzip.decompress(prebytes)),dtype={k:str for k in f15.KEY})
    post=pd.DataFrame(raw)
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    if len(post)==0:raise RuntimeError('POST_FLOW_ALL_MISSING')
    post['big2_actor']=post.actor.isin(['기관합계','외국인합계'])
    post['day']=pd.to_numeric(post.day).astype(int)
    b=post.loc[post.big2_actor].groupby(f15.KEY+['day','trade_date'],as_index=False)[['net_krw','buy_krw','sell_krw']].sum()
    # Every successful event has exactly one investor row for each of 15 post-signal trading sessions.
    f15.chk(len(b)==m['n_complete']*15,'BIG2_AGG_COUNT')
    turnover=base[f15.KEY+['day','price_day','price_status','entry_open','daily_close','daily_high','daily_traded_krw','close_net_20bps_pct','cum_high_gross_pct']]
    q=b.merge(turnover,left_on=f15.KEY+['day','trade_date'],right_on=f15.KEY+['day','price_day'],how='left',validate='one_to_one')
    f15.chk(len(q)==len(b) and q.price_day.notna().all(),'PRICE_CALENDAR_JOIN')
    q=q.sort_values(f15.KEY+['day']).reset_index(drop=True)
    gr=q.groupby(f15.KEY,sort=False)
    q['big2_cum_net_krw']=gr.net_krw.cumsum()
    q['big2_cum_buy_krw']=gr.buy_krw.cumsum()
    q['big2_cum_sell_krw']=gr.sell_krw.cumsum()
    q['cum_turnover_krw']=gr.daily_traded_krw.cumsum()
    q['big2_daily_net_pct_of_turnover']=[pct(a,b) for a,b in zip(q.net_krw,q.daily_traded_krw)]
    q['big2_cum_net_pct_of_turnover']=[pct(a,b) for a,b in zip(q.big2_cum_net_krw,q.cum_turnover_krw)]
    q['net_buy_day']=q.net_krw.gt(0).astype(int)
    q['cum_positive_days']=gr.net_buy_day.cumsum()
    q['cum_net_new_high']=gr.big2_cum_net_krw.cummax().eq(q.big2_cum_net_krw).astype(int)
    q.to_csv(out/'R1D27E_POST15_DAILY_FLOW_PRICE_1_15.csv',index=False)
    actor=post[post.actor.isin(['기관합계','외국인합계'])].copy().sort_values(f15.KEY+['actor','day'])
    ga=actor.groupby(f15.KEY+['actor'],sort=False)
    actor['cum_net_krw']=ga.net_krw.cumsum()
    actor['cum_buy_krw']=ga.buy_krw.cumsum()
    actor['cum_sell_krw']=ga.sell_krw.cumsum()
    actor['cum_positive_days']=ga.net_krw.transform(lambda x:(x>0).cumsum())
    actor=actor.merge(turnover[f15.KEY+['day','daily_traded_krw']],on=f15.KEY+['day'],how='left',validate='many_to_one')
    actor['daily_net_pct_of_stock_turnover']=[pct(n,t) for n,t in zip(actor.net_krw,actor.daily_traded_krw)]
    actor.to_csv(out/'R1D27E_POST15_INST_FOREIGN_DAILY_SEPARATE.csv',index=False)
    # Paired signal-time features remain logically separated from future diagnostics.
    q=q.merge(pre[f15.KEY+['big2_net10_krw','inst_net10_krw','foreign_net10_krw',
                         'big2_net10_pct_of_market_turnover','both_inst_foreign_10d_net_acceleration_pos']],
        on=f15.KEY,how='left',validate='many_to_one')
    t=[]
    for key,g in q.groupby(f15.KEY,sort=False):
        a=g.sort_values('day');t14=a.loc[a.day.le(10),'net_krw'].sum();p10=a.big2_net10_krw.iloc[0]
        flow10=a.loc[a.day.le(10),'net_krw'].sum();flow5=a.loc[a.day.le(5),'net_krw'].sum()
        mid5=a.loc[a.day.between(6,10),'net_krw'].sum();late5=a.loc[a.day.between(11,15),'net_krw'].sum()
        mask=a.price_status.eq('COMPLETE')
        d15=a.loc[a.day.eq(15)].iloc[0]
        peak_idx=a.daily_high.idxmax() if mask.all() else None
        peak_day=float(a.loc[peak_idx,'day']) if peak_idx is not None else np.nan
        flow_after_peak=float(a.loc[a.day.gt(peak_day),'net_krw'].sum()) if peak_idx is not None else np.nan
        max_hi=float(a.daily_high.max()) if mask.all() else np.nan
        entry=float(a.entry_open.iloc[0])
        giveback=pct(max_hi-float(d15.daily_close),entry) if mask.all() else np.nan
        v={**dict(zip(f15.KEY,key)),
           'd15_price_complete':bool(mask.all()),
           'd15_close_net_pct':float(d15.close_net_20bps_pct) if mask.all() else np.nan,
           'd15_cum_high_pct':float(d15.cum_high_gross_pct) if mask.all() else np.nan,
           'd15_peak_to_close_giveback_pct_of_entry':giveback,
           'd15_price_peak_day':peak_day,
           'net_d1_5_krw':int(flow5),'net_d6_10_krw':int(mid5),'net_d11_15_krw':int(late5),
           'net_d1_10_krw':int(flow10),'net_d1_15_krw':int(a.net_krw.sum()),
           'net_d1_15_pct_of_turnover':float(a.big2_cum_net_pct_of_turnover.iloc[-1]),
           'net_d1_10_minus_presignal10_krw':float(flow10-p10) if pd.notna(p10) else np.nan,
           'net_d11_15_minus_d6_10_krw':int(late5-mid5),
           'net_d6_10_minus_d1_5_krw':int(mid5-flow5),
           'positive_net_days_d1_15':int((a.net_krw>0).sum()),
           'all_15_days_net_buy':int((a.net_krw>=0).all()),
           'net_since_price_peak_krw':flow_after_peak,
           'both_accel_pre10_flag':float(a.both_inst_foreign_10d_net_acceleration_pos.iloc[0]),
           'presignal_net10_krw':float(p10)}
        for d in (3,5,7,10,12,15):
            subset=a[a.day.le(d)]
            v[f'cum_net_d{d}_krw']=int(subset.net_krw.sum())
            v[f'cum_net_d{d}_pct_of_turnover']=pct(subset.net_krw.sum(),subset.daily_traded_krw.sum())
            v[f'cum_positive_days_d{d}']=int((subset.net_krw>0).sum())
            v[f'price_cum_high_d{d}_pct']=float(subset.cum_high_gross_pct.iloc[-1]) if mask.all() else np.nan
        for goal in (5,7,10,12,15):v[f'hit_{goal}_pct_by_d15']=int(bool(mask.all()) and v['d15_cum_high_pct']>=goal)
        t.append(v)
    events=pd.DataFrame(t)
    # Separately expose how institutions and foreigners behaved, not just their netted sum.
    actor_evt=[]
    for key,g in actor.groupby(f15.KEY+['actor'],sort=False):
        part=g.sort_values('day')
        a={**dict(zip(f15.KEY+['actor'],key)),
           'net_d1_15_krw':int(part.net_krw.sum()),
           'net_d1_5_krw':int(part.loc[part.day.le(5),'net_krw'].sum()),
           'net_d6_10_krw':int(part.loc[part.day.between(6,10),'net_krw'].sum()),
           'net_d11_15_krw':int(part.loc[part.day.between(11,15),'net_krw'].sum()),
           'positive_days_d15':int(part.net_krw.gt(0).sum()),
           'all_days_net_nonnegative':int(part.net_krw.ge(0).all())}
        for d in (3,5,7,10,12,15):
            a[f'cum_net_d{d}_krw']=int(part.loc[part.day.le(d),'net_krw'].sum())
        actor_evt.append(a)
    pd.DataFrame(actor_evt).to_csv(out/'R1D27E_POST15_INST_FOREIGN_EVENT_TRENDS.csv',index=False)
    events.to_csv(out/'R1D27E_POST15_EVENT_FLOW_TREND_1500.csv',index=False)
    # Descriptive associations only. No retrospective feature is a trading-time signal.
    complete=events[events.d15_price_complete].copy()
    complete['flow_profile']=np.select([
        complete.net_d1_15_krw.gt(0)&complete.net_d11_15_krw.ge(0),
        complete.net_d1_5_krw.gt(0)&complete.net_d11_15_krw.lt(0),
        complete.net_d1_15_krw.le(0)],
        ['NET_INFLOW_THROUGH_D15','EARLY_INFLOW_LATE_SELL','NONPOSITIVE_NET_D15'],default='MIXED')
    complete['peak_given_back_to_nonprofit']=complete.d15_cum_high_pct.ge(10)&complete.d15_close_net_pct.le(0)
    summary=[]
    for name,g in complete.groupby('flow_profile'):
        d={'group':name,'n':len(g),'mean_d15_close_net_pct':g.d15_close_net_pct.mean(),
           'median_d15_giveback_pct':g.d15_peak_to_close_giveback_pct_of_entry.median(),
           'd15_hit10_rate':g.hit_10_pct_by_d15.mean(),
           'hit10_then_nonprofit_rate_unconditional':g.peak_given_back_to_nonprofit.mean(),
           'positive_net15_rate':g.net_d1_15_krw.gt(0).mean(),
           'all_15_buy_days_rate':g.all_15_days_net_buy.mean()}
        summary.append(d)
    pd.DataFrame(summary).to_csv(out/'R1D27E_POST15_FLOW_TREND_DESCRIPTIVE.csv',index=False)
    rows=[]
    for day,g in q.groupby('day'):
        rows.append({'day':day,'n_flow_complete':len(g),'mean_daily_net_pct_of_turnover':g.big2_daily_net_pct_of_turnover.mean(),
                     'median_cum_net_pct_of_turnover':g.big2_cum_net_pct_of_turnover.median(),
                     'positive_net_day_share':g.net_krw.gt(0).mean(),
                     'share_cum_net_positive':g.big2_cum_net_krw.gt(0).mean(),
                     'n_price_complete_on_day':g.price_status.eq('COMPLETE').sum()})
    pd.DataFrame(rows).to_csv(out/'R1D27E_POST15_DAY_BY_DAY_FLOW_TREND.csv',index=False)
    # D+3..D+15 flow-vs-intraday target summaries; groups are concurrent future observables,
    # NOT signal-date predictors. Target touches are based on past highs up to each day.
    tgt=[]
    for (day,flow_up),g in q.loc[q.day.ge(3)&q.price_status.eq('COMPLETE')].assign(
            cum_flow_positive=lambda x:x.big2_cum_net_krw.gt(0)).groupby(['day','cum_flow_positive']):
        row={'day':int(day),'cumulative_flow_positive':bool(flow_up),'n':len(g),
             'average_daily_net_pct_turnover':float(g.big2_daily_net_pct_of_turnover.mean()),
             'average_cum_net_pct_turnover':float(g.big2_cum_net_pct_of_turnover.mean()),
             'mean_close_net_pct':float(g.close_net_20bps_pct.mean())}
        for threshold in (5,7,10,12,15):
            row[f'cum_high_hit_{threshold}_pct_rate']=float(g.cum_high_gross_pct.ge(threshold).mean())
        tgt.append(row)
    pd.DataFrame(tgt).to_csv(out/'R1D27E_POST15_PRICE_HIGH_TARGET_BY_FLOW_D3_D15.csv',index=False)
    missing=pd.DataFrame([*old.values()]);missing.loc[missing.source_complete.astype(str).ne('1')].to_csv(out/'R1D27E_POST15_QUARANTINE_NO_IMPUTATION.csv',index=False)
    summary=dict(version=f15.VERSION,phase='POST_SIGNAL_RETROSPECTIVE_DESCRIPTIVE',frozen=1500,
        post_flow_complete=m['n_complete'],post_flow_quarantined=m['n_quarantined'],
        paired_pre_signal_and_post_flow=int(events.presignal_net10_krw.notna().sum()),
        d15_price_complete_and_postflow=int(complete.shape[0]),
        net_outflows_not_missing=True,post_signal_flow_is_future_outcome=True,
        no_signal_time_prediction_from_future_flow=True,no_trade_executed=True,research_only=True,
        no_causal_claim=True,no_LIVE_change=True,prospective_OOS=False,
        source_checkpoint_sha=f15.sha((root/f15.MAN).read_bytes()),
        files_sha256={p.name:f15.sha(p.read_bytes()) for p in out.glob('*.csv')})
    (out/'R1D27E_POST15_ANALYSIS_MANIFEST.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    print('R1D27E_POST15_ANALYSIS',json.dumps({k:v for k,v in summary.items() if k not in ('files_sha256',)},ensure_ascii=False))
    return summary

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--freeze-file',required=True);ap.add_argument('--base-dir',required=True);ap.add_argument('--checkpoint',required=True);ap.add_argument('--out',required=True);a=ap.parse_args();analyze(a.freeze_file,a.base_dir,a.checkpoint,a.out)
if __name__=='__main__':main()
