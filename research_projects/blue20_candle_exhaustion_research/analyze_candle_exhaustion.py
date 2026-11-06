#!/usr/bin/env python3
"""RESEARCH_ONLY: prior-20 turnover x candle exhaustion x MA x day+1 confirmation.

Strict PIT: D0 features are known only after D0 close. The D1 candle is NEVER
used to decide an entry at D1 open. D1 EOD confirmation can trigger D2 OPEN.
Fixed D0+20 outcome, original cohort, stop-first OHLC ambiguity and 0.25%p
round-trip cost. Original BLUE20 RSI code/production are not modified.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
MA_RESEARCH = REPO / 'research_projects' / 'blue20_ma_structure_research'
sys.path.insert(0, str(MA_RESEARCH))
try:
    from analyze_ma import load_events, load_manifest
    from post_entry_flow import add_bar_features, simulate_fixed_or_structural
except ModuleNotFoundError as ex:
    raise RuntimeError('Existing research_projects/blue20_ma_structure_research files required') from ex

VERSION='BLUE20_UPPER_CANDLE_EXHAUSTION_R1_RESEARCH_ONLY'
COUNT=1804


def sha256(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda:f.read(1<<20),b''):h.update(chunk)
    return h.hexdigest()


def candle_features(frame):
    """Daily point-in-time indicators; rolling baselines exclude today's bar."""
    d=add_bar_features(frame)
    prevclose=d.close.shift(1)
    tr=pd.concat([(d.high-d.low),(d.high-prevclose).abs(),(d.low-prevclose).abs()],axis=1).max(axis=1)
    d['atr20']=tr.rolling(20,min_periods=20).mean()
    d['prior_atr20']=d['atr20'].shift(1)  # candle length against volatility BEFORE D0
    d['body_signed_pct_open']=100*(d.close/d.open-1)
    d['body_abs_atr20']=abs(d.close-d.open)/d.atr20.replace(0,np.nan)
    d['body_abs_prior_atr20']=abs(d.close-d.open)/d.prior_atr20.replace(0,np.nan)
    d['range_atr20']=(d.high-d.low)/d.atr20.replace(0,np.nan)
    d['prev20_high']=d.high.shift(1).rolling(20,min_periods=20).max()
    d['prev20_low']=d.low.shift(1).rolling(20,min_periods=20).min()
    d['prior5_return_pct']=100*(d.close.shift(1)/d.close.shift(6)-1)
    d['prior20_return_pct']=100*(d.close.shift(1)/d.close.shift(21)-1)
    d['close_over_ma60_pct']=100*(d.close/d.ma60-1)
    d['close_over_ma112_pct']=100*(d.close/d.ma112-1)
    d['highest_prev20_break_pct']=100*(d.close/d.prev20_high-1)
    return d


def bin_numeric(x,edges,labels):
    if pd.isna(x):return 'PIT_MISSING'
    return str(pd.cut([float(x)],bins=edges,labels=labels,right=False,include_lowest=True)[0])


def d0_features(d,j):
    r=d.iloc[j]
    required=['atr20','prior_atr20','amount_ratio20','volume_ratio20','ma60','ma112','ma60_slope10','ma112_slope10']
    if any(pd.isna(r[x]) or (x in ['atr20','ma60','ma112'] and r[x]<=0) for x in required):
        raise ValueError('D0_TECHNICAL_WARMUP_MISSING '+str(r.date))
    if r.close<=r.open:raise ValueError('D0_NOT_BULL_CANDLE '+str(r.date))
    return {
        'd0_open':float(r.open),'d0_high':float(r.high),'d0_low':float(r.low),'d0_close':float(r.close),
        'd0_prev_close':float(d.close.iloc[j-1]),
        'd0_return_pct':float(r.price_return1_pct),
        'd0_body_signed_pct_open':float(r.body_signed_pct_open),
        'd0_body_abs_atr20':float(r.body_abs_atr20),
        'd0_body_abs_prior_atr20':float(r.body_abs_prior_atr20),
        'd0_range_atr20':float(r.range_atr20),
        'd0_body_to_range':float(r.body_to_range),
        'd0_upper_wick_to_range':float(r.upper_wick_to_range),
        'd0_lower_wick_to_range':float(r.lower_wick_to_range),
        'd0_close_location':float(r.close_location),
        'd0_candle_shape':str(r.candle_shape),
        'd0_amount_ratio20':float(r.amount_ratio20),
        'd0_volume_ratio20':float(r.volume_ratio20),
        'd0_prior5_return_pct':float(r.prior5_return_pct),
        'd0_prior20_return_pct':float(r.prior20_return_pct),
        'd0_high20_break_pct':float(r.highest_prev20_break_pct),
        'd0_close_over_ma60_pct':float(r.close_over_ma60_pct),
        'd0_close_over_ma112_pct':float(r.close_over_ma112_pct),
        'd0_ma60_slope10':float(r.ma60_slope10),'d0_ma112_slope10':float(r.ma112_slope10),
        'd0_ma_inverse':bool(r.ma60<r.ma112<r.ma224),
        'd0_upper_band':float(r.bb20_upper_shift26),
        'd0_lower_band':float(r.bb20_lower_shift26),
    }


def d1_features(d,j,d0_idx):
    """Only D1 EOD facts; never input into a D1-open strategy."""
    a=d.iloc[d0_idx];r=d.iloc[j]
    mid=(float(a.open)+float(a.close))/2
    upper=float(r.bb20_upper_shift26)
    return dict(
        d1_date=str(r.date),d1_open=float(r.open),d1_high=float(r.high),
        d1_low=float(r.low),d1_close=float(r.close),
        d1_open_gap_pct=100*(float(r.open)/float(a.close)-1),
        d1_return_from_d0close_pct=100*(float(r.close)/float(a.close)-1),
        d1_amount_ratio20=float(r.amount_ratio20),
        d1_close_location=float(r.close_location),
        d1_candle_shape=str(r.candle_shape),
        d1_mid_body_hold=bool(r.close>=mid),
        d1_upper_band_hold=bool(r.close>=upper),
        d1_above_d0_high=bool(r.close>=a.high),
        d1_bear_candle=bool(r.close<r.open),
        d1_day_only_eod_known=True,
    )


def high_risk_d0(f):
    """Predeclared candidate, not a fitted winning filter."""
    return bool(f['d0_amount_ratio20']>=10 and
                f['d0_ma60_slope10']<0 and f['d0_ma112_slope10']<0)


def screen_baseline(df):
    if len(df)!=COUNT:raise ValueError('BASELINE_EVENT_COUNT_INVALID')
    if df.duplicated(['code','date','band']).any():raise ValueError('DUPLICATE_BASELINE_EVENTS')
    if set(df.year)!={2023,2024}:raise ValueError('REQUIRES_BOTH_2023_AND_2024')
    if not (df.band.eq('UPPER').sum()==834 and df.band.eq('LOWER').sum()==970):
        raise ValueError('REFERENCE_UPPER_LOWER_COUNT_MISMATCH')


def build_table(events,frames):
    output=[]
    for e in events.itertuples(index=False):
        d=frames[e.code]
        ix=pd.Series(d.index,index=d.date)
        if e.date not in ix or e.gc_date not in ix or e.GC_NEXT_OPEN_entry_date not in ix:
            raise ValueError('EVENT_DATE_NOT_IN_SOURCE '+e.code+':'+e.date)
        d0=int(ix[e.date]);gc=int(ix[e.gc_date]);entry=int(ix[e.GC_NEXT_OPEN_entry_date])
        if not (d0<=gc<entry<=d0+20<len(d)):raise ValueError('NO_CAUSAL_TRADING_WINDOW')
        d0f=d0_features(d,d0)
        if abs(d0f['d0_close']-float(e.d0_close))>max(0.01,1e-7*d0f['d0_close']):
            raise ValueError('EVENT_D0_OHLCV_CLOSE_MISMATCH '+e.code)
        if abs(float(e.GC_NEXT_OPEN_entry_price)-float(d.open.iloc[entry]))>max(0.01,1e-7*float(d.open.iloc[entry])):
            raise ValueError('EVENT_ENTRY_OHLCV_OPEN_MISMATCH '+e.code)
        if abs(float(e.d0_amount_ratio)-d0f['d0_amount_ratio20'])>1e-5:
            raise ValueError('D0_TURNOVER_RATIO_SOURCE_MISMATCH '+e.code)
        replay=simulate_fixed_or_structural(d,d0,entry,float(d.open.iloc[entry]),False)
        if abs(replay['net_pct']-float(e.GC_NEXT_OPEN_net_pct))>1e-6:
            raise ValueError('BASELINE_FIXED_EXIT_NOT_REPRODUCED '+e.code+' '+e.date+' '+str(replay['net_pct']))
        record=dict(code=e.code,date=str(e.date),year=int(e.year),band=str(e.band),
              gc_offset=int(e.gc_offset),gc_date=str(e.gc_date),entry_date=str(e.GC_NEXT_OPEN_entry_date),
              baseline_entry_price=float(e.GC_NEXT_OPEN_entry_price),
              baseline_net_pct=float(e.GC_NEXT_OPEN_net_pct),
              baseline_exit_reason=str(e.GC_NEXT_OPEN_exit_reason),
              baseline_exit_date=str(e.GC_NEXT_OPEN_exit_date),
              baseline_stop=str(e.GC_NEXT_OPEN_exit_reason).startswith('SL'),
              baseline_tp=str(e.GC_NEXT_OPEN_exit_reason).startswith('TP'),
              baseline_mfe_pct=float(e.GC_NEXT_OPEN_mfe_pct),
              baseline_mae_pct=float(e.GC_NEXT_OPEN_mae_pct),
              market_regime=str(e.gc_market_regime),
              wave_state=str(e.wave_state),stoch_state=str(e.stoch_state_gc),
              **d0f)
        record['d0_high_risk_turnover_ma_down']=high_risk_d0(d0f)
        record['d0_gain_bucket']=bin_numeric(record['d0_return_pct'],[-np.inf,5,10,15,20,np.inf],['<5%','5-10%','10-15%','15-20%','20%+'])
        record['d0_body_atr_bucket']=bin_numeric(record['d0_body_abs_prior_atr20'],[-np.inf,1,2,3,np.inf],['<1x ATR','1-2x ATR','2-3x ATR','3x+ ATR'])
        record['d0_ma60_gap_bucket']=bin_numeric(record['d0_close_over_ma60_pct'],[-np.inf,0,5,10,15,20,np.inf],['below MA60','0-5%','5-10%','10-15%','15-20%','20%+'])
        record['d0_turnover_bucket']=bin_numeric(record['d0_amount_ratio20'],[-np.inf,2,5,10,20,np.inf],['<2x','2-5x','5-10x','10-20x','20x+'])
        record['d0_upper_wick_bucket']=bin_numeric(record['d0_upper_wick_to_range'],[-np.inf,.2,.4,np.inf],['<20%','20-40%','40%+'])
        record['d0_close_location_bucket']=bin_numeric(record['d0_close_location'],[-np.inf,.5,.8,np.inf],['lower50%','50-80%','top20%'])
        record['d0_prior20_return_bucket']=bin_numeric(record['d0_prior20_return_pct'],[-np.inf,0,10,20,np.inf],['prior_down','0-10%','10-20%','20%+'])
        if int(e.gc_offset)==0:
            if entry!=d0+1:raise ValueError('D0_GC_MUST_ENTER_D1_OPEN')
            dd=d1_features(d,d0+1,d0)
            record.update(dd)
            record['d1_signal_can_only_enter_d2']=True
            for mode,accepted in [
                ('D1_CLOSE_HOLD_BODY_MID',dd['d1_mid_body_hold']),
                ('D1_CLOSE_HOLD_UPPER_BAND',dd['d1_upper_band_hold']),
                ('D1_BOTH_MID_AND_UPPER',dd['d1_mid_body_hold'] and dd['d1_upper_band_hold']),
                ('D1_CLOSE_ABOVE_D0_HIGH',dd['d1_above_d0_high']),
            ]:
                record[mode+'_enter']=bool(accepted)
                if accepted:
                    delayed=simulate_fixed_or_structural(d,d0,d0+2,float(d.open.iloc[d0+2]),False)
                    record[mode+'_net_pct_per_opportunity']=float(delayed['net_pct'])
                    record[mode+'_exit_reason']=delayed['exit_reason']
                    record[mode+'_entry_price']=float(d.open.iloc[d0+2])
                    record[mode+'_exit_date']=delayed['exit_date']
                else:
                    record[mode+'_net_pct_per_opportunity']=0.0
                    record[mode+'_exit_reason']='NO_TRADE'
                    record[mode+'_entry_price']=np.nan
                    record[mode+'_exit_date']=''
        output.append(record)
    d=pd.DataFrame(output)
    screen_baseline(d)
    return d


def stat_summary(s):
    n=len(s); ny=(s.year.value_counts().to_dict());
    return dict(events=n,stocks=int(s.code.nunique()),
                n_2023=int(ny.get(2023,0)),n_2024=int(ny.get(2024,0)),
                avg_net_pct=round(float(s.baseline_net_pct.mean()),5),
                median_net_pct=round(float(s.baseline_net_pct.median()),5),
                sl_rate_pct=round(float(s.baseline_stop.mean()*100),3),
                tp_rate_pct=round(float(s.baseline_tp.mean()*100),3),
                avg_mfe_pct=round(float(s.baseline_mfe_pct.mean()),4),
                screen_min_n30_stocks15_bothyears10=bool(n>=30 and s.code.nunique()>=15 and ny.get(2023,0)>=10 and ny.get(2024,0)>=10))


def summarise(df,columns):
    result=[]
    for col in columns:
        for (band,val),s in df.groupby(['band',col],dropna=False):
            result.append(dict(feature=col,band=band,value=str(val),**stat_summary(s)))
    return pd.DataFrame(result)


def paired_policy_summary(df):
    p=df[(df.gc_offset==0)&(df.band=='UPPER')].copy()
    if p.empty:raise ValueError('NO_UPPER_D0_RSI_GC')
    modes=['D1_CLOSE_HOLD_BODY_MID','D1_CLOSE_HOLD_UPPER_BAND','D1_BOTH_MID_AND_UPPER','D1_CLOSE_ABOVE_D0_HIGH']
    rows=[]
    for category,sub in [('ALL_UPPER_D0_GC',p),('D0_HIGH_RISK',p[p.d0_high_risk_turnover_ma_down]),
                         ('D0_NOT_HIGH_RISK',p[~p.d0_high_risk_turnover_ma_down]),
                         ('2023',p[p.year==2023]),('2024',p[p.year==2024])]:
        for mode in modes:
            eligible=sub[mode+'_enter'].fillna(False)
            take=sub[eligible]
            avgtr=take[mode+'_net_pct_per_opportunity'].mean() if len(take) else np.nan
            opp=sub[mode+'_net_pct_per_opportunity']
            rows.append(dict(cohort=category,policy=mode,opportunities=len(sub),stocks=sub.code.nunique(),
                executed_trades=len(take),entry_rate_pct=round(100*float(eligible.mean()),3) if len(sub) else 0,
                baseline_all_opportunities_net_pct=float(sub.baseline_net_pct.mean()) if len(sub) else np.nan,
                policy_all_opportunities_net_pct=float(opp.mean()) if len(sub) else np.nan,
                delta_per_opportunity_pp=float((opp-sub.baseline_net_pct).mean()) if len(sub) else np.nan,
                policy_per_executed_trade_net_pct=float(avgtr) if len(take) else np.nan,
                skipped_zero_return_count=int((~eligible).sum()),
                policy_loss_over10_count=int((take[mode+'_net_pct_per_opportunity']<=-10).sum()) if len(take) else 0,
                compared_on_exact_same_D0_events=True, D1_close_used_only_for_D2_open=True))
    return pd.DataFrame(rows)


def filter_policy_summary(df):
    """A skip is a zero-return opportunity; count missed winners as well as saved stops."""
    up=df[df.band=='UPPER'].copy()
    filters={
        'NONE_BASELINE':pd.Series(False,index=up.index),
        'SKIP_D0_GAIN_GE20':up.d0_return_pct.ge(20),
        'SKIP_D0_BODY_PRIOR_ATR_GE3':up.d0_body_abs_prior_atr20.ge(3),
        'SKIP_D0_TURNOVER_GE10':up.d0_amount_ratio20.ge(10),
        'SKIP_MA60_DISLOCATION_GE15':up.d0_close_over_ma60_pct.ge(15),
        'SKIP_TURNOVER_GE10_AND_MA60_112_BOTH_DOWN':up.d0_high_risk_turnover_ma_down,
        'SKIP_TURNOVER_GE10_AND_PRIOR_ATR_GE3_AND_BOTH_MAS_DOWN':(
            up.d0_amount_ratio20.ge(10) & up.d0_body_abs_prior_atr20.ge(3) & up.d0_high_risk_turnover_ma_down),
    }
    results=[]
    for cohort,s in [('ALL_UPPER',up),('2023',up[up.year==2023]),('2024',up[up.year==2024]),
                     ('RSI_GC_D0_ONLY',up[up.gc_offset==0])]:
        for name,mask in filters.items():
            skipped=s[mask.reindex(s.index).fillna(False)]
            kept=s[~mask.reindex(s.index).fillna(False)]
            netkept=kept.baseline_net_pct
            result=dict(cohort=cohort,policy=name,all_opportunities=len(s),trades=len(kept),
                skips=len(skipped),trade_rate_pct=100*len(kept)/len(s),
                baseline_all_opportunity_avg_pct=float(s.baseline_net_pct.mean()),
                filter_all_opportunity_avg_pct=float(netkept.sum()/len(s)),
                filter_executed_trade_avg_pct=float(netkept.mean()) if len(kept) else np.nan,
                delta_per_opportunity_pp=float(-skipped.baseline_net_pct.sum()/len(s)),
                skipped_original_TP=int(skipped.baseline_tp.sum()),
                skipped_original_SL=int(skipped.baseline_stop.sum()),
                skipped_original_mfe_ge20=int(skipped.baseline_mfe_pct.ge(20).sum()),
                skipped_2023=int((skipped.year==2023).sum()),
                skipped_2024=int((skipped.year==2024).sum()),
                repeated_stocks_not_independent=True)
            results.append(result)
    return pd.DataFrame(results)


def select_chart_cases(df):
    up=df[df.band=='UPPER'].copy()
    cases=[]
    wanted=[('028300','2023-02-20'),('006220','2024-01-24')]
    for code,date in wanted:
        s=up[(up.code==code)&(up.date==date)]
        if len(s)!=1:raise ValueError('NAMED_REFERENCE_CASE_MISSING '+code+':'+date)
        cases.append(s.iloc[0])
    candidates=up[(up.d0_body_abs_prior_atr20>=2)&(up.d0_amount_ratio20>=5)].copy()
    for label,reason in [('SUCCESS','TP'),('FAILURE','SL')]:
        c=candidates[candidates.baseline_exit_reason.eq(reason)].sort_values(['d0_body_abs_prior_atr20','d0_amount_ratio20'],ascending=False)
        for x in c.itertuples(index=False):
            if len([e for e in cases if (e.baseline_exit_reason=='TP' if label=='SUCCESS' else e.baseline_exit_reason=='SL')])>=4:
                break
            if (x.code,x.date) in {(e.code,e.date) for e in cases}:continue
            cases.append(pd.Series(x._asdict()))
    return pd.DataFrame(cases)


def draw_case(one, d, path):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    ix=pd.Series(d.index,index=d.date)
    signal=int(ix[one.date]);left=max(0,signal-72);right=min(len(d)-1,signal+20)
    ds=d.iloc[left:right+1];xs=np.arange(left,right+1)
    fig,(ax,av)=plt.subplots(2,1,figsize=(14,7.5),sharex=True,gridspec_kw={'height_ratios':[4,1.4],'hspace':.08})
    for x,r in zip(xs,ds.itertuples(index=False)):
        col='#d04456' if r.close>=r.open else '#2b71ae'
        ax.vlines(x,r.low,r.high,color=col,lw=.7)
        ax.add_patch(Rectangle((x-.3,min(r.open,r.close)),.6,max(abs(r.close-r.open),r.close*.0007),facecolor=col,edgecolor=col,lw=.3))
    for n,c in [(20,'#b0a7ac'),(60,'#00a399'),(112,'#855ac0'),(224,'#4c566a')]:
        ax.plot(xs,ds[f'ma{n}'],label=f'MA{n}',lw=1.2,color=c)
    ax.plot(xs,ds['bb20_upper_shift26'],ls='--',lw=1.5,color='#e48d23',label='Orange shifted upper')
    ax.plot(xs,ds['bb20_lower_shift26'],ls='--',lw=1.5,color='#2f79d7',label='Blue shifted lower')
    ax.axvline(signal,color='#2f5bbc',ls='--',lw=1.2,label='D0 breakout')
    ax.axvline(signal+1,color='#9c7b22',ls=':',lw=1,label='D1 close confirmation (not D1-open info)')
    ax.axvline(signal+2,color='#268a62',ls=':',lw=1,label='D2 open hypothetical')
    ax.hlines((one.d0_open+one.d0_close)/2,xmin=signal,xmax=min(signal+4,right),lw=1.2,color='#0a856b',label='D0 body midpoint')
    ax.axhline(one.baseline_entry_price,lw=.65,color='#817158',alpha=.6,ls=':')
    av.bar(xs,ds.amount/1e8,width=.75,color=np.where(ds.close>=ds.open,'#d04456','#2b71ae'),alpha=.7)
    axv=av.twinx();axv.plot(xs,ds.amount_ratio20,lw=.85,color='#b67728',alpha=.85,label='Turnover / prior 20')
    cap=np.nanpercentile(ds.amount_ratio20,98) if ds.amount_ratio20.notna().any() else 4
    axv.set_ylim(0,min(max(float(cap)*1.12,2),120));axv.set_ylabel('Turnover x',color='#b67728')
    av.set_ylabel('Value traded (100m KRW)');ax.set_ylabel('KRW')
    ax.set_title(f'{one.code} D0 {one.date} | {one.band} | {one.baseline_exit_reason} {one.baseline_net_pct:+.2f}%\n'
                 f'D0 +{one.d0_return_pct:.1f}%  Body/prior ATR20={one.d0_body_abs_prior_atr20:.2f}x  Turnover={one.d0_amount_ratio20:.1f}x  D0-to-MA60={one.d0_close_over_ma60_pct:+.1f}%',fontsize=11)
    ticks=[x for x in xs if x==signal or x==signal+1 or (x-left)%10==0]
    av.set_xticks(ticks,[str(d.date.iloc[x])[5:] for x in ticks],rotation=45,ha='right')
    for a in (ax,av):a.grid(alpha=.16)
    ax.legend(loc='upper left',fontsize=8,ncol=4)
    fig.text(.5,.005,'R118 SHA256-pinned historical actual OHLCV, research only. D1 closing signal is usable from D2 OPEN.',ha='center',fontsize=8)
    fig.savefig(path,dpi=145,bbox_inches='tight',facecolor='white');plt.close(fig)


def gallery_html(cases):
    cards=[]
    for e in cases.itertuples(index=False):
        file=f'{e.code}_{e.date}_{e.band}.png'
        d1f=('D1 close held original candle midpoint: '+str(bool(e.d1_mid_body_hold))) if e.gc_offset==0 else 'RSI GC > D0; D1 strategy not applicable'
        cards.append(f'<section><h2>{html.escape(e.code)} {html.escape(e.date)} ({html.escape(e.band)})</h2><p>Original trade {html.escape(e.baseline_exit_reason)} {e.baseline_net_pct:+.2f}% · '+html.escape(d1f)+'</p><img src="charts/{html.escape(file)}" alt="Actual PIT OHLCV candle chart for {html.escape(e.code)}"></section>')
    return '<!doctype html><html lang="ko"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BLUE20 장대양봉 실제 차트</title><style>body{font:15px/1.6 sans-serif;background:#f3f6fa;color:#15243b;max-width:1120px;margin:auto;padding:18px}section{background:white;border:1px solid #dce4f1;padding:18px;margin:16px 0;border-radius:14px}img{width:100%;height:auto}small{color:#555}</style><h1>장대양봉 성공·실패 실제 일봉 비교</h1><small>D0 장대양봉, D1 종가 확인, D2 시가 진입 가상전략은 인과관계를 지켜 분리합니다. 사후 최고 상승을 실현수익이라 부르지 않습니다.</small>'+''.join(cards)+'</html>'


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--events',required=True)
    p.add_argument('--manifest',required=True)
    p.add_argument('--out',required=True)
    args=p.parse_args(argv)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    stage=out/'RUN_STAGE.json'
    def update(name,**kw):
        stage.write_text(json.dumps(dict(version=VERSION,stage=name,success=False,**kw),ensure_ascii=False,indent=2),encoding='utf-8')
    update('LOADING_VERIFIED_SOURCES')
    try:
        ev=load_events(args.events)
        frames,hashes=load_manifest(args.manifest,set(ev.code))
        frames={code:candle_features(raw) for code,raw in frames.items()}
        update('PIT_FEATURES_AND_BASELINE_REPLAY',n_events=len(ev))
        df=build_table(ev,frames)
        df.sort_values(['date','code','band'],inplace=True)
        df.to_csv(out/'01_ALL_1804_EVENT_DAY_CANDLES.csv.gz',index=False,compression='gzip')
        columns=['d0_gain_bucket','d0_body_atr_bucket','d0_ma60_gap_bucket','d0_turnover_bucket',
                 'd0_upper_wick_bucket','d0_close_location_bucket','d0_prior20_return_bucket',
                 'd0_high_risk_turnover_ma_down','d0_ma_inverse','market_regime','wave_state']
        summarise(df,columns).to_csv(out/'02_CANDLE_MA_TURNOVER_OUTCOME_GROUPS.csv',index=False)
        d0up=df[(df.band=='UPPER')&(df.gc_offset==0)].copy()
        if len(d0up)<100:raise ValueError('UPPER_D0_TOO_SMALL')
        summarise(d0up,['d1_candle_shape','d1_mid_body_hold','d1_upper_band_hold',
                        'd1_above_d0_high','d1_bear_candle']).to_csv(out/'03_D1_EOD_REACTION_OUTCOME_ONLY.csv',index=False)
        policy=paired_policy_summary(df)
        policy.to_csv(out/'04_D0_GC_D1_CONFIRMATION_POLICIES.csv',index=False)
        filter_policy_summary(df).to_csv(out/'04b_D0_FILTER_SKIP_VS_LOST_WINNERS.csv',index=False)
        for band,sub in df.groupby('band'):
            pass
        byyear=[]
        for yr,g in df.groupby('year'):
            byyear.append(summarise(g,columns).assign(year=yr))
        pd.concat(byyear,ignore_index=True).to_csv(out/'05_STRATIFIED_BY_YEAR.csv',index=False)
        studycols=['code','date','year','band','gc_offset','entry_date','baseline_net_pct','baseline_exit_reason',
            'd0_return_pct','d0_body_signed_pct_open','d0_body_abs_atr20','d0_body_abs_prior_atr20','d0_amount_ratio20',
            'd0_ma60_slope10','d0_ma112_slope10','d0_close_over_ma60_pct',
            'd1_date','d1_close','d1_candle_shape','d1_mid_body_hold','d1_upper_band_hold',
            'd1_above_d0_high']
        df[studycols].to_csv(out/'06_EVENT_CASE_SCREENING_LIST.csv',index=False)
        cases=select_chart_cases(df)
        cases.to_csv(out/'07_VERIFIED_SUCCESS_FAILURE_CHART_CASES.csv',index=False)
        (out/'charts').mkdir(exist_ok=True)
        for e in cases.itertuples(index=False):
            draw_case(e,frames[e.code],out/'charts'/f'{e.code}_{e.date}_{e.band}.png')
        (out/'08_ACTUAL_CHART_GALLERY.html').write_text(gallery_html(cases),encoding='utf-8')
        audit=dict(version=VERSION,success=True,selection_asof='2022-12 (R118 pinned cohort)',
            sampled_years=[2023,2024],n_primary_events=len(df),n_upper=int(df.band.eq('UPPER').sum()),
            n_lower=int(df.band.eq('LOWER').sum()),n_upper_gc_d0=len(d0up),
            baseline_replayed_and_matches_all_1804=True,
            event_source_sha256=sha256(args.events),manifest_sha256=sha256(args.manifest),
            unique_price_sha256=hashes,charts=len(cases),D1_eod_usable_no_earlier_than_D2_open=True,
            D0_candle_EOD_usable_for_D1_open_entry=True,
            fixed_common_horizon='D0+20',tp_pct=10,stop_pct=-5,roundtrip_cost_pp=.25,
            same_day_both_hit_stop_first=True,nontrade_opportunity_return_zero=True,
            repeated_stock_and_date_signals_non_independent=True,
            reference_RSI_formula_UNCERTIFIED=True,source_archived_KRX_derived_NOT_official_direct=True,
            multiple_testing_no_causal_claim=True,not_out_of_sample=True,
            no_foreign_institution_orderflow_claims=True,live_search_watch_untouched=True,
            production_ready=False, strategy_upgrade_allowed=False)
        (out/'AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
        stage.write_text(json.dumps(dict(version=VERSION,stage='COMPLETED_RESEARCH_ONLY',success=True,
            n_events=len(df),charts=len(cases),production_ready=False),ensure_ascii=False,indent=2),encoding='utf-8')
        print('BLUE20_CANDLE_EXHAUSTION_COMPLETE '+json.dumps(dict(events=len(df),upper_d0=len(d0up),charts=len(cases))),flush=True)
    except BaseException as ex:
        update('FAILED_CLOSED',error_type=type(ex).__name__,error=str(ex));raise


if __name__=='__main__':main()
