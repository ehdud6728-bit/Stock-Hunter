#!/usr/bin/env python3
"""RESEARCH_ONLY: fixed 500-stock BLUE20 event ledger x 60/112/224 SMA structure.

Rebuild technical indicators from SHA256-verified daily OHLCV; no event CSV
can be used to invent missing historic chart bars. D0 and GC are distinct
point-in-time snapshots. Outcome measures are never passed to classifiers.
"""
from __future__ import annotations
import argparse, hashlib, html, json, math
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
WINDOW=20
MA=(5,20,60,112,224)
CANDIDATES=[
    ('006220','2024-01-24','UPPER','TP','제주은행'),
    ('099430','2024-11-20','UPPER','SL','099430'),
    ('000400','2023-09-11','UPPER','TP','롯데손해보험'),
    ('000250','2023-08-01','UPPER','SL','000250'),
    ('025900','2023-08-09','LOWER','TP','025900'),
    ('101730','2023-09-12','LOWER','SL','101730'),
    ('137310','2024-07-31','LOWER','TP','137310'),
    ('950130','2024-07-17','LOWER','SL','엑세스바이오'),
]


def sha256(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for block in iter(lambda:f.read(1<<20),b''):h.update(block)
    return h.hexdigest()


def load_events(path):
    d=pd.read_csv(path,compression='infer',dtype={'code':str},low_memory=False)
    needed={'code','date','band','label','rsi_mode','gc_status','gc_offset','gc_date',
            'GC_NEXT_OPEN_entry_date','GC_NEXT_OPEN_entry_price','GC_NEXT_OPEN_exit_reason',
            'GC_NEXT_OPEN_exit_date','GC_NEXT_OPEN_net_pct','GC_NEXT_OPEN_mfe_pct','GC_NEXT_OPEN_mae_pct'}
    if not needed.issubset(d.columns):raise ValueError('EVENT_SCHEMA_MISSING '+str(needed-set(d.columns)))
    d.code=d.code.str.zfill(6)
    keep=(d.rsi_mode.eq('WILDER_SMA9') & d.label.eq('CLEAR_BODY') &
          d.gc_status.eq('GC_OBSERVED') & d.gc_offset.between(0,10))
    d=d.loc[keep].copy()
    if d.duplicated(['code','date','band','label']).any():
        raise ValueError('DUPLICATE_EVENT_ID')
    if len(d)!=1804:raise ValueError(f'REFERENCE_1804_MISMATCH got={len(d)}')
    return d


def load_manifest(path, codes):
    mpath=Path(path);m=pd.read_csv(mpath,dtype=str).fillna('')
    if len(m)!=500 or not {'code','path','sha256'}.issubset(m):
        raise ValueError('EXACT_PINNED_500_REQUIRED')
    if m.code.duplicated().any():raise ValueError('DUPLICATE_MANIFEST_CODE')
    frames={};hashes={}
    for row in m.itertuples(index=False):
        if row.code not in codes:continue
        rel=Path(row.path)
        if rel.is_absolute() or '..' in rel.parts:raise ValueError('UNSAFE_MANIFEST_PATH')
        f=(mpath.parent/rel).resolve()
        if not f.is_file() or not f.is_relative_to(mpath.parent.resolve()):
            raise ValueError('PINNED_PRICE_NOT_FOUND '+row.code)
        h=sha256(f)
        if h.lower()!=row.sha256.lower():raise ValueError('PINNED_PRICE_HASH_MISMATCH '+row.code)
        d=pd.read_csv(f,dtype={'code':str,'date':str})
        d.code=d.code.str.zfill(6)
        required={'code','date','open','high','low','close','volume','amount'}
        if not required.issubset(d):raise ValueError('PRICE_SCHEMA_MISSING '+row.code)
        if not d.code.eq(row.code).all() or d.date.duplicated().any() or not d.date.is_monotonic_increasing:
            raise ValueError('PRICE_IDENTITY_DATE_ERROR '+row.code)
        if d[['open','high','low','close']].le(0).any().any():raise ValueError('BAD_PRICE '+row.code)
        frames[row.code]=calc_indicators(d.reset_index(drop=True));hashes[row.code]=h
    if set(frames)!=set(codes):raise ValueError('MISSING_REQUIRED_STOCKS')
    return frames,hashes


def calc_indicators(d):
    d=d.copy()
    for n in MA:
        d[f'ma{n}']=d.close.rolling(n,min_periods=n).mean()
        for lag in (5,10,20):
            d[f'ma{n}_slope{lag}']=100*(d[f'ma{n}']/d[f'ma{n}'].shift(lag)-1)
    # gap contraction measures the absolute 60-vs-112 spread, not a cross.
    d['gap_60_112_pct']=100*abs(d.ma60/d.ma112-1)
    d['gap_60_112_converging20']=d['gap_60_112_pct']<d['gap_60_112_pct'].shift(20)
    d['gap_112_224_pct']=100*abs(d.ma112/d.ma224-1)
    d['break_over_ma60']=d.close.gt(d.ma60)&d.close.shift(1).le(d.ma60.shift(1))
    d['break_over_ma112']=d.close.gt(d.ma112)&d.close.shift(1).le(d.ma112.shift(1))
    d['ma60_112_bullcross']=d.ma60.gt(d.ma112)&d.ma60.shift(1).le(d.ma112.shift(1))
    # Archive's frozen BB convention: SHIFT of 20/2 band by 26 bars.
    avg=d.close.rolling(20,min_periods=20).mean()
    stdev=d.close.rolling(20,min_periods=20).std(ddof=0)
    d['bb20_upper_shift26']=(avg+2*stdev).shift(26)
    d['bb20_lower_shift26']=(avg-2*stdev).shift(26)
    # Wilder14 RSI and RSI SMA9 independent PIT recomputation.
    p=d.close.to_numpy(float);rsi=np.full(len(d),np.nan)
    if len(p)>14:
        delta=np.diff(p);g=np.maximum(delta,0);l=np.maximum(-delta,0)
        ag=g[:14].mean();al=l[:14].mean()
        for i in range(14,len(p)):
            if i>14:
                ag=(13*ag+g[i-1])/14;al=(13*al+l[i-1])/14
            rsi[i]=50 if ag==0 and al==0 else 100 if al==0 else 100-100/(1+ag/al)
    d['rsi_wilder14']=rsi
    d['rsi_signal9']=pd.Series(rsi).rolling(9,min_periods=9).mean()
    lo=d.low.rolling(14,min_periods=14).min();hi=d.high.rolling(14,min_periods=14).max()
    fast=(100*(d.close-lo)/(hi-lo)).where(hi.ne(lo),50).clip(0,100)
    d['slow_k']=fast.rolling(3,min_periods=3).mean()
    d['slow_d']=d.slow_k.rolling(3,min_periods=3).mean()
    return d


def characterize(row):
    """Only uses known close and historical MA states at the specified date."""
    ma60,ma112,ma224=(row.get('ma60'),row.get('ma112'),row.get('ma224'))
    if any(pd.isna(x) or x<=0 for x in (ma60,ma112,ma224)):
        return dict(alignment='INSUFFICIENT_224_HISTORY',ma60_vs112='UNKNOWN',ma112_vs224='UNKNOWN',
                    price_position='UNKNOWN',reversal_stage='UNKNOWN')
    close=float(row['close']);ma60=float(ma60);ma112=float(ma112);ma224=float(ma224)
    align=('MA_BULL_60_112_224' if ma60>ma112>ma224
           else 'MA_INVERSE_60_112_224' if ma60<ma112<ma224
           else 'MA_EARLY_60_UP_OVER_112_BELOW_224' if ma60>ma112 and ma112<ma224
           else 'MA_OTHER_MIXED')
    stage=('PRICE_ABOVE_60_112' if close>ma60 and close>ma112
           else 'PRICE_ABOVE_60_BELOW_112' if close>ma60 and close<ma112
           else 'PRICE_BELOW_60_ABOVE_112' if close<ma60 and close>ma112
           else 'PRICE_BELOW_60_112')
    return dict(alignment=align,ma60_vs112='ABOVE' if ma60>ma112 else 'BELOW',
                ma112_vs224='ABOVE' if ma112>ma224 else 'BELOW',
                price_position=stage,reversal_stage=('EARLY_RECLAIM_60' if close>ma60 and ma60<ma112 else 'OTHER'))


def flatten_pit(evt,bar,side):
    result={'code':evt.code,'date':evt.date,'band':evt.band}
    result.update({f'{side}_{k}':v for k,v in characterize(bar).items()})
    for n in (60,112,224):
        m=bar[f'ma{n}'];result[f'{side}_ma{n}']=m
        result[f'{side}_close_over_ma{n}']=float(bar.close/m-1)*100 if pd.notna(m) and m>0 else np.nan
        for lag in (5,10,20):result[f'{side}_ma{n}_slope{lag}']=bar[f'ma{n}_slope{lag}']
    for k in ['gap_60_112_pct','gap_60_112_converging20','gap_112_224_pct','break_over_ma60','break_over_ma112','ma60_112_bullcross']:
        result[f'{side}_{k}']=bar[k]
    return result


def enrich_events(events,frames):
    out=[]
    for e in events.itertuples(index=False):
        d=frames[e.code];idx=pd.Series(d.index,index=d.date)
        if e.date not in idx or e.gc_date not in idx or e.GC_NEXT_OPEN_entry_date not in idx:
            raise ValueError('EVENT_PRICE_DATES_MISSING '+e.code+' '+e.date)
        a=int(idx[e.date]);b=int(idx[e.gc_date]);j=int(idx[e.GC_NEXT_OPEN_entry_date])
        if not (a<=b<j):raise ValueError('EVENT_SEQUENCE_INVALID '+e.code+' '+e.date)
        if abs(float(d.close.iloc[a])-float(e.d0_close))>max(0.01,float(e.d0_close)*1e-7):
            raise ValueError('D0_CLOSE_MISMATCH '+e.code+' '+e.date)
        if abs(float(d.open.iloc[j])-float(e.GC_NEXT_OPEN_entry_price))>max(0.01,float(e.GC_NEXT_OPEN_entry_price)*1e-7):
            raise ValueError('ENTRY_OPEN_MISMATCH '+e.code+' '+e.date)
        r={'code':e.code,'name':'','date':e.date,'year':int(e.year),'band':e.band,
           'gc_date':e.gc_date,'gc_offset':int(e.gc_offset),
           'entry_date':e.GC_NEXT_OPEN_entry_date,'entry_price':e.GC_NEXT_OPEN_entry_price,
           'exit_date':e.GC_NEXT_OPEN_exit_date,'exit_reason':e.GC_NEXT_OPEN_exit_reason,
           'net_pct':e.GC_NEXT_OPEN_net_pct,'mfe_pct':e.GC_NEXT_OPEN_mfe_pct,
           'mae_pct':e.GC_NEXT_OPEN_mae_pct,'wave_state':e.wave_state,
           'market_regime':e.gc_market_regime,'stoch_state':e.stoch_state_gc,
           'd0_amount_ratio':e.d0_amount_ratio,
           'success_tp':str(e.GC_NEXT_OPEN_exit_reason).startswith('TP'),
           'failed_sl':str(e.GC_NEXT_OPEN_exit_reason).startswith('SL')}
        r.update(flatten_pit(e,d.iloc[a],'d0'))
        r.update(flatten_pit(e,d.iloc[b],'gc'))
        out.append(r)
    return pd.DataFrame(out)


def stats(sub):
    n=len(sub);val=sub.net_pct
    return {'n':n,'stocks':sub.code.nunique(),'n_2023':sum(sub.year==2023),'n_2024':sum(sub.year==2024),
            'avg_net_pct':round(float(val.mean()),4),
            'med_net_pct':round(float(val.median()),4),
            'tp_rate_pct':round(float(sub.success_tp.mean()*100),3),
            'sl_rate_pct':round(float(sub.failed_sl.mean()*100),3),
            'avg_mfe_pct':round(float(sub.mfe_pct.mean()),4),
            'avg_mae_pct':round(float(sub.mae_pct.mean()),4),
            'research_screen_n30_stocks15_eachyear10':bool(n>=30 and sub.code.nunique()>=15 and sum(sub.year==2023)>=10 and sum(sub.year==2024)>=10)}


def group_summary(df):
    rows=[]
    features=['alignment','price_position','reversal_stage',
              'gap_60_112_converging20','break_over_ma60','break_over_ma112']
    for phase in ['d0','gc']:
        for n in (60,112,224):
            for lag in (5,10,20):
                f=f'{phase}_ma{n}_slope{lag}'
                # Slope +/-0.1% over horizon is a *predeclared descriptive bin*, not optimized.
                df[f'{f}_group']=np.select([df[f].gt(0.1),df[f].lt(-0.1)],['UP','DOWN'],default='FLAT_OR_MISSING')
                features.append(f'ma{n}_slope{lag}_group') if False else None
        cols=[f'{phase}_{k}' for k in features]+[f'{phase}_ma{n}_slope{lag}_group' for n in (60,112,224) for lag in (5,10,20)]
        for feature in cols:
            for band,stock in df.groupby('band'):
                for value,sub in stock.groupby(feature,dropna=False):
                    rows.append({'phase':phase,'band':band,'feature':feature,'value':str(value),**stats(sub)})
    return pd.DataFrame(rows)


def yearly_summary(df):
    base=group_summary(df)
    # Group result by distinct 2023/2024 year, preserving the phase and signal-basis.
    rows=[]
    for yr,sub in df.groupby('year'):
        part=group_summary(sub)
        part.insert(0,'year',int(yr));rows.append(part)
    return pd.concat(rows,ignore_index=True) if rows else pd.DataFrame()


def core_tables(df):
    rows=[]
    keys=[['band'],['band','gc_alignment'],['band','d0_alignment'],
          ['band','gc_price_position'],['band','gc_ma60_slope10_group'],
          ['band','gc_ma112_slope20_group'],['band','gc_ma224_slope20_group'],
          ['band','gc_gap_60_112_converging20'],
          ['band','wave_state','gc_alignment'],
          ['band','market_regime','gc_alignment'],
          ['band','stoch_state','gc_alignment'],
          ['band','gc_alignment','gc_ma60_slope10_group','gc_ma112_slope20_group']]
    for cols in keys:
        for group,sub in df.groupby(cols,dropna=False):
            if not isinstance(group,tuple):group=(group,)
            rows.append({'segment':' × '.join(cols),'value':' × '.join(str(x) for x in group),**stats(sub)})
    return pd.DataFrame(rows)


def select_cases(df):
    used=set();selected=[]
    for code,date,band,typ,name in CANDIDATES:
        q=df[(df.code==code)&(df.date==date)&(df.band==band)]
        if len(q)!=1:continue
        e=q.iloc[0]
        if typ=='TP' and not bool(e.success_tp):continue
        if typ=='SL' and not bool(e.failed_sl):continue
        if (code,date,band) not in used:
            z=e.copy();z['name']=name;z['case_type']='SUCCESS_TP' if typ=='TP' else 'FAIL_SL'
            selected.append(z);used.add((code,date,band))
    # Extra non-extreme typical cases by the median MFE of each TP/SL group.
    # Retrospective educational examples, NEVER interpreted as a success predictor.
    for year,band in [(2023,'UPPER'),(2024,'LOWER')]:
        for typ in ('TP','SL'):
            q=df[(df.year==year)&(df.band==band)]
            q=q[q.success_tp] if typ=='TP' else q[q.failed_sl]
            if q.empty:continue
            middle=float(q.mfe_pct.median())
            q=q.assign(dist_from_typical=abs(q.mfe_pct-middle)).sort_values(['dist_from_typical','code','date'])
            for _,e in q.iterrows():
                key=(e.code,e.date,e.band)
                if key in used:continue
                z=e.copy();z['name']=e.code;z['case_type']='TYPICAL_TP' if typ=='TP' else 'TYPICAL_SL'
                selected.append(z);used.add(key)
                break
    return pd.DataFrame(selected)


def draw_chart(one,frame,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.patches import Rectangle
    d=frame.copy();idx=pd.Series(d.index,index=d.date)
    signal=int(idx[one.date]);right=min(len(d)-1,signal+20)
    left=max(0,signal-125)
    show=d.iloc[left:right+1].copy()
    xs=np.arange(left,right+1)
    fig,(ax,vol,osc)=plt.subplots(3,1,figsize=(15,9),sharex=True,
                                 gridspec_kw={'height_ratios':[5,1.2,1.7],'hspace':0.06})
    # Complete actual historical OHLC daily candles, not interpolated prices.
    for x,r in zip(xs,show.itertuples(index=False)):
        c='#d13a46' if r.close>=r.open else '#3379b7'
        ax.vlines(x,r.low,r.high,color=c,lw=0.65,alpha=.85)
        y=min(r.open,r.close);h=max(abs(r.close-r.open),r.close*.0008)
        ax.add_patch(Rectangle((x-.32,y),.64,h,facecolor=c,edgecolor=c,linewidth=.3))
    for n,col in [(5,'#8b5cf6'),(20,'#e59a28'),(60,'#00a0a0'),(112,'#a55bb9'),(224,'#4f5967')]:
        ax.plot(xs,show[f'ma{n}'],lw=1.55 if n in [60,112,224] else .8,
                color=col,label=f'MA{n}',alpha=.95)
    # Render BOTH user-identified price guides, not just the crossed line.
    ax.plot(xs,show['bb20_upper_shift26'],color='#df8b21',linestyle='--',lw=1.35,label='Orange UPPER shifted BB')
    ax.plot(xs,show['bb20_lower_shift26'],color='#2f74d4',linestyle='--',lw=1.35,label='Blue LOWER shifted BB')
    # Volume/turnover/candle annotations below are END-OF-DAY descriptions.
    from post_entry_flow import add_bar_features
    annotated=add_bar_features(d)
    ss=annotated.iloc[left:right+1]
    true_signal=ss.index>=eni if 'eni' in locals() else ss.index>=int(idx[one.entry_date])
    for x,candle,flow,after in zip(xs,ss.candle_shape,ss.postentry_flow_label,true_signal):
        if not after:continue
        z=annotated.iloc[x]
        if str(candle)=='LONG_LOWER_WICK':
            ax.scatter([x],[z.low],color='#1a996e',marker='^',s=50,zorder=9)
        if str(flow)=='HEAVY_DOWN_PROXY':
            ax.scatter([x],[z.high],color='#ad2d31',marker='v',s=48,zorder=9)
        if bool(z.upper_body_break):
            ax.scatter([x],[z.close],color='#e48d1d',marker='*',s=85,zorder=10)
    gci=int(idx[one.gc_date]);eni=int(idx[one.entry_date])
    axi=[(signal,'Breakout D0','#3064d4'),(gci,'RSI GC','#12905c'),(eni,'Entry next open','#df9800')]
    for val,label,col in axi:
        ax.axvline(val,lw=1.0,color=col,alpha=.75,ls='--')
    # Mark prior swing peak and pullback trough (descriptive, based on past bars).
    pkleft=max(0,signal-35);pkright=max(0,signal-2)
    if pkright>pkleft:
        peak=d.iloc[pkleft:pkright].high.idxmax()
        afterpeak=d.iloc[peak+1:signal]
        if not afterpeak.empty:
            trough=afterpeak.low.idxmin()
            ax.scatter([peak],[d.high.iloc[peak]],marker='v',color='#b56b22',s=65,zorder=7,label='Prior peak')
            ax.scatter([trough],[d.low.iloc[trough]],marker='^',color='#209b80',s=65,zorder=7,label='Pullback low')
    if isinstance(one.exit_date,str) and one.exit_date in idx:
        exi=int(idx[one.exit_date]);ax.axvline(exi,lw=1.1,color='#ba3232',alpha=.8,ls=':')
    ax.axhline(float(one.entry_price),color='#df9800',lw=.7,ls=':',alpha=.5)
    vc=np.where(show.close>=show.open,'#d13a46','#3379b7')
    vol.bar(xs,show.volume/1e6,color=vc,width=.62,alpha=.75)
    vol.set_ylabel('Volume (M)',fontsize=9)
    amtaxis=vol.twinx()
    amtaxis.plot(xs,ss.amount_ratio20,color='#a85e2a',lw=.85,alpha=.85,label='Turnover / prior20')
    amtaxis.axhline(1,color='#a85e2a',lw=.5,ls=':',alpha=.6)
    amtaxis.set_ylabel('Turnover x',fontsize=8,color='#a85e2a')
    amtaxis.set_ylim(0,min(8,max(2.2,float(ss.amount_ratio20.replace([np.inf,-np.inf],np.nan).quantile(.98)*1.1 or 2.2))))
    osc.plot(xs,show.rsi_wilder14,color='#9b5fca',lw=1.15,label='RSI Wilder14')
    osc.plot(xs,show.rsi_signal9,color='#d89b35',lw=1.0,label='RSI SMA9')
    osc.plot(xs,show.slow_k,color='#2f94a2',lw=.95,label='Slow K 14,3,3')
    osc.plot(xs,show.slow_d,color='#d1473c',lw=.95,label='Slow D 14,3,3')
    osc.axhline(20,color='grey',lw=.6,ls=':');osc.axhline(80,color='grey',lw=.6,ls=':')
    osc.set_ylim(-5,105);osc.legend(loc='upper left',fontsize=8,ncol=4)
    for panel in (ax,vol,osc):panel.grid(alpha=.15)
    shortlabels=[];ticks=[]
    for x in range(left,right+1):
        if (x-left)%15==0 or x==signal:
            ticks.append(x);shortlabels.append(d.date.iloc[x][5:])
    osc.set_xticks(ticks,shortlabels,rotation=45,ha='right',fontsize=9)
    ax.legend(loc='upper left',ncol=6,fontsize=9)
    tp='TP' if one.success_tp else 'SL' if one.failed_sl else 'TIME'
    ax.set_title((f'Stock {one.code} | {one.band} D0 {one.date} | RSI GC {one.gc_date} (D+{one.gc_offset})\n'
                  f'{tp} {one.net_pct:+.2f}%  |  D+20 MFE {one.mfe_pct:+.2f}%  MAE {one.mae_pct:+.2f}%  |  '
                  f'GC 60-slope10 {one.gc_ma60_slope10:+.2f}%  112-slope20 {one.gc_ma112_slope20:+.2f}%'),fontsize=12)
    ax.set_ylabel('Price (KRW)')
    fig.text(.5,.006,'Source: R118 SHA256-pinned 2022-2024 daily OHLCV; 2023/24 archive. Outcome uses stop-first D+20 rule.',
             ha='center',fontsize=8,color='#56616c')
    fig.savefig(out,dpi=145,bbox_inches='tight',facecolor='white');plt.close(fig)


def report_html(cases,statsfile,charts,diary=None):
    def h(x):return html.escape(str(x))
    rows=[]
    for e in cases.itertuples(index=False):
        fn=f'{e.code}_{e.date}_{e.band}.png'
        snippet=''
        if diary is not None:
            sub=diary[(diary.code==e.code)&(diary.date==e.date)&(diary.band==e.band)].head(22)
            if len(sub):
                htmlrows=[]
                for bar in sub.itertuples(index=False):
                    ratio=h(f'{bar.amount_ratio20:.2f}' if pd.notna(bar.amount_ratio20) else '?')
                    flag=' (기존 매매 종료 뒤 관찰)' if bar.after_baseline_exit_observation_only else ''
                    htmlrows.append('<tr><td>'+h(bar.bar_date)+'</td><td>'+h(bar.candle_shape)+'</td><td>'+ratio+'x</td><td>'+h(bar.flow_label)+flag+'</td></tr>')
                snippet='<h3>진입 후 일봉 캔들·거래대금 경로 (종가 확정 기준)</h3><div style="overflow:auto"><table style="border-collapse:collapse;width:100%"><thead><tr><th>날짜</th><th>캔들</th><th>거래대금/전20일</th><th>관찰 지표</th></tr></thead><tbody>'+''.join(htmlrows)+'</tbody></table></div>'
        rows.append('<article class="card"><h2>'+h(e.name)+' '+h(e.code)+' — '+h(e.date)+' '+h(e.band)+'</h2>'
            +'<p><strong>'+h(e.case_type)+'</strong> | GC '+h(e.gc_date)+' | 순손익 '+h(f'{e.net_pct:+.2f}%')
            +' | 고점 MFE '+h(f'{e.mfe_pct:+.2f}%')+'</p>'
            +'<p>GC MA 배열: '+h(e.gc_alignment)+' · 60일 기울기(10일): '+h(f'{e.gc_ma60_slope10:+.2f}%')
            +' · 112일 기울기(20일): '+h(f'{e.gc_ma112_slope20:+.2f}%')+'</p>'
            +f'<img src="charts/{fn}" alt="Verified daily OHLCV with MA5 MA20 MA60 MA112 MA224 for {h(e.code)} {h(e.date)}" loading="lazy"/>'+snippet+'</article>')
    page='''<!doctype html><html lang="ko"><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Stock-Hunter MA + EOD candle and turnover comparison</title><style>body{font:15px/1.65 -apple-system,BlinkMacSystemFont,'Malgun Gothic',sans-serif;background:#eef3f7;color:#192b40;max-width:1250px;margin:auto;padding:24px}h1{font-size:26px}.card{background:#fff;border:1px solid #dbe5ec;border-radius:18px;padding:18px;margin:18px 0;box-shadow:0 3px 12px #16344a12}.card img{display:block;width:100%;height:auto;border-radius:10px}p{margin:7px 0 12px}aside{background:#fff4df;padding:14px;border-radius:12px}th,td{border-bottom:1px solid #ddd;padding:6px;text-align:left;font-size:12px}th{background:#e6eef6}</style><h1>이평선·파란선·주황선 + 진입후 캔들·거래대금</h1><aside>실제 2022~2024 일봉 데이터를 SHA 검증한 뒤 재계산한 차트입니다. 캔들/거래대금 신호는 해당일 종가 확정 뒤에만 알 수 있습니다. 거래대금은 외국인/기관 실매수량이 아니라 단순 거래활동의 대리 지표입니다. -5% 당일 캔들로 당일 손절을 취소하지 않습니다. 손절 뒤의 캔들은 미래 관찰용입니다. 실전 성과나 독립표본 검증은 완료되지 않았습니다.</aside>'''+''.join(rows)+'</html>'
    return page


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--events',required=True)
    p.add_argument('--manifest',required=True)
    p.add_argument('--out',required=True)
    p.add_argument('--investor-pit',default=None,help='Optional archived PIT investor net buy CSV (not inferred)')
    x=p.parse_args(argv);out=Path(x.out);out.mkdir(parents=True,exist_ok=True)
    state={'research_only':True,'success':False,'stage':'INITIAL','oos':False,'production_ready':False}
    def save(): (out/'RUN_STAGE.json').write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8')
    save()
    try:
        e=load_events(x.events);state['stage']='VERIFYING_500_MANIFEST';save()
        frames,hashes=load_manifest(x.manifest,set(e.code))
        state['stage']='ENRICHING_AT_D0_AND_GC';save()
        enriched=enrich_events(e,frames)
        enriched.to_csv(out/'01_ALL_1804_MA_EVENT_FEATURES.csv.gz',index=False,compression='gzip')
        summary=group_summary(enriched)
        summary.to_csv(out/'02_MA_FEATURE_OUTCOME_GROUPS.csv',index=False)
        yearly_summary(enriched).to_csv(out/'02b_MA_FEATURE_BY_YEAR.csv',index=False)
        combos=core_tables(enriched)
        combos.to_csv(out/'03_MA_60_112_224_INTERACTION.csv',index=False)
        cases=select_cases(enriched)
        cases.to_csv(out/'04_ACTUAL_SUCCESS_FAILURE_CHART_CASES.csv',index=False)
        state['stage']='STUDY_POST_ENTRY_OHLCV_AND_REPLAY_STOPS';save()
        from post_entry_flow import study_post_entry
        flowaudit,flowdiary=study_post_entry(enriched,frames,out,investor_pit_path=x.investor_pit)
        state['post_entry_rows']=flowaudit['post_entry_observation_rows'];save()
        state['stage']='RENDERING_REAL_CANDLE_CHARTS';save()
        chartdir=out/'charts';chartdir.mkdir(exist_ok=True)
        for e in cases.itertuples(index=False):
            draw_chart(e,frames[e.code],chartdir/f'{e.code}_{e.date}_{e.band}.png')
        (out/'05_REAL_OHLCV_CHART_GALLERY.html').write_text(report_html(cases,summary,chartdir,flowdiary),encoding='utf-8')
        audit={'source':'R118_PINNED_2022_ASOF_500_2022_TO_2024', 'events_source_sha256':sha256(x.events),
               'manifest_source_sha256':sha256(x.manifest),'price_sha256_by_code':hashes,
               'primary_events':len(enriched),'stocks':int(enriched.code.nunique()),'drawn_charts':len(cases),
               'insufficient_224_history_d0':int((enriched.d0_alignment=='INSUFFICIENT_224_HISTORY').sum()),
               'insufficient_224_history_gc':int((enriched.gc_alignment=='INSUFFICIENT_224_HISTORY').sum()),
               'entry_prices_point_in_time_checked':True,'cross_formula_certified_against_HTS':False,
               'sample_charts_retrospectively_selected':True,'all_group_scans_exploratory':True,
               'missing_sector_theme_pit':True,'post_entry_audit':flowaudit,
               'production_ready':False}
        (out/'AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
        state.update(stage='COMPLETE_RESEARCH_ONLY',success=True,events=len(enriched),actual_charts=len(cases));save()
        print('MA_STRUCTURAL_CHART_RESEARCH_COMPLETE',json.dumps({k:v for k,v in audit.items() if k in ['primary_events','drawn_charts','insufficient_224_history_gc']}))
        return 0
    except Exception as err:
        state.update(stage='FAILED_CLOSED',error_type=type(err).__name__,error=str(err));save();raise

if __name__=='__main__':raise SystemExit(main())
