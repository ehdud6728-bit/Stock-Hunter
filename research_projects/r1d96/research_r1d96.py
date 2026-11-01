"""R1-D96: POINT-IN-TIME automatic bi-directional trendline review.
Research-only. Candidate anchors are determined from confirmed pivots and OHLCV as of the requested date.
No manual hard-coded pivot dates in candidate discovery, score, or selection.
"""
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import json, hashlib, math
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
R=Path(__file__).resolve().parent
OUT=R/'results'
OUT.mkdir(exist_ok=True)
(OUT/'charts').mkdir(exist_ok=True)
COLORS={'RESISTANCE':'#B45309','SUPPORT':'#128679'}
plt.rcParams['axes.unicode_minus']=False
try:
    from matplotlib import font_manager
    font_path=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font_path.exists():
        font_manager.fontManager.addfont(str(font_path))
        plt.rcParams['font.family']='NanumGothic'
except Exception: pass

PIVOT_RIGHT=2

def load(code):
    a=pd.read_csv(R/'data'/f'{code}_KRX.csv',dtype={'code':str},parse_dates=['date'])
    a=a.sort_values('date').reset_index(drop=True)
    a['idx']=np.arange(len(a))
    for n in (112,224,448):a[f'MA{n}']=a.close.rolling(n,min_periods=n).mean()
    for n in (20,40):
        mean=a.close.rolling(n,min_periods=n).mean()
        a[f'ENV{n}_10_LO']=mean*.9
        if n==20:
            a['BB20_LO']=mean-2*a.close.rolling(n,min_periods=n).std(ddof=0)
            a['BB20_HI']=mean+2*a.close.rolling(n,min_periods=n).std(ddof=0)
    a['AMOUNT20']=a.amount.shift(1).rolling(20,min_periods=20).mean()
    return a

def pivot_candidates(d,asof_idx,radius=PIVOT_RIGHT):
    """Important: a pivot at index j exists at time t only if j+radius<=t."""
    records=[]
    assert 0 <= asof_idx < len(d)
    for typ,field in [('HIGH','close'),('LOW','low')]:
        vals=d[field].to_numpy(dtype=float)
        for i in range(radius,asof_idx-radius+1):
            window=vals[i-radius:i+radius+1]
            if len(window)!=2*radius+1:continue
            v=vals[i]
            if typ=='HIGH':valid=(v==np.max(window) and np.count_nonzero(window==v)==1)
            else:valid=(v==np.min(window) and np.count_nonzero(window==v)==1)
            if not valid:continue
            prior=d.iloc[max(0,i-12):i]
            if len(prior):
                prominence=((v/max(float(prior.low.min()),1)-1)*100 if typ=='HIGH'
                            else (float(prior.high.max())/max(v,1)-1)*100)
            else:prominence=0
            records.append(dict(kind=typ,idx=i,date=d.iloc[i].date.strftime('%Y-%m-%d'),value=float(v),
                                confirmed_idx=i+radius,confirmed_date=d.iloc[i+radius].date.strftime('%Y-%m-%d'),
                                local_prominence_pct=round(prominence,3)))
    return records

def calc_line(y1,y2,i,j,k):
    return y1+(y2-y1)*(k-i)/(j-i)

def generation(d,asof,lookback=135):
    """Generate all viable lines from 2 confirmed extrema, without manual seed anchors.
    A resistance connects descending CLOSE swing highs; support connects rising LOW swing lows.
    Each remains its OWN line, not a smoothed line through candles.
    """
    t=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0])
    piv=pivot_candidates(d,t)
    rows=[]
    close=float(d.iloc[t].close)
    for typ in ['HIGH','LOW']:
        p=[x for x in piv if x['kind']==typ and t-135<=x['idx']<=t-PIVOT_RIGHT]
        for b in p:
            j=b['idx']
            age=t-j
            if age>65:continue
            for a in p:
                i=a['idx']
                gap=j-i
                if gap<4 or gap>100:continue
                y1,y2=a['value'],b['value']
                delta=(y2/y1-1)*100
                if typ=='HIGH' and delta > -0.8:continue
                if typ=='LOW' and delta < 1:continue
                slope=(y2-y1)/gap
                now=calc_line(y1,y2,i,j,t)
                if now<=0:continue
                dist=(close/now-1)*100
                if abs(dist)>25:continue
                x=np.arange(i,j+1)
                fit=y1+slope*(x-i)
                v=d.iloc[i:j+1]
                if typ=='HIGH':
                    interior_pen=np.maximum(100*(v.close.to_numpy(dtype=float)[1:-1]/fit[1:-1]-1),0)
                    bad=float(interior_pen.max()) if len(interior_pen) else 0.0
                    recent_bias= -0.5 if age>35 else 0.0
                    # high resistance should still be overhead or recently broken, not deeply under price
                    active=dist <= 12
                else:
                    interior_pen=np.maximum(100*(fit[1:-1]/v.low.to_numpy(dtype=float)[1:-1]-1),0)
                    bad=float(interior_pen.max()) if len(interior_pen) else 0.0
                    recent_bias=0
                    active=dist >= -12
                if bad>2.5 or not active:continue
                tier='SMALL' if gap<=20 else 'MIDDLE'
                # Wick-driven false resistance: H2 formed after a crash and closed far below its high.
                # Penalties affect priority, not candidate existence; retain details for manual review.
                h2_upper_wick_pct=100*(float(d.iloc[j].high)/max(float(d.iloc[j].close),1)-1) if typ=='HIGH' else 0.0
                interior_close_low=float(d.iloc[i:j+1].close.min())
                inter_anchor_drawdown_pct=100*(interior_close_low/y1-1) if typ=='HIGH' else 0.0
                inter_anchor_largest_one_day_drop_pct=(float((d.close.iloc[i+1:j+1].to_numpy(dtype=float)/d.close.iloc[i:j].to_numpy(dtype=float)-1).min()*100) if typ=='HIGH' else 0.0)
                shock=False
                if typ=='LOW':
                    # Very sharp rebound support is a separate, risky shape; never delete just for steepness.
                    shock=100*slope/y1>1.6
                # Rank a natural local swing: recent second anchor, high silhouette fit, meaningful span,
                # price proximity, high prominence and moderate slope. Score is NOT expected return.
                prom=min(b['local_prominence_pct'],40)
                closeness = max(0, 12-abs(dist))
                recency = max(0, 50-age)/5
                gap_quality = min(gap,30)*.12
                # Only a NEW crossing may be a breakout signal; previously crossed lines remain references.
                first_cross_date=''
                if typ=='HIGH':
                    for scan_k in range(b['confirmed_idx']+1,t+1):
                        prev_line=calc_line(y1,y2,i,j,scan_k-1)
                        curr_line=calc_line(y1,y2,i,j,scan_k)
                        if (float(d.iloc[scan_k-1].close) <= prev_line*1.003
                                and float(d.iloc[scan_k].close)>curr_line*1.003):
                            first_cross_date=d.iloc[scan_k].date.strftime('%Y-%m-%d')
                            break
                score= 2.7*closeness+1.5*recency+gap_quality + .07*prom - 5*bad
                if typ=='HIGH':
                    # A huge upper wick on the second rebound high can make CLOSE-line inference dubious.
                    # A deep crash between H1 and H2 means their connection may cross two different waves.
                    score+=max(0,80-(t-i))*.12
                    score+=recent_bias
                    if h2_upper_wick_pct>=8:score-=25
                    elif h2_upper_wick_pct>=4:score-=8
                    if inter_anchor_largest_one_day_drop_pct<=-12:score-=17
                else:
                    # near horizontal most useful for stable supports, but allow steep lines explicitly
                    score-=max(0,100*slope/y1-1.5)*1.1
                rows.append(dict(kind='RESISTANCE' if typ=='HIGH' else 'SUPPORT',
                    tier=tier,h1=a['date'],h2=b['date'],h1_value=y1,h2_value=y2,
                    h1_idx=i,h2_idx=j,h2_confirm_date=b['confirmed_date'],h2_confirm_idx=b['confirmed_idx'],
                    asof=asof,age_bars=age,gap_bars=gap,line_asof=round(now,3),
                    close_asof=close,close_minus_line_pct=round(dist,3),
                    interior_pen_pct=round(bad,3),slope_pct_per_bar=round(100*slope/y1,3),
                    steep_postshock_support=shock,
                    h2_upper_wick_pct=round(h2_upper_wick_pct,3),
                    inter_anchor_drawdown_pct=round(inter_anchor_drawdown_pct,3),
                    inter_anchor_largest_one_day_drop_pct=round(inter_anchor_largest_one_day_drop_pct,3),
                    resistance_structure_warning=bool(typ=='HIGH' and (h2_upper_wick_pct>=8 or inter_anchor_largest_one_day_drop_pct<=-12)),
                    rank_score=round(score,4),
                    first_close_break_date=first_cross_date,
                    new_close_break_today=bool(first_cross_date==asof),
                    prior_close_break=bool(first_cross_date and first_cross_date<asof),
                    third_reaction_validated=False,  # needs 3rd independently confirmed pivot; never assume
                    line_id=f'{typ}|{a["date"]}|{b["date"]}'))
    rows.sort(key=lambda z: (-z['rank_score'],z['kind'],z['h2']))
    return rows,piv

def choose(d,asof):
    rows,piv=generation(d,asof)
    # distinct classes and supports. Show 2 per tier for transparent comparison.
    selected=[]
    for typ in ['RESISTANCE','SUPPORT']:
        for tier in ['SMALL','MIDDLE']:
            candidates=[r for r in rows if r['kind']==typ and r['tier']==tier and r['rank_score']>=16]
            selected.extend(candidates[:2])
    return rows,piv,selected

def fmt_date(s):return pd.Timestamp(s).strftime('%m/%d')

def chart(d,asof,ending,selected,code,tag):
    stop=int(np.flatnonzero(d.date.eq(pd.Timestamp(ending)))[0]);start=max(0,stop-(78 if code=="053800" else 70))
    v=d.iloc[start:stop+1].copy()
    fig,(ax,axvol)=plt.subplots(2,1,figsize=(15.5,8.7),sharex=True,
            gridspec_kw={'height_ratios':[3.35,1.1]})
    for r in v.itertuples():
        col='#C94545' if r.close>=r.open else '#276CB0'
        ax.vlines(r.idx,r.low,r.high,color=col,lw=1.05,zorder=3)
        ax.add_patch(Rectangle((r.idx-.30,min(r.open,r.close)),.60,max(1,abs(r.close-r.open)),
                           facecolor=col,edgecolor=col,lw=.4,zorder=3))
    # all relevant indicators, same original KRX basis
    for n,c in [(112,'#257E35'),(224,'#151C25'),(448,'#9B3CC8')]:
        ax.plot(v.idx,v[f'MA{n}'],color=c,alpha=.6,lw=1.5,label=f'MA{n}')
    ax.plot(v.idx,v['BB20_LO'],color='#218CAD',alpha=.75,lw=1.1,label='BB20 하단')
    ax.plot(v.idx,v['ENV20_10_LO'],color='#C49E21',alpha=.65,lw=1,ls=':',label='ENV20 -10%')
    ax.plot(v.idx,v['ENV40_10_LO'],color='#A17E11',alpha=.6,lw=1,ls='--',label='ENV40 -10%')
    # Present one top line per category/tier on charts, retain all ranked selections in CSV.
    display=[]
    seen=set()
    for item in selected:
        group=(item['kind'],item['tier'])
        if group not in seen:
            display.append(item);seen.add(group)
    for rank,r in enumerate(display):
        # FROZEN asof selection must not change after asof. For pre-asof visualization, show whole line after H1,
        # but NOT imply it was known before the H2 confirmation date.
        i,j=r['h1_idx'],r['h2_idx'];q=v[v.idx>=i]
        if len(q)==0:continue
        y=np.array([calc_line(r['h1_value'],r['h2_value'],i,j,x) for x in q.idx])
        opacity=.95
        col=COLORS[r['kind']]
        label=f'{"저항" if r["kind"]=="RESISTANCE" else "지지"} {r["tier"]} {fmt_date(r["h1"])}→{fmt_date(r["h2"])}'
        ax.plot(q.idx,y,ls='--' if r['kind']=='RESISTANCE' else '-.',color=col,
                lw=max(1.3,2.4-rank*.22),alpha=opacity,label=label)
        if True:
            ax.scatter([i,j],[r['h1_value'],r['h2_value']],marker='o',s=39,color=col,zorder=5)
    asi=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0]);
    ax.axvline(asi,color='#777777',ls=':',alpha=.7,lw=1.2,label='기준일(정보 제한)')
    if stop>asi:ax.axvspan(asi,stop,alpha=.045,color='#C09830')
    axvol.bar(v.idx,v.amount/1e8,width=.73,color=np.where(v.close>=v.open,'#C94545','#276CB0'))
    axvol.axvline(asi,color='#777777',ls=':',alpha=.7)
    ax.set_xlim(start-.5,stop+.5)
    ax.set_ylabel('KRX 원시가격 (원)');axvol.set_ylabel('거래대금(억원)')
    ax.set_title(f'{"안랩" if code=="053800" else "켐트로스"}({code}) | 자동 고점·저점 추세선 | 기준일 {asof} / 표시마감 {ending}',fontsize=13)
    ax.grid(alpha=.12);axvol.grid(alpha=.12)
    ax.legend(loc='upper left',fontsize=7.5,ncol=2)
    ticks=v.iloc[::max(1,len(v)//13)]
    axvol.set_xticks(ticks.idx);axvol.set_xticklabels(ticks.date.dt.strftime('%m/%d'),rotation=35)
    fig.tight_layout()
    fn=f'{code}_{tag}_AUTO_UPPER_LOWER_ASOF.png'
    fig.savefig(OUT/'charts'/fn,dpi=155,bbox_inches='tight');plt.close(fig)
    return fn

def daily_status(d,asof,selected,through):
    t0=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0]);t1=int(np.flatnonzero(d.date.eq(pd.Timestamp(through)))[0]);out=[]
    for r in selected:
        for k in range(t0,t1+1):
            v=d.iloc[k];line=calc_line(r['h1_value'],r['h2_value'],r['h1_idx'],r['h2_idx'],k)
            if r['kind']=='RESISTANCE':
                status=('ABOVE_POSSIBLE_FLIP' if v.close>line else 'BELOW_RESISTANCE')
                contact=bool(v.low<=line*1.01 and v.high>=line*.99)
            else:
                status=('SUPPORT_HOLD_CLOSE' if v.close>=line else 'SUPPORT_BREAK_CLOSE')
                contact=bool(v.low<=line*1.01 and v.high>=line*.99)
            out.append(dict(line_id=r['line_id'],tier=r['tier'],date=v.date.strftime('%Y-%m-%d'),
                            h2_confirm_date=r['h2_confirm_date'],available=bool(k>=r['h2_confirm_idx']),
                            line=round(line,2),low=float(v.low),high=float(v.high),close=float(v.close),
                            touch=contact,status=status,close_distance_pct=round(100*(v.close/line-1),3)))
    return out

def main():
    specs=[('053800','2024-10-17','2024-10-24','AHN'),
           ('220260','2024-08-19','2024-08-28','CHEM_BEFORE'),
           ('220260','2024-08-20','2024-08-28','CHEM_AFTER')]
    audit=[];piv_out=[];selection=[];daily=[]
    for code,asof,ending,tag in specs:
        d=load(code)
        rows,piv,selected=choose(d,asof)
        for x in rows:audit.append(dict(code=code,**x))
        for x in piv:piv_out.append(dict(code=code,asof=asof,**x))
        for x in selected:selection.append(dict(code=code,**x))
        for x in daily_status(d,asof,selected,ending):daily.append(dict(code=code,asof=asof,**x))
        chart(d,asof,asof,selected,code,tag+'_AT_DATE')
        chart(d,asof,ending,selected,code,tag+'_AFTER')
        print('\nCASE',code,asof,'number candidates',len(rows),'selection',len(selected))
        print(pd.DataFrame(selected)[['kind','tier','h1','h2','h2_confirm_date','gap_bars','interior_pen_pct','close_minus_line_pct','rank_score']].to_string(index=False) if selected else 'none')
    for name,data in [('R1D96_ALL_CANDIDATE_LINES.csv',audit),('R1D96_PIT_CONFIRMED_PIVOTS.csv',piv_out),
                      ('R1D96_AUTOMATIC_SELECTED_LINES.csv',selection),('R1D96_FROZEN_LINE_DAILY_STATUS.csv',daily)]:
        pd.DataFrame(data).to_csv(OUT/name,index=False,encoding='utf-8-sig')
    info={
        'research_only':True,'version':'R1D96_AUTO_BIDIRECTIONAL_2_STOCK_PIT',
        'cases':len(specs),'selected_lines':len(selection),'all_candidate_lines':len(audit),
        'at_least_2_right_bars_confirmed':True,'price_basis':'KRX ORIGINAL UNADJUSTED OHLCV',
        'not_yet_certified':['PIT full corporate actions','PIT market membership','out of sample accuracy','live search'],
        'definitions':{'HIGH':'close swing maxima','LOW':'intraday low swing minima',
                       'right_confirm_bars':PIVOT_RIGHT,'tiers':'gap <=20 SMALL, >20 MIDDLE',
                       'ranking':'price proximity, recency, gap, interior fits: research only'}}
    (OUT/'R1D96_AUDIT.json').write_text(json.dumps(info,indent=2,ensure_ascii=False),encoding='utf-8')

if __name__=='__main__': main()
