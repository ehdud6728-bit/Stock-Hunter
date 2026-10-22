#!/usr/bin/env python3
"""RESEARCH ONLY: render original-KRX historical charts with no future prices/returns.

One deterministic sealed case per 66 historical main7 formula labels, where bars
exist. Chart titles contain only opaque review IDs; formula and PRE status are
stored in a separate sealed map. NEVER interpolate missing OHLCV/Amount.
"""
from __future__ import annotations
import argparse
import csv
import json
from pathlib import Path
import pandas as pd


def choose_cases(rows,limit=66):
    """Balance formula coverage; prefer actual PRE_TRUE and then available controls."""
    grouped={}
    for r in rows:
        idx=int(r['formula_index'])
        if str(r['KRX_asof_chart_bars_available'])!='1':continue
        grouped.setdefault(idx,[]).append(r)
    chosen=[]
    for i in range(66):
        rows=grouped.get(i,[])
        rows.sort(key=lambda r:(r['role_from_legacy_capture']!='PRE_TRUE_FROM_CAPTURE',r['review_id']))
        if rows:chosen.append(rows[0])
        if len(chosen)>=limit:break
    return chosen


def render(root,max_charts=66):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.ticker import MaxNLocator
    root=Path(root)
    mp=root/'formula66_blind_review_sealed_keys.csv'
    bars=root/'formula66_krx_blind_bars_to_signal_day_only.csv.gz'
    if not mp.is_file() or not bars.is_file():raise RuntimeError('BLIND_CASES_OR_ASOF_BARS_MISSING')
    with mp.open(newline='',encoding='utf-8-sig') as f:keys=list(csv.DictReader(f))
    selected=choose_cases(keys,limit=max_charts)
    needed={r['review_id'] for r in selected}
    # 326 * max260 = ~85k bars; no forward outcome is ever loaded here.
    ds=pd.read_csv(bars,compression='gzip',dtype={'review_id':str,'date':str,'signal_date':str,'code':str},low_memory=False)
    if 'known_at_signal' not in ds or not ds['known_at_signal'].eq(1).all():raise RuntimeError('LOOKAHEAD_BAR_FLAG_NOT_SAFE')
    ds=ds[ds['review_id'].isin(needed)]
    if (ds['date']>ds['signal_date']).any():raise RuntimeError('FUTURE_BAR_IN_BLIND_CHART')
    out=root/'charts_blind_KRX_asof_no_future';out.mkdir(parents=True,exist_ok=True)
    drawn=[]
    for r in selected:
        g=ds[ds['review_id']==r['review_id']].sort_values('date')
        if g.empty:continue
        x=range(len(g));o=g['open'].to_numpy();h=g['high'].to_numpy();l=g['low'].to_numpy();c=g['close'].to_numpy()
        amt=g['actual_amount_KRW'].to_numpy()/1e8
        fig,(ax,sub)=plt.subplots(2,1,figsize=(11,6.2),height_ratios=[3,1],sharex=True,layout='constrained')
        inks=['#bf4637' if x else '#3978b6' for x in (c>=o)]
        xx=list(range(len(g)))
        ax.vlines(xx,l,h,color=inks,linewidth=.6)
        ax.vlines(xx,np.minimum(o,c),np.maximum(o,c),color=inks,linewidth=2.4)
        av=g['close'].rolling(224,min_periods=224).mean()
        ax.plot(list(x),av.to_numpy(),color='#7343b5',linewidth=1.4,label='MA224 (when 224 rows available)')
        ax.legend(loc='upper left',fontsize=8)
        ax.set_title('BLIND KRX AS-OF SIGNAL-DAY CHART  |  '+r['review_id'],fontsize=10)
        ax.set_ylabel('Unadjusted KRX price (KRW)',fontsize=9)
        ax.grid(alpha=.2)
        sub.bar(list(x),amt,color='#777777',width=.82)
        sub.set_ylabel('Actual amount\n(100m KRW)',fontsize=8)
        sub.grid(alpha=.12)
        sub.xaxis.set_major_locator(MaxNLocator(nbins=9,integer=True))
        ticks=sub.get_xticks();labels=[]
        for t in ticks:
            j=int(round(t));labels.append(str(g.iloc[j]['date']) if 0<=j<len(g) else '')
        sub.set_xticks(ticks,labels,fontsize=7,rotation=30)
        fig.savefig(out/(r['review_id']+'.png'),dpi=115)
        plt.close(fig)
        drawn.append({'review_id':r['review_id'],'filename':r['review_id']+'.png','bars_used':len(g),'pattern_label_hidden':1,'outcomes_hidden':1})
    if len(drawn)!=len(selected):raise RuntimeError('SILENT_MISSING_BLIND_CHART')
    with (root/'formula66_blind_chart_png_index.csv').open('w',newline='',encoding='utf-8-sig') as f:
        writer=csv.DictWriter(f,fieldnames=['review_id','filename','bars_used','pattern_label_hidden','outcomes_hidden'])
        writer.writeheader();writer.writerows(drawn)
    print('FORMULA66_R1D1_BLIND_CHARTS',json.dumps({'charts':len(drawn),'selected_formula_coverage':len({int(r['formula_index']) for r in selected}),
                   'no_future':True,'strategy_ready':False}),flush=True)
    return drawn


def main():
    a=argparse.ArgumentParser();a.add_argument('--result-root',required=True);a.add_argument('--max-charts',type=int,default=66)
    p=a.parse_args()
    if not 1<=p.max_charts<=132:raise RuntimeError('INVALID_CHART_LIMIT')
    render(p.result_root,p.max_charts)

if __name__=='__main__':main()
