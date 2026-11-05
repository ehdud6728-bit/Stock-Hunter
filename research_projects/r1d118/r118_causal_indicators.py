#!/usr/bin/env python3
"""Causal candle/MA/Ichimoku and trading-rule library for research only.
Visible cloud at D is shifted *from D-26*, not future-looking D+26.
"""
import numpy as np
import pandas as pd
COST=0.25

def feat(df):
    s=df.copy()
    for c in ['open','high','low','close','amount','volume']:
        s[c]=pd.to_numeric(s[c],errors='raise')
    for n in (7,15,20,60,120,224):
        s[f'ma{n}']=s.close.rolling(n,min_periods=n).mean()
    s['ma20_slope5_pct']=100*(s.ma20/s.ma20.shift(5)-1)
    high9=s.high.rolling(9,min_periods=9).max();low9=s.low.rolling(9,min_periods=9).min()
    high26=s.high.rolling(26,min_periods=26).max();low26=s.low.rolling(26,min_periods=26).min()
    high52=s.high.rolling(52,min_periods=52).max();low52=s.low.rolling(52,min_periods=52).min()
    s['tenkan']=(high9+low9)/2
    s['kijun']=(high26+low26)/2
    # Important: cloud visible at D was calculated as of D-26 (no future leakage).
    s['cloudA_today']=((s.tenkan+s.kijun)/2).shift(26)
    s['cloudB_today']=((high52+low52)/2).shift(26)
    s['cloud_top']=s[['cloudA_today','cloudB_today']].max(axis=1).where(s[['cloudA_today','cloudB_today']].notna().all(axis=1))
    s['cloud_bottom']=s[['cloudA_today','cloudB_today']].min(axis=1).where(s[['cloudA_today','cloudB_today']].notna().all(axis=1))
    s['c3ago']=s.close.shift(3)
    s['c1ago']=s.close.shift(1)
    s['o1ago']=s.open.shift(1)
    s['h1ago']=s.high.shift(1)
    s['l1ago']=s.low.shift(1)
    s['max3prev']=s.high.shift(1).rolling(3,min_periods=3).max()
    s['3upcloses']=((s.close>s.close.shift(1)) & (s.close.shift(1)>s.close.shift(2)) & (s.close.shift(2)>s.close.shift(3)))
    s['3higherlows']=((s.low>s.low.shift(1)) & (s.low.shift(1)>s.low.shift(2)))
    s['engulf']=((s.close>s.open)&(s.close.shift(1)<s.open.shift(1))&(s.open<=s.close.shift(1))&(s.close>=s.open.shift(1)))
    s['bull_d']=(s.close>s.open)
    s['bullish_count3']=(s.close>s.open).astype(int)+(s.close.shift(1)>s.open.shift(1)).astype(int)+(s.close.shift(2)>s.open.shift(2)).astype(int)
    s['close_location']=(s.close-s.low)/(s.high-s.low).replace(0,np.nan)
    s['strong_candle']=((s.close>s.open)&(s.close_location>=.75)&((s.close-s.open)/(s.high-s.low).replace(0,np.nan)>=.40))
    s['prev1_high_break']=(s.close>s.h1ago)
    s['prev3_high_break']=(s.close>s.max3prev)
    s['above_ma20']=s.close>s.ma20
    s['above_ma60']=s.close>s.ma60
    s['above_ma224']=s.close>s.ma224
    s['ma20_rising']=s.ma20_slope5_pct>0
    s['ma20_gt_ma60']=s.ma20>s.ma60
    s['tenkan_gt_kijun']=s.tenkan>s.kijun
    s['close_above_kijun']=s.close>s.kijun
    s['ma7_gt_ma20']=s.ma7>s.ma20
    s['ma7_crossup_ma20']=(s.ma7>s.ma20)&(s.ma7.shift(1)<=s.ma20.shift(1))
    s['close_crossup_ma20']=(s.close>s.ma20)&(s.close.shift(1)<=s.ma20.shift(1))
    s['tk_bull_cross']=(s.tenkan>s.kijun)&(s.tenkan.shift(1)<=s.kijun.shift(1))
    s['cloud_bull']=s.cloudA_today>s.cloudB_today
    s['cloud_entry_up']=(s.close>s.cloud_top)&(s.close.shift(1)<=s.cloud_top.shift(1))
    s['cloud_position']=np.select([s.close>s.cloud_top,s.close<s.cloud_bottom],['ABOVE','BELOW'],default='INSIDE')
    s.loc[s.cloud_bottom.isna(),'cloud_position']='UNAVAILABLE'
    return s


def exit_path(s, i, tp, sl, max_hold=10, commission=COST):
    """D+1 open entry; orders active immediately after entry. Gaps D+2 onward executed at open;
    both hits inside the same daily bar: stop-first pessimistic (intraday order not known).
    tp/sl are gross trigger levels, costs are applied on realized exit.
    """
    ent=float(s.at[i+1,'open'])
    stop=ent*(1-sl/100);target=ent*(1+tp/100)
    for k in range(1,max_hold+1):
        op=float(s.at[i+k,'open']);low=float(s.at[i+k,'low']);high=float(s.at[i+k,'high']);cl=float(s.at[i+k,'close'])
        if k>1:
            if op<=stop:return {'exit_reason':'GAP_STOP','exit_day':k,'exit_net_pct':100*(op/ent-1)-commission,'intraday_ambiguous':False}
            if op>=target:return {'exit_reason':'GAP_TP','exit_day':k,'exit_net_pct':100*(op/ent-1)-commission,'intraday_ambiguous':False}
        st=low<=stop;tk=high>=target
        if st and tk:return {'exit_reason':'BOTH_STOP_FIRST','exit_day':k,'exit_net_pct':-sl-commission,'intraday_ambiguous':True}
        if st:return {'exit_reason':'STOP','exit_day':k,'exit_net_pct':-sl-commission,'intraday_ambiguous':False}
        if tk:return {'exit_reason':'TP','exit_day':k,'exit_net_pct':tp-commission,'intraday_ambiguous':False}
    return {'exit_reason':'TIME','exit_day':max_hold,'exit_net_pct':100*(float(s.at[i+max_hold,'close'])/ent-1)-commission,'intraday_ambiguous':False}

