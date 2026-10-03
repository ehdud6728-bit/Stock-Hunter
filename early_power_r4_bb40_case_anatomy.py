#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""EARLY POWER R4 — S3 + BB40 squeeze 17-case path anatomy (RESEARCH_ONLY).

Consumes the frozen R3 S3 structure ledger and PIT marcap OHLCV. Reconstructs
signal-time/pre-signal descriptors only, then compares the already-frozen R2
outcome labels. No production selection/ranking/order changes and no fitted
threshold promotion.
"""
from __future__ import annotations
import argparse, json, re
from pathlib import Path
import numpy as np
import pandas as pd

REV='EARLY_POWER_R4_S3_BB40_CASE_ANATOMY_20261004'

def norm_code(v):
    s=re.sub(r'\D','',str(v or ''))
    return s[-6:].zfill(6) if s else ''

def load_marcap(paths):
    xs=[]
    for p in paths:
        x=pd.read_parquet(p)
        if 'Date' not in x.columns:x=x.reset_index()
        xs.append(x)
    x=pd.concat(xs,ignore_index=True,sort=False)
    x['Date']=pd.to_datetime(x['Date'],errors='coerce').dt.normalize()
    x['Code']=x['Code'].map(norm_code)
    for c in ['Open','High','Low','Close','Volume','Amount']:
        if c in x.columns:x[c]=pd.to_numeric(x[c],errors='coerce')
    x=x.dropna(subset=['Date','Code','Open','High','Low','Close'])
    x=x.sort_values(['Code','Date']).drop_duplicates(['Code','Date'],keep='last')
    return x

def add_signal_features(g):
    g=g.sort_values('Date').copy(); c=g.Close.astype(float); v=g.Volume.fillna(0).astype(float)
    for n in [5,20,40,60,112,224]: g[f'ma{n}']=c.rolling(n,min_periods=n).mean()
    sd40=c.rolling(40,min_periods=40).std(); g['bb40_width']=sd40*4/g.ma40*100
    g['conv20_60_112']=(pd.concat([g.ma20,g.ma60,g.ma112],axis=1).max(axis=1)-pd.concat([g.ma20,g.ma60,g.ma112],axis=1).min(axis=1))/pd.concat([g.ma20,g.ma60,g.ma112],axis=1).max(axis=1)*100
    sign=np.sign(c.diff()).fillna(0); g['obv']=(sign*v).cumsum()
    g['ret1']=c.pct_change()*100
    return g

def fdiv(a,b):
    try:
        return float(a)/float(b) if pd.notna(a) and pd.notna(b) and float(b)!=0 else np.nan
    except:return np.nan

def reconstruct_case(g, sd):
    g=add_signal_features(g.reset_index(drop=True)); sd=pd.Timestamp(sd).normalize()
    hits=g.index[g.Date.eq(sd)].tolist()
    if not hits:return None,None
    i=hits[-1]; r=g.loc[i]; pre=g.iloc[max(0,i-40):i+1].copy()
    last5=g.iloc[max(0,i-4):i+1]; prev15=g.iloc[max(0,i-19):max(0,i-4)]
    last10=g.iloc[max(0,i-9):i+1]
    # causal compression velocities
    bb_now=r.bb40_width
    bb5=float(g.loc[i-5,'bb40_width']) if i>=5 else np.nan
    bb10=float(g.loc[i-10,'bb40_width']) if i>=10 else np.nan
    cv_now=r.conv20_60_112
    cv5=float(g.loc[i-5,'conv20_60_112']) if i>=5 else np.nan
    cv10=float(g.loc[i-10,'conv20_60_112']) if i>=10 else np.nan
    # volume/amount cooling: recent 5 vs prior 15
    v5=float(last5.Volume.median()) if len(last5) else np.nan
    vp=float(prev15.Volume.median()) if len(prev15) else np.nan
    a5=float(last5.Amount.median()) if 'Amount' in last5 and len(last5) else np.nan
    ap=float(prev15.Amount.median()) if 'Amount' in prev15 and len(prev15) else np.nan
    # up/down volume asymmetry last20
    w20=g.iloc[max(0,i-19):i+1].copy()
    up=w20[w20.ret1>0].Volume; dn=w20[w20.ret1<0].Volume
    # impulse chronology: 40-bar low before/at signal -> subsequent max high up to signal
    p40=g.iloc[max(0,i-39):i+1].copy(); low_idx=p40.Low.idxmin(); low=float(g.loc[low_idx,'Low'])
    after_low=g.loc[low_idx:i]; high_idx=after_low.High.idxmax(); high=float(g.loc[high_idx,'High'])
    impulse=(high/low-1)*100 if low>0 else np.nan
    retrace=(high-float(r.Close))/(high-low)*100 if high>low else np.nan
    low_to_high=int(high_idx-low_idx); high_to_signal=int(i-high_idx)
    # candle quality signal day
    rng=float(r.High-r.Low); body=float(r.Close-r.Open)
    upper=float(r.High-max(r.Open,r.Close)); lower=float(min(r.Open,r.Close)-r.Low)
    close_loc=(float(r.Close-r.Low)/rng*100) if rng>0 else np.nan
    rec={
      'bb40_width_rebuilt':bb_now,'bb40_change_5d':bb_now-bb5 if pd.notna(bb5) else np.nan,'bb40_change_10d':bb_now-bb10 if pd.notna(bb10) else np.nan,
      'bb40_ratio_vs_5d':fdiv(bb_now,bb5),'bb40_ratio_vs_10d':fdiv(bb_now,bb10),
      'conv_rebuilt':cv_now,'conv_change_5d':cv_now-cv5 if pd.notna(cv5) else np.nan,'conv_change_10d':cv_now-cv10 if pd.notna(cv10) else np.nan,
      'volume_last5_vs_prev15':fdiv(v5,vp),'amount_last5_vs_prev15':fdiv(a5,ap),
      'up_down_volume_ratio20':fdiv(up.mean() if len(up) else np.nan,dn.mean() if len(dn) else np.nan),
      'obv_change5':float(r.obv-g.loc[i-5,'obv']) if i>=5 else np.nan,'obv_change10':float(r.obv-g.loc[i-10,'obv']) if i>=10 else np.nan,
      'impulse_low_to_high_pct':impulse,'retrace_high_to_signal_pct':retrace,'low_to_high_sessions':low_to_high,'high_to_signal_sessions':high_to_signal,
      'signal_close_loc_pct':close_loc,'signal_body_pct':body/float(r.Open)*100 if r.Open else np.nan,
      'signal_upper_wick_pct':upper/float(r.Close)*100 if r.Close else np.nan,'signal_lower_wick_pct':lower/float(r.Close)*100 if r.Close else np.nan,
      'signal_vol20_ratio_rebuilt':fdiv(float(r.Volume),float(w20.Volume.mean()) if len(w20) else np.nan),
      'signal_amount_b':float(r.Amount)/1e9 if 'Amount' in r and pd.notna(r.Amount) else np.nan,
    }
    # timeline -20..+10 normalized to entry close, only descriptive
    rows=[]; entry=float(r.Close)
    for j in range(max(0,i-20),min(len(g),i+11)):
        z=g.loc[j]
        rows.append({'offset':int(j-i),'date':pd.Timestamp(z.Date).date().isoformat(),
          'close_ret_from_signal_pct':(float(z.Close)/entry-1)*100,
          'high_ret_from_signal_pct':(float(z.High)/entry-1)*100,
          'low_ret_from_signal_pct':(float(z.Low)/entry-1)*100,
          'volume_vs_signal':fdiv(float(z.Volume),float(r.Volume)),
          'bb40_width':z.bb40_width,'conv20_60_112':z.conv20_60_112,
          'dist_ma112_pct':(float(z.Close)/float(z.ma112)-1)*100 if pd.notna(z.ma112) and z.ma112 else np.nan,
          'dist_ma224_pct':(float(z.Close)/float(z.ma224)-1)*100 if pd.notna(z.ma224) and z.ma224 else np.nan})
    return rec,rows

def cliffs(a,b):
    a=pd.to_numeric(pd.Series(a),errors='coerce').dropna().values; b=pd.to_numeric(pd.Series(b),errors='coerce').dropna().values
    if len(a)==0 or len(b)==0:return np.nan
    return float(((a[:,None]>b[None,:]).sum()-(a[:,None]<b[None,:]).sum())/(len(a)*len(b)))

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--r3-ledger',required=True);ap.add_argument('--marcap',nargs='+',required=True);ap.add_argument('--out',default='reports/early_power_r4');args=ap.parse_args()
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    r3=pd.read_csv(args.r3_ledger,dtype={'code':str}); r3['code']=r3.code.map(norm_code)
    q=r3[(r3.stage=='S3_CONVERGENCE') & (r3.tag_bb40_squeeze10.astype(str).str.lower().eq('true'))].copy()
    if len(q)!=17: print('WARN_EXPECTED_17_GOT',len(q))
    px=load_marcap(args.marcap); by={c:g for c,g in px.groupby('Code',sort=False)}
    cases=[];tls=[]
    for _,r in q.iterrows():
        g=by.get(r.code)
        if g is None: continue
        rec,rows=reconstruct_case(g,r.signal_date)
        if rec is None: continue
        base=r.to_dict(); base.update(rec); cases.append(base)
        for z in rows:
            z.update({'signal_date':r.signal_date,'code':r.code,'name':r.get('name',''),'outcome_group':r.r2_outcome_group});tls.append(z)
    c=pd.DataFrame(cases); t=pd.DataFrame(tls)
    c.to_csv(out/'r4_bb40_17_casebook.csv',index=False,encoding='utf-8-sig'); t.to_csv(out/'r4_bb40_17_timeline.csv',index=False,encoding='utf-8-sig')
    feats=['bb40_change_5d','bb40_change_10d','conv_change_5d','conv_change_10d','volume_last5_vs_prev15','amount_last5_vs_prev15','up_down_volume_ratio20','obv_change5','obv_change10','impulse_low_to_high_pct','retrace_high_to_signal_pct','low_to_high_sessions','high_to_signal_sessions','signal_close_loc_pct','signal_body_pct','signal_upper_wick_pct','signal_vol20_ratio_rebuilt','signal_amount_b','dist_ma112_pct','dist_ma224_pct','space_high60_pct','space_high120_pct','ret_5d','ret_20d','ma20_60_112_conv_pct','bb40_width']
    rows=[]
    w=c[c.r2_outcome_group=='CLEAN_PLUS10']; l=c[c.r2_outcome_group=='STOP_FIRST_NEG']; o=c[c.r2_outcome_group=='OTHER']
    for f in feats:
        if f not in c:continue
        for comp,a,b in [('WIN_vs_STOP',w,l),('WIN_vs_OTHER',w,o)]:
            aa=pd.to_numeric(a[f],errors='coerce');bb=pd.to_numeric(b[f],errors='coerce')
            rows.append({'comparison':comp,'feature':f,'a_n':int(aa.notna().sum()),'b_n':int(bb.notna().sum()),'a_median':aa.median(),'b_median':bb.median(),'median_diff':aa.median()-bb.median(),'cliffs_delta':cliffs(aa,bb)})
    fc=pd.DataFrame(rows).sort_values(['comparison','cliffs_delta'],key=lambda s:s.abs() if s.name=='cliffs_delta' else s,ascending=[True,False])
    fc.to_csv(out/'r4_feature_contrast.csv',index=False,encoding='utf-8-sig')
    # compact case sheet ordered outcome then date
    keep=['signal_date','code','name','r2_outcome_group','entry_close','d10_close_ret_pct','mfe10_pct','mae10_pct','first_plus10_day_h10','first_minus5_day_h10']+feats
    keep=[x for x in keep if x in c]
    c[keep].sort_values(['r2_outcome_group','signal_date','code']).to_csv(out/'r4_compact_case_sheet.csv',index=False,encoding='utf-8-sig')
    meta={'revision':REV,'research_only':True,'production_changed':False,'production_search_changed':False,'production_score_changed':False,'production_rank_changed':False,'production_order_changed':False,'automatic_orders':False,'same_sample_retuning':False,'population':'Frozen R3 S3_CONVERGENCE + BB40 width <=10% only','expected_cases':17,'actual_cases':int(len(c)),'outcome_counts':c.r2_outcome_group.value_counts().to_dict(),'note':'Case/path anatomy only. New descriptors are descriptive signal-time features; no threshold is promoted from this sample.'}
    (out/'meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
    rep=['# EARLY POWER R4 — S3+BB40 17-case anatomy','',json.dumps(meta,ensure_ascii=False,indent=2),'','## Case sheet',c[keep].sort_values(['r2_outcome_group','signal_date','code']).to_string(index=False),'','## Feature contrasts',fc.head(40).to_string(index=False)]
    (out/'REPORT.txt').write_text('\n'.join(rep),encoding='utf-8')
    print('EARLY_POWER_R4_CASE_ANATOMY_PASS'); print(json.dumps(meta,ensure_ascii=False)); print(fc.head(25).to_string(index=False))
if __name__=='__main__':main()
