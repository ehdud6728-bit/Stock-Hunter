#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd

REV='EARLY_POWER_R2_S2_S3_ANATOMY_20261003'
FEATURES=[
'inverse_days_120','ma20_60_112_conv_pct','bb40_width','disp_ma20','rsi14','ret_5d','ret_20d','vol20_ratio','obv_delta20','dist_ma112_pct','dist_ma224_pct','space_high60_pct','space_high120_pct','impulse40_pct','retrace_from_15h_pct','overheat_count','ma5_slope5_pct','ma20_slope5_pct','ma60_slope5_pct','ma112_slope5_pct','ma224_slope5_pct','context_hits_7']
STAGES=['S2_CROSS_5_20','S3_CONVERGENCE']

def cliffs_delta(x,y):
    a=pd.to_numeric(pd.Series(x),errors='coerce').dropna().to_numpy(float)
    b=pd.to_numeric(pd.Series(y),errors='coerce').dropna().to_numpy(float)
    if len(a)==0 or len(b)==0:return np.nan
    # rank-sum equivalent, robust for this sample size
    gt=sum((a[:,None]>b[None,:]).ravel())
    lt=sum((a[:,None]<b[None,:]).ravel())
    return (gt-lt)/(len(a)*len(b))

def classify(df):
    p5=pd.to_numeric(df['first_plus5_day_h10'],errors='coerce')
    p10=pd.to_numeric(df['first_plus10_day_h10'],errors='coerce')
    m5=pd.to_numeric(df['first_minus5_day_h10'],errors='coerce')
    d10=pd.to_numeric(df['d10_close_ret_pct'],errors='coerce')
    out=np.full(len(df),'OTHER',dtype=object)
    clean=p10.notna() & (m5.isna() | (p10<m5))
    stop=m5.notna() & (p5.isna() | (m5<p5)) & d10.lt(0)
    out[clean.to_numpy()]='CLEAN_PLUS10'
    out[stop.to_numpy()]='STOP_FIRST_NEG'
    return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--ledger',required=True)
    ap.add_argument('--out',default='reports/early_power_r2')
    a=ap.parse_args(); out=Path(a.out); out.mkdir(parents=True,exist_ok=True)
    z=pd.read_csv(a.ledger,low_memory=False)
    z=z[z['stage'].isin(STAGES)].copy()
    z['r2_outcome_group']=classify(z)
    z.to_csv(out/'r2_s2_s3_event_ledger.csv',index=False,encoding='utf-8-sig')

    summary=[]
    for st,g in z.groupby('stage',sort=False):
        for grp,gg in g.groupby('r2_outcome_group',sort=False):
            rec={'stage':st,'outcome_group':grp,'n':len(gg),'signal_days':gg['signal_date'].nunique()}
            for h in [1,3,5,10,20]:
                c=pd.to_numeric(gg[f'd{h}_close_ret_pct'],errors='coerce')
                rec[f'd{h}_n']=int(c.notna().sum()); rec[f'd{h}_mean']=float(c.mean()) if c.notna().any() else np.nan; rec[f'd{h}_median']=float(c.median()) if c.notna().any() else np.nan
                mfe=pd.to_numeric(gg[f'mfe{h}_pct'],errors='coerce'); mae=pd.to_numeric(gg[f'mae{h}_pct'],errors='coerce')
                rec[f'mfe{h}_median']=float(mfe.median()) if mfe.notna().any() else np.nan; rec[f'mae{h}_median']=float(mae.median()) if mae.notna().any() else np.nan
            summary.append(rec)
    pd.DataFrame(summary).to_csv(out/'r2_outcome_summary.csv',index=False,encoding='utf-8-sig')

    comps=[]
    for st in STAGES:
        g=z[z.stage.eq(st)]; w=g[g.r2_outcome_group.eq('CLEAN_PLUS10')]; l=g[g.r2_outcome_group.eq('STOP_FIRST_NEG')]
        for f in FEATURES:
            x=pd.to_numeric(w[f],errors='coerce'); y=pd.to_numeric(l[f],errors='coerce')
            d=cliffs_delta(x,y)
            comps.append({'stage':st,'feature':f,'winner_n':int(x.notna().sum()),'loser_n':int(y.notna().sum()),'winner_median':x.median(),'loser_median':y.median(),'median_diff':x.median()-y.median(),'cliffs_delta_winner_vs_loser':d,'abs_cliffs_delta':abs(d) if pd.notna(d) else np.nan})
    comp=pd.DataFrame(comps).sort_values(['stage','abs_cliffs_delta'],ascending=[True,False])
    comp.to_csv(out/'r2_feature_contrast.csv',index=False,encoding='utf-8-sig')

    # Context-hit table is descriptive only; no threshold promotion.
    ctx=[]
    for (st,ch),g in z.groupby(['stage','context_hits_7'],dropna=False):
        rec={'stage':st,'context_hits_7':ch,'n':len(g),'signal_days':g.signal_date.nunique()}
        for h in [3,5,10,20]:
            c=pd.to_numeric(g[f'd{h}_close_ret_pct'],errors='coerce')
            rec[f'd{h}_n']=int(c.notna().sum()); rec[f'd{h}_mean']=c.mean(); rec[f'd{h}_median']=c.median()
            mfe=pd.to_numeric(g[f'mfe{h}_pct'],errors='coerce'); mae=pd.to_numeric(g[f'mae{h}_pct'],errors='coerce')
            rec[f'mfe{h}_ge10_pct']=float((mfe.dropna()>=10).mean()*100) if mfe.notna().any() else np.nan
            rec[f'mae{h}_median']=mae.median()
        ctx.append(rec)
    pd.DataFrame(ctx).sort_values(['stage','context_hits_7']).to_csv(out/'r2_context_descriptive.csv',index=False,encoding='utf-8-sig')

    # Casebooks for manual inspection
    cols=['signal_date','code','name','stage','entry_close','r2_outcome_group']+FEATURES+['d10_close_ret_pct','mfe10_pct','mae10_pct','first_plus5_day_h10','first_plus10_day_h10','first_minus5_day_h10','d20_close_ret_pct','mfe20_pct','mae20_pct']
    cols=[c for c in cols if c in z.columns]
    z[z.r2_outcome_group.eq('CLEAN_PLUS10')][cols].sort_values(['stage','mfe10_pct'],ascending=[True,False]).to_csv(out/'r2_clean_plus10_casebook.csv',index=False,encoding='utf-8-sig')
    z[z.r2_outcome_group.eq('STOP_FIRST_NEG')][cols].sort_values(['stage','mae10_pct'],ascending=[True,True]).to_csv(out/'r2_stop_first_casebook.csv',index=False,encoding='utf-8-sig')

    meta={'revision':REV,'research_only':True,'production_changed':False,'production_search_changed':False,'production_score_changed':False,'production_rank_changed':False,'production_order_changed':False,'automatic_orders':False,'same_sample_retuning':False,'source':'EARLY_POWER_MA_TRANSITION_R1 event ledger','stages':STAGES,'clean_winner_definition':'+10% MFE reached by D10 before any -5% touch','stop_first_negative_definition':'-5% touch before +5% (or +5 absent) and D10 close return < 0','note':'Descriptive anatomy only. No same-sample threshold or weight promotion.'}
    (out/'meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
    report=['# EARLY POWER R2 S2/S3 anatomy','',json.dumps(meta,ensure_ascii=False,indent=2),'','## Outcome summary',pd.DataFrame(summary).to_string(index=False),'','## Largest feature contrasts',comp.groupby('stage').head(10).to_string(index=False)]
    (out/'REPORT.txt').write_text('\n'.join(report),encoding='utf-8')
    print('EARLY_POWER_R2_ANATOMY_PASS')
    print(pd.DataFrame(summary).to_string(index=False))
    print(comp.groupby('stage').head(10).to_string(index=False))
if __name__=='__main__': main()
