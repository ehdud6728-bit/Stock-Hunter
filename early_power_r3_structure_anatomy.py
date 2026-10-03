#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV='EARLY_POWER_R3_S3_STRUCTURE_ANATOMY_20261003'

def pct(a,b): return float(a/b*100) if b else np.nan

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--r2-ledger', required=True)
    ap.add_argument('--out', default='reports/early_power_r3')
    a=ap.parse_args(); out=Path(a.out); out.mkdir(parents=True,exist_ok=True)
    x=pd.read_csv(a.r2_ledger, low_memory=False)
    x=x[x['stage'].eq('S3_CONVERGENCE')].copy()
    if x.empty: raise SystemExit('NO_S3_ROWS')

    # Descriptive, non-exclusive structure tags. Existing project thresholds are reused where available.
    # No thresholds below are fitted to R2 outcomes.
    x['tag_below224_convergence']=pd.to_numeric(x['dist_ma224_pct'],errors='coerce').lt(0)
    x['tag_below112_approach']=pd.to_numeric(x['dist_ma112_pct'],errors='coerce').lt(0)
    x['tag_first_wave_pullback_context']=x['first_wave_context'].astype(str).str.lower().isin(['true','1'])
    x['tag_bb40_squeeze10']=x['bb40_tight10'].astype(str).str.lower().isin(['true','1'])
    x['tag_obv_hold_up']=x['obv_hold_up'].astype(str).str.lower().isin(['true','1'])
    x['tag_inverse30plus']=pd.to_numeric(x['inverse_days_120'],errors='coerce').ge(30)
    x['tag_ma20_rising']=pd.to_numeric(x['ma20_slope5_pct'],errors='coerce').ge(0)
    x['tag_no_overheat']=pd.to_numeric(x['overheat_count'],errors='coerce').eq(0)

    tagcols=[c for c in x.columns if c.startswith('tag_')]
    x['structure_tag_count']=x[tagcols].sum(axis=1)
    x.to_csv(out/'r3_s3_structure_ledger.csv',index=False,encoding='utf-8-sig')

    # Single-tag descriptive outcome table.
    rows=[]
    for t in tagcols:
        for present in [True,False]:
            g=x[x[t].eq(present)]
            if g.empty: continue
            clean=int(g['r2_outcome_group'].eq('CLEAN_PLUS10').sum())
            stop=int(g['r2_outcome_group'].eq('STOP_FIRST_NEG').sum())
            rows.append({
                'tag':t,'present':present,'n':len(g),'signal_days':g['signal_date'].nunique(),
                'clean_plus10_n':clean,'clean_plus10_pct':pct(clean,len(g)),
                'stop_first_n':stop,'stop_first_pct':pct(stop,len(g)),
                'd3_median':pd.to_numeric(g['d3_close_ret_pct'],errors='coerce').median(),
                'd5_median':pd.to_numeric(g['d5_close_ret_pct'],errors='coerce').median(),
                'd10_median':pd.to_numeric(g['d10_close_ret_pct'],errors='coerce').median(),
                'mfe10_median':pd.to_numeric(g['mfe10_pct'],errors='coerce').median(),
                'mae10_median':pd.to_numeric(g['mae10_pct'],errors='coerce').median(),
            })
    tag_summary=pd.DataFrame(rows)
    tag_summary.to_csv(out/'r3_structure_tag_summary.csv',index=False,encoding='utf-8-sig')

    # Count strata: descriptive only, no promotion rule.
    rows=[]
    for k,g in x.groupby('structure_tag_count'):
        clean=int(g['r2_outcome_group'].eq('CLEAN_PLUS10').sum())
        stop=int(g['r2_outcome_group'].eq('STOP_FIRST_NEG').sum())
        rows.append({
            'structure_tag_count':int(k),'n':len(g),'signal_days':g['signal_date'].nunique(),
            'clean_plus10_n':clean,'clean_plus10_pct':pct(clean,len(g)),
            'stop_first_n':stop,'stop_first_pct':pct(stop,len(g)),
            'd10_median':pd.to_numeric(g['d10_close_ret_pct'],errors='coerce').median(),
            'mfe10_median':pd.to_numeric(g['mfe10_pct'],errors='coerce').median(),
            'mae10_median':pd.to_numeric(g['mae10_pct'],errors='coerce').median(),
        })
    count_summary=pd.DataFrame(rows)
    count_summary.to_csv(out/'r3_structure_count_summary.csv',index=False,encoding='utf-8-sig')

    # Outcome-group prevalence for each structure tag.
    prev=[]
    for grp,g in x.groupby('r2_outcome_group'):
        row={'outcome_group':grp,'n':len(g),'signal_days':g['signal_date'].nunique()}
        for t in tagcols: row[t+'_pct']=float(g[t].mean()*100)
        prev.append(row)
    pd.DataFrame(prev).to_csv(out/'r3_tag_prevalence_by_outcome.csv',index=False,encoding='utf-8-sig')

    # Casebooks for manual chart review.
    clean=x[x['r2_outcome_group'].eq('CLEAN_PLUS10')].sort_values(['structure_tag_count','mfe10_pct'],ascending=[False,False])
    stop=x[x['r2_outcome_group'].eq('STOP_FIRST_NEG')].sort_values(['structure_tag_count','mae10_pct'],ascending=[False,True])
    clean.to_csv(out/'r3_clean_plus10_structure_casebook.csv',index=False,encoding='utf-8-sig')
    stop.to_csv(out/'r3_stop_first_structure_casebook.csv',index=False,encoding='utf-8-sig')

    meta={
      'revision':REV,'research_only':True,'production_changed':False,
      'production_search_changed':False,'production_score_changed':False,
      'production_rank_changed':False,'production_order_changed':False,
      'automatic_orders':False,'same_sample_retuning':False,
      'source':'Frozen EARLY POWER R2 ledger; S3_CONVERGENCE only',
      'tag_definitions':{
        'below224_convergence':'Close < MA224 at S3 convergence',
        'below112_approach':'Close < MA112 at S3 convergence',
        'first_wave_pullback_context':'Frozen R1 first_wave_context',
        'bb40_squeeze10':'Frozen project BB40 width <= 10%',
        'obv_hold_up':'Frozen R1 OBV delta20 >= 0',
        'inverse30plus':'Frozen R1 context component inverse_days_120 >= 30',
        'ma20_rising':'Frozen R1 context component MA20 5-session slope >= 0',
        'no_overheat':'Frozen R1 overheat_count == 0'
      },
      'note':'Descriptive non-exclusive structure anatomy only. No new production filter or ranking threshold is promoted from this sample.'
    }
    (out/'meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
    report=['# EARLY POWER R3 S3 Structure Anatomy','',json.dumps(meta,ensure_ascii=False,indent=2),'','## Tag summary',tag_summary.to_string(index=False),'','## Tag-count summary',count_summary.to_string(index=False)]
    (out/'REPORT.txt').write_text('\n'.join(report),encoding='utf-8')
    print('EARLY_POWER_R3_STRUCTURE_ANATOMY_PASS')
    print(tag_summary[tag_summary.present.eq(True)].sort_values('clean_plus10_pct',ascending=False).to_string(index=False))
    print(count_summary.to_string(index=False))

if __name__=='__main__': main()
