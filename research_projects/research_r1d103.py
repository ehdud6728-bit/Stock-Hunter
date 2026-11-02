"""R1-D103 descriptive MA roles, event-by-event PIT with independent outcomes.

Usage (same existing YML): python research_projects/r1d103/research_r1d103.py
Research only, no production/LIVE changes. No MA224 selection gate.
"""
from __future__ import annotations
import hashlib,json,sys
from collections import Counter
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d100'),str(ROOT/'r1d101'),str(ROOT/'r1d102'),str(HERE)]
from pre_rise import build_features,future_labels,MA_WINDOWS
from cloud_geometry import enrich_clouds
from dynamic_structure import structure
from ma_roles import (PRIMARY_WINDOWS,ALL_WINDOWS,FUTURE_BARS,inventory_events,attach_outcomes,
                      cooldown_events,summary_tables,before_event,classify_regime,classify_interaction)
import research_r1d96 as frozen

CASES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-04'),('006400','삼성SDI','2024-02-07')]
EXPECTED={
 '039840_KRX.csv':'330ffdc5febbbf76f3f959d72181a2a37cc144ec567e6e7f691985b7dc82e3e4',
 '071840_KRX.csv':'aba4ffb8702961fbc8d3d411138b755092551f2fd6ef1a2ac3ce02da2c221c1b',
 '006400_KRX.csv':'d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566',
}
OUT=HERE/'results'


def verify():
    for filename,hashval in EXPECTED.items():
        p=ROOT/'r1d97'/'data'/filename
        if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=hashval:
            raise ValueError('Frozen KRX CSV missing or edited: '+filename)
    for p in (ROOT/'r1d101'/'pre_rise.py',ROOT/'r1d102'/'cloud_geometry.py',ROOT/'r1d100'/'dynamic_structure.py'):
        if not p.is_file():raise FileNotFoundError('Missing dependency '+str(p))


def csv(name,rows):
    if isinstance(rows,pd.DataFrame):data=rows
    else:data=pd.DataFrame(rows)
    data.to_csv(OUT/name,index=False,encoding='utf-8-sig')


def reference_rows(d,features,labels,code,name,ref):
    t=int(features.index[features.date.eq(pd.Timestamp(ref))][0]);q=features.iloc[t]
    if t<max(MA_WINDOWS)+61:raise ValueError('reference missing complete history: '+ref)
    b=classify_regime(features,t)
    label=labels.iloc[t]
    s=structure(d,ref)
    result=[]
    for n in PRIMARY_WINDOWS:
        result.append({'code':code,'name':name,'asof':ref,'ma_period':n,'ma_role':{
          5:'FAST_MOMENTUM',20:'SHORT_SWING',60:'INTERMEDIATE_TREND',112:'MEDIUM_LONG_TREND',224:'LONG_CONTEXT'}[n],
          'event':classify_interaction(features,t,n) or 'NO_DIRECT_INTERACTION',
          'close':float(q.close),'ma_value':float(q[f'ma{n}']),
          'close_ma_gap_pct':float(q[f'ma{n}_price_gap_pct']),
          'ma_slope5_pct':float(q[f'ma{n}_slope5_pct']),
          'ma_slope20_pct':float(q[f'ma{n}_slope20_pct']),
          'ma_slope_accel5_pp':float(q[f'ma{n}_slope20_acceleration5_pp']),
          'bb20_width_pct':float(q.bb20_width_pct),'amount_vs_prev20':float(q.amount_vs_prev20),
          'structure_asof':s['structure'],
          'forward20_close_pct_EXPOST':float(label.forward20_close_pct),**b})
    return result


def chart(features,code,name,ref):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.exists():
        font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    t=int(features.index[features.date.eq(pd.Timestamp(ref))][0]);left=max(0,t-100);right=min(len(features)-1,t+20)
    p=features.iloc[left:right+1]
    fig,(ax,bx,cx)=plt.subplots(3,1,figsize=(14,10),sharex=True,gridspec_kw={'height_ratios':[5,1.7,1.5]})
    ax.plot(p.idx,p.close,color='black',lw=1.5,label='Close')
    for n,color in zip(PRIMARY_WINDOWS,('#f97316','#0891b2','#4f46e5','#16a34a','#a855f7')):
        ax.plot(p.idx,p[f'ma{n}'],lw=1.3,label=f'MA{n}',color=color)
    ax.fill_between(p.idx.to_numpy(),p.bb20_lower.to_numpy(),p.bb20_upper.to_numpy(),alpha=.06,color='#64748b',label='BB20 range')
    ax.axvline(t,ls='--',lw=1.2,color='black',label='ASOF cutoff')
    ax.axvspan(t,right,color='gold',alpha=.09,label='EX-POST price area')
    ax.grid(alpha=.15);ax.legend(loc='upper left',ncol=4,fontsize=8)
    bx.plot(p.idx,p.bb20_width_pct,color='#2563eb',label='BB20 width %')
    bx.plot(p.idx,p.ma_cloud_fast_thickness_pct,color='#ea580c',label='Fast MA cloud %')
    bx.plot(p.idx,p.ma_cloud_mid_thickness_pct,color='#059669',label='Mid MA cloud %')
    bx.legend(ncol=3,fontsize=8);bx.grid(alpha=.15)
    cx.bar(p.idx,p.amount/1e8,color='#94a3b8',label='amount / 100M KRW')
    cx.legend(fontsize=8);cx.grid(alpha=.15)
    step=max(1,len(p)//8);view=p.iloc[::step]
    cx.set_xticks(view.idx);cx.set_xticklabels(view.date.dt.strftime('%Y-%m-%d'),rotation=30,fontsize=8)
    fig.suptitle(f'{name}({code}) | MA roles: 5/20/60/112/224 | {ref} | RESEARCH ONLY',fontsize=12)
    fig.tight_layout()
    target=OUT/'charts'/f'{code}_{ref}_MA_ROLES.png';fig.savefig(target,dpi=120);plt.close(fig)
    return target.name


def run(*,input_override=None,out=None):
    global OUT
    if out is not None:OUT=Path(out)
    if input_override is None:verify()
    OUT.mkdir(parents=True,exist_ok=True);(OUT/'charts').mkdir(exist_ok=True)
    eligible_all=[];events_all=[];sample_all=[];snapshots_all=[];references=[];pit=[];counts=[];charts=[]
    for code,name,ref in CASES:
        print('CASE_START',code,name,flush=True)
        raw=input_override(code) if input_override is not None else frozen.load(code)
        f=enrich_clouds(build_features(raw));label=future_labels(f)
        inv=inventory_events(f)
        attributed=attach_outcomes(inv,label,f)
        selected=cooldown_events(attributed)
        # Recalculate high/low pivot structure at each case DATE. This is the
        # archived dynamic detector, never a future structure or feature label.
        if not selected.empty:
            selected['structure_asof']=[structure(raw,asof)['structure'] for asof in selected['asof']]
        # Each MA event is independent of outcomes; multiple MAs can occur on the same stock-day.
        for table in (attributed,selected):
            if len(table):table.insert(0,'code',code);table.insert(1,'name',name)
        if not attributed.empty:events_all.append(attributed)
        if not selected.empty:
            sample_all.append(selected)
            snap=before_event(f,selected)
            snap.insert(0,'code',code);snap.insert(1,'name',name);snapshots_all.append(snap)
        eligible=(f.iloc[max(540,max(MA_WINDOWS)+61):len(f)-FUTURE_BARS]).copy()
        l=label.iloc[eligible.index]
        for idx,r in eligible.iterrows():
            base=classify_regime(f,int(idx))
            eligible_all.append({'code':code,'date':str(r.date.date()),'idx':int(idx),
                'ma224_zone':base['ma224_zone'],'long_slope_context':base['long_slope_context'],
                'ma_stack_context':base['ma_stack_context'],
                'future20_close_pct_EXPOST':float(l.loc[idx,'forward20_close_pct']),
                'outcome_EXPOST':'SURGE' if bool(l.loc[idx,'is_sustained_surge']) else
                    'CONTROL' if bool(l.loc[idx,'is_nonrise_control']) else 'OTHER'})
        if input_override is None:
            references.extend(reference_rows(raw,f,label,code,name,ref))
            charts.append(chart(f,code,name,ref))
        else:
            # synthetic fixtures may lack historical fixed-reference dates
            pass
        # PIT property: a changed FUTURE may change its labels but never current events.
        asof_idx=int(f.index[f.date.eq(pd.Timestamp(ref))][0]) if input_override is None else max(550,len(f)-70)
        alt=raw.copy()
        for field in ('open','high','low','close'):
            alt[field]=alt[field].astype(float)
            alt.loc[alt.index>asof_idx,field]*=1.217
        alt_f=enrich_clouds(build_features(alt))
        compare_cols=[x for x in f.columns if x not in ('date','code')]
        pd.testing.assert_series_equal(f.iloc[asof_idx][compare_cols],alt_f.iloc[asof_idx][compare_cols],check_names=False)
        prior=inv.loc[inv.idx<=asof_idx].reset_index(drop=True) if len(inv) else inv
        modified=inventory_events(alt_f)
        modified=modified.loc[modified.idx<=asof_idx].reset_index(drop=True) if len(modified) else modified
        pd.testing.assert_frame_equal(prior,modified,check_dtype=False)
        pit.append({'code':code,'date':str(f.iloc[asof_idx].date.date()),'passed':True})
        for period in PRIMARY_WINDOWS:
            count=int((inv.ma_period==period).sum()) if len(inv) else 0
            n_sample=int((selected.ma_period==period).sum()) if len(selected) else 0
            counts.append({'code':code,'name':name,'ma_period':period,'n_events':count,'n_cooldown_sample':n_sample})
        print('CASE_DONE',code,'eligible',len(eligible),'raw_events',len(inv),'sampled',len(selected),'PIT_PASS',flush=True)
    allevents=pd.concat(events_all,ignore_index=True) if events_all else pd.DataFrame()
    samples=pd.concat(sample_all,ignore_index=True) if sample_all else pd.DataFrame()
    pre=pd.concat(snapshots_all,ignore_index=True) if snapshots_all else pd.DataFrame()
    grouped=summary_tables(samples)
    grouped_bands=summary_tables(samples,keys=('ma_period','event','ma224_zone',
                                                'bb20_width_context','amount_context'))
    grouped_structure=summary_tables(samples,keys=('ma_period','event','structure_asof',
                                                   'long_slope_context'))
    # Overall base rates for each observed context; NO MA interaction required.
    baseline_rows=[]
    population_frame=pd.DataFrame(eligible_all)
    for (zone,long_ctx),g in population_frame.groupby(['ma224_zone','long_slope_context']):
        baseline_rows.append({'ma224_zone':zone,'long_slope_context':long_ctx,
            'eligible_dates':len(g),'unique_stock_dates':g[['code','date']].drop_duplicates().shape[0],
            'surge_days':int((g.outcome_EXPOST=='SURGE').sum()),
            'control_days':int((g.outcome_EXPOST=='CONTROL').sum()),
            'positive_d20_rate':float((g.future20_close_pct_EXPOST>0).mean()),
            'median_d20_pct':float(g.future20_close_pct_EXPOST.median()),
            'research_only':True})
    # A co-occurrence ledger avoids treating five same-day MA observations as five independent stock-days.
    overlap=(samples.groupby(['code','asof'],as_index=False).agg(
        signal_count=('ma_period','size'),different_ma_count=('ma_period','nunique'),
        event_types=('event',lambda s:'|'.join(sorted(set(s))))).sort_values(['code','asof'])
        if not samples.empty else pd.DataFrame())
    for fname,df in [
        ('R1D103_ALL_ELIGIBLE_BASELINE.csv',eligible_all),
        ('R1D103_ALL_MA_EVENTS_UNFILTERED.csv',allevents),
        ('R1D103_MA_EVENTS_20BAR_COOLDOWN_SAMPLE.csv',samples),
        ('R1D103_PRE_EVENT_DMINUS20_TO_D0.csv',pre),
        ('R1D103_EXPOST_EVENT_GROUP_STATS.csv',grouped),
        ('R1D103_BB_AMOUNT_MA_CONTEXT_COMPARE.csv',grouped_bands),
        ('R1D103_STRUCTURE_MA_CONTEXT_COMPARE.csv',grouped_structure),
        ('R1D103_MA224_ZONE_BASELINE_RATES.csv',baseline_rows),
        ('R1D103_SAME_DAY_OVERLAP_LEDGER.csv',overlap),
        ('R1D103_3STOCK_REFERENCE_5MA.csv',references),
        ('R1D103_MA_COUNTS_BY_STOCK.csv',counts),
        ('R1D103_PIT_TAMPER.csv',pit)]:csv(fname,df)
    audit={'version':'R1D103_MA_ROLE_EVENTS_RESEARCH_ONLY',
        'ma_windows_all':list(ALL_WINDOWS),'ma_windows_interaction':list(PRIMARY_WINDOWS),
        'event_evaluation':'EOD; high/low intraday touch only candidate, not causal proof',
        'ma224_gate':False,'population_dates':len(eligible_all),'raw_events':len(allevents),
        'sample_events':len(samples),'unique_stock_dates':len(overlap),
        'pit_all_pass':all(z['passed'] for z in pit),'source_hash_verified':input_override is None,
        'reference_rows':len(references),'charts':charts,'frozen_watch_untouched':True,
        'limitations':['Not an investment recommendation or OOS backtest',
          'Three selected stocks; correlated overlapping windows and moving averages',
          '20-bar cooldown is per MA/event, not globally independent',
          'Future results are ex-post LABELS only; future cannot change PIT features or selected events',
          'No new screening filter or production changes',
          'Raw historical corporate action adjustments remain unaudited',
          'A short/medium/long MA role is hypothesis, not an empirically established universal rule']}
    (OUT/'R1D103_AUDIT.json').write_text(json.dumps(audit,indent=2,ensure_ascii=False),encoding='utf-8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ('population_dates','raw_events','sample_events','unique_stock_dates','pit_all_pass')},ensure_ascii=False),flush=True)
    return audit

if __name__=='__main__':run()
