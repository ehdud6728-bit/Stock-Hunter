"""R1-D98 compare structural/state-aware trendlines against frozen R1-D96.

Research only. Never writes to production workflows, search or trading code.
Run from any directory: python research_projects/r1d98/research_r1d98.py
"""
from __future__ import annotations
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
BASE = HERE.parent / 'r1d97'
sys.path.insert(0, str(BASE))
import research_r1d96 as baseline  # noqa: E402
from state_machine import enrich, evaluate, line_at, pick  # noqa: E402

OUT = HERE/'results'
CASES = [
    ('039840','디오','2024-09-13'),
    ('071840','롯데하이마트','2024-03-04'),
    ('006400','삼성SDI','2024-02-07'),
]
SHA = {
    '006400_KRX.csv':'d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566',
    '039840_KRX.csv':'330ffdc5febbbf76f3f959d72181a2a37cc144ec567e6e7f691985b7dc82e3e4',
    '071840_KRX.csv':'aba4ffb8702961fbc8d3d411138b755092551f2fd6ef1a2ac3ce02da2c221c1b',
}


def must_verify_sources():
    if not (BASE/'research_r1d96.py').exists():
        raise RuntimeError('Missing frozen R1D96 source inside r1d97')
    for file, expected in SHA.items():
        p=BASE/'data'/file
        if not p.is_file():
            raise RuntimeError(f'Missing baseline input: {p}')
        got=hashlib.sha256(p.read_bytes()).hexdigest()
        if got!=expected:
            raise RuntimeError(f'Input changed: {file}, expected {expected}, got {got}')


def render(d, code, name, asof, baseline_sel, ranked, tag):
    # All overlays reflect the ASOF selection; after20 is outcomes only, not input to selection.
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    t=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0]);stop=t+(20 if tag=='FUTURE20' else 0)
    stop=min(len(d)-1,stop);start=max(0,t-78)
    v=d.iloc[start:stop+1];fig,(ax,axma,axamt)=plt.subplots(
        3,1,figsize=(15.0,9.4),sharex=True,gridspec_kw={'height_ratios':[4,1.4,0.9]})
    for q in v.itertuples():
        k=int(q.idx);cl='#d34d47' if q.close>=q.open else '#357ba8'
        ax.vlines(k,q.low,q.high,color=cl,lw=0.8)
        ax.add_patch(Rectangle((k-.3,min(q.open,q.close)),.6,max(.25,abs(q.open-q.close)),
                               color=cl,alpha=.82,lw=0.3))
    for col,co in [('BB20_LO','#399fc5'),('ENV20_10_LO','#c59630'),('ENV40_10_LO','#9b723f')]:
        ax.plot(v.idx,v[col],color=co,lw=.95,alpha=.65,label=col)
    if tag == 'BASELINE':
        lines=baseline_sel
    else:
        lines=ranked['PRIMARY']+ranked['WATCH']
    used=set()
    for row in lines:
        grp=(row['kind'],row['tier'])
        # Show only 1 per type+tier+state for readable chart.
        key=(grp if tag=='BASELINE' else (grp,row['bucket']))
        if key in used:continue
        used.add(key)
        lo=max(start,int(row['h2_confirm_idx']));x=np.arange(lo,stop+1)
        if not len(x):continue
        y=[line_at(row,int(k)) for k in x]
        state=row.get('status','R1D96_BASELINE')
        color=('#b45309' if row['kind']=='RESISTANCE' else '#087e8b')
        style='--' if row['kind']=='RESISTANCE' else '-'
        if tag!='BASELINE' and row['bucket']=='WATCH':style=':'
        ax.plot(x,y,color=color,ls=style,lw=1.9,alpha=.9,
                label=f'{row["kind"][:1]} {row["tier"]} {row["h2"]} {state}')
    ax.axvline(t,color='#6c6c6c',lw=1.4,ls='-.',label='ASOF date')
    if stop>t:ax.axvspan(t,stop,color='#edb86c',alpha=.12)
    ax.grid(alpha=.12);ax.legend(loc='upper left',fontsize=7,ncol=2)
    ax.set_ylabel('RAW OHLC / BAND / LINE')
    for col,c in [('close','#555555'),('MA112','#28883e'),('MA224','#191c25'),('MA448','#8053af')]:
        axma.plot(v.idx,v[col],color=c,lw=1.05,label=col)
    axma.legend(fontsize=8,ncol=4);axma.grid(alpha=.12)
    axma.set_ylabel('CLOSE / MA')
    axamt.bar(v.idx,v.amount/1e8,width=.65,color='#959595');axamt.set_ylabel('amount (100M KRW)')
    ticks=v.iloc[::max(1,len(v)//11)]
    axamt.set_xticks(ticks.idx)
    axamt.set_xticklabels(ticks.date.dt.strftime('%Y-%m-%d'),rotation=32,fontsize=7)
    ax.set_xlim(start-.5,stop+.5)
    fig.suptitle(f'{code} {name} | {asof} | R1-D98 {tag} | ASOF-only selection; CA unverified',size=11)
    fig.tight_layout();p=OUT/'charts'/f'{code}_{asof}_{tag}.png'
    fig.savefig(p,dpi=140,bbox_inches='tight');plt.close(fig)
    return p.name


def audit_timeline(d, asof, lines, code):
    t=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0]);out=[]
    for r in lines:
        for k in range(max(int(r['h2_confirm_idx']),t-50),t+1):
            day=str(d.iloc[k].date.date())
            status=evaluate(r,d,day)
            out.append(dict(code=code,asof=asof,day=day,line_id=r['line_id'],
                            kind=r['kind'],tier=r['tier'],**status))
    return out


def output_csv(name,records):
    pd.DataFrame(records).to_csv(OUT/name,index=False,encoding='utf-8-sig')


def main():
    must_verify_sources()
    (OUT/'charts').mkdir(parents=True,exist_ok=True)
    all_lines=[];baseline_rows=[];primary=[];watch=[];side=[];timeline=[];pit=[];charts=[]
    for code,name,asof in CASES:
        d=baseline.load(code)
        old, piv, oldsel=baseline.choose(d,asof)
        annotated=enrich(old,d,asof)
        picks=pick(annotated)
        byid={r['line_id']:r for r in annotated}
        original_ids={r['line_id'] for r in oldsel}
        new_primary_ids={r['line_id'] for r in picks['PRIMARY']}
        new_watch_ids={r['line_id'] for r in picks['WATCH']}
        for r in annotated:all_lines.append(dict(code=code,name=name,**r))
        for r in oldsel:baseline_rows.append(dict(code=code,name=name,**byid[r['line_id']]))
        for r in picks['PRIMARY']:primary.append(dict(code=code,name=name,**r))
        for r in picks['WATCH']:watch.append(dict(code=code,name=name,**r))
        for lid in sorted(original_ids|new_primary_ids|new_watch_ids):
            r=byid[lid]
            side.append(dict(code=code,name=name,asof=asof,line_id=lid,
                old_selected=lid in original_ids,new_primary=lid in new_primary_ids,
                new_watch=lid in new_watch_ids,kind=r['kind'],tier=r['tier'],
                score=r['rank_score'],status=r['status'],flags=r['structure_flags'],
                first_event_date=r['first_event_date'],fresh_break_today=r['fresh_break_today']))
        selected_for_timeline=[byid[lid] for lid in sorted(original_ids|new_primary_ids|new_watch_ids)]
        timeline.extend(audit_timeline(d,asof,selected_for_timeline,code))
        for label,lines in [('BASELINE',oldsel),('IMPROVED',picks['PRIMARY']+picks['WATCH'])]:
            if label=='BASELINE':
                fn=render(d,code,name,asof,oldsel,picks,'BASELINE')
                charts.append(fn)
            else:
                fn=render(d,code,name,asof,oldsel,picks,'IMPROVED')
                charts.append(fn)
                fn=render(d,code,name,asof,oldsel,picks,'FUTURE20')
                charts.append(fn)
        t=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0]);altered=d.copy()
        for col in ('open','high','low','close'):
            altered[col]=altered[col].astype(float)
            altered.loc[altered.index>t,col]*=1.217
        changed_old,_,_=baseline.choose(altered,asof)
        changed=enrich(changed_old,altered,asof)
        sig=lambda rows: sorted((r['line_id'],r['kind'],r['tier'],r['rank_score'],r['status'],
                                 r['first_event_date'],r['fresh_break_today']) for r in rows)
        if sig(annotated)!=sig(changed):
            raise AssertionError(f'R1-D98 future taint failure {code}')
        pit.append(dict(code=code,asof=asof,passed=True,baseline_candidates=len(old),
                        primary=len(picks['PRIMARY']),watch=len(picks['WATCH']),
                        structure_quarantined=sum(r['status']=='STRUCTURE_QUARANTINE' for r in annotated)))
        print(code,name,asof,'old',len(oldsel),'new_primary',len(picks['PRIMARY']),
              'new_watch',len(picks['WATCH']),'quarantine',pit[-1]['structure_quarantined'])
    for f,x in [('R1D98_ALL_LINES_CLASSIFIED.csv',all_lines),('R1D98_FROZEN_R1D96_SELECTION.csv',baseline_rows),
                ('R1D98_NEW_PRIMARY_LINES.csv',primary),('R1D98_NEW_WATCH_LINES.csv',watch),
                ('R1D98_SIDE_BY_SIDE.csv',side),('R1D98_LINE_STATE_TIMELINE.csv',timeline),
                ('R1D98_PIT_TAMPER.csv',pit)]:output_csv(f,x)
    output={
        'version':'R1D98_PIT_STATE_AND_STRUCTURE_RESEARCH_ONLY',
        'baseline':'R1D96 unchanged, import from r1d97',
        'data_hash_verified':True,'cases':len(CASES),
        'baseline_selected':len(baseline_rows),'new_primary':len(primary),'new_watch':len(watch),
        'candidate_lines':len(all_lines),'quarantine':sum(r['status']=='STRUCTURE_QUARANTINE' for r in all_lines),
        'status_counts':dict(Counter(r['status'] for r in all_lines)),
        'future_tamper_test':all(r['passed'] for r in pit),
        'charts':charts,'price_basis':'raw KRX OHLCV; full corporate actions not audited',
        'readiness':'CASE_STUDY_ONLY_NOT_LIVE_NOT_FULLMARKET',
        'limitations':['Not a profit/backtest statistic','Retest not proof of support','Thresholds not OOS calibrated',
                       'Full-market false-negative review required','R1D97 selection unchanged'],
    }
    (OUT/'R1D98_AUDIT.json').write_text(json.dumps(output,indent=2,ensure_ascii=False),encoding='utf-8')
    print('SUMMARY',json.dumps({k:output[k] for k in ('candidate_lines','baseline_selected','new_primary','new_watch','quarantine')}))


if __name__=='__main__':main()
