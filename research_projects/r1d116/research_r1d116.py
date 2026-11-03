"""R1-D116: independent recent-turn active-line challenge vs frozen R1-D112.

Strict separation:
- Discovery = immutable R111 source candidates (all methods), no human refs.
- Proposed active-display selection = recent_turn.replay_one, no future bars.
- R112 daily decisions = retained and audited, NEVER rewritten.
- Manual anchor comparisons = AFTER selection, never used to train/score.
- Future retests = separate EXPOST file, never used in PIT selection.
"""
from __future__ import annotations
import hashlib,json,sys
from pathlib import Path
import pandas as pd
import numpy as np
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d104'),str(HERE)]
from recent_turn import replay_one,line_level,scale_of,RULES
R111=ROOT/'r1d111'/'results'
R112=ROOT/'r1d112'/'results'
SOURCE_FILES={
 'candidates':('r111','R1D111_INDEPENDENT_METHOD_LINES_PIT.csv'),
 'r112_daily':('r112','R1D112_MULTISCALE_ACTIVE_LINES_DAILY_PIT.csv'),
}
CASES=[('039840','디오','2024-09-09'),('039840','디오','2024-09-10'),('039840','디오','2024-09-13'),
       ('071840','롯데하이마트','2024-03-13'),('071840','롯데하이마트','2024-03-14'),
       ('006400','삼성SDI','2024-02-16')]
# NEVER passed into replay_one. Only used to audit output AFTER it is frozen.
REFERENCES=[
 ('039840','DIO_BULL_BODY_UPPER','BULL_BODY_TOP','UPPER','2024-09-03','2024-09-09'),
 ('039840','DIO_LOW_WICK_LOWER','WICK_LOW_PIVOT2','LOWER','2024-08-26','2024-09-06'),
 ('071840','LOTTE_BULL_BODY_UPPER','BULL_BODY_TOP','UPPER','2024-02-20','2024-03-13'),
 ('071840','LOTTE_LOW_WICK_LOWER','WICK_LOW_PIVOT2','LOWER','2024-02-01','2024-03-11')]


def read_sources(r111=R111,r112=R112):
    roots={'r111':Path(r111),'r112':Path(r112)}
    paths={k:roots[part]/fname for k,(part,fname) in SOURCE_FILES.items()}
    for p in paths.values():
        if not p.exists():raise FileNotFoundError('Required upstream full results missing: '+str(p))
    hashes={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    src={k:pd.read_csv(p,dtype={'code':str},low_memory=False) for k,p in paths.items()}
    for d in src.values():d['code']=d.code.astype(str).str.zfill(6)
    if src['candidates'][['code','algorithm','line_id']].duplicated().any():raise AssertionError('Ambiguous R111 line id')
    if src['r112_daily'][['code','date','algorithm','scale','kind']].duplicated().any():raise AssertionError('Ambiguous R112 lane')
    if src['r112_daily'].groupby(['code','date']).size().ne(24).any():raise AssertionError('R112 24-lane continuity failure')
    for key in ['v1','v2','m','i1','i2','known_idx','known_date','line_id']:
        if key not in src['candidates']:raise AssertionError('Missing immutable anchor '+key)
    return src,paths,hashes


def _ref_audit(cands,proposed,old_states):
    out=[]
    for code,label,family,kind,a1,a2 in REFERENCES:
        for alg in sorted(cands[cands.code.eq(code)].algorithm.unique()):
            sub=cands[cands.code.eq(code)&cands.algorithm.eq(alg)&cands.family.eq(family)&
                      cands.kind.eq(kind)&cands.a1.eq(a1)&cands.a2.eq(a2)]
            if len(sub)>1:raise AssertionError('Ambiguous human audit candidate '+label)
            base={'code':code,'algorithm':alg,'manual_label':label,'family':family,'kind':kind,
                  'anchor1':a1,'anchor2':a2,'discovered':bool(len(sub)),'manual_used_in_selection':False}
            if len(sub):
                q=sub.iloc[0];id=str(q.line_id)
                p=proposed[proposed.code.eq(code)&proposed.algorithm.eq(alg)&proposed.line_id.eq(id)]
                o=old_states[old_states.code.eq(code)&old_states.algorithm.eq(alg)&old_states.active_line_id.eq(id)]
                base.update(known_date=str(q.known_date),known_idx=int(q.known_idx),scale=scale_of(q),
                   proposed_first_active_date=str(p.iloc[0].date) if len(p) else '',
                   baseline_first_active_date=str(o.iloc[0].date) if len(o) else '',
                   proposed_selected_ever=bool(len(p)),baseline_selected_ever=bool(len(o)))
            else:base.update(known_date='',known_idx=None,scale='',proposed_first_active_date='',
                 baseline_first_active_date='',proposed_selected_ever=False,baseline_selected_ever=False)
            out.append(base)
    return pd.DataFrame(out)


def _comparison(proposed,original,catalog):
    old=original[['code','algorithm','date','scale','kind','idx','active_line_id','active_level','state','event','anchor1','anchor2']].copy()
    old=old.rename(columns={'active_line_id':'r112_line_id','active_level':'r112_level',
          'state':'r112_state','event':'r112_event','anchor1':'r112_anchor1','anchor2':'r112_anchor2'})
    d=proposed.merge(old,on=['code','algorithm','date','scale','kind','idx'],validate='one_to_one')
    d['r112_line_id']=d.r112_line_id.fillna('')
    d['different_line']=d.line_id.ne(d.r112_line_id)
    d['both_present']=d.line_id.ne('')&d.r112_line_id.ne('')
    lookup=catalog[['code','algorithm','line_id','i1','i2','v1','m']].rename(columns={
       'line_id':'r112_line_id','i1':'r112_i1','i2':'r112_i2',
       'v1':'r112_v1','m':'r112_m'})
    d=d.merge(lookup,on=['code','algorithm','r112_line_id'],how='left',validate='many_to_one')
    present=d.r112_line_id.ne('')
    if d.loc[present,'r112_i1'].isna().any():
        raise AssertionError('Missing frozen R112 line in original immutable catalog')
    exact=d.r112_v1+d.r112_m*(d.idx-d.r112_i1)
    delta=(exact-d.r112_level).abs()
    if (delta[present]>.02).any():
        raise AssertionError('R112 source price failed exact anchor projection')
    age=d.idx-d.r112_i2
    d['r112_older_than_recent_endpoint_limit']=False
    for scale,rule in RULES.items():
        mask=present & d.scale.eq(scale)
        d.loc[mask,'r112_older_than_recent_endpoint_limit']=age.loc[mask]>rule['endpoint_age']
    d=d.drop(columns=['r112_i1','r112_i2','r112_v1','r112_m'])
    return d


def _expost_retests(proposed_events,raw, *, horizon=10):
    """Future touch observations in a SEPARATE result, NEVER ranking features."""
    if raw is None:return pd.DataFrame()
    required={'idx','date','open','high','low','close'}
    if not required.issubset(raw.columns):raise AssertionError('raw OHLCV lacks '+str(required-set(raw.columns)))
    bars=raw.set_index('idx');date_map=bars.date.astype(str).to_dict()
    out=[]
    for ev in proposed_events.to_dict('records'):
        event=ev.get('prior_line_cross','NONE')
        if event=='NONE':continue
        t=int(ev['idx']);source_line=str(ev['prior_line_id']);orig_price=ev.get('prior_line_level_on_day')
        if not source_line or pd.isna(orig_price):continue
        # Exact source line comes from immutable R111, handled by caller.
        # A historical confirmed breakout watch needs its precise future
        # projected level; caller supplies all geometry to this function.
        for dt in range(1,horizon+1):
            i=t+dt
            if i not in bars.index:continue
            q=bars.loc[i]
            out.append({'code':ev['code'],'algorithm':ev['algorithm'],'date':ev['date'],
              'idx':t,'cross':event,'line_id':source_line,'future_idx':i,
              'future_date':date_map[i],'future_close_EXPOST':float(q.close),
              'future_high_EXPOST':float(q.high),'future_low_EXPOST':float(q.low),
              'warning':'FUTURE_ONLY_NOT_ELIGIBLE_FOR_SELECTION'})
    return pd.DataFrame(out)


def _plot_case(source_raw,all_lines,baseline,alternative,code,name,date,out):
    """Actual immutable anchors and actual OHLCV; no synthetic candles."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.is_file():font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    raw=source_raw.copy();raw['date']=pd.to_datetime(raw.date)
    if 'idx' not in raw:raw['idx']=np.arange(len(raw))
    raw=raw.sort_values('idx')
    s=raw[raw.date.dt.strftime('%Y-%m-%d').eq(date)]
    if len(s)!=1:return None
    t=int(s.iloc[0].idx);bars=raw[(raw.idx.between(t-65,t))].copy()
    if len(bars)<20:return None
    fig,(ax,bx)=plt.subplots(2,1,figsize=(16,9),sharex=True,
            gridspec_kw={'height_ratios':[4.5,1]},layout='constrained')
    for _,bar in bars.iterrows():
        j=int(bar.idx);up=float(bar.close)>=float(bar.open);color='#d94848' if up else '#2778b9'
        ax.vlines(j,float(bar.low),float(bar.high),color=color,lw=.8,zorder=3)
        ax.add_patch(Rectangle((j-.30,min(float(bar.open),float(bar.close))),.6,
              max(.3,abs(float(bar.close)-float(bar.open))),facecolor=color,alpha=.9,zorder=3))
        bx.bar(j,float(bar.amount)/1e8,width=.7,color=color,alpha=.6)
    # As-of close, TS_Body_Wick only for legible side-by-side comparison.
    line_catalog=all_lines[(all_lines.code.eq(code))&(all_lines.algorithm.eq('TS_BODY_WICK_PROXY'))].set_index('line_id')
    alt=alternative[(alternative.code.eq(code))&(alternative.algorithm.eq('TS_BODY_WICK_PROXY'))&(alternative.date.eq(date))]
    old=baseline[(baseline.code.eq(code))&(baseline.algorithm.eq('TS_BODY_WICK_PROXY'))&(baseline.date.eq(date))]
    primary=alt[alt.scale.isin(['SHORT','MEDIUM'])&alt.kind.eq('UPPER')].sort_values('scale')
    # Plot true line segments through the two anchor values, never inferred from percent slope.
    plotted=set()
    def draw(lid,color,label,style='-',width=2.1,alpha=.9):
        if not lid or lid in plotted:return
        if lid not in line_catalog.index:raise AssertionError('Unknown chart line ID '+lid)
        q=line_catalog.loc[lid];known=int(q.known_idx)
        if known>t:raise AssertionError('Future-known chart line '+lid)
        ix=np.arange(max(int(bars.idx.min()),int(q.i1)),t+1,dtype=int)
        if not len(ix):return
        vals=np.array([line_level(q,i) for i in ix]);pre=ix<known;live=ix>=known
        if pre.any():ax.plot(ix[pre],vals[pre],c=color,ls=':',lw=width*.65,alpha=.46)
        if live.any():ax.plot(ix[live],vals[live],c=color,ls=style,lw=width,alpha=alpha,label=label)
        for k,v in [(int(q.i1),float(q.v1)),(int(q.i2),float(q.v2))]:
            if bars.idx.min()<=k<=t:
                ax.scatter([k],[v],s=46,c=color,edgecolors='black',linewidths=.5,zorder=10)
        plotted.add(lid)
    for _,r in primary.iterrows():
        if r.line_id:draw(str(r.line_id),'#e67e22',f'R116 {r.scale} upper | {r.anchor1} / {r.anchor2}',width=2.7)
    for _,r in old[old.scale.isin(['SHORT','MEDIUM'])&old.kind.eq('UPPER')].iterrows():
        if isinstance(r.active_line_id,str) and r.active_line_id:
            draw(r.active_line_id,'#6b7280',f'ORIGINAL R112 {r.scale} upper',style='--',width=1.5,alpha=.55)
    # Lower-side R116 line, only the nearest short/medium candidate.
    lo=alt[alt.scale.isin(['SHORT','MEDIUM'])&alt.kind.eq('LOWER')&alt.line_id.ne('')].copy()
    if len(lo):
        lo['dist']=(lo.level.astype(float)-float(s.iloc[0].close)).abs()
        r=lo.sort_values('dist').iloc[0]
        draw(str(r.line_id),'#168c78',f'R116 {r.scale} lower',width=1.9)
    manual={('039840','2024-09-09'):('2024-09-03','2024-09-09'),
            ('039840','2024-09-10'):('2024-09-03','2024-09-09'),
            ('039840','2024-09-13'):('2024-09-03','2024-09-09'),
            ('071840','2024-03-13'):('2024-02-20','2024-03-13'),
            ('071840','2024-03-14'):('2024-02-20','2024-03-13')}
    if (code,date) in manual:
        a,b=manual[(code,date)];m=line_catalog[(line_catalog.a1.eq(a))&(line_catalog.a2.eq(b))&line_catalog.family.eq('BULL_BODY_TOP')]
        if len(m)==1:
            lid=str(m.index[0])
            # Human annotation remains distinguishable if it is already auto-selected.
            if lid not in plotted:draw(lid,'#7c3aed','MANUAL audit reference (not forced into selection)',style='-.',width=1.7,alpha=.8)
    ax.axvline(t,color='black',lw=1,ls=':');ax.set_ylabel('KRW')
    ax.set_title(f'{name} {date} | R116 Recent-Turn vs ORIGINAL R112 | REAL OHLCV, known-at-close geometry')
    ax.legend(loc='upper left',fontsize=8,ncol=2);ax.grid(alpha=.17);bx.grid(alpha=.14)
    tick=np.linspace(int(bars.idx.min()),t,9,dtype=int)
    by_idx=raw.set_index('idx').date.dt.strftime('%Y-%m-%d').to_dict()
    bx.set_xticks(tick);bx.set_xticklabels([by_idx.get(i,str(i))[5:] for i in tick],rotation=25)
    bx.set_ylabel('Amount (100M KRW)')
    out.mkdir(parents=True,exist_ok=True)
    file=out/f'{code}_{date}_R1D116_REAL_ANCHOR_ACTIVE_COMPARISON.png'
    fig.savefig(file,dpi=145);plt.close(fig);return str(file)


def run(*,r111=R111,r112=R112,output=None,raw_provider=None,draw=True,codes=None):
    source,paths,source_hashes=read_sources(r111,r112)
    output=Path(output or HERE/'results');output.mkdir(parents=True,exist_ok=True)
    if draw:(output/'charts').mkdir(exist_ok=True)
    original=source['r112_daily'];catalog=source['candidates']
    stocks=sorted(codes or original.code.unique())
    all_daily=[];all_events=[];all_shortlists=[];charts=[]
    for code in stocks:
        mark=original[original.code.eq(code)]
        for alg in sorted(mark.algorithm.unique()):
            frozen=mark[mark.algorithm.eq(alg)]
            stamps=frozen[frozen.scale.eq('SHORT')&frozen.kind.eq('UPPER')][['idx','date','close']]
            candidates=catalog[catalog.code.eq(code)&catalog.algorithm.eq(alg)].to_dict('records')
            daily,events,ranking=replay_one(code,alg,candidates,stamps,top_n=3)
            all_daily.append(daily);all_events.append(events);all_shortlists.append(ranking)
        print('CASE_DONE',code,'real days',int(mark.date.nunique()),'methods',int(mark.algorithm.nunique()),flush=True)
    prop=pd.concat(all_daily,ignore_index=True)
    ev=pd.concat(all_events,ignore_index=True)
    shortlist=pd.concat(all_shortlists,ignore_index=True)
    cmp=_comparison(prop,original[original.code.isin(stocks)],catalog[catalog.code.isin(stocks)])
    refs=_ref_audit(catalog[catalog.code.isin(stocks)],prop,original[original.code.isin(stocks)])
    if draw:
        if raw_provider is None:
            from research_r1d96 import load as raw_provider
            from research_r1d104 import _verify
            _verify()
        for code,name,date in CASES:
            if code not in stocks:continue
            real=raw_provider(code).copy()
            if 'idx' not in real:real.insert(0,'idx',np.arange(len(real),dtype=int))
            file=_plot_case(real,catalog,original,prop,code,name,date,output/'charts')
            if file:charts.append(Path(file).name)
    def save(frame,name):
        frame.to_csv(output/name,index=False,encoding='utf-8-sig')
    save(prop,'R1D116_RECENT_TURN_ACTIVE_LANES_PIT.csv')
    save(ev,'R1D116_ACTIVE_LINE_CHANGES_PIT.csv')
    save(shortlist,'R1D116_CANDIDATE_TOP3_SCORE_COMPONENTS_PIT.csv')
    save(cmp,'R1D116_ORIGINAL_R112_VS_RECENT_TURN_PIT.csv')
    save(refs,'R1D116_MANUAL_REFERENCES_POSTSELECTION_AUDIT.csv')
    for key,p in paths.items():
        if hashlib.sha256(p.read_bytes()).hexdigest()!=source_hashes[key]:raise AssertionError('Original file modified '+key)
    audit={'version':'R1D116_RECENT_TURN_COMPETITION_RESEARCH_ONLY',
        'input_sha256':source_hashes,'source_unchanged':True,'manual_not_in_selection':True,
        'candidate_catalog_rows':int(len(catalog)),'baseline_original_state_rows':int(len(original)),
        'new_independent_daily_lanes':int(len(prop)),
        'days_with_different_selected_line':int(cmp.different_line.sum()),
        'new_candidates_top3_rows':int(len(shortlist)),
        'line_change_log_rows':int(len(ev)),
        'reference_audit':refs.to_dict('records'),
        'chart_files':charts,'parameters':RULES,
        'limitations':['Candidate score and recency rules are exploratory geometry heuristics, not trained profit predictions.',
        'Manual reference lines are evaluated AFTER selection and NEVER admitted as weights.',
        'R112 decisions remain immutable; R116 is an independent challenger only.',
        't daily selections are available AFTER t close, never earlier.',
        'Old broken lines have historical identity but R116 does not declare price-direction forecast success.',
        'Candidate selection is not validated out-of-sample; many overlapping observations share the same 3 stocks.',
        'Backfilled exact historical anchors can be displayed for context only after their known_date; dotted pre-known segments are illustrative.']}
    (output/'R1D116_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ('candidate_catalog_rows','baseline_original_state_rows','new_independent_daily_lanes','days_with_different_selected_line','line_change_log_rows','source_unchanged')},ensure_ascii=False),flush=True)
    return audit

if __name__=='__main__':run()
