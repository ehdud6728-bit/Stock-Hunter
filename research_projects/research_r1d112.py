"""R1-D112 independent RESEARCH_ONLY multi-scale trendline role manager.

Consumes immutable R1-D111 candidate catalog and R1-D109 technical context;
old R1-D111 single-line baseline remains frozen and auditable.
"""
from __future__ import annotations
import sys,hashlib,json
from pathlib import Path
import pandas as pd
import numpy as np
HERE=Path(__file__).resolve().parent;ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d104'),str(ROOT/'r1d111'),str(HERE)]
from algorithms import prepare, ALGORITHMS
from multiscale import SCALES,CONFIG,monitor_multiscale,manual_audit,level,structure_asof

R111=ROOT/'r1d111'/'results';R109=ROOT/'r1d109'/'results'
FILES={
 'lines':'R1D111_INDEPENDENT_METHOD_LINES_PIT.csv',
 'baseline':'R1D111_FOUR_METHODS_ACTIVE_LINES_DAILY_PIT.csv',
 'manual':'R1D111_HUMAN_REFERENCE_RECALL_AND_TIMING_AUDIT.csv',
 'context':'R1D109_ALL_DAYS_PIT_TREND_CHANGE_FULL_CONFLUENCE.csv',
}
# Human references MUST remain confined to evaluation and chart rendering.
REFERENCES={
 '039840':[('2024-09-03','2024-09-09','BULL_BODY_TOP','UPPER','DIO_BULL_BODY_UPPER'),
           ('2024-08-26','2024-09-06','WICK_LOW_PIVOT2','LOWER','DIO_LOW_WICK_LOWER')],
 '071840':[('2024-02-20','2024-03-13','BULL_BODY_TOP','UPPER','LOTTE_BULL_BODY_UPPER'),
           ('2024-02-01','2024-03-11','WICK_LOW_PIVOT2','LOWER','LOTTE_LOW_WICK_LOWER')]}
CASES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-13'),('006400','삼성SDI','2024-02-16')]
TECH=['candle_body_share_pct','candle_upper_wick_share_pct','candle_lower_wick_share_pct',
      'close_vs_prev3_high_max_pct','amount_vs_prev20','volume_vs_prev20',
      'ma20_slope20_pct','ma60_slope20_pct','ma112_slope20_pct','ma224_slope20_pct',
      'ma20_gap_pct','ma224_gap_pct','bb20_width_pct','bb20_width_delta5_pp',
      'env20_10_position','env40_10_position','ma_cloud_fast_thickness_pct',
      'ma_cloud_mid_thickness_pct','ichimoku_current_thickness_pct']


def inputs(root111=R111,root109=R109):
    paths={k:(Path(root109) if k=='context' else Path(root111))/v for k,v in FILES.items()}
    for p in paths.values():
        if not p.is_file():raise FileNotFoundError('R1-D111 and R1-D109 must run before R1-D112: '+str(p))
    hashes={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    dfs={k:pd.read_csv(p,dtype={'code':str},low_memory=False) for k,p in paths.items()}
    for d in dfs.values():d['code']=d.code.str.zfill(6)
    if dfs['lines'][['code','line_id']].duplicated().any():raise AssertionError('duplicate candidate')
    if dfs['context'][['code','date']].duplicated().any():raise AssertionError('duplicate context date')
    if dfs['baseline'][['code','algorithm','date','kind']].duplicated().any():raise AssertionError('duplicate baseline lane')
    if any('EXPOST' in x.upper() for x in dfs['context'].columns):raise AssertionError('future-labelled PIT input')
    for k in ['date','open','high','low','close','volume','amount',*TECH]:
        if k not in dfs['context'].columns:raise AssertionError('missing technical context '+k)
    return dfs,paths,hashes


def _rebase(lines,offset,length):
    """Allow local exact-OHLCV subset smoke; GitHub full run uses offset=0."""
    if not offset:return lines
    out=[]
    for l in lines:
        if int(l['i1'])<offset or int(l['known_idx'])<offset or int(l['known_idx'])>=offset+length:continue
        l=dict(l)
        for key in ('i1','i2','known_idx'):l[key]=int(l[key])-offset
        out.append(l)
    return out


def _chart(d,states,lines,code,name,asof,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    f=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if f.exists():font_manager.fontManager.addfont(str(f));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    ids=np.flatnonzero(d.date.dt.strftime('%Y-%m-%d').eq(asof))
    if len(ids)!=1:return None
    t=int(ids[0]);st=max(0,t-42);en=min(len(d)-1,t+8)
    fig,ax=plt.subplots(figsize=(16,9),layout='constrained')
    for j in range(st,en+1):
        r=d.iloc[j];co='#d64d4d' if r.close>=r.open else '#3772b5'
        ax.vlines(j,r.low,r.high,color=co,lw=.7)
        ax.add_patch(Rectangle((j-.24,min(r.open,r.close)),.48,max(.1,abs(r.close-r.open)),color=co,alpha=.85))
    selected=states[(states.idx.eq(t)) & (states.kind.eq('UPPER'))]
    cmap={'SHORT':'#e88d10','MEDIUM':'#16a085','LONG':'#7c3aed'}
    source={l['line_id']:l for l in lines}
    for _,row in selected.iterrows():
        if not row.active_line_id or row.active_line_id not in source:continue
        l=source[row.active_line_id];a=max(st,int(l['known_idx']))
        if a>t:continue
        xs=np.arange(a,t+1)
        ax.plot(xs,[level(l,int(j)) for j in xs],lw=1.6,alpha=.6,color=cmap[row.scale],
            label=f'{row.algorithm.split("_")[0]} {row.scale} {l["a1"][5:]}→{l["a2"][5:]}')
    # Human reference overlay is only for transparency, not used for selection.
    for a,b,f,k,label in REFERENCES.get(code,[]):
        match=[l for l in lines if l['algorithm']=='TS_BODY_WICK_PROXY' and l['family']==f and l['a1']==a and l['a2']==b]
        if match and match[0]['known_idx']<=t:
            l=match[0];xs=np.arange(max(st,int(l['known_idx'])),t+1)
            ax.plot(xs,[level(l,int(j)) for j in xs],ls='--',color='#f59e0b' if k=='UPPER' else '#059669',lw=2.6,
                label=f'MANUAL AUDIT ONLY {a[5:]}→{b[5:]}')
    ax.axvline(t,color='black',ls=':',alpha=.7)
    ax.grid(alpha=.15);ticks=np.linspace(st,en,9,dtype=int);ax.set_xticks(ticks)
    ax.set_xticklabels(d.date.iloc[ticks].dt.strftime('%m-%d'),rotation=22)
    ax.set_ylabel('KRW');ax.set_title(f'{name} {asof} | R1D112 scale-separated automatic trendlines | manual reference = dashed')
    ax.legend(fontsize=6,ncol=3,loc='best')
    p=out/'charts'/f'{code}_{asof}_R1D112_MULTISCALE.png';fig.savefig(p,dpi=130);plt.close(fig)
    return p.name


def run(*,root111=R111,root109=R109,output=None,raw_provider=None,draw=True,codes=None,index_offsets=None):
    """Requires exactly same full raw KRX data for production GitHub run.

    For local smoke only, caller may supply subset OHLCV and corresponding
    original global index offsets; outputs must be labelled subset.
    """
    dfs,paths,hashes=inputs(root111,root109)
    if raw_provider is None:
        from research_r1d96 import load as raw_provider
        from research_r1d104 import _verify
        _verify()
    output=Path(output or HERE/'results');output.mkdir(parents=True,exist_ok=True)
    if draw:(output/'charts').mkdir(exist_ok=True)
    codes=sorted(codes) if codes else sorted(dfs['context'].code.unique())
    index_offsets=index_offsets or {}
    fullstates=[];allevents=[];allroles=[];manual=[];summaries=[];charts=[]
    for code in codes:
        ctx=dfs['context'][dfs['context'].code.eq(code)]
        if ctx.empty:raise AssertionError('missing technical context '+code)
        name=str(ctx.name.iloc[0]);d=prepare(raw_provider(code));offset=int(index_offsets.get(code,0))
        lineframes=dfs['lines'][dfs['lines'].code.eq(code)]
        reference=REFERENCES.get(code,[])
        for algorithm in ALGORITHMS:
            lns=_rebase(lineframes[lineframes.algorithm.eq(algorithm)].to_dict('records'),offset,len(d))
            states,events,roles=monitor_multiscale(d,algorithm,lns)
            states.insert(0,'code',code);states.insert(1,'name',name)
            fullstates.append(states)
            if len(events):events.insert(0,'code',code);events.insert(1,'name',name);allevents.append(events)
            if len(roles):roles.insert(0,'code',code);roles.insert(1,'name',name);allroles.append(roles)
            if reference:
                old=dfs['manual'][dfs['manual'].code.eq(code)]
                ma=manual_audit(d,algorithm,lns,states,reference,old)
                ma.insert(0,'code',code);ma.insert(1,'name',name);manual.append(ma)
            summaries.append({'code':code,'name':name,'algorithm':algorithm,'raw_days':len(d),
               'candidate_lines_in_replay':len(lns),'daily_state_rows':len(states),
               'active_created':int((states.event=='CREATE_ACTIVE').sum()),
               'reanchors':int((states.event=='REANCHOR_NEW_CONFIRMED').sum()),
               'two_close_breaks':int((states.event=='TWO_CLOSE_BREAK_ARCHIVE').sum()),
               'stale_archives':int((states.event=='ARCHIVE_STALE_DISPLAY').sum()),
               'role_retest_events':len(roles)})
        if draw:
            ss=pd.concat([x for x in fullstates if x.code.iloc[0]==code],ignore_index=True)
            # Drawing all selected lines doesn't alter selection.
            lns=_rebase(lineframes.to_dict('records'),offset,len(d))
            for c,n,day in CASES:
                if c==code:
                    img=_chart(d,ss,lns,code,n,day,output)
                    if img:charts.append(img)
        print('CASE_DONE',code,name,'raw_days',len(d),'algorithms',len(ALGORITHMS),'PIT_PASS',flush=True)
    def save(obj,name):
        out=pd.concat(obj,ignore_index=True) if isinstance(obj,list) and len(obj) else pd.DataFrame()
        if isinstance(obj,pd.DataFrame):out=obj
        out.to_csv(output/name,index=False,encoding='utf-8-sig');return out
    states=save(fullstates,'R1D112_MULTISCALE_ACTIVE_LINES_DAILY_PIT.csv')
    changes=save(allevents,'R1D112_MULTISCALE_TRANSITIONS_PIT.csv')
    geometry=pd.DataFrame()
    if len(states):
        # Attach confirmed HH/HL/LL/LH context only if independently available.
        confirmed=dfs['context'][['code','date','pivot_structure_PIT']].drop_duplicates(['code','date'])
        gframes=[]
        for code0,g in states.groupby('code'):
            z=structure_asof(g)
            z.insert(0,'code',code0);z.insert(1,'name',str(g.name.iloc[0]))
            gframes.append(z)
        geometry=pd.concat(gframes,ignore_index=True).merge(confirmed,on=['code','date'],how='left',validate='many_to_one')
    save(geometry,'R1D112_MULTISCALE_STRUCTURE_ASOF_PIT.csv')

    role=save(allroles,'R1D112_BROKEN_LINE_ROLE_RETEST_PIT.csv')
    ref=save(manual,'R1D112_HUMAN_REFERENCE_ACTIVE_COMPARISON_PIT.csv')
    stats=save(pd.DataFrame(summaries),'R1D112_ALGORITHM_LIFECYCLE_COUNTS.csv')
    # Confirm each lane is one and only one active/wait state on each asof.
    expected=sum(int(z['raw_days'])*6 for z in summaries)
    if len(states)!=expected or states[['code','algorithm','date','scale','kind']].duplicated().any():
        raise AssertionError('multiscale daily state lane integrity')
    # At R109 known source dates attach exact same-day context; never forward-join.
    if len(changes):
        technical=dfs['context'][['code','date',*TECH]].copy()
        annotated=changes.merge(technical,on=['code','date'],how='left',validate='many_to_one',indicator=True)
        annotated=annotated.rename(columns={'_merge':'tech_context_match'})
    else:annotated=pd.DataFrame()
    save(annotated,'R1D112_TRANSITIONS_CANDLE_MA_BB_ENV_CLOUD_FLOW_PIT.csv')
    # Compare original R111 one UPPER+LOWER / algo with 3-scale output.
    b=dfs['baseline'].copy();b=b[['code','algorithm','kind','date','active_line_id']]
    new=states[['code','algorithm','kind','scale','date','active_line_id']].copy()
    new=new.rename(columns={'active_line_id':'r112_active_line_id'})
    old=b.rename(columns={'active_line_id':'r111_single_active_id'})
    compare=new.merge(old,on=['code','algorithm','kind','date'],how='left',validate='many_to_one')
    compare['same_as_single_lane']=compare.r112_active_line_id.fillna('').eq(compare.r111_single_active_id.fillna(''))
    save(compare,'R1D112_R111_SINGLE_VS_MULTISCALE_DAILY_AUDIT.csv')
    after={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    if after!=hashes:raise AssertionError('upstream R111 or R109 mutated')
    audit={'version':'R1D112_MULTISCALE_ACTIVE_TRENDLINE_ROLE_RETEST_RESEARCH_ONLY',
        'github_full_history':not bool(index_offsets),'source_hashes_unchanged':True,
        'source_sha256':hashes,'scales':CONFIG,'algorithms':list(ALGORITHMS),
        'stocks':summaries,'daily_state_rows':len(states),'transition_rows':len(changes),
        'role_retest_rows':len(role),'structure_rows':len(geometry),'manual_reference_rows':len(ref),
        'manual_discovered':int(ref.discovered.sum()) if len(ref) else 0,
        'manual_new_active':int(ref.r112_active_ever.sum()) if len(ref) else 0,
        'same_as_single_lane_rows':int(compare.same_as_single_lane.sum()),
        'charts':charts,'manual_reference_used_to_select':False,'uses_future_returns_to_select':False,
        'limitations':['Explanatory tier thresholds; not optimized or predictive','Vendor-inspired proxies are not vendor products',
            'R111 candidate catalog unchanged, so candidate recall not expanded','Role retest is observed, not a guaranteed support/resistance',
            'Single/three-lane comparison is not investment performance','No selection tuned using manually selected reference cases',
            'Intraday ordering is unknowable from daily OHLCV','EXPOST profitability not calculated by this study']}
    (output/'R1D112_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ('daily_state_rows','transition_rows','role_retest_rows','manual_reference_rows','manual_new_active','structure_rows','source_hashes_unchanged')},ensure_ascii=False),flush=True)
    return audit

if __name__=='__main__':run()
