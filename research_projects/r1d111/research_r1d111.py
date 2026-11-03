"""R1-D111: four independent PIT automatic trendline method comparisons.

No proprietary service code. Public-method-inspired, transparent independent
proxies; no model calibration on user's four reference lines or future returns.
All R1-D106, D109, D110 inputs are SHA-preserved and untouched.
"""
from __future__ import annotations
import hashlib,json,sys
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent;ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d104'),str(HERE)]
from algorithms import prepare,make_anchors,generate_candidates,quality,value,ALGORITHMS,PARAMETERS
from lifecycle import monitor,assess_manual,archived_line_retests
from patterns import pattern_asof

R109=ROOT/'r1d109'/'results'
R106=ROOT/'r1d106'/'results'
REQUIRED={
'context':('r1d109','R1D109_ALL_DAYS_PIT_TREND_CHANGE_FULL_CONFLUENCE.csv'),
'baseline':('r1d106','R1D106_ALL_IMMUTABLE_LINES.csv')}
REFS={
 '039840':[
  ('2024-09-03','2024-09-09','BULL_BODY_TOP','UPPER','DIO_BULL_BODY_UPPER'),
  ('2024-08-26','2024-09-06','WICK_LOW_PIVOT2','LOWER','DIO_LOW_WICK_LOWER')],
 '071840':[
  ('2024-02-20','2024-03-13','BULL_BODY_TOP','UPPER','LOTTE_BULL_BODY_UPPER'),
  ('2024-02-01','2024-03-11','WICK_LOW_PIVOT2','LOWER','LOTTE_LOW_WICK_LOWER')]}
CASE_DATES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-13')]
TECH_FIELDS=['candle_body_share_pct','candle_upper_wick_share_pct','candle_lower_wick_share_pct',
 'close_vs_prev3_high_max_pct','volume_vs_prev20','amount_vs_prev20',
 'ma5_slope20_pct','ma20_slope20_pct','ma60_slope20_pct','ma112_slope20_pct','ma224_slope20_pct',
 'ma5_gap_pct','ma20_gap_pct','ma60_gap_pct','ma224_gap_pct',
 'bb20_width_pct','bb20_width_delta5_pp','bb20_upper_slope5_pct',
 'bb30_width_pct','bb40_width_pct','env20_10_position','env40_10_position',
 'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct','ma_cloud_fast_height_vs_price_pct',
 'ichimoku_current_thickness_pct','pivot_structure_PIT','candidate_trend_change_today']


def load_inputs(context_path=R109/'R1D109_ALL_DAYS_PIT_TREND_CHANGE_FULL_CONFLUENCE.csv',
                baseline_path=R106/'R1D106_ALL_IMMUTABLE_LINES.csv'):
    paths={'context':Path(context_path),'baseline':Path(baseline_path)}
    for k,p in paths.items():
        if not p.is_file():raise FileNotFoundError(f'Run earlier research before R1-D111: {p}')
    hashes={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    context=pd.read_csv(paths['context'],dtype={'code':str},low_memory=False)
    baseline=pd.read_csv(paths['baseline'],dtype={'code':str},low_memory=False)
    for d in (context,baseline):d['code']=d.code.str.zfill(6)
    if context[['code','date']].duplicated().any() or baseline[['code','line_id']].duplicated().any():
        raise AssertionError('upstream data duplicate identity')
    for col in ['code','name','date','open','high','low','close','volume','amount',*TECH_FIELDS]:
        if col not in context:raise AssertionError('missing R109 context '+col)
    if any('EXPOST' in c.upper() for c in context.columns):raise AssertionError('future labels in PIT context')
    return context,baseline,paths,hashes


def _chart(d,events,states,lines,code,name,asof,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    f=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if f.exists():font_manager.fontManager.addfont(str(f));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    idxs=np.flatnonzero(d.date.dt.strftime('%Y-%m-%d').eq(asof))
    if len(idxs)!=1:return None
    t=int(idxs[0]);start=max(0,t-37);end=min(len(d)-1,t+10)
    fig,ax=plt.subplots(figsize=(15,8),layout='constrained')
    for j in range(start,end+1):
        r=d.iloc[j];col='#d85950' if r.close>=r.open else '#3475b6'
        ax.vlines(j,r.low,r.high,color=col,lw=.7,zorder=3)
        ax.add_patch(Rectangle((j-.24,min(r.open,r.close)),.48,max(.15,abs(r.close-r.open)),facecolor=col,edgecolor=col,alpha=.85,zorder=3))
    chosen=states[(states.date.eq(asof))&(states.kind.eq('UPPER'))]
    byid={l['line_id']:l for l in lines}
    colors={'TV_PIVOT_ATR_PROXY':'#a855f7','TS_BODY_WICK_PROXY':'#ed8b14',
            'AC_PATTERN_FLOW_PROXY':'#03a67a','OSS_TOUCH_PEN_PROXY':'#333333'}
    for _,r in chosen.iterrows():
        if r.active_line_id and r.active_line_id in byid:
            l=byid[r.active_line_id];left=max(start,l['known_idx'])
            if left<=t:
                x=np.arange(left,t+1);ax.plot(x,[value(l,k) for k in x],lw=2.2,
                   label=f"AUTO {r.algorithm.replace('_PROXY','')} ({l['a1'][5:]}→{l['a2'][5:]})",color=colors[r.algorithm],alpha=.82)
    # Reference lines only drawn for an EXPLICIT AUDIT, not used in auto selection/ranking.
    for a,b,fam,kind,label in REFS.get(code,[]):
        x=[l for l in lines if l['a1']==a and l['a2']==b and l['family']==fam and l['algorithm']=='TS_BODY_WICK_PROXY']
        if x and x[0]['known_idx']<=t:
            l=x[0];left=max(start,l['known_idx']);xx=np.arange(left,t+1)
            # Faint historical anchor segment is illustration only, never an early signal.
            past=np.arange(max(start,l['i1']),min(t,l['known_idx'])+1)
            if len(past):ax.plot(past,[value(l,k) for k in past],ls=':',lw=.8,alpha=.24,color='#f59e0b' if kind=='UPPER' else '#10b981')
            if len(xx):ax.plot(xx,[value(l,k) for k in xx],ls='--',lw=2.7,color='#ffcc00' if kind=='UPPER' else '#22c55e',
                 label=f'MANUAL AUDIT only {a[5:]}→{b[5:]} {kind}')
    ax.axvline(t,color='#ef4444',lw=1.6,ls=':');ax.axvspan(t,end,color='#f59e0b',alpha=.06)
    ax.grid(alpha=.15)
    ticks=np.linspace(start,end,9,dtype=int);ax.set_xticks(ticks);ax.set_xticklabels(d.date.iloc[ticks].dt.strftime('%m-%d'),rotation=25)
    ax.legend(loc='best',fontsize=7,ncol=2)
    ax.set_title(f'{name} | {asof} | 4 method ACTIVE resistance lines + manual-only reference',fontsize=13)
    ax.set_ylabel('KRW / PIT confirmed anchors only')
    p=out/'charts'/f'{code}_{asof}_R1D111_FOUR_METHODS.png';fig.savefig(p,dpi=135);plt.close(fig)
    return p.name


def run(*,output=None,base_context=None,base_baseline=None,raw_provider=None,draw=True,codes=None):
    output=Path(output or HERE/'results');output.mkdir(parents=True,exist_ok=True)
    if draw:(output/'charts').mkdir(exist_ok=True)
    ctx,bsl,paths,hashes=load_inputs(
      base_context or R109/'R1D109_ALL_DAYS_PIT_TREND_CHANGE_FULL_CONFLUENCE.csv',
      base_baseline or R106/'R1D106_ALL_IMMUTABLE_LINES.csv')
    if raw_provider is None:
        # Identical frozen KRX OHLCV underlying R1-D96..R1-D110.
        from research_r1d96 import load as raw_provider
        from research_r1d104 import _verify
        _verify()
    codes=sorted(codes) if codes else sorted(ctx.code.unique())
    all_candidates=[];all_states=[];all_events=[];all_manual=[];all_counts=[];all_charts=[];all_retests=[];all_patterns=[]
    source_lengths={};source_dates={}
    for code in codes:
        subgroup=ctx[ctx.code.eq(code)].copy().sort_values('date')
        if subgroup.empty:raise AssertionError('missing source case '+code)
        raw=raw_provider(code);d=prepare(raw)
        name=str(subgroup.iloc[0]['name']);source_lengths[code]=len(d)
        source_dates[code]=(str(d.date.iloc[0].date()),str(d.date.iloc[-1].date()))
        anchors=make_anchors(d)
        for algorithm in ALGORITHMS:
            lines=generate_candidates(d,algorithm,anchors)
            states,events=monitor(d,algorithm,lines)
            if algorithm=='AC_PATTERN_FLOW_PROXY':
                # Dates chosen without future returns: every 10 bars + auto transition dates.
                times=set(range(550,len(d),10))
                if len(events):times.update(int(j) for j in events.idx)
                for cc,nm,day in CASE_DATES:
                    if cc==code:
                        ix=np.flatnonzero(d.date.dt.strftime('%Y-%m-%d').eq(day))
                        if len(ix):times.add(int(ix[0]))
                for t0 in sorted(t for t in times if 0<=t<len(d)):
                    pats=pattern_asof(d,lines,t0)
                    for rec in pats:rec.update({'code':code,'name':name})
                    all_patterns.extend(pats)
            # Align exported monitor and any future technical merges by real date.
            states.insert(0,'code',code);states.insert(1,'name',name)
            if len(events):events.insert(0,'code',code);events.insert(1,'name',name)
            if lines:
                candidate=pd.DataFrame(lines);candidate.insert(0,'code',code);candidate.insert(1,'name',name)
                all_candidates.append(candidate)
            all_states.append(states)
            if len(events):
                all_events.append(events)
                ret=archived_line_retests(d,algorithm,lines,events)
                if len(ret):
                    ret.insert(0,'code',code);ret.insert(1,'name',name)
                    all_retests.append(ret)
            if code in REFS:
                manual=assess_manual(d,algorithm,lines,states,REFS[code])
                manual.insert(0,'code',code);manual.insert(1,'name',name)
                # Frozen R106 reference is a separate baseline; never inserted into algorithm selection.
                for i,r in manual.iterrows():
                    b=bsl[(bsl.code.eq(code))&(bsl.a1.eq(r.a1))&(bsl.a2.eq(r.a2))&(bsl.family.eq(r.family))]
                    manual.loc[i,'found_in_frozen_R106']=bool(len(b))
                    manual.loc[i,'R106_first_known_date']=str(b.iloc[0].known_date) if len(b) else None
                all_manual.append(manual)
            all_counts.append({'code':code,'name':name,'algorithm':algorithm,
                'raw_history_rows':len(d),'candidate_lines':len(lines),'state_rows':len(states),
                'transitions':len(events),'reanchors':int(events.event.eq('REANCHORED_NEW_CONFIRMED_PIVOT_OR_CANDLE').sum()) if len(events) else 0,
                'confirmed_breaks':int(events.event.eq('BREAK_CONFIRMED_TWO_CLOSES').sum()) if len(events) else 0,
                'stale_retirements':int(events.event.eq('RETIRED_STALE').sum()) if len(events) else 0})
            print('CASE_METHOD_DONE',code,algorithm,'candidates',len(lines),'changes',len(events),flush=True)
        # chart after collecting all algorithms of this code
        if draw:
            events=pd.concat([x for x in all_events if len(x) and x.code.iloc[0]==code],ignore_index=True) if all_events else pd.DataFrame()
            states=pd.concat([x for x in all_states if x.code.iloc[0]==code],ignore_index=True)
            lines=[rec for p in all_candidates if p.code.iloc[0]==code for rec in p.to_dict('records')]
            for code0,nm,day in CASE_DATES:
                if code0==code:
                    namechart=_chart(d,events,states,lines,code,nm,day,output)
                    if namechart:all_charts.append(namechart)
    def export(rows,filename):
        if isinstance(rows,pd.DataFrame):p=rows
        elif rows and isinstance(rows[0],pd.DataFrame):p=pd.concat(rows,ignore_index=True)
        else:p=pd.DataFrame(rows)
        p.to_csv(output/filename,index=False,encoding='utf-8-sig');return p
    candidates=export(all_candidates,'R1D111_INDEPENDENT_METHOD_LINES_PIT.csv')
    states=export(all_states,'R1D111_FOUR_METHODS_ACTIVE_LINES_DAILY_PIT.csv')
    transitions=export(all_events,'R1D111_DYNAMIC_REANCHOR_BREAK_ARCHIVE_PIT.csv')
    manual=export(all_manual,'R1D111_HUMAN_REFERENCE_RECALL_AND_TIMING_AUDIT.csv')
    retests=export(all_retests,'R1D111_OLD_LINE_ROLE_FLIP_RETESTS_PIT.csv')
    patterns=export(all_patterns,'R1D111_AC_PATTERN_CANDLE_VOLUME_GEOMETRY_PIT.csv')
    counts=export(all_counts,'R1D111_FOUR_ALGORITHM_COMPARISON_COUNTS.csv')
    # Preserve the user's complete indicator system as *context*, not a return-fitted score.
    context=ctx[['code','date','name',*TECH_FIELDS]].copy()
    context=context[context.code.isin(codes)].copy()
    merged=transitions.merge(context,on=['code','date'],how='left',validate='many_to_one',suffixes=('','_context')) if len(transitions) else pd.DataFrame()
    if len(merged) and merged.ma20_slope20_pct.isna().all():raise AssertionError('missing MA context merge')
    export(merged,'R1D111_TRANSITIONS_CANDLE_MA_BB_ENV_CLOUD_VOLUME_PIT.csv')
    # Exact point-in-time snapshots for user's hand-drawn dates only as EVALUATION.
    cases=[]
    for code,name,date in CASE_DATES:
        snap=states[(states.code.eq(code))&(states.date.eq(date))]
        if len(snap)!=len(ALGORITHMS)*2:raise AssertionError('missing manual chart snapshot')
        for q in snap.to_dict('records'):
            q.update({'reference_case_only':True,'comparison_asof':date})
            subset=context[(context.code.eq(code))&(context.date.eq(date))]
            if len(subset):q.update({k:subset.iloc[0][k] for k in TECH_FIELDS})
            cases.append(q)
    export(cases,'R1D111_TWO_STOCK_REFERENCE_ASOF_COMPARISON_PIT.csv')
    # Stage category counts are NOT distinct trade signals or performance metrics.
    summary={'version':'R1D111_FOUR_PUBLIC_METHOD_INSPIRED_PIT_DYNAMIC_TREND_COMPARE_RESEARCH_ONLY',
      'algorithms':list(ALGORITHMS),'parameters':PARAMETERS,'stocks':len(codes),
      'source_rows':source_lengths,'source_date_ranges':source_dates,
      'total_candidate_lines':len(candidates),'total_daily_states':len(states),
      'total_dynamic_transitions':len(transitions),'role_retest_rows':len(retests),'pattern_geometry_rows':len(patterns),'human_reference_rows':len(manual),
      'manual_candidates_detected':int(manual.candidate_detected.sum()) if len(manual) else 0,
      'reference_cases':CASE_DATES,'charts':all_charts,
      'source_sha256_before':hashes,'source_sha256_after':{},
      'official_reproduction':False,'future_returns_used_for_line_selection':False,
      'is_predictive_backtest':False,'single_algorithm_winner_claimed':False,
      'limitations':['Four independent public-concept-inspired PROXIES; vendor proprietary internals not available.',
        'TradingView proxy uses confirmed 5/25 local pivots and ATR prominence, but not exact vendor ZigZag touch geometry.',
        'TrendSpider proxy approximates Wick/Body and activation; not vendor ranking or gap-island implementation.',
        'Autochartist proxy rates two-edge geometry, clarity, uniformity, breakout candle and flow; NOT vendor pattern engine.',
        'Open-source proxy imitates contact/penetration principles; does NOT import/copy pytrendline/trendln code.',
        'User-supplied anchors used in an OUT-OF-SELECTION audit only.',
        'ATR14 uses trailing 14-period simple TR average; official implementations may differ.',
        'Daily OHLCV cannot establish intraday order of indicators.',
        'Two-close break rule is a research design, not vendor-specific rule.',
        'No 3-stock findings imply profitability or broad market generalization.']}
    summary['source_sha256_after']={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    if summary['source_sha256_before']!=summary['source_sha256_after']:raise AssertionError('UPSTREAM_SHA256_CHANGED')
    summary['UPSTREAM_SHA256_UNCHANGED']=True
    (output/'R1D111_AUDIT.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2,default=str),encoding='utf8')
    print('SUMMARY',json.dumps({k:summary[k] for k in ('stocks','total_candidate_lines','total_daily_states','total_dynamic_transitions','manual_candidates_detected','UPSTREAM_SHA256_UNCHANGED')},ensure_ascii=False),flush=True)
    return summary

if __name__=='__main__':run()
