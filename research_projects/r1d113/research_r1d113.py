"""R1-D113 DUAL-TRACK structural patterns + preserved R1-D112 dynamic trendlines.

A: R112 scale-separated active trendline continuity is retained verbatim.
B: R113 independent box/channel/triangle/wedge candidate geometry.
C: PIT-only cross-lane attribution and separately labelled EXPOST outcomes.
Research-only; never touches LIVE, SEARCH, WATCH or old studies.
"""
from __future__ import annotations
import hashlib,json,sys
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d104'),str(HERE)]
from structures import scan, _raw, _level

R112=ROOT/'r1d112'/'results';R109=ROOT/'r1d109'/'results'
STATES='R1D112_MULTISCALE_ACTIVE_LINES_DAILY_PIT.csv'
CHANGES='R1D112_MULTISCALE_TRANSITIONS_PIT.csv'
CONTEXT='R1D109_ALL_DAYS_PIT_TREND_CHANGE_FULL_CONFLUENCE.csv'
TECH=['candle_body_share_pct','candle_upper_wick_share_pct','candle_lower_wick_share_pct',
 'close_vs_prev3_high_max_pct','amount_vs_prev20','volume_vs_prev20',
 'ma20_slope20_pct','ma112_slope20_pct','ma224_slope20_pct','ma20_gap_pct','ma224_gap_pct',
 'bb20_width_pct','bb20_width_delta5_pp','env20_10_position','env40_10_position',
 'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct',
 'ichimoku_current_thickness_pct']
CASE_DATES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-13'),('006400','삼성SDI','2024-02-16')]
# Manual illustrations ONLY used in audit/plot, NEVER in scans or selection.
AUDIT_REFERENCES=[('039840','2024-09-09','BULL_BODY_TOP','2024-09-03','2024-09-09'),
 ('071840','2024-03-13','BULL_BODY_TOP','2024-02-20','2024-03-13')]


def inputs(root112=R112,root109=R109):
    files={'states':Path(root112)/STATES,'changes':Path(root112)/CHANGES,'context':Path(root109)/CONTEXT}
    for p in files.values():
        if not p.is_file():raise FileNotFoundError('Run R1-D112/R1-D109 first: '+str(p))
    hashes={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in files.items()}
    d={k:pd.read_csv(p,dtype={'code':str},low_memory=False) for k,p in files.items()}
    for x in d.values():x['code']=x.code.str.zfill(6)
    if d['states'][['code','date','algorithm','scale','kind']].duplicated().any():raise AssertionError('R112 duplicate state')
    if d['context'][['code','date']].duplicated().any():raise AssertionError('R109 duplicate technical context')
    if any('EXPOST' in f.upper() for f in d['context'].columns):raise AssertionError('lookahead in context')
    for k in TECH:
        if k not in d['context']:raise AssertionError('missing technical '+k)
    return d,files,hashes


def _clean_level(v):return float(v) if pd.notna(v) and float(v)>0 else None


def _confluence(states,events,context):
    if events.empty:return pd.DataFrame()
    ss=states.copy();ss['date']=pd.to_datetime(ss['date']);ee=events.copy();ee['date']=pd.to_datetime(ee.date)
    # Prior-close R112 state is the ex-ante trendline reading.
    prev=ss[['date','algorithm','scale','kind','active_level','event','state','active_line_id']].copy()
    all_events=[]
    for r in ee.to_dict('records'):
        date=r['date'];index=ss[ss.date.eq(date)]
        pidx=ss[ss.date.lt(date)];prior=pidx[pidx.date.eq(pidx.date.max())] if len(pidx) else pidx
        px=float(r['close']);rec=dict(r)
        for prefix,q in (('previous_close',prior),('event_close',index)):
            upper=[x for x in q[q.kind.eq('UPPER')].active_level if _clean_level(x)]
            lower=[x for x in q[q.kind.eq('LOWER')].active_level if _clean_level(x)]
            rec[prefix+'_active_upper_lanes']=len(upper)
            rec[prefix+'_active_lower_lanes']=len(lower)
            rec[prefix+'_nearest_upper_distance_pct']=round(min(abs(100*(px/x-1)) for x in upper),4) if upper else None
            rec[prefix+'_nearest_lower_distance_pct']=round(min(abs(100*(px/x-1)) for x in lower),4) if lower else None
            rec[prefix+'_reanchors']=int(q.event.eq('REANCHOR_NEW_CONFIRMED').sum())
            rec[prefix+'_archives_on_break']=int(q.event.eq('TWO_CLOSE_BREAK_ARCHIVE').sum())
        all_events.append(rec)
    a=pd.DataFrame(all_events);a['date']=a.date.dt.strftime('%Y-%m-%d')
    if not context.empty:
        ctx=context[['date',*TECH]].copy()
        a=a.merge(ctx,on='date',how='left',validate='many_to_one')
    return a


def _expost(d,events,horizons=(1,3,5,10,20)):
    if events.empty:return pd.DataFrame()
    recs=[]
    for ev in events.to_dict('records'):
        t=int(ev['idx']);signal=ev['event']
        if signal not in ('BREAKOUT_UP','BREAKDOWN_DOWN'):continue
        px=float(d.iloc[t].close);rec={k:ev.get(k) for k in ('idx','date','pattern','basis','event','formed_asof','formed_idx','level','close')}
        for h in horizons:
            end=t+h;key=f'Dp{h}'
            rec[key+'_available']=bool(end<len(d))
            rec[key+'_return_EXPOST']=round(100*(float(d.iloc[end].close)/px-1),4) if end<len(d) else None
        # A break above an old upper boundary might become support; below lower
        # might become resistance. Only FUTURE candles produce retest labels.
        orig=float(ev['level']);touched=[];held=[]
        use_dynamic=all(pd.notna(ev.get(x)) for x in ('upper_m','upper_b','lower_m','lower_b'))
        for j in range(t+1,min(len(d),t+11)):
            if j>=len(d):break
            r=d.iloc[j];tol=max(.003*float(r.close),.3*float(ev.get('atr14',0) or 0))
            side='upper' if signal=='BREAKOUT_UP' else 'lower'
            line_now=(float(ev[side+'_m'])*j+float(ev[side+'_b'])) if use_dynamic else orig
            if float(r.low)<=line_now+tol and float(r.high)>=line_now-tol:
                touched.append(j)
                held.append(bool(float(r.close)>=line_now) if signal=='BREAKOUT_UP' else bool(float(r.close)<=line_now))
        rec['retest_observed_EXPOST']=bool(touched)
        rec['retest_first_date_EXPOST']=str(d.iloc[touched[0]].date.date()) if touched else None
        rec['retest_first_held_EXPOST']=held[0] if held else None
        rec['retest_two_held_EXPOST']=bool(len(held)>=2 and held[0] and held[1])
        rec['retest_scan_complete_EXPOST']=bool(t+10<len(d))
        recs.append(rec)
    return pd.DataFrame(recs)


def _plot(d,candidates,states,code,name,date,output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.exists():font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    dates=d.date.dt.strftime('%Y-%m-%d').tolist()
    if date not in dates:return None
    t=dates.index(date);start=max(0,t-48);end=min(len(d)-1,t+10)
    fig,(ax,bx)=plt.subplots(2,1,figsize=(16,10),sharex=True,
        gridspec_kw={'height_ratios':[5,1]},layout='constrained')
    for j in range(start,end+1):
        r=d.iloc[j];co='#dc4a4a' if r.close>=r.open else '#3175b7'
        ax.vlines(j,r.low,r.high,color=co,lw=.7,zorder=3)
        ax.add_patch(Rectangle((j-.25,min(r.open,r.close)),.5,max(.1,abs(r.close-r.open)),color=co,zorder=4))
        bx.bar(j,float(r.amount)/1e8,color=co,width=.7,alpha=.7)
    # Structure must exist BEFORE the case bar to count as candidate.
    avail=candidates[candidates.asof_idx.lt(t)]
    if len(avail):
        sample=avail[avail.asof_idx.ge(t-8)].sort_values(['asof_idx','geometry_score'],ascending=[False,False]).head(1)
        if len(sample):
            r=sample.iloc[0];xx=np.arange(max(start,int(r.start_idx)),end+1)
            ax.plot(xx,r.upper_m*xx+r.upper_b,'k-',lw=2.1,label='R113 structure upper (prior known)')
            ax.plot(xx,r.lower_m*xx+r.lower_b,'k-',lw=2.1,label='R113 structure lower (prior known)')
            ax.fill_between(xx,r.lower_m*xx+r.lower_b,r.upper_m*xx+r.upper_b,color='#c5dcfa',alpha=.17)
    # Keep prior R112 independent trendlines on the same price chart.
    ss=states[(states.date.eq(date)) & states.algorithm.eq('TS_BODY_WICK_PROXY')]
    for _,r in ss.iterrows():
        if pd.isna(r.active_level) or float(r.active_level)<=0:continue
        v=float(r.active_level);sp=float(r.active_slope_pct_per_bar) if pd.notna(r.active_slope_pct_per_bar) else 0
        slope=v*sp/100;xx=np.arange(max(start,t-20),t+1)
        y=v+slope*(xx-t)
        col='#e69923' if r.kind=='UPPER' else '#109a6e'
        ax.plot(xx,y,ls=':',lw=1.3,alpha=.8,color=col,label=f'R112 {r.scale} {r.kind}')
    ax.axvline(t,color='black',ls='--',lw=1.2)
    if end>t:ax.axvspan(t,end,color='#f59e0b',alpha=.06)
    ax.set_title(f'{name} {date} | separate R112 trendline + R113 box/channel/convergence')
    ax.set_ylabel('KRW');bx.set_ylabel('억원');ax.grid(alpha=.16);bx.grid(alpha=.12)
    tick=np.linspace(start,end,10,dtype=int);bx.set_xticks(tick);bx.set_xticklabels([dates[j][5:] for j in tick],rotation=25)
    ax.legend(loc='upper left',fontsize=7,ncol=3)
    p=output/'charts'/f'{code}_{date}_R1D113_DUAL_TRACK.png'
    fig.savefig(p,dpi=120);plt.close(fig)
    return p.name


def run(*,root112=R112,root109=R109,output=None,raw_provider=None,draw=True,codes=None):
    sources,files,hashes=inputs(root112,root109)
    if raw_provider is None:
        from research_r1d96 import load as raw_provider
        from research_r1d104 import _verify
        _verify()
    output=Path(output or HERE/'results');output.mkdir(parents=True,exist_ok=True)
    if draw:(output/'charts').mkdir(exist_ok=True)
    codes=sorted(codes) if codes else sorted(sources['states'].code.unique())
    all_candidates=[];all_states=[];all_events=[];all_links=[];all_outcomes=[];all_trend=[];all_manual=[];charts=[];summary=[]
    for code in codes:
        d=_raw(raw_provider(code));name=str(sources['states'][sources['states'].code.eq(code)].name.iloc[0])
        ss=sources['states'][sources['states'].code.eq(code)].copy()
        tt=sources['changes'][sources['changes'].code.eq(code)].copy()
        ctx=sources['context'][sources['context'].code.eq(code)].copy()
        if ss.empty:raise AssertionError('R112 trendline track missing '+code)
        # Exact compatibility check for full original history or date-sliced smoke.
        raw_dates=set(d.date.dt.strftime('%Y-%m-%d'))
        if not set(ss.date.astype(str)).issubset(raw_dates):raise AssertionError('R112 state date not present in raw OHLCV '+code)
        # Canonical lane is EXACTLY 4 algorithms × 3 scales × 2 sides per date.
        if ss.groupby('date').size().ne(24).any():raise AssertionError('R112 incomplete 24-lane dates '+code)
        original_state_hash=hashlib.sha256(ss.to_csv(index=False).encode()).hexdigest()
        found,life,ev=scan(d)
        for frame in (found,life,ev):
            if not frame.empty:frame.insert(0,'code',code);frame.insert(1,'name',name)
        if not found.empty:all_candidates.append(found)
        if not life.empty:all_states.append(life)
        if not ev.empty:
            all_events.append(ev)
            link=_confluence(ss,ev,ctx)
            if not link.empty:all_links.append(link)
            post=_expost(d,ev)
            if not post.empty:post.insert(0,'code',code);post.insert(1,'name',name);all_outcomes.append(post)
        # Independently replayed structure never mutates any R112 selection.
        active=ss[ss.state.eq('ACTIVE')]
        bydate=ss.groupby('date').agg(r112_lane_count=('scale','size'),r112_active_line_count=('state',lambda x:int((x=='ACTIVE').sum())),
             r112_reanchors=('event',lambda x:int((x=='REANCHOR_NEW_CONFIRMED').sum())),
             r112_break_archives=('event',lambda x:int((x=='TWO_CLOSE_BREAK_ARCHIVE').sum()))).reset_index()
        bydate.insert(0,'code',code);bydate.insert(1,'name',name)
        if not life.empty:
            added=life.groupby('date').agg(r113_shapes_visible=('pattern','nunique'),r113_boundary_hypotheses=('basis','size')).reset_index()
            bydate=bydate.merge(added,on='date',how='left',validate='one_to_one')
        all_trend.append(bydate)
        if hashlib.sha256(ss.to_csv(index=False).encode()).hexdigest()!=original_state_hash:
            raise AssertionError('R112 in-memory active states mutated')
        # Human illustrations are ONLY audit references after automatic selection.
        for cc,asof,fam,a,b in AUDIT_REFERENCES:
            if cc!=code:continue
            selection=found[found['asof'].eq(asof)] if not found.empty else pd.DataFrame()
            all_manual.append({'code':code,'name':name,'reference_date':asof,
                'manual_family':fam,'manual_anchor1':a,'manual_anchor2':b,
                'r113_shapes_on_reference_date':int(selection.pattern.nunique()) if len(selection) else 0,
                'r113_structure_shape_names':'|'.join(sorted(selection.pattern.unique())) if len(selection) else '',
                'r112_active_lanes_on_reference_date':int(ss[ss.date.eq(asof)].state.eq('ACTIVE').sum()),
                'note':'REFERENCE_NOT_USED_IN_CANDIDATE_SELECTION; line anchor match must be audited in R112 separately'})
        if draw:
            for cc,n,date in CASE_DATES:
                if cc==code:
                    path=_plot(d,found,ss,code,n,date,output)
                    if path:charts.append(path)
        summary.append({'code':code,'name':name,'raw_days':len(d),'r112_active_state_rows':len(ss),
           'r112_prior_transitions':len(tt),'r113_pattern_snapshots':len(found),
           'r113_structure_breaks':int(ev.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN']).sum()) if len(ev) else 0,
           'r113_structure_type_count':int(found.pattern.nunique()) if len(found) else 0})
        print('CASE_DONE',code,name,'raw_days',len(d),'r112_rows',len(ss),
              'r113_shapes',len(found),'r113_breaks',summary[-1]['r113_structure_breaks'],'PIT_PASS',flush=True)
    def save(frames,name):
        x=pd.concat(frames,ignore_index=True) if frames else pd.DataFrame()
        x.to_csv(output/name,index=False,encoding='utf-8-sig');return x
    pat=save(all_candidates,'R1D113_STRUCTURE_PATTERN_SNAPSHOTS_PIT.csv')
    save(all_states,'R1D113_STRUCTURE_DAILY_STATES_PIT.csv')
    evt=save(all_events,'R1D113_STRUCTURE_BREAK_EVENTS_PIT.csv')
    # Group repeated breaks within a ten-bar study window; do not interpret
    # repeated line crossings as independent pattern/trading signals.
    episodes=[]
    if not evt.empty:
        breaks=evt[evt.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN'])].copy()
        for (cc,pat_name,basis,direction),g in breaks.groupby(['code','pattern','basis','event']):
            previous=-10**9;seq=0;first_date=None;last_date=None;count=0
            for r in g.sort_values('idx').to_dict('records'):
                idx=int(r['idx'])
                if idx-previous>10:
                    if count:episodes.append({'code':cc,'pattern':pat_name,'basis':basis,'direction':direction,'sequence':seq,
                          'first_break_date':first_date,'last_break_date':last_date,'raw_break_records':count})
                    seq+=1;first_date=r['date'];count=0
                last_date=r['date'];previous=idx;count+=1
            if count:episodes.append({'code':cc,'pattern':pat_name,'basis':basis,'direction':direction,'sequence':seq,
                     'first_break_date':first_date,'last_break_date':last_date,'raw_break_records':count})
    episode_df=save([pd.DataFrame(episodes)] if episodes else [],'R1D113_PATTERN_EVENT_GROUPS_AUDIT.csv')
    links=save(all_links,'R1D113_STRUCTURE_TRENDLINE_CANDLE_INDICATOR_CONFLUENCE_PIT.csv')
    ex=save(all_outcomes,'R1D113_STRUCTURE_FOLLOWTHROUGH_EXPOST.csv')
    trend=save(all_trend,'R1D113_R112_TRENDLINE_CONTINUITY_DAILY_AUDIT_PIT.csv')
    save([pd.DataFrame(all_manual)] if all_manual else [],'R1D113_MANUAL_ILLUSTRATIONS_AUDIT_ONLY.csv')
    save([pd.DataFrame(summary)],'R1D113_STOCK_RESEARCH_COUNTS.csv')
    for k,p in files.items():
        if hashlib.sha256(p.read_bytes()).hexdigest()!=hashes[k]:raise AssertionError('upstream file mutated '+k)
    audit={'version':'R1D113_DUAL_TRACK_TRENDLINE_PLUS_STRUCTURE_RESEARCH_ONLY',
       'input_hashes_SHA256':hashes,'upstream_unchanged':True,
       'r112_trendline_states_preserved':int(sum(q['r112_active_state_rows'] for q in summary)),
       'structure_snapshots':len(pat),'structure_break_events':int(evt.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN']).sum()) if len(evt) else 0,
       'confluence_rows':len(links),'EXPOST_rows':len(ex),'pattern_event_groups_10bar':len(episode_df),
       'stocks':summary,'chart_files':charts,
       'manual_references_not_used_in_discovery':True,
       'limitations':['Geometry uses trailing confirmed 2/2 pivots and public research heuristics, not a vendor engine',
         'R112 selection is read-only and not altered by R113',
         'A box is not a verified order-book volume-at-price profile',
         'A pattern appearing only on breakout day is not a preexisting breakout signal',
         'R109 full technical context exists on sampled eligible stock-days, not every raw bar',
         'R112 active lines are as-of-close selection and not intraday signals',
         'Not an investment strategy, no out-of-sample profitability established']}
    (output/'R1D113_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ('r112_trendline_states_preserved','structure_snapshots','structure_break_events','confluence_rows','EXPOST_rows','upstream_unchanged')},ensure_ascii=False),flush=True)
    return audit

if __name__=='__main__':run()
