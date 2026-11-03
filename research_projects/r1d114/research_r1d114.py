"""R1-D114 frozen R112+R113 dual-track study: visual shape, lifecycle & full-day PIT.

Consumes original immutable R113 event/candidate outputs without changing them.
Uses ORIGINAL frozen R97 OHLCV to compute 100% event-date technical context,
with ordinary indicator warmups explicitly distinguished from missing data.
"""
from __future__ import annotations
import sys,hashlib,json
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d104'),str(HERE)]
from structure_audit import classify_all,replay_lifecycle,event_strict_gate
from full_context import features,join_event_context

R113=ROOT/'r1d113'/'results'
R112=ROOT/'r1d112'/'results'
INPUTS={'patterns':('r113','R1D113_STRUCTURE_PATTERN_SNAPSHOTS_PIT.csv'),
        'events':('r113','R1D113_STRUCTURE_BREAK_EVENTS_PIT.csv'),
        'r112':('r112','R1D112_MULTISCALE_ACTIVE_LINES_DAILY_PIT.csv'),
        'r113_continuity':('r113','R1D113_R112_TRENDLINE_CONTINUITY_DAILY_AUDIT_PIT.csv')}
CASES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-13'),
       ('006400','삼성SDI','2024-02-16')]


def read_source(r113=R113,r112=R112):
    roots={'r113':Path(r113),'r112':Path(r112)}
    paths={k:roots[where]/filename for k,(where,filename) in INPUTS.items()}
    for p in paths.values():
        if not p.is_file():raise FileNotFoundError('R1-D112 and R1-D113 must finish first: '+str(p))
    hashes={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    d={k:pd.read_csv(p,dtype={'code':str},low_memory=False) for k,p in paths.items()}
    for x in d.values():x['code']=x.code.str.zfill(6)
    return d,paths,hashes


def combine_r112_prior(events,states):
    """At each R113 event use R112 states known at previous trading close."""
    if events.empty:return events.copy()
    ss=states.copy()
    if ss[['code','idx','algorithm','scale','kind']].duplicated().any():
        raise AssertionError('R112 duplicate lane')
    sizes=ss.groupby(['code','idx']).size()
    if not sizes.eq(24).all():raise AssertionError('R112 lane not exactly 24')
    groups=ss.groupby(['code','idx']).agg(
      r112_active_lanes=('state',lambda s:int((s=='ACTIVE').sum())),
      r112_reanchors=('event',lambda s:int((s=='REANCHOR_NEW_CONFIRMED').sum())),
      r112_archived_breaks=('event',lambda s:int((s=='TWO_CLOSE_BREAK_ARCHIVE').sum())))
    prev=groups.reset_index();prev['idx']+=1
    prev=prev.rename(columns={'r112_active_lanes':'prior_close_r112_active_lanes',
       'r112_reanchors':'prior_close_r112_reanchors',
       'r112_archived_breaks':'prior_close_r112_archived_breaks'})
    out=events.merge(prev,on=['code','idx'],how='left',validate='many_to_one')
    out['prior_close_r112_available']=out.prior_close_r112_active_lanes.notna()
    return out


def _picture(d,candidates,states,code,name,date,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    f=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if f.exists():font_manager.fontManager.addfont(str(f));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    dt=pd.to_datetime(d.date).dt.strftime('%Y-%m-%d');hits=np.flatnonzero(dt.eq(date))
    if len(hits)!=1:return None
    t=int(hits[0]);left=max(0,t-55);right=min(len(d)-1,t+12)
    fig,(ax,bx)=plt.subplots(2,1,figsize=(15,9),sharex=True,gridspec_kw={'height_ratios':[4.5,1]},layout='constrained')
    for j in range(left,right+1):
        o=d.iloc[j];up=float(o.close)>=float(o.open);col='#df4949' if up else '#2d77b7'
        ax.vlines(j,o.low,o.high,color=col,lw=.8)
        ax.add_patch(Rectangle((j-.27,min(o.open,o.close)),.54,max(.1,abs(o.close-o.open)),color=col,alpha=.9))
        bx.bar(j,o.amount/1e8,width=.7,color=col,alpha=.65)
    # Display only PREVIOUSLY KNOWN geometry as of day t-1.
    valid=candidates[(candidates.code.eq(code))&(candidates.asof_idx.lt(t))&
                     (candidates.asof_idx.ge(t-10))&(candidates.strict_eligible)]
    if not valid.empty:
        r=valid.sort_values(['asof_idx','geometry_score'],ascending=[False,False]).iloc[0]
        xs=np.arange(max(left,int(r.start_idx)),right+1)
        ax.plot(xs,r.upper_m*xs+r.upper_b,'k-',lw=2.1,label=f"STRICT {r.strict_pattern} upper")
        ax.plot(xs,r.lower_m*xs+r.lower_b,'k-',lw=2.1,label='STRICT lower')
    ss=states[(states.code.eq(code))&(states.date.eq(date))&states.algorithm.eq('TS_BODY_WICK_PROXY')]
    for _,r in ss.iterrows():
        if r.state!='ACTIVE' or pd.isna(r.active_level) or float(r.active_level)<=0:continue
        lvl=float(r.active_level);s=float(r.active_slope_pct_per_bar) if pd.notna(r.active_slope_pct_per_bar) else 0
        xs=np.arange(max(left,t-24),t+1)
        ax.plot(xs,lvl+lvl*s/100*(xs-t),ls=':',lw=1.3,
          color='#d68f16' if r.kind=='UPPER' else '#168f6b',alpha=.78,label=f'R112 {r.scale} {r.kind}')
    ax.axvline(t,color='black',ls='--',lw=1)
    ax.axvspan(t,right,color='#f59e0b',alpha=.06)
    xt=np.linspace(left,right,10,dtype=int);bx.set_xticks(xt);bx.set_xticklabels([dt.iloc[j][5:] for j in xt],rotation=20)
    ax.grid(alpha=.15);bx.grid(alpha=.15);ax.legend(loc='upper left',fontsize=7,ncol=3)
    ax.set_title(f'{name} {date} | strict full-window structure (prior known) + preserved R112 trendlines')
    ax.set_ylabel('KRW');bx.set_ylabel('거래대금 억원')
    p=out/'charts'/f'{code}_{date}_R1D114_STRICT_DUAL_TRACK.png'
    fig.savefig(p,dpi=120);plt.close(fig)
    return p.name


def run(*,root113=R113,root112=R112,output=None,raw_provider=None,
        include_full_ohlcv=True,draw=True,cases=CASES):
    src,paths,hashes=read_source(root113,root112)
    out=Path(output or HERE/'results');out.mkdir(parents=True,exist_ok=True)
    if draw:(out/'charts').mkdir(exist_ok=True)
    raw=src['patterns']; events=src['events'];states=src['r112']
    if states.groupby(['code','idx']).size().ne(24).any():raise AssertionError('R112 missing lanes')
    strict=classify_all(raw)
    gate=event_strict_gate(events,strict)
    life=replay_lifecycle(strict,gate)
    gate=combine_r112_prior(gate,states)
    # No R113 record may be silently discarded by stricter R114 geometry.
    if len(strict)!=len(raw) or len(gate)!=len(events):
        raise AssertionError('append-only overlay lost original candidates/events')
    # R113's full-history R112 continuity is an immutable baseline.
    continuity=src['r113_continuity']
    if not continuity.groupby('code').size().eq(states.groupby('code').size()/24).all():
        raise AssertionError('R113/R112 continuity row-count mismatch')
    if not continuity.r112_lane_count.eq(24).all():raise AssertionError('R112 continuity damaged')
    allcontext=pd.DataFrame();charts=[]
    if include_full_ohlcv:
        if raw_provider is None:
            from research_r1d96 import load as raw_provider
            from research_r1d104 import _verify
            _verify()
        prices={}
        for code in sorted(states.code.unique()):
            d=raw_provider(code)
            if len(d)*24!=len(states[states.code.eq(code)]):
                raise AssertionError('R112 full raw date coverage mismatch '+code)
            prices[code]=d
        allcontext=join_event_context(gate,prices)
        if not allcontext['all_event_ohlcv_joined'].all():raise AssertionError('event date missing')
        if draw:
            for code,name,date in cases:
                if code in prices:
                    s=_picture(prices[code],strict,states,code,name,date,out)
                    if s:charts.append(s)
    def save(d,file):
        d.to_csv(out/file,index=False,encoding='utf-8-sig');return d
    save(strict,'R1D114_ALL_R113_PATTERN_STRICT_GEOMETRY_PIT.csv')
    save(life,'R1D114_STRICT_STRUCTURE_REGIME_LIFECYCLE_PIT.csv')
    save(gate,'R1D114_ALL_R113_EVENTS_STRICT_FORMATION_GATE_PIT.csv')
    if include_full_ohlcv:save(allcontext,'R1D114_ALL_EVENT_DATES_FULL_TECH_PIT.csv')
    original_by=raw.groupby('pattern').size().rename('r113_original_snapshot_count')
    eligible_by=strict[strict.strict_eligible].groupby('strict_pattern').size().rename('r114_strict_snapshot_count')
    counts=pd.concat([original_by,eligible_by],axis=1).fillna(0).astype(int).reset_index().rename(columns={'index':'pattern'})
    save(counts,'R1D114_OLD_VS_STRICT_SHAPE_COUNTS.csv')
    invalid=gate[(gate.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN'])) & (~gate.strict_eligible)]
    successes=gate[(gate.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN'])) & gate.strict_eligible]
    if len(invalid)+len(successes)!=int(events.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN']).sum()):
        raise AssertionError('strict gate did not partition breakout events')
    for k,p in paths.items():
        if hashlib.sha256(p.read_bytes()).hexdigest()!=hashes[k]:raise AssertionError('SOURCE MUTATION '+k)
    report={'version':'R1D114_STRICT_GEOMETRY_LIFECYCLE_ALL_EVENT_TECH_PIT_RESEARCH_ONLY',
      'upstream_sha256_unchanged':True,'upstream_hashes':hashes,
      'r112_trendline_state_rows_preserved':len(states),
      'r113_original_pattern_snapshots_preserved':len(raw),
      'r114_strict_geometry_snapshots':int(strict.strict_eligible.sum()),
      'r113_original_events_preserved':len(events),
      'r113_breakout_breakdown_records':int(events.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN']).sum()),
      'r114_strict_gate_breakout_breakdown_records':len(successes),
      'r114_rejected_or_unmatched_breakout_breakdown_records':len(invalid),
      'strict_regime_lifecycle_rows':len(life),
      'all_raw_event_dates_joined':len(allcontext) if include_full_ohlcv else None,
      'all_raw_event_ohlcv_coverage':bool(len(allcontext)==len(events)) if include_full_ohlcv else None,
      'raw_indicator_warmup_explicit':True,'chart_files':charts,
      'reference_dates_never_used_in_selection':True,'no_future_return_used_in_selection':True,
      'limitations':['R112 selection and R113 hypotheses are immutable baseline; R114 is an append-only reclassification.',
         'Strict thresholds are predeclared research rules, not optimized or a vendor engine.',
         'R113 source event labels and recurring records do NOT establish independent trades.',
         'R114 may reject R113 event retrospectively by its PREVIOUS known formation, never by future candles.',
         'All event-day rolling features are OHLCV computed as of event close; some long MA/cloud entries lack historical warmup.',
         'Visible Ichimoku spans use left-shifted projected lines (26 bars), and are not identical to every prior study variant.',
         'No price/volume-by-price volume-profile claims or OOS profitability established.']}
    (out/'R1D114_AUDIT.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print('SUMMARY',json.dumps({k:report[k] for k in ('r112_trendline_state_rows_preserved',
       'r113_original_pattern_snapshots_preserved','r114_strict_geometry_snapshots',
       'r113_original_events_preserved','r113_breakout_breakdown_records',
       'r114_strict_gate_breakout_breakdown_records','all_raw_event_dates_joined','upstream_sha256_unchanged')}),flush=True)
    return report

if __name__=='__main__':run()
