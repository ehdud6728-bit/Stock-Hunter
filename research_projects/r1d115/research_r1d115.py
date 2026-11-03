"""Stock-Hunter R1-D115: user supplied ATR pivot + cluster baseline vs R112/R114.

Research only. A) R112 trendline archive is read only, B) R114 strict structure
archive is read only, C) user supplied SciPy ATR-peak price zone is an INDEPENDENT
comparison method. Event snapshots at t use candidates frozen at close t-1.
Return horizons are output into a separate EXPOST CSV and never into selection.
"""
from __future__ import annotations
import hashlib,json,sys
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d104'),str(ROOT/'r1d114'),str(HERE)]
from atr_cluster_baseline import raw_check,original_like_scan,safe_scan_at,candidate_snapshot,group_repeated_events
from full_context import features

R112=ROOT/'r1d112'/'results'
R114=ROOT/'r1d114'/'results'
FILES={
    'r112':('r112','R1D112_MULTISCALE_ACTIVE_LINES_DAILY_PIT.csv'),
    'r114':('r114','R1D114_ALL_R113_EVENTS_STRICT_FORMATION_GATE_PIT.csv'),
}
CASES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-13'),
       ('006400','삼성SDI','2024-02-16')]
INDICATORS=['atr14','candle_body_pct','upper_wick_pct','lower_wick_pct',
            'close_vs_prev3_high_pct','amount_vs_prev20','volume_vs_prev20',
            'ma20_slope20_pct','ma20_gap_pct','ma112_gap_pct','ma224_gap_pct',
            'bb20_width_pct','env20_10_position','ichimoku_visible_thickness_pct']


def read_sources(r112=R112,r114=R114):
    roots={'r112':Path(r112),'r114':Path(r114)}
    paths={k:roots[rt]/fname for k,(rt,fname) in FILES.items()}
    for p in paths.values():
        if not p.is_file():raise FileNotFoundError('Run R1-D112 and R1-D114 first: '+str(p))
    hashes={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    src={k:pd.read_csv(p,dtype={'code':str},low_memory=False) for k,p in paths.items()}
    for d in src.values():d['code']=d.code.str.zfill(6)
    if src['r112'][['code','date','algorithm','scale','kind']].duplicated().any():
        raise AssertionError('Duplicate R112 lane')
    if not {'strict_eligible','event','date','code','idx'}.issubset(src['r114'].columns):
        raise AssertionError('Missing R114 original events / strict gate')
    return src,paths,hashes


def _number(v):
    try:
        x=float(v)
        return x if np.isfinite(x) else None
    except (TypeError,ValueError):return None


def prior_r112_context(code,prev_date,px,source):
    prev=source[(source.code.eq(code))&(source.date.eq(prev_date))&source.state.eq('ACTIVE')]
    out={'r112_prior_active_lanes':len(prev),'r112_source_date':prev_date}
    for kind in ('UPPER','LOWER'):
        a=prev[prev.kind.eq(kind)]
        vals=[]
        for r in a.to_dict('records'):
            v=_number(r.get('active_level'))
            if v is None or v<=0:continue
            slope=_number(r.get('active_slope_pct_per_bar')) or 0
            today_level=v*(1+slope/100.)
            if today_level>0:
                vals.append((abs(100*(px/today_level-1)),today_level,r))
        if vals:
            _,v,r=min(vals,key=lambda x:x[0])
            out[f'r112_nearest_{kind.lower()}_level_projected']=round(v,4)
            out[f'r112_nearest_{kind.lower()}_line_id']=r.get('active_line_id')
            out[f'r112_nearest_{kind.lower()}_scale']=r.get('scale')
            out[f'r112_nearest_{kind.lower()}_gap_pct']=round(100*(px/v-1),4)
        else:
            out[f'r112_nearest_{kind.lower()}_level_projected']=None
            out[f'r112_nearest_{kind.lower()}_line_id']=None
            out[f'r112_nearest_{kind.lower()}_scale']=None
            out[f'r112_nearest_{kind.lower()}_gap_pct']=None
    return out


def strict_r114_context(code,date,direction,src):
    q=src[src.code.eq(code)&src.date.eq(date)&src.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN'])]
    # bool column may parse from CSV as booleans or strings
    good=q[q.strict_eligible.astype(str).str.lower().eq('true')]
    target='BREAKOUT_UP' if direction=='UP' else 'BREAKDOWN_DOWN'
    subset=good[good.event.eq(target)]
    return {'r114_same_day_raw_pattern_breaks':len(q),
            'r114_same_day_strict_pattern_breaks':len(good),
            'r114_same_direction_strict_breaks':len(subset),
            'r114_same_direction_structures':'|'.join(sorted(set(subset.strict_pattern.astype(str))))}


def _fwd(d,events,horizons=(1,3,5,10,20,40)):
    # Absolute date-based lookup from an event saved earlier: never joined to the PIT event table.
    records=[]
    for r in events.to_dict('records'):
        t=int(r['idx']);px=float(r['close_event']);rec={k:r.get(k) for k in
                 ('code','name','idx','date','event','candidate_id','episode_id','direction')}
        for days in horizons:
            k=f'Dp{days}'
            rec[k+'_complete']=bool(t+days<len(d))
            rec[k+'_close_return_pct_EXPOST']=round(100*(float(d.close.iloc[t+days])/px-1),4) if t+days<len(d) else None
        records.append(rec)
    return pd.DataFrame(records)


def _plot(d,asof,code,name,events,s112,out,forward=0):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.is_file():font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    dt=d.date.dt.strftime('%Y-%m-%d');hits=np.flatnonzero(dt.eq(asof))
    if len(hits)!=1:return None
    t=int(hits[0]);start=max(0,t-79);end=min(len(d)-1,t+forward)
    fig,(ax,bx)=plt.subplots(2,1,figsize=(15,9),sharex=True,
         gridspec_kw={'height_ratios':[4,1]},layout='constrained')
    for j in range(start,end+1):
        r=d.iloc[j];up=float(r.close)>=float(r.open);col='#de4444' if up else '#2673c0'
        ax.vlines(j,r.low,r.high,color=col,lw=.7)
        ax.add_patch(Rectangle((j-.28,min(r.open,r.close)),.56,max(.05,abs(r.close-r.open)),color=col,alpha=.9))
        bx.bar(j,r.amount/1e8,width=.7,color=col,alpha=.55)
    # Actual price-level candidates are frozen on the PRIOR close.
    frozen,_=candidate_snapshot(d.iloc[:t]);shown=0
    for z in sorted((r for r in frozen if r['kind']=='RESISTANCE'),
                    key=lambda r:abs(r['level']/float(d.close.iloc[t])-1))[:2]:
        lv=float(z['level']);ax.hlines(lv,start,t,linestyle='--',lw=1.5,color='#c47b1b',
                 label=f'prior known ATR high cluster {lv:,.0f} (touch {z["touches"]})');shown+=1
    for z in sorted((r for r in frozen if r['kind']=='SUPPORT'),
                    key=lambda r:abs(r['level']/float(d.close.iloc[t])-1))[:2]:
        ax.hlines(float(z['level']),start,t,linestyle='--',lw=1.1,color='#218b7b',alpha=.85,label='prior known ATR low cluster')
    for z in frozen:
        if z['kind']=='DESC_RESISTANCE':
            # same fitted regression used for signal, not independently redrawn from different anchors
            a=max(start,min(z['anchor_indices']));x=np.arange(a,t+1)
            y=float(z['slope'])*(x-a)+(float(z['level'])-float(z['slope'])*(t-1-a))
            ax.plot(x,y,color='#111827',lw=1.4,label='prior known ATR pivot regression')
    prevdate=str(d.date.iloc[t-1].date())
    lines=s112[(s112.date.eq(prevdate))&(s112.algorithm.eq('TS_BODY_WICK_PROXY'))&s112.state.eq('ACTIVE')]
    for _,l in lines.iterrows():
        level=_number(l.active_level)
        if not level or level<=0:continue
        slope=_number(l.active_slope_pct_per_bar) or 0.
        x=np.arange(max(start,t-25),t+1)
        y=level*(1+slope*(x-(t-1))/100)
        ax.plot(x,y,linestyle=':',lw=1.,alpha=.75,color=('#a16207' if l.kind=='UPPER' else '#047857'),
                label=f'R112 {l.scale} {l.kind} (frozen t-1)')
    today_events=events[events.date.eq(asof)] if not events.empty else pd.DataFrame()
    ax.axvline(t,color='#6b7280',ls='--',lw=1.1)
    if forward>0:ax.axvspan(t+.4,end+.5,alpha=.08,color='#eab308')
    for _,r in today_events.iterrows():
        v=_number(r.get('level_event'))
        if v:ax.scatter([t],[v],s=80,edgecolors='#000',facecolors='none',zorder=8)
    ax.set_title(f'{name} {asof}: independent ATR-cluster vs preserved R112 | '+('ASOF ONLY' if not forward else f'AFTER +{forward} EXPOST'))
    ax.set_ylabel('KRW');bx.set_ylabel('Amount 100M KRW');ax.grid(alpha=.15)
    ax.legend(loc='upper left',ncol=2,fontsize=7)
    ticks=np.linspace(start,end,10,dtype=int);bx.set_xticks(ticks);bx.set_xticklabels([dt.iloc[j][5:] for j in ticks],rotation=25)
    outname=f'{code}_{asof}_R1D115_'+('ASOF_PIT' if forward==0 else 'EXPOST_20')+'.png'
    path=out/'charts'/outname;fig.savefig(path,dpi=110);plt.close(fig)
    return outname


def run(*,root112=R112,root114=R114,output=None,raw_provider=None,codes=None,draw=True,cases=CASES):
    src,paths,hashes=read_sources(root112,root114)
    if raw_provider is None:
        from research_r1d96 import load as raw_provider
        from research_r1d104 import _verify
        _verify() # verifies frozen data hashes; no LIVE/SEARCH writes
    out=Path(output or HERE/'results');out.mkdir(parents=True,exist_ok=True)
    if draw:(out/'charts').mkdir(exist_ok=True)
    codes=sorted(codes) if codes else sorted(src['r112'].code.unique())
    all_safe=[];all_original=[];all_refs=[];all_fwd=[];summary=[];charts=[]
    for code in codes:
        d=raw_check(raw_provider(code));code=str(code).zfill(6)
        lanes=src['r112'][src['r112'].code.eq(code)].copy()
        originals=src['r114'][src['r114'].code.eq(code)].copy()
        if lanes.empty:raise AssertionError('Missing R112 frozen lanes for '+code)
        if lanes.groupby('date').size().ne(24).any():raise AssertionError('R112 incomplete 24 lanes '+code)
        dates=set(d.date.dt.strftime('%Y-%m-%d'))
        if not set(lanes.date).issubset(dates):raise AssertionError('R112 dates not in raw '+code)
        if not set(originals.date).issubset(dates):raise AssertionError('R114 dates not in raw '+code)
        nm=str(lanes.name.iloc[0]);raw_safe=[];raw_original=[]
        for t in range(81,len(d)):
            date=str(d.date.iloc[t].date())
            orig=original_like_scan(d.iloc[:t+1])
            for label,volume_ok in orig:
                raw_original.append({'code':code,'name':nm,'idx':t,'date':date,'name_original':label,
                     'volume_surge_original':volume_ok,'note':'REFERENCE_CODE_ASOF_EVENT_CLOSE_NOT_FROZEN_T_MINUS_1'})
            safe,_=safe_scan_at(d,t)
            for r in safe:r.update(code=code,name=nm)
            raw_safe.extend(safe)
        x=pd.DataFrame(raw_safe);xx=pd.DataFrame(raw_original)
        if not x.empty:
            x,episodes=group_repeated_events(x)
            ctx=features(d)
            for k in INDICATORS:
                if k not in ctx.columns:raise AssertionError('Missing R114 technical '+k)
            enriched=[]
            for r in x.to_dict('records'):
                t=int(r['idx']);date=r['date'];prevdate=str(d.date.iloc[t-1].date())
                rec=dict(r);rec.update({k:ctx.iloc[t][k] for k in INDICATORS})
                rec.update(prior_r112_context(code,prevdate,float(r['close_event']),lanes))
                rec.update(strict_r114_context(code,date,r['direction'],originals))
                enriched.append(rec)
            x=pd.DataFrame(enriched)
            all_safe.append(x)
            f=_fwd(d,x)
            if not f.empty:all_fwd.append(f)
        if not xx.empty:all_original.append(xx)
        # Reference dates are only AFTER algorithmic replay and never enter selection.
        for cc,label,date in cases:
            if code!=cc:continue
            dt=d.date.dt.strftime('%Y-%m-%d');pos=np.flatnonzero(dt.eq(date))
            if len(pos)!=1:continue
            t=int(pos[0]);frozen,_=candidate_snapshot(d.iloc[:t])
            current=x[x.date.eq(date)] if not x.empty else pd.DataFrame()
            all_refs.append({'code':code,'name':nm,'date':date,'original_signal_count':int(len(xx[xx.date.eq(date)])) if not xx.empty else 0,
                'causal_signal_count':len(current),
                'causal_signal_names':'|'.join(current.event.tolist()) if len(current) else '',
                'prior_resistance_clusters':int(sum(z['kind']=='RESISTANCE' for z in frozen)),
                'prior_support_clusters':int(sum(z['kind']=='SUPPORT' for z in frozen)),
                'prior_falling_regression_candidates':int(sum(z['kind']=='DESC_RESISTANCE' for z in frozen)),
                'reference_used_only_after_replay':True})
            if draw:
                for fw in [0,20]:
                    p=_plot(d,date,code,label,x,lanes,out,fw)
                    if p:charts.append(p)
        summary.append({'code':code,'name':nm,'raw_days':len(d),
            'original_like_signal_records':len(xx),'causal_signal_records':len(x),
            'causal_unique_dates':int(x.date.nunique()) if len(x) else 0,
            'R112_lane_rows_readonly':len(lanes),'R114_events_readonly':len(originals)})
        print('CASE_DONE',code,nm,'raw',len(d),'original',len(xx),'causal',len(x),flush=True)
    def save(seq,fname):
        x=pd.concat(seq,ignore_index=True) if seq else pd.DataFrame()
        x.to_csv(out/fname,index=False,encoding='utf-8-sig');return x
    safe=save(all_safe,'R1D115_CAUSAL_ATR_PIVOT_CLUSTER_EVENTS_PIT.csv')
    orig=save(all_original,'R1D115_USER_ORIGINAL_LIKE_DIAGNOSTIC_ASOF.csv')
    refs=save([pd.DataFrame(all_refs)] if all_refs else [],'R1D115_FIXED_CASE_AUDIT_NOT_TRAINING.csv')
    fwd=save(all_fwd,'R1D115_CAUSAL_EVENT_FOLLOWTHROUGH_EXPOST.csv')
    dedup=save([group_repeated_events(safe)[1]] if not safe.empty else [],'R1D115_DESCRIPTIVE_DEDUP_EPISODES.csv')
    save([pd.DataFrame(summary)],'R1D115_BY_STOCK_COUNTS.csv')
    # No stale output/indicator/PIT source writes were performed.
    for k,p in paths.items():
        if hashlib.sha256(p.read_bytes()).hexdigest()!=hashes[k]:
            raise AssertionError('R112/R114 source changed: '+str(p))
    audit={'version':'R1D115_USER_ATR_PIVOT_CLUSTER_INDEPENDENT_VISUAL_RESEARCH_ONLY',
      'raw_sources':'research_r1d96.load: frozen KRX OHLCV',
      'r112_r114_input_hash_sha256':hashes,'upstream_unchanged':True,
      'research_only':True,'user_reference_not_in_detection':True,
      'signal_decision_candidate_asof':'PREVIOUS_TRADING_CLOSE',
      'stocks':summary,'causal_signal_records':len(safe),
      'original_like_signal_records':len(orig),'unique_episodes_descriptive':len(dedup),
      'manual_case_audit_rows':len(refs),'EXPOST_rows':len(fwd),
      'charts':charts,'limitations':[
       'Original function is independently reproduced for reference; does not predict returns',
       'scipy prominence uses the entire past 80-bar prefix and is re-evaluated at each close',
       'A resistance/support level cluster is NOT a verified bid-ask or volume-at-price orderbook profile',
       'R112 active line and R114 strict structure studies remain immutable and independently managed',
       'Dedup groups and descriptions are diagnostic and not backtested buy scores',
       'PIT scans require prior close available; same-day candles known after close only',
       'Case dates selected for display only; no reference-specific discovery rules',
       'No OOS predictive efficacy established']}
    (out/'R1D115_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2,default=str),encoding='utf8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ['causal_signal_records','original_like_signal_records','unique_episodes_descriptive','EXPOST_rows','upstream_unchanged']},ensure_ascii=False),flush=True)
    return audit

if __name__=='__main__':run()
