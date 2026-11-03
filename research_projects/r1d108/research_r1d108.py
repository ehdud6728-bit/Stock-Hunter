"""R1-D108: causal active trendline transitions × candlestick anatomy × full indicator co-movement.

RESEARCH_ONLY. Takes frozen R1-D97 price files and immutable R106 catalogs,
merges R105's full (R104-origin) PIT technical snapshots, and writes future
outcomes in different files. No trading recommendations or score fitting.
"""
from __future__ import annotations
import hashlib,json,sys
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/f'r1d{x}') for x in (97,100,101,102,103,104,105,106,107)]+[str(HERE)]
from candle_context import candle_features,CANDLE_COLUMNS
from lifecycle import track_lines,LANES

R105=ROOT/'r1d105'/'results'/'R1D105_ALL_ANCHOR_PRE_SNAPSHOTS.csv'
R106=ROOT/'r1d106'/'results'
LINE_FILE=R106/'R1D106_ALL_IMMUTABLE_LINES.csv'
CROSS_FILE=R106/'R1D106_LINE_CROSS_EVENTS_PIT.csv'
TRI_FILE=R106/'R1D106_ALL_TRIANGLE_CANDIDATES_PIT.csv'
FROZEN_FILES=(LINE_FILE,CROSS_FILE,TRI_FILE,R105)
CASES=[('039840','디오','2024-09-09','USER_DIO_SECOND_BULL_BODY'),
       ('039840','디오','2024-09-10','USER_DIO_FIRST_JOINT_CONVERGENCE'),
       ('039840','디오','2024-09-13','USER_DIO_LATER_PIVOT_BREAK'),
       ('071840','롯데하이마트','2024-02-20','USER_LOTTE_FIRST_BULL'),
       ('071840','롯데하이마트','2024-03-13','USER_LOTTE_SECOND_BULL'),
       ('071840','롯데하이마트','2024-03-21','LOTTE_APEX_NEAR'),
       ('006400','삼성SDI','2024-02-07','SDI_LONG_UPPER_WICK'),
       ('006400','삼성SDI','2024-02-08','SDI_FOLLOW_UP'),
       ('006400','삼성SDI','2024-02-16','SDI_LATER_BREAK')]


def _load_inputs(base_r105=R105,base_r106=R106):
    fp=Path(base_r105);base=Path(base_r106)
    required=(fp,base/'R1D106_ALL_IMMUTABLE_LINES.csv',base/'R1D106_LINE_CROSS_EVENTS_PIT.csv',base/'R1D106_ALL_TRIANGLE_CANDIDATES_PIT.csv')
    for p in required:
        if not p.is_file():raise FileNotFoundError('Run R1-D105 and R1-D106 first: '+str(p))
    hashes={str(p.name):hashlib.sha256(p.read_bytes()).hexdigest() for p in required}
    snapshots=pd.read_csv(fp,dtype={'code':str})
    snapshots.code=snapshots.code.str.zfill(6)
    # The original snapshots repeat as event D-20...D0. Identical PIT date rows only.
    meta=('code','idx','observation_date')
    chk=snapshots.groupby(list(meta),dropna=False)['close'].nunique()
    if (chk>1).any():raise AssertionError('Inconsistent same-date PIT snapshot')
    snapshots=snapshots.sort_values(['code','idx','anchor_date']).drop_duplicates(list(meta))
    if snapshots[['code','idx']].duplicated().any():raise AssertionError('Duplicate snapshot date')
    # Future outcome labels must never enter the signal-context table.
    strip=('anchor_date','anchor_type','relative_bar','name','observation_date')
    snapshots=snapshots.drop(columns=[c for c in snapshots if c in strip or ('EXPOST' in c.upper())],errors='ignore')
    lines=pd.read_csv(base/'R1D106_ALL_IMMUTABLE_LINES.csv',dtype={'code':str})
    crosses=pd.read_csv(base/'R1D106_LINE_CROSS_EVENTS_PIT.csv',dtype={'code':str})
    triangles=pd.read_csv(base/'R1D106_ALL_TRIANGLE_CANDIDATES_PIT.csv',dtype={'code':str})
    for z in (lines,crosses,triangles):z.code=z.code.str.zfill(6)
    return snapshots,lines,crosses,triangles,hashes


def _cross_count(e):
    if e.empty:return pd.DataFrame(columns=['idx','cross_upper_up','cross_upper_down','cross_lower_up','cross_lower_down','cross_strong_up_count','cross_strong_down_count'])
    f=e.copy()
    f['cross_upper_up']=((f.kind=='UPPER')&(f.event=='CROSS_UP')).astype(int)
    f['cross_upper_down']=((f.kind=='UPPER')&(f.event=='CROSS_DOWN')).astype(int)
    f['cross_lower_up']=((f.kind=='LOWER')&(f.event=='CROSS_UP')).astype(int)
    f['cross_lower_down']=((f.kind=='LOWER')&(f.event=='CROSS_DOWN')).astype(int)
    strong=f.strong_close_0p4pct.astype(str).str.lower().eq('true')
    f['cross_strong_up_count']=(strong&(f.event=='CROSS_UP')).astype(int)
    f['cross_strong_down_count']=(strong&(f.event=='CROSS_DOWN')).astype(int)
    cols=['cross_upper_up','cross_upper_down','cross_lower_up','cross_lower_down','cross_strong_up_count','cross_strong_down_count']
    return f.groupby('idx',as_index=False)[cols].sum()


def _state_wide(states):
    base=['state','active_line_id','active_family','known_date','anchor1','anchor2',
          'active_line_level','active_line_slope_pct','close_gap_to_active_pct',
          'retired_line_id','prior_break_idx']
    frames=[]
    for lane in LANES:
        r=states[states.lane.eq(lane)][['idx',*base]].copy()
        r=r.rename(columns={k:lane.lower()+'_'+k for k in base})
        frames.append(r.set_index('idx'))
    return pd.concat(frames,axis=1).reset_index()


def _append_expost(d:pd.DataFrame,features:pd.DataFrame,start_idx:int,end_idx:int):
    res=[]
    for t in range(start_idx,end_idx+1):
        if t>=len(d):continue
        candle=features.iloc[t];px=float(candle.close)
        r={'idx':t,'date':str(candle.date.date()),'signal_close':px,
           'signal_high':float(candle.high),'signal_low':float(candle.low),
           'signal_body_top':max(float(candle.open),px)}
        for h in (1,3,5,10,20):
            j=t+h
            valid=j<len(d)
            r[f'dp{h}_complete_EXPOST']=valid
            r[f'dp{h}_ret_pct_EXPOST']=round(100*(float(d.iloc[j].close)/px-1),4) if valid else np.nan
            r[f'dp{h}_close_above_signal_high_EXPOST']=bool(float(d.iloc[j].close)>r['signal_high']) if valid else None
            r[f'dp{h}_close_above_signal_bodytop_EXPOST']=bool(float(d.iloc[j].close)>=r['signal_body_top']) if valid else None
            r[f'dp{h}_close_below_signal_low_EXPOST']=bool(float(d.iloc[j].close)<r['signal_low']) if valid else None
        res.append(r)
    return pd.DataFrame(res)


def _charts(raw,merged,lines,code,name,date,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    fp=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if fp.exists():font_manager.fontManager.addfont(str(fp));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    d=raw.copy();d.date=pd.to_datetime(d.date);d=d.reset_index(drop=True)
    matches=np.flatnonzero(d.date.dt.strftime('%Y-%m-%d').eq(date))
    if len(matches)!=1:return None
    t=int(matches[0]);beg=max(0,t-28);end=min(len(d)-1,t+12)
    fig,(ax,bx)=plt.subplots(2,1,figsize=(15,8.8),sharex=True,gridspec_kw={'height_ratios':[4.8,1]},layout='constrained')
    for i in range(beg,end+1):
        r=d.iloc[i];co='#d94841' if r.close>=r.open else '#2b69b1'
        ax.vlines(i,r.low,r.high,color=co,lw=.9)
        ax.add_patch(Rectangle((i-.31,min(r.open,r.close)),.62,max(.3,abs(r.close-r.open)),color=co))
        bx.bar(i,float(r.amount)/1e8,color=co,alpha=.66,width=.75)
    context=merged.set_index('idx');indices=np.arange(beg,end+1)
    if 'ma20_value' in context:
        ma=[float(context.at[k,'ma20_value']) if k in context.index else np.nan for k in indices]
        ax.plot(indices,ma,label='MA20',lw=1.1,color='#2563eb')
    if 'bb20_upper' in context:
        u=[float(context.at[k,'bb20_upper']) if k in context.index else np.nan for k in indices]
        ax.plot(indices,u,label='BB20 upper',lw=.9,color='#d97706',ls='--')
    if 'env20_10_upper' in context:
        u=[float(context.at[k,'env20_10_upper']) if k in context.index else np.nan for k in indices]
        ax.plot(indices,u,label='ENV20 upper',lw=.9,color='#be185d',ls='--')
    p3=float(d.high.iloc[max(0,t-3):t].max())
    ax.hlines(p3,t-3,t+3,color='#7c3aed',ls=':',lw=1.3,label='Previous 3 bars high')
    # Historical reference lines are shown only if confirmed by chart date.
    references={
      '039840':[('BULL_BODY_TOP','2024-09-03','2024-09-09'),('WICK_LOW_PIVOT2','2024-08-26','2024-09-06')],
      '071840':[('BULL_BODY_TOP','2024-02-20','2024-03-13'),('WICK_LOW_PIVOT2','2024-02-01','2024-03-11')]}
    selected=lines[lines.known_idx.le(t)]
    for fam,a,b in references.get(code,[]):
        r=selected[selected.family.eq(fam)&selected.a1.eq(a)&selected.a2.eq(b)]
        if r.empty:continue
        l=r.iloc[0];left=max(beg,int(l.known_idx));xs=np.arange(left,end+1)
        val=float(l.v2)+float(l.slope_krw_per_bar)*(xs-int(l.i2))
        ax.plot(xs,val,lw=2,ls='-',color='#ea580c' if l.kind=='UPPER' else '#059669',
                label=f'{fam} {a[5:]}→{b[5:]} (known {l.known_date[5:]})')
    ax.axvline(t,color='black',lw=1.3,ls='--');ax.axvspan(t,end,alpha=.06,color='#d97706')
    ax.grid(alpha=.13);bx.grid(alpha=.13);ax.legend(fontsize=8,ncol=3,loc='upper left')
    x=np.linspace(beg,end,min(9,end-beg+1),dtype=int)
    bx.set_xticks(x);bx.set_xticklabels(d.date.iloc[x].dt.strftime('%m-%d'),rotation=15)
    # The candle quality diagnostics printed at t belong solely to that day's OHLCV.
    case=candle_features(d.iloc[:t+1]).iloc[t]
    ax.set_title(f'{name} {date} | 몸통 {case.candle_body_share_pct:.1f}% · 윗꼬리 {case.candle_upper_wick_share_pct:.1f}% · 아랫꼬리 {case.candle_lower_wick_share_pct:.1f}%')
    ax.set_ylabel('Price (KRW)');bx.set_ylabel('Turnover (100m KRW)')
    p=out/'charts'/f'{code}_{date}_R1D108_CANDLE_TREND_CONTEXT.png'
    fig.savefig(p,dpi=130,bbox_inches='tight');plt.close(fig)
    return p.name


def run(*,out=None,raw_provider=None,r105_path=R105,r106_dir=R106,plot=True,case_refs=None):
    out=Path(out or HERE/'results');out.mkdir(parents=True,exist_ok=True);(out/'charts').mkdir(exist_ok=True)
    verified_frozen_ohlcv = raw_provider is None
    if raw_provider is None:
        from research_r1d96 import load as raw_provider
        from research_r1d104 import _verify
        _verify()
    snapshots,lines,crosses,triangles,hashes=_load_inputs(r105_path,r106_dir)
    cases=CASES if case_refs is None else case_refs
    all_days=[];lives=[];later=[];examples=[];charts=[];count=[]
    for code,ln in lines.groupby('code',sort=True):
        nm=str(ln.iloc[0]['name']);raw=raw_provider(code).copy()
        raw.date=pd.to_datetime(raw.date);raw=raw.sort_values('date').reset_index(drop=True)
        candles=candle_features(raw)
        context=snapshots[snapshots.code.eq(code)].copy()
        if context.empty:raise AssertionError('Missing R105 PIT confluence for '+code)
        start=int(context.idx.min());stop=int(context.idx.max())
        # Use R104 comparable eligible dates starting at 541, not earlier lookback snapshots.
        eligible=context[context.idx.ge(541)].copy()
        start=541;stop=int(eligible.idx.max())
        state=track_lines(raw,ln,start_idx=start,end_idx=stop)
        wide=_state_wide(state)
        d=candles.iloc[start:stop+1].copy()
        d['idx']=range(start,stop+1)
        d['code']=code;d['name']=nm;d['date']=d.date.dt.strftime('%Y-%m-%d')
        # Only current-date OHLCV, geometry, and PIT context; NOT future labels.
        keep=['code','name','idx','date','open','high','low','close','volume','amount',
              'candle_range','candle_body','candle_upper_wick','candle_lower_wick','candle_body_top','candle_body_bottom',*CANDLE_COLUMNS]
        x=d[keep].merge(eligible.drop(columns=['code'],errors='ignore'),on='idx',validate='one_to_one',suffixes=('','_r105'))
        if 'close_r105' in x:
            if not np.allclose(x.close.to_numpy(float),x.close_r105.to_numpy(float)):
                raise AssertionError('Historical R105 close disagrees with frozen OHLCV')
            x=x.drop(columns=['close_r105'])
        x=x.merge(wide,on='idx',how='left',validate='one_to_one')
        ev=_cross_count(crosses[crosses.code.eq(code)])
        x=x.merge(ev,on='idx',how='left',validate='one_to_one')
        for c in ('cross_upper_up','cross_upper_down','cross_lower_up','cross_lower_down','cross_strong_up_count','cross_strong_down_count'):
            x[c]=x[c].fillna(0).astype(int)
        x['any_known_upper_break']=x.cross_upper_up.gt(0)
        x['any_strong_up_cross']=x.cross_strong_up_count.gt(0)
        x['long_upper_wick_vs_body']=x.candle_upper_wick_body_ratio.ge(1)
        x['long_lower_wick_vs_body']=x.candle_lower_wick_body_ratio.ge(1)
        x['upper_break_with_3bar_high_exceed']=x.any_known_upper_break&x.close_vs_prev3_high_max_pct.gt(.3)
        x['upper_break_without_3bar_high_exceed']=x.any_known_upper_break&~x.close_vs_prev3_high_max_pct.gt(.3)
        x['line_state_changed']=x[[lane.lower()+'_state' for lane in LANES]].apply(
            lambda r:any(v not in ('UNCHANGED','WAIT_NEW_CONFIRMED_ANCHOR','NO_ACTIVE_LINE') for v in r),axis=1)
        x['event_day']=x.line_state_changed|x.cross_upper_up.gt(0)|x.cross_upper_down.gt(0)|x.cross_lower_up.gt(0)|x.cross_lower_down.gt(0)
        all_days.append(x)
        state.insert(0,'code',code);state.insert(1,'name',nm);lives.append(state)
        f=_append_expost(raw,candles,start,stop)
        f.insert(0,'code',code);f.insert(1,'name',nm);later.append(f)
        for c,n,day,kind in cases:
            if c!=code:continue
            a=x[x.date.eq(day)]
            if len(a)!=1:raise AssertionError('Missing fixed historical case '+str((c,day)))
            row=a.iloc[0].to_dict();row['case_kind']=kind;examples.append(row)
            if plot and day in ('2024-09-13','2024-03-13','2024-02-07'):
                charts.append(_charts(raw,x,ln,code,n,day,out))
        count.append({'code':code,'name':nm,'eligible_days':len(x),'line_state_changes':int(x.line_state_changed.sum()),
                      'upper_cross_days':int(x.any_known_upper_break.sum()),'3bar_verified_upper_cross_days':int(x.upper_break_with_3bar_high_exceed.sum()),
                      'long_upper_wick_days':int(x.long_upper_wick_vs_body.sum())})
        print('CASE_DONE',code,nm,'eligible',len(x),'upper_cross_dates',int(x.any_known_upper_break.sum()),'state_change_days',int(x.line_state_changed.sum()),flush=True)
    combined=pd.concat(all_days,ignore_index=True)
    life=pd.concat(lives,ignore_index=True)
    outcome=pd.concat(later,ignore_index=True)
    case=pd.DataFrame(examples)
    if 'EXPOST' in '|'.join(combined.columns).upper():raise AssertionError('Future outcome leaked into PIT feature table')
    if combined[['code','date']].duplicated().any():raise AssertionError('Duplicated PIT stock-date')
    events=combined[combined.event_day].copy()
    fused=combined[['code','name','date','idx','candle_shape_diagnostic','close_vs_prev3_high_max_class',
      'any_known_upper_break','upper_break_with_3bar_high_exceed','long_upper_wick_vs_body']].merge(
      outcome[['code','date','dp20_ret_pct_EXPOST','dp20_complete_EXPOST']],on=['code','date'],validate='one_to_one')
    descriptions=[]
    for keys,g in fused.groupby(['candle_shape_diagnostic','any_known_upper_break','close_vs_prev3_high_max_class'],dropna=False):
        v=g.loc[g.dp20_complete_EXPOST,'dp20_ret_pct_EXPOST'].dropna()
        descriptions.append({'candle_shape':keys[0],'upper_line_cross_today':keys[1],
           'close_vs_prev3_high':keys[2],'stock_dates':len(g),'different_stocks':g.code.nunique(),
           'dplus20_full':len(v),'dplus20_positive_pct_EXPOST':round(100*(v>0).mean(),2) if len(v) else None,
           'dplus20_median_ret_pct_EXPOST':round(float(v.median()),3) if len(v) else None,
           'warning':'OVERLAPPING_DATES_DESCRIPTIVE_NOT_PROBABILITY'})
    descriptions=pd.DataFrame(descriptions)
    def export(df,name):
        df.to_csv(out/name,index=False,encoding='utf-8-sig')
    export(combined,'R1D108_ALL_ELIGIBLE_PIT_CANDLE_TREND_MA_BB_ENV_CLOUD_FLOW.csv')
    export(life,'R1D108_TRENDLINE_ACTIVE_STATE_LIFECYCLE_PIT.csv')
    export(events,'R1D108_LINE_CHANGE_AND_CROSS_CANDLE_CONTEXT_PIT.csv')
    export(outcome,'R1D108_CANDLE_FORWARD_VALIDATION_EXPOST.csv')
    export(case,'R1D108_NINE_ACTUAL_CASES_CANDLE_CONTEXT_PIT.csv')
    export(descriptions,'R1D108_PATTERN_CROSS_COMPARISON_DESCRIPTIVE_EXPOST.csv')
    export(pd.DataFrame(count),'R1D108_STOCK_COUNTS.csv')
    # Strongly separate source/current features from future labels even in presentation.
    doc=['# R1-D108 실제 캔들·변곡선·전체 기술지표 통합 사례','',
         '원본: 동결된 R1-D97 OHLCV, R1-D106 추세선, R1-D105의 R1-D104 기반 PIT 지표.',
         '점수·캔들 모양·돌파는 수익률이나 매매 신호가 아닙니다. D+는 결과 확인용 사후 관찰입니다.','']
    for row in examples:
        c=row
        doc.append(f"## {c['name']} {c['date']} — {c['case_kind']}")
        doc.append(f"종가 {c['close']:,.0f}원 · 몸통 {c['candle_body_share_pct']:.1f}% · 윗꼬리 {c['candle_upper_wick_share_pct']:.1f}% · 아랫꼬리 {c['candle_lower_wick_share_pct']:.1f}% · 종가위치 {c['candle_close_location_pct']:.1f}%")
        doc.append(f"2일 전 고가대비 종가 {c['close_vs_tminus2_high_pct']:.2f}% · 3일 전 {c['close_vs_tminus3_high_pct']:.2f}% · 직전 3거래일 최고가대비 종가 {c['close_vs_prev3_high_max_pct']:.2f}%")
        doc.append(f"양봉/음봉 구조: {c['candle_shape_diagnostic']} · 상단 돌파선 후보 {int(c['cross_upper_up'])}개 · 하단선 하향교차 {int(c['cross_lower_down'])}개")
        doc.append(f"MA20 기울기(20봉): {c.get('ma20_slope20_pct',float('nan')):.2f}% · BB20 폭: {c.get('bb20_width_pct',float('nan')):.2f}% · 거래대금/20일평균: {c.get('amount_vs_prev20',float('nan')):.2f}배\n")
    (out/'R1D108_ACTUAL_CANDLE_TREND_CASE_REPORT_KO.md').write_text('\n'.join(doc),encoding='utf8')
    for p in (Path(r105_path),Path(r106_dir)/'R1D106_ALL_IMMUTABLE_LINES.csv',Path(r106_dir)/'R1D106_LINE_CROSS_EVENTS_PIT.csv',Path(r106_dir)/'R1D106_ALL_TRIANGLE_CANDIDATES_PIT.csv'):
        if hashlib.sha256(p.read_bytes()).hexdigest()!=hashes[p.name]:raise AssertionError('Immutable upstream file changed '+p.name)
    audit={'version':'R1D108_DYNAMIC_LINE_CANDLE_FULL_CONFLUENCE_RESEARCH_ONLY',
      'source_stock_dates':len(combined),'stocks':count,
      'state_rows':len(life),'event_stock_dates':len(events),'expost_rows':len(outcome),
      'fixed_actual_cases':len(case),'charts':[x for x in charts if x],
      'upstream_sha256_unchanged':True,'raw_source_verified_by_R104_SHA256':verified_frozen_ohlcv,
      'time_is_pit':True,'full_market_oos':False,
      'notes':['Upper/lower wick ratios to body undefined for doji','2/3-bar exact and rolling-high comparisons are separate',
               'Dynamic line states are provisional fixed diagnostics, not optimized buy/sell rules',
               'Full 13 MA, 20/30/40 BB, 20/40 ENV and cloud indicators imported intact from R105 PIT snapshots',
               'Historic R106 line catalog never expires or mutates; active roles can change',
               'EXPOST outputs never feed feature/event selection',
               '3 stocks with overlapping dates: not a validated predictive model']}
    (out/'R1D108_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ('source_stock_dates','state_rows','event_stock_dates','expost_rows','fixed_actual_cases','upstream_sha256_unchanged')},ensure_ascii=False),flush=True)
    return audit

if __name__=='__main__':run()
