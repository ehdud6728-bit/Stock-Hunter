"""R1-D109 PIT trend-CHANGE sequence × candle/MA/BB/ENV/cloud/flow research.

Uses existing completed R1-D108 and R1-D106 outputs unchanged. This is not a
return-optimized screen. Pivot dates, confirmations, active-line breaks,
re-anchors, and later possible role retests are separated chronologically.
"""
from __future__ import annotations
import hashlib,json,sys
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d106'),str(ROOT/'r1d108'),str(HERE)]
from turning_points import confirmed_pivots,line_transition_events,role_retest_events

R108=ROOT/'r1d108'/'results'
R106=ROOT/'r1d106'/'results'
FIELDS={
    'full':'R1D108_ALL_ELIGIBLE_PIT_CANDLE_TREND_MA_BB_ENV_CLOUD_FLOW.csv',
    'state':'R1D108_TRENDLINE_ACTIVE_STATE_LIFECYCLE_PIT.csv',
    'expost':'R1D108_CANDLE_FORWARD_VALIDATION_EXPOST.csv',
    'lines':'R1D106_ALL_IMMUTABLE_LINES.csv'}
REFS=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-02-23'),
      ('071840','롯데하이마트','2024-03-13'),('006400','삼성SDI','2024-02-07')]
BASE_TECH=[
    'ma5_slope20_pct','ma20_slope20_pct','ma60_slope20_pct','ma112_slope20_pct','ma224_slope20_pct',
    'ma5_gap_pct','ma20_gap_pct','ma60_gap_pct','ma112_gap_pct','ma224_gap_pct',
    'ma20_slope_accel5_pp','bb20_width_pct','bb20_width_delta5_pp','bb20_upper_slope5_pct',
    'bb20_lower_slope5_pct','bb30_width_pct','bb40_width_pct',
    'env20_10_position','env40_10_position',
    'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct',
    'ma_cloud_fast_height_vs_price_pct','ichimoku_current_thickness_pct',
    'candle_shape_diagnostic','candle_body_share_pct','candle_upper_wick_share_pct',
    'candle_lower_wick_share_pct','candle_close_location_pct','close_vs_prev3_high_max_pct',
    'volume_vs_prev20','amount_vs_prev20']


def read_sources(base_r108=R108,base_r106=R106):
    paths={
      'full':Path(base_r108)/FIELDS['full'],'state':Path(base_r108)/FIELDS['state'],
      'expost':Path(base_r108)/FIELDS['expost'],'lines':Path(base_r106)/FIELDS['lines']}
    for p in paths.values():
        if not p.is_file():raise FileNotFoundError('Run R1-D105, R1-D106 and R1-D108 first: '+str(p))
    hashes={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    vals={k:pd.read_csv(p,dtype={'code':str},low_memory=False) for k,p in paths.items()}
    for d in vals.values():d['code']=d.code.str.zfill(6)
    return vals,paths,hashes


def _require_sources(src):
    full,states,future,lines=(src[k] for k in ('full','state','expost','lines'))
    if full[['code','idx']].duplicated().any():raise AssertionError('Duplicate R108 PIT stock-date')
    if future[['code','idx']].duplicated().any():raise AssertionError('Duplicate R108 outcome stock-date')
    if states[['code','idx','lane']].duplicated().any():raise AssertionError('Duplicate R108 active lane')
    if lines[['code','line_id']].duplicated().any():raise AssertionError('Duplicate frozen R106 line')
    required=['date','idx','code','name','open','high','low','close','amount_vs_prev20',
              'candle_body_share_pct','ma20_slope20_pct','bb20_width_pct','env20_10_position',
              'ma_cloud_fast_thickness_pct','ichimoku_current_thickness_pct']
    for k in required:
        if k not in full:raise AssertionError('Missing full technical PIT context '+k)
    if any('EXPOST' in k.upper() for k in full.columns):raise AssertionError('Forward outcome leaked into PIT full table')


def run(*,base_r108=R108,base_r106=R106,out=None,draw=True,refs=REFS):
    src,paths,hashes=read_sources(base_r108,base_r106)
    _require_sources(src)
    output=Path(out or HERE/'results');output.mkdir(parents=True,exist_ok=True)
    if draw:(output/'charts').mkdir(exist_ok=True)
    all_days=[];all_pivots=[];all_transitions=[];all_roles=[];all_episodes=[];all_history=[];all_expost=[];all_cases=[];charts=[];counts=[]
    for code,group in src['full'].groupby('code',sort=True):
        d=group.sort_values('idx').copy().reset_index(drop=True)
        nm=str(d.iloc[0]['name'])
        states=src['state'][src['state'].code.eq(code)].copy().sort_values(['idx','lane'])
        ln=src['lines'][src['lines'].code.eq(code)].copy()
        if len(states)!=len(d)*4:raise AssertionError('Not exactly four active line lanes per day '+code)
        piv,pivot_day=confirmed_pivots(d)
        changes=line_transition_events(states)
        roles=role_retest_events(d,states,ln)
        if not piv.empty:all_pivots.append(piv)
        if not changes.empty:all_transitions.append(changes)
        if not roles.empty:all_roles.append(roles)
        bydate=(changes.groupby('idx',as_index=False).agg(
            active_support_breaks=('transition_kind_PIT',lambda a:int((a=='ACTIVE_SUPPORT_BROKEN').sum())),
            active_resistance_breaks=('transition_kind_PIT',lambda a:int((a=='ACTIVE_RESISTANCE_BROKEN').sum())),
            active_slope_sign_flips=('slope_sign_flip','sum'),
            reanchors=('line_state',lambda a:int((a=='REANCHORED_NEW_PIVOT_OR_CANDLE').sum())),
            new_lines_after_break=('transition_kind_PIT',lambda a:int((a=='NEW_LINE_AFTER_PRIOR_BREAK').sum()))
         ) if not changes.empty else pd.DataFrame(columns=['idx','active_support_breaks','active_resistance_breaks','active_slope_sign_flips','reanchors','new_lines_after_break']))
        daily=d.merge(pivot_day.drop(columns=['code','date']),on='idx',how='left',validate='one_to_one')
        daily=daily.merge(bydate,on='idx',how='left',validate='one_to_one')
        event_cols=['active_support_breaks','active_resistance_breaks','active_slope_sign_flips','reanchors','new_lines_after_break']
        for k in event_cols:daily[k]=daily[k].fillna(0).astype(int)
        daily['any_active_break']=(daily.active_support_breaks+daily.active_resistance_breaks)>0
        daily['any_line_reanchor']=daily.reanchors>0
        daily['pivot_turn_confirmed_today']=daily.pivot_structure_changed_today.fillna(False).astype(bool)
        daily['candidate_trend_change_today']=daily.any_active_break|daily.pivot_turn_confirmed_today|daily.active_slope_sign_flips.gt(0)
        daily['has_new_pivot_today']=daily.confirmed_high_today|daily.confirmed_low_today
        daily['structural_event_today']=daily.candidate_trend_change_today|daily.has_new_pivot_today|daily.any_line_reanchor
        daily['trend_reversal_proven_today']=False
        # Technical indicators jointly present at t; do not collapse to a score.
        daily['ma20_slope_turn_positive_today']=(daily.ma20_slope20_pct.shift(1).le(0)&daily.ma20_slope20_pct.gt(0)).fillna(False)
        daily['ma20_slope_turn_negative_today']=(daily.ma20_slope20_pct.shift(1).ge(0)&daily.ma20_slope20_pct.lt(0)).fillna(False)
        daily['bb20_expansion_now']=daily.bb20_width_delta5_pp.gt(0)
        daily['large_body_now']=daily.candle_body_share_pct.ge(65)
        daily['three_bar_high_reclaimed_now']=daily.close_vs_prev3_high_max_pct.gt(.3)
        daily['turnover_above_prev20_now']=daily.amount_vs_prev20.gt(1)
        # Scope-specific dates: no invented data before R108 eligible period.
        if daily[['code','date']].duplicated().any():raise AssertionError('Daily row duplication')
        all_days.append(daily)
        study=daily[daily.structural_event_today].copy()
        if len(study):
            look=dict(zip(daily.idx,daily.to_dict('records')))
            for ev in study.to_dict('records'):
                for rel in (-5,-3,-1,0):
                    x=look.get(int(ev['idx'])+rel)
                    if x is None:continue
                    rec={'code':code,'name':nm,'anchor_date':ev['date'],'anchor_idx':ev['idx'],
                         'relative_bar':rel,'observation_date':x['date'],
                         'observed_at_or_before_event':True,
                         'pivot_structure_PIT':x['pivot_structure_PIT'],
                         'trend_change_candidate_today':x['candidate_trend_change_today']}
                    for c in BASE_TECH:rec[c]=x.get(c)
                    all_history.append(rec)
        # Choose spaced EVENT anchors without consulting their outcomes. They
        # are descriptive episodes, NOT independent return observations.
        prev_anchor=-10**9
        episodes=[]
        for ev in daily[daily.candidate_trend_change_today].to_dict('records'):
            if int(ev['idx'])-prev_anchor<20:continue
            prev_anchor=int(ev['idx']);episodes.append(ev)
        if episodes:all_episodes.extend(episodes)
        actual_out=src['expost'][src['expost'].code.eq(code)].copy()
        future_cols=['code','idx','date']+[c for c in actual_out if 'EXPOST' in c.upper()]
        selected=daily[daily.candidate_trend_change_today][['code','name','idx','date',
           'pivot_structure_PIT','pivot_structure_changed_today','active_support_breaks',
           'active_resistance_breaks','active_slope_sign_flips','candle_body_share_pct',
           'candle_upper_wick_share_pct','candle_lower_wick_share_pct',
           'ma20_slope20_pct','bb20_width_delta5_pp','amount_vs_prev20']]
        future=selected.merge(actual_out[future_cols],on=['code','idx','date'],validate='one_to_one',how='left')
        if len(future):
            future['EXPOST_DISCLAIMER']='DEPENDENT_STOCK_DATES_NOT_WIN_RATE'
            all_expost.append(future)
        cases=[(n,day) for c,n,day in refs if c==code]
        for n,day in cases:
            hit=daily[daily.date.eq(day)]
            if len(hit)!=1:raise AssertionError('Missing fixed reference case '+str((code,day)))
            row=hit.iloc[0].to_dict()
            before=daily[daily.idx.between(int(row['idx'])-10,int(row['idx']))]
            relevant=before[before.structural_event_today]
            brief=[]
            for rr in relevant.to_dict('records'):
                kinds=[]
                if rr['active_support_breaks']:kinds.append('지지선 이탈 '+str(rr['active_support_breaks']))
                if rr['active_resistance_breaks']:kinds.append('저항선 돌파 '+str(rr['active_resistance_breaks']))
                if rr['active_slope_sign_flips']:kinds.append('새 추세선 기울기 부호 변화 '+str(rr['active_slope_sign_flips']))
                if rr['pivot_structure_changed_today']:kinds.append('확정 피벗 구조 변화')
                if rr['has_new_pivot_today']:kinds.append('피벗 신규 확인')
                if rr['any_line_reanchor']:kinds.append('추세선 기준점 갱신 '+str(rr['reanchors']))
                brief.append({'date':rr['date'],'details':' / '.join(kinds)})
            all_cases.append({'code':code,'name':n,'date':day,'close':row['close'],
              'pivot_structure_asof':row['pivot_structure_PIT'],
              'upper_breaks_today':row['active_resistance_breaks'],
              'lower_breaks_today':row['active_support_breaks'],
              'slope_sign_flips_today':row['active_slope_sign_flips'],
              'candle_body_share_pct':row['candle_body_share_pct'],
              'candle_upper_wick_share_pct':row['candle_upper_wick_share_pct'],
              'candle_lower_wick_share_pct':row['candle_lower_wick_share_pct'],
              'ma20_slope20_pct':row['ma20_slope20_pct'],
              'bb20_width_pct':row['bb20_width_pct'],
              'amount_vs_prev20':row['amount_vs_prev20'],
              'events_last10_PIT':brief})
            if draw:charts.append(_chart_case(daily,states,piv,code,n,day,output))
        counts.append({'code':code,'name':nm,'days':len(daily),'pivot_confirmations':len(piv),
           'active_transition_events':len(changes),'slope_sign_flip_reanchors':int(changes.slope_sign_flip.sum()) if len(changes) else 0,
           'structural_event_days':int(daily.structural_event_today.sum()),
           'candidate_trend_change_days':int(daily.candidate_trend_change_today.sum()),
           'role_retest_evidence_rows':len(roles)})
        print('CASE_DONE',code,nm,'days',len(daily),'pivot',len(piv),'line_changes',len(changes),
              'candidate_change_dates',int(daily.candidate_trend_change_today.sum()),flush=True)
    result=pd.concat(all_days,ignore_index=True)
    if any('EXPOST' in k.upper() for k in result.columns):raise AssertionError('Future data leaked into PIT table')
    reports={
      'R1D109_ALL_DAYS_PIT_TREND_CHANGE_FULL_CONFLUENCE.csv':result,
      'R1D109_PIT_PIVOT_CONFIRMATIONS.csv':pd.concat(all_pivots,ignore_index=True) if all_pivots else pd.DataFrame(),
      'R1D109_PIT_ACTIVE_LINE_CHANGES.csv':pd.concat(all_transitions,ignore_index=True) if all_transitions else pd.DataFrame(),
      'R1D109_PIT_OLD_LINE_ROLE_RETESTS.csv':pd.concat(all_roles,ignore_index=True) if all_roles else pd.DataFrame(),
      'R1D109_PIT_PRE_EVENT_TIMELINES.csv':pd.DataFrame(all_history),
      'R1D109_20BAR_SPACED_CHANGE_EPISODES_PIT.csv':pd.DataFrame(all_episodes),
      'R1D109_TREND_CHANGE_OUTCOMES_EXPOST.csv':pd.concat(all_expost,ignore_index=True) if all_expost else pd.DataFrame(),
      'R1D109_STOCK_COUNTS.csv':pd.DataFrame(counts)}
    for fname,frame in reports.items():frame.to_csv(output/fname,index=False,encoding='utf-8-sig')
    for key,p in paths.items():
        if hashlib.sha256(p.read_bytes()).hexdigest()!=hashes[key]:raise AssertionError('Frozen upstream changed '+key)
    audit={'version':'R1D109_PIT_DYNAMIC_TREND_CHANGES_FULL_TECH_RESEARCH_ONLY',
      'sources':'R1D108 full eligible 320-field PIT OHLCV/indicator table + R108 active line states + R106 immutable lines',
      'source_stocks':len(counts),'stock_dates':len(result),'indicator_columns':len(result.columns),
      'pivot_confirmations':sum(x['pivot_confirmations'] for x in counts),
      'active_line_transition_events':sum(x['active_transition_events'] for x in counts),
      'reanchor_slope_sign_flip_events':sum(x['slope_sign_flip_reanchors'] for x in counts),
      'candidate_change_stock_dates':sum(x['candidate_trend_change_days'] for x in counts),
      'role_retest_rows':sum(x['role_retest_evidence_rows'] for x in counts),
      'spaced_20bar_episodes':len(all_episodes),'chart_cases':len(charts),
      'PIT_SIGNAL_EXPOST_SEPARATED':True,'UPSTREAM_SHA256_UNCHANGED':True,
      'source_sha256':hashes,'charts':charts,
      'limitations':['Three actual KRX stock histories only; no market-wide OOS or predictive accuracy established',
        'R108 source context eligible dates only; pivot left neighborhood truncated on its first four eligible days',
        'An active line re-anchor changes LINE slope; it does not retroactively change prior historical line',
        'Geometry sign flip is not proof of reversal; candle family differences audited separately',
        'A support/resistance break is candidate change only, not confirmed reversal',
        'Former line re-tests are sequentially observed, not known at the break signal date',
        'OHLCV unadjusted; corporate actions not fully audited; overlapping dated observations',
        'All forward returns segregated in EXPOST output; no return-optimized scoring']}
    (output/'R1D109_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf8')
    _case_report(all_cases,output)
    print('SUMMARY',json.dumps({k:audit[k] for k in ('stock_dates','pivot_confirmations','active_line_transition_events','reanchor_slope_sign_flip_events','candidate_change_stock_dates','UPSTREAM_SHA256_UNCHANGED')},ensure_ascii=False),flush=True)
    return audit


def _chart_case(d,states,piv,code,name,day,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    ft=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if ft.exists():font_manager.fontManager.addfont(str(ft));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    hit=d.index[d.date.eq(day)]
    if len(hit)!=1:return None
    t=int(hit[0]);beg=max(0,t-40);end=min(len(d)-1,t+7)
    vis=d.iloc[beg:end+1];bars=vis.idx.to_numpy(int);signal_idx=int(d.iloc[t].idx)
    fig=plt.figure(figsize=(15,12))
    gs=fig.add_gridspec(4,1,height_ratios=[5,1,1,1],hspace=.09)
    ax,ma,bb,flow=[fig.add_subplot(gs[i]) for i in range(4)]
    for i in range(beg,end+1):
        q=d.iloc[i];x=int(q.idx);color='#db4f43' if q.close>=q.open else '#3471a3'
        ax.vlines(x,q.low,q.high,color=color,lw=.85)
        ax.add_patch(Rectangle((x-.32,min(q.open,q.close)),.64,max(.1,abs(q.close-q.open)),color=color,alpha=.8))
    for n,color in ((20,'#2563eb'),(60,'#16a34a')):
        col=f'ma{n}_value'
        if col in vis:ax.plot(bars,vis[col],color=color,lw=1,label=f'MA{n}')
    # The R108 lanes are dynamic historically: draw segments for each active line ID.
    # We draw their observed as-of levels, never a later line retroactively before known_date.
    for lane,color in (('UPPER_SHORT','#e78a17'),('LOWER_SHORT','#15916a'),('UPPER_MEDIUM','#9a46b6'),('LOWER_MEDIUM','#007ab7')):
        q=states[states.lane.eq(lane)&states.idx.between(int(bars[0]),int(bars[-1]))].copy().sort_values('idx')
        for _,sub in q.groupby((q.active_line_id.ne(q.active_line_id.shift())).cumsum()):
            sub=sub[sub.active_line_id.notna()&sub.active_line_id.ne('')&sub.active_line_level.notna()]
            if len(sub)>1:ax.plot(sub.idx,sub.active_line_level,color=color,lw=1.5,alpha=.8,label=lane if lane not in ax.get_legend_handles_labels()[1] else None)
    if len(piv):
        # Historical pivot markers may appear only once confirmed as of the chart ref.
        for kind,mark,c in (('HIGH','v','#a647a1'),('LOW','^','#0f9b79')):
            a=piv[piv.pivot_type.eq(kind)&piv.pivot_idx.between(int(bars[0]),signal_idx)&piv.confirmed_idx.le(signal_idx)]
            if len(a):ax.scatter(a.pivot_idx,a.pivot_price,marker=mark,s=38,color=c,zorder=4,label=f'확정 {kind} 피벗')
    for i in range(beg,end+1):
        q=d.iloc[i]
        if q['candidate_trend_change_today'] and int(q.idx)<=signal_idx:
            ax.axvline(int(q.idx),color='#64748b',alpha=.25,ls=':',lw=.9)
    ma.axhline(0,color='#777777',lw=.8)
    for n,color in ((20,'#2563eb'),(60,'#16a34a'),(224,'#64748b')):
        col=f'ma{n}_slope20_pct'
        if col in vis:ma.plot(bars,vis[col],lw=1.1,color=color,label=f'MA{n} 20일기울기')
    bb.plot(bars,vis.bb20_width_pct,lw=1.2,color='#bf7800',label='BB20 폭 %')
    flow.plot(bars,vis.amount_vs_prev20,lw=1,color='#334155',label='거래대금/직전20일평균')
    flow.axhline(1,color='#d46a49',ls='--',lw=.9)
    for a in (ax,ma,bb,flow):
        a.grid(alpha=.15);a.axvline(signal_idx,color='#111827',ls='--',lw=1.25)
        a.axvspan(signal_idx,int(bars[-1]),color='#e5a54c',alpha=.07)
    ax.legend(loc='upper left',fontsize=7,ncol=4)
    for a in (ma,bb,flow):a.legend(loc='upper left',fontsize=7,ncol=3)
    ticks=np.linspace(0,len(vis)-1,min(8,len(vis)),dtype=int)
    flow.set_xticks(bars[ticks]);flow.set_xticklabels(vis.iloc[ticks].date.str[5:],rotation=22)
    for a in (ax,ma,bb):a.tick_params(labelbottom=False)
    fig.suptitle(f'{name} {day} | PIT 변곡 확인·기존/신규 추세선·캔들·MA/BB/거래대금',fontsize=14)
    fname=f'{code}_{day}_R1D109_DYNAMIC_TREND_CHANGE.png'
    fig.savefig(out/'charts'/fname,dpi=125,bbox_inches='tight');plt.close(fig)
    return fname


def _case_report(cases,out):
    notes=['# R1-D109 실제 추세선 변화의 발생 순서 — PIT 사례','',
      '기존 추세선 돌파/이탈 = 시장 추세 반전 확정이 아닙니다. 피벗 확정일과 신규선 확정일은 구분합니다.','',
      '이평선·BB·ENV·구름대·캔들·거래대금은 동일 거래일 R108 PIT 원본에서 가져왔습니다.','']
    for c in cases:
        notes.append(f"## {c['name']} ({c['code']}) {c['date']}")
        for k,title in [('pivot_structure_asof','확정 피벗 구조'),('close','종가'),
               ('upper_breaks_today','활성 저항 돌파 개수'),('lower_breaks_today','활성 지지 이탈 개수'),
               ('slope_sign_flips_today','활성선 기울기 부호 변경 수'),('candle_body_share_pct','몸통 비율 %'),
               ('candle_upper_wick_share_pct','윗꼬리 비율 %'),('candle_lower_wick_share_pct','아랫꼬리 비율 %'),
               ('ma20_slope20_pct','MA20 기울기 %'),('bb20_width_pct','BB20 폭 %'),
               ('amount_vs_prev20','거래대금 / 이전20일 평균')]:notes.append(f'- {title}: {c[k]}')
        notes.append('- 전 10거래일 동안 당일 확정 가능한 사건:')
        for v in c['events_last10_PIT']:
            notes.append(f"  - {v['date']}: {v['details']}")
        notes.append('')
    (out/'R1D109_THREE_STOCKS_FOUR_TREND_TURN_CASES_KO.md').write_text('\n'.join(notes),encoding='utf8')


if __name__=='__main__':run()
