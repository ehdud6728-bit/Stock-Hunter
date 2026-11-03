"""R1-D110: causal event-episode assembly from R1-D109 full PIT state.

NEVER edits R1-D109 files. Outcomes in isolated EXPOST outputs. All thresholds
are fixed explanatory descriptions, not backtest-optimized trading rules.
"""
from __future__ import annotations
import hashlib,json
from pathlib import Path
import pandas as pd
import numpy as np
from episodes import compute,expost_outcomes,pre_anchor_context,validate_source,TECH,day_components
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
R109=ROOT/'r1d109'/'results'
INPUTS={
 'daily':'R1D109_ALL_DAYS_PIT_TREND_CHANGE_FULL_CONFLUENCE.csv',
 'role':'R1D109_PIT_OLD_LINE_ROLE_RETESTS.csv',
 'pivot':'R1D109_PIT_PIVOT_CONFIRMATIONS.csv',
 'transitions':'R1D109_PIT_ACTIVE_LINE_CHANGES.csv',
}
CASES=[('039840','2024-09-09'),('039840','2024-09-10'),('039840','2024-09-13'),('039840','2024-09-20'),
       ('071840','2024-02-20'),('071840','2024-02-22'),('071840','2024-03-13'),('071840','2024-03-14'),
       ('006400','2024-02-07'),('006400','2024-02-16')]


def load_inputs(base):
    base=Path(base);paths={key:base/name for key,name in INPUTS.items()}
    for p in paths.values():
        if not p.is_file():raise FileNotFoundError('Run R1-D109 first: '+str(p))
    hashes={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    frames={k:pd.read_csv(p,dtype={'code':str},low_memory=False) for k,p in paths.items()}
    for f in frames.values():
        f['code']=f.code.str.zfill(6)
        if 'date' in f.columns:f['date']=f['date'].astype(str)
    validate_source(frames['daily'])
    if frames['role'][['code','idx','broken_line_id','date','role_observation_PIT']].isna().all(axis=None):
        raise ValueError('empty role context')
    return frames,paths,hashes


def _simultaneous_case_days(observed):
    """Group multiple component events per close; no false intraday ordering."""
    first=observed[observed.is_first_this_component_in_episode]
    blocks=[]
    for day,g in first.groupby('observation_date',sort=True):
        blocks.append(str(day)+'['+'|'.join(sorted(set(g.component.astype(str))))+']')
    return ' / '.join(blocks)


def _case_audit(d,opens,timeline,onsets,cases):
    records=[]
    for code,date in cases:
        hit=d[d.code.eq(code)&d.date.eq(date)]
        if len(hit)!=1:raise AssertionError('Missing case '+str((code,date)))
        row=hit.iloc[0]
        rel=timeline[(timeline.code==code)&(timeline.idx==row.idx)]
        in_ep=bool(len(rel));found=rel.iloc[0] if in_ep else None
        # ONLY read events observed BY case date; not the future episode summary.
        observed=onsets[(onsets.code==code)&(onsets.observation_idx<=row.idx)]
        if in_ep:observed=observed[observed.episode_id==found.episode_id]
        else:observed=observed.iloc[:0]
        record={'code':code,'name':row['name'],'reference_date':date,'reference_idx':int(row.idx),
          'close':float(row.close),'episode_active_at_reference':in_ep,
          'episode_id_asof':str(found.episode_id) if in_ep else '',
          'episode_anchor_date_asof':str(found.episode_anchor_date) if in_ep else '',
          'episode_age_bars_asof':int(found.bar_from_anchor) if in_ep else None,
          'observed_components_through_reference_PIT':_simultaneous_case_days(observed),
          'reference_day_events_PIT':str(found.component_events_today) if in_ep else '',
          'pivot_structure_PIT':row.pivot_structure_PIT,
          'candle_body_share_pct':row.candle_body_share_pct,
          'candle_upper_wick_share_pct':row.candle_upper_wick_share_pct,
          'candle_lower_wick_share_pct':row.candle_lower_wick_share_pct,
          'close_vs_prev3_high_max_pct':row.close_vs_prev3_high_max_pct,
          'ma20_slope20_pct':row.ma20_slope20_pct,
          'ma224_slope20_pct':row.ma224_slope20_pct,
          'ma20_gap_pct':row.ma20_gap_pct,
          'bb20_width_pct':row.bb20_width_pct,
          'bb20_width_delta5_pp':row.bb20_width_delta5_pp,
          'env20_10_position':row.env20_10_position,
          'ma_cloud_fast_position':row.ma_cloud_fast_position,
          'ichimoku_current_position':row.ichimoku_current_position,
          'amount_vs_prev20':row.amount_vs_prev20,
          'data_only_as_of_reference_close':True}
        records.append(record)
    return pd.DataFrame(records)


def chart(d,opens,onsets,code,date,out):
    """As-of line snapshots on each day; never back-project a future active line."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.exists():font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    q=d[d.code.eq(code)].sort_values('idx').reset_index(drop=True)
    h=np.flatnonzero(q.date.eq(date))
    if len(h)!=1:return None
    t=int(h[0]);lo=max(0,t-38);hi=min(len(q)-1,t+15)
    v=q.iloc[lo:hi+1]; x=v.idx.astype(int).to_numpy(); idx=int(q.iloc[t].idx)
    fig=plt.figure(figsize=(16,11)); gs=fig.add_gridspec(4,1,height_ratios=[4.5,1,1,1.1],hspace=.11)
    ax,ma,bb,flow=[fig.add_subplot(gs[k]) for k in range(4)]
    for r in v.itertuples():
        c='#dd584a' if r.close>=r.open else '#3476a8'; j=int(r.idx)
        ax.vlines(j,r.low,r.high,color=c,lw=.9)
        ax.add_patch(Rectangle((j-.3,min(r.close,r.open)),.6,max(.2,abs(r.close-r.open)),color=c,alpha=.83))
    for n,color in ((20,'#366bd3'),(60,'#16a085')):
        key=f'ma{n}_value'
        if key in v.columns:ax.plot(x,v[key],lw=1.2,color=color,alpha=.85,label=f'MA{n}')
    for col,color in (('bb20_upper','#c9922b'),('bb20_lower','#c9922b')):
        if col in v.columns:ax.plot(x,v[col],lw=.8,color=color,ls=':',alpha=.6,label='BB20 상·하단' if col.endswith('upper') else None)
    # Line levels are daily active snapshots; contiguous runs of the SAME line ID only.
    for lane,color in (('upper_short','#d46a2a'),('lower_short','#198a65')):
        idcol=lane+'_active_line_id';lev=lane+'_active_line_level'
        if idcol not in v:continue
        segment=(v[idcol].fillna('')!=v[idcol].fillna('').shift()).cumsum()
        for _,group in v.groupby(segment):
            vals=group[group[idcol].notna()&group[idcol].ne('')&group[lev].notna()]
            if len(vals)>=2:
                ax.plot(vals.idx,vals[lev],lw=1.3,color=color,alpha=.9,label=lane if lane not in ax.get_legend_handles_labels()[1] else None)
    ma.axhline(0,color='#666666',lw=.6)
    for n,col in ((20,'#366bd3'),(112,'#16a085'),(224,'#798494')):
        key=f'ma{n}_slope20_pct'
        if key in v:ma.plot(x,v[key],lw=1.1,color=col,label=f'MA{n} 20일기울기')
    bb.plot(x,v.bb20_width_pct,lw=1.3,color='#aa8120',label='BB20 폭')
    bb.plot(x,v.bb20_width_delta5_pp,lw=.85,color='#c25d53',label='BB20 폭 변화(5일)')
    flow.plot(x,v.amount_vs_prev20,lw=1.1,color='#555d6f',label='거래대금/20일 평균')
    flow.axhline(1,color='#b84d4d',ls='--',lw=.7)
    ev=onsets[onsets.code.eq(code)&onsets.observation_idx.between(int(x[0]),min(idx,int(x[-1])))]
    # Previously observed event dates only; plotting a future event as preknown is forbidden.
    mark=ev[ev.family.isin(['STRUCTURE','LINE'])].drop_duplicates('observation_idx')
    for j in mark.observation_idx:
        ax.axvline(int(j),color='#7b5da8',lw=.6,alpha=.35,ls=':')
    ep=opens[(opens.code==code)&(opens.anchor_idx.between(int(x[0]),idx))]
    for j in ep.anchor_idx:
        ax.axvline(int(j),color='#008b7b',lw=1,alpha=.5,ls='--')
    for a in (ax,ma,bb,flow):
        a.axvline(idx,color='#1f2937',lw=1.5,ls='--')
        a.axvspan(idx,int(x[-1]),alpha=.09,color='#c98644')
        a.grid(alpha=.14)
    ax.legend(loc='upper left',fontsize=7,ncol=3)
    for a in (ma,bb,flow):a.legend(loc='upper left',fontsize=8,ncol=3)
    ticks=np.linspace(0,len(v)-1,min(9,len(v)),dtype=int)
    flow.set_xticks(x[ticks]);flow.set_xticklabels(v.iloc[ticks].date.str[5:],rotation=25)
    for a in (ax,ma,bb):a.tick_params(labelbottom=False)
    name=str(q.iloc[t]['name']);ax.set_ylabel('KRW')
    fig.suptitle(f'R1-D110 | {name} {date} | PIT 변곡·추세선·캔들·MA·BB·거래대금 | 음영=사후관찰',fontsize=14)
    fname=f'{code}_{date}_R1D110_EPISODE_SEQUENCE.png'
    fig.savefig(out/fname,dpi=125,bbox_inches='tight');plt.close(fig)
    return fname


def run(*,source=R109,out=None,draw=True,case_refs=CASES,window=20):
    frames,paths,hashes=load_inputs(source)
    d=frames['daily'];roles=frames['role'];dest=Path(out or HERE/'results');dest.mkdir(parents=True,exist_ok=True)
    opens,timeline,onsets,closes=compute(d,roles,window=window)
    leadin=pre_anchor_context(d,opens)
    future=expost_outcomes(d,opens)
    cases=_case_audit(d,opens,timeline,onsets,case_refs)
    for x in (opens,timeline,onsets,closes,cases,leadin):
        if any('EXPOST' in c.upper() for c in x.columns):raise AssertionError('future outcome in PIT export')
    if opens.episode_id.duplicated().any() or timeline[['code','idx']].duplicated().any():raise AssertionError('duplicate episode')
    if (closes.end_idx<closes.anchor_idx).any():raise AssertionError('negative episode age')
    event_age=onsets.merge(opens[['episode_id','anchor_idx']],on='episode_id',how='left',validate='many_to_one')
    if event_age.anchor_idx.isna().any() or (event_age.observation_idx<event_age.anchor_idx).any():
        raise AssertionError('bad episode event chronology')
    reports={
     'R1D110_PIT_EPISODE_OPEN.csv':opens,
     'R1D110_PIT_PRE_ANCHOR_5BAR_CONTEXT.csv':leadin,
     'R1D110_PIT_EPISODE_DAILY_TIMELINE.csv':timeline,
     'R1D110_PIT_COMPONENT_EVENTS.csv':onsets,
     'R1D110_EPISODE_CLOSE_ASOF_ONLY.csv':closes,
     'R1D110_EPISODE_OUTCOMES_EXPOST.csv':future,
     'R1D110_REFERENCE_CASES_PIT_ASOF.csv':cases}
    for filename,table in reports.items():table.to_csv(dest/filename,index=False,encoding='utf-8-sig')
    imgs=[]
    if draw:
        chartdir=dest/'charts';chartdir.mkdir(exist_ok=True)
        for code,date in [('039840','2024-09-13'),('071840','2024-02-22'),('071840','2024-03-13'),('006400','2024-02-07')]:
            x=chart(d,opens,onsets,code,date,chartdir)
            if x:imgs.append(x)
    cnt={str(code):{'episode_count':int((opens.code==code).sum()),
      'component_observations':int((onsets.code==code).sum()),
      'right_censored_count':int((closes[closes.code==code].right_censored).sum())}
     for code in sorted(d.code.unique())}
    # Zero changes to all upstream R109 data.
    for k,p in paths.items():
        if hashlib.sha256(p.read_bytes()).hexdigest()!=hashes[k]:raise AssertionError('R109 input modified '+k)
    report={'version':'R1D110_TREND_EPISODE_TEMPORAL_CONFLUENCE_PIT_RESEARCH_ONLY',
       'source':'R1-D109 GitHub output PIT 348-field stock-date series; role as-of retests',
       'input_hashes':hashes,'UPSTREAM_SHA256_UNCHANGED':True,
       'stock_dates':len(d),'stocks':len(cnt),'unique_episodes':len(opens),
       'pre_anchor_rows':len(leadin),'episode_daily_rows':len(timeline),'component_event_rows':len(onsets),
       'complete_episodes':int(closes.complete_window.sum()),
       'right_censored_episodes':int(closes.right_censored.sum()),
       'case_reference_rows':len(cases),'charts':imgs,'per_stock':cnt,
       'PIT_SIGNAL_EXPOST_SEPARATED':True,
       'no_outcome_optimized_thresholds':True,
       'window_bars':window,
       'limitations':['Three stocks only, not full-market OOS. All episodes are descriptive, not independent trades.',
           '20-bar boundaries defined by first PIT structural candidate; no future returns used to anchor episodes',
           'Episode CLOSE summaries are known at episode end, never at episode opening',
           'Overlapping technical indicators (BB/MA/ENV) are NOT independent votes',
           'Within-bar component order cannot be inferred from daily candles',
           'Long upper/lower wicks are observations, not bearish/bullish causal proof',
           'The first eligible day contains only eligible-window context; 2-bar-confirmed pivots already PIT',
           'EXPOST results are separately exported and include right-censored horizon flags',
           'Corporate actions/unadjusted OHLCV not fully audited, cohort selection bias and regime risk remain']}
    (dest/'R1D110_AUDIT.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    _write_readme(cases,opens,onsets,closes,future,report,dest)
    print('SUMMARY',json.dumps({k:report[k] for k in ('stock_dates','unique_episodes','episode_daily_rows','component_event_rows','complete_episodes','right_censored_episodes','UPSTREAM_SHA256_UNCHANGED')},ensure_ascii=False),flush=True)
    return report


def _write_readme(cases,opens,onsets,closes,future,report,dest):
    lines=['# R1-D110 실제 PIT 변곡·추세전환 에피소드 통합 연구','',
        '모든 구성 요소는 관찰일 마감에 알려진 사실입니다. 같은 거래일에 발생한 지표의 선후 순서는 알 수 없습니다.','',
        f"실제 분석 {report['stocks']}종목 {report['stock_dates']}일, 구분된 에피소드 {report['unique_episodes']}개, 완료 {report['complete_episodes']}개.",
        '', '## 단계별 자료 활용',
        '- `PIT_EPISODE_OPEN`: 에피소드 첫날에 이미 알려진 정보만',
        '- `PIT_PRE_ANCHOR_5BAR_CONTEXT`: 시작일 이전 D-5..0 구간 지표 변화; 시작일에 이미 확인 가능한 정보만',
        '- `PIT_COMPONENT_EVENTS`: 각 지표군이 관찰된 **그 날**의 행 (미래 사전 누출 금지)',
        '- `PIT_EPISODE_DAILY_TIMELINE`: 매일 현재까지의 관찰 정보만',
        '- `EPISODE_CLOSE_ASOF_ONLY`: 마감일에 확정된 순서. **시작일 매매 신호로 사용 금지**',
        '- `EPISODE_OUTCOMES_EXPOST`: 별도 사후 가격 결과; 독립 표본/수익 예측 아님',
        '', '## 비교 사례 (기준일에 확인 가능했던 값)', '']
    for r in cases.to_dict('records'):
        lines.append(f"### {r['name']} {r['reference_date']}")
        lines.append(f"- 상태: {r['pivot_structure_PIT']}; 에피소드: {r['episode_id_asof'] or '해당 없음'}")
        lines.append(f"- 캔들 몸통 {r['candle_body_share_pct']:.1f}% / 윗꼬리 {r['candle_upper_wick_share_pct']:.1f}% / 아랫꼬리 {r['candle_lower_wick_share_pct']:.1f}%")
        lines.append(f"- MA20 기울기 {r['ma20_slope20_pct']:.2f}%, BB20 폭 {r['bb20_width_pct']:.2f}%, 거래대금 {r['amount_vs_prev20']:.2f}배")
        lines.append(f"- 현재까지 최초 확인 지표: {r['observed_components_through_reference_PIT'][:270]}" )
        lines.append('')
    lines += ['## 주의',
        'R1-D109 원본은 SHA256으로 보호합니다. R1-D107 분석은 실행 필요가 없어 별도 실행하지 않습니다.',
        '수렴·추세 변경·상승 결과는 동일한 것이 아닙니다. 다양한 다른 종목과 시장 국면에서 OOS가 필요합니다.']
    (dest/'R1D110_THREE_STOCK_EVENT_ORDER_REPORT_KO.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')

if __name__=='__main__':run()
