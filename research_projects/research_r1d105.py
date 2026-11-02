"""R1-D105 actual case studies and ALL-source event union, RESEARCH_ONLY.

Frozen upstream R1-D104 recalculated from real KRX input on GitHub.
Feature/event detection point-in-time. Forward 20-day outcome appended ONLY AFTER selection.
"""
from __future__ import annotations
import hashlib,json,sys,tempfile
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d100'),str(ROOT/'r1d101'),str(ROOT/'r1d102'),
              str(ROOT/'r1d103'),str(ROOT/'r1d104'),str(HERE)]
from event_sequences import detect_events,unique_event_dates,episode_anchors,attach_later_outcomes,collect_snapshots,episode_sequence,OUTCOME_FIELDS

FIXED=[('039840','디오','2024-09-13','FIXED_LOSING_BREAKOUT'),
       ('071840','롯데하이마트','2024-03-04','FIXED_WEAK_VOLUME'),
       ('006400','삼성SDI','2024-02-07','FIXED_REBOUND'),
       ('006400','삼성SDI','2024-01-22','EXPOST_REBOUND_EXAMPLE'),
       ('039840','디오','2024-08-02','EXPOST_DRAWNDOWN_EXAMPLE')]
CONTEXT_FILE='R1D104_ALL_ELIGIBLE_CONFLUENCE.csv'


def read_context(path):
    fr=pd.read_csv(path,dtype={'code':str});fr['code']=fr.code.str.zfill(6)
    if fr[['code','date']].duplicated().any():raise AssertionError('Duplicate input dates')
    if len(fr)==0:raise ValueError('Empty context')
    return fr.sort_values(['code','idx']).reset_index(drop=True)


def all_family_stats(catalog, context):
    if catalog.empty:return pd.DataFrame()
    table=catalog[['code','date','family']].drop_duplicates().merge(
        context[['code','date','forward20_close_pct_EXPOST','outcome_EXPOST']],on=['code','date'],validate='many_to_one')
    rows=[]
    for family,g in table.groupby('family'):
        y=g.forward20_close_pct_EXPOST.dropna()
        rows.append({'family':family,'stock_dates':len(g),'different_stocks':g.code.nunique(),
                     'positive20_pct':round(100*(y>0).mean(),2) if len(y) else None,
                     'median20_return_pct':round(float(y.median()),3) if len(y) else None,
                     'rise_cases':int((g.outcome_EXPOST=='SURGE').sum()),
                     'nonrise_cases':int((g.outcome_EXPOST=='CONTROL').sum()),
                     'warning':'DEPENDENT_DATES_AND_STOCKS_NOT_WIN_RATE'})
    return pd.DataFrame(rows).sort_values('stock_dates',ascending=False)


def complete_case_timeline(context, cases=FIXED):
    out=[]
    for code,name,date,kind in cases:
        g=context[context.code.eq(code)].sort_values('idx')
        anchor=g[g.date.eq(date)]
        if len(anchor)!=1:raise AssertionError('Missing fixed illustrative case '+code+' '+date)
        idx=int(anchor.iloc[0]['idx']);base=float(anchor.iloc[0]['close'])
        for offset in (-20,-10,-5,-1,0,1,5,10,20):
            t=g[g.idx.eq(idx+offset)]
            if len(t)!=1:
                if offset>0:continue # handle late anchor censoring, mark missing
                raise AssertionError('Missing D-minus prerequisite '+code+' '+date+' '+str(offset))
            row=t.iloc[0]
            item={'code':code,'name':name,'case_date':date,'case_kind':kind,
                  'relative_bar':offset,'observation_date':row['date'],'close':row['close'],
                  'change_from_anchor_close_pct':round(100*(float(row['close'])/base-1),3)}
            for key in ('structure','short_high_trend_direction','short_low_trend_direction',
              'short_high_trend_validated_cross','short_low_trend_validated_cross',
              'ma5_slope20_pct','ma20_slope20_pct','ma60_slope20_pct',
              'ma112_slope20_pct','ma224_slope20_pct','ma20_slope_accel5_pp',
              'bb20_position','bb20_width_pct','bb20_width_delta5_pp',
              'bb30_width_pct','bb40_width_pct',
              'env20_10_position','env20_10_upper','env20_10_lower',
              'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct',
              'ichimoku_current_thickness_pct','amount_vs_prev20',
              'volume_vs_prev20','forward20_close_pct_EXPOST'):
                item[key]=row[key]
            out.append(item)
    return pd.DataFrame(out)


def _plot_one(frame, code, name, date, out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.exists():
        font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    g=frame[frame.code.eq(code)].sort_values('idx').reset_index(drop=True)
    anchor=g[g.date.eq(date)];q=int(anchor.index[0]);start=max(0,q-65);end=min(len(g),q+21)
    d=g.iloc[start:end];x=d.idx.to_numpy(dtype=int);at=int(anchor.iloc[0]['idx'])
    fig=plt.figure(figsize=(16,11));gs=fig.add_gridspec(4,1,height_ratios=[5,1.5,1.5,1.5],hspace=.12)
    ax,bb,ma,vol=[fig.add_subplot(gs[i]) for i in range(4)]
    ax.plot(x,d.close,label='종가',color='#111827',lw=2)
    for period,color in [(5,'#dc2626'),(20,'#2563eb'),(60,'#16a34a'),(112,'#7c3aed'),(224,'#64748b')]:
        ax.plot(x,d[f'ma{period}_value'],label=f'MA{period}',color=color,lw=1.05,alpha=.85)
    for key,color in [('bb20','#f59e0b'),('env20_10','#db2777')]:
        ax.plot(x,d[key+'_upper'],color=color,lw=1.0,ls='-',label=key.upper()+' 상단')
        ax.plot(x,d[key+'_lower'],color=color,lw=1.0,ls='--',label=key.upper()+' 하단')
    # Dynamic confirmed high/low trendline values, each as known on THAT day.
    # Re-anchor changes are discontinuities, not historical lines redrawn with future anchors.
    for key,color,label in [('short_high_trend','#e11d48','동적 고점 추세선'),
                            ('short_low_trend','#059669','동적 저점 추세선')]:
        ax.plot(x,d[key+'_level'],color=color,ls=':',lw=1.25,label=label)
    ax.axvline(at,color='#111827',ls=':',lw=1.4)
    ax.axvspan(at,x[-1],color='#f59e0b',alpha=.09)
    ax.legend(loc='upper left',ncol=5,fontsize=7);ax.grid(alpha=.15)
    bb.plot(x,d.bb20_width_pct,label='BB20 폭 %',color='#f59e0b')
    bb.plot(x,d.ma_cloud_fast_thickness_pct,label='단기 이평 구름 두께 %',color='#059669')
    bb.plot(x,d.ma_cloud_mid_thickness_pct,label='중기 이평 구름 두께 %',color='#6366f1')
    bb.legend(loc='upper left',ncol=3,fontsize=8);bb.grid(alpha=.14)
    for n,c in [(5,'#dc2626'),(20,'#2563eb'),(60,'#16a34a'),(112,'#7c3aed'),(224,'#64748b')]:
        ma.plot(x,d[f'ma{n}_slope20_pct'],label=f'MA{n} 기울기',color=c,lw=1.1)
    ma.axhline(0,color='#444',lw=.7);ma.legend(loc='upper left',ncol=5,fontsize=8);ma.grid(alpha=.15)
    vol.bar(x,d.amount_vs_prev20,color='#94a3b8',label='당일 거래대금 / 이전20일평균')
    vol.axhline(1,color='#dc2626',ls='--',lw=1);vol.grid(alpha=.15);vol.legend(loc='upper left',fontsize=8)
    pos=np.linspace(0,len(d)-1,min(9,len(d)),dtype=int);vol.set_xticks(x[pos]);vol.set_xticklabels(d.iloc[pos].date.str[:10],rotation=25,fontsize=8)
    fig.suptitle(f'{name} {date} | 추세선·MA·BB·ENV·구름·거래대금 통합 사례 | 음영: 사후 관찰구간',fontsize=13)
    dest=out/'charts'/f'{code}_{date}_R1D105_CASE.png'
    fig.savefig(dest,dpi=130,bbox_inches='tight');plt.close(fig)
    return dest.name


def _case_markdown(cases, catalog, context):
    text=['# R1-D105 실제 종목 사례 — PIT 통합 지표 연구',
          '원자료: R1-D104 불변 KRX OHLCV, R1-D104 계산 로그에서 재현.',
          '선정된 날짜 이후 결과는 **사후 결과**이며 당시 신호에 사용하지 않았습니다.',
          '등장하는 진단 상태는 수익성 증거가 아닙니다.','']
    for code,name,date,kind in cases:
        a=context[(context.code.eq(code))&(context.date.eq(date))].iloc[0]
        eve=catalog[(catalog.code.eq(code))&(catalog.date.eq(date))]
        fmt=lambda key: ('결측' if pd.isna(a[key]) else f'{float(a[key]):+.2f}')
        text += [f'## {name} ({code}) {date} · {kind}',
                 f'- 종가 {int(a.close):,}원 / 미래 20거래일 종가 변화 {fmt("forward20_close_pct_EXPOST")}% (사후)',
                 f'- 고점·저점 추세 구조: {a.structure}, 신규 검증된 상단 돌파: {a.short_high_trend_validated_cross}, 하단 이탈: {a.short_low_trend_validated_cross}',
                 f'- MA5/20/60/112/224 기울기(최근 20일 %): ' + ', '.join(f'{n}일 {fmt(f"ma{n}_slope20_pct")}' for n in (5,20,60,112,224)),
                 f'- BB20: {a.bb20_position} / 폭 {fmt("bb20_width_pct")}% / 최근 5일 폭 변화 {fmt("bb20_width_delta5_pp")} %p',
                 f'- 엔벨로프20 ±10%: {a.env20_10_position} / 상·하단 {a.env20_10_upper:,.0f}원, {a.env20_10_lower:,.0f}원',
                 f'- 단기/중기 구름대 두께: {fmt("ma_cloud_fast_thickness_pct")}% / {fmt("ma_cloud_mid_thickness_pct")}%',
                 f'- 당일 거래대금 / 이전20일평균: {a.amount_vs_prev20:.2f}배',
                 f'- 당일 감지 이벤트 ({len(eve)}개): '+(' / '.join((eve.family+':'+eve.signal).tolist()[:15]) if len(eve) else '없음 — 고정 참조 날짜도 보존'),
                 '- 주의: 상승 또는 실패 원인을 단일 지표에 인과적으로 귀속할 수 없습니다.','']
    return '\n'.join(text)


def run(*,source_csv=None,out=None,plots=True):
    """Without source_csv, recompute frozen R1-D104 from GitHub stock data first."""
    output=Path(out) if out else HERE/'results';output.mkdir(parents=True,exist_ok=True);(output/'charts').mkdir(exist_ok=True)
    if source_csv is None:
        from research_r1d104 import run as upstream
        with tempfile.TemporaryDirectory(prefix='r1d105_frozen_r104_') as tmp:
            upstream(out=tmp,plot=False)
            context=read_context(Path(tmp)/CONTEXT_FILE)
        basis='R1D104_RECOMPUTED_KRX_SHA256_VERIFIED'
    else:
        context=read_context(source_csv)
        basis='R1D104_ARCHIVED_MATERIALIZED_CONTEXT'
    print('R1D105_CONTEXT',len(context),'STOCKS',context.code.nunique(),flush=True)
    # No lookahead anywhere in the event catalog.
    safe=context.drop(columns=[k for k in OUTCOME_FIELDS if k in context.columns],errors='ignore')
    catalog=detect_events(safe)
    refs=[(a,c) for a,_,c,_ in FIXED]
    union=unique_event_dates(catalog,safe,refs)
    # Retain event days regardless of event family; reference dates always present.
    anchored=union.merge(safe[['code','date','name','idx']].drop_duplicates(['code','date']),on=['code','date','idx'],how='left',validate='one_to_one')
    anchored=attach_later_outcomes(anchored,context)
    anchors=episode_anchors(catalog,bar_separation=20)
    episodes=attach_later_outcomes(anchors,context)
    sequences=episode_sequence(catalog,anchors,before=20,after=20)
    # Snapshot source: for beginning dates (idx=541), missing D-20 is explicitly recorded.
    snap_context=safe
    if source_csv is None:
        # R104 exports eligible idx>=541. Recover frozen PIT idx521..540 for
        # complete D-minus20 snapshots at the first eligible dates, without
        # modifying the eligible-event population.
        from research_r1d96 import load as load_raw
        from pre_rise import build_features
        from cloud_geometry import enrich_clouds
        from confluence import integrate_dates
        historic=[]
        for code,name,_,_ in FIXED[:3]:
            # FIXED has no duplicate codes in its first three entries.
            before=integrate_dates(enrich_clouds(build_features(load_raw(code))),
                                   start=521,max_rows=20)
            before.insert(0,'code',code);before.insert(1,'name',name)
            historic.append(before)
        snap_context=pd.concat([*historic,safe],ignore_index=True)
    snap=collect_snapshots(snap_context,anchored)
    expected= len(anchored)*5
    anchor_coverage=(snap.groupby(['code','anchor_date']).relative_bar.nunique().reset_index(name='available_snapshots') if len(snap) else pd.DataFrame())
    cases=complete_case_timeline(context)
    stats=all_family_stats(catalog,context)
    baseline=context[(context.code.eq('039840')&context.date.eq('2024-09-13')) |
                     (context.code.eq('071840')&context.date.eq('2024-03-04')) |
                     (context.code.eq('006400')&context.date.eq('2024-02-07'))].copy()
    if len(baseline)!=3:raise AssertionError('Missing frozen three references')
    # Tests: all R104 detected line crosses MUST appear in new union (zero missed).
    prior_mask=context.trendline_signal.ne('NO_NEW_CROSS_OR_REANCHOR')
    prior_dates=set(zip(context.loc[prior_mask,'code'],context.loc[prior_mask,'date']))
    new_dates=set(zip(catalog.loc[catalog.family.eq('TRENDLINE'),'code'],catalog.loc[catalog.family.eq('TRENDLINE'),'date']))
    missing=prior_dates-new_dates
    if missing:raise AssertionError(f'Forgot {len(missing)} R104 trendline signals: '+str(sorted(missing)[:4]))
    # PIT test: after anchor date, tamper with all future row inputs; earlier events invariant.
    pit=[]
    for code,_,anchor,_ in FIXED[:3]:
        g=safe[safe.code.eq(code)].copy();t=int(g[g.date.eq(anchor)].iloc[0].idx)
        original=detect_events(g); changed=g.copy();mask=changed.idx>t
        for k in ('close','ma5_value','ma20_value','ma60_value','ma112_value','ma224_value',
                  'bb20_width_pct','amount_vs_prev20'):
            changed[k]=changed[k].astype(float)
            changed.loc[mask,k]=changed.loc[mask,k]*1.217
        new=detect_events(changed)
        left=original[original.idx<=t][['event_id']].reset_index(drop=True)
        right=new[new.idx<=t][['event_id']].reset_index(drop=True)
        passed=left.equals(right)
        pit.append({'code':code,'asof':anchor,'PIT_pass':passed,'events_through_asof':len(left)})
        if not passed:raise AssertionError('R1D105 FUTURE LEAK '+code)
    chart_files=[]
    if plots:
        for code,name,date,_ in FIXED:
            chart_files.append(_plot_one(context,code,name,date,output))
    files={
     'R1D105_EVENT_CATALOG_ALL_FAMILIES.csv':catalog,
     'R1D105_UNION_EVENT_STOCK_DATES.csv':anchored,
     'R1D105_20BAR_EPISODE_ANCHORS.csv':episodes,
     'R1D105_EPISODE_EVENT_SEQUENCE_DMINUS20_TO_DPLUS20.csv':sequences,
     'R1D105_ALL_ANCHOR_PRE_SNAPSHOTS.csv':snap,
     'R1D105_CASE_5STOCK_DATE_TIMELINE.csv':cases,
     'R1D105_EVENT_FAMILY_DESCRIPTIVES.csv':stats,
     'R1D105_FIXED_3STOCK_REFERENCE_UNCHANGED.csv':baseline,
     'R1D105_SNAPSHOT_COVERAGE.csv':anchor_coverage,
     'R1D105_PIT_TAMPER.csv':pd.DataFrame(pit)}
    for name,table in files.items():table.to_csv(output/name,index=False,encoding='utf-8-sig')
    (output/'R1D105_5CASE_DETAILED_REPORT_KO.md').write_text(_case_markdown(FIXED,catalog,context),encoding='utf-8')
    audit={'version':'R1D105_UNION_EVENTS_AND_CASE_STUDIES_RESEARCH_ONLY',
      'source':basis,'source_dates':len(context),'case_stock_dates':len(FIXED),'stocks':context.code.nunique(),
      'raw_all_family_events':len(catalog),'union_unique_event_stock_dates':len(union),
      'twenty_bar_episodes':len(episodes),'pre_event_snapshots':len(snap),'event_sequence_rows':len(sequences),
      'missing_snapshot_rows':expected-len(snap),
      'trendline_R104_omission_count':len(missing),
      'family_counts':catalog.family.value_counts().to_dict(),
      'PIT_ALL_PASS':all(x['PIT_pass'] for x in pit),
      'charts':chart_files,
      'limitations':['Three stocks only, not validation of predictive profitability',
        'Episodes and five case studies descriptive, no future labels used in event detection',
        'Overlapping raw event windows: only spaced episode anchors are outcome compared',
        'First 20 eligible dates may lack D-minus20 snapshots in frozen R104 context',
        'Technical events not causal evidence of accumulation or support',
        'KRX unadjusted OHLCV; corporate actions not fully verified',
        'No full market or independent out-of-sample check']}
    (output/'R1D105_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ('source_dates','raw_all_family_events','union_unique_event_stock_dates','twenty_bar_episodes','pre_event_snapshots','event_sequence_rows','missing_snapshot_rows','trendline_R104_omission_count','PIT_ALL_PASS')}),flush=True)
    return audit

if __name__=='__main__':run()
