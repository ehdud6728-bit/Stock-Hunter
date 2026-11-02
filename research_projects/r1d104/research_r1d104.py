"""R1-D104 PIT integrated trendline+MA+BB+ENV+cloud+volume research, no LIVE change."""
from __future__ import annotations
import hashlib,json,sys
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d100'),str(ROOT/'r1d101'),
              str(ROOT/'r1d102'),str(ROOT/'r1d103'),str(HERE)]
from pre_rise import build_features,future_labels
from cloud_geometry import enrich_clouds
from dynamic_structure import structure
from ma_roles import inventory_events,attach_outcomes,cooldown_events
from confluence import integrate_dates,attach_expost,events_for_dates,compare_groups

CASES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-04'),('006400','삼성SDI','2024-02-07')]
EXPECTED={'039840_KRX.csv':'330ffdc5febbbf76f3f959d72181a2a37cc144ec567e6e7f691985b7dc82e3e4',
          '071840_KRX.csv':'aba4ffb8702961fbc8d3d411138b755092551f2fd6ef1a2ac3ce02da2c221c1b',
          '006400_KRX.csv':'d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566'}


def _verify():
    for filename,expected in EXPECTED.items():
        fp=ROOT/'r1d97'/'data'/filename
        if not fp.exists() or hashlib.sha256(fp.read_bytes()).hexdigest()!=expected:
            raise AssertionError('Frozen source hash invalid: '+filename)

def _plot_case(features,raw,ref,code,name,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.exists():font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    idx=int(features.index[features.date.eq(pd.Timestamp(ref))][0]);left=max(0,idx-90);right=min(len(features)-1,idx+20)
    d=features.iloc[left:right+1]
    fig,(ax,bx,cx)=plt.subplots(3,1,figsize=(15,11),sharex=True,gridspec_kw={'height_ratios':[5,1.3,1.5]})
    ax.plot(d.idx,d.close,color='black',lw=1.4,label='Close')
    for n,col in [(5,'#e76f51'),(20,'#228be6'),(60,'#16a34a'),(112,'#8b5cf6'),(224,'#64748b')]:
        ax.plot(d.idx,d[f'ma{n}'],label=f'MA{n}',lw=1.2,color=col,alpha=.85)
    ax.plot(d.idx,d.bb20_upper,label='BB20 upper',color='#f59e0b',lw=1.2)
    ax.plot(d.idx,d.bb20_lower,label='BB20 lower',color='#f59e0b',lw=1.2,ls='--')
    ax.plot(d.idx,d.env20_10_upper,label='ENV20 +10%',color='#db2777',lw=1)
    ax.plot(d.idx,d.env20_10_lower,label='ENV20 -10%',color='#db2777',lw=1,ls='--')
    snap=structure(raw,ref)
    for line in snap['lines']:
        if line['tier']!='SHORT':continue
        beg=max(left,int(line['h1_idx']));end=idx
        x=np.arange(beg,end+1,dtype=float)
        y=float(line['h1_value'])+(float(line['h2_value'])-float(line['h1_value']))*(x-float(line['h1_idx']))/(float(line['h2_idx'])-float(line['h1_idx']))
        ax.plot(x,y,lw=1.7,ls=':',color='red' if line['kind']=='HIGH' else '#0d9488',label=f'{line["kind"]} short pivot')
    ax.axvline(idx,color='black',ls='--',lw=1);ax.axvspan(idx,right,color='gold',alpha=.08)
    ax.grid(alpha=.15);ax.legend(ncol=4,loc='upper left',fontsize=8)
    bx.plot(d.idx,d.bb20_width_pct,label='BB20 width',color='#f59e0b')
    bx.plot(d.idx,d.ma_cloud_fast_thickness_pct,label='Fast cloud thickness',color='#16a34a')
    bx.plot(d.idx,d.ma_cloud_mid_thickness_pct,label='Mid cloud thickness',color='#6366f1')
    bx.grid(alpha=.15);bx.legend(ncol=3,fontsize=8)
    cx.bar(d.idx,d.amount/1e8,label='Amount (100M KRW)',color='#94a3b8')
    cx.grid(alpha=.15);cx.legend(fontsize=8)
    xs=d.iloc[::max(1,len(d)//8)];cx.set_xticks(xs.idx);cx.set_xticklabels(xs.date.dt.strftime('%Y-%m-%d'),rotation=30,fontsize=8)
    fig.suptitle(f'{name}({code}) {ref} | trend + MA + BB + ENV + amount | RESEARCH_ONLY')
    fig.tight_layout(); dest=out/'charts'/f'{code}_{ref}_INTEGRATED.png';fig.savefig(dest,dpi=120);plt.close(fig)
    return dest.name


def run(*,input_override=None,out=None,plot=True,max_rows=None):
    if input_override is None:_verify()
    if input_override is None:
        from research_r1d96 import load as load_raw
    else: load_raw=input_override
    output=Path(out) if out else HERE/'results';output.mkdir(parents=True,exist_ok=True);(output/'charts').mkdir(exist_ok=True)
    combined=[];events_all=[];unique_all=[];pre_event_rows=[];pit=[];references=[];charts=[];case_rows=[]
    for code,name,ref in CASES:
        print('CASE_START',code,name,flush=True)
        raw=load_raw(code)
        features=enrich_clouds(build_features(raw))
        labels=future_labels(features)
        # Extra 20 PIT lookback rows, excluded from outcome-eligible population.
        timeline=integrate_dates(features,start=521,max_rows=(None if max_rows is None else max_rows+20))
        contexts=timeline[timeline.idx>=541].reset_index(drop=True)
        outcomes=attach_expost(contexts,labels)
        inv=inventory_events(features)
        # chronological sampling independent of outcome attributes
        selected=cooldown_events(attach_outcomes(inv,labels,features))
        # Align with R1-D103 complete-history baseline (index >=541).
        selected=selected[selected.idx>=541].copy() if len(selected) else selected
        by_day=events_for_dates(outcomes,selected[['idx','ma_period','event']] if len(selected) else selected)
        matched=by_day[by_day.ma_event_count>0].copy()
        before_lookup={int(v['idx']):v for v in timeline.to_dict('records')}
        snapshot_fields=('structure','short_high_trend_direction','short_low_trend_direction',
                         'short_high_trend_validated_cross','short_low_trend_validated_cross',
                         'ma5_slope20_pct','ma20_slope20_pct','ma20_slope_accel5_pp',
                         'ma60_slope20_pct','ma112_slope20_pct','ma224_slope20_pct',
                         'bb20_position','bb20_width_pct','bb20_width_delta5_pp',
                         'bb30_width_pct','bb40_width_pct','env20_10_position',
                         'env20_10_upper_distance_pct','env20_10_lower_distance_pct',
                         'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct',
                         'ichimoku_current_thickness_pct','volume_vs_prev20','amount_vs_prev20')
        for anchor in matched.to_dict('records'):
            for offset in (-20,-10,-5,-1,0):
                record=before_lookup.get(int(anchor['idx'])+offset)
                if record is None:raise AssertionError('Missing PIT pre-event snapshot')
                pre_event_rows.append({'code':code,'name':name,
                    'anchor_date':anchor['date'],'observation_date':record['date'],
                    'relative_bar':offset,'ma_event_types':anchor['ma_event_types'],
                    'ma_periods':anchor['ma_periods'],
                    'outcome_at_anchor_EXPOST':anchor['outcome_EXPOST'],
                    'forward20_close_at_anchor_EXPOST':anchor['forward20_close_pct_EXPOST'],
                    **{k:record[k] for k in snapshot_fields}})
        for frame in (outcomes,matched):
            frame.insert(0,'code',code);frame.insert(1,'name',name)
        combined.append(outcomes);unique_all.append(by_day.assign(code=code,name=name))
        if not matched.empty:events_all.append(matched)
        if input_override is None:
            fixed=by_day[by_day.date.eq(ref)]
            if len(fixed)!=1:raise AssertionError('Missing fixed 3-stock reference: '+code)
            references.extend(fixed.to_dict('records'))
            if plot:charts.append(_plot_case(features,raw,ref,code,name,output))
            audit_t=int(features.index[features.date.eq(pd.Timestamp(ref))][0])
        else:audit_t=max(540,len(features)-70)
        # PIT integrity: modify ALL future OHLCV, compare the entire unified
        # feature row and every line id, not only selected feature columns.
        alternate=raw.copy()
        for field in ('open','high','low','close'):
            alternate[field]=alternate[field].astype(float)
            alternate.loc[alternate.index>audit_t,field]*=1.217
        for field in ('volume','amount'):
            alternate[field]=alternate[field].astype(float)
            alternate.loc[alternate.index>audit_t,field]*=1.333
        altered_features=enrich_clouds(build_features(alternate))
        # Compute only one ASOF context; all full frames are generated historically
        date=str(features.iloc[audit_t].date.date())
        old_date=str(features.iloc[audit_t-1].date.date())
        from confluence import _as_row
        original_row=_as_row(features,audit_t,structure(raw,date),old_snapshot=structure(raw,old_date))
        changed_row=_as_row(altered_features,audit_t,structure(alternate,date),old_snapshot=structure(alternate,old_date))
        if original_row!=changed_row:raise AssertionError('PIT future contamination '+code)
        pit.append({'code':code,'asof':date,'pass':True,'full_context_equal':True})
        case_rows.append({'code':code,'name':name,'eligible_dates':len(outcomes),'raw_ma_events':len(inv),
                          'sample_ma_events':len(selected),'unique_sampled_stock_dates':len(matched),
                          'pit_pass':True})
        print('CASE_DONE',code,'eligible',len(outcomes),'MA sample events',len(selected),
              'unique dates',len(matched),'PIT_PASS',flush=True)
    all_dates=pd.concat(combined,ignore_index=True)
    unique_dates=pd.concat(unique_all,ignore_index=True)
    event_dates=pd.concat(events_all,ignore_index=True) if events_all else unique_dates.head(0)
    grouped=compare_groups(all_dates)
    grouped_event=compare_groups(event_dates,keys=(
        'structure','ma_event_category','trendline_signal',
        'bb20_width_state','env20_10_position','flow_today'))
    # No outcome-aware selection; just a chronological fixed sample of candidate charts.
    cols=['code','name','date','ma_event_count','ma_periods','ma_event_types','structure',
          'short_high_trend_direction','short_low_trend_direction',
          'short_high_trend_validated_cross','short_low_trend_validated_cross',
          'ma20_slope20_pct','ma20_slope_accel5_pp','ma60_slope20_pct','ma112_slope20_pct','ma224_slope20_pct',
          'bb20_position','bb20_width_pct','bb20_width_delta5_pp','bb30_width_pct','bb40_width_pct',
          'env20_10_position','env20_10_upper','env20_10_lower','env40_10_position',
          'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct','ichimoku_current_thickness_pct',
          'volume_vs_prev20','amount_vs_prev20','outcome_EXPOST','forward20_close_pct_EXPOST']
    slim=unique_dates[cols].copy()
    for fname,df in [('R1D104_ALL_ELIGIBLE_CONFLUENCE.csv',all_dates),
                     ('R1D104_UNIQUE_STOCK_DATE_MA_EVENTS.csv',unique_dates),
                     ('R1D104_EVENT_ONLY_UNIQUE_DATES.csv',event_dates),
                     ('R1D104_PRE_EVENT_DMINUS20_TO_D0.csv',pre_event_rows),
                     ('R1D104_INTEGRATED_CASE_BRIEF.csv',slim),
                     ('R1D104_CONFLUENCE_ALL_BASELINE.csv',grouped),
                     ('R1D104_CONFLUENCE_EVENT_COMPARE.csv',grouped_event),
                     ('R1D104_FIXED_3STOCK_REFERENCE.csv',references),
                     ('R1D104_CASE_COUNTS.csv',case_rows),
                     ('R1D104_PIT_TAMPER.csv',pit)]:
        pd.DataFrame(df).to_csv(output/fname,index=False,encoding='utf-8-sig')
    audit={'version':'R1D104_INTEGRATED_CONFLUENCE_RESEARCH_ONLY',
           'frozen_r1d96_to_r1d103_untouched':True,'LIVE_unchanged':True,'ma224_gate':False,
           'cases':len(CASES),'dates':len(all_dates),'sample_event_stock_dates':len(event_dates),'pre_event_snapshots':len(pre_event_rows),
           'same_day_ma_counted_once':True,'PIT_all_pass':all(x['pass'] for x in pit),
           'bb_windows':[20,30,40],'env_windows':[20,40], 'MA_windows':[3,5,10,20,30,40,60,112,120,224,240,448,480],
           'charts':charts,'limitations':[
              'Three historical stocks only; NOT full market / independent OOS / profitability proof',
              'No cherry-picked winners; labels added after PIT features and event detection',
              'Overlapping 20-day future windows are dependent; n is stock-dates not independent trades',
              'No score weighting or optimized trading thresholds',
              'Trend line touches are price geometry, not causal support proof',
              'ENV +/-10 percent relative width constant; absolute width reflects center MA',
              'Unadjusted historical KRX series, corporate actions not fully audited']}
    (output/'R1D104_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ('cases','dates','sample_event_stock_dates','PIT_all_pass')},ensure_ascii=False),flush=True)
    return audit

if __name__=='__main__':run()
