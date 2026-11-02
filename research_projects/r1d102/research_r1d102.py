"""R1-D102 cloud geometry and unrestricted MA224-zone descriptive research, RESEARCH_ONLY.

Run from Stock-Hunter repository root:
  python research_projects/r1d102/research_r1d102.py
Uses frozen historical input snapshots from R1-D97 and immutable R1-D101 features.
"""
from __future__ import annotations
from pathlib import Path
import sys,hashlib,json
from collections import Counter
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d100'),str(ROOT/'r1d101'),str(HERE)]
from pre_rise import build_features,MA_WINDOWS,future_labels,append_reference_case
from dynamic_structure import structure
from cloud_geometry import enrich_clouds,pattern_state,choose_unrestricted,MA_GROUPS
import research_r1d96 as frozen

CASES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-04'),('006400','삼성SDI','2024-02-07')]
EXPECTED={
 '039840_KRX.csv':'330ffdc5febbbf76f3f959d72181a2a37cc144ec567e6e7f691985b7dc82e3e4',
 '071840_KRX.csv':'aba4ffb8702961fbc8d3d411138b755092551f2fd6ef1a2ac3ce02da2c221c1b',
 '006400_KRX.csv':'d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566',
}
OUT=HERE/'results'


def verify():
    for file,digest in EXPECTED.items():
        path=ROOT/'r1d97'/'data'/file
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=digest:
            raise RuntimeError('frozen input modified or missing: '+str(path))
    for f in [ROOT/'r1d101'/'pre_rise.py',ROOT/'r1d100'/'dynamic_structure.py']:
        if not f.exists():raise FileNotFoundError(f)


def write_csv(file,rows):
    pd.DataFrame(rows).to_csv(OUT/file,index=False,encoding='utf-8-sig')


def draw_reference(d,anchor,code,name):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib import font_manager
    fp=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if fp.exists():
        font_manager.fontManager.addfont(str(fp));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    i=int(d.index[d.date.eq(pd.Timestamp(anchor))][0]);left=max(0,i-90);right=min(len(d)-1,i+20)
    s=d.iloc[left:right+1]
    fig,(ax,bx,cx)=plt.subplots(3,1,figsize=(15,10),sharex=True,gridspec_kw={'height_ratios':[5,1.6,1.8]})
    ax.plot(s.idx,s.close,lw=1.6,color='#111827',label='close')
    # Ribbon clouds distinct from Ichimoku cloud, which is separately filled.
    for name2,color in [('fast','#2b7a78'),('mid','#576cc7'),('long','#bc8a26')]:
        lo=s[f'ma_cloud_{name2}_lower'];hi=s[f'ma_cloud_{name2}_upper']
        ax.fill_between(s.idx.to_numpy(),lo.to_numpy(),hi.to_numpy(),color=color,alpha=.15,label=f'MA {name2} ribbon')
    ax.fill_between(s.idx.to_numpy(),s.ichimoku_current_lower.to_numpy(),s.ichimoku_current_upper.to_numpy(),
                    color='#a855f7',alpha=.12,label='Ichimoku CURRENT displayed cloud')
    ax.axvline(i,ls='--',color='#111827',lw=1.2,label='ASOF historical features')
    ax.axvspan(i,right,color='#facc15',alpha=.07,label='future outcomes only')
    ax.legend(ncol=3,fontsize=8);ax.grid(alpha=.12)
    bx.bar(s.idx,s.amount/1e8,color='#8795a2',alpha=.65)
    bx.set_ylabel('Amount / 100M KRW');bx.grid(alpha=.15)
    cx.plot(s.idx,s.ma_cloud_fast_thickness_pct,label='Fast MA cloud thickness %',color='#2b7a78')
    cx.plot(s.idx,s.ma_cloud_mid_thickness_pct,label='Mid MA cloud thickness %',color='#576cc7')
    cx.plot(s.idx,s.ma_cloud_long_thickness_pct,label='Long MA cloud thickness %',color='#bc8a26')
    cx.plot(s.idx,s.ichimoku_current_thickness_pct,label='Ichimoku thickness %',color='#a855f7')
    cx.plot(s.idx,s.bb20_width_pct,label='BB20 width %',ls=':',color='#1d4ed8')
    cx.legend(ncol=3,fontsize=7);cx.grid(alpha=.15)
    sm=s.iloc[::max(1,len(s)//9)]
    cx.set_xticks(sm.idx);cx.set_xticklabels(sm.date.dt.strftime('%Y-%m-%d'),rotation=30,fontsize=8)
    fig.suptitle(f'{code} {name} | PIT cloud geometry | {anchor} | RESEARCH_ONLY',fontsize=12)
    fig.tight_layout()
    path=OUT/'charts'/f'{code}_{anchor}_CLOUD_REFERENCE.png'
    fig.savefig(path,dpi=130);plt.close(fig)
    return path.name


def snapshots_including_cloud_states(features, cases, back=(-20,-10,-5,-1,0)):
    """R1-D102 equivalent of immutable R1-D101 snapshots, supports categorical cloud states."""
    rows=[]
    for c in cases.itertuples():
        for off in back:
            t=int(c.idx)+off
            if t<0: continue
            x=features.iloc[t]
            r={'label':c.label,'anchor_date':c.asof,'relative_bar':off,
               'observation_date':str(x.date.date()),'idx':int(x.idx),'close':float(x.close)}
            for key in features.columns:
                if key in ('date','idx','code','open','high','low','close','volume','amount'):continue
                v=x[key]
                if isinstance(v,(str,)):r[key]=v
                elif pd.isna(v):r[key]=None
                elif isinstance(v,(np.bool_,bool)):r[key]=bool(v)
                elif isinstance(v,(int,float,np.integer,np.floating)):r[key]=float(v)
                else:raise TypeError('unsupported PIT feature type: '+key+' / '+type(v).__name__)
            rows.append(r)
    return pd.DataFrame(rows)


def transition_report(snap: pd.DataFrame, cases: pd.DataFrame) -> pd.DataFrame:
    """Classify pre-event feature TRAJECTORIES, never use the future outcome in features."""
    numeric=(
      'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct','ma_cloud_long_thickness_pct',
      'ma_cloud_fast_height_vs_price_pct','ma_cloud_mid_height_vs_price_pct','ma_cloud_long_height_vs_price_pct',
      'ichimoku_current_thickness_pct','ichimoku_current_height_vs_price_pct',
      'bb20_width_pct','bb40_width_pct','env20_10_width_abs','env40_10_width_abs',
      'ma20_slope20_pct','ma112_slope20_pct','ma224_slope20_pct','ma448_slope20_pct',
      'ma20_slope20_acceleration5_pp','ma112_slope20_acceleration5_pp','ma224_slope20_acceleration5_pp',
      'amount_vs_prev20','volume_vs_prev20',
    )
    def shape(x0,xmid,xend):
        if pd.isna(x0) or pd.isna(xmid) or pd.isna(xend):return 'NA'
        if xmid<x0 and xend>xmid:return 'CONTRACT_THEN_EXPAND'
        if xmid>x0 and xend<xmid:return 'EXPAND_THEN_CONTRACT'
        if xend>xmid>x0:return 'CONTINUOUS_RISE'
        if xend<xmid<x0:return 'CONTINUOUS_FALL'
        return 'MIXED_OR_FLAT'
    ans=[]
    for c in cases.itertuples():
        z=snap[snap.code.eq(c.code)&snap.anchor_date.eq(c.asof)].set_index('relative_bar')
        if not {-20,-10,0}.issubset(set(z.index)):continue
        row={'code':c.code,'name':c.name,'asof':c.asof,'label':c.label,'ma224_gap_at_d0_pct':z.loc[0,'ma224_price_gap_pct'],
             'observed_ma224_zone':'BELOW' if z.loc[0,'close'] < z.loc[0,'ma224'] else 'ABOVE'}
        for k in numeric:
            if k not in z.columns:continue
            a,b,e=(z.loc[i,k] for i in (-20,-10,0))
            row[k+'_Dminus20']=a;row[k+'_Dminus10']=b;row[k+'_D0']=e
            row[k+'_Dminus20_to_D0_change']=(e-a if pd.notna(a) and pd.notna(e) else None)
            # Shapes describe numeric movement, not proof of an entry signal.
            row[k+'_pre_event_shape']=shape(a,b,e)
        ans.append(row)
    return pd.DataFrame(ans)


def run():
    verify();OUT.mkdir(parents=True,exist_ok=True);(OUT/'charts').mkdir(exist_ok=True)
    summary=[];population=[];selected=[];snapshots=[];states=[];patterns=[];pit=[];charts=[];counts=[]
    for code,name,reference in CASES:
        print('CASE_START',code,name,flush=True)
        raw=frozen.load(code)
        features=enrich_clouds(build_features(raw))
        labels=future_labels(features)
        eligible,cases=choose_unrestricted(features,labels,min_idx=max(MA_WINDOWS)+61,max_per_group=8,cooldown=20)
        for c in eligible.itertuples():
            population.append({'code':code,'name':name,'idx':int(c.idx),'date':str(c.date.date()),
               'ma224_zone':c.ma224_zone,'label':c.outcome,'future20_peak_pct':float(c.forward20_peak_pct),
               'future20_close_pct':float(c.forward20_close_pct)})
        cases=append_reference_case(cases,features,labels,reference)
        cases['code']=code;cases['name']=name
        selected.extend(cases.to_dict('records'))
        snap=snapshots_including_cloud_states(features,cases)
        snap.insert(0,'code',code);snap.insert(1,'name',name)
        snapshots.extend(snap.to_dict('records'))
        for q in cases.itertuples():
            index=int(q.idx)
            row=features.iloc[index]
            label=q.label
            pat=pattern_state(row)
            asof_structure=structure(raw,q.asof)['structure']
            after_dt=str(features.iloc[index+20].date.date())
            after_structure=structure(raw,after_dt)['structure']
            states.append({'code':code,'name':name,'asof':q.asof,'label':label,
                           'ma224_zone':pat['ma224_zone'],'asof_structure':asof_structure,
                           'future20_structure_EXPOST_ONLY':after_structure,
                           'future20_close_pct_EXPOST_ONLY':float(q.forward20_close_pct),**pat})
            print('CASE',code,q.asof,label,'zone',pat['ma224_zone'],
                  'fast-cloud width',round(float(row.ma_cloud_fast_thickness_pct),2),
                  'long-cloud width',round(float(row.ma_cloud_long_thickness_pct),2),
                  'ichimoku position',str(row.ichimoku_current_position),
                  'MA20 slope20 accel5',round(float(row.ma20_slope20_acceleration5_pp),2),
                  'future20',round(float(q.forward20_close_pct),2),flush=True)
        # Strong PIT: future OHLC values changed, all features (including current/projected clouds) at t unchanged.
        test_i=int(features.index[features.date.eq(pd.Timestamp(reference))][0])
        alt=raw.copy()
        for field in ('open','high','low','close'):
            alt[field]=alt[field].astype(float)
            alt.loc[alt.index>test_i,field]*=1.217
        changed=enrich_clouds(build_features(alt))
        pd.testing.assert_series_equal(features.iloc[test_i],changed.iloc[test_i],check_names=False)
        pit.append({'code':code,'asof':reference,'pass':True})
        tab=eligible.groupby(['ma224_zone','outcome'],dropna=False).size().to_dict()
        for zone in ['BELOW','ABOVE']:
            counts.append({'code':code,'name':name,'ma224_zone':zone,'population':int(sum(v for (z,_),v in tab.items() if z==zone)),
                           'surge':int(tab.get((zone,'SURGE'),0)),
                           'control':int(tab.get((zone,'CONTROL'),0)),
                           'other':int(tab.get((zone,'OTHER'),0))})
        charts.append(draw_reference(features,reference,code,name))
        summary.append({'code':code,'name':name,'eligible_dates_all_zones':len(eligible),
                        'samples':len(cases),'pit_pass':True})
        print('CASE_DONE',code,'eligible',len(eligible),'sampled',len(cases),'PIT_PASS',flush=True)
    stateframe=pd.DataFrame(states)
    # No outcomes used to *create* states. Tabulate separately AFTER labels.
    for key in ('fast_cloud_thickness_trend','mid_cloud_thickness_trend',
                'long_cloud_thickness_trend','fast_cloud_height_trend','ma20_slope_accel',
                'ma112_slope_accel','ma224_slope_accel','bb20_width_trend','amount_vs_prev20_zone',
                'ichimoku_current_position','ichimoku_projected_bullish','asof_structure'):
        for (zone,label,val),g in stateframe[stateframe.label.ne('REFERENCE')].groupby(['ma224_zone','label',key],dropna=False):
            patterns.append({'feature':key,'ma224_zone':zone,'outcome':label,'feature_value':val,
                             'n_cases':len(g),'denominator':int(((stateframe.label==label)&(stateframe.ma224_zone==zone)).sum()),
                             'research_only':True})
    trajectory=transition_report(pd.DataFrame(snapshots),pd.DataFrame(selected))
    for feature in ('ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct',
                    'ma_cloud_long_thickness_pct','bb20_width_pct','ma112_slope20_pct',
                    'amount_vs_prev20'):
        key=feature+'_pre_event_shape'
        if key not in trajectory.columns:continue
        for (zone,label,value),g in trajectory[trajectory.label.ne('REFERENCE')].groupby(['observed_ma224_zone','label',key],dropna=False):
            patterns.append({'feature':key,'ma224_zone':zone,'outcome':label,'feature_value':value,
                             'n_cases':len(g),'denominator':int(((trajectory.label==label)&(trajectory.observed_ma224_zone==zone)).sum()),
                             'research_only':True})
    write_csv('R1D102_PRE_EVENT_PATTERN_TRANSITIONS.csv',trajectory.to_dict('records'))
    write_csv('R1D102_ALL_ELIGIBLE_ABOVE_BELOW_MA224.csv',population)
    write_csv('R1D102_DEOVERLAPPED_CASES_PLUS_REFERENCES.csv',selected)
    write_csv('R1D102_PRE_EVENT_SNAPSHOTS_ALL_CLOUD_MA_BANDS_VOLUME.csv',snapshots)
    write_csv('R1D102_CASE_PATTERN_STATES_EXPOST_SEPARATED.csv',states)
    write_csv('R1D102_PATTERN_COUNTS_DESCRIPTIVE_NOT_PREDICTIVE.csv',patterns)
    write_csv('R1D102_MA224_STRATA_ALL_ELIGIBLE_COUNTS.csv',counts)
    write_csv('R1D102_PIT_TAMPER.csv',pit)
    audit={'version':'R1-D102_PIT_MULTI_CLOUD_PATTERN_RESEARCH_ONLY','cases':summary,
      'candidate_population_count':len(population),'sample_count':len(selected),
      'reference_count':len(CASES),'snapshot_count':len(snapshots),'transition_count':len(trajectory),'ma_windows':list(MA_WINDOWS),
      'ma_ribbon_groups':{k:list(v) for k,v in MA_GROUPS.items()},
      'ichimoku_settings':{'tenkan':9,'kijun':26,'span_b':52,'displacement':26},
      'current_ichimoku_display_uses_shifted_historical_inputs':True,
      'future_ichimoku_plot_not_future_prediction':True,
      'under_ma224_not_selection_gate':True,'ma224_zone_stratification':True,
      'pit_all_pass':all(z['pass'] for z in pit),'chart_names':charts,
      'source_input_verified':True,'research_only':True,'production_modified':False,
      'limitations':['3-stock historical case study, not cross-market validation',
        'Future performance is a LABEL for EX-POST research only, never a PIT feature',
        'Sustained surge threshold is an arbitrary research label; not a trade entry signal',
        'MA clouds are ranges of selected windows, NOT Ichimoku clouds',
        'Ichimoku current displayed values use 26-bar shift, projected values known today can be plotted forward but are NOT future price data',
        'Corporate actions/adjustment are not fully audited',
        'Cases chosen chronologically with 20-bar cooldown across BOTH outcomes; patterns descriptive only',
        'Below MA224 is not a valuation metric; intrinsic value not assessed',
        'No out-of-sample or market regime/industry matched control verification']}
    (OUT/'R1D102_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
    print('SUMMARY',json.dumps({'eligible':len(population),'cases':len(selected),'snapshots':len(snapshots),
                                'patterns':len(patterns),'transitions':len(trajectory),'PIT':audit['pit_all_pass']},ensure_ascii=False),flush=True)
if __name__=='__main__':run()
