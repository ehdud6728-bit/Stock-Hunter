"""R1-D101 complete moving-average / band / volume PIT pre-rise study.

RESEARCH_ONLY. Reads frozen R1-D97 KRX inputs. Does not modify R1-D96..D100
or a LIVE strategy. Outcome labels are created AFTER feature construction.
"""
from __future__ import annotations
import hashlib
import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d100'),str(HERE)]
from pre_rise import MA_WINDOWS,build_features,validate,future_labels,choose_cases,event_snapshots,descriptive_compare,append_reference_case
from dynamic_structure import structure
import research_r1d96 as frozen

CASES=[('039840','디오'),('071840','롯데하이마트'),('006400','삼성SDI')]
REFERENCE_DATES={'039840':'2024-09-13','071840':'2024-03-04','006400':'2024-02-07'}
EXPECTED={
 '039840_KRX.csv':'330ffdc5febbbf76f3f959d72181a2a37cc144ec567e6e7f691985b7dc82e3e4',
 '071840_KRX.csv':'aba4ffb8702961fbc8d3d411138b755092551f2fd6ef1a2ac3ce02da2c221c1b',
 '006400_KRX.csv':'d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566',
}
OUT=HERE/'results'
BASE=ROOT/'r1d97'/'data'


def verify():
    for file,expected in EXPECTED.items():
        p=BASE/file
        if not p.exists() or hashlib.sha256(p.read_bytes()).hexdigest()!=expected:
            raise RuntimeError('Missing/frozen input SHA256 mismatch: '+str(p))
    if not (ROOT/'r1d100'/'dynamic_structure.py').exists():
        raise RuntimeError('Missing R1-D100 frozen structure source')


def write(name,items):
    pd.DataFrame(items).to_csv(OUT/name,index=False,encoding='utf-8-sig')


def draw_case(features, anchor, code, name, label):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib import font_manager
    f=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if f.exists():
        font_manager.fontManager.addfont(str(f))
        plt.rcParams['font.family']='NanumGothic'
        plt.rcParams['axes.unicode_minus']=False
    t=int(anchor)
    left=max(0,t-65);right=min(len(features)-1,t+20)
    s=features.iloc[left:right+1];end=features.iloc[t]
    fig,(ax,bx,cx)=plt.subplots(3,1,figsize=(14,9),sharex=True,gridspec_kw={'height_ratios':[5,1.6,1.5]})
    ax.plot(s.idx,s.close,lw=1.6,color='#101827',label='close')
    for n,color in [(5,'#d97706'),(20,'#15803d'),(60,'#0d9488'),(112,'#2563eb'),(224,'#7c3aed'),(448,'#db2777')]:
        ax.plot(s.idx,s[f'ma{n}'],lw=1.0,alpha=.8,color=color,label=f'MA{n}')
    for p,color in [('bb20','#3f83c5'),('env20_10','#ae8b32')]:
        ax.plot(s.idx,s[f'{p}_upper'],ls='--',lw=.95,color=color,label=f'{p} upper')
        ax.plot(s.idx,s[f'{p}_lower'],ls=':',lw=.95,color=color,label=f'{p} lower')
    # Both upper and lower lines selected as-of only; later line extension is illustrative.
    lines=structure(features,str(end.date.date()))['lines']
    for line in lines:
        i,j=int(line['h1_idx']),int(line['h2_idx'])
        if j<=i:continue
        xs=np.arange(max(left,int(line['h2_confirm_idx'])),right+1)
        ys=float(line['h1_value'])+(float(line['h2_value'])-float(line['h1_value']))*(xs-i)/(j-i)
        ax.plot(xs,ys,lw=1.25,alpha=.7,ls='-.' if line['kind']=='HIGH' else ':',
                color='#b91c1c' if line['kind']=='HIGH' else '#047857',
                label=f"{line['kind']} {line['tier']} asof line")
    ax.axvline(t,color='#111827',ls='--',lw=1.1,label='ASOF (end of input features)')
    ax.axvspan(t,right,alpha=.07,color='#eab308',label='future OUTCOME only')
    ax.legend(ncol=4,fontsize=7,loc='upper left');ax.grid(alpha=.15)
    bx.bar(s.idx,s.amount/1e8,color='#6b7280',alpha=.7,width=.7)
    bx.set_ylabel('amount / 100M KRW');bx.grid(alpha=.12)
    cx.plot(s.idx,s.bb20_width_pct,label='BB20 width %',color='#2563eb')
    cx.plot(s.idx,s.bb40_width_pct,label='BB40 width %',color='#0891b2',ls='--')
    cx.plot(s.idx,s.amount_vs_prev20,label='amount / previous 20D',color='#b45309')
    cx.legend(loc='upper left',fontsize=8);cx.grid(alpha=.12)
    xt=s.iloc[::max(1,len(s)//10)];cx.set_xticks(xt.idx);cx.set_xticklabels(xt.date.dt.strftime('%Y-%m-%d'),rotation=35,fontsize=8)
    fig.suptitle(f'{code} {name} | {label} | {end.date.date()} | R1D101 PIT PRE-RISE',fontsize=12)
    fig.tight_layout()
    fp=OUT/'charts'/f'{code}_{end.date.date()}_{label}.png';fig.savefig(fp,dpi=120);plt.close(fig)
    return fp.name


def run():
    verify();OUT.mkdir(parents=True,exist_ok=True);(OUT/'charts').mkdir(exist_ok=True)
    summary=[];all_cases=[];all_snap=[];all_lines=[];all_descriptive=[];all_candidates=[];trend_outcomes=[];pit=[];charts=[]
    for code,name in CASES:
        print('CASE_START',code,name,flush=True)
        # Input is frozen historical KRX raw OHLCV and amount.
        raw=frozen.load(code)
        features=build_features(raw)
        labels=future_labels(features)
        # Entire eligible population is preserved, BEFORE de-overlap and cohort selection.
        eligible=(features['idx']>=max(MA_WINDOWS)+61)&features['ma224'].notna()&(features.close<features.ma224)&labels.forward20_peak_pct.notna()
        population=labels.loc[eligible,['idx','date','forward20_peak_pct','forward20_close_pct','is_sustained_surge','is_nonrise_control']]
        for q in population.itertuples():
            all_candidates.append({'code':code,'name':name,'idx':int(q.idx),'date':str(q.date.date()),
                                   'future_peak_pct':q.forward20_peak_pct,'future_d20_pct':q.forward20_close_pct,
                                   'surge_label':bool(q.is_sustained_surge),'control_label':bool(q.is_nonrise_control)})
        cases=choose_cases(features,labels,study_start=max(MA_WINDOWS)+61,max_cases=8)
        if code in REFERENCE_DATES:
            cases=append_reference_case(cases,features,labels,REFERENCE_DATES[code])
        # PIT audit for EVERY stock, even if no future-surge cases were sampled.
        t=int(cases.iloc[0].idx) if not cases.empty else min(len(features)-21,max(MA_WINDOWS)+61)
        altered=raw.copy()
        for col in ('open','high','low','close'):
            altered[col]=altered[col].astype(float)
            altered.loc[altered.index>t,col]*=1.217
        newf=build_features(altered)
        pd.testing.assert_series_equal(features.iloc[t],newf.iloc[t],check_names=False)
        pit.append({'code':code,'asof':str(features.iloc[t].date.date()),'future_tamper_pass':True})
        if cases.empty:
            summary.append({'code':code,'name':name,'eligible_count':len(population),
                            'raw_surge_count':int(population.is_sustained_surge.sum()),
                            'raw_control_count':int(population.is_nonrise_control.sum()),
                            'sampled_surge':0,'sampled_control':0,'pit':True})
            print('CASE_DONE',code,'no_sampled_cases',flush=True)
            continue
        cases['code']=code;cases['name']=name
        all_cases.extend(cases.to_dict('records'))
        snaps=event_snapshots(features,cases)
        snaps.insert(0,'code',code);snaps.insert(1,'name',name)
        all_snap.extend(snaps.to_dict('records'))
        for c in cases.itertuples():
            # EX-POST diagnostic, never joined back into an as-of feature.
            future_t=int(c.idx)+20
            before=structure(raw,c.asof)['structure']
            after=structure(raw,str(features.iloc[future_t].date.date()))['structure']
            trend_outcomes.append({'code':code,'name':name,'asof':c.asof,'label':c.label,
                                   'asof_structure':before,'future20_structure':after,
                                   'future20_date':str(features.iloc[future_t].date.date()),
                                   'future20_hh_hl':after=='HIGHER_HIGHS_HIGHER_LOWS',
                                   'future20_return_pct':float(c.forward20_close_pct)})
            observed=features.iloc[int(c.idx)]
            print('OBSERVED_COHORT',code,c.asof,c.label,
                  'MA112_slope20_pct',round(float(observed.ma112_slope20_pct),2),
                  'MA224_price_gap_pct',round(float(observed.ma224_price_gap_pct),2),
                  'BB20_width_pct',round(float(observed.bb20_width_pct),2),
                  'amount_vs_prev20',round(float(observed.amount_vs_prev20),2),
                  'pre_structure',before,'outcome20_structure',after,
                  'FUTURE20_close_pct',round(float(c.forward20_close_pct),2),flush=True)
            for off in (-20,-10,-5,-1,0):
                k=int(c.idx)+off;dt=str(features.iloc[k].date.date())
                info=structure(raw,dt)
                for ln in info['lines']:
                    all_lines.append({'code':code,'name':name,'case_asof':c.asof,'label':c.label,
                                      'relative_bar':off,'observation_date':dt,'structure':info['structure'],
                                      'kind':ln['kind'],'tier':ln['tier'],'h1':ln['h1'],'h2':ln['h2'],
                                      'h2_confirm_date':ln['h2_confirm_date'],'line_id':ln['line_id'],
                                      'slope_pct_per_day':ln['slope_pct_per_day'],
                                      'direction':ln['direction'],'line_asof':ln['line_asof'],
                                      'distance_close_to_line_pct':ln['distance_close_to_line_pct']})
        comparisons=descriptive_compare(cases,snaps)
        if not comparisons.empty:
            comparisons['code']=code;comparisons['name']=name
            all_descriptive.extend(comparisons.to_dict('records'))
        for label in ('SURGE','CONTROL','REFERENCE'):
            sub=cases[cases.label==label]
            if len(sub):charts.append(draw_case(features,sub.iloc[0].idx,code,name,label))
        summary.append({'code':code,'name':name,'eligible_count':len(population),
                        'raw_surge_count':int(population.is_sustained_surge.sum()),
                        'raw_control_count':int(population.is_nonrise_control.sum()),
                        'sampled_surge':int((cases.label=='SURGE').sum()),
                        'sampled_control':int((cases.label=='CONTROL').sum()),
                        'pit':True})
        print('CASE_DONE',code,'raw_eligible',len(population),'sampled_surge',int((cases.label=='SURGE').sum()),
              'sampled_control',int((cases.label=='CONTROL').sum()),flush=True)
    write('R1D101_ALL_ELIGIBLE_OUTCOMES.csv',all_candidates)
    write('R1D101_DEOVERLAPPED_CASES.csv',all_cases)
    write('R1D101_PRE_RISE_ALL_MA_BB_ENV_VOLUME_SNAPSHOTS.csv',all_snap)
    write('R1D101_DYNAMIC_TRENDLINE_AT_SNAPSHOTS.csv',all_lines)
    write('R1D101_PER_STOCK_DESCRIPTIVE_COMPARE_NOT_PREDICTIVE.csv',all_descriptive)
    write('R1D101_POSTEVENT_TREND_TRANSITIONS_EXPOST_ONLY.csv',trend_outcomes)
    write('R1D101_CASE_SUMMARY.csv',summary)
    write('R1D101_PIT_TAMPER.csv',pit)
    audit={'version':'R1-D101_RESEARCH_ONLY','cases':summary,
           'feature_periods':list(MA_WINDOWS),'feature_mas_per_stock':len(MA_WINDOWS),
           'all_ma_pairs':len(MA_WINDOWS)*(len(MA_WINDOWS)-1)//2,
           'bollinger_windows':[20,30,40], 'envelope_windows':[20,40],
           'outcome_definition':'future20 max high >= +10% AND D+20 close >= +5%; label is OUTCOME only',
           'control_definition':'future20 max high <= +5% AND D+20 close <= 0%; label is OUTCOME only',
           'candidate_population':'close below PIT MA224, min history and full D+20',
           'sample_policy':'chronological, 20-bar nonoverlap within label, up to 8 per stock/label; add frozen R1D100 reference date separately',
           'point_in_time_feature_separation':True,'all_pit_pass':len(pit)==len(CASES) and all(z['future_tamper_pass'] for z in pit),
           'charts':charts,'research_only':True,'live_changes':False,
           'limitations':['NOT OOS, not proof of predictive power','Only 3 frozen stocks, not representative',
           'PREDECLARED REFERENCE dates not included in outcome cohort medians',
           'Surge labels overlap in raw population; only case shortlist is de-overlapped within label',
           'CONTROL is not propensity matched for industry/regime; descriptive only',
           'All raw prices; corporate-action corrections not certified',
           'Maximum selected cohorts are arbitrary research definitions; run sensitivity and nonchosen controls',
           'MA all = all configured MA windows, not infinite mathematical MA periods']}
    (OUT/'R1D101_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
    print('SUMMARY',json.dumps({'eligible':len(all_candidates),'cases':len(all_cases),'snapshots':len(all_snap),
                                'trend_snapshots':len(all_lines),'pit_all_pass':audit['all_pit_pass']}),flush=True)

if __name__=='__main__':run()
