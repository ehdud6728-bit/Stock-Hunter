"""R1-D100 dynamic high/low lines and long-term moving average study.

Frozen R1-D96-D99 untouched. Existing GitHub Actions YAML reused via update.
Historical future snapshots are research outcomes only, never ASOF selection.
"""
from __future__ import annotations
import hashlib,json,sys
from pathlib import Path
from collections import Counter
import numpy as np
import pandas as pd
from dynamic_structure import compare_asofs,structure,ma_report,timeline,index_at
from band_structure import enrich_bands,band_report

HERE=Path(__file__).resolve().parent
BASE=HERE.parent/'r1d97'
R98=HERE.parent/'r1d98'
R99=HERE.parent/'r1d99'
OUT=HERE/'results'
sys.path[:0]=[str(BASE),str(R98),str(R99)]
from research_r1d96 import load,choose
from state_machine import enrich as enrich98,pick as pick98
from recency_dedup import enrich as enrich99,assert_unmodified

CASES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-04'),('006400','삼성SDI','2024-02-07')]
SHA={'039840':'330ffdc5febbbf76f3f959d72181a2a37cc144ec567e6e7f691985b7dc82e3e4',
'071840':'aba4ffb8702961fbc8d3d411138b755092551f2fd6ef1a2ac3ce02da2c221c1b',
'006400':'d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566'}


def write_csv(filename,records):
    pd.DataFrame(records).to_csv(OUT/filename,index=False,encoding='utf-8-sig')


def verify():
    for code,sha in SHA.items():
        f=BASE/'data'/f'{code}_KRX.csv'
        if not f.is_file() or hashlib.sha256(f.read_bytes()).hexdigest()!=sha:
            raise RuntimeError(f'Frozen KRX data absent/changed: {code}')


def draw(d,code,name,asof,after,basename):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    import matplotlib.font_manager as fm
    for fp in ('/usr/share/fonts/truetype/nanum/NanumGothic.ttf',):
        if Path(fp).exists():fm.fontManager.addfont(fp)
    for fontname in ('NanumGothic','Noto Sans CJK KR','Malgun Gothic'):
        if any(f.name==fontname for f in fm.fontManager.ttflist):
            plt.rcParams['font.family']=fontname;break
    t=index_at(d,after);start=max(0,t-85)
    v=d.iloc[start:t+1];cur=structure(d,after)
    fig,(ax,axma,axwidth,axamt)=plt.subplots(4,1,sharex=True,figsize=(15,13),
          gridspec_kw={'height_ratios':[3.8,1.4,1.3,.85]})
    for q in v.itertuples():
        col='#d5514c' if q.close>=q.open else '#3979a6'
        ax.vlines(q.idx,q.low,q.high,color=col,linewidth=1)
        ax.add_patch(Rectangle((q.idx-.28,min(q.open,q.close)),.56,max(.25,abs(q.close-q.open)),
                               color=col,alpha=.86))
    # Complete upper/lower pairs, matching period and line style. BB30/40 widths
    # are analysed in CSV; the main price panel intentionally keeps BB20 readable.
    for upper,lower,color,label in (
        ('BB20_HI','BB20_LO','#39a4bf','BB20 +/-2sd'),
        ('ENV20_10_HI','ENV20_10_LO','#cb9a3a','ENV20 +/-10%'),
        ('ENV40_10_HI','ENV40_10_LO','#927253','ENV40 +/-10%')):
        ax.plot(v.idx,v[upper],color=color,lw=1.1,ls='-',alpha=.77,label=label+' upper')
        ax.plot(v.idx,v[lower],color=color,lw=1.1,ls='--',alpha=.77,label=label+' lower')
    palette={('HIGH','SHORT'):'#f08c28',('HIGH','MEDIUM'):'#a45516',
             ('LOW','SHORT'):'#18ac91',('LOW','MEDIUM'):'#187865'}
    for l in cur['lines']:
        first=max(start,l['h2_confirm_idx'])
        xx=np.arange(first,t+1)
        if not len(xx):continue
        yy=l['h1_value']+(l['h2_value']-l['h1_value'])*(xx-l['h1_idx'])/(l['h2_idx']-l['h1_idx'])
        ax.plot(xx,yy,color=palette[l['kind'],l['tier']],lw=2.1 if l['tier']=='SHORT' else 1.2,
                linestyle='--' if l['kind']=='HIGH' else '-',
                label=f'{l["kind"]} {l["tier"]}: {l["h1"]} -> {l["h2"]} ({l["direction"]})')
        for idx,price in ((l['h1_idx'],l['h1_value']),(l['h2_idx'],l['h2_value'])):
            if start<=idx<=t:ax.scatter([idx],[price],s=22,color=palette[l['kind'],l['tier']],zorder=5)
    initial=index_at(d,asof)
    ax.axvline(initial,color='#7b7b7b',ls='-.',lw=1.4,label='Original ASOF')
    if t>initial:ax.axvspan(initial,t,color='#e6a656',alpha=.08)
    for n,c in ((112,'#358b49'),(224,'#242424'),(448,'#9866b6')):
        axma.plot(v.idx,v[f'MA{n}'],color=c,linewidth=1.3,label=f'MA{n}')
    axma.plot(v.idx,v.close,color='#777',lw=.9,label='close')
    axma.legend(loc='upper left',fontsize=8,ncol=4)
    # Relative BB bandwidth measures volatility. Envelope RELATIVE width is
    # fixed at 20% by construction, so its ABSOLUTE width belongs on right axis.
    for n,color in ((20,'#299fc3'),(30,'#6255af'),(40,'#23a480')):
        axwidth.plot(v.idx,v[f'BB{n}_WIDTH_PCT'],color=color,lw=1.2,label=f'BB{n} width %')
    axwidth.set_ylabel('BB relative width %')
    aw=axwidth.twinx()
    for n,color in ((20,'#c39b42'),(40,'#8f674a')):
        aw.plot(v.idx,v[f'ENV{n}_WIDTH_ABS'],color=color,lw=1.15,ls=':',label=f'ENV{n} abs width KRW')
    aw.set_ylabel('ENV absolute width KRW')
    axwidth.legend(loc='upper left',fontsize=7,ncol=3)
    aw.legend(loc='upper right',fontsize=7,ncol=2)
    axamt.bar(v.idx,v.amount/1e8,color='#929292',width=.7)
    axamt.set_ylabel('amount / 100M KRW')
    xt=v.iloc[::max(1,len(v)//12)]
    axamt.set_xticks(xt.idx)
    axamt.set_xticklabels(xt.date.dt.strftime('%Y-%m-%d'),rotation=38,fontsize=8)
    ax.legend(loc='upper left',fontsize=7.3,ncol=2)
    for a in (ax,axma,axwidth,axamt):a.grid(alpha=.13)
    ax.set_ylabel('OHLC / BANDS / LIVE RE-ANCHOR')
    axma.set_ylabel('MA112/224/448')
    ax.set_xlim(start-.6,t+.6)
    ax.set_title(f'{name} ({code}) ASOF {after} | {cur["structure"]} | Historical raw prices; CA not certified')
    fig.tight_layout();p=OUT/'charts'/f'{code}_{basename}.png';fig.savefig(p,dpi=135,bbox_inches='tight');plt.close(fig)
    return p.name


def run():
    verify();(OUT/'charts').mkdir(parents=True,exist_ok=True)
    all_ma=[];all_bands=[];all_lines=[];all_events=[];all_compare=[];all_tests=[];all_preservation=[];charts=[]
    for code,name,asof in CASES:
        d=enrich_bands(load(code));t=index_at(d,asof)
        if t+20>=len(d):raise ValueError(f'Insufficient ex-post bars: {code}')
        days=[str(d.iloc[t+k].date.date()) for k in (0,10,20)]
        views=compare_asofs(d,days)
        for stage,day,ma,st in zip(('ASOF','D10_EXPOST','D20_EXPOST'),days,views['ma'],views['structure']):
            all_ma.append({'code':code,'name':name,'stage':stage,**ma,'price_basis':'RAW_UNADJUSTED'})
            bb_env=band_report(d,day)
            for band in bb_env:
                all_bands.append({'code':code,'name':name,'stage':stage,**band})
            for line in st['lines']:
                all_lines.append({'code':code,'name':name,'stage':stage,
                                  'structure':st['structure'],**line})
            all_compare.append({'code':code,'name':name,'stage':stage,'asof':day,
                                'close':ma['close'],'short_pattern':st['structure'],
                                'high_direction':next((z['direction'] for z in st['lines'] if z['kind']=='HIGH' and z['tier']=='SHORT'),'MISSING'),
                                'low_direction':next((z['direction'] for z in st['lines'] if z['kind']=='LOW' and z['tier']=='SHORT'),'MISSING'),
                                'ma_order':ma['ma_order'],
                                'slope112_20d':ma['MA112_slope20_pct'],
                                'slope224_20d':ma['MA224_slope20_pct'],
                                'slope448_20d':ma['MA448_slope20_pct'],
                                'spread112_224':ma['MA112_to_MA224_spread_pct'],
                                'spread224_448':ma['MA224_to_MA448_spread_pct']})
        for ev in timeline(d,days[-1],lookback=40):all_events.append({'code':code,'name':name,**ev})
        # Old WATCH / PRIMARY MUST be immutable. R1D100 never touches R98/R99 selections.
        old,_,_=choose(d,asof);r98=enrich98(old,d,asof);picks=pick98(r98)
        r99=enrich99(r98,d,asof);assert_unmodified(r98,r99)
        original_ids=[r['line_id'] for bucket in ('PRIMARY','WATCH') for r in picks[bucket]]
        watch_ids=[r['line_id'] for r in picks['WATCH']]
        if not all(any(z['line_id']==sid and z['bucket']=='WATCH' for z in r99) for sid in watch_ids):
            raise AssertionError('WATCH preservation failure')
        all_preservation.append({'code':code,'asof':asof,'r98_selected':len(original_ids),
            'r99_selected_unchanged':len(original_ids),'watch_unchanged':len(watch_ids),
            'old_lines_deleted':0,'new_line_catalog_separate':True})
        # Future prices cannot affect current snapshot (including MA values and pivot confirmation).
        original=(ma_report(d,asof),structure(d,asof),band_report(d,asof))
        altered=d.copy()
        for col in ('open','high','low','close'):
            altered.loc[altered.index>t,col]=altered.loc[altered.index>t,col].astype(float)*1.217
        altered=enrich_bands(altered)
        if original!=(ma_report(altered,asof),structure(altered,asof),band_report(altered,asof)):
            raise AssertionError('PIT mutation violated: '+code)
        all_tests.append({'code':code,'asof':asof,'passed':True,'D10':days[1],'D20':days[2]})
        charts.extend([draw(d,code,name,asof,asof,'ASOF'),draw(d,code,name,asof,days[-1],'D20_EXPOST')])
        print(f'{code} {name} asof={days[0]} d20={days[2]}',
              f'structure={views["structure"][0]["structure"]}->{views["structure"][2]["structure"]}',
              f'112MA slope={views["ma"][0]["MA112_slope20_pct"]:.2f}%->{views["ma"][2]["MA112_slope20_pct"]:.2f}%',
              f'WATCH preserved={len(watch_ids)}',
              f'BB20 width={band_report(d,days[0])[0]["band_width_pct"]:.2f}% -> {band_report(d,days[2])[0]["band_width_pct"]:.2f}%')
    for file,rows in [('R1D100_MA_SLOPE_GAP_CHANGES.csv',all_ma),('R1D100_BB_ENV_FULL_BAND_WIDTH.csv',all_bands),('R1D100_DYNAMIC_HIGH_LOW_LINES.csv',all_lines),
            ('R1D100_REANCHOR_EVENTS.csv',all_events),('R1D100_SNAPSHOT_COMPARE.csv',all_compare),
            ('R1D100_R98_R99_WATCH_PRESERVED.csv',all_preservation),('R1D100_PIT_TAMPER.csv',all_tests)]:
        write_csv(file,rows)
    audit={'version':'R1D100_DYNAMIC_DUAL_TRENDLINE_MA_112_224_448_RESEARCH_ONLY',
           'cases':len(CASES),'snapshot_rows':len(all_compare),'dynamic_lines':len(all_lines),
           'reanchor_events':len(all_events),'full_band_rows':len(all_bands),
           'bb_periods':[20,30,40],'env_periods':[20,40],'env_k_pct':10,'bb_sigma':2,
           'frozen_watch_preserved':sum(r['watch_unchanged'] for r in all_preservation),
           'all_pit_test_pass':all(r['passed'] for r in all_tests), 'source_hash_verified':True,
           'charts':charts,'limitations':['Historical unadjusted KRX price: corporate actions not fully verified',
           'No full market/OOS test','Slope/stack states are research diagnostics, not buy recommendations',
           'Envelope normalized width is mathematically fixed at 20%; only absolute width follows SMA',
           'Forward snapshots computed separately and never used in historical selection',
           'Pivot HIGH uses CLOSE, pivot LOW uses LOW to preserve original basis',
           'No hypothesis return-calibrated selection thresholds']}
    (OUT/'R1D100_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ('cases','snapshot_rows','dynamic_lines','reanchor_events','frozen_watch_preserved','all_pit_test_pass')}))
if __name__=='__main__':run()
