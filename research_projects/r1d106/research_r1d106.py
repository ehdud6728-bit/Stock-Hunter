"""R1-D106 research-only line candidate lifetime & case comparisons.

R1D105 must run first and materialize the PIT snapshot file; the R1D106
signals never select anchors based on future returns or future tests.
"""
from __future__ import annotations
import sys,json,hashlib
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d104'),str(HERE)]
from multiline import make_lines,price_crosses,triangles,followthrough,line_value,validate,BULL,LOW

FIXED=[('039840','디오','2024-09-13','BREAKOUT_FAILURE'),
       ('071840','롯데하이마트','2024-03-04','RISING_LOW_LINE_FAILURE'),
       ('006400','삼성SDI','2024-02-07','REBOUND'),
       ('006400','삼성SDI','2024-01-22','EXPOST_REBOUND_REFERENCE'),
       ('039840','디오','2024-08-02','EXPOST_DRAWDOWN_REFERENCE')]
CONTEXT_PATH=ROOT/'r1d105'/'results'/'R1D105_ALL_ANCHOR_PRE_SNAPSHOTS.csv'
PIT_COLS=['ma5_slope20_pct','ma20_slope20_pct','ma60_slope20_pct','ma112_slope20_pct',
  'ma224_slope20_pct','ma20_slope_accel5_pp','bb20_width_pct','bb20_width_delta5_pp',
  'bb20_position','bb30_width_pct','bb40_width_pct','env20_10_position','env40_10_position',
  'ma_cloud_fast_thickness_pct','ma_cloud_mid_thickness_pct',
  'ichimoku_current_thickness_pct','amount_vs_prev20','volume_vs_prev20','structure',
  'short_high_trend_line_id','short_low_trend_line_id','medium_high_trend_line_id','medium_low_trend_line_id']


def _chart(df,ln,case_date,code,name,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.exists():
        font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    dates=df.date.dt.strftime('%Y-%m-%d')
    ids=np.flatnonzero(dates.eq(case_date))
    if len(ids)!=1:return None
    t=int(ids[0]);start=max(0,t-45);end=min(len(df)-1,t+20)
    fig,(ax,bx)=plt.subplots(2,1,figsize=(16,9),sharex=True,gridspec_kw={'height_ratios':[4.7,1]},layout='constrained')
    xs=np.arange(start,end+1)
    for i in xs:
        q=df.iloc[i];up=q.close>=q.open;col='#df4848' if up else '#3170bb'
        ax.vlines(i,q.low,q.high,color=col,lw=.85,zorder=3)
        ax.add_patch(Rectangle((i-.28,min(q.open,q.close)),.56,max(abs(q.close-q.open),.02),color=col,zorder=3))
        bx.bar(i,q.amount/1e8,color=col,width=.72,alpha=.7)
    # Lines selected using ONLY t-known information. Draw from first-known idx,
    # never reconstruct an historical signal with a future pivot.
    eligible=ln[(ln.known_idx<=t)&(ln.i2>=start-15)&(ln.i2<=t)]
    upper=eligible[eligible.kind=='UPPER'].copy()
    lower=eligible[eligible.kind=='LOWER'].copy()
    selected=[]
    for subset in [upper,lower]:
        if subset.empty:continue
        subset=subset.assign(dist=subset.apply(lambda x:abs(line_value(x,t)-df.iloc[t].close)/df.iloc[t].close,axis=1))
        selected.extend(subset.sort_values(['dist','i2','i1']).head(2).to_dict('records'))
    # Explicitly include the DIO manual reference lines when they were already known.
    if code=='039840' and case_date=='2024-09-13':
        for a,b,f in [('2024-09-03','2024-09-09',BULL),('2024-08-26','2024-09-06',LOW)]:
            r=ln[(ln.a1==a)&(ln.a2==b)&(ln.family==f)&(ln.known_idx<=t)]
            if len(r):
                x=r.iloc[0].to_dict()
                if x['line_id'] not in [z['line_id'] for z in selected]:selected.append(x)
    if code=='039840' and case_date=='2024-09-13':
        selected=[r for r in selected if r['family'] not in ('BULL_BODY_TOP','WICK_LOW_PIVOT2')]
        selected=[r for r in selected if r['kind']=='UPPER'][:1]+[r for r in selected if r['kind']=='LOWER'][:1]
        # The two visually user-proposed lines remain highlighted, regardless of proximity rank.
        for a,b,f in [('2024-09-03','2024-09-09',BULL),('2024-08-26','2024-09-06',LOW)]:
            r=ln[(ln.a1==a)&(ln.a2==b)&(ln.family==f)&(ln.known_idx<=t)]
            if len(r):selected.append(r.iloc[0].to_dict())
    for k,line in enumerate(selected):
        left=max(start,int(line['known_idx']));right=end
        x=np.arange(left,right+1)
        y=[line_value(line,i) for i in x]
        focus=(code=='039840' and line['a1'] in ('2024-09-03','2024-08-26') and
               line['a2'] in ('2024-09-09','2024-09-06'))
        color=('#f59e0b' if line['kind']=='UPPER' else '#059669') if focus else ('#e11d48' if line['kind']=='UPPER' else '#155e75')
        ax.plot(x,y,ls='-' if focus else ':',lw=2.5 if focus else 1.05,
            alpha=1 if focus else .65,label=f"{line['family']} {line['a1'][5:]}→{line['a2'][5:]} (known {line['known_date'][5:]})")
    ax.axvline(t,ls='--',color='black',alpha=.8)
    ax.axvspan(t,end,color='#f59e0b',alpha=.07)
    ax.grid(alpha=.15);bx.grid(alpha=.14)
    ticks=np.linspace(start,end,num=min(10,end-start+1),dtype=int)
    bx.set_xticks(ticks);bx.set_xticklabels(dates.iloc[ticks].str[5:],rotation=25)
    ax.set_ylabel('가격 (원)');bx.set_ylabel('거래대금 (억원)')
    ax.legend(loc='upper left',fontsize=7,ncol=2)
    fig.suptitle(f'{name} {case_date} | PIT 후보추세선 + 실제 캔들 | 음영=사후관찰',fontsize=14)
    dest=out/'charts'/f'{code}_{case_date}_R1D106_ANCHORS.png'
    fig.savefig(dest,dpi=135);plt.close(fig)
    return dest.name


def _prehistory(df,rows,ln,case_date,code,name,offsets=(-20,-10,-5,-1,0,1,3,5,10,20)):
    matches=np.flatnonzero(df.date.dt.strftime('%Y-%m-%d').eq(case_date))
    if len(matches)!=1:return []
    t=int(matches[0]);result=[]
    for shift in offsets:
        j=t+shift
        if j<0 or j>=len(df):continue
        q=df.iloc[j];r=rows[rows.date.eq(str(q.date.date()))]
        if len(r)>1:raise AssertionError('duplicate context date')
        record={'code':code,'name':name,'case_date':case_date,'relative_bar':shift,
                'observation_date':str(q.date.date()),'close':q.close,'open':q.open,'high':q.high,'low':q.low,
                'when_known':'BEFORE_OR_ON_CASE_DATE' if shift<=0 else 'EXPOST_ONLY'}
        if len(r):
            for key in PIT_COLS:record[key]=r.iloc[0].get(key,np.nan)
        now=ln[(ln.known_idx<=j)&(ln.i2<=j)].copy()
        for kind in ['UPPER','LOWER']:
            near=now[now.kind==kind]
            if len(near):
                near=near.assign(gap=near.apply(lambda x:abs(line_value(x,j)-q.close),axis=1))
                hit=near.sort_values('gap').iloc[0];record[kind+'_closest_id']=hit.line_id
                record[kind+'_closest_price']=round(line_value(hit,j),3)
                record[kind+'_closest_distance_pct']=round(100*(q.close/line_value(hit,j)-1),3)
        result.append(record)
    return result


def run(*,input_provider=None,context_override=None,output=None,charts=True,strict_fixed=True):
    """If fixtures are provided, no access to GitHub, no upstream file edits."""
    output=Path(output or HERE/'results');output.mkdir(parents=True,exist_ok=True)
    (output/'charts').mkdir(exist_ok=True)
    if input_provider is None:
        from research_r1d96 import load as input_provider
        from research_r1d104 import _verify
        _verify()
    if context_override is None:
        if not CONTEXT_PATH.exists():
            raise FileNotFoundError('R1D104 context missing: run R1D105 step first')
        context=pd.read_csv(CONTEXT_PATH,dtype={'code':str})
        # R1-D105 materializes D-20..D0 snapshots. It recomputes R1-D104
        # in a TEMP directory, so the R104 results directory does NOT persist.
        # Reuse the durable PIT snapshot instead of requesting a second R104 run.
        if 'observation_date' not in context:raise ValueError('R105 snapshot date missing')
        context=context.rename(columns={'observation_date':'date'})
        missing=[k for k in PIT_COLS if k not in context.columns]
        if missing:raise ValueError('R105 snapshot lacks PIT fields '+str(missing))
        inconsistent=context.groupby(['code','date'],dropna=False)[PIT_COLS].nunique(dropna=False)
        if (inconsistent>1).any().any():raise AssertionError('materialized PIT snapshot identity mismatch')
        context=context.sort_values(['code','date']).drop_duplicates(['code','date']).reset_index(drop=True)
    else:context=context_override.copy()
    context['code']=context.code.astype(str).str.zfill(6)
    if context[['code','date']].duplicated().any():raise ValueError('duplicate PIT date')
    if any(x.endswith('_EXPOST') for x in PIT_COLS):raise AssertionError('outcome leaked into PIT')
    lines_all=[];events_all=[];triangles_all=[];outcomes_all=[];case_all=[];pre_all=[];cases=[];audits=[];copies=[]
    stock_set=list(dict.fromkeys([(x[0],x[1]) for x in FIXED]))
    if not strict_fixed:stock_set=list(dict.fromkeys((str(c).zfill(6),n) for c,n in zip(context.code,context.get('name',context.code))))
    for code,name in stock_set:
        raw=input_provider(code).copy();raw['date']=pd.to_datetime(raw['date']);validate(raw)
        ln=make_lines(raw);ev=price_crosses(raw,ln);tri=triangles(raw,ln)
        if ln.line_id.duplicated().any():raise AssertionError('Duplicate line IDs')
        if len(ev) and (ev.known_idx>=ev.idx).any():raise AssertionError('line used on confirmation day')
        if len(tri) and ((tri.idx<tri.upper_known_date.map(lambda z:int(np.flatnonzero(raw.date.eq(z))[0]))).any()):
            raise AssertionError('triangle before upper known')
        # Globally unique IDs across tickers, while keeping the anchor's identity.
        ln['line_id']=code+'|'+ln['line_id'].astype(str)
        if len(ev):ev['line_id']=code+'|'+ev['line_id'].astype(str)
        if len(tri):
            tri['upper_id']=code+'|'+tri['upper_id'].astype(str)
            tri['lower_id']=code+'|'+tri['lower_id'].astype(str)
        # Output ALL lines, not only active. No cleanup based on recency or outcomes.
        ln.insert(0,'code',code);ln.insert(1,'name',name)
        if len(ev):ev.insert(0,'code',code);ev.insert(1,'name',name)
        if len(tri):tri.insert(0,'code',code);tri.insert(1,'name',name)
        ctx=context[context.code.eq(code)];
        if not ctx.empty and len(ev):
            met=ev.merge(ctx[['date',*PIT_COLS]],on='date',how='left',validate='many_to_one')
            met['type']='PIT_CROSS_EVENT';case_all.append(met)
        ln_original=ln.drop(columns=['code','name'])
        if len(ev):
            fol=followthrough(raw,ev,ln_original)
            fol.insert(0,'code',code);fol.insert(1,'name',name)
            outcomes_all.append(fol)
        for cc,nm,dt,kind in FIXED:
            if cc!=code:continue
            if not raw.date.dt.strftime('%Y-%m-%d').eq(dt).any():
                if strict_fixed:raise AssertionError('missing fixed date '+code+dt)
                continue
            pre_all+=_prehistory(raw,ctx,ln_original,dt,code,name)
            ix=int(np.flatnonzero(raw.date.dt.strftime('%Y-%m-%d').eq(dt))[0])
            known=ln_original[ln_original.known_idx<=ix]
            cases.append({'code':code,'name':name,'date':dt,'kind':kind,'known_lines':len(known),
                          'upper_known':int((known.kind=='UPPER').sum()),
                          'lower_known':int((known.kind=='LOWER').sum()),
                          'close':float(raw.iloc[ix].close),
                          'forward20_close_pct_EXPOST':round(100*(raw.iloc[ix+20].close/raw.iloc[ix].close-1),3)
                               if ix+20<len(raw) else np.nan})
            if charts:
                _chart(raw,ln_original,dt,code,name,output)
        print('CASE_DONE',code,name,'lines',len(ln),'crosses',len(ev),'triangles',len(tri),flush=True)
        lines_all.append(ln);events_all.append(ev);triangles_all.append(tri)
        audits.append({'code':code,'input_rows':len(raw),'lines':len(ln),'crosses':len(ev),
                       'triangles':len(tri),'last_raw_date':str(raw.iloc[-1].date.date())})
    def save(frames,name):
        valid=[x for x in frames if len(x)]
        z=pd.concat(valid,ignore_index=True) if valid else pd.DataFrame()
        z.to_csv(output/name,index=False,encoding='utf-8-sig');return z
    ln=save(lines_all,'R1D106_ALL_IMMUTABLE_LINES.csv')
    events=save(events_all,'R1D106_LINE_CROSS_EVENTS_PIT.csv')
    tris=save(triangles_all,'R1D106_ALL_TRIANGLE_CANDIDATES_PIT.csv')
    save(case_all,'R1D106_CONFLUENCE_AT_CROSSES_PIT.csv')
    save(outcomes_all,'R1D106_LINE_FOLLOWTHROUGH_EXPOST.csv')
    pre=save([pd.DataFrame(pre_all)],'R1D106_FIVE_CASES_PRE_POST_TIMELINES.csv')
    save([pd.DataFrame(cases)],'R1D106_FIVE_CASES_SUMMARY.csv')
    # DIO: audited manual reference: use line candidates but not a future-fitted selection.
    dio=ln[ln.code.eq('039840')]
    if len(dio):
        upper=dio[(dio.family==BULL)&(dio.a1=='2024-09-03')&(dio.a2=='2024-09-09')]
        lower=dio[(dio.family==LOW)&(dio.a1=='2024-08-26')&(dio.a2=='2024-09-06')]
        if strict_fixed and (len(upper)!=1 or len(lower)!=1):raise AssertionError('DIO known reference lines not found')
        if len(upper)==1 and len(lower)==1:
            u=upper.iloc[0];d=lower.iloc[0]
            if strict_fixed:
                assert u.known_date=='2024-09-09' and d.known_date=='2024-09-10'
                assert u.slope_krw_per_bar==-100
            dio_cases=[]
            raw=input_provider('039840').copy();raw['date']=pd.to_datetime(raw.date)
            for day in ['2024-09-03','2024-09-06','2024-09-09','2024-09-10','2024-09-11',
                        '2024-09-12','2024-09-13','2024-09-19','2024-09-25']:
                matches=raw.index[raw.date.dt.strftime('%Y-%m-%d').eq(day)]
                if len(matches)!=1:continue
                idx=int(matches[0]);v=raw.loc[idx];
                e={'date':day,'close':v.close,'open':v.open,'high':v.high,'low':v.low,'idx':idx,
                   'upper_first_known':u.known_date,'lower_first_known':d.known_date,
                   'upper_level':round(line_value(u,idx),3) if idx>=u.known_idx else np.nan,
                   'lower_level':round(line_value(d,idx),3) if idx>=d.known_idx else np.nan,
                   'upper_known_at_close':bool(idx>=u.known_idx),
                   'lower_known_at_close':bool(idx>=d.known_idx)}
                e['both_lines_known']=e['upper_known_at_close'] and e['lower_known_at_close']
                if idx>=u.known_idx:e['close_to_upper_pct']=round(100*(v.close/e['upper_level']-1),3)
                if idx>=d.known_idx:e['close_to_lower_pct']=round(100*(v.close/e['lower_level']-1),3)
                dio_cases.append(e)
            pd.DataFrame(dio_cases).to_csv(output/'R1D106_DIO_0903_0909_TRIANGLE_FACTCHECK.csv',index=False,encoding='utf-8-sig')
    report={'version':'R1D106_MULTILINE_BODY_PIVOT_TRIANGLE_RESEARCH_ONLY',
            'tests':'TESTED_SEPARATELY','stocks':audits,'raw_lines':len(ln),
            'cross_events':len(events),'triangle_geometry_candidates':len(tris),
            'pit_rules':['body anchor known on its own close','wick pivot confirmed at j+2',
                        'cross strictly requires line known on preceding day',
                        'triangle first known at max(upper confirmed, lower confirmed)',
                        'expost outcomes written separately',
                        'all original line IDs persisted, scan-age is NOT expiry'],
            'limitations':['not a trading signal','only 3 historical stocks, no OOS',
                           'PIT corporate action adjustment not audited',
                           'historical selected lines are hypotheses not full automatic pattern validation']}
    (output/'R1D106_AUDIT.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print('SUMMARY',json.dumps({'lines':len(ln),'crosses':len(events),'triangles':len(tris),
                                 'case_timeline_rows':len(pre)},ensure_ascii=False),flush=True)
    return report

if __name__=='__main__':run()
