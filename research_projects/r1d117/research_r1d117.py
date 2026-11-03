"""R1-D117 research-only: conservative active-line continuity and post-break role watch.
Frozen inputs R111, R112 and R116 are read-only, with full SHA-256 preservation.
"""
from __future__ import annotations
import json,hashlib,sys
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent; ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d104'),str(HERE)]
from stable_roles import replay_one,verify,level,POLICY

SOURCES={
 'catalog':ROOT/'r1d111'/'results'/'R1D111_INDEPENDENT_METHOD_LINES_PIT.csv',
 'original':ROOT/'r1d112'/'results'/'R1D112_MULTISCALE_ACTIVE_LINES_DAILY_PIT.csv',
 'challenge':ROOT/'r1d116'/'results'/'R1D116_RECENT_TURN_ACTIVE_LANES_PIT.csv',
 'top3':ROOT/'r1d116'/'results'/'R1D116_CANDIDATE_TOP3_SCORE_COMPONENTS_PIT.csv',
}
CASE_DATES=[('039840','디오','2024-09-10'),('039840','디오','2024-09-13'),
            ('071840','롯데하이마트','2024-03-14'),('071840','롯데하이마트','2024-03-21'),
            ('006400','삼성SDI','2024-02-16')]
REFERENCES=[('039840','DIO_BULL_TOP','BULL_BODY_TOP','2024-09-03','2024-09-09'),
 ('039840','DIO_WICK_LOW','WICK_LOW_PIVOT2','2024-08-26','2024-09-06'),
 ('071840','LOTTE_BULL_TOP','BULL_BODY_TOP','2024-02-20','2024-03-13'),
 ('071840','LOTTE_WICK_LOW','WICK_LOW_PIVOT2','2024-02-01','2024-03-11')]

def read_sources(paths=None):
    paths={k:Path(v) for k,v in (paths or SOURCES).items()}
    for p in paths.values():
        if not p.is_file():raise FileNotFoundError('Required historical study missing '+str(p))
    sha={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    frames={k:pd.read_csv(p,dtype={'code':str},low_memory=False) for k,p in paths.items()}
    for k,x in frames.items():x['code']=x.code.astype(str).str.zfill(6)
    c=frames['catalog'];s=frames['original'];b=frames['challenge']
    if c[['code','algorithm','line_id']].duplicated().any():raise AssertionError('Catalog duplicate line')
    for n,f in [('original',s),('challenge',b)]:
        if f[['code','algorithm','scale','kind','idx']].duplicated().any():raise AssertionError('duplicate lane '+n)
        if f.groupby(['code','idx']).size().ne(24).any():raise AssertionError('incomplete 24 lanes '+n)
    if not s[['code','algorithm','scale','kind','idx']].equals(b[['code','algorithm','scale','kind','idx']]):
        # Do not require order; compare identities with a sort.
        fields=['code','algorithm','scale','kind','idx']
        if not s[fields].sort_values(fields).reset_index(drop=True).equals(b[fields].sort_values(fields).reset_index(drop=True)):
            raise AssertionError('R112-R116 identity mismatch')
    return frames,paths,sha

def manual_audit(cat,original,challenge,stable):
    rows=[]
    for code,label,family,a,b in REFERENCES:
        algs=sorted(cat[cat.code.eq(code)].algorithm.unique())
        for alg in algs:
            x=cat[(cat.code.eq(code)) & (cat.algorithm.eq(alg)) & (cat.family.eq(family)) &
                  cat.a1.eq(a) & cat.a2.eq(b)]
            if len(x)>1:raise AssertionError('Ambiguous reference anchors')
            rec={'code':code,'algorithm':alg,'label':label,'anchor1':a,'anchor2':b,
                 'discovered':len(x)==1,'reference_not_used_in_selection':True}
            if len(x):
                q=x.iloc[0];lid=str(q.line_id)
                rec['known_date']=str(q.known_date);rec['line_id']=lid
                for tag,df,col in [('r112',original,'active_line_id'),('r116',challenge,'line_id'),('r117',stable,'line_id')]:
                    match=df[df.code.eq(code)&df.algorithm.eq(alg)&df[col].eq(lid)]
                    rec[tag+'_ever_active']=bool(len(match))
                    rec[tag+'_first_active_date']=str(match.iloc[0].date) if len(match) else ''
            rows.append(rec)
    return pd.DataFrame(rows)

def plot_case(raw,catalog,original,challenge,stable,code,name,date,out):
    """Plot actual OHLCV and immutable geometry. Never extrapolate percentage slope.
    All lines display only when known_idx <= signal t; historical known-date
    dotted portions are context only and not implied as historical tradable data.
    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.exists():font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    d=raw.copy();d['date']=pd.to_datetime(d.date)
    if 'idx' not in d:d.insert(0,'idx',np.arange(len(d)))
    cases=d[d.date.dt.strftime('%Y-%m-%d').eq(date)]
    if len(cases)!=1:return None
    t=int(cases.iloc[0].idx); bars=d[d.idx.between(t-45,t)]
    if len(bars)<20:return None
    fig,(ax,vol)=plt.subplots(2,1,figsize=(17,10),sharex=True,gridspec_kw={'height_ratios':[5,1]},layout='constrained')
    for r in bars.to_dict('records'):
        j=int(r['idx']);o=float(r['open']);c=float(r['close']);h=float(r['high']);l=float(r['low'])
        co='#d44848' if c>=o else '#3777c2'
        ax.vlines(j,l,h,color=co,lw=.8)
        ax.add_patch(Rectangle((j-.29,min(o,c)),.58,max(.1,abs(c-o)),facecolor=co,alpha=.9))
        vol.bar(j,float(r['amount'])/1e8,color=co,width=.7,alpha=.55)
    lines=catalog[catalog.code.eq(code)&catalog.algorithm.eq('TS_BODY_WICK_PROXY')].set_index('line_id')
    def one(lid,color,label,style,width):
        if not lid or lid not in lines.index:return
        q=lines.loc[lid];verify(q);known=int(q.known_idx)
        if known>t:return
        x=np.arange(max(int(q.i1),int(bars.idx.min())),t+1)
        yy=np.array([level(q,j) for j in x])
        p=x<known
        if p.any():ax.plot(x[p],yy[p],color=color,ls=':',lw=max(1,width*.6),alpha=.45)
        if (~p).any():ax.plot(x[~p],yy[~p],c=color,ls=style,lw=width,label=label)
        for ix,vl in [(int(q.i1),float(q.v1)),(int(q.i2),float(q.v2))]:
            if ix>=int(bars.idx.min()) and ix<=t:ax.scatter([ix],[vl],s=47,c=color,edgecolors='black',lw=.6,zorder=15)
    def at(df,column):
        d=df[df.code.eq(code)&df.algorithm.eq('TS_BODY_WICK_PROXY')&df.date.eq(date)&
             df.scale.isin(['SHORT','MEDIUM'])&df.kind.eq('UPPER')]
        return [str(i) for i in d[column].fillna('') if str(i)]
    r112=at(original,'active_line_id');r116=at(challenge,'line_id');r117=at(stable,'line_id')
    watch=at(stable,'fresh_watch_line_id')
    close_now=float(cases.iloc[0].close)
    def nearest(ids):
        xs=[str(i) for i in dict.fromkeys(ids) if i in lines.index and int(lines.loc[i].known_idx)<=t]
        if not xs:return None
        return min(xs,key=lambda i:abs(level(lines.loc[i],t)/close_now-1))
    seen=set()
    def unique(lid,color,title,style,width,dist_limit=None):
        if not lid or lid in seen:return
        if dist_limit is not None and abs(level(lines.loc[lid],t)/close_now-1)>dist_limit:return
        seen.add(lid);one(lid,color,title,style,width)
    for lid in dict.fromkeys(r117):unique(lid,'#dc4936','R117 stable active', '-',2.5)
    for lid in dict.fromkeys(watch):unique(lid,'#ef9b19','R117 recent watch ONLY','-.',1.9,.09)
    unique(nearest(r116),'#d38a29','R116 challenger',':',1.6,.09)
    unique(nearest(r112),'#8d9099','R112 frozen baseline','--',1.4,.09)
    for rc,label,fam,a,b in REFERENCES:
        if rc!=code or (code=='039840' and date<'2024-09-09') or (code=='071840' and date<'2024-03-13') or fam!='BULL_BODY_TOP':continue
        matches=lines[(lines.family.eq(fam))&(lines.a1.eq(a))&(lines.a2.eq(b))]
        if len(matches)==1:unique(str(matches.index[0]),'#7c3aed','Human reference (audit only)','-.',1.8)
    lower=stable[(stable.code.eq(code)) & stable.algorithm.eq('TS_BODY_WICK_PROXY')&
                 stable.date.eq(date)&stable.kind.eq('LOWER')&stable.line_id.notna()&stable.line_id.ne('')]
    if len(lower):
        nearest=lower.iloc[(lower.exact_level.astype(float)-float(cases.iloc[0].close)).abs().argmin()]
        one(str(nearest.line_id),'#11976b','R117 closest lower','-',1.6)
    a=stable[stable.code.eq(code)&stable.algorithm.eq('TS_BODY_WICK_PROXY')&stable.date.eq(date)]
    crossed=a[a.event.eq('TWO_CLOSE_BREAK_ARCHIVE')]
    for r in crossed.to_dict('records'):
        if r['previous_line_id'] in lines.index:one(r['previous_line_id'],'#3b82f6','Retired on confirmation',':',2.0)
    ax.axvline(t,c='black',ls=':',lw=1);ax.set_ylabel('Price KRW')
    ax.set_title(f'{name} {date}: same real OHLCV | R112 vs R116 vs R117 (post-close)')
    ax.grid(alpha=.14);vol.grid(alpha=.12)
    bydate=d.set_index('idx').date.dt.strftime('%Y-%m-%d').to_dict()
    ticks=np.linspace(int(bars.idx.min()),t,9,dtype=int);vol.set_xticks(ticks)
    vol.set_xticklabels([bydate.get(x,str(x))[5:] for x in ticks],rotation=25)
    vol.set_ylabel('Amount 100M KRW');ax.legend(loc='upper left',fontsize=7,ncol=2)
    out.mkdir(parents=True,exist_ok=True);p=out/f'{code}_{date}_R1D117_STABILITY_TRUE_ANCHORS.png'
    fig.savefig(p,dpi=140);plt.close(fig);return p.name

def run(*,sources=None,output=None,raw_provider=None,draw=True,codes=None,require_full_raw=True):
    frames,paths,hashes=read_sources(sources)
    cat=frames['catalog'];orig=frames['original'];base=frames['challenge'];top=frames['top3']
    stocks=sorted(codes or orig.code.unique());out=Path(output or HERE/'results');out.mkdir(parents=True,exist_ok=True)
    if raw_provider is None:
        from research_r1d96 import load as raw_provider
        from research_r1d104 import _verify
        _verify()
    full_daily=[];full_events=[];full_roles=[];chart=[];coverage=[]
    for code in stocks:
        cc=cat[cat.code.eq(code)];ss=base[base.code.eq(code)];rr=top[top.code.eq(code)]
        raw=raw_provider(code).copy()
        if 'idx' not in raw:raw.insert(0,'idx',np.arange(len(raw)))
        raw['date']=pd.to_datetime(raw.date)
        if (raw.idx.duplicated().any() or raw.date.duplicated().any() or not raw.idx.is_monotonic_increasing):raise AssertionError('invalid raw date index')
        if require_full_raw and not set(ss.idx.unique()).issubset(set(raw.idx)):
            raise AssertionError('R116 state indices lack real raw OHLCV '+code)
        for alg in sorted(ss.algorithm.unique()):
            d,e,r=replay_one(code,alg,
                   ss[ss.algorithm.eq(alg)],rr[rr.algorithm.eq(alg)],
                   cc[cc.algorithm.eq(alg)].to_dict('records'),raw=raw)
            full_daily.append(d);full_events.append(e)
            if len(r):full_roles.append(r)
        coverage.append({'code':code,'raw_rows':len(raw),'daily_lanes':int(len(ss)),'ohlcv_matched':int(ss.idx.isin(raw.idx).sum())})
        print('CASE_DONE',code,'raw',len(raw),'proposals',len(ss),'PIT_PASS',flush=True)
    stable=pd.concat(full_daily,ignore_index=True);ev=pd.concat(full_events,ignore_index=True)
    roles=pd.concat(full_roles,ignore_index=True) if full_roles else pd.DataFrame()
    # Frozen R112 and R116 remain independent comparison columns.
    old=orig[['code','algorithm','idx','scale','kind','active_line_id','event','state']].rename(columns={
       'active_line_id':'r112_line_id','event':'r112_event','state':'r112_state'})
    cp=base[['code','algorithm','idx','scale','kind','line_id','status']].rename(columns={
       'line_id':'r116_line_id','status':'r116_status'})
    cmp=stable.merge(old,on=['code','algorithm','idx','scale','kind'],validate='one_to_one').merge(
       cp,on=['code','algorithm','idx','scale','kind'],validate='one_to_one')
    for col in ['line_id','r112_line_id','r116_line_id']:cmp[col]=cmp[col].fillna('')
    cmp['r117_vs_r112_different']=cmp.line_id.ne(cmp.r112_line_id)
    cmp['r117_vs_r116_different']=cmp.line_id.ne(cmp.r116_line_id)
    refs=manual_audit(cat,orig,base,stable)
    for code,name,date in CASE_DATES:
        if draw and code in stocks:
            file=plot_case(raw_provider(code),cat,orig,base,stable,code,name,date,out/'charts')
            if file:chart.append(file)
    def save(frame,name):frame.to_csv(out/name,index=False,encoding='utf-8-sig')
    save(stable,'R1D117_STABLE_ACTIVE_LINES_DAILY_PIT.csv')
    save(ev,'R1D117_STABILITY_TRANSITIONS_PIT.csv')
    save(roles,'R1D117_PRIOR_BREAK_ROLE_RETEST_EVENTS_PIT.csv')
    save(cmp,'R1D117_R112_R116_R117_SIDE_BY_SIDE_PIT.csv')
    save(refs,'R1D117_MANUAL_ILLUSTRATIONS_POST_SELECTION_AUDIT.csv')
    for key,path in paths.items():
        if hashlib.sha256(path.read_bytes()).hexdigest()!=hashes[key]:raise AssertionError('source mutated: '+key)
    audit={'version':'R1D117_STABILITY_ROLE_WATCH_RESEARCH_ONLY',
         'input_sha256':hashes,'source_unchanged':True,'no_reference_in_selector':True,
         'candidate_catalog_count':int(len(cat)),'r112_original_daily_rows':int(len(orig)),
         'r116_original_daily_rows':int(len(base)),'r117_daily_rows':int(len(stable)),
         'r117_transition_records':int(len(ev)),
         'r117_new_activations':int(ev.event.eq('ACTIVATE_NEW_CONFIRMED_LINE').sum()),
         'r117_reanchors':int(ev.event.eq('REANCHOR_MAJOR_NEW_ENDPOINT').sum()),
         'r117_confirmed_breaks':int(ev.event.eq('TWO_CLOSE_BREAK_ARCHIVE').sum()),
         'r117_first_break_pending':int(ev.event.eq('FIRST_CLOSE_OUTSIDE_PENDING').sum()),
         'role_touch_events':int(len(roles)),
         'active_lanes':int(stable.status.ne('WAIT_NEW_CONFIRMED_LINE').sum()),
         'same_vs_r112':int(cmp.line_id.eq(cmp.r112_line_id).sum()),
         'same_vs_r116':int(cmp.line_id.eq(cmp.r116_line_id).sum()),
         'reference_audit':refs.to_dict('records'),
         'raw_ohlcv_coverage':coverage,'charts':chart,'fixed_policy':POLICY,
         'full_raw_required':bool(require_full_raw),
         'limitations':['Returns were not used, no predictive superiority or trading win-rate inferred.',
          'Two-close break and minimum hold are unoptimized research hypotheses.',
          'Only R111 candidates are considered. No guarantee of visually preferred line.',
          'Real OHLCV retests are post-break observations at t close, never available at initial break.',
          'Study uses the same historical 3 stocks as prior comparisons and has no OOS outcome validation.']}
    (out/'R1D117_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf8')
    print('SUMMARY',json.dumps({k:audit[k] for k in ['r117_daily_rows','r117_new_activations','r117_reanchors',
        'r117_confirmed_breaks','role_touch_events','source_unchanged']},ensure_ascii=False),flush=True)
    return audit

if __name__=='__main__':run()
