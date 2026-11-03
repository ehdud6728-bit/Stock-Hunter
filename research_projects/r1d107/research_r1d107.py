"""R1-D107 PIT line reliability and constrained wedge recognition.

Reads R1-D106 outputs, preserves them unchanged, and re-evaluates the catalog
using frozen R1-D97 price files. Selection never uses EXPOST returns.
"""
from __future__ import annotations
import hashlib, json, sys
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
sys.path[:0]=[str(ROOT/'r1d97'),str(ROOT/'r1d106'),str(HERE)]
from multiline import line_value
from quality import line_quality,display_groups,triangle_quality,apex_limited_outcome

R106=ROOT/'r1d106'/'results'
CASES=[('039840','디오','2024-09-10','DIO_FIRST_BOTH_KNOWN'),
       ('039840','디오','2024-09-13','DIO_LATER_BREAKOUT_FAILURE_EXAMPLE'),
       ('071840','롯데하이마트','2024-03-13','LOTTE_BODY_LINE_CREATED'),
       ('071840','롯데하이마트','2024-03-21','LOTTE_NEAR_APEX_BREAK'),
       ('006400','삼성SDI','2024-02-07','SDI_REFERENCE'),
       ('006400','삼성SDI','2024-02-08','SDI_AFTER_REBOUND')]
# Explicit user-supplied illustration only. Never used to choose any line or threshold.
USER_REFERENCE_LINES=[('039840','BULL_BODY_TOP','2024-09-03','2024-09-09'),
                      ('039840','WICK_LOW_PIVOT2','2024-08-26','2024-09-06'),
                      ('071840','BULL_BODY_TOP','2024-02-20','2024-03-13'),
                      ('071840','WICK_LOW_PIVOT2','2024-02-01','2024-03-11')]
MA_WINDOWS=(3,5,10,20,30,40,60,112,120,224,240,448,480)


def enrich_asof(fr):
    """Rolling left-only time series. All columns at t read bars <=t."""
    d=fr.copy()
    c=d.close.astype(float)
    for n in MA_WINDOWS:
        ma=c.rolling(n,min_periods=n).mean();d[f'ma{n}']=ma
        d[f'ma{n}_slope5_pct']=100*(ma/ma.shift(5)-1)
        d[f'ma{n}_slope20_pct']=100*(ma/ma.shift(20)-1)
        d[f'price_to_ma{n}_pct']=100*(c/ma-1)
    for n in (20,30,40):
        ma=d[f'ma{n}']; sd=c.rolling(n,min_periods=n).std(ddof=0)
        d[f'bb{n}_width_pct']=100*(4*sd/ma)
        d[f'bb{n}_top']=ma+2*sd;d[f'bb{n}_bottom']=ma-2*sd
    for n in (20,40):
        d[f'env{n}_upper']=d[f'ma{n}']*1.1
        d[f'env{n}_lower']=d[f'ma{n}']*.9
    for field in ('volume','amount'):
        d[f'{field}_vs_prev20']=d[field]/d[field].shift(1).rolling(20,min_periods=20).mean()
    for tag, windows in (('fast',(3,5,10,20)),('mid',(30,40,60,112,120)),('long',(224,240,448,480))):
        mat=d[[f'ma{x}' for x in windows]]
        d[f'{tag}_cloud_thickness_pct']=100*(mat.max(axis=1)-mat.min(axis=1))/mat.mean(axis=1)
    return d


def _snapshot(d,t):
    r=d.iloc[t]; out={'asof':str(r.date.date()),'asof_idx':t,'close':float(r.close)}
    for w in (5,20,60,112,224):
        for suf in ('slope5_pct','slope20_pct'):
            v=r[f'ma{w}_{suf}'];out[f'ma{w}_{suf}']=round(float(v),3) if pd.notna(v) else None
    for n in (20,30,40):
        k=f'bb{n}_width_pct';out[k]=round(float(r[k]),3) if pd.notna(r[k]) else None
    for n in (20,40):
        for s in ('upper','lower'):
            k=f'env{n}_{s}';out[k]=round(float(r[k]),3) if pd.notna(r[k]) else None
    for k in ('fast_cloud_thickness_pct','mid_cloud_thickness_pct','long_cloud_thickness_pct',
              'volume_vs_prev20','amount_vs_prev20'):
        out[k]=round(float(r[k]),3) if pd.notna(r[k]) else None
    return out


def _plot(d,case,quality_rows,lines,tri,output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib import font_manager
    font=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
    if font.exists():
        font_manager.fontManager.addfont(str(font));plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    code,name,date,_=case
    ts=d.date.dt.strftime('%Y-%m-%d');hits=np.flatnonzero(ts.eq(date))
    if len(hits)!=1:return None
    t=int(hits[0]);st=max(0,t-40);end=min(len(d)-1,t+12)
    fig,(ax,bx)=plt.subplots(2,1,figsize=(15,9),sharex=True,gridspec_kw={'height_ratios':[4.5,1]})
    for j in range(st,end+1):
        r=d.iloc[j];color='#d94841' if r.close>=r.open else '#3378b7'
        ax.vlines(j,r.low,r.high,color=color,lw=.8)
        ax.add_patch(Rectangle((j-.28,min(r.open,r.close)),.56,max(.1,abs(r.close-r.open)),color=color,alpha=.85))
        bx.bar(j,r.amount/1e8,color=color,width=.65,alpha=.6)
    for n,col in ((5,'#ef4444'),(20,'#2563eb'),(60,'#16a34a')):
        ax.plot(range(st,end+1),d.iloc[st:end+1][f'ma{n}'],color=col,lw=.8,alpha=.6,label=f'MA{n}')
    # Display top non-overlapping lines. Additionally show user reference lines,
    # but never force them into automatic ranking or claim they were top picks.
    reps=quality_rows[quality_rows.is_display_representative]
    selected=lines[lines.line_id.isin(reps.line_id)]
    for code0,f,a,b in USER_REFERENCE_LINES:
        if code0!=code:continue
        rr=lines[(lines.family==f)&(lines.a1==a)&(lines.a2==b)&(lines.known_idx<=t)]
        if len(rr):selected=pd.concat([selected,rr],ignore_index=True)
    selected=selected.drop_duplicates('line_id').head(13)
    for _,l in selected.iterrows():
        focus=any(code0==code and f==l.family and a==l.a1 and b==l.a2 for code0,f,a,b in USER_REFERENCE_LINES)
        left=max(st,int(l.known_idx));right=end
        if left>right:continue
        xs=np.arange(left,right+1);ys=[line_value(l,j) for j in xs]
        ax.plot(xs,ys,color=('#e79520' if l.kind=='UPPER' else '#169369') if focus else '#777777',
            ls='-' if focus else ':',lw=2.2 if focus else .9,alpha=.96 if focus else .45,
            label=('사용자 기준 ' if focus else '')+f'{l.family} {l.a1[5:]}→{l.a2[5:]}')
        if focus:ax.scatter([int(l.known_idx)],[line_value(l,int(l.known_idx))],s=20,color='#111111',zorder=4)
    tx=tri[(tri.idx<=t)&(tri.upper_a2<=date)&(tri.lower_a2<=date)].copy() if len(tri) else pd.DataFrame()
    # Selected triangle geometry only, never a posthoc best-return line.
    if len(tx):
        tx=tx[tx.apex_index_estimate>=t].sort_values(['width_pct_price','idx']).head(1)
        if len(tx):
            apex=tx.iloc[0].apex_index_estimate
            if t<=apex<=end:ax.axvline(apex,color='#9333ea',ls=':',alpha=.7,label='수렴 꼭짓점 예상')
    ax.axvline(t,color='black',ls='--',label='기준일')
    ax.axvspan(t,end,color='#fbbf24',alpha=.06)
    ax.grid(alpha=.12);bx.grid(alpha=.12)
    ax.legend(fontsize=6,ncol=3,loc='upper left');ax.set_ylabel('원');bx.set_ylabel('거래대금(억원)')
    tk=np.linspace(st,end,min(9,end-st+1),dtype=int);bx.set_xticks(tk);bx.set_xticklabels(ts.iloc[tk].str[5:],rotation=20)
    fig.suptitle(f'R1-D107 | {name} {date} | 의미 있는 추세선·수렴·원봉 검증 (사후 음영)',fontsize=13)
    p=output/'charts'/f'{code}_{date}_R1D107_QUALITY.png'
    fig.savefig(p,dpi=130,bbox_inches='tight');plt.close(fig)
    return p.name


def run(output=None, base=R106, raw_provider=None, plot=True,
        case_refs=None, user_refs=None):
    """Use the exact R106 catalog produced by same GitHub workflow; never change it."""
    case_refs=CASES if case_refs is None else case_refs
    user_refs=USER_REFERENCE_LINES if user_refs is None else user_refs
    output=Path(output or HERE/'results');output.mkdir(parents=True,exist_ok=True);(output/'charts').mkdir(exist_ok=True)
    base=Path(base)
    paths={k:base/k for k in ('R1D106_ALL_IMMUTABLE_LINES.csv','R1D106_ALL_TRIANGLE_CANDIDATES_PIT.csv')}
    for k,p in paths.items():
        if not p.is_file():raise FileNotFoundError('Run R1-D106 first: '+str(p))
    upstream_hash={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in paths.items()}
    all_lines=pd.read_csv(paths['R1D106_ALL_IMMUTABLE_LINES.csv'],dtype={'code':str})
    all_triangles=pd.read_csv(paths['R1D106_ALL_TRIANGLE_CANDIDATES_PIT.csv'],dtype={'code':str})
    all_lines.code=all_lines.code.str.zfill(6);all_triangles.code=all_triangles.code.str.zfill(6)
    if all_lines.line_id.duplicated().any():raise AssertionError('R106 duplicated original line id')
    if raw_provider is None:
        from research_r1d96 import load as raw_provider
        from research_r1d104 import _verify
        _verify()
    scored_lines=[];rankings=[];scan_selection=[];scored_triangles=[];expost=[];examples=[];visual=[];audits=[]
    for code,lines in all_lines.groupby('code',sort=True):
        d=raw_provider(code).copy();d['date']=pd.to_datetime(d.date);d=d.sort_values('date').reset_index(drop=True)
        d=enrich_asof(d)
        tn=all_triangles[all_triangles.code.eq(code)].copy()
        ln=lines.sort_values('known_idx').copy()
        byid={r['line_id']:r for r in ln.to_dict('records')}
        # ALL lines evaluated ONCE on their own historically-known date.
        # This is an append-only diagnostic overlay, not a replacement catalog.
        entries=[]
        for l in ln.to_dict('records'):
            q=line_quality(d,l,int(l['known_idx']));q.update({'code':code,'name':l['name']})
            entries.append(q)
        scored_lines.extend(entries)
        # All triangle candidates evaluated at first jointly known date (not ex-post).
        for row in tn.to_dict('records'):
            v=triangle_quality(d,row,byid,int(row['idx']))
            v['code']=code;scored_triangles.append(v)
        # Chronological every-10-bar monitor, never selects dates by later returns.
        # Full raw lines remain in R106. Store only display representatives.
        for t_scan in range(550,len(d)-20,10):
            px_scan=float(d.iloc[t_scan].close)
            active_scan=ln[(ln.known_idx<=t_scan)&(ln.known_idx>=t_scan-35)]
            if active_scan.empty:continue
            # Diagnostic screen only; no lines are removed from original history.
            scan_rows=[]
            for candidate in active_scan.to_dict('records'):
                level=line_value(candidate,t_scan)
                if level>0 and abs(px_scan/level-1)<=.12:
                    scan_rows.append(line_quality(d,candidate,t_scan))
            if not scan_rows:continue
            grouped=display_groups(pd.DataFrame(scan_rows))
            winners=grouped[grouped.is_display_representative].copy()
            if winners.empty:continue
            winners.insert(0,'code',code);winners.insert(1,'name',ln.iloc[0]['name'])
            winners['reference_date']=str(d.iloc[t_scan].date.date())
            for k,v in _snapshot(d,t_scan).items():
                if k not in winners:winners[k]=v
            scan_selection.append(winners)
        case_dates=[c for c in case_refs if c[0]==code]
        for case in case_dates:
            _,name,asof,kind=case
            hit=np.flatnonzero(d.date.dt.strftime('%Y-%m-%d').eq(asof))
            if len(hit)!=1:raise AssertionError('missing reference date '+str(case))
            t=int(hit[0]);px=float(d.iloc[t].close)
            active=ln[(ln.known_idx<=t)&(ln.i2<=t)&(ln.known_idx>=t-45)].copy()
            # Broad enough to include references but candidate filters use no outcomes.
            active=active[active.apply(lambda l:line_value(l,t)>0 and abs(100*(px/line_value(l,t)-1))<=25,axis=1)]
            q=pd.DataFrame([line_quality(d,l,t) for l in active.to_dict('records')])
            if len(q):
                grouping=display_groups(q);grouping.insert(0,'code',code)
                grouping.insert(1,'name',name);grouping['reference_date']=asof;rankings.append(grouping)
            else:grouping=pd.DataFrame()
            hist=_snapshot(d,t);hist.update({'code':code,'name':name,'reference_date':asof,'case_kind':kind,
                'reference_observation_only':True,'no_future_used':True})
            # Explicit audit: user proposals do not force quality rank or candidate selection.
            for cc,f,a,b in user_refs:
                if cc!=code:continue
                r=ln[(ln.family==f)&(ln.a1==a)&(ln.a2==b)]
                if len(r)!=1:raise AssertionError('manual example did not match exactly one R106 line '+str((cc,f,a,b)))
                line=r.iloc[0].to_dict();first_known=int(line['known_idx'])
                record={'code':code,'name':name,'reference_date':asof,
                  'manual_check_only':True,'family':f,'anchor1':a,'anchor2':b,
                  'first_known_date':line['known_date'],'available_on_reference':first_known<=t,
                  'was_automatically_discovered_in_R106':True,'line_id':line['line_id']}
                if first_known<=t:
                    qa=line_quality(d,line,t)
                    record.update({k:qa[k] for k in ('line_price','close_to_line_pct','quality_diagnostic_0_100','readiness','post_confirmation_respected_contacts')})
                    if len(grouping):
                        qq=grouping[grouping.line_id.eq(line['line_id'])]
                        record['automatic_display_rank']=int(qq.iloc[0].display_rank) if len(qq) else None
                        record['automatically_selected_for_display']=bool(qq.iloc[0].is_display_representative) if len(qq) else False
                else:
                    record['line_price']=None;record['automatic_display_rank']=None
                examples.append(record)
            case_tri=tn[tn.idx<=t].copy()
            # Re-evaluate only when both lines were known AND before/near apex.
            case_tri=case_tri[(case_tri.apex_index_estimate>=t-1)&(case_tri.idx>=t-24)]
            relevant=[]
            for tr in case_tri.to_dict('records'):
                v=triangle_quality(d,tr,byid,t)
                if v['width_pct_close']<=20:relevant.append(v)
            if relevant:
                rd=pd.DataFrame(relevant)
                hist['triangle_count_geometry_candidate_asof']=len(relevant)
                hist['triangle_inside_active_asof']=int(rd.state_PIT.eq('ACTIVE_INSIDE').sum())
                hist['triangle_pit_upbreak_asof']=int(rd.state_PIT.eq('PIT_UP_BREAK_WITH_PRIOR_INSIDE').sum())
                hist['triangle_expired_or_apex_near_asof']=int(rd.state_PIT.isin(['APEX_PASSED_OR_INVALID','APEX_TOO_CLOSE_FOR_NEW_BREAKOUT']).sum())
            else:hist.update({'triangle_count_geometry_candidate_asof':0,'triangle_inside_active_asof':0,'triangle_pit_upbreak_asof':0,'triangle_expired_or_apex_near_asof':0})
            if t+20<len(d):hist['forward20_close_pct_EXPOST']=round(100*(float(d.iloc[t+20].close)/px-1),3)
            examples.append(hist)
            if plot:visual.append(_plot(d,case,grouping,ln,tn,output))
        # Triangle status series at first-known and controlled follow-up indices.
        # Future values are never used to *select* candidates.
        for row in tn.to_dict('records'):
            t0=int(row['idx']);apex=float(row['apex_index_estimate'])
            # Only a finite number of diagnostic asof points; include near-apex but not post-apex.
            at={t0+off for off in (1,2,3,5,10,15,20) if t0+off<len(d) and t0+off<=apex}
            for t in sorted(at):
                v=triangle_quality(d,row,byid,t)
                if v['state_PIT']=='PIT_UP_BREAK_WITH_PRIOR_INSIDE':
                    v.update(_snapshot(d,t));v['code']=code;v['name']=ln.iloc[0]['name']
                    scored_triangles.append(v)
                    post=apex_limited_outcome(d,row,byid,t)
                    expost.append({'code':code,'name':ln.iloc[0]['name'],'date':v['asof'],
                                   'upper_id':row['upper_id'],'lower_id':row['lower_id'],
                                   'state_at_signal_PIT':v['state_PIT'],**post})
        audits.append({'code':code,'lines_in_R106':len(ln),'candidate_triangles_in_R106':len(tn),
                       'lines_quality_snapshot':len(entries),'raw_source_rows':len(d)})
        print('CASE_DONE',code,'lines',len(ln),'triangles',len(tn),'PIT_DIAGNOSTICS_PASS',flush=True)
    def export(rows,file):
        x=rows.copy() if isinstance(rows,pd.DataFrame) else pd.DataFrame(rows)
        x.to_csv(output/file,index=False,encoding='utf-8-sig');return x
    scored=export(scored_lines,'R1D107_ALL_LINE_CREATION_QUALITY_PIT.csv')
    ranked=export(pd.concat(rankings,ignore_index=True) if rankings else pd.DataFrame(),
                  'R1D107_CASE_DISPLAY_GROUPS_PIT.csv')
    scan=export(pd.concat(scan_selection,ignore_index=True) if scan_selection else pd.DataFrame(),
                'R1D107_CHRONOLOGICAL_10BAR_TOP_LINES_PIT.csv')
    tr=export(scored_triangles,'R1D107_TRIANGLE_READINESS_PIT.csv')
    breakout=tr[tr.state_PIT.eq('PIT_UP_BREAK_WITH_PRIOR_INSIDE')].copy() if len(tr) else pd.DataFrame()
    export(breakout,'R1D107_TRIANGLE_CANDIDATE_BREAKOUTS_PIT.csv')
    export(expost,'R1D107_TRIANGLE_FOLLOWTHROUGH_EXPOST.csv')
    ex=pd.DataFrame(examples)
    export(ex[ex.manual_check_only.eq(True)] if 'manual_check_only' in ex else [],'R1D107_MANUAL_REFERENCE_LINES_AUDIT.csv')
    export(ex[ex.reference_observation_only.eq(True)] if 'reference_observation_only' in ex else [],'R1D107_SIX_CONTEXT_REFERENCE_SNAPSHOTS.csv')
    # Preserve original R106 input files: verify byte identity after all processing.
    for k,p in paths.items():
        if hashlib.sha256(p.read_bytes()).hexdigest()!=upstream_hash[k]:raise AssertionError('R106 upstream changed')
    report={'version':'R1D107_QUALITY_LINE_TRIANGLE_APEX_PIT_RESEARCH_ONLY','source':'R1D106 immutable catalogs + R1D97 frozen OHLCV',
            'input_hashes_verified_unchanged':True,'input_hashes_sha256':upstream_hash,
            'stocks':audits,'original_line_count':len(all_lines),'line_creation_quality_rows':len(scored),
            'original_triangle_count':len(all_triangles),'triangle_readiness_rows':len(tr),
            'pit_validated_breakout_rows':len(breakout),'expost_breakout_rows':len(expost),
            'context_case_count':len(case_refs),'chronological_display_rows':len(scan),'images':[v for v in visual if v],
            'full_market_oos':False,'no_outcome_optimized_thresholds':True,
            'limitations':['Three stocks, overlapping dates, raw non-adjusted corporate actions not fully verified',
              'The point scores measure descriptive geometry, not forecast success',
              'A triangle formed on breakout day cannot be assumed known in advance',
              'User example lines kept separate from automatic display ranks',
              'Post-apex extrapolation is not valid triangle support',
              'Only 1/2/3/5/10/15/20-bar after-known offsets evaluated for triangle later-state audit',
              'PIT/EXPOST separated; trade profitability not established']}
    (output/'R1D107_AUDIT.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
    print('SUMMARY',json.dumps({k:report[k] for k in ('original_line_count','original_triangle_count','triangle_readiness_rows','pit_validated_breakout_rows','context_case_count','chronological_display_rows','input_hashes_verified_unchanged')},ensure_ascii=False),flush=True)
    return report

if __name__=='__main__':run()
