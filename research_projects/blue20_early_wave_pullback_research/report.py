from pathlib import Path
from html import escape
import argparse,json,os
import numpy as np
import pandas as pd
from study import HERE,load_prices,normalize_marcap,indicators,resistance,FEATURES
CASES={('028300','2023-02-20'):'HLB',('145990','2023-05-08'):'삼양사',('383220','2024-07-17'):'F&F',('180640','2023-09-14'):'한진칼',('006220','2024-01-24'):'제주은행',('222080','2024-03-05'):'씨아이에스',('038500','2024-06-05'):'삼표시멘트',('034120','2023-02-07'):'SBS'}
def table(d):
    lines=['|'+'|'.join(d.columns)+'|','|'+'|'.join(['---']*len(d.columns))+'|']
    for row in d.itertuples(index=False,name=None):lines.append('|'+'|'.join('—' if pd.isna(v) else f'{v:.3f}' if isinstance(v,(float,np.floating)) else str(v) for v in row)+'|')
    return '\n'.join(lines)
def stats(t):
    a=t[t.closed];return dict(signals=len(t),entries=int(t.entered.sum()),closed=len(a),entry_pct=100*t.entered.mean(),mean_net=a.net_pct.mean(),median_net=a.net_pct.median(),tp_pct=100*a.reason.str.startswith('TP').mean(),sl_pct=100*a.reason.str.contains('SL').mean(),unresolved=len(t)-len(a))

def gallery(manifest,signals,trades,episodes,out):
    frames=load_prices(Path(manifest));warm,_=normalize_marcap(pd.read_parquet(HERE/'cache/marcap-2021.parquet'),2021)
    selected=[dict(code=c,date=d,label=n+' / 기존 기준일',kind='REFERENCE') for (c,d),n in CASES.items()]
    # Outcome-based example selection is explanatory only; no rule thresholds use these labels.
    for year in [2023,2024]:
        for label,part in [('P0 미발전',episodes[episodes.origin_stage.eq('P0')&episodes.final_status.eq('NO_P1_20D')]),('눌림 실패',episodes[episodes.final_status.eq('P2_BASE_LOW_FAILED')]),('재상승 도달',episodes[episodes.origin_stage.eq('P1')&episodes.final_status.eq('P3_REACHED')])]:
            p=part[part.origin_date.str.startswith(str(year))].sort_values(['origin_date','code'])
            if len(p):r=p.iloc[0];selected.append(dict(code=r.code,date=r.origin_date,label=f'{year} {label} / 날짜순 첫 예시',kind='ILLUSTRATIVE'))
        p=trades[trades.stage.eq('P3')&trades.closed&trades.reason.str.contains('SL',na=False)&trades.year.eq(year)].sort_values(['date','code'])
        if len(p):r=p.iloc[0];selected.append(dict(code=r.code,date=r.date,label=f'{year} 재상승 후 손절 / 날짜순 첫 예시',kind='ILLUSTRATIVE'))
    unique={(r['code'],r['date']):r for r in selected};selected=list(unique.values())
    page=['<!doctype html><html lang="ko"><meta charset="utf-8"><title>BLUE20 P0~P3 연구</title><style>body{font:16px system-ui;max-width:1340px;margin:auto;padding:22px;color:#19273c}svg{width:100%;background:#fafbfc;border:1px solid #ddd}table{border-collapse:collapse;font-size:13px}td,th{border:1px solid #ddd;padding:5px}section{margin:48px 0}.note{background:#fff6da;padding:12px}</style>',
    '<h1>BLUE20 초기 관심·첫 파동·눌림·재상승</h1><p class="note">실제 원천 일봉. 검은 세로선은 기준일, 오른쪽 음영은 기준일에 아직 몰랐던 후속 가격입니다. P0~P3는 각 날짜 종가 이후의 관측이며 당일 시가 진입이 아닙니다. 거래량 감소는 매집·매물 소화 인증이 아닙니다. 저항 존은 기준일 전일까지 확인된 가격 반락 증거입니다.</p>',
    '<p>MA60 초록 / MA112 회색 / MA224 갈색 / 별도 MA448 보라. BLUE20 UPPER 주황·LOWER 파랑. P0 보라·P1 빨강·P2 청록·P3 검정. 기준 사례 외 추가 예시는 결과 유형별 날짜순 첫 사례로, 규칙 개발에 사용하지 않았습니다.</p>']
    base=pd.read_csv(HERE/'reference/BASELINE.csv.gz',dtype={'code':str});assessment=[]
    calendar={v:i for i,v in enumerate(sorted(set(v for d in frames.values() for v in d.date)))}
    for sel in selected:
        code,date=sel['code'],sel['date'];raw=frames[code];pre=warm[warm.code.eq(code)][['date','Close']].rename(columns={'Close':'close'})
        ext=pd.concat([pre,raw[['date','close']]],ignore_index=True).sort_values('date');ext['ma448']=ext.close.rolling(448).mean();d=indicators(raw,ext.set_index('date').ma448)
        j=int(d.index[d.date.eq(date)][0]);start=max(0,j-90);end=min(len(d),j+61);w=d.iloc[start:end].reset_index(drop=True);r,zs=resistance(d,j)
        lo=w.low.min();hi=w.high.max()
        # Keep the actual 448 comparison line visible without filling missing history.
        for col in ['ma60','ma112','ma224','ma448','UPPER','LOWER']:
            if w[col].notna().any():lo=min(lo,w[col].min());hi=max(hi,w[col].max())
        if zs:lo=min(lo,min(z['low'] for z in zs));hi=max(hi,max(z['high'] for z in zs))
        span=hi-lo;y=lambda p:365-310*(p-lo)/span;x=lambda k:60+1120*k/(len(w)-1)
        svg=[f'<svg viewBox="0 0 1260 600"><rect x="{x(j-start)}" y="35" width="{1180-x(j-start)}" height="530" fill="#e5e7eb" opacity=".6"/><text x="{x(j-start)+5}" y="25">사후 관측 영역</text>']
        for z in zs:
            color='#de9d39' if z['position']=='ABOVE' else '#359c77'
            svg.append(f'<rect x="60" y="{y(z["high"])}" width="1120" height="{y(z["low"])-y(z["high"])}" fill="{color}" opacity=".08"><title>D−1 가격 저항 {z["low"]:.0f}~{z["high"]:.0f}, {z["tests"]}회, {z["position"]}</title></rect>')
        for col,color in [('ma60','#2d9555'),('ma112','#777'),('ma224','#8c5e37'),('ma448','#9b39a6'),('UPPER','#ef8c13'),('LOWER','#2968c8')]:
            path=[]
            for k,b in w.iterrows():
                if pd.notna(b[col]):path.append(('M' if not path else 'L')+f'{x(k):.2f},{y(b[col]):.2f}')
            if path:svg.append(f'<path d="{" ".join(path)}" fill="none" stroke="{color}" stroke-width="1.6" stroke-dasharray="{ "4 4" if col in ["UPPER","LOWER"] else "none"}"/>')
        for k,b in w.iterrows():
            color='#c44' if b.close>=b.open else '#2674b5';xx=x(k)
            svg.append(f'<g><title>{b.date} O={b.open} H={b.high} L={b.low} C={b.close} V={b.volume} A={b.amount}</title><path d="M{xx} {y(b.high)}V{y(b.low)}" stroke="{color}"/><rect x="{xx-2}" y="{y(max(b.open,b.close))}" width="4" height="{max(1,abs(y(b.open)-y(b.close)))}" fill="{color}"/></g>')
            svg.append(f'<rect x="{xx-2}" y="{455-60*b.volume/w.volume.max()}" width="4" height="{60*b.volume/w.volume.max()}" fill="{color}" opacity=".6"/><rect x="{xx-2}" y="{555-65*b.amount/w.amount.max()}" width="4" height="{65*b.amount/w.amount.max()}" fill="{color}" opacity=".6"/>')
        visible=signals[signals.code.eq(code)&signals.date.between(w.date.iloc[0],w.date.iloc[-1])]
        for s in visible.itertuples():
            k=int(w.index[w.date.eq(s.date)][0]);color={'P0':'#914bb0','P1':'#bd3b3b','P2':'#148e92','P3':'#222'}[s.stage]
            svg.append(f'<circle cx="{x(k)}" cy="{y(w.high.iloc[k])-7}" r="4" fill="{color}"><title>{s.stage} {s.date}; 종가 후 확정 / P1={s.p1_date}</title></circle><text x="{x(k)+3}" y="{y(w.high.iloc[k])-10}" font-size="10" fill="{color}">{s.stage}</text>')
        bt=base[base.code.eq(code)&base.date.eq(date)]
        for b in bt.itertuples():
            for day,price,label in [(b.entry_date,b.baseline_entry_price,'기존 진입'),(b.baseline_exit_date,raw.close.iloc[0] if pd.isna(b.baseline_exit_date) else None,'기존 청산')]:
                if day in set(w.date):
                    k=int(w.index[w.date.eq(day)][0]);svg.append(f'<path d="M{x(k)} 40V370" stroke="#8548a6" stroke-dasharray="2 4"/><text x="{x(k)+3}" y="{390 if label=="기존 진입" else 377}" font-size="10">{label}</text>')
        for tr in trades[trades.signal_id.isin(visible.signal_id)&trades.closed].itertuples():
            for day,px,label in [(tr.entry_date,tr.entry_price,'E'),(tr.exit_date,tr.exit_price,'X')]:
                if day in set(w.date):
                    k=int(w.index[w.date.eq(day)][0]);svg.append(f'<circle cx="{x(k)}" cy="{y(px)}" r="3" fill="#222"><title>{tr.stage} {label} {day} {px}</title></circle>')
        svg.append(f'<path d="M{x(j-start)} 35V565" stroke="black"/><text x="60" y="385">거래량</text><text x="60" y="480">거래대금</text><text x="60" y="585">{w.date.iloc[0]} → {w.date.iloc[-1]}</text>')
        for p in np.linspace(lo,hi,5):svg.append(f'<text x="4" y="{y(p)}" font-size="11">{p:,.0f}</text>')
        svg.append('</svg>')
        page.extend([f'<section><h2>{escape(sel["label"])} {code} {date}</h2>',''.join(svg),f'<p>강한 상단 가격 저항 증거: {r["strong_overhead"]} / 목표 +10% 사이 후보: {r.get("target_zone_count","미확인")} / 448일선: {"UNKNOWN" if pd.isna(d.ma448.iloc[j]) else "관측 가능"}</p>'])
        cols=['date','stage','p1_date','rsi_gc','slow_gc','strong_overhead'];page.append(visible[cols].to_html(index=False))
        if sel['kind']=='REFERENCE':
            p1=episodes[episodes.code.eq(code)&episodes.origin_stage.eq('P1')&episodes.origin_date.eq(date)]
            prior=signals[signals.code.eq(code)&signals.stage.eq('P0')&signals.session.between(calendar[date]-20,calendar[date]-1)]
            row=dict(code=code,date=date,name=CASES[(code,date)],p0_prior20=len(prior)>0,p1_same_day=len(p1)>0,p2_date=p1.p2_date.iloc[0] if len(p1) else '',p3_date=p1.p3_date.iloc[0] if len(p1) else '',episode_status=p1.final_status.iloc[0] if len(p1) else 'NOT_P1_BY_R0',strong_overhead=r['strong_overhead'],repeat_overhead=r['repeat_overhead'],repeat_zone_inside=any(z['position']=='INSIDE' for z in zs),nearest_room_pct=r.get('resistance_room_pct',np.nan))
            assessment.append(row);page.append(pd.DataFrame([row]).to_html(index=False))
        page.append('</section>')
    page.append('</html>');(out/'GALLERY.html').write_text('\n'.join(page),encoding='utf-8');pd.DataFrame(assessment).to_csv(out/'EIGHT_CASE_ASSESSMENT.csv',index=False);pd.DataFrame(selected).to_csv(out/'GALLERY_SELECTION.csv',index=False)
    return len(selected)

def main(manifest):
    out=HERE/'results';s=pd.read_csv(out/'SIGNALS.csv.gz',dtype={'code':str});t=pd.read_csv(out/'TRADES.csv.gz',dtype={'code':str});e=pd.read_csv(out/'EPISODES.csv.gz',dtype={'code':str});op=pd.read_csv(out/'P1_SAME_OPPORTUNITIES.csv.gz',dtype={'code':str});b=pd.read_csv(out/'BASELINE_IDENTICAL_OPPORTUNITIES.csv.gz',dtype={'code':str})
    evidence=pd.read_csv(out/'VOLUME_RESISTANCE_EVIDENCE.csv.gz',dtype={'code':str})
    pricezones=pd.read_csv(out/'PRICE_RESISTANCE_ZONES.csv.gz',dtype={'code':str})
    strong=pricezones[pricezones.tests.ge(3)&pricezones.median_drop_atr.ge(1)]
    inside=set(strong[strong.position.eq('INSIDE')].signal_id);below=set(strong[strong.position.eq('BELOW')].signal_id)
    evidence['price_strong_inside']=evidence.signal_id.isin(inside)
    evidence['price_strong_historical_below']=evidence.signal_id.isin(below)
    repeat_inside=set(pricezones[pricezones.position.eq('INSIDE')].signal_id)
    repeat_above=set(pricezones[pricezones.position.eq('ABOVE')].signal_id)
    evidence['price_repeat_inside']=evidence.signal_id.isin(repeat_inside)
    evidence['price_repeat_above']=evidence.signal_id.isin(repeat_above)
    evidence.loc[evidence.price_strong_inside,'overall_evidence']='PRESENT'
    evidence['any_resistance_candidate']=np.where(evidence.price_repeat_inside|evidence.price_repeat_above|evidence.PAST_ONLY_volume_evidence.eq('MODEL_STABLE_OVERHEAD_CANDIDATE'),'CANDIDATE_PRESENT_NOT_CERTIFIED','UNKNOWN_OR_NOT_DETECTED')
    evidence.to_csv(out/'VOLUME_RESISTANCE_EVIDENCE.csv.gz',index=False)
    geometry=[];zone_map={sid:g for sid,g in pricezones.groupby('signal_id')};trade_map=t.set_index('signal_id')
    for r in s.itertuples():
        tr=trade_map.loc[r.signal_id];record=dict(signal_id=r.signal_id,code=r.code,date=r.date,stage=r.stage,signal_known_at='DAY_END_ASSUMED_PUBLICATION_UNVERIFIED',
            signal_close_target_zone_count=r.target_zone_count,dist_ma112_pct=100*(r.close/r.ma112-1) if pd.notna(r.ma112) else np.nan,
            dist_ma224_pct=100*(r.close/r.ma224-1) if pd.notna(r.ma224) else np.nan,dist_BLUE20_lower_pct=100*(r.close/r.LOWER-1) if pd.notna(r.LOWER) else np.nan,
            close_above_ma112=bool(r.close>=r.ma112) if pd.notna(r.ma112) else None,close_above_BLUE20_lower=bool(r.close>=r.LOWER) if pd.notna(r.LOWER) else None,
            low_holds_reference_open=bool(r.low>=r.reference_open) if pd.notna(r.reference_open) else None,low_holds_reference_mid=bool(r.low>=r.reference_mid) if pd.notna(r.reference_mid) else None,
            low_holds_reference_low=bool(r.low>=r.reference_low) if pd.notna(r.reference_low) else None)
        if pd.notna(tr.entry_price):
            zz=zone_map.get(r.signal_id,pricezones.iloc[:0]);record.update(entry_date=tr.entry_date,entry_gap_pct=100*(tr.entry_price/r.close-1),entry_target_zone_count=int((zz.high.ge(tr.entry_price)&zz.low.le(tr.entry_price*1.1)).sum()),entry_audit_known_at='NEXT_SESSION_OPEN_NOT_D0_FEATURE')
        geometry.append(record)
    geom=pd.DataFrame(geometry);geom.to_csv(out/'SUPPORT_AND_ENTRY_GEOMETRY.csv.gz',index=False)
    gs=t.merge(geom,on=['signal_id','code','date','stage'],validate='one_to_one',suffixes=('','_audit'));support_groups=[]
    for axis in ['low_holds_reference_open','low_holds_reference_mid','close_above_ma112','close_above_BLUE20_lower']:
        for key,g in gs[gs.stage.isin(['P2','P3'])&gs.closed&gs[axis].notna()].groupby(['year','stage',axis]):support_groups.append(dict(axis=axis,year=key[0],stage=key[1],value=key[2],n=len(g),mean_net=g.net_pct.mean()))
    pd.DataFrame(support_groups).to_csv(out/'SUPPORT_OBSERVATION_GROUPS.csv',index=False)
    market=pd.DataFrame({'date':sorted(s.date.unique())})
    for index in ['KOSPI','KOSDAQ']:
        x=pd.concat([pd.read_csv(HERE/'reference'/f'{index}_{y}.csv') for y in [2022,2023,2024]]).sort_values('Date');ma=x.Close.rolling(60).mean();x[index+'_regime']=np.where(ma.isna(),'UNKNOWN',np.where(x.Close>=ma,'ABOVE_MA60','BELOW_MA60'));market=market.merge(x[['Date',index+'_regime']].rename(columns={'Date':'date'}),on='date',how='left')
    enriched=t.merge(s[['signal_id','ma_alignment','video_alignment','rsi_gc','slow_gc','strong_overhead','has_known_source_issue']],on='signal_id',validate='one_to_one').merge(market,on='date',how='left')
    summaries=[]
    for subset,g in [('ALL',enriched),('NONOVERLAP60',enriched[enriched.nonoverlap60]),('NO_LARGE_GAP',enriched[~enriched.quality_gap_through20]),('KNOWN_ISSUES_EXCLUDED',enriched[~enriched.has_known_source_issue])]:
        for key,z in g.groupby(['year','stage']):summaries.append(dict(subset=subset,year=key[0],stage=key[1],**stats(z)))
    summary=pd.DataFrame(summaries);summary.to_csv(out/'PERFORMANCE_SUMMARY.csv',index=False)
    stability=[]
    for axis in ['ma_alignment','video_alignment','rsi_gc','slow_gc','strong_overhead','KOSPI_regime','KOSDAQ_regime']:
        for key,z in enriched.groupby(['year','stage',axis]):stability.append(dict(axis=axis,year=key[0],stage=key[1],value=key[2],**stats(z)))
    pd.DataFrame(stability).to_csv(out/'STABILITY_GROUPS.csv',index=False)
    ep=e.copy();ep['year']=ep.origin_date.str[:4].astype(int);funnel=ep.groupby(['year','origin_stage','final_status']).size().rename('n').reset_index();funnel.to_csv(out/'FUNNEL.csv',index=False)
    path=[]
    for (year,stage),g in s.groupby(['year','stage']):
        for h in [5,10,20,40,60]:
            a=g[g[f'coverage_{h}'].eq('COMPLETE')];path.append(dict(year=year,stage=stage,horizon=h,signals=len(g),complete=len(a),mean_return=a[f'return_{h}'].mean(),median_return=a[f'return_{h}'].median(),mean_max_up=a[f'max_up_{h}'].mean(),mean_max_down=a[f'max_down_{h}'].mean()))
    pd.DataFrame(path).to_csv(out/'FORWARD_PATHS.csv',index=False)
    p1flags=s[s.stage.eq('P1')][['signal_id','nonoverlap60','quality_gap_through20']].rename(columns={'signal_id':'opportunity_id'});op=op.merge(p1flags,on='opportunity_id',validate='many_to_one')
    ops=[]
    for subset,g in [('ALL',op),('NONOVERLAP60',op[op.nonoverlap60]),('NO_LARGE_GAP',op[~op.quality_gap_through20])]:
        for (year,policy),z in g.groupby(['year','policy']):
            a=z[z.closed];valid=z[z.path_coverage.eq('COMPLETE')];ops.append(dict(subset=subset,year=year,policy=policy,opportunities=len(z),common_complete_opportunities=len(valid),entries=int(z.entered.sum()),closed=len(a),entry_pct=100*z.entered.mean(),mean_trade_net=a.net_pct.mean(),mean_opportunity_net=valid.opportunity_net_cash.mean(),net_unknown=int(valid.opportunity_net_cash.isna().sum()),missed10=int(z.missed10.sum()),missed20=int(z.missed20.sum())))
    osummary=pd.DataFrame(ops);osummary.to_csv(out/'OPPORTUNITY_SUMMARY.csv',index=False)
    bs=[]
    for key,z in b.groupby(['year','band','gc_offset','policy']):
        a=z[z.closed];bs.append(dict(zip(['year','band','gc_offset','policy'],key),opportunities=len(z),p1_matches=int(z.has_p1_same_day.sum()),entered=int(z.entered.sum()),baseline_net=z.baseline_net.mean(),stage_net=a.net_pct.mean(),opportunity_net=z.net_if_cash_else_zero.mean(),missed10=int(z.missed10.sum()),missed20=int(z.missed20.sum())))
    pd.DataFrame(bs).to_csv(out/'BASELINE_OPPORTUNITY_SUMMARY.csv',index=False)
    b['rsi_confirmation']=np.where(b.gc_offset.eq(0),'GC0','GC_DELAY_1_10');rsi_rows=[]
    for key,g in b.groupby(['year','band','rsi_confirmation','policy']):
        a=g[g.closed];rsi_rows.append(dict(zip(['year','band','rsi_confirmation','policy'],key),opportunities=len(g),entered=int(g.entered.sum()),baseline_net=g.baseline_net.mean(),stage_trade_net=a.net_pct.mean(),opportunity_net=g.net_if_cash_else_zero.mean(),missed10=int(g.missed10.sum()),missed20=int(g.missed20.sum())))
    pd.DataFrame(rsi_rows).to_csv(out/'RSI_CONFIRMATION_GROUPS.csv',index=False)
    # P0 lead to P1: label-based measurement, not an input to P0.
    p0=e[e.origin_stage.eq('P0')].copy();p0['year']=p0.origin_date.str[:4];p0summary=[]
    for year,z in p0.groupby('year'):
        reached=z.final_status.eq('P1_REACHED');known=z.final_status.isin(['P1_REACHED','NO_P1_20D'])
        p0summary.append(dict(year=year,p0=len(z),p1_reached=int(reached.sum()),no_p1=int(z.final_status.eq('NO_P1_20D').sum()),censored_or_missing=int((~known).sum()),reached_pct_among_known=100*reached.sum()/known.sum() if known.sum() else np.nan,median_days_to_p1=z.loc[reached,'days_to_p1'].median()))
    pd.DataFrame(p0summary).to_csv(out/'P0_REACH.csv',index=False)
    chains=pd.read_csv(out/'P0_CHAIN_OUTCOMES.csv.gz',dtype={'code':str});chains['year']=chains.origin_date.str[:4];chain_summary=[]
    for year,g in chains.groupby('year'):chain_summary.append(dict(year=year,p0=len(g),p1=int(g.p1_date.notna().sum()),p2=int(g.p2_date.notna().sum()),p3=int(g.p3_date.notna().sum()),p3_fraction_pct=100*g.p3_date.notna().mean()))
    pd.DataFrame(chain_summary).to_csv(out/'P0_CHAIN_SUMMARY.csv',index=False)
    lead=pd.read_csv(out/'LEAD_COMPARISONS.csv.gz',dtype={'code':str});lead_summary=[]
    all_days=pd.read_csv(out/'ALL_STOCK_DAYS.csv.gz',dtype={'code':str});stock_sessions={code:g.session.to_numpy() for code,g in all_days.groupby('code')}
    coverage_grid=pd.MultiIndex.from_product([sorted(all_days.code.unique()),sorted(all_days.date.unique())],names=['code','date']).to_frame(index=False)
    coverage_grid=coverage_grid.merge(all_days[['code','date','p0_condition','p1_raw','ma_alignment']],on=['code','date'],how='left',validate='one_to_one')
    coverage_grid['ohlcv_observed']=coverage_grid.ma_alignment.notna()
    coverage_grid['p0_classifiable']=coverage_grid.ohlcv_observed&coverage_grid.ma_alignment.ne('UNKNOWN')
    coverage_grid.loc[~coverage_grid.p0_classifiable,'p0_condition']=pd.NA
    for stage in ['P0','P1','P2','P3']:
        keys=set(map(tuple,s[s.stage.eq(stage)][['code','date']].to_numpy()))
        coverage_grid[stage+'_signal']=pd.Series([(c,date) in keys for c,date in coverage_grid[['code','date']].to_numpy()],dtype='boolean')
        coverage_grid.loc[~coverage_grid.p0_classifiable if stage=='P0' else ~coverage_grid.ohlcv_observed,stage+'_signal']=pd.NA
    coverage_grid.to_csv(out/'FULL_STOCK_DAY_COVERAGE.csv.gz',index=False)
    coverage_status=dict(stock_market_day_rows=len(coverage_grid),observed_ohlcv_rows=int(coverage_grid.ohlcv_observed.sum()),missing_or_nontrading_rows=int((~coverage_grid.ohlcv_observed).sum()),p0_classifiable_rows=int(coverage_grid.p0_classifiable.sum()))
    (out/'COVERAGE_STATUS.json').write_text(json.dumps(coverage_status,indent=2),encoding='utf-8')
    session_by_signal=s.set_index('signal_id').session.to_dict();last_session=all_days.session.max()
    def coverage(r):
        cur=session_by_signal[r.signal_id];a=stock_sessions[r.code]
        if last_session-cur<60:return 'RIGHT_CENSORED'
        return 'COMPLETE' if np.searchsorted(a,cur+60,side='right')-np.searchsorted(a,cur,side='right')==60 else 'MISSING_BARS'
    lead['coverage60']=[coverage(r) for r in lead.itertuples()];lead.to_csv(out/'LEAD_COMPARISONS.csv.gz',index=False)
    for (year,stage),g in lead.groupby(['year','stage']):
        a=g[g.lead_sessions.notna()];lead_summary.append(dict(year=year,stage=stage,n=len(g),next_blue_within60=len(a),median_lead=a.lead_sessions.median(),same_day=int(a.lead_sessions.eq(0).sum()),unresolved_right_censored=int((g.coverage60.eq('RIGHT_CENSORED')&g.lead_sessions.isna()).sum()),missing_coverage=int(g.coverage60.eq('MISSING_BARS').sum())))
    pd.DataFrame(lead_summary).to_csv(out/'LEAD_SUMMARY.csv',index=False)
    count=gallery(manifest,s,t,e,out)
    status=json.loads((out/'STATUS.json').read_text());status['independent_2025_2026']='OHLCV_NOT_ACCESSED_VIDEO_2025_SUCCESS_ALREADY_SEEN';(out/'STATUS.json').write_text(json.dumps(status,ensure_ascii=False,indent=2),encoding='utf-8');cases=pd.read_csv(out/'EIGHT_CASE_ASSESSMENT.csv',dtype={'code':str})
    ev=evidence.groupby(['stage','price_strong_overhead','PAST_ONLY_volume_evidence']).size().rename('n').reset_index();ev.to_csv(out/'RESISTANCE_EVIDENCE_SUMMARY.csv',index=False)
    lines=['# BLUE20 초기 관심→첫 파동→눌림→재상승 R0 연구',
    '기존 목표인 224일선 아래/부근의 준비 과정을 유지하고, P0~P3를 별도 관측 규칙으로 추가했다. 기존 60·112·224, BLUE20·RSI·Slow를 교체하지 않았다. 모든 임계값은 PROTOCOL_KO.md에 사전 고정했으며 이번 결과를 보고 튜닝하지 않았다. 2023~2024년 탐색 결과다.',
    '## 보존·원천 상태',json.dumps(status,ensure_ascii=False,indent=2),
    '기존 1,804건의 BLUE20 해당 밴드 돌파, RSI 확인일, +10%/−5%/비용0.25%p 결과·청산일을 재현했다. 원장 전체는 미래 RSI 관측으로 선택된 기준선이므로 신규 P0를 그 원장에서 역으로 만들지 않았다. 신규 P0는 모든 종목·거래일을 검사했다. ALL_STOCK_DAYS.csv.gz에 비신호 거래일도 남겼다. 2021년 데이터는 별도448일선만 준비하며 기존 RSI 초기화·이평 계산은 유지한다.',
    'FULL_STOCK_DAY_COVERAGE.csv.gz는500종목×전체 관측 시장거래일의 완전한 격자다. 원천에서 유효OHLCV를 관측하지 못한 날을 무신호로 채우지 않고 미확인으로 남겼다. SMA224 이력 부족도 P0 분류불가로 표시한다. 상장/거래정지/기업행사별 원인까지 분류 완료한 것은 아니다.',json.dumps(coverage_status,ensure_ascii=False,indent=2),
    '가격 보정·거래대금 집계 범위·실체결 분포는 여전히 미검증이다. 아래 수익률은 원천 재현용 탐색값이며 기업행사를 가로지르는 경제적 손익을 인증하지 않는다. 갭 단서 제외는 사후 품질 민감도이지 실시간 진입 필터가 아니다. 수익 평균은 거래당 값이며 포트폴리오 수익률이 아니다.',
    '## P0 조기 관심의 실제 전이',table(pd.DataFrame(p0summary)),
    'P0 거짓 경보는20시장거래일 내 P1 미도달로 정의한다. 끝부분 관측부족·중간 원천 봉 누락을 실패와 분리했다. 아직 상승하지 않았다는 사실과 영구적인 투자 실패는 같지 않다. P1에 도달하지 않은 P0도 제거하지 않았다. 초기 시점 검토에서 P0가 P1형 급등과 겹칠 수 있는 경우를 막는 의미상 회귀 검사를 추가했고, 성과 비교 전에 당일·직전20봉 P1형 급등 제외를 명시했다. 수익률을 보고 임계값을 조정한 것은 아니다.',
    '동일한 P0에서 실제로 이어진 전체 사슬은 다음과 같다. 말기 관측부족도 P0 분모에 남아 있으므로 p3_fraction은 독립적으로 완결된 성공확률 인증이 아니다. P0_CHAIN_OUTCOMES에 각 단계·미완료 상태를 보존했다.',table(pd.DataFrame(chain_summary)),
    '## P1 이후 모든 실패·미완료',table(funnel),
    'P1은 성공 파동이 아니라 관측된 큰 양봉이다. P2는 눌림의 초기 후보이며 완성된 바닥이라는 뜻이 아니다. P3는 그날 종가 확인 후 다음 시장 거래일에만 진입할 수 있다. 기준봉 저가 아래 종가가 먼저 나오면 실패로 종료한다. 중간 원천 봉 누락은 불명으로 분리한다.',
    'SUPPORT_AND_ENTRY_GEOMETRY에는 당시 알려진 기준봉 시가·저가·몸통중간 유지, MA112/224·BLUE20 하단까지 거리, 신호 종가 기준 저항 수를 기록했다. 실제 다음 시가~+10% 사이 저항 수는 동결한 사전 존으로 다시 세되 NEXT_SESSION_OPEN_NOT_D0_FEATURE로 구분했다. 다음 날 갭 가격을 전날 매수 판단에 소급하지 않았다. SUPPORT_OBSERVATION_GROUPS는 지지 상태별 기술적 손익 비교이며 새 진입 필터 최적화가 아니다.',
    '## 단계별+20일 보조 거래 결과',table(summary[summary.subset.eq('ALL')][['year','stage','signals','entries','closed','mean_net','tp_pct','sl_pct','unresolved']]),
    '단계별 만기를 다시20일로 잡은 표라서 P2/P3의 시간 여유가 다르다. 이 표만으로 진입 우월성을 비교하지 않는다. signal은 후보 수, entries는 다음 시장 시가 자료가 있어 진입한 수, closed는 평가기간 내 검증 가능한 청산 수다. 미완료·누락은 0% 수익으로 채우지 않는다.',
    '## 동일 P1 기회·동일 P1+20일 만기 주 비교',table(osummary[osummary.subset.eq('ALL')]),
    'P2/P3가 없거나 만기 뒤에 나타나도 같은 원래 P1 기회 분모에 남겼다. mean_opportunity_net은 세 정책에 공통인 P1 이후20일 전체 경로 관측 가능 집합(common_complete_opportunities)의 현금 포함 평균이다. 조기 청산은 알지만 나중 원천 봉이 없어 경로 전체를 검증할 수 없는 사건은 이 평균에서 제외될 수 있어 거래당 평균과 분모가 다르다. 진입하지 않은 현금 상태는 순익0으로 두되 데이터 누락·미완료는 미확인이다. +10/+20 놓친 경로는 원래 P1 다음 시가 기준의 전체 고가 경로이며 실현 가능 익절 성과와 같지 않다. P3가 늦어 P1+20 이후면 주 비교에서 진입하지 않고 단계별+20 표에서만 별도로 평가한다.',
    '## 기존 연구와 비교',
    'BASELINE_IDENTICAL_OPPORTUNITIES.csv.gz는 기존1,804기회를 그대로 두고 해당일에 R0 P1이 관측됐는지, 그 P2/P3가 같은 기존 D0+20 만기 안에 진입했는지를 비교한다. GC0와 D+1~10은 gc_offset으로 분리했다. 기존 사건 앞60일 내 새 신호를 연결한 BASELINE_COMPARISONS는 선행 발견의 기술적 대조이며, 각기 다른 만기의 순익을 인과적 우월성으로 해석하지 않는다. 새 조건이 제외한 기존 성공도 같은 표에 남는다. 60일 전부터 이어진 것인지의 실제 과정은 차트와 상태 원장을 확인해야 한다. LEAD_COMPARISONS는 미래RSI 선택을 거치지 않은 전체 거래일 BLUE20 CLEAR_BODY까지의 선행 거래일을 따로 기록했다. 기준선1,804에 연결되지 않았다는 것을 기존 검색기가 항상 놓쳤다는 뜻으로 해석하지 않는다.',table(pd.DataFrame(lead_summary)),
    '## 지정8사례의 규칙 해당 여부',table(cases),
    'NOT_P1_BY_R0는 새 규칙에 맞지 않는다는 뜻이며 사례를 살리려고 조건을 완화하지 않았다. P0 선행이 없거나 P3가 확인되지 않은 것도 그대로 기록했다. 추가 차트는 전이 실패·재상승 후 손절·P3 도달의 날짜순 첫 사례로 선정했다. 이 선택은 설명을 위한 사후 사례 선택이며 신호 성능 추정의 표본을 바꾸지 않는다.',
    '## 강한 매물대는 계속 연구한다',
    '강한 반복 가격 저항 증거는 D−1 이전의 최소3회 독립 반락과 중앙 반락폭1ATR 이상으로 고정했다. strong_overhead는 존 전체가 종가 위에 남은 경우다. 종가가 강한 존 안에 있는 price_strong_inside와 이미 아래로 통과한 price_strong_historical_below를 따로 표시한다. 상단이 없다는 것을 존 안 저항까지 없다는 의미로 읽지 않는다. 일봉 거래 집중은 별도로 균등·대표가격이 같은 상단 구간에서 일치하는 후보를 표시한다. NONE_DETECTED는 해당 모델/기간의 미검출이며 실제 매도 물량 부재가 아니다. UNKNOWN은 이력 부족이다. 실제 매도 잔량 인증은 전부 UNVERIFIED다. 매물대를 못 찾았다고 문제를 버리거나, 거래량 감소를 기관 매집으로 바꾸어 해석하지 않았다.',
    'VOLUME_RESISTANCE_EVIDENCE.csv.gz와 PRICE_RESISTANCE_ZONES.csv.gz에 각 신호 시점의 존재 증거를 저장했다. 실체결량 미검증 상태에서 이 플래그로 보류 규칙을 최적화하지 않았다. TradingView 공식 FRVP의 낮은 시간봉·고정 범위·호가 구간 개념은 VIDEO_AND_TRADINGVIEW_KO.md에 출처와 함께 정리했다. 실제 TradingView 수치 대조는 아직 하지 않았다.',
    '## 연도·중복·시장·이평 안정성',
    'PERFORMANCE_SUMMARY에2023/2024·60일중복제거·큰갭제외·기존 원천 단서 제외를 제공한다. STABILITY_GROUPS는60/112/224 배열과112/224/448 별도배열, RSI GC·Slow GC, KOSPI/KOSDAQ의 당일종가 대60일선 상태별 결과다. 이는 동시 특성의 기술적 비교이며 하위집단을 골라 유리한 조건으로 채택하지 않는다. 표본이 작고 종목 간 상관이 있어 독립 검증이나 통계적 유의성 인증이 아니다.',
    '## R0 해석',
    'P0는 관심 후보이며 매수 신호가 아니다. 관측 완료 집합의 P1 도달률은 두 해 모두 약21.7%로, 약78%는20일 안에 정한 첫 파동으로 발전하지 않았다. P3 기술적 재상승 관측도 수익 성공과 다르다. 현재 동일기회 비교의 거래당 평균은 양의 우월성을 입증하지 못했고, P3 현금 포함 손실이 작아 보이는 경우에는 낮은 진입률과 놓친 상승도 함께 봐야 한다. 기존 성공 사례를 다수 제외할 수 있으므로 기존 검색기를 대체할 근거가 없다.\n\nHLB에는3월7일 P3가 잡힌다. 삼양사에는6월9일 P3가 잡히지만 다음6월12일 시가 진입의 단계별+20일 고정 손절 비교는−5.25%였다. 6월9일이 사용자가 보신 두 번째 기회와 같은 날짜인지는 아직 확인하지 않았다. 후속 기회를 찾는 기능과 그 기회가 좋은 매매인지의 문제를 분리해야 한다. 이 사례에 맞춰 P3나 저항 임계값을 바꾸지 않았다.',
    'FORWARD_PATHS에 D+5/10/20/40/60 종가 수익·최고/최저 경로를 제공한다. 2024년 말의 미래 자료는 관측부족으로 남겼다. 2025~2026년 OHLCV는 읽지 않았다. 다만 참고 영상은 이미2025년 성공 사례를 보여 주므로 완전히 보지 않은 검증 자료로 간주하지 않는다. 추후 종목을 식별하면 해당 영상 사례를 독립 평가에서 구분해야 한다.',
    '## 실행·미검증 범위',
    f'차트 {count}개. 로컬 코드·원장 계산을 실행했다. GitHub 별도 브랜치 생성은 연결권한403으로 실패했으며 이번 신규 Actions는 미실행이다. 별도 브랜치와 검토용 패치 ZIP을 제공한다. 기존 main을 변경하지 않았다. 참고 영상의 점선 수식·종목·실매매 시점, HTS/TradingView 동일 설정 수치, 실체결량·전체 기업행사 가격·수량 기준, 2025~2026년 독립 검증은 미완료다.',
    '충분한 검증과 사용자 승인 전 기존 알고리즘에 통합하지 않는다. 기존 D−1 사전 확정 저항 R1 패치는 원격에서 확인되지 않았고 적용 완료로 간주하지 않았다. 이번 D−1 반락 존은 새 프로젝트의 독립 규칙이다.']
    if os.environ.get('GITHUB_ACTIONS')=='true':lines[-2]=f'GitHub Actions 안에서 실행: https://github.com/{os.environ.get("GITHUB_REPOSITORY")}/actions/runs/{os.environ.get("GITHUB_RUN_ID")}. 전체 성공 여부는 최종 작업 상태를 확인한다. 실제 영상/HTS/체결량·전체 기업행사·독립 검증 상태는 위 한계를 유지한다.'
    (out/'REPORT_KO.md').write_text('\n\n'.join(lines),encoding='utf-8');print(json.dumps({'charts':count,'reports':'COMPLETE'}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args().manifest)
